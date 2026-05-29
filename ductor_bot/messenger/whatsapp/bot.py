"""WhatsApp Cloud API bot, parallel to TelegramBot and MatrixBot.

Implements BotProtocol so the supervisor can manage it identically
to the other transports without knowing which one is active.

Incoming messages arrive via aiohttp webhook (POST /whatsapp/webhook).
The pywa library handles the WhatsApp Cloud API low-level HTTP calls.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import TYPE_CHECKING

from ductor_bot.bus.bus import MessageBus
from ductor_bot.bus.lock_pool import LockPool
from ductor_bot.commands import BOT_COMMANDS, MULTIAGENT_SUB_COMMANDS
from ductor_bot.config import AgentConfig
from ductor_bot.files.allowed_roots import resolve_allowed_roots
from ductor_bot.messenger.commands import classify_command
from ductor_bot.messenger.notifications import NotificationService
from ductor_bot.messenger.whatsapp.auth import is_allowed, is_group_jid, normalize_phone
from ductor_bot.messenger.whatsapp.formatting import md_to_whatsapp, split_message
from ductor_bot.messenger.whatsapp.media import extract_file_tags, send_file_to_whatsapp
from ductor_bot.messenger.whatsapp.typing import WhatsAppTypingContext
from ductor_bot.session.key import SessionKey
from ductor_bot.text.response_format import SEP, fmt

if TYPE_CHECKING:
    from ductor_bot.infra.updater import UpdateObserver
    from ductor_bot.multiagent.bus import AsyncInterAgentResult
    from ductor_bot.orchestrator.core import Orchestrator
    from ductor_bot.tasks.models import TaskResult
    from ductor_bot.workspace.paths import DuctorPaths

logger = logging.getLogger(__name__)

# Chat ID counter for mapping string JIDs to integer session chat_ids
_CHAT_ID_COUNTER: dict[str, int] = {}
_CHAT_ID_REVERSE: dict[int, str] = {}
_CHAT_ID_SEQ: list[int] = [0]  # mutable int in a list for closure mutation


def _jid_to_int(jid: str) -> int:
    """Map a WhatsApp JID to a stable integer chat_id."""
    if jid not in _CHAT_ID_COUNTER:
        _CHAT_ID_SEQ[0] += 1
        _CHAT_ID_COUNTER[jid] = _CHAT_ID_SEQ[0]
        _CHAT_ID_REVERSE[_CHAT_ID_SEQ[0]] = jid
    return _CHAT_ID_COUNTER[jid]


def _int_to_jid(chat_id: int) -> str | None:
    """Reverse-map an integer chat_id back to a WhatsApp JID."""
    return _CHAT_ID_REVERSE.get(chat_id)


class WhatsAppNotificationService:
    """NotificationService implementation for WhatsApp."""

    def __init__(self, bot: WhatsAppBot) -> None:
        self._bot = bot

    async def notify(self, chat_id: int, text: str) -> None:
        jid = _int_to_jid(chat_id)
        if jid:
            await self._bot.send_message(jid, text)
        else:
            logger.warning(
                "notify: cannot resolve chat_id=%d to JID, falling back to notify_all", chat_id
            )
            await self.notify_all(text)

    async def notify_all(self, text: str) -> None:
        await self._bot.broadcast(text)


class WhatsAppBot:
    """WhatsApp transport bot implementing BotProtocol."""

    def __init__(
        self,
        config: AgentConfig,
        *,
        agent_name: str = "main",
        bus: MessageBus | None = None,
        lock_pool: LockPool | None = None,
    ) -> None:
        try:
            import pywa  # noqa: F401
        except ImportError:
            raise ImportError(
                "pywa is required for WhatsApp transport. "
                "Install with: pip install 'ductor[whatsapp]'"
            ) from None

        self._config = config
        self._agent_name = agent_name
        self._lock_pool = lock_pool or LockPool()
        self._bus = bus or MessageBus(lock_pool=self._lock_pool)

        from ductor_bot.messenger.whatsapp.transport import WhatsAppTransport

        self._bus.register_transport(WhatsAppTransport(self))

        self._orchestrator: Orchestrator | None = None
        self._startup_hooks: list[Callable[[], Awaitable[None]]] = []
        self._notification_service: NotificationService = WhatsAppNotificationService(self)
        self._abort_all_callback: Callable[[], Awaitable[int]] | None = None
        self._exit_code: int = 0
        self._update_observer: UpdateObserver | None = None
        self._runner: object | None = None  # aiohttp AppRunner

        # Allowed users set for fast lookup
        self._allowed_users: list[str] = list(config.whatsapp.allowed_users)

        # Last active JID (fallback for broadcast when no allowed_users)
        self._last_active_jid: str | None = None

        # Keep fire-and-forget tasks alive
        self._background_tasks: set[asyncio.Task[None]] = set()

        # Dedup: recent message IDs we've already processed
        self._seen_message_ids: deque[str] = deque(maxlen=500)

        # pywa client (lazy init in run())
        self._wa: object | None = None

    # --- BotProtocol implementation ---

    @property
    def _orch(self) -> Orchestrator:
        if self._orchestrator is None:
            msg = "Orchestrator not initialized -- call after startup"
            raise RuntimeError(msg)
        return self._orchestrator

    @property
    def orchestrator(self) -> Orchestrator | None:
        return self._orchestrator

    @property
    def config(self) -> AgentConfig:
        return self._config

    @property
    def notification_service(self) -> NotificationService:
        return self._notification_service

    def register_startup_hook(self, hook: Callable[[], Awaitable[None]]) -> None:
        self._startup_hooks.append(hook)

    def set_abort_all_callback(self, callback: Callable[[], Awaitable[int]]) -> None:
        self._abort_all_callback = callback

    def file_roots(self, paths: DuctorPaths) -> list[Path] | None:
        return resolve_allowed_roots(self._config.file_access, paths.workspace)

    async def notify_startup(self, text: str) -> None:
        """Send startup notification to configured targets or broadcast."""
        configured = self._config.notifications.startup_targets
        if not configured:
            await self._notification_service.notify_all(text)
            return
        targets = [tgt for tgt in configured if tgt.enabled and tgt.chat_id is not None]
        for target in targets:
            try:
                assert target.chat_id is not None
                await self._notification_service.notify(target.chat_id, text)
            except Exception:
                logger.warning(
                    "notify_startup: delivery failed for chat_id=%s",
                    target.chat_id,
                    exc_info=True,
                )

    async def notify_upgrade(self, text: str) -> None:
        """Send upgrade-available notification."""
        configured = self._config.notifications.upgrade_targets
        if not configured:
            await self.broadcast(text)
            return
        targets = [tgt for tgt in configured if tgt.enabled and tgt.chat_id is not None]
        for target in targets:
            try:
                assert target.chat_id is not None
                await self._notification_service.notify(target.chat_id, text)
            except Exception:
                logger.warning(
                    "notify_upgrade: delivery failed for chat_id=%s",
                    target.chat_id,
                    exc_info=True,
                )

    async def run(self) -> int:
        """Start the WhatsApp webhook server. Blocks until shutdown."""
        from aiohttp import web

        wa_cfg = self._config.whatsapp
        self._wa = self._build_pywa_client(wa_cfg)

        app = web.Application()
        from ductor_bot.messenger.whatsapp.handlers import register_webhook_routes

        register_webhook_routes(app, self, wa_cfg.webhook_path)

        from ductor_bot.messenger.whatsapp.startup import run_whatsapp_startup

        # Run startup (orchestrator, observers, hooks)
        await run_whatsapp_startup(self)

        # Start aiohttp server
        runner = web.AppRunner(app)
        self._runner = runner
        await runner.setup()
        site = web.TCPSite(runner, wa_cfg.webhook_host, wa_cfg.webhook_port)
        await site.start()
        logger.info(
            "WhatsApp webhook server started on %s:%d%s",
            wa_cfg.webhook_host,
            wa_cfg.webhook_port,
            wa_cfg.webhook_path,
        )

        # Block forever (until shutdown() cancels us or sets exit code)
        try:
            while self._exit_code == 0:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

        return self._exit_code

    async def shutdown(self) -> None:
        """Gracefully shut down."""
        if self._update_observer:
            await self._update_observer.stop()

        if self._runner:
            await self._runner.cleanup()  # type: ignore[union-attr]

        if self._orchestrator:
            await self._orchestrator.shutdown()

        logger.info("WhatsAppBot shut down")

    # --- pywa client factory ---

    def _build_pywa_client(self, wa_cfg: object) -> object:
        """Create and return a pywa WhatsApp client in server mode."""
        from pywa import WhatsApp  # type: ignore[import-untyped]

        return WhatsApp(
            phone_id=wa_cfg.phone_number_id,  # type: ignore[union-attr]
            token=wa_cfg.access_token,  # type: ignore[union-attr]
            server=None,  # webhook mode — we handle HTTP ourselves
            verify_token=wa_cfg.verify_token,  # type: ignore[union-attr]
        )

    # --- Task management ---

    def _spawn_task(
        self, coro: Coroutine[object, object, None], *, name: str
    ) -> asyncio.Task[None]:
        task: asyncio.Task[None] = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    # --- Webhook payload processing ---

    async def process_webhook_payload(self, payload: object) -> None:
        """Parse a WhatsApp Cloud API webhook payload and dispatch events."""
        try:
            entries = (payload or {}).get("entry", [])  # type: ignore[union-attr]
            for entry in entries:
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    messages = value.get("messages", [])
                    contacts = value.get("contacts", [])

                    # Build sender display name index
                    name_index: dict[str, str] = {}
                    for contact in contacts:
                        wa_id = contact.get("wa_id", "")
                        name = contact.get("profile", {}).get("name", wa_id)
                        name_index[wa_id] = name

                    for msg in messages:
                        await self._dispatch_message_payload(msg, name_index)
        except Exception:
            logger.warning("Error processing WhatsApp webhook payload", exc_info=True)

    async def _dispatch_message_payload(
        self, msg: dict[str, object], name_index: dict[str, str]
    ) -> None:
        """Route a single WhatsApp message to the appropriate handler."""
        msg_id = str(msg.get("id", ""))
        if msg_id and msg_id in self._seen_message_ids:
            return
        if msg_id:
            self._seen_message_ids.append(msg_id)

        sender_jid = str(msg.get("from", ""))
        chat_jid = str(msg.get("context", {}).get("from", sender_jid))  # type: ignore[union-attr]

        # For group messages, the "from" field is the group JID
        # and the actual sender is in "participant"
        if is_group_jid(sender_jid):
            group_jid = sender_jid
            actual_sender = str(msg.get("participant", sender_jid))
        else:
            group_jid = None
            actual_sender = sender_jid

        msg_type = str(msg.get("type", "text"))

        if msg_type == "text":
            text_body = str((msg.get("text") or {}).get("body", ""))  # type: ignore[union-attr]
            await self._on_message(
                chat_jid=group_jid or actual_sender,
                sender_jid=actual_sender,
                text=text_body,
                is_group=group_jid is not None,
            )
        elif msg_type in ("image", "document", "audio", "video"):
            await self._on_media(
                chat_jid=group_jid or actual_sender,
                sender_jid=actual_sender,
                msg=msg,
                is_group=group_jid is not None,
            )
        # Status updates and other types are silently ignored

    async def _on_message(
        self,
        *,
        chat_jid: str,
        sender_jid: str,
        text: str,
        is_group: bool,
    ) -> None:
        """Handle an incoming text message."""
        if not text.strip():
            return

        # Authorization
        if not is_allowed(sender_jid, self._allowed_users):
            logger.debug("WhatsApp: unauthorized sender %s", sender_jid)
            return

        # Group handling: commands disabled, process only @mention messages
        if is_group:
            text = self._filter_group_message(text)
            if text is None:
                return

        self._last_active_jid = chat_jid
        chat_id = _jid_to_int(chat_jid)
        key = SessionKey.for_transport("wa", chat_id)

        # Command dispatch
        if text.startswith("/"):
            if not is_group:
                await self._handle_command(text, chat_jid, chat_id, key)
            # Commands are disabled in groups
            return

        self._spawn_task(
            self._dispatch_with_lock(key, text, chat_jid),
            name=f"wa-msg-{chat_jid[-8:]}",
        )

    async def _on_media(
        self,
        *,
        chat_jid: str,
        sender_jid: str,
        msg: dict[str, object],
        is_group: bool,
    ) -> None:
        """Handle an incoming media message (image, document, audio, video)."""
        if not is_allowed(sender_jid, self._allowed_users):
            return

        # In groups, only process if it's a reply/mention (simplistic: always process for now)
        if is_group:
            return  # skip media in groups for now

        self._last_active_jid = chat_jid
        chat_id = _jid_to_int(chat_jid)
        key = SessionKey.for_transport("wa", chat_id)

        # Build a text prompt describing the media
        msg_type = str(msg.get("type", "document"))
        caption = str((msg.get(msg_type) or {}).get("caption", ""))  # type: ignore[union-attr]
        prompt = caption.strip() if caption else f"[Received {msg_type}]"

        self._spawn_task(
            self._dispatch_with_lock(key, prompt, chat_jid),
            name=f"wa-media-{chat_jid[-8:]}",
        )

    def _filter_group_message(self, text: str) -> str | None:
        """In groups: return message text only if the bot is @mentioned.

        Strips the mention prefix and returns the remaining text.
        Returns None if the message is not addressed to the bot.
        """
        wa_cfg = self._config.whatsapp
        bot_number = normalize_phone(wa_cfg.phone_number_id)

        # Check for @<phone> mention
        mention = f"@{bot_number.lstrip('+')}"
        if mention not in text and bot_number not in text:
            return None

        # Strip mention
        cleaned = text.replace(mention, "").replace(bot_number, "").strip()
        return cleaned or None

    # --- Command handling ---

    async def _handle_command(
        self, text: str, chat_jid: str, chat_id: int, key: SessionKey
    ) -> None:
        """Handle a slash command from a 1-1 chat."""
        cmd = text.split(maxsplit=1)[0].lower().lstrip("/")

        handler = self._COMMAND_DISPATCH.get(cmd)
        if handler is not None:
            if cmd in self._IMMEDIATE_COMMANDS:
                await handler(self, text=text, chat_jid=chat_jid, key=key)
            else:
                self._spawn_task(
                    self._run_handler_with_lock(handler, text=text, chat_jid=chat_jid, key=key),
                    name=f"wa-cmd-{cmd}",
                )
        elif classify_command(cmd) in ("orchestrator", "multiagent"):
            self._spawn_task(
                self._cmd_orchestrator_locked(text=text, chat_jid=chat_jid, key=key),
                name=f"wa-orch-{cmd}",
            )
        else:
            # Unknown command → treat as regular message
            self._spawn_task(
                self._dispatch_with_lock(key, text, chat_jid),
                name=f"wa-cmd-{cmd}",
            )

    async def _cmd_stop(self, *, text: str, chat_jid: str, key: SessionKey) -> None:
        orch = self._orchestrator
        killed = 0
        if orch:
            killed = await orch.abort(key.chat_id)
        from ductor_bot.i18n import t

        msg = t("abort_all.done", count=killed) if killed else t("abort_all.nothing")
        await self.send_message(chat_jid, msg)

    async def _cmd_interrupt(self, *, text: str, chat_jid: str, key: SessionKey) -> None:
        orch = self._orchestrator
        if orch:
            from ductor_bot.i18n import t

            interrupted = orch.interrupt(key.chat_id)
            msg = t("interrupt.done", count=interrupted) if interrupted else t("interrupt.nothing")
            await self.send_message(chat_jid, msg)

    async def _cmd_new(self, *, text: str, chat_jid: str, key: SessionKey) -> None:
        orch = self._orchestrator
        if orch:
            result = await orch.handle_message(key, "/new")
            if result and result.text:
                await self.send_message(chat_jid, result.text)

    async def _cmd_help(self, *, text: str, chat_jid: str, key: SessionKey) -> None:
        await self.send_message(chat_jid, self._build_help_text())

    async def _cmd_info(self, *, text: str, chat_jid: str, key: SessionKey) -> None:
        from ductor_bot.infra.version import get_current_version

        version = get_current_version()
        out = fmt(
            "*ductor — WhatsApp transport*",
            f"Version: {version}",
            SEP,
            "Commands use /slash syntax. Streaming is not available on WhatsApp.",
        )
        await self.send_message(chat_jid, out)

    async def _cmd_orchestrator(self, *, text: str, chat_jid: str, key: SessionKey) -> None:
        orch = self._orchestrator
        if not orch:
            return
        result = await orch.handle_message(key, text)
        if result and result.text:
            await self.send_message(chat_jid, result.text)

    async def _dispatch_with_lock(
        self, key: SessionKey, text: str, chat_jid: str
    ) -> None:
        lock = self._lock_pool.get(key.lock_key)
        async with lock:
            await self._dispatch_message(key, text, chat_jid)

    async def _run_handler_with_lock(
        self, handler: Callable[..., Awaitable[None]], **kwargs: object
    ) -> None:
        key: SessionKey = kwargs["key"]  # type: ignore[assignment]
        lock = self._lock_pool.get(key.lock_key)
        async with lock:
            await handler(self, **kwargs)

    async def _cmd_orchestrator_locked(
        self, *, text: str, chat_jid: str, key: SessionKey
    ) -> None:
        lock = self._lock_pool.get(key.lock_key)
        async with lock:
            await self._cmd_orchestrator(text=text, chat_jid=chat_jid, key=key)

    async def _dispatch_message(self, key: SessionKey, text: str, chat_jid: str) -> None:
        """Route a message through the orchestrator (non-streaming: typing → send)."""
        orch = self._orchestrator
        if orch is None:
            return

        async with WhatsAppTypingContext(self._wa, chat_jid):
            result = await orch.handle_message(key, text)

        self._maybe_append_footer(result)
        if result.text:
            await self.send_message(chat_jid, result.text)

    def _build_help_text(self) -> str:
        cmd_desc = {**dict(BOT_COMMANDS), **dict(MULTIAGENT_SUB_COMMANDS)}

        def _line(c: str) -> str:
            desc = cmd_desc.get(c, "")
            return f"`/{c}` — {desc}" if desc else f"`/{c}`"

        return fmt(
            "*ductor commands (WhatsApp)*",
            SEP,
            f"*Daily*\n{_line('new')}\n{_line('stop')}\n{_line('model')}\n{_line('status')}",
            f"*Automation*\n{_line('session')}\n{_line('tasks')}\n{_line('cron')}",
            f"*Info*\n{_line('info')}\n{_line('help')}",
            SEP,
            "In group chats: @mention the bot to send a message. Commands are disabled in groups.",
        )

    def _maybe_append_footer(self, result: object) -> None:
        from ductor_bot.orchestrator.registry import OrchestratorResult

        if not isinstance(result, OrchestratorResult):
            return
        if not self._config.scene.technical_footer or not result.model_name:
            return
        from ductor_bot.text.response_format import format_technical_footer

        footer = format_technical_footer(
            result.model_name,
            result.total_tokens,
            result.input_tokens,
            result.cost_usd,
            result.duration_ms,
        )
        result.text += footer

    _COMMAND_DISPATCH: dict[str, Callable[..., Awaitable[None]]] = {
        "stop": _cmd_stop,
        "interrupt": _cmd_interrupt,
        "new": _cmd_new,
        "help": _cmd_help,
        "start": _cmd_help,
        "info": _cmd_info,
    }

    _IMMEDIATE_COMMANDS: frozenset[str] = frozenset(
        {"stop", "interrupt", "help", "start", "info"}
    )

    # --- Message sending ---

    async def send_message(self, chat_jid: str, text: str) -> None:
        """Send a WhatsApp text message, splitting if needed."""
        if not text.strip():
            return

        wa_text = md_to_whatsapp(text)
        file_tags, wa_text = extract_file_tags(wa_text)

        # Send text chunks
        for chunk in split_message(wa_text):
            try:
                await asyncio.to_thread(
                    self._wa.send_message,  # type: ignore[union-attr]
                    chat_jid,
                    chunk,
                )
            except Exception:
                logger.warning("Failed to send WhatsApp message to %s", chat_jid, exc_info=True)

        # Send any file attachments
        if file_tags and self._orchestrator:
            allowed_roots = self.file_roots(self._orchestrator.paths)
            for tag in file_tags:
                from ductor_bot.files.tags import path_from_file_tag

                file_path = path_from_file_tag(tag)
                await send_file_to_whatsapp(self._wa, chat_jid, file_path, allowed_roots)

    async def broadcast(self, text: str) -> None:
        """Send a message to all allowed WhatsApp chats (or last active)."""
        targets: list[str] = list(self._allowed_users)
        if not targets and self._last_active_jid:
            targets = [self._last_active_jid]
        if not targets:
            logger.warning("broadcast: no targets available, message lost: %s", text[:80])
            return
        for jid in targets:
            await self.send_message(jid, text)

    def resolve_chat_id(self, chat_id: int) -> str | None:
        """Reverse-map a MessageBus chat_id integer to a WhatsApp JID."""
        return _int_to_jid(chat_id)

    # --- Inter-agent & task handlers (BotProtocol) ---

    async def on_async_interagent_result(self, result: AsyncInterAgentResult) -> None:
        from ductor_bot.bus.adapters import (
            build_interagent_injection_prompt,
            from_interagent_result,
        )

        if result.transport and result.transport != "wa":
            logger.debug(
                "Skipping async interagent result for transport=%s in WhatsApp handler",
                result.transport,
            )
            return

        chat_id = self._default_chat_id()
        if not chat_id:
            text = result.result_text or f"Inter-agent result from {result.recipient}"
            await self._notification_service.notify_all(text)
            return

        injection_prompt = build_interagent_injection_prompt(
            result,
            agent_name=self._agent_name,
            transport_label="WhatsApp chat",
        )

        await self._bus.submit(
            from_interagent_result(
                result,
                chat_id,
                injection_prompt=injection_prompt,
                transport="wa",
            )
        )

    async def on_task_result(self, result: TaskResult) -> None:
        from ductor_bot.bus.adapters import from_task_result

        await self._bus.submit(from_task_result(result))

    async def on_task_question(
        self,
        task_id: str,
        question: str,
        prompt_preview: str,
        chat_id: int,
        thread_id: int | None = None,
    ) -> None:
        from ductor_bot.bus.adapters import from_task_question

        if not chat_id:
            chat_id = self._default_chat_id()
        await self._bus.submit(from_task_question(task_id, question, prompt_preview, chat_id))

    def _default_chat_id(self) -> int:
        """Default delivery target: first allowed user, or last active JID."""
        if self._allowed_users:
            jid = self._allowed_users[0]
            return _jid_to_int(jid)
        if self._last_active_jid:
            return _jid_to_int(self._last_active_jid)
        logger.warning("No default chat_id: no allowed_users and no active JID yet")
        return 0
