"""WhatsApp delivery adapter for the MessageBus.

Translates :class:`Envelope` instances into WhatsApp messages, mirroring
the structure of the Matrix and Telegram transport adapters.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING

from ductor_bot.bus.cron_sanitize import sanitize_cron_result_text
from ductor_bot.bus.envelope import Envelope, Origin
from ductor_bot.text.response_format import SEP, fmt

if TYPE_CHECKING:
    from ductor_bot.messenger.whatsapp.bot import WhatsAppBot

logger = logging.getLogger(__name__)


class WhatsAppTransport:
    """Implements the ``TransportAdapter`` protocol for WhatsApp delivery."""

    def __init__(self, bot: WhatsAppBot) -> None:
        self._bot = bot

    # -- Protocol methods ---------------------------------------------------

    @property
    def transport_name(self) -> str:
        return "wa"

    async def deliver(self, envelope: Envelope) -> None:
        """Deliver a unicast envelope to the target WhatsApp chat."""
        handler = _HANDLERS.get(envelope.origin)
        if handler is not None:
            await handler(self, envelope)
        else:
            logger.warning("No handler for origin=%s", envelope.origin.value)

    async def deliver_broadcast(self, envelope: Envelope) -> None:
        """Deliver an envelope to all allowed chats."""
        handler = _BROADCAST_HANDLERS.get(envelope.origin)
        if handler is not None:
            await handler(self, envelope)
        else:
            logger.warning("No broadcast handler for origin=%s", envelope.origin.value)

    # -- Internal helpers ---------------------------------------------------

    async def _send(self, chat_id: str, text: str) -> None:
        await self._bot.send_message(chat_id, text)

    def _resolve_chat(self, env: Envelope) -> str | None:
        """Resolve envelope chat_id (int) back to a WhatsApp JID string."""
        return self._bot.resolve_chat_id(env.chat_id)

    def _allowed_roots(self) -> list[Path] | None:
        orch = self._bot.orchestrator
        if orch is None:
            return None
        return self._bot.file_roots(orch.paths)

    # -- Origin handlers (unicast) -----------------------------------------

    async def _deliver_background(self, env: Envelope) -> None:
        chat_id = self._resolve_chat(env)
        if not chat_id:
            return
        elapsed = f"{env.elapsed_seconds:.0f}s"
        if env.session_name:
            if env.status == "aborted":
                text = fmt(f"*[{env.session_name}] Cancelled*", SEP, f"_{env.prompt_preview}_")
            elif env.is_error:
                body = env.result_text[:2000] if env.result_text else "_No output._"
                text = fmt(f"*[{env.session_name}] Failed* ({elapsed})", SEP, body)
            else:
                text = fmt(
                    f"*[{env.session_name}] Complete* ({elapsed})",
                    SEP,
                    env.result_text or "_No output._",
                )
        else:
            task_id = env.metadata.get("task_id", "?")
            if env.status == "aborted":
                text = fmt(
                    "*Background Task Cancelled*",
                    SEP,
                    f"Task `{task_id}` was cancelled.\nPrompt: _{env.prompt_preview}_",
                )
            elif env.is_error:
                text = fmt(
                    f"*Background Task Failed* ({elapsed})",
                    SEP,
                    f"Task `{task_id}` failed ({env.status}).\nPrompt: _{env.prompt_preview}_\n\n"
                    + (env.result_text[:2000] if env.result_text else "_No output._"),
                )
            else:
                text = fmt(
                    f"*Background Task Complete* ({elapsed})",
                    SEP,
                    env.result_text or "_No output._",
                )
        await self._send(chat_id, text)

    async def _deliver_heartbeat(self, env: Envelope) -> None:
        chat_id = self._resolve_chat(env)
        if chat_id and env.result_text:
            await self._send(chat_id, env.result_text)

    async def _deliver_interagent(self, env: Envelope) -> None:
        chat_id = self._resolve_chat(env)
        if not chat_id:
            return
        if env.is_error:
            session_info = f"\nSession: `{env.session_name}`" if env.session_name else ""
            text = (
                f"*Inter-Agent Request Failed*\n\n"
                f"Agent: `{env.metadata.get('recipient', '?')}`{session_info}\n"
                f"Error: {env.metadata.get('error', 'unknown')}\n"
                f"Request: _{env.prompt_preview}_"
            )
            await self._send(chat_id, text)
            return

        notice = env.metadata.get("provider_switch_notice", "")
        if notice:
            await self._send(chat_id, f"*Provider Switch Detected*\n\n{notice}")
        if env.result_text:
            await self._send(chat_id, env.result_text)

    async def _deliver_task_result(self, env: Envelope) -> None:
        chat_id = self._resolve_chat(env)
        if not chat_id:
            return
        name = env.metadata.get("name", env.metadata.get("task_id", "?"))

        note = ""
        if env.status == "done":
            duration = f"{env.elapsed_seconds:.0f}s"
            target = f"{env.provider}/{env.model}" if env.provider else ""
            detail = f"{duration}, {target}" if target else duration
            note = f"*Task `{name}` completed* ({detail})"
        elif env.status == "cancelled":
            note = f"*Task `{name}` cancelled*"
        elif env.status == "failed":
            note = f"*Task `{name}` failed*\nReason: {env.metadata.get('error', 'unknown')}"

        if note:
            await self._send(chat_id, note)
        if env.needs_injection and env.result_text:
            await self._send(chat_id, env.result_text)

    async def _deliver_task_question(self, env: Envelope) -> None:
        chat_id = self._resolve_chat(env)
        if not chat_id:
            return
        task_id = env.metadata.get("task_id", "?")
        note = f"*Task `{task_id}` has a question:*\n{env.prompt}"
        await self._send(chat_id, note)
        if env.result_text:
            await self._send(chat_id, env.result_text)

    async def _deliver_webhook_wake(self, env: Envelope) -> None:
        chat_id = self._resolve_chat(env)
        if chat_id and env.result_text:
            await self._send(chat_id, env.result_text)

    async def _deliver_cron(self, env: Envelope) -> None:
        """Deliver cron result unicast; fall back to broadcast on failure."""
        chat_id = self._resolve_chat(env)
        if not chat_id:
            logger.warning(
                "Cron unicast: cannot resolve chat_id=%d, falling back to broadcast",
                env.chat_id,
            )
            await self._broadcast_cron(env)
            return
        title = env.metadata.get("title", "?")
        clean_result = sanitize_cron_result_text(env.result_text)
        if env.result_text and not clean_result and env.status == "success":
            return
        text = (
            f"*TASK: {title}*\n\n{clean_result}"
            if clean_result
            else f"*TASK: {title}*\n\n_{env.status}_"
        )
        await self._send(chat_id, text)

    # -- Origin handlers (broadcast) ----------------------------------------

    async def _broadcast_cron(self, env: Envelope) -> None:
        title = env.metadata.get("title", "?")
        clean_result = sanitize_cron_result_text(env.result_text)
        if env.result_text and not clean_result and env.status == "success":
            return
        text = (
            f"*TASK: {title}*\n\n{clean_result}"
            if clean_result
            else f"*TASK: {title}*\n\n_{env.status}_"
        )
        await self._broadcast(text)

    async def _broadcast_webhook_cron(self, env: Envelope) -> None:
        title = env.metadata.get("hook_title", "?")
        text = (
            f"*WEBHOOK (CRON TASK): {title}*\n\n{env.result_text}"
            if env.result_text
            else f"*WEBHOOK (CRON TASK): {title}*\n\n_{env.status}_"
        )
        await self._broadcast(text)

    async def _broadcast(self, text: str) -> None:
        """Send to all allowed WhatsApp chats."""
        await self._bot.broadcast(text)


# ---------------------------------------------------------------------------
# Handler dispatch tables
# ---------------------------------------------------------------------------

_Handler = Callable[[WhatsAppTransport, Envelope], Awaitable[None]]

_HANDLERS: dict[Origin, _Handler] = {
    Origin.BACKGROUND: WhatsAppTransport._deliver_background,
    Origin.CRON: WhatsAppTransport._deliver_cron,
    Origin.HEARTBEAT: WhatsAppTransport._deliver_heartbeat,
    Origin.INTERAGENT: WhatsAppTransport._deliver_interagent,
    Origin.TASK_RESULT: WhatsAppTransport._deliver_task_result,
    Origin.TASK_QUESTION: WhatsAppTransport._deliver_task_question,
    Origin.WEBHOOK_WAKE: WhatsAppTransport._deliver_webhook_wake,
}

_BROADCAST_HANDLERS: dict[Origin, _Handler] = {
    Origin.CRON: WhatsAppTransport._broadcast_cron,
    Origin.WEBHOOK_CRON: WhatsAppTransport._broadcast_webhook_cron,
}
