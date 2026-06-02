import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING

from ductor_bot.config import AgentConfig
from ductor_bot.messenger.notifications import NotificationService
from ductor_bot.messenger.protocol import BotProtocol
from ductor_bot.session.key import SessionKey
from ductor_bot.messenger.whatsapp.id_map import WhatsAppIdMap

if TYPE_CHECKING:
    from ductor_bot.bus.bus import MessageBus
    from ductor_bot.bus.lock_pool import LockPool
    from ductor_bot.multiagent.bus import AsyncInterAgentResult
    from ductor_bot.orchestrator.core import Orchestrator
    from ductor_bot.tasks.models import TaskResult
    from ductor_bot.workspace.paths import DuctorPaths

logger = logging.getLogger(__name__)

class WANotificationService(NotificationService):
    def __init__(self, bot: "WhatsAppBot"):
        self._bot = bot

    async def notify(self, chat_id: int, text: str) -> None:
        await self._bot.send_text(chat_id, text)

    async def notify_all(self, text: str) -> None:
        # Broadcasting to all WA users not fully implemented, skip for now
        pass

class WhatsAppBot(BotProtocol):
    def __init__(
        self,
        config: AgentConfig,
        *,
        agent_name: str = "main",
        bus: "MessageBus | None" = None,
        lock_pool: "LockPool | None" = None,
    ) -> None:
        self._config = config
        self._agent_name = agent_name
        self._bus = bus
        self._lock_pool = lock_pool
        self._orchestrator: "Orchestrator | None" = None
        self._abort_all_callback: Callable[[], Awaitable[int]] | None = None
        self._startup_hooks: list[Callable[[], Awaitable[None]]] = []
        self._notification_service = WANotificationService(self)
        self._process: asyncio.subprocess.Process | None = None
        self._sidecar_task: asyncio.Task | None = None
        self._id_map: WhatsAppIdMap | None = None
        self._my_jid: str | None = None
        self._my_lid: str | None = None

    @property
    def orchestrator(self) -> "Orchestrator | None":
        return self._orchestrator

    @property
    def config(self) -> AgentConfig:
        return self._config

    @property
    def notification_service(self) -> NotificationService:
        return self._notification_service

    async def run(self) -> int:
        if not self._config.whatsapp.enabled:
            logger.info("WhatsApp transport is disabled.")
            return 0

        for hook in self._startup_hooks:
            await hook()

        from ductor_bot.workspace.paths import resolve_paths
        paths = resolve_paths(self._config.ductor_home)
        sidecar_dir = Path(__file__).resolve().parent / "sidecar"
        auth_dir = paths.wa_auth_dir

        # Let's try to start it
        index_js = sidecar_dir / "index.js"
        if not index_js.exists():
            logger.error("WhatsApp sidecar index.js not found. Did you run 'ductor wa setup'?")
            return 1

        self._id_map = WhatsAppIdMap(auth_dir)

        self._process = await asyncio.create_subprocess_exec(
            "node", "index.js", str(auth_dir), "daemon",
            cwd=str(sidecar_dir),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("WhatsApp sidecar started with PID %s", self._process.pid)

        self._sidecar_task = asyncio.create_task(self._read_stdout())
        
        await self._process.wait()
        return 0

    async def _read_stdout(self) -> None:
        if not self._process or not self._process.stdout or not self._process.stdin:
            return
        try:
            async for line in self._process.stdout:
                line_str = line.decode("utf-8").strip()
                if not line_str:
                    continue
                try:
                    data = json.loads(line_str)
                    
                    if data.get("type") == "connection":
                        self._my_jid = data.get("myJid")
                        user_obj = data.get("user", {})
                        # LID is sometimes in id, sometimes in lid, let's just grab if it exists
                        self._my_lid = user_obj.get("lid") or (user_obj.get("id", "") if "@lid" in user_obj.get("id", "") else None)
                        if not self._my_lid and user_obj.get("lid"):
                            self._my_lid = user_obj.get("lid")
                            
                    elif data.get("type") == "message":
                        msg = data.get("message", {})
                        key = msg.get("key", {})
                        if key.get("fromMe"):
                            continue
                            
                        jid = key.get("remoteJid")
                        
                        message_content = msg.get("message", {})
                        text = message_content.get("conversation") or message_content.get("extendedTextMessage", {}).get("text") or ""
                        
                        is_group = bool(jid and "@g.us" in jid)
                        if is_group:
                            # Only reply in groups if mentioned
                            context_info = message_content.get("extendedTextMessage", {}).get("contextInfo", {})
                            mentioned_jids = context_info.get("mentionedJid", [])
                            participant = context_info.get("participant", "")  # If it's a direct reply
                            
                            logger.info(f"Group message received. Bot JID: {self._my_jid}, Bot LID: {self._my_lid}, Mentioned: {mentioned_jids}, Replied to: {participant}, Text: {text}")
                            
                            is_mentioned = False
                            
                            # Helper to get base JID/LID without device suffix
                            def _base(j: str | None) -> str:
                                if not j: return ""
                                return j.split(":")[0] + ("@" + j.split("@")[1] if "@" in j else "")
                                
                            my_base_jid = _base(self._my_jid)
                            my_base_lid = _base(self._my_lid)
                            
                            base_mentioned = [_base(m) for m in mentioned_jids]
                            base_participant = _base(participant)
                            
                            if my_base_jid and (my_base_jid in base_mentioned or my_base_jid == base_participant):
                                is_mentioned = True
                            if my_base_lid and (my_base_lid in base_mentioned or my_base_lid == base_participant):
                                is_mentioned = True
                                
                            bot_number = self._my_jid.split("@")[0].split(":")[0] if self._my_jid else ""
                            bot_lid_number = self._my_lid.split("@")[0].split(":")[0] if self._my_lid else ""
                            
                            if bot_number and f"@{bot_number}" in text:
                                is_mentioned = True
                            if bot_lid_number and f"@{bot_lid_number}" in text:
                                is_mentioned = True
                            
                            if not is_mentioned:
                                continue
                            
                            # Remove the mention text if it's there
                            if bot_number:
                                text = text.replace(f"@{bot_number}", "")
                            if bot_lid_number:
                                text = text.replace(f"@{bot_lid_number}", "")
                            text = text.strip()
                            
                            # If they only typed the mention, give it a default greeting
                            if not text:
                                text = "Halo!"
                            
                        if text and self._orchestrator and self._id_map:
                            cmd_check = text.strip().lower()
                            is_command = cmd_check.startswith("/") or cmd_check.startswith("!")
                            if not is_command:
                                push_name = msg.get("pushName", "")
                                sender_phone = ""
                                if is_group:
                                    # In groups, the actual sender is in participant
                                    participant = key.get("participant", "")
                                    sender_phone = participant.split("@")[0] if participant else ""
                                else:
                                    sender_phone = jid.split("@")[0] if jid else ""
                                    
                                context_info = []
                                if push_name:
                                    context_info.append(f"Name='{push_name}'")
                                if sender_phone:
                                    context_info.append(f"Phone='{sender_phone}'")
                                if context_info:
                                    text = f"[System Context - Sender: {', '.join(context_info)}]\n{text}"

                            chat_id = self._id_map.jid_to_int(jid)
                            session_key = SessionKey("whatsapp", chat_id=chat_id, topic_id=None)
                            asyncio.create_task(self._handle_wa_message(session_key, text, jid))
                            
                except json.JSONDecodeError:
                    logger.debug("WhatsApp raw: %s", line_str)
        except asyncio.CancelledError:
            pass

    async def _handle_wa_message(self, session_key: SessionKey, text: str, jid: str) -> None:
        if not self._orchestrator:
            return

        cmd = text.strip().lower()
        if cmd in ("/stop", "/interrupt", "/stop_all"):
            killed = 0
            if self._orchestrator:
                killed += await self._orchestrator.abort_all()
            if self._abort_all_callback:
                killed += await self._abort_all_callback()
            msg = f"Berhasil menyetop {killed} proses yang sedang berjalan." if killed else "Tidak ada proses yang sedang berjalan."
            await self._send_raw(jid, msg)
            return

        class WATypingContext:
            def __init__(self, bot: "WhatsAppBot", jid: str):
                self.bot = bot
                self.jid = jid
                self.task = None

            async def __aenter__(self):
                self.task = asyncio.create_task(self._loop())
                return self

            async def __aexit__(self, *args):
                if self.task:
                    self.task.cancel()

            async def _loop(self):
                try:
                    while True:
                        if self.bot._process and self.bot._process.stdin:
                            cmd = json.dumps({"action": "typing", "jid": self.jid}) + "\n"
                            self.bot._process.stdin.write(cmd.encode("utf-8"))
                            await self.bot._process.stdin.drain()
                        await asyncio.sleep(8)
                except asyncio.CancelledError:
                    pass

        async with WATypingContext(self, jid):
            result = await self._orchestrator.handle_message(session_key, text)

        if result.text:
            await self._send_raw(jid, result.text)

    async def send_text(self, chat_id: int, text: str) -> None:
        if not self._id_map:
            return
        jid = self._id_map.int_to_jid(chat_id)
        if jid:
            await self._send_raw(jid, text)
            
    async def _send_raw(self, jid: str, text: str) -> None:
        if not self._process or not self._process.stdin:
            return
        cmd = json.dumps({"action": "send", "jid": jid, "text": text}) + "\n"
        self._process.stdin.write(cmd.encode("utf-8"))
        await self._process.stdin.drain()

    async def shutdown(self) -> None:
        if self._sidecar_task:
            self._sidecar_task.cancel()
        if self._process:
            try:
                self._process.terminate()
            except ProcessLookupError:
                pass
            await self._process.wait()

    def register_startup_hook(self, hook: Callable[[], Awaitable[None]]) -> None:
        self._startup_hooks.append(hook)

    def set_abort_all_callback(self, callback: Callable[[], Awaitable[int]]) -> None:
        self._abort_all_callback = callback

    async def on_async_interagent_result(self, result: "AsyncInterAgentResult") -> None:
        if not result.chat_id or getattr(result, "transport", "") != "whatsapp":
            return
        from ductor_bot.bus.adapters import build_interagent_injection_prompt
        prompt = build_interagent_injection_prompt(
            result,
            agent_name=self._agent_name,
            transport_label="WhatsApp chat",
        )
        if not prompt:
            await self.send_text(result.chat_id, result.result_text)
            return
        if self._orchestrator:
            key = SessionKey("whatsapp", chat_id=result.chat_id, topic_id=result.topic_id)
            await self._orchestrator.handle_async_interagent_result(key, prompt)

    async def on_task_result(self, result: "TaskResult") -> None:
        if not self._orchestrator or getattr(result, "transport", "") != "whatsapp":
            return
        key = SessionKey("whatsapp", chat_id=result.chat_id, topic_id=result.thread_id)
        await self._orchestrator.handle_task_result(key, result)
        # Assuming the orchestrator will send the reply back via message bus or directly,
        # but wait, handle_task_result is fire-and-forget for injection.
        # We also need to send the actual result text back to WA:
        if result.result_text:
            await self.send_text(result.chat_id, result.result_text)

    async def on_task_question(
        self,
        task_id: str,
        question: str,
        prompt_preview: str,
        chat_id: int,
        thread_id: int | None = None,
    ) -> None:
        await self.send_text(chat_id, f"**Task Question:**\n{question}")

    def file_roots(self, paths: "DuctorPaths") -> list[Path] | None:
        return None
