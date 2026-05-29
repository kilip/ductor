"""WhatsApp typing indicator context manager.

Sends a ``typing`` status while the AI processes a request, then clears it.
WhatsApp Cloud API supports ``typing_on``/``typing_off`` via the
``/messages`` endpoint with ``type: "text"`` and ``preview_url: false``
— or more accurately through the ``pywa`` client's built-in helpers.

Because WhatsApp automatically clears the typing indicator after ~10 s,
a background keep-alive task re-sends it periodically.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)


class WhatsAppTypingContext:
    """Context manager that shows a typing indicator in a WhatsApp chat.

    Usage::

        async with WhatsAppTypingContext(wa_client, chat_id):
            result = await orchestrator.handle_message(key, text)
    """

    def __init__(
        self,
        wa: object,  # pywa.WhatsApp — imported lazily to keep typing optional
        chat_id: str,
        *,
        interval: float = 8.0,
    ) -> None:
        self._wa = wa
        self._chat_id = chat_id
        self._interval = interval
        self._task: asyncio.Task[None] | None = None

    async def _send_typing(self) -> None:
        """Send a single typing_on event (fire-and-forget, suppress errors)."""
        with contextlib.suppress(Exception):
            # pywa exposes set_typing_status(to, typing)
            await asyncio.to_thread(self._wa.set_typing_status, self._chat_id, True)  # type: ignore[union-attr]

    async def _clear_typing(self) -> None:
        """Send typing_off event."""
        with contextlib.suppress(Exception):
            await asyncio.to_thread(self._wa.set_typing_status, self._chat_id, False)  # type: ignore[union-attr]

    async def _keep_alive(self) -> None:
        """Periodically re-send typing indicator."""
        while True:
            await asyncio.sleep(self._interval)
            await self._send_typing()

    async def __aenter__(self) -> Self:
        await self._send_typing()
        self._task = asyncio.create_task(self._keep_alive(), name=f"wa-typing-{self._chat_id}")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._clear_typing()
