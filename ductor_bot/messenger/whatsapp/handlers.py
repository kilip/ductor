"""aiohttp request handlers for the WhatsApp Cloud API webhook.

Mounts two routes on the shared aiohttp app:
  GET  <path>   — webhook verification challenge (one-time Meta setup)
  POST <path>   — incoming events (messages, status updates)

All message processing is delegated to ``WhatsAppBot._on_message`` and
``WhatsAppBot._on_media``.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from ductor_bot.messenger.whatsapp.bot import WhatsAppBot

logger = logging.getLogger(__name__)


def register_webhook_routes(app: web.Application, bot: WhatsAppBot, path: str) -> None:
    """Register GET + POST webhook handlers on *app* at *path*."""
    app.router.add_get(path, _make_verify_handler(bot))
    app.router.add_post(path, _make_event_handler(bot))
    logger.info("WhatsApp webhook registered at %s", path)


def _make_verify_handler(bot: WhatsAppBot) -> web.RequestHandler:
    """Return a GET handler that answers Meta's webhook verification challenge."""

    async def verify(request: web.Request) -> web.Response:
        mode = request.rel_url.query.get("hub.mode", "")
        token = request.rel_url.query.get("hub.verify_token", "")
        challenge = request.rel_url.query.get("hub.challenge", "")

        if mode == "subscribe" and token == bot._config.whatsapp.verify_token:
            logger.info("WhatsApp webhook verification OK")
            return web.Response(text=challenge)

        logger.warning("WhatsApp webhook verification failed (token mismatch or bad mode)")
        return web.Response(status=403, text="Forbidden")

    return verify  # type: ignore[return-value]


def _make_event_handler(bot: WhatsAppBot) -> web.RequestHandler:
    """Return a POST handler that processes incoming WhatsApp events."""

    async def handle_event(request: web.Request) -> web.Response:
        try:
            body = await request.read()
        except Exception:
            logger.warning("Failed to read webhook body", exc_info=True)
            return web.Response(status=400)

        # Signature verification (optional but strongly recommended)
        app_secret = bot._config.whatsapp.app_secret
        if app_secret:
            sig_header = request.headers.get("X-Hub-Signature-256", "")
            if not _verify_signature(body, app_secret, sig_header):
                logger.warning("WhatsApp webhook: bad signature, rejecting")
                return web.Response(status=403, text="Forbidden")

        try:
            import json

            payload = json.loads(body)
        except Exception:
            logger.warning("WhatsApp webhook: invalid JSON body")
            return web.Response(status=400)

        # Process asynchronously — always return 200 immediately so Meta
        # does not retry the delivery
        import asyncio

        asyncio.create_task(  # noqa: RUF006  (fire-and-forget, bot owns lifecycle)
            bot.process_webhook_payload(payload),
            name="wa-webhook",
        )

        return web.Response(status=200, text="OK")

    return handle_event  # type: ignore[return-value]


def _verify_signature(body: bytes, secret: str, header: str) -> bool:
    """Return *True* when the HMAC-SHA256 signature matches.

    *header* is the ``X-Hub-Signature-256`` value: ``sha256=<hex>``.
    """
    if not header.startswith("sha256="):
        return False
    expected = header[len("sha256=") :]
    computed = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed, expected)
