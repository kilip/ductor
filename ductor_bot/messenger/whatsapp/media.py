"""WhatsApp media helpers: MIME-type detection and file sending.

Maps file MIME types to the correct WhatsApp Cloud API media categories
(``image``, ``audio``, ``video``, ``document``) and uploads them via
the ``pywa`` client.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from ductor_bot.files.tags import guess_mime, path_from_file_tag

if TYPE_CHECKING:
    pass  # pywa types imported lazily

logger = logging.getLogger(__name__)

_FILE_TAG_RE = re.compile(r"<file:(.*?)>")

# WhatsApp Cloud API media type by MIME prefix
_MIME_TO_WA_TYPE: dict[str, str] = {
    "image/": "image",
    "audio/": "audio",
    "video/": "video",
}


def mime_to_wa_type(mime: str) -> str:
    """Map a MIME type to the WhatsApp media category.

    Returns ``"document"`` for anything not matched (the safe fallback).
    """
    for prefix, wa_type in _MIME_TO_WA_TYPE.items():
        if mime.startswith(prefix):
            return wa_type
    return "document"


def extract_file_tags(text: str) -> tuple[list[str], str]:
    """Extract ``<file:path>`` tags from *text*.

    Returns ``(paths, cleaned_text)`` where *paths* is a list of file
    path strings and *cleaned_text* has all ``<file:...>`` tags removed.
    """
    paths = _FILE_TAG_RE.findall(text)
    cleaned = _FILE_TAG_RE.sub("", text).strip()
    return paths, cleaned


def _read_file(file_path: Path) -> tuple[str, bytes, str]:
    """Read a file synchronously. Returns (mime, data, name)."""
    mime = guess_mime(file_path)
    data = file_path.read_bytes()
    return mime, data, file_path.name


async def send_file_to_whatsapp(
    wa: object,  # pywa.WhatsApp
    chat_id: str,
    file_path: Path,
    allowed_roots: list[Path] | None = None,
) -> bool:
    """Upload and send *file_path* as a WhatsApp media message.

    Returns *True* on success, *False* on any failure (logged).
    """
    if not file_path.exists():
        logger.warning("File not found: %s", file_path)
        return False

    if allowed_roots is not None and not any(
        file_path.resolve().is_relative_to(root.resolve()) for root in allowed_roots
    ):
        logger.warning("File outside allowed roots: %s", file_path)
        return False

    try:
        mime, data, name = await asyncio.to_thread(_read_file, file_path)
    except OSError:
        logger.warning("Cannot read file: %s", file_path, exc_info=True)
        return False

    wa_type = mime_to_wa_type(mime)
    logger.debug("Sending %s (%s, %d bytes) to %s", name, wa_type, len(data), chat_id)

    try:
        await asyncio.to_thread(
            _send_media_sync,
            wa,
            chat_id,
            wa_type,
            data,
            name,
            mime,
        )
    except Exception:
        logger.warning("Failed to send file %s to %s", name, chat_id, exc_info=True)
        return False

    return True


def _send_media_sync(
    wa: object,
    chat_id: str,
    wa_type: str,
    data: bytes,
    name: str,
    mime: str,
) -> None:
    """Synchronous media send — called from ``asyncio.to_thread``."""
    import io

    buf = io.BytesIO(data)
    buf.name = name  # pywa uses ``.name`` to infer content-type when mime not given

    if wa_type == "image":
        wa.send_image(chat_id, buf, mime_type=mime)  # type: ignore[union-attr]
    elif wa_type == "audio":
        wa.send_audio(chat_id, buf, mime_type=mime)  # type: ignore[union-attr]
    elif wa_type == "video":
        wa.send_video(chat_id, buf, mime_type=mime)  # type: ignore[union-attr]
    else:
        wa.send_document(chat_id, buf, mime_type=mime, filename=name)  # type: ignore[union-attr]
