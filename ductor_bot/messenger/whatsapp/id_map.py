"""Bidirectional mapping between WhatsApp JID strings and integer chat_ids."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

from ductor_bot.infra.atomic_io import atomic_text_save

logger = logging.getLogger(__name__)


class WhatsAppIdMap:
    """Bidirectional JID ↔ int mapping with collision detection."""

    def __init__(self, store_path: Path) -> None:
        self._jid_to_int: dict[str, int] = {}
        self._int_to_jid: dict[int, str] = {}
        # Ensure store_path exists
        store_path.mkdir(parents=True, exist_ok=True)
        self._path = store_path / "jid_map.json"
        self._load()

    def jid_to_int(self, jid: str) -> int:
        """Get or create a deterministic int for a WhatsApp JID."""
        if jid in self._jid_to_int:
            return self._jid_to_int[jid]

        h = int.from_bytes(hashlib.sha256(jid.encode()).digest()[:8], "big")
        while h in self._int_to_jid and self._int_to_jid[h] != jid:
            h = int.from_bytes(
                hashlib.sha256(f"{jid}:{h}".encode()).digest()[:8],
                "big",
            )

        self._jid_to_int[jid] = h
        self._int_to_jid[h] = jid
        self._save()
        return h

    def int_to_jid(self, chat_id: int) -> str | None:
        """Resolve an int chat_id back to a WhatsApp JID."""
        return self._int_to_jid.get(chat_id)

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            for jid, int_id in data.items():
                self._jid_to_int[jid] = int_id
                self._int_to_jid[int_id] = jid
        except (json.JSONDecodeError, OSError):
            logger.warning("Failed to load jid_map.json, starting fresh")

    def _save(self) -> None:
        """Persist mappings to disk atomically."""
        atomic_text_save(
            self._path,
            json.dumps(self._jid_to_int, indent=2),
        )
