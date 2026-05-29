"""Authorization helpers for the WhatsApp transport.

WhatsApp identifies senders by phone number JIDs.  In 1-1 chats the JID
looks like ``+15551234567@s.whatsapp.net``; in group chats the group ID
looks like ``123456789@g.us`` and the sender is carried separately.

Allowed users are stored as normalized E.164 strings, e.g. ``+15551234567``.
"""

from __future__ import annotations

import re

# Strip @s.whatsapp.net, @g.us, or any other suffix
_JID_SUFFIX_RE = re.compile(r"@[^@]+$")


def normalize_phone(jid_or_phone: str) -> str:
    """Extract the bare phone number / JID local part.

    Examples:
      ``+15551234567@s.whatsapp.net`` → ``+15551234567``
      ``+15551234567``                → ``+15551234567``
      ``628123456789@s.whatsapp.net`` → ``628123456789``
    """
    normalized = _JID_SUFFIX_RE.sub("", jid_or_phone).strip()
    # Ensure leading '+' when it looks like a pure number without it
    # (WhatsApp sometimes omits the '+' for non-international numbers)
    if normalized.lstrip("0123456789") == "" and not normalized.startswith("+"):
        normalized = "+" + normalized
    return normalized


def is_allowed(sender_jid: str, allowed_users: list[str]) -> bool:
    """Return *True* when *sender_jid* is on the *allowed_users* list.

    An empty *allowed_users* list means *allow all* (open bot).
    """
    if not allowed_users:
        return True
    normalized = normalize_phone(sender_jid)
    return normalized in allowed_users


def is_group_jid(jid: str) -> bool:
    """Return *True* when *jid* refers to a WhatsApp group chat."""
    return "@g.us" in jid
