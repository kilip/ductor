"""Convert Markdown to WhatsApp-compatible formatting.

WhatsApp uses a limited subset of formatting markers:
  *bold*       ← **bold** or __bold__
  _italic_     ← *italic* or _italic_
  ~strike~     ← ~~strike~~
  ```code```   ← ```code blocks```  (same)
  `inline`     ← `inline`           (same)
  *Heading*    ← # Heading (converted to bold)
  > quote      ← > quote            (native)

Tables, HTML tags, and unsupported block elements are stripped to plain text.

Max practical message length on WhatsApp Cloud API: 4096 characters.
"""

from __future__ import annotations

import re

_WA_MAX_LEN = 4096

# Regex patterns (applied in order)
_HEADING_RE = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
_BOLD_DOUBLE_STAR_RE = re.compile(r"\*\*(.+?)\*\*")
_BOLD_DOUBLE_UNDER_RE = re.compile(r"__(.+?)__")
_ITALIC_SINGLE_STAR_RE = re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)")
_ITALIC_SINGLE_UNDER_RE = re.compile(r"(?<!\w)_(.+?)_(?!\w)")
_STRIKE_RE = re.compile(r"~~(.+?)~~")
_INLINE_CODE_RE = re.compile(r"`([^`]+)`")
_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_TABLE_ROW_RE = re.compile(r"^\|.*\|$", re.MULTILINE)
_TABLE_SEP_RE = re.compile(r"^\s*\|[\s\-\|:]+\|\s*$", re.MULTILINE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HORIZONTAL_RULE_RE = re.compile(r"^---+$", re.MULTILINE)
_BUTTON_MARKER_RE = re.compile(r"\[button:[^\]]+\]")


def md_to_whatsapp(text: str) -> str:
    """Convert Markdown *text* to WhatsApp-formatted string.

    Conversion rules:
      - Code blocks (``` ... ```) are preserved as-is (WhatsApp renders them).
      - Headings become bold lines.
      - Bold/italic/strikethrough use WhatsApp syntax.
      - Tables and HTML tags are stripped to plain text.
      - Links: only the label is kept, the URL is dropped.
    """
    if not text:
        return text

    # 1. Remove [button:...] markers (ductor internal)
    text = _BUTTON_MARKER_RE.sub("", text)

    # 2. Process code blocks first (protect their content)
    parts: list[str] = []
    last = 0
    for m in re.finditer(r"```[\w]*\n?([\s\S]*?)```", text):
        before = text[last : m.start()]
        parts.append(_convert_inline(before))
        # Code block: WhatsApp renders ``` ... ``` natively
        inner = m.group(1).rstrip()
        parts.append(f"```\n{inner}\n```")
        last = m.end()
    parts.append(_convert_inline(text[last:]))

    return "".join(parts).strip()


def _convert_inline(text: str) -> str:
    """Apply all inline conversions (outside code blocks)."""
    # Strip HTML tags
    text = _HTML_TAG_RE.sub("", text)

    # Strip table separators first, then table rows (just keep content)
    text = _TABLE_SEP_RE.sub("", text)
    text = _TABLE_ROW_RE.sub(lambda m: _strip_table_row(m.group(0)), text)

    # Headings → *Heading*
    text = _HEADING_RE.sub(lambda m: f"*{m.group(1).strip()}*", text)

    # Horizontal rule → em-dashes
    text = _HORIZONTAL_RULE_RE.sub("——————", text)

    # Links → label only
    text = _LINK_RE.sub(r"\1", text)

    # Bold: **text** and __text__ → *text*
    text = _BOLD_DOUBLE_STAR_RE.sub(r"*\1*", text)
    text = _BOLD_DOUBLE_UNDER_RE.sub(r"*\1*", text)

    # Italic: *text* → _text_ (single star only)
    text = _ITALIC_SINGLE_STAR_RE.sub(r"_\1_", text)
    # _text_ is already WhatsApp italic — leave as-is (no transform needed)

    # Strikethrough: ~~text~~ → ~text~
    text = _STRIKE_RE.sub(r"~\1~", text)

    # Inline code: `text` → `text` (WhatsApp renders it natively — no change)

    return text


def _strip_table_row(row: str) -> str:
    """Convert a Markdown table row to a plain text line."""
    cells = [c.strip() for c in row.strip().strip("|").split("|")]
    return "  ".join(c for c in cells if c)


def split_message(text: str, max_len: int = _WA_MAX_LEN) -> list[str]:
    """Split *text* into chunks of at most *max_len* characters.

    Splits prefer paragraph boundaries (double newline), then newlines,
    then hard-cuts at *max_len* as a last resort.
    """
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break

        # Try to split at paragraph boundary
        cut = text.rfind("\n\n", 0, max_len)
        if cut == -1:
            cut = text.rfind("\n", 0, max_len)
        if cut == -1:
            cut = max_len

        chunks.append(text[:cut].rstrip())
        text = text[cut:].lstrip()

    return [c for c in chunks if c]
