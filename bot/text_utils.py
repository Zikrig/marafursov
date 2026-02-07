from __future__ import annotations

from typing import Iterable


def _utf8_len(s: str) -> int:
    return len(s.encode("utf-8"))


def split_for_telegram_html(text: str, *, max_bytes: int = 3800) -> list[str]:
    """
    Split a message into chunks safe for Telegram (4096 char limit; we use bytes margin).
    Assumes the message is HTML-safe (i.e. user-provided content already escaped),
    so splitting won't break entity parsing.
    """
    if _utf8_len(text) <= max_bytes:
        return [text]

    chunks: list[str] = []
    cur = ""

    # Prefer splitting by lines first
    for raw_line in text.splitlines(keepends=True):
        if not raw_line:
            continue

        # If this line alone is too big, split it by characters
        if _utf8_len(raw_line) > max_bytes:
            # flush current
            if cur:
                chunks.append(cur.rstrip("\n"))
                cur = ""

            buf = ""
            for ch in raw_line:
                if _utf8_len(buf + ch) > max_bytes:
                    chunks.append(buf.rstrip("\n"))
                    buf = ch
                else:
                    buf += ch
            if buf:
                chunks.append(buf.rstrip("\n"))
            continue

        if _utf8_len(cur + raw_line) > max_bytes:
            if cur:
                chunks.append(cur.rstrip("\n"))
            cur = raw_line
        else:
            cur += raw_line

    if cur.strip():
        chunks.append(cur.rstrip("\n"))
    return chunks


def join_lines_for_telegram_html(lines: Iterable[str], *, max_bytes: int = 3800) -> list[str]:
    """
    Join already-safe lines into chunks, splitting oversized lines if needed.
    """
    text = "".join(lines)
    return split_for_telegram_html(text, max_bytes=max_bytes)


