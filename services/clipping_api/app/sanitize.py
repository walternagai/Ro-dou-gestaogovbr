"""Sanitização de texto externo vindo de diários oficiais."""

import html
import re
from html.parser import HTMLParser

_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WHITESPACE_RE = re.compile(r"\s+")
_BLOCK_TAGS = {"p", "div", "br", "li", "tr", "td", "th", "h1", "h2", "h3", "h4"}
_IGNORED_TAGS = {"script", "style"}


class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        lowered = tag.lower()
        if lowered in _IGNORED_TAGS:
            self._ignored_depth += 1
            return
        if self._ignored_depth == 0 and lowered in _BLOCK_TAGS:
            self._parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in _IGNORED_TAGS:
            if self._ignored_depth > 0:
                self._ignored_depth -= 1
            return
        if self._ignored_depth == 0 and lowered in _BLOCK_TAGS:
            self._parts.append(" ")

    def handle_data(self, data: str) -> None:
        if self._ignored_depth == 0:
            self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def sanitize_text(value: str | None, *, max_length: int) -> str | None:
    """Remove HTML e caracteres de controle, normaliza espaços e trunca."""
    if value is None:
        return None

    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    text = html.unescape(parser.get_text())
    text = _CONTROL_CHARS_RE.sub("", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()

    if len(text) > max_length:
        return text[: max_length - 1].rstrip() + "…"
    return text


def sanitize_item(item: dict) -> dict:
    """Retorna cópia do item com campos textuais sanitizados e limitados."""
    cleaned = dict(item)
    cleaned["title"] = sanitize_text(cleaned.get("title"), max_length=500)
    cleaned["abstract"] = sanitize_text(cleaned.get("abstract"), max_length=1000)
    cleaned["section"] = sanitize_text(cleaned.get("section"), max_length=300)
    cleaned["org"] = sanitize_text(cleaned.get("org"), max_length=300)
    return cleaned
