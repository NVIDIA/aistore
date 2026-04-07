"""Shared utilities for Fern YAML generation scripts."""

__all__ = ["quote_yaml_title"]

# Characters that require YAML double-quoting
_YAML_SPECIAL = set(':&*?|>{}[]!%@`#,"\\')


def quote_yaml_title(title: str) -> str:
    """Return a YAML-safe scalar for a title string.

    If the title contains characters that are special in YAML, escapes
    backslashes and double quotes, then wraps in double quotes.
    Otherwise returns the title unchanged.
    """
    if any(c in _YAML_SPECIAL for c in title):
        title = title.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{title}"'
    return title
