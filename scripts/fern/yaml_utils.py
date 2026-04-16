"""Shared utilities for Fern YAML generation scripts."""

import os
import sys

__all__ = ["quote_yaml_title", "inject_into_docs_yml", "atomic_write"]

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


def atomic_write(path: str, content: str) -> None:
    """Write content to path atomically via tmp + rename (same-filesystem)."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


def inject_into_docs_yml(inject_file: str, placeholder: str, yaml_content: str) -> None:
    """Replace `placeholder` in `inject_file` with `yaml_content`.

    Exits the process with an error message on any I/O or missing-placeholder failure.
    """
    if not os.path.isfile(inject_file):
        print(f"ERROR: Inject file not found: {inject_file}", file=sys.stderr)
        sys.exit(1)
    try:
        with open(inject_file, encoding="utf-8") as f:
            content = f.read()
    except OSError as exc:
        print(f"ERROR: Cannot read {inject_file}: {exc}", file=sys.stderr)
        sys.exit(1)
    if placeholder not in content:
        print(
            f"ERROR: Placeholder '{placeholder}' not found in {inject_file}",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        atomic_write(inject_file, content.replace(placeholder, yaml_content))
    except OSError as exc:
        print(f"ERROR: Cannot write to {inject_file}: {exc}", file=sys.stderr)
        sys.exit(1)
