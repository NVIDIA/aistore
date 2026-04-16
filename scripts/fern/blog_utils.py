"""Shared helpers for Fern blog scripts (filename parsing, frontmatter, dates)."""

import os
import re

import yaml

__all__ = [
    "MONTH_ABBR",
    "parse_post_filename",
    "display_date",
    "parse_frontmatter",
    "strip_frontmatter",
    "load_post",
    "strip_leading_h1",
]

MONTH_ABBR = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}

_POST_FILENAME_RE = re.compile(r"(\d{4})-(\d{2})-([\dx]{2})-(.+)\.md$")
_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---", re.DOTALL)


def parse_post_filename(filepath: str):
    """Parse a Jekyll blog filename (YYYY-MM-DD-slug.md).

    Returns (year, month, day, slug) or None if the name doesn't match or slug is README.
    """
    m = _POST_FILENAME_RE.match(os.path.basename(filepath))
    if not m or m.group(4) == "README":
        return None
    return m.groups()


def display_date(year: str, month: str, day: str) -> str:
    """Format as 'Mon DD, YYYY' (e.g. 'Apr 13, 2026')."""
    return f"{MONTH_ABBR.get(month, month)} {day}, {year}"


def parse_frontmatter(content: str) -> dict:
    """Extract title, author, and categories from Jekyll YAML frontmatter.

    Accepts scalar or YAML-list forms for `author`/`authors` and `categories`.
    Space-separated inline categories (Jekyll convention) are split.
    """
    m = _FRONTMATTER_RE.search(content)
    if not m:
        return {}
    try:
        fm = yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError:
        return {}
    if not isinstance(fm, dict):
        return {}

    result = {}
    if fm.get("title") is not None:
        result["title"] = str(fm["title"])

    raw_author = fm.get("author") or fm.get("authors")
    if isinstance(raw_author, list):
        result["author"] = ", ".join(str(a) for a in raw_author)
    elif raw_author is not None:
        result["author"] = str(raw_author)

    raw_cats = fm.get("categories")
    if isinstance(raw_cats, list):
        result["categories"] = [str(c) for c in raw_cats]
    elif isinstance(raw_cats, str) and raw_cats.strip():
        result["categories"] = raw_cats.split()

    return result


def strip_frontmatter(content: str) -> str:
    """Remove Jekyll YAML frontmatter block from the start of content."""
    if not content.startswith("---"):
        return content
    m = re.match(r"^---\s*\n.*?\n---\s*\n?", content, re.DOTALL)
    return content[m.end() :] if m else content


def load_post(filepath: str):
    """Load and pre-parse a blog post file.

    Returns ((year, month, day, slug), frontmatter_meta, content) or None if the
    filename is not a valid post name or the file can't be read.
    """
    parsed = parse_post_filename(filepath)
    if not parsed:
        return None
    try:
        with open(filepath, encoding="utf-8") as f:
            content = f.read()
    except OSError:
        return None
    return parsed, parse_frontmatter(content), content


def strip_leading_h1(body: str) -> str:
    """Remove a leading '# Heading' line (and following blank lines) if present."""
    return re.sub(r"^#\s+.*\n*", "", body)
