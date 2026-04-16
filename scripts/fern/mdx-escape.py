#!/usr/bin/env python3
"""Escape MDX-unsafe patterns in .md files for Fern.

Fern parses .md files with MDX, so curly braces and bare angle-bracket
placeholders outside code fences/backticks must be escaped.
"""

import argparse
import glob
import re

SAFE_TAGS = {
    "br",
    "hr",
    "img",
    "a",
    "p",
    "div",
    "span",
    "table",
    "tr",
    "td",
    "th",
    "thead",
    "tbody",
    "ul",
    "ol",
    "li",
    "em",
    "strong",
    "code",
    "pre",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "details",
    "summary",
    "sup",
    "sub",
    "b",
    "i",
    "u",
    "s",
    "del",
    "ins",
    "mark",
    "small",
    "big",
    "abbr",
    "cite",
    "q",
    "blockquote",
    "dl",
    "dt",
    "dd",
    "figure",
    "figcaption",
    "section",
    "article",
    "header",
    "footer",
    "main",
    "nav",
    "iframe",
    "video",
    "audio",
    "source",
    "picture",
    "svg",
    "path",
    "rect",
    "circle",
    "line",
    "g",
}


def _escape_brace(ch, new_line):
    """Escape { or } unless preceded by an odd number of backslashes."""
    escaped = "\\{" if ch == "{" else "\\}"
    # Count trailing backslashes — odd means the brace is already escaped
    trailing = len(new_line) - len(new_line.rstrip("\\"))
    if trailing % 2 == 1:
        return ch, False
    return escaped, True


def _escape_angle(line, idx):
    """Escape < when it's not a safe HTML tag or comment."""
    rest = line[idx:]
    if rest.startswith("<!--"):
        return None  # HTML comment, safe

    m = re.match(r"</?([A-Za-z][A-Za-z0-9_:-]*)[\s/>]?", rest)
    if m and m.group(1) in SAFE_TAGS:
        return None  # known safe tag

    if m and m.group(1) not in SAFE_TAGS:
        return "&lt;"

    # < followed by non-alpha (like <-, <=, <1)
    if idx + 1 < len(line) and line[idx + 1] not in ("!", "/", "&"):
        if re.match(r"<[^a-zA-Z/!&]", rest):
            return "&lt;"
    return None


def _escape_line(line):
    """Escape MDX-unsafe characters in a single line (outside backticks)."""
    new_line = ""
    in_backtick = False
    changed = False
    idx = 0

    while idx < len(line):
        ch = line[idx]
        if ch == "`":
            in_backtick = not in_backtick
            new_line += ch
        elif in_backtick:
            new_line += ch
        elif ch in "{}":
            replacement, did_change = _escape_brace(ch, new_line)
            new_line += replacement
            changed = changed or did_change
        elif ch == "<":
            replacement = _escape_angle(line, idx)
            if replacement:
                new_line += replacement
                changed = True
            else:
                new_line += ch
        else:
            new_line += ch
        idx += 1

    return new_line, changed


def _apply_outside_backticks(line, transform_fn):
    """Apply a transform function only to text outside inline backticks."""
    parts = line.split("`")
    changed = False
    for i in range(0, len(parts), 2):  # even indices are outside backticks
        original = parts[i]
        parts[i] = transform_fn(parts[i])
        if parts[i] != original:
            changed = True
    return "`".join(parts), changed


def _escape_operators(line):
    """Escape comparison operators and arrows in prose (backtick-aware)."""

    def _transform(text):
        text = text.replace("<=>", "&lt;=&gt;")
        text = text.replace(" <- ", " &lt;- ")
        text = text.replace(" <= ", " &lt;= ")
        text = text.replace(" >= ", " &gt;= ")
        return text

    return _apply_outside_backticks(line, _transform)


def _fix_void_elements(line):
    """Self-close void HTML elements (backtick-aware)."""

    def _transform(text):
        text = text.replace("<br>", "<br />")
        text = text.replace("<hr>", "<hr />")
        text = re.sub(r"<img ([^>]*[^/])>", r"<img \1 />", text)
        return text

    return _apply_outside_backticks(line, _transform)


def _fix_double_backtick_fence(line):
    """Fix double-backtick code fences to triple."""
    m = re.match(r"^(\s*)``([^`]|$)", line)
    if m:
        return m.group(1) + "```" + line[m.end(1) + 2 :], True
    return line, False


def escape_file(filepath):
    """Escape MDX-unsafe patterns in a file, skipping code fences."""
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    lines = content.split("\n")
    result = []
    in_code = False
    changed = False

    for line in lines:
        # Check for code fence toggle (but also fix double-backtick fences)
        if not in_code:
            line, fence_fixed = _fix_double_backtick_fence(line)
            changed = changed or fence_fixed

        if line.strip().startswith("```"):
            in_code = not in_code
            result.append(line)
            continue
        if in_code:
            result.append(line)
            continue

        # All escaping below is code-fence aware (only runs outside fences)
        new_line, line_changed = _escape_line(line)
        changed = changed or line_changed

        new_line, op_changed = _escape_operators(new_line)
        changed = changed or op_changed

        new_line, void_changed = _fix_void_elements(new_line)
        changed = changed or void_changed

        result.append(new_line)

    if changed:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(result))
    return changed


def main():
    parser = argparse.ArgumentParser(
        description="Escape MDX-unsafe patterns in Fern .md pages."
    )
    parser.add_argument(
        "pages_dir", nargs="?", default="fern/pages", help="fern pages directory"
    )
    args = parser.parse_args()

    files = glob.glob(f"{args.pages_dir}/**/*.md", recursive=True)
    fixed = sum(1 for f in files if escape_file(f))
    print(f"MDX-escaped {fixed} of {len(files)} files")


if __name__ == "__main__":
    main()
