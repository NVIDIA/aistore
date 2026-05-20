#!/usr/bin/env python3
"""Strip Jekyll frontmatter from blog posts and inject metadata HTML.

Reads frontmatter (author, date, categories) from source posts in docs/_posts/,
strips the frontmatter from the Fern copies, and prepends a metadata block.
"""

import argparse
import glob
import html
import os
import sys

from blog_utils import display_date, load_post, strip_frontmatter
from yaml_utils import atomic_write

# Inline styles instead of a CSS class hook: Fern's `global-theme: nvidia`
# replaces the user-supplied `css:` entry in docs.yml, so an external
# main.css with .blog-* rules never reaches the bundle. Inline `style="..."`
# survives that override. The var(--grayscale-a*) tokens adapt to light/dark.
_META_WRAP = (
    "display: flex; align-items: center; gap: 0.5rem;"
    " font-size: 0.9rem; color: var(--grayscale-a11);"
    " margin-bottom: 0.25rem; flex-wrap: wrap;"
)
_META_DATE = "font-weight: 500;"
_META_SEP = "color: var(--grayscale-a9);"
_META_AUTHOR = "font-style: italic;"
_TAGS_WRAP = "display: flex; flex-wrap: wrap; gap: 0.375rem; margin-bottom: 1.5rem;"
_TAG = (
    "display: inline-block; padding: 0.125rem 0.5rem;"
    " font-size: 0.75rem; font-weight: 500; border-radius: 999px;"
    " background-color: var(--grayscale-a3); color: var(--grayscale-a11);"
)


def _build_meta_html(date_str: str, author: str, categories: list) -> str:
    """Build the HTML metadata block to prepend to blog posts."""
    lines = [f'<div style="{_META_WRAP}">']
    lines.append(f'  <span style="{_META_DATE}">{html.escape(date_str)}</span>')
    if author:
        lines.append(f'  <span style="{_META_SEP}">&middot;</span>')
        lines.append(f'  <span style="{_META_AUTHOR}">{html.escape(author)}</span>')
    lines.append("</div>")

    if categories:
        lines.append(f'<div style="{_TAGS_WRAP}">')
        for cat in categories:
            lines.append(f'  <span style="{_TAG}">{html.escape(cat)}</span>')
        lines.append("</div>")

    lines.append("")  # blank line before post content
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Strip blog frontmatter and inject author/date/tags HTML."
    )
    parser.add_argument(
        "posts_dir", nargs="?", default="docs/_posts", help="path to _posts directory"
    )
    parser.add_argument(
        "blog_dir",
        nargs="?",
        default="fern/pages/blog",
        help="path to fern blog pages directory",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.posts_dir):
        sys.exit(f"ERROR: posts_dir not found: {args.posts_dir}")
    if not os.path.isdir(args.blog_dir):
        sys.exit(f"ERROR: blog_dir not found: {args.blog_dir}")

    # slug -> (parsed_filename, meta, content) for every source _posts file
    source_posts = {}
    for f in glob.glob(os.path.join(args.posts_dir, "*.md")):
        loaded = load_post(f)
        if loaded:
            (_, _, _, slug), _, _ = loaded
            source_posts[slug] = loaded

    count = 0
    for fern_file in sorted(glob.glob(os.path.join(args.blog_dir, "*.md"))):
        bn = os.path.basename(fern_file)
        if bn == "index.md":
            continue

        slug = os.path.splitext(bn)[0]
        with open(fern_file, encoding="utf-8") as fh:
            fern_content = fh.read()

        loaded = source_posts.get(slug)
        if not loaded:
            # No source match — still strip frontmatter
            atomic_write(fern_file, strip_frontmatter(fern_content))
            continue

        (year, month, day, _), meta, _ = loaded
        meta_html = _build_meta_html(
            date_str=display_date(year, month, day),
            author=meta.get("author", ""),
            categories=meta.get("categories", []),
        )
        atomic_write(fern_file, meta_html + strip_frontmatter(fern_content))
        count += 1

    print(f"Injected blog metadata into {count} posts")


if __name__ == "__main__":
    main()
