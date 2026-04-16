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


def _build_meta_html(date_str: str, author: str, categories: list) -> str:
    """Build the HTML metadata block to prepend to blog posts."""
    lines = ['<div class="blog-post-meta">']
    lines.append(f'  <span class="blog-meta-date">{html.escape(date_str)}</span>')
    if author:
        lines.append('  <span class="blog-meta-separator">&middot;</span>')
        lines.append(f'  <span class="blog-meta-author">{html.escape(author)}</span>')
    lines.append("</div>")

    if categories:
        lines.append('<div class="blog-post-tags">')
        for cat in categories:
            lines.append(f'  <span class="blog-tag">{html.escape(cat)}</span>')
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
