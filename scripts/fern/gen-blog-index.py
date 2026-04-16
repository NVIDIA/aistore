#!/usr/bin/env python3
"""Generate a blog index page (blog landing) from docs/_posts/.

Creates fern/pages/blog/index.md with all posts listed by date, grouped by year.
"""

import argparse
import glob
import os

from blog_utils import (
    display_date,
    load_post,
    strip_frontmatter,
    strip_leading_h1,
)


def extract_post_info(filepath: str) -> dict:
    loaded = load_post(filepath)
    if not loaded:
        return None
    (year, month, day, slug), meta, content = loaded

    title = meta.get("title") or slug.replace("-", " ").title()
    author = meta.get("author", "")

    # Extract description from first non-heading/non-image paragraph of body
    body = strip_leading_h1(strip_frontmatter(content).strip())
    description = first_paragraph(body, limit=200)

    return {
        "year": year,
        "date": f"{year}-{month}-{day}",
        "display_date": display_date(year, month, day),
        "slug": slug,
        "title": title,
        "author": author,
        "description": description,
    }


def first_paragraph(body: str, limit: int) -> str:
    for line in body.split("\n"):
        line = line.strip()
        if line and line[0] not in "#!<":
            return line[:limit] + ("..." if len(line) > limit else "")
    return ""


def main():
    parser = argparse.ArgumentParser(
        description="Generate a blog index page (fern/pages/blog/index.md) from docs/_posts/."
    )
    parser.add_argument(
        "posts_dir", nargs="?", default="docs/_posts", help="path to _posts directory"
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="fern/pages/blog/index.md",
        help="output index markdown file",
    )
    args = parser.parse_args()

    posts = []
    for f in glob.glob(os.path.join(args.posts_dir, "*.md")):
        info = extract_post_info(f)
        if info:
            posts.append(info)

    posts.sort(key=lambda p: p["date"], reverse=True)

    years = {}
    for p in posts:
        years.setdefault(p["year"], []).append(p)

    lines = ["# AIStore Blog\n"]
    for year in sorted(years.keys(), reverse=True):
        lines.append(f"## {year}\n")
        for p in years[year]:
            meta_parts = [p["display_date"]]
            if p["author"]:
                meta_parts.append(f"by {p['author']}")
            lines.append(
                f"**[{p['title']}](/aistore/blog/{p['slug']})** — {' · '.join(meta_parts)}"
            )
            if p["description"]:
                lines.append(f"> {p['description']}")
            lines.append("")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Generated blog index with {len(posts)} posts at {args.output}")


if __name__ == "__main__":
    main()
