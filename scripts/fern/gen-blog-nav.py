#!/usr/bin/env python3
"""Generate Fern blog navigation YAML from docs/_posts/.

Scans docs/_posts/*.md, sorts by date descending, extracts titles from
frontmatter, and outputs YAML entries for docs.yml.

Usage:
    python3 scripts/fern/gen-blog-nav.py docs/_posts > /tmp/blog-nav.yml
    # or inject into docs.yml:
    python3 scripts/fern/gen-blog-nav.py docs/_posts --inject fern/docs.yml
"""

import argparse
import glob
import os

from blog_utils import load_post
from yaml_utils import inject_into_docs_yml, quote_yaml_title


def extract_post_info(filepath: str) -> dict:
    """Extract date, slug, and title from a blog post."""
    loaded = load_post(filepath)
    if not loaded:
        return None
    (year, month, day, slug), meta, _ = loaded
    return {
        "date": f"{year}-{month}-{day}",
        "slug": slug,
        "title": meta.get("title") or slug.replace("-", " ").title(),
        "path": filepath,
    }


def generate_yaml(posts: list, indent: str = "  ") -> str:
    """Generate YAML for blog as hidden pages (no tab, accessible via navbar link)."""
    lines = []
    # Blog index — hidden page at /blog
    lines.append(f"{indent}- page: All Posts")
    lines.append(f"{indent}  path: ./pages/blog/index.md")
    lines.append(f"{indent}  slug: blog")
    lines.append(f"{indent}  hidden: true")
    # All blog posts in a hidden section at /blog/<slug>
    lines.append(f"{indent}- section: Blog Posts")
    lines.append(f'{indent}  slug: "blog"')
    lines.append(f"{indent}  hidden: true")
    lines.append(f"{indent}  contents:")
    for post in posts:
        title = quote_yaml_title(post["title"])
        lines.append(f"{indent}    - page: {title}")
        lines.append(f"{indent}      path: ./pages/blog/{post['slug']}.md")
        lines.append(f"{indent}      slug: {post['slug']}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Fern blog navigation YAML from docs/_posts/."
    )
    parser.add_argument(
        "posts_dir", nargs="?", default="docs/_posts", help="path to _posts directory"
    )
    parser.add_argument(
        "--inject", dest="inject_file", help="inject blog entries into this docs.yml"
    )
    args = parser.parse_args()

    posts_dir = args.posts_dir
    inject_file = args.inject_file

    # Collect all posts
    posts = []
    for f in glob.glob(os.path.join(posts_dir, "*.md")):
        info = extract_post_info(f)
        if info:
            posts.append(info)

    # Sort by date descending
    posts.sort(key=lambda p: p["date"], reverse=True)

    yaml_content = generate_yaml(posts)

    if inject_file:
        inject_into_docs_yml(
            inject_file, "  # AUTO_GENERATED_BLOG_ENTRIES", yaml_content
        )
        print(f"Injected {len(posts)} blog entries into {inject_file}")
    else:
        print(yaml_content)


if __name__ == "__main__":
    main()
