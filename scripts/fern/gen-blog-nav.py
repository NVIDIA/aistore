#!/usr/bin/env python3
"""Generate Fern blog navigation YAML from docs/_posts/.

Scans docs/_posts/*.md, sorts by date descending, extracts titles from
frontmatter, and outputs YAML entries for docs.yml.

Usage:
    python3 scripts/fern/gen-blog-nav.py docs/_posts > /tmp/blog-nav.yml
    # or inject into docs.yml:
    python3 scripts/fern/gen-blog-nav.py docs/_posts --inject fern/docs.yml
"""

import glob
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from yaml_utils import quote_yaml_title


def extract_post_info(filepath: str) -> dict:
    """Extract date, slug, and title from a blog post."""
    basename = os.path.basename(filepath)

    # Extract date and slug from filename: YYYY-MM-DD-slug.md
    date_match = re.match(r"(\d{4}-\d{2}-\d{2})-(.+)\.md$", basename)
    if not date_match:
        # Handle non-standard filenames (e.g., draft with xx in date)
        date_match = re.match(r"(\d{4}-\d{2}-[\dx]{2})-(.+)\.md$", basename)
        if not date_match:
            return None

    date = date_match.group(1)
    slug = date_match.group(2)

    if slug == "README":
        return None

    # Extract title from frontmatter
    title = slug.replace("-", " ").title()
    with open(filepath, encoding="utf-8") as f:
        content = f.read()
    fm_match = re.search(r"^---\s*\n(.*?)\n---", content, re.DOTALL)
    if fm_match:
        title_match = re.search(
            r'^title:\s*["\']?(.*?)["\']?\s*$', fm_match.group(1), re.MULTILINE
        )
        if title_match:
            title = title_match.group(1)

    return {"date": date, "slug": slug, "title": title, "path": filepath}


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
        if not os.path.isfile(inject_file):
            print(f"ERROR: Inject file not found: {inject_file}", file=sys.stderr)
            sys.exit(1)
        try:
            with open(inject_file, encoding="utf-8") as f:
                content = f.read()
        except IOError as exc:
            print(f"ERROR: Cannot read {inject_file}: {exc}", file=sys.stderr)
            sys.exit(1)
        placeholder = "  # AUTO_GENERATED_BLOG_ENTRIES"
        if placeholder not in content:
            print(
                f"ERROR: Placeholder '{placeholder}' not found in {inject_file}",
                file=sys.stderr,
            )
            sys.exit(1)
        content = content.replace(placeholder, yaml_content)
        try:
            with open(inject_file, "w", encoding="utf-8") as f:
                f.write(content)
        except IOError as exc:
            print(f"ERROR: Cannot write to {inject_file}: {exc}", file=sys.stderr)
            sys.exit(1)
        print(f"Injected {len(posts)} blog entries into {inject_file}")
    else:
        print(yaml_content)


if __name__ == "__main__":
    main()
