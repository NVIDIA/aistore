#!/usr/bin/env python3
"""Generate Fern docs navigation YAML from docs/docs.md.

Parses docs/docs.md sections and links, maps local /docs/ links to Fern page
entries, and outputs navigation YAML for injection into docs.yml.

Usage:
    python3 scripts/fern/gen-docs-nav.py docs/docs.md --pages-dir fern/pages --inject fern/docs.yml
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from yaml_utils import quote_yaml_title


def parse_docs_md(filepath: str) -> list:
    """Parse docs.md into sections with links."""
    if not os.path.isfile(filepath):
        print(f"ERROR: Input file not found: {filepath}", file=sys.stderr)
        sys.exit(1)
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    # Strip Jekyll frontmatter
    content = re.sub(r"^---\n.*?\n---\n", "", content, flags=re.DOTALL)

    sections = []
    current_section = None

    for line in content.split("\n"):
        # Section heading
        heading = re.match(r"^## (.+)$", line)
        if heading:
            current_section = {"title": heading.group(1), "links": []}
            sections.append(current_section)
            continue

        if current_section is None:
            continue

        # Link line: - [Title](path) or   - [Title](path) (indented sub-items included)
        link = re.match(r"^\s*- \[(.+?)\]\((.+?)\)$", line)
        if link:
            title = link.group(1)
            path = link.group(2)
            current_section["links"].append({"title": title, "path": path})

    return sections


def path_to_fern_page(path: str, pages_dir: str) -> dict:
    """Convert a docs/docs.md link path to a Fern page entry.

    Returns None if the path is external or the file doesn't exist.
    """
    # Skip external links
    if path.startswith("http://") or path.startswith("https://"):
        return None

    # Skip anchor-only links
    if path.startswith("#"):
        return None

    # Strip anchor
    path_no_anchor = path.split("#")[0]

    # Normalize path: /docs/foo.md -> foo.md, /README.md -> readme.md
    if path_no_anchor.startswith("/docs/"):
        rel = path_no_anchor[6:]  # strip /docs/
    elif path_no_anchor.startswith("/"):
        rel = path_no_anchor[1:]  # strip leading /
    else:
        rel = path_no_anchor

    # Map README.md -> readme.md
    if rel == "README.md":
        rel = "readme.md"

    # Strip .md extension for slug
    slug = rel
    if slug.endswith(".md"):
        slug = slug[:-3]

    # Check if the page file exists
    page_path = os.path.join(pages_dir, rel)
    if not os.path.isfile(page_path):
        return None

    return {
        "path": f"./pages/{rel}",
        "slug": slug,
    }


def generate_yaml(sections: list, pages_dir: str, indent: str = "      ") -> str:
    """Generate Fern navigation YAML from parsed sections."""
    lines = []

    for section in sections:
        title = quote_yaml_title(section["title"])
        lines.append(f"{indent}- section: {title}")
        lines.append(f'{indent}  slug: ""')
        lines.append(f"{indent}  contents:")

        for link in section["links"]:
            entry = path_to_fern_page(link["path"], pages_dir)
            if entry is None:
                continue

            title = quote_yaml_title(link["title"])

            lines.append(f"{indent}    - page: {title}")
            lines.append(f"{indent}      path: {entry['path']}")
            lines.append(f"{indent}      slug: {entry['slug']}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Fern docs navigation YAML from docs/docs.md."
    )
    parser.add_argument(
        "docs_md", nargs="?", default="docs/docs.md", help="path to docs.md"
    )
    parser.add_argument(
        "--pages-dir", default="fern/pages", help="path to fern pages directory"
    )
    parser.add_argument(
        "--inject", dest="inject_file", help="inject navigation into this docs.yml"
    )
    args = parser.parse_args()

    docs_md = args.docs_md
    pages_dir = args.pages_dir
    inject_file = args.inject_file

    sections = parse_docs_md(docs_md)

    # Count pages
    total_pages = 0
    for s in sections:
        for link in s["links"]:
            entry = path_to_fern_page(link["path"], pages_dir)
            if entry:
                total_pages += 1

    yaml_content = generate_yaml(sections, pages_dir)

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
        placeholder = "      # AUTO_GENERATED_DOCS_ENTRIES"
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
        print(
            f"Injected {len(sections)} sections, {total_pages} pages into {inject_file}"
        )
    else:
        print(yaml_content)


if __name__ == "__main__":
    main()
