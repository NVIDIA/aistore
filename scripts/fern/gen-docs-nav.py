#!/usr/bin/env python3
"""Generate Fern docs navigation YAML from docs/docs.md.

Parses docs/docs.md sections and links, maps local /docs/ links to Fern page
entries, and outputs navigation YAML for injection into docs.yml.

Usage:
    python3 scripts/fern/gen-docs-nav.py docs/docs.md --pages-dir fern/pages --inject fern/docs.yml
"""

import argparse
import glob
import os
import re
import sys

from yaml_utils import inject_into_docs_yml, quote_yaml_title


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
            # Strip backticks — inline code doesn't render in Fern navigation
            title = title.replace("`", "")
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


def _discover_all_pages(pages_dir: str) -> list:
    """Auto-discover all .md pages in fern/pages/ recursively."""
    pages = []
    for f in sorted(glob.glob(os.path.join(pages_dir, "**", "*.md"), recursive=True)):
        rel = os.path.relpath(f, pages_dir)
        # Skip special files and blog (handled separately)
        if rel in ("readme.md", "docs.md") or rel.startswith("blog/"):
            continue
        name = os.path.splitext(os.path.basename(f))[0]
        # full_slug includes directory prefix (e.g., cli/archive) for dedup matching
        full_slug = os.path.splitext(rel)[0]
        # page_slug is filename-only (used in YAML inside a section with dir as slug)
        page_slug = name
        # Prefix numeric-only slugs (e.g., release notes "3.30" -> "v3.30")
        if page_slug and page_slug[0].isdigit():
            page_slug = f"v{page_slug}"
        title = name.replace("_", " ").replace("-", " ").title()
        # Prefix numeric titles too (YAML parses "3.30" as float)
        if title and title[0].isdigit():
            title = f"v{title}"
        pages.append(
            {
                "title": title,
                "path": f"./pages/{rel}",
                "slug": page_slug,
                "full_slug": full_slug,
            }
        )
    return pages


def _emit_hidden_pages(pages_dir: str, all_seen_slugs: set, indent: str) -> list:
    """Emit hidden YAML entries for pages not in the main navigation."""
    lines = []
    all_pages = _discover_all_pages(pages_dir)
    unlisted = [p for p in all_pages if p["full_slug"] not in all_seen_slugs]

    if not unlisted:
        return lines

    groups = {}
    for page in unlisted:
        rel = os.path.relpath(page["path"], "./pages")
        directory = os.path.dirname(rel)
        groups.setdefault(directory or "", []).append(page)

    for group_dir, pages in sorted(groups.items()):
        if group_dir:
            section_title = group_dir.replace("/", " - ").replace("_", " ").title()
            lines.append(f"{indent}- section: {quote_yaml_title(section_title)}")
            lines.append(f'{indent}  slug: "{group_dir}"')
            lines.append(f"{indent}  hidden: true")
            lines.append(f"{indent}  contents:")
            for page in pages:
                page_title = quote_yaml_title(page["title"])
                lines.append(f"{indent}    - page: {page_title}")
                lines.append(f"{indent}      path: {page['path']}")
                lines.append(f"{indent}      slug: {page['slug']}")
        else:
            for page in pages:
                page_title = quote_yaml_title(page["title"])
                lines.append(f"{indent}- page: {page_title}")
                lines.append(f"{indent}  path: {page['path']}")
                lines.append(f"{indent}  slug: {page['slug']}")
                lines.append(f"{indent}  hidden: true")

    return lines


def generate_yaml(sections: list, pages_dir: str, indent: str = "  ") -> str:
    """Generate Fern navigation YAML from parsed sections."""
    lines = []
    all_seen_slugs = {"readme"}

    for section in sections:
        title = quote_yaml_title(section["title"])
        lines.append(f"{indent}- section: {title}")
        lines.append(f'{indent}  slug: ""')
        lines.append(f"{indent}  contents:")

        for link in section["links"]:
            link_title = quote_yaml_title(link["title"])
            path = link["path"]

            # External links → emit as Fern link (opens in new tab)
            if path.startswith("http://") or path.startswith("https://"):
                lines.append(f"{indent}    - link: {link_title}")
                lines.append(f'{indent}      href: "{path}"')
                continue

            entry = path_to_fern_page(path, pages_dir)
            if entry is None:
                continue

            if entry["slug"] == "readme":
                continue

            all_seen_slugs.add(entry["slug"])
            lines.append(f"{indent}    - page: {link_title}")
            lines.append(f"{indent}      path: {entry['path']}")
            lines.append(f"{indent}      slug: {entry['slug']}")

    lines.extend(_emit_hidden_pages(pages_dir, all_seen_slugs, indent))
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
        inject_into_docs_yml(
            inject_file, "  # AUTO_GENERATED_DOCS_ENTRIES", yaml_content
        )
        print(
            f"Injected {len(sections)} sections, {total_pages} pages into {inject_file}"
        )
    else:
        print(yaml_content)


if __name__ == "__main__":
    main()
