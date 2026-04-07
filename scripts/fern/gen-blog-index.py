#!/usr/bin/env python3
"""Generate a blog index page (blog landing) from docs/_posts/.

Creates fern/pages/blog/index.md with all posts listed by date, grouped by year.
"""

import glob
import os
import re
import sys


def extract_post_info(filepath: str) -> dict:
    basename = os.path.basename(filepath)
    date_match = re.match(r"(\d{4})-(\d{2})-(\d{2})-(.+)\.md$", basename)
    if not date_match:
        date_match = re.match(r"(\d{4})-(\d{2})-([\dx]{2})-(.+)\.md$", basename)
        if not date_match:
            return None

    year = date_match.group(1)
    month = date_match.group(2)
    day = date_match.group(3)
    slug = date_match.group(4)

    if slug == "README":
        return None

    date_str = f"{year}-{month}-{day}"

    # Friendly month names
    months = {
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
    display_date = f"{months.get(month, month)} {day}, {year}"

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

    # Extract description/first paragraph
    description = ""
    body = content
    if fm_match:
        body = content[fm_match.end() :].strip()
    # Skip H1 if present
    body = re.sub(r"^#\s+.*\n*", "", body)
    # Get first paragraph
    for line in body.split("\n"):
        line = line.strip()
        if (
            line
            and not line.startswith("#")
            and not line.startswith("!")
            and not line.startswith("<")
        ):
            description = line[:200]
            if len(line) > 200:
                description += "..."
            break

    return {
        "year": year,
        "date": date_str,
        "display_date": display_date,
        "slug": slug,
        "title": title,
        "description": description,
    }


def main():
    posts_dir = sys.argv[1] if len(sys.argv) > 1 else "docs/_posts"
    output = sys.argv[2] if len(sys.argv) > 2 else "fern/pages/blog/index.md"

    posts = []
    for f in glob.glob(os.path.join(posts_dir, "*.md")):
        info = extract_post_info(f)
        if info:
            posts.append(info)

    posts.sort(key=lambda p: p["date"], reverse=True)

    # Group by year
    years = {}
    for p in posts:
        years.setdefault(p["year"], []).append(p)

    lines = ["# AIStore Blog\n"]

    for year in sorted(years.keys(), reverse=True):
        lines.append(f"## {year}\n")
        for p in years[year]:
            lines.append(f"**[{p['title']}](/blog/{p['slug']})** — {p['display_date']}")
            if p["description"]:
                lines.append(f"> {p['description']}")
            lines.append("")

    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Generated blog index with {len(posts)} posts at {output}")


if __name__ == "__main__":
    main()
