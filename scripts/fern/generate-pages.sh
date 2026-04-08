#!/bin/bash
#
# Generate Fern documentation pages from docs/ source files.
#
# Usage: scripts/fern/generate-pages.sh
#
# This script copies markdown from docs/ to fern/pages/, fixes image paths,
# strips Jekyll frontmatter, escapes MDX-unsafe patterns, and auto-generates
# navigation for docs and blog tabs.
#
# macOS note: uses perl instead of sed -i for portability.
#

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FERN_DIR="$REPO_ROOT/fern"
FERN_PAGES="$FERN_DIR/pages"
DOCS_DIR="$REPO_ROOT/docs"
SCRIPTS_DIR="$REPO_ROOT/scripts/fern"

echo "Generating Fern pages from docs/..."

# ── Clean ────────────────────────────────────────────────────────────────

rm -rf "$FERN_PAGES"
[ -f "$FERN_DIR/docs.yml.bak" ] && mv "$FERN_DIR/docs.yml.bak" "$FERN_DIR/docs.yml" || true

# ── 1. Create directories ────────────────────────────────────────────────

mkdir -p "$FERN_PAGES"/{cli,blog,relnotes,tutorials/etl,proposals,images,assets}

# ── 2. Copy source files ────────────────────────────────────────────────

cp "$REPO_ROOT/README.md" "$FERN_PAGES/readme.md"

for f in "$DOCS_DIR"/*.md; do
    bn=$(basename "$f")
    case "$bn" in index.md|blog.md|README.md) continue;; esac
    cp "$f" "$FERN_PAGES/$bn"
done

cp "$DOCS_DIR"/cli/*.md "$FERN_PAGES/cli/"
cp "$DOCS_DIR"/relnotes/*.md "$FERN_PAGES/relnotes/"
cp "$DOCS_DIR"/tutorials/etl/*.md "$FERN_PAGES/tutorials/etl/" 2>/dev/null || true
cp "$DOCS_DIR"/proposals/*.md "$FERN_PAGES/proposals/" 2>/dev/null || true

# Blog posts: strip YYYY-MM-DD- date prefix from filenames
for f in "$DOCS_DIR"/_posts/*.md; do
    bn=$(basename "$f")
    [ "$bn" = "README.md" ] && continue
    slug=$(echo "$bn" | sed 's/^[0-9]\{4\}-[0-9]\{2\}-[0-9x]\{2\}-//')
    cp "$f" "$FERN_PAGES/blog/$slug"
done

# ── 3. Copy image assets ────────────────────────────────────────────────

cp -rn "$DOCS_DIR"/images/* "$FERN_PAGES/images/" 2>/dev/null || true
if command -v rsync &>/dev/null; then
    rsync -a --ignore-existing "$DOCS_DIR/assets/" "$FERN_PAGES/assets/" 2>/dev/null || true
else
    cp -rn "$DOCS_DIR"/assets/* "$FERN_PAGES/assets/" 2>/dev/null || true
fi

# ── 4. Strip Jekyll frontmatter ─────────────────────────────────────────

find "$FERN_PAGES" -name '*.md' | while read -r f; do
    if head -1 "$f" | grep -q '^---$'; then
        awk 'BEGIN{skip=0} /^---$/{skip++;next} skip<2{next} {print}' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
    fi
done

# ── 5. Fix image paths (portable: perl instead of sed -i) ───────────────

# Root-level pages: images/ and assets/ are siblings
find "$FERN_PAGES" -maxdepth 1 -name '*.md' -exec perl -pi \
    -e 's|\]\(/docs/images/|](images/|g;' \
    -e 's|\]\(/docs/assets/|](assets/|g;' \
    -e 's|\]\(/images/|](images/|g;' \
    -e 's|\]\(/assets/|](assets/|g;' \
    -e 's|src="/docs/images/|src="images/|g;' \
    -e 's|src="/images/|src="images/|g;' \
    {} +

# Subdirectory pages: need ../ prefix
for subdir in cli blog relnotes proposals; do
    find "$FERN_PAGES/$subdir" -name '*.md' -exec perl -pi \
        -e 's|\]\(/docs/images/|](../images/|g;' \
        -e 's|\]\(/docs/assets/|](../assets/|g;' \
        -e 's|\]\(/images/|](../images/|g;' \
        -e 's|\]\(/assets/|](../assets/|g;' \
        -e 's|\]\(images/|](../images/|g;' \
        -e 's|\]\(assets/|](../assets/|g;' \
        -e 's|src="/docs/images/|src="../images/|g;' \
        -e 's|src="/images/|src="../images/|g;' \
        -e 's|src="/assets/|src="../assets/|g;' \
        {} + 2>/dev/null || true
done

# Tutorial pages: need ../../ prefix
find "$FERN_PAGES/tutorials" -name '*.md' -exec perl -pi \
    -e 's|\]\(\.\./images/|](../../images/|g;' \
    -e 's|\]\(/docs/images/|](../../images/|g;' \
    -e 's|\]\(/docs/assets/|](../../assets/|g;' \
    -e 's|\]\(/images/|](../../images/|g;' \
    -e 's|\]\(/assets/|](../../assets/|g;' \
    -e 's|\]\(images/|](../../images/|g;' \
    {} + 2>/dev/null || true

# ── 6. Fix internal links ───────────────────────────────────────────────

# Strip .md extensions from internal links only (skip http/https URLs)
find "$FERN_PAGES" -name '*.md' -exec perl -pi \
    -e 's/(\]\((?!https?:\/\/)[^)]*?)\.md(\))/$1$2/g;' \
    -e 's/(\]\((?!https?:\/\/)[^)]*?)\.md(#)/$1$2/g;' \
    {} +

# Fix README link to point to docs/readme (internal only)
find "$FERN_PAGES" -name '*.md' -exec perl -pi -e 's|\]\(/README\)|](/docs/readme)|g' {} +

# Convert repo-internal paths to full GitHub URLs
# Any absolute link starting with / that is NOT /docs/ or /blog/ is a repo path
GITHUB_BASE="https://github.com/NVIDIA/aistore/blob/main"
find "$FERN_PAGES" -name '*.md' -exec perl -pi \
    -e 's{\]\(/(?!docs/|blog/|cli/|assets/|images/|[#])([^)]+)\)}{]('"$GITHUB_BASE"'/$1)}g;' \
    {} +

# ── 7. Fix known source doc issues ──────────────────────────────────────

# Case-sensitive filename
cp "$FERN_PAGES/assets/aistore-fast-tier/network-with-k8s.png" \
   "$FERN_PAGES/assets/aistore-fast-tier/network-with-K8s.png" 2>/dev/null || true

# README: convert bold title to H1, remove badge lines
perl -pi -e 's/^\*\*(.*)\*\*$/# $1/ if $. == 1' "$FERN_PAGES/readme.md" 2>/dev/null || true
perl -ni -e 'print unless /^!\[(License|Version|Go Report)/' "$FERN_PAGES/readme.md" 2>/dev/null || true

# ── 8. Auto-generate navigation ─────────────────────────────────────────

cp "$FERN_DIR/docs.yml" "$FERN_DIR/docs.yml.bak"
python3 "$SCRIPTS_DIR/gen-docs-nav.py" "$DOCS_DIR/docs.md" --pages-dir "$FERN_PAGES" --inject "$FERN_DIR/docs.yml"
python3 "$SCRIPTS_DIR/gen-blog-index.py" "$DOCS_DIR/_posts" "$FERN_PAGES/blog/index.md"
python3 "$SCRIPTS_DIR/gen-blog-nav.py" "$DOCS_DIR/_posts" --inject "$FERN_DIR/docs.yml"

# ── 9. Escape curly braces and angle brackets via Python ────────────────

python3 "$SCRIPTS_DIR/mdx-escape.py" "$FERN_PAGES"

echo "Fern pages generated: $(find "$FERN_PAGES" -name '*.md' | wc -l) files"

# ── Optional: preview or build ───────────────────────────────────────────

if [[ "${1:-}" == "--preview" || "${1:-}" == "--build" ]]; then
    if ! command -v fern &>/dev/null; then
        echo "ERROR: 'fern' CLI not found. Install with: npm install -g fern-api" >&2
        exit 1
    fi

    cd "$REPO_ROOT"

    if [[ "${1:-}" == "--preview" ]]; then
        fern docs dev --port 3000
    else
        fern generate --docs
    fi
fi
