#!/bin/bash
#
# Generate Fern documentation pages from docs/ source files.
#
# Usage: scripts/fern/generate-pages.sh [--preview|--check|--build|--publish-preview]
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
MODE="${1:-}"

case "$MODE" in
    ""|"--preview"|"--check"|"--build"|"--publish-preview") ;;
    *)
        echo "ERROR: Unknown option: $MODE" >&2
        echo "Usage: scripts/fern/generate-pages.sh [--preview|--check|--build|--publish-preview]" >&2
        exit 1
        ;;
esac

restore_docs_yml() {
    if [ -f "$FERN_DIR/docs.yml.bak" ]; then
        mv "$FERN_DIR/docs.yml.bak" "$FERN_DIR/docs.yml"
    fi
}

if [[ "$MODE" == "--check" ]]; then
    trap restore_docs_yml EXIT
fi

if [[ "$MODE" == "--publish-preview" && -z "${FERN_TOKEN:-}" ]]; then
    echo "ERROR: FERN_TOKEN is required to publish a Fern preview link." >&2
    exit 1
fi

echo "Generating Fern pages from docs/..."

# ── Clean ────────────────────────────────────────────────────────────────

rm -rf "$FERN_PAGES"
[ -f "$FERN_DIR/docs.yml.bak" ] && mv "$FERN_DIR/docs.yml.bak" "$FERN_DIR/docs.yml" || true

# ── 1. Create directories ────────────────────────────────────────────────

mkdir -p "$FERN_PAGES"/{cli,blog,tutorials/etl,proposals,images,assets}

# ── 2. Copy source files ────────────────────────────────────────────────

cp "$REPO_ROOT/README.md" "$FERN_PAGES/readme.md"

for f in "$DOCS_DIR"/*.md; do
    bn=$(basename "$f")
    case "$bn" in index.md|blog.md|README.md) continue;; esac
    cp "$f" "$FERN_PAGES/$bn"
done

cp "$DOCS_DIR"/cli/*.md "$FERN_PAGES/cli/"
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
#
# TODO: consolidate this step. Non-blog stripping goes away entirely once the
# /docs → docs.nvidia.com/aistore redirect lands (Jekyll will serve those
# pages). The blog path should then be rewritten so frontmatter stripping and
# metadata injection live in a single place (Python), not split across bash
# and Python.

# Strip frontmatter from non-blog files (blog posts handled separately with metadata injection)
find "$FERN_PAGES" -name '*.md' -not -path "*/blog/*" | while read -r f; do
    if head -1 "$f" | grep -q '^---$'; then
        awk 'BEGIN{skip=0} /^---$/{skip++;next} skip<2{next} {print}' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
    fi
done

# Strip blog frontmatter and inject author/date/tags metadata
python3 "$SCRIPTS_DIR/inject-blog-meta.py" "$DOCS_DIR/_posts" "$FERN_PAGES/blog"

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
for subdir in cli blog proposals; do
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

# All internal links use /aistore/ as the basepath (matches docs.nvidia.com/aistore/)
BASEPATH="/aistore"
GITHUB_BASE="https://github.com/NVIDIA/aistore/blob/main"

# Convert repo-internal paths (not /docs/, /blog/, /cli/) to full GitHub URLs.
# MUST come before the .md-stripping step below; otherwise paths like
# /memsys/README.md lose their .md and GitHub 404s the resulting URL.
find "$FERN_PAGES" -name '*.md' -exec perl -pi \
    -e 's{\]\(/(?!docs/|blog/|cli/|tutorials/|proposals/|assets/|images/|README|[#])([^)]+)\)}{]('"$GITHUB_BASE"'/$1)}g;' \
    {} +

# Strip .md extensions from remaining internal links only (skip http/https URLs)
find "$FERN_PAGES" -name '*.md' -exec perl -pi \
    -e 's/(\]\((?!https?:\/\/)[^)]*?)\.md(\))/$1$2/g;' \
    -e 's/(\]\((?!https?:\/\/)[^)]*?)\.md(#)/$1$2/g;' \
    {} +

# Rewrite /docs/X → /aistore/X, /blog/X → /aistore/blog/X, /README → /aistore/
find "$FERN_PAGES" -name '*.md' -exec perl -pi \
    -e "s{\\]\\(/docs/([^)]+)\\)}{]($BASEPATH/\$1)}g;" \
    -e "s{\\]\\(/blog/([^)]+)\\)}{]($BASEPATH/blog/\$1)}g;" \
    -e "s{\\]\\(/README(#[^)]*)?\\)}{]($BASEPATH/\$1)}g;" \
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

# ── Optional: preview, check, build, or publish branch preview ───────────

if [[ "$MODE" == "--preview" || "$MODE" == "--check" || "$MODE" == "--build" || "$MODE" == "--publish-preview" ]]; then
    if ! command -v fern &>/dev/null; then
        echo "ERROR: 'fern' CLI not found. Install with: npm install -g fern-api" >&2
        exit 1
    fi

    cd "$REPO_ROOT"

    if [[ "$MODE" == "--publish-preview" ]]; then
        echo "Generating Python API reference (fern docs md generate)..."
        fern docs md generate

        fern check

        preview_id="${FERN_PREVIEW_ID:-${CI_COMMIT_REF_SLUG:-branch-preview}}"
        preview_log_file="${FERN_PREVIEW_LOG_FILE:-}"
        preview_dotenv_file="${FERN_PREVIEW_DOTENV_FILE:-}"
        echo "Publishing Fern preview with id: ${preview_id}"
        set +e
        output=$(FERN_TOKEN="$FERN_TOKEN" fern generate --docs --preview --id "$preview_id" --force 2>&1)
        status=$?
        set -e
        if [ -n "$preview_log_file" ]; then
            printf '%s\n' "$output" | tee "$preview_log_file"
        else
            printf '%s\n' "$output"
        fi
        if [ "$status" -ne 0 ]; then
            exit "$status"
        fi

        preview_url=$(printf '%s\n' "$output" | sed -n 's/.*Published docs to \(https:\/\/[^[:space:]()]*\).*/\1/p' | tail -n 1)
        if [ -z "$preview_url" ]; then
            echo "ERROR: Fern preview URL was not found in command output." >&2
            exit 1
        fi
        if [ -n "$preview_dotenv_file" ]; then
            printf 'FERN_PREVIEW_URL=%s\n' "$preview_url" > "$preview_dotenv_file"
        fi
        echo "Fern preview URL: ${preview_url}"
        exit 0
    fi

    # fern/pages/python/ is auto-generated from source docstrings by
    # `fern docs md generate` (requires FERN_TOKEN). Run it here so users get
    # real Python API reference docs in their local preview without remembering
    # the extra step. Falls back to a stub if the token isn't set or the call
    # fails — otherwise the navigation ref in docs.yml triggers a blank 500.
    PYTHON_PAGES="$FERN_PAGES/python"
    if [ -n "${FERN_TOKEN:-}" ]; then
        echo "Generating Python API reference (fern docs md generate)..."
        if ! fern docs md generate; then
            echo "WARNING: 'fern docs md generate' failed; falling back to stub" >&2
        fi
    else
        echo "FERN_TOKEN not set; skipping 'fern docs md generate' (stub will be used)"
    fi

    if [ ! -d "$PYTHON_PAGES" ] || [ -z "$(ls -A "$PYTHON_PAGES" 2>/dev/null)" ]; then
        mkdir -p "$PYTHON_PAGES"
        cat > "$PYTHON_PAGES/index.mdx" <<'EOF'
# Python SDK API Reference

The Python SDK API reference is auto-generated from source docstrings by
`fern docs md generate` (requires `FERN_TOKEN`). To preview it locally, set
`FERN_TOKEN` and re-run `make fern-preview`.

View the published reference at
[docs.nvidia.com/aistore/api-reference](https://docs.nvidia.com/aistore/api-reference).
EOF
        echo "Created local-preview stub: $PYTHON_PAGES/index.mdx"
    fi

    if [[ "$MODE" == "--preview" ]]; then
        fern docs dev --port 3000
    elif [[ "$MODE" == "--check" ]]; then
        fern check
    else
        fern generate --docs
    fi
fi
