#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

find docs -type f -name "*.md" | while read -r file; do
  # Skip directories that already have front-matter–controlled docs
  if [[ "$file" == */_posts/* || "$file" == */_drafts/* || \
        "$file" == */_pages/* || "$file" == */changelog/* ]]; then
    echo "Skip $file (special dir)"
    continue
  fi

  bname=$(basename "$file" .md)
  # Skip README and index.md
  if [[ "$bname" == "README" || "$bname" == "index" ]]; then
    echo "Skip $file (README/index)"
    continue
  fi

  # Skip if front-matter exists
  if [[ "$(head -c 3 "$file")" == "---" ]]; then
    echo "Skip $file (already has front-matter)"
    continue
  fi

  rel_path=${file#docs/}
  dir_path=$(dirname "$rel_path")

  if [[ "$dir_path" == "." ]]; then
    permalink="/docs/$bname"
    redirect1="/${bname}.md/"
    redirect2="/docs/${bname}.md/"
  else
    permalink="/docs/$dir_path/$bname"
    redirect1="/${dir_path}/${bname}.md/"
    redirect2="/docs/${dir_path}/${bname}.md/"
  fi

  frontmatter=$(cat <<EOF
---
layout: post
title: ${bname^^}
permalink: $permalink
redirect_from:
 - $redirect1
 - $redirect2
---
EOF
)

  tmp=$(mktemp)
  printf '%s\n\n' "$frontmatter" > "$tmp"
  cat "$file" >> "$tmp"
  mv "$tmp" "$file"
  echo "Added front-matter to $file"
done
