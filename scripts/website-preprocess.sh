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
  elif [[ "$dir_path" == "Models" ]]; then
    # Models need .html extension for proper MIME type
    permalink="/docs/$dir_path/$bname.html"
    redirect1="/${dir_path}/${bname}.md/"
    redirect2="/docs/${dir_path}/${bname}.md/"
  else
    permalink="/docs/$dir_path/$bname"
    redirect1="/${dir_path}/${bname}.md/"
    redirect2="/docs/${dir_path}/${bname}.md/"
  fi

  if [[ "$dir_path" == "Models" ]]; then
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
  else
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
  fi

  tmp=$(mktemp)
  printf '%s\n\n' "$frontmatter" > "$tmp"
  
  if [[ "$file" == *"http-api.md"* ]]; then
    # Fix escaped underscores in parameter names (e.g., parameter\_name -> parameter_name)
    sed 's/\([a-zA-Z0-9]\)\\\_\([a-zA-Z0-9]\)/\1_\2/g' "$file" >> "$tmp"
  else
    cat "$file" >> "$tmp"
  fi
  
  mv "$tmp" "$file"
  echo "Added front-matter to $file"
done
