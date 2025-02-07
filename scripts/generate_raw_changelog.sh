#!/bin/bash

OUT1="/tmp/stage1.md"
OUT2="/tmp/stage2.md"
OUT3="/tmp/stage3.md"
OUT4="/tmp/stage4.md"
OUT5="/tmp/stage5.md"
OUT6="/tmp/stage6.md"
OUT7="/tmp/stage7.md"

VERSION="v3.26"
TAG="1.3.26"
DATE="2025-02-06"
CHANGELOG_FILE="docs/changelog/${VERSION}.md"

run_stage1() {
  exec > $OUT1

  # list commits between v1.3.25 and main, sorted descending
  commits=$(git log v1.3.25..main --oneline --no-merges)

  previous_commit_message=""
  commit_sequence=""

  # iterate
  echo "$commits" | while read line; do
    commit_hash=$(echo $line | cut -d ' ' -f 1)
    commit_message=$(echo $line | cut -d ' ' -f 2-)

    # full commit message wo/ sign-off
    full_commit_message=$(git log --format=%B -n 1 $commit_hash)
    full_commit_message=$(echo "$full_commit_message" | sed '/^Signed-off-by:/d' | sed '/^$/d')

    # commit chain
    if [[ "$commit_message" =~ part\ [0-9]+ ]]; then
      # If there's already a chain started, add this commit to it
      if [[ -n "$commit_sequence" ]]; then
        commit_sequence="$commit_sequence
  |   * \"$commit_message\" [${commit_hash}](https://github.com/NVIDIA/aistore/commit/${commit_hash})"
        continue
      fi
      # Start a new chain with this commit
      commit_sequence="* \"$commit_message\" [${commit_hash}](https://github.com/NVIDIA/aistore/commit/${commit_hash})"
    else
      # If this commit isn't part of a chain, print the chain if there was one
      if [[ -n "$commit_sequence" ]]; then
        echo "$commit_sequence"
        commit_sequence=""
      fi

      # print
      echo "* \"$commit_message\" [${commit_hash}](https://github.com/NVIDIA/aistore/commit/${commit_hash})"

      # commit body
      if [[ "$full_commit_message" != "$commit_message" ]]; then
        # Add the cleaned-up full commit message as bullet points (excluding the title)
        echo "$full_commit_message" | while IFS= read -r line; do
          # Trim leading spaces and print the bullet point properly
          line=$(echo "$line" | sed 's/^[[:space:]]*//')
          if [[ -n "$line" ]]; then
            echo "|   * $line"
          fi
        done
      fi
      echo
    fi
  done

  # print the last chain if exists
  if [[ -n "$commit_sequence" ]]; then
    echo "$commit_sequence"
  fi
  echo "Stage 1 done" >&2
}

run_stage2() {
  exec > $OUT2

  while IFS= read -r line; do
    # If the line starts with a pipe (|), it's part of the commit body
    if [[ "$line" =~ ^\| ]]; then
      # remove the leading '|' and trim prefix
      echo "  - ${line:6}"
    else
      echo "$line"
    fi
  done < $OUT1
  echo "Stage 2 done" >&2
}

run_stage3() {
  # NOTE: group names hardcoded
  declare -A groups
  groups["CLI"]="## CLI"
  groups["Python"]="## Python"
  groups["Docs"]="## Docs"
  groups["ETL"]="## ETL"
  groups["Core"]="## Core"
  groups["Build"]="## Build"
  groups["Chores"]="## Chores"
  groups["Tests"]="## Tests"
  groups["OCI"]="## OCI"
  groups["Observability"]="## Observability"
  groups["EC"]="## Erasure Coding"
  groups["Scrub"]="## Scrub"

  declare -A group_commits
  uncategorized_commits=""

  while IFS= read -r line || [[ -n "$line" ]]; do
    # Check if the line is a commit title (starts with "*")
    if [[ "$line" =~ ^\* ]]; then
      # Extract commit message for grouping (case-insensitive)
      commit_message=$(echo "$line" | cut -d '"' -f 2)
      commit="$line"$'\n'
      details=""

      # Read the following lines for the commit details
      while IFS= read -r detail_line || [[ -n "$detail_line" ]]; do
        if [[ "$detail_line" =~ ^\* ]]; then
          # If the next line starts with *, it's a new commit
          # Break the inner loop to process the new commit
          break
        fi
        details+="$detail_line"$'\n'
      done

      # combine
      full_commit="$commit$details"

      # ignore minor alterations
      if [[ "$commit_message" =~ ^(v[0-9]+\.[0-9]+\.[0-9]+|build:.*patch) ]]; then
        continue
      fi

      # grouping
      if grep -qiw "\<cli\>" <<< "$commit_message"; then
        group_commits["CLI"]+="$full_commit"$'\n'
      elif grep -qiw "\<python\>" <<< "$commit_message"; then
        group_commits["Python"]+="$full_commit"$'\n'
      elif grep -qiw "\<docs\>" <<< "$commit_message"; then
        group_commits["Docs"]+="$full_commit"$'\n'
      elif grep -qiw "\<etl\>" <<< "$commit_message"; then
        group_commits["ETL"]+="$full_commit"$'\n'
      elif grep -qiw "\<core\>" <<< "$commit_message"; then
        group_commits["Core"]+="$full_commit"$'\n'
      elif grep -qiw "\<build\>" <<< "$commit_message"; then
        group_commits["Build"]+="$full_commit"$'\n'
      elif grep -qiw "\<chores\>" <<< "$commit_message"; then
        group_commits["Chores"]+="$full_commit"$'\n'
      elif grep -qiw "\<ec\>" <<< "$commit_message"; then
        group_commits["EC"]+="$full_commit"$'\n'
      elif grep -qiw "\<scrub\>" <<< "$commit_message"; then
        group_commits["Scrub"]+="$full_commit"$'\n'
      elif grep -qiw "\<tests\>" <<< "$commit_message"; then
        group_commits["Tests"]+="$full_commit"$'\n'
      elif grep -qiw "\<oci\>" <<< "$commit_message"; then
        group_commits["OCI"]+="$full_commit"$'\n'
      elif grep -qiw "\<observability\>" <<< "$commit_message"; then
        group_commits["Observability"]+="$full_commit"$'\n'
      else
        uncategorized_commits+="$full_commit"$'\n'
      fi
    else
      # everything else
      uncategorized_commits+="$line"$'\n'
    fi
  done < "$OUT2"

  # print the groups
  {
    for group in "${!groups[@]}"; do
      # Print the group header if any commits exist for the group
      if [[ -n "${group_commits[$group]}" ]]; then
        echo "${groups[$group]}"
        echo -e "${group_commits[$group]}"
      fi
    done
    # Print uncategorized commits at the end with their checksums and links
    if [[ -n "$uncategorized_commits" ]]; then
      echo "## Uncategorized"
      echo -e "$uncategorized_commits"
    fi
  } > "$OUT3"
  echo "Stage 3 done" >&2
}

run_stage4() {
  exec > "$OUT4"

  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ -n "$line" ]]; then
      echo "$line"
    fi
  done < "$OUT3"
  echo "Stage 4 done" >&2
}

run_stage5() {
  {
    echo "# ${VERSION} (${DATE})"
    echo
    echo "## Table of Contents"
    echo
    echo "- [CLI](#cli)"
    echo "- [Python](#python)"
    echo "- [Docs](#docs)"
    echo "- [ETL](#etl)"
    echo "- [Core](#core)"
    echo "- [Build](#build)"
    echo "- [Erasure Coding](#erasure-coding)"
    echo "- [Scrub](#scrub)"
    echo "- [Chores](#chores)"
    echo "- [Tests](#tests)"
    echo "- [OCI](#oci)"
    echo "- [Observability](#observability)"
    echo "- [Uncategorized](#uncategorized)"
    echo
    cat "$OUT4"
  } > "$OUT5"
  echo "Stage 5 done" >&2
}

run_stage6() {
  exec > "$OUT6"

  previous_line=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^## ]]; then
      # Add a new line before each group title if it's not the first line
      if [[ -n "$previous_line" && ! "$previous_line" =~ ^## ]]; then
        echo
      fi
      echo "$line"
      echo
    else
      echo "$line"
    fi
    previous_line="$line"
  done < "$OUT5"

  # Add a footer with the generation date and user information
  echo >> "$OUT6"
  echo "---" >> "$OUT6"
  echo "Changelog generated on ${DATE}" >> "$OUT6"
  echo "Stage 6 done" >&2
}

run_stage7() {
  exec > "$OUT7"

  # Read the output from Stage 6 and remove redundant prefixes
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^\* ]]; then
      # Remove specific prefixes (case-insensitive)
      line=$(echo "$line" | sed -E 's/\* "CLI: /* "/I; s/\* "cli: /* "/I; s/\* "build: /* "/I; s/\* "core: /* "/I; s/\* "docs: /* "/I')
    fi
    echo "$line"
  done < "$OUT6"
  echo "Stage 7 done" >&2
}

run_stage1
run_stage2
run_stage3
run_stage4
run_stage5
run_stage6
run_stage7

# final output
mv "$OUT7" "$CHANGELOG_FILE"
echo "Changelog generated: ${CHANGELOG_FILE}" >&2
