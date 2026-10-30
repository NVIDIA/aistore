#!/bin/bash
# Compute unpublished root module checksums from a clean release branch.
set -euo pipefail
readonly GIT_TAG_RE='^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
readonly MODULE=github.com/NVIDIA/aistore
export GIT_NO_REPLACE_OBJECTS=1

usage() {
  cat <<EOF
Usage: $0 GIT_TAG
       $0 -h | --help

Print two go.sum entries for an unpublished root module git tag at HEAD.

  GIT_TAG  Release git tag in v1.N.N format, with a v prefix and no leading zeros.

Example:
  $0 v1.5.2

Requires a clean branch containing current GitHub main, no merge commits above
main, no Git attributes outside the defaults, and an absent local and GitHub tag.
Fetches GitHub main; does not edit module files, commit, tag, or publish.

For details, see local README.md.
EOF
}

die() { echo "gosum: $*" >&2; exit 1; }

if [[ $# == 1 && ( $1 == -h || $1 == --help ) ]]; then
  usage
  exit 0
fi
if [[ $# != 1 ]]; then
  usage >&2
  exit 2
fi
[[ $1 =~ $GIT_TAG_RE ]] || die "expected git tag v1.N.N (v prefix required; no leading zeros or prerelease suffix)"
version=$1 ## git tag

command -v git >/dev/null || die "git is required"
command -v go >/dev/null || die "go is required"
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
cd -- "$repo"

shallow=$(git rev-parse --is-shallow-repository)
[[ $shallow == false ]] || die "a complete Git history is required (shallow checkout)"
git symbolic-ref -q HEAD >/dev/null || die "check out a release branch (detached HEAD)"
for state in MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD rebase-merge rebase-apply sequencer; do
  state_path=$(git rev-parse --git-path "$state")
  [[ ! -e $state_path ]] || die "finish or abort the in-progress Git operation: $state"
done

check_clean() {
  local changes
  changes=$(git status --porcelain --untracked-files=all --ignore-submodules=none) || die "could not check Git status"
  [[ -z $changes ]] || die "checkout must be clean, including staged and untracked files"
}
check_clean
revision=$(git rev-parse --verify 'HEAD^{commit}')

# Module ZIPs come from 'git archive', which applies Git attributes (export-ignore, export-subst).
# The module proxy sees only the committed tree, so local attribute files would change the hash.
info_attrs=$(git rev-parse --git-path info/attributes)
[[ ! -s $info_attrs ]] || die "$info_attrs must be empty: 'git archive' applies it, the module proxy does not"
tree_files=$(git ls-tree -r --name-only "$revision")
tree_attrs=$(grep -E '(^|/)\.gitattributes$' <<<"$tree_files" || true)
[[ -z $tree_attrs ]] || die "committed .gitattributes would affect the module ZIP: $tree_attrs"

# Query GitHub itself, regardless of the branch's configured remote or upstream.
upstream=https://github.com/NVIDIA/aistore.git
git fetch --no-tags "$upstream" refs/heads/main >&2
main=$(git rev-parse --verify 'FETCH_HEAD^{commit}')
git merge-base --is-ancestor "$main" "$revision" || die "HEAD must contain current GitHub main; rebase onto it first"
merges=$(git rev-list --merges "$main..$revision")
[[ -z $merges ]] || die "release commits above GitHub main must not contain merges"

tag=$version
if git show-ref --verify --quiet "refs/tags/$tag"; then
  die "tag already exists locally: $tag"
else
  status=$?
  [[ $status == 1 ]] || die "could not check the local tag (git exit $status)"
fi
if git ls-remote --exit-code --tags "$upstream" "refs/tags/$tag" >/dev/null; then
  die "tag already exists on GitHub: $tag"
else
  status=$?
  [[ $status == 2 ]] || die "could not check the tag on GitHub (git exit $status)"
fi

echo "gosum: $MODULE $version at $revision (tag $tag)" >&2
tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT
# Ignore enclosing Go workspaces; require the committed dependency configuration.
GOWORK=off go build -mod=readonly -o "$tmp_dir/gosum" ./scripts/github-release/gosum.go
# Run the helper without system, global, or user attribute files, and without system or global Git config.
sums=$(GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_ATTR_NOSYSTEM=1 \
  GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.attributesFile GIT_CONFIG_VALUE_0=/dev/null \
  "$tmp_dir/gosum" . "$MODULE" "$version" "$revision")
current=$(git rev-parse --verify 'HEAD^{commit}')
[[ $current == "$revision" ]] || die "HEAD changed while computing checksums"
check_clean
printf '%s\n' "$sums"
