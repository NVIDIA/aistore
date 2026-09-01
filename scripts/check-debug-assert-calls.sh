#!/bin/bash
#
# Find debug.Assert* calls with arguments emitting a CALL in a production
# (debug-off) aisnode binary.
#
# Background:
# In non-debug builds cmn/debug's helpers are empty functions:
#     func Assert(_ bool, _ ...any) {}
#     func AssertNoErr(_ error)     {}
#
# The helpers inline to nothing and the variadic []any dies with them, but Go
# must still evaluate every argument. A plain variable costs nothing after
# optimization (including its interface boxing); a function call the compiler
# cannot inline may remain in the binary. Those calls are exactly what
# this script finds so they can be eliminated from production builds.
#
# The binary must be optimized, debug-off, and unstripped. Build normally, but
# omit the production linker flags "-w -s" so `go tool objdump` has the symbols
# and source positions it needs. Do not use MODE=debug: it changes both the
# assertion behavior and compiler optimization.
#
# Example usage:
# ./check-debug-assert-calls.sh /tmp/debug-off/aisnode ~/gocode/src/github.com/NVIDIA/aistore
#
# Selected runtime.* helpers representing actual argument work are always
# included. Use --include-runtime to retain all runtime.* calls.
#
# TODO: Inlining - see Step 2 comment below.

set -euo pipefail
export LC_ALL=C # join and sort must agree on collation

usage() {
	cat <<EOF
Usage: ${0##*/} [options] <binary> [src-tree]

Find CALL instructions attributed to debug.Assert* source lines in an
optimized, debug-off, unstripped Go binary. If src-tree is omitted, use the
Git worktree containing the current directory.

Options:
  --asm FILE             Reuse or create this exact disassembly file
  --include-runtime      Include all runtime.* callees
  --allow-mismatch       Warn instead of failing on a VCS revision mismatch
  --findings FILE        Write matched CALL records to FILE
  --unmatched FILE       Write rejected basename/line collisions to FILE
  -h, --help             Show this help

Environment equivalents:
  ASM, INCLUDE_RUNTIME=1, ALLOW_MISMATCH=1, FINDINGS, UNMATCHED

Exit status:
  0  no matched CALLs
  1  one or more matched CALLs
  2  usage, tool, identity, or stale-source error

Unmatched records are informational and do not affect the exit status.
EOF
}

die() {
	echo "ERROR: $*" >&2
	exit 2
}

unexpected_error() {
	local line=$1 status=$2
	trap - ERR
	echo "ERROR: unexpected failure at line $line (status $status)" >&2
	exit 2
}
trap 'unexpected_error "$LINENO" "$?"' ERR

ASM=${ASM:-}
INCLUDE_RUNTIME=${INCLUDE_RUNTIME:-0}
ALLOW_MISMATCH=${ALLOW_MISMATCH:-0}
FINDINGS=${FINDINGS:-}
UNMATCHED=${UNMATCHED:-}

while (( $# )); do
	case $1 in
		--asm)
			(( $# >= 2 )) || die "--asm requires a file"
			ASM=$2
			shift 2
			;;
		--include-runtime)
			INCLUDE_RUNTIME=1
			shift
			;;
		--allow-mismatch)
			ALLOW_MISMATCH=1
			shift
			;;
		--findings)
			(( $# >= 2 )) || die "--findings requires a file"
			FINDINGS=$2
			shift 2
			;;
		--unmatched)
			(( $# >= 2 )) || die "--unmatched requires a file"
			UNMATCHED=$2
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		--)
			shift
			break
			;;
		-*)
			die "unknown option: $1 (try --help)"
			;;
		*)
			break
			;;
	esac
done

(( $# == 1 || $# == 2 )) || die "expected <binary> [src-tree] (try --help)"

[[ -f $1 ]] || die "not a file: $1"
BIN=$(realpath -e -- "$1")

if (( $# == 2 )); then
	[[ -d $2 ]] || die "not a directory: $2"
	SRC=$(realpath -e -- "$2")
else
	command -v git >/dev/null 2>&1 || die "src-tree omitted and git is unavailable"
	_src_guess=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null || true)
	[[ -n $_src_guess ]] || die "src-tree omitted and the current directory is not in a Git worktree"
	SRC=$(realpath -e -- "$_src_guess")
fi

[[ -f $SRC/go.mod ]] || die "missing module definition: $SRC/go.mod"
MOD=$(awk '$1 == "module" { print $2; exit }' "$SRC/go.mod")
[[ -n $MOD ]] || die "cannot determine module path from $SRC/go.mod"

TAB=$(printf '\t')
[[ $INCLUDE_RUNTIME == 0 || $INCLUDE_RUNTIME == 1 ]] || die "INCLUDE_RUNTIME must be 0 or 1"
[[ $ALLOW_MISMATCH == 0 || $ALLOW_MISMATCH == 1 ]] || die "ALLOW_MISMATCH must be 0 or 1"

# Content identity vs full objdump
_key=$(sha256sum "$BIN" | awk '{print $1}')
ASM=${ASM:-/tmp/debug-assert-calls.$_key.asm}
FINDINGS=${FINDINGS:-/tmp/debug-assert-calls.$_key.txt}
UNMATCHED=${UNMATCHED:-/tmp/debug-assert-calls.$_key.unmatched.txt}

WORKDIR=$(mktemp -d /tmp/debug-assert-calls.work.XXXXXX)
cleanup() {
	[[ -n ${WORKDIR:-} && -d $WORKDIR ]] && rm -rf -- "$WORKDIR"
}
trap cleanup EXIT

CALLS=$WORKDIR/calls
ASSERTS=$WORKDIR/asserts
RAW_ASSERTS=$WORKDIR/asserts.raw

# A freshly generated dump of an old binary is still stale. Compare sources to
# the binary itself, not to the dump's creation time.
_newest=$(find "$SRC" -name '*.go' ! -name '*_test.go' -newer "$BIN" -print -quit 2>/dev/null || true)
if [[ -n $_newest ]]; then
	echo "ERROR: $BIN predates source file(s), e.g. $_newest" >&2
	echo "       rebuild and recopy the binary before scanning this tree" >&2
	exit 2
fi

# When VCS metadata is embedded, reject a revision mismatch.
if command -v go >/dev/null 2>&1 && command -v git >/dev/null 2>&1 && git -C "$SRC" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	_meta=$(go version -m "$BIN" 2>/dev/null || true)
	_bin_rev=$(awk '/vcs\.revision=/ { sub(/^.*vcs\.revision=/, ""); print; exit }' <<<"$_meta")
	_bin_modified=$(awk '/vcs\.modified=/ { sub(/^.*vcs\.modified=/, ""); print; exit }' <<<"$_meta")
	_src_rev=$(git -C "$SRC" rev-parse HEAD)

	if [[ -n $_bin_rev && $_bin_rev != "$_src_rev" ]]; then
		if [[ $ALLOW_MISMATCH != 1 ]]; then
			echo "ERROR: binary revision $_bin_rev != source revision $_src_rev" >&2
			echo "       set ALLOW_MISMATCH=1 only for an intentional comparison" >&2
			exit 2
		fi
		echo "WARNING: binary revision $_bin_rev != source revision $_src_rev" >&2
	fi
	if [[ $_bin_modified == true ]]; then
		echo "WARNING: binary carries vcs.modified=true; exact source identity is not provable" >&2
	fi
	_source_pathspec=('*.go' '*.s' '*.S' '*.c' '*.h' 'go.mod' 'go.sum')
	if ! git -C "$SRC" diff --quiet --ignore-submodules -- "${_source_pathspec[@]}" ||
		! git -C "$SRC" diff --cached --quiet --ignore-submodules -- "${_source_pathspec[@]}"; then
		echo "WARNING: source tree has tracked build-source changes; timestamp guard is the remaining check" >&2
	fi
fi

if [[ ! -s $ASM ]]; then
	command -v go >/dev/null 2>&1 || { echo "ERROR: go is required to create $ASM" >&2; exit 2; }
	echo "disassembling $BIN -> $ASM (slow, large)" >&2

	if ! go tool objdump "$BIN" > "$WORKDIR/objdump" 2> "$WORKDIR/objdump.err"; then
	       if grep -q 'no symbol section' "$WORKDIR/objdump.err"; then
		       echo "ERROR: $BIN is stripped - no symbol table to disassemble" >&2
		       echo "       rebuild without the production linker flags \"-w -s\"" >&2
		       exit 2
	       fi
	       cat -- "$WORKDIR/objdump.err" >&2
	       die "go tool objdump failed"
	fi

	mv -- "$WORKDIR/objdump" "$ASM"
else
	echo "reusing cached disassembly $ASM" >&2
	echo "  (sha256=$_key, binary=$BIN)" >&2
fi

# 1. CALLs -> basename:line <TAB> pkgdir <TAB> owner-symbol <TAB> callee
# Use INCLUDE_RUNTIME=1 to include all runtime helpers
# (concatstring and few selected are always included)
awk -v mod="$MOD" -v include_runtime="$INCLUDE_RUNTIME" '
	/^TEXT / {
		sym = $2
		pkg = ""
		if (index(sym, mod "/") == 1 || index(sym, mod ".") == 1) {
			rest = substr(sym, length(mod) + 2) # after "mod/" or "mod."
			d = index(rest, ".")
			pkg = (d ? substr(rest, 1, d - 1) : rest)
			if (index(sym, mod ".") == 1)
				pkg = "" # module-root package
		} else {
			pkg = "\x01" # non-aistore: never package-matches
		}
		next
	}
	$4 == "CALL" {
		callee = $5
		# always include assorted certain runtime helpers
		if (!include_runtime && callee ~ /^runtime\./ &&
		   callee !~ /^runtime\.(concatstring|concatbyte|mapaccess|slicebytetostring|slicerunetostring|stringtoslicebyte|stringtoslicerune)/)
			next
		if ($1 ~ /^[^ \t]+\.go:[0-9]+$/)
			print $1 "\t" pkg "\t" sym "\t" callee
	}
' "$ASM" | sort -u > "$CALLS"

# TODO: Handle inlining. For example, see api/client.go line 429 -
# api.readMultipart calls multipart.Part.FormName() in debug.Assert arguments where
# emitted calls evade the join below.

# 2. Assertion sites -> basename:line <TAB> pkgdir <TAB> fullpath:line
grep -rnE 'debug\.(Assert[A-Za-z]*|FailTypeCast)\(' \
	--include='*.go' "$SRC" > "$RAW_ASSERTS" || true

awk -F: -v src="$SRC" '
	$1 !~ /_test\.go$/ && $1 !~ /\/cmn\/debug\// {
		full = $1
		line = $2
		rel = substr(full, length(src) + 2)
		n = split(rel, p, "/")
		base = p[n]
		dir = ""
		for (i = 1; i < n; i++)
			dir = dir (i > 1 ? "/" : "") p[i]
		print base ":" line "\t" dir "\t" full ":" line
	}
' "$RAW_ASSERTS" | sort -u > "$ASSERTS"

# 3. Join on basename:line, then require owning package agreement. Include the
# actual callee so every finding can be explained from one line.
: > "$FINDINGS"
: > "$UNMATCHED"

join -t"$TAB" -j1 -o 1.3,2.3,2.4,1.2,2.2 "$ASSERTS" "$CALLS" \
	| awk -F"$TAB" -v ok="$FINDINGS" -v no="$UNMATCHED" '
		{
			if ($4 == $5)
				print $1 "\t" $2 "\t" $3 > ok
			else
				print $1 "\t" $2 "\t" $3 "\t(pkg " $4 " vs " $5 ")" > no
		}
'

sort -u -o "$FINDINGS" "$FINDINGS"
sort -u -o "$UNMATCHED" "$UNMATCHED"

cat "$FINDINGS"
echo >&2
_calls=$(wc -l < "$FINDINGS")
_sites=$(cut -f1 "$FINDINGS" | sort -u | wc -l)
_unmatched=$(wc -l < "$UNMATCHED")
echo "assert sites    : $_sites ($_calls CALL records) -> $FINDINGS" >&2
echo "unmatched       : $_unmatched -> $UNMATCHED" >&2

(( _sites == 0 )) || exit 1
