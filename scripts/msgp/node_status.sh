#!/bin/bash
set -e  # exit on any error

usage() {
  cat <<EOF
Usage: $(basename "$0") <command>
Commands:
  gen | generate    Run msgp code generation
  clean | cleanup   Remove generated files
  help | usage      Show this help
EOF
}

gen_if() {
  src="$1"
  gen="${src%.go}_gen.go"
  if [ ! -e "$gen" ] || [ "$src" -nt "$gen" ]; then
    echo "Generating $src => $gen"
    msgp -file "$src" -tests=false -marshal=false -unexported
  else
    echo "Skipping $src => $gen (up-to-date)"
  fi
}


run_gen() {
  gen_if cmn/cos/node_state.go
  gen_if api/apc/sys.go
  gen_if sys/mem.go
  gen_if sys/proc.go
  gen_if fs/api.go
  gen_if sys/loadavg.go
  gen_if cmn/cos/fs_unix.go

  gen_if stats/api_wire.go
  gen_if cmn/bck_wire.go
  gen_if core/xaction_wire.go
  gen_if core/meta/snode_wire.go
}

run_clean() {
  rm -f api/apc/sys_gen.go cmn/bck_wire_gen.go cmn/cos/node_state_gen.go cmn/cos/fs_unix_gen.go \
       core/meta/snode_wire_gen.go core/xaction_wire_gen.go fs/api_gen.go stats/api_wire_gen.go \
       sys/mem_gen.go sys/proc_gen.go sys/loadavg_gen.go
}

# No args -> show usage
if [ $# -eq 0 ]; then
  usage
  exit 1
fi

case "$1" in
  gen|generate)
    run_gen
    ;;
  clean|cleanup)
    run_clean
    ;;
  help|usage|--help|-h)
    usage
    ;;
  *)
    echo "Unknown command: $1" >&2
    usage
    exit 2
    ;;
esac
