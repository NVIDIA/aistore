#!/bin/bash
set -e  # exit on any error

msgp -file cmn/cos/node_state.go -tests=false -marshal=false -unexported
msgp -file api/apc/sys.go        -tests=false -marshal=false -unexported
msgp -file sys/mem.go            -tests=false -marshal=false -unexported
msgp -file sys/proc.go           -tests=false -marshal=false -unexported
msgp -file fs/api.go             -tests=false -marshal=false -unexported
msgp -file sys/loadavg.go        -tests=false -marshal=false -unexported
msgp -file cmn/cos/fs_unix.go    -tests=false -marshal=false -unexported

msgp -file stats/api_wire.go       -tests=false -marshal=false -unexported
msgp -file cmn/bck_wire.go         -tests=false -marshal=false -unexported
msgp -file core/xaction_wire.go    -tests=false -marshal=false -unexported
msgp -file core/meta/snode_wire.go -tests=false -marshal=false -unexported

## to remove
## rm api/apc/sys_gen.go cmn/bck_wire_gen.go cmn/cos/node_state_gen.go cmn/cos/fs_unix.go core/meta/snode_wire_gen.go \
##    core/xaction_wire_gen.go fs/api_gen.go stats/api_wire_gen.go sys/mem_gen.go sys/proc_gen.go sys/loadavg_gen.go
