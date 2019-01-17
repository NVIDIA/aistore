package main

import (
	"fmt"

	"github.com/NVIDIA/dfcpub/ais"
)

// NOTE: these variables are set by ldflags in `deploy.sh`
var (
	version string
	build   string
)

func main() {
	fmt.Printf("version: %s | build_time: %s\n", version, build)
	ais.Run()
}
