// Package nstlvl is intended to measure impact (or lack of thereof) of POSIX directory nesting on random read performance.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package nstlvl

import (
	"os/exec"

	"github.com/NVIDIA/aistore/cmn"
)

func dropCaches() {
	cmd := exec.Command("echo", "3", ">", "/proc/sys/vm/drop_caches") // https://www.kernel.org/doc/Documentation/sysctl/vm.txt
	_, err := cmd.Output()
	cmn.AssertNoErr(err)
}
