// Package nstlvl is intended to measure impact (or lack of thereof) of POSIX directory nesting on random read performance.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package nstlvl

import (
	"os/exec"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func dropCaches() {
	cmd := exec.Command("purge")
	_, err := cmd.Output()
	cos.AssertNoErr(err)
}
