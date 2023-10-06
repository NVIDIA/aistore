// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"os/exec"
	"testing"

	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func TestGetWarmValidateUsingScripts(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{
		RemoteBck: true,
		Bck:       cliBck,
		Long:      true,
	})
	cmd := exec.Command("./scripts/get-validate.sh", "--bucket", cliBck.Cname(""))
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}
