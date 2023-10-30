// Package integration_test.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"os/exec"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func TestGetWarmValidateUsingScript(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{
		CloudBck: true,
		Bck:      cliBck,
	})
	// note additional limitation
	normp, _ := cmn.NormalizeProvider(cliBck.Provider)
	if normp != apc.AWS {
		t.Skipf("skipping %s - the test uses s3cmd (command line tool) and requires s3 bucket (see \"prerequisites\")", t.Name())
	}

	cmd := exec.Command("./scripts/s3-get-validate.sh", "--bucket", cliBck.Cname(""))
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}
