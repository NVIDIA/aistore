// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"os/exec"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestDsortUsingScripts(t *testing.T) {
	var (
		src = cmn.Bck{Name: "src_" + trand.String(6), Provider: apc.AIS}
		dst = cmn.Bck{Name: "dst_" + trand.String(6), Provider: apc.AIS}
		cmd = exec.Command("./scripts/dsort-ex1.sh", "--srcbck", src.Cname(""), "--dstbck", dst.Cname(""))
	)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}
