//go:build dsort

// Package integration_test.
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"os/exec"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestDistributedSortUsingScripts(t *testing.T) {
	for _, spec := range []string{"dsort-spec1.json", "dsort-spec2.json", "dsort-spec3.json"} {
		t.Run(spec, func(t *testing.T) {
			var (
				src     = cmn.Bck{Name: "src_" + trand.String(6), Provider: apc.AIS}
				srcName = src.Cname("")
				dst     = cmn.Bck{Name: "dst_" + trand.String(6), Provider: apc.AIS}
				dstName = dst.Cname("")

				cmd = exec.Command("./scripts/dsort-ex1.sh", "--srcbck", srcName, "--dstbck", dstName)
			)
			cmd.Args = append(cmd.Args, "--spec", spec)

			out, err := cmd.CombinedOutput()
			if len(out) > 0 {
				tlog.Logln(string(out))
			}
			tassert.CheckFatal(t, err)
		})
	}
}
