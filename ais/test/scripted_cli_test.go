// Package integration_test.
/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestGetWarmValidateS3UsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		CloudBck: true,
		Bck:      cliBck,
	})
	// note additional limitation
	normp, _ := cmn.NormalizeProvider(cliBck.Provider)
	if normp != apc.AWS {
		t.Skipf("skipping %s - the test uses s3cmd (command line tool) and requires s3 bucket (see \"prerequisites\")", t.Name())
	}

	var (
		bucketName = cliBck.Cname("")
		cmd        = exec.Command("./scripts/s3-get-validate.sh", "--bucket", bucketName)
	)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestGetWarmValidateRemaisUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})

	bck := cliBck
	if bck.IsRemoteAIS() {
		tlog.Logf("using existing %s ...\n", bck.Cname(""))
	} else {
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
			Ns:       cmn.Ns{UUID: tools.RemoteCluster.Alias},
		}
		tlog.Logf("using temp bucket %s ...\n", bck.Cname(""))
	}

	var (
		bucketName = bck.Cname("")
		cmd        = exec.Command("./scripts/remais-get-validate.sh", "--bucket", bucketName)
	)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestPrefetchLatestS3UsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		CloudBck: true,
		Bck:      cliBck,
	})
	// note additional limitation
	normp, _ := cmn.NormalizeProvider(cliBck.Provider)
	if normp != apc.AWS {
		t.Skipf("skipping %s - the test uses s3cmd (command line tool) and requires s3 bucket (see \"prerequisites\")", t.Name())
	}

	var (
		bucketName = cliBck.Cname("")
		cmd        = exec.Command("./scripts/s3-prefetch-latest-prefix.sh", "--bucket", bucketName)
	)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestPrefetchLatestRemaisUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})

	bck := cliBck
	if bck.IsRemoteAIS() {
		tlog.Logf("using existing %s ...\n", bck.Cname(""))
	} else {
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
			Ns:       cmn.Ns{UUID: tools.RemoteCluster.Alias},
		}
		tlog.Logf("using temp bucket %s ...\n", bck.Cname(""))
	}

	var (
		bucketName = bck.Cname("")
		cmd        = exec.Command("./scripts/remais-prefetch-latest.sh", "--bucket", bucketName)
	)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestCopySyncWithOutOfBandUsingRemaisScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long:                  true,
		RequiresRemoteCluster: true,
	})

	bck := cliBck
	var (
		bucketName = bck.Cname("")
		cmd        = exec.Command("./scripts/cp-sync-remais-out-of-band.sh", "--bucket", bucketName)
	)
	tlog.Logln("note: this will take a while...")
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}
