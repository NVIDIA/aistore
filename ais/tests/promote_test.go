// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

var genfiles = `for f in {%d..%d}; do b=$RANDOM; for i in {1..10}; do echo $b; done > %s/$f.txt; done`

func TestPromoteBasic(t *testing.T) {
	var (
		m          = ioContext{t: t, bck: cmn.Bck{Provider: cmn.ProviderAIS, Name: cos.RandString(10)}}
		from, to   = 10000, 99999
		baseParams = tutils.BaseAPIParams()
	)
	m.initWithCleanup()
	tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	if testing.Short() {
		to = from + 100
	}
	tempDir, err := os.MkdirTemp("", "prm")
	tassert.CheckFatal(t, err)
	err = cos.CreateDir(tempDir)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	// generate
	tlog.Logln("Generating files...")
	cmd := fmt.Sprintf(genfiles, from, to, tempDir)
	_, err = exec.Command("bash", "-c", cmd).CombinedOutput()
	tassert.CheckFatal(t, err)

	// promote
	args := api.PromoteArgs{
		BaseParams: baseParams,
		Bck:        m.bck,
		SrcFQN:     tempDir,
	}
	err = api.PromoteFileOrDir(&args)
	tassert.CheckFatal(t, err)

	tlog.Logf("Waiting to %q %s => %s\n", cmn.ActPromote, tempDir, m.bck)
	xargs := api.XactReqArgs{Kind: cmn.ActPromote, Timeout: rebalanceTimeout}
	time.Sleep(2 * time.Second)
	// TODO -- FIXME must work: api.WaitForXactionIC(baseParams, xargs)
	err = api.WaitForXactionNode(baseParams, xargs, xactSnapNotRunning)
	tassert.CheckFatal(t, err)

	// list
	tlog.Logln("Listing and counting objects...")
	list, err := api.ListObjects(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)
	n := to - from + 1
	tassert.Errorf(t, len(list.Entries) == n, "expected %d to be promoted, got %d", n, len(list.Entries))
}
