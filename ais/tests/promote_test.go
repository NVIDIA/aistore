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

// TODO -- FIXME: cover permutations (rand-target, recursive, keep, few vs many files)
//                                   (wait-xact-by-id, wait-for-node)

var genfiles = `for f in {%d..%d}; do b=$RANDOM; for i in {1..10}; do echo $b; done > %s/$f.txt; done`

type prmTestPermut struct {
	num          int
	singleTarget bool
	recurs       bool
	keep         bool
}

func TestPromoteBasic(t *testing.T) {
	tests := []prmTestPermut{
		{num: 90000, singleTarget: false, recurs: false, keep: false},
		{num: 90000, singleTarget: true, recurs: false, keep: false},
		{num: 10, singleTarget: false, recurs: false, keep: false},
		{num: 10, singleTarget: true, recurs: false, keep: false},
	}
	for _, test := range tests {
		var name string
		if test.num < 100 {
			name += "/few-files"
		}
		if test.singleTarget {
			name += "/single-target"
		}
		if test.recurs {
			name += "/recurs"
		}
		if test.keep {
			name += "/keep"
		}
		if name == "" {
			name = "basic"
		} else {
			name = name[1:]
		}
		t.Run(name, test.do)
	}
}

func (test *prmTestPermut) do(t *testing.T) {
	var (
		m          = ioContext{t: t, bck: cmn.Bck{Provider: cmn.ProviderAIS, Name: cos.RandString(10)}}
		from       = 10000
		to         = from + test.num - 1
		baseParams = tutils.BaseAPIParams()
	)
	m.initWithCleanupAndSaveState()
	tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	if testing.Short() {
		to = from + cos.Max(cos.Min(test.num/100, 99), 10)
	}
	tempDir, err := os.MkdirTemp("", "prm")
	tassert.CheckFatal(t, err)
	err = cos.CreateDir(tempDir)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	// generate
	tlog.Logf("Generating %d files...\n", to-from+1)
	cmd := fmt.Sprintf(genfiles, from, to, tempDir)
	_, err = exec.Command("bash", "-c", cmd).CombinedOutput()
	tassert.CheckFatal(t, err)

	// prep request
	args := api.PromoteArgs{
		BaseParams: baseParams,
		Bck:        m.bck,
		SrcFQN:     tempDir,
	}
	if test.singleTarget {
		target, _ := m.smap.GetRandTarget()
		tlog.Logf("Promoting via %s\n", target.StringEx())
		args.Target = target.ID()
	}

	// promote
	err = api.PromoteFileOrDir(&args)
	tassert.CheckFatal(t, err)
	time.Sleep(2 * time.Second)

	tlog.Logf("Waiting to %q %s => %s\n", cmn.ActPromote, tempDir, m.bck)
	xargs := api.XactReqArgs{Kind: cmn.ActPromote, Timeout: rebalanceTimeout}
	if m.smap.CountActiveProxies() > 4 /* TODO -- FIXME: can use IC */ && args.Target == "" {
		// cluster
		notifStatus, err := api.WaitForXactionIC(baseParams, xargs)
		if notifStatus != nil && (notifStatus.AbortedX || notifStatus.ErrMsg != "") {
			tlog.Logf("notif-status: %+v\n", notifStatus)
		}
		if cmn.IsStatusNotFound(err) {
			time.Sleep(time.Second)
		} else {
			tassert.CheckFatal(t, err)
		}
	} else {
		// singled target // TODO -- FIXME: permutate
		err := api.WaitForXactionNode(baseParams, xargs, xactSnapNotRunning)
		tassert.CheckFatal(t, err)
	}

	// list
	tlog.Logln("Listing and counting objects...")
	list, err := api.ListObjects(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)
	n := to - from + 1
	tassert.Errorf(t, len(list.Entries) == n, "expected %d to be promoted, got %d", n, len(list.Entries))
}
