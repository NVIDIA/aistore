// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	iofs "io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

// TODO: permutations (wait-xact-by-id | wait-for-node)

var genfiles = `for f in {%d..%d}; do b=$RANDOM;
for i in {1..3}; do echo $b; done > %s/$f.test;
for i in {1..5}; do echo $b --- $b; done > %s/%s/$f.test.test;
done`

type prmTestPermut struct {
	num          int
	singleTarget bool
	recurs       bool
	keep         bool
}

func TestPromoteBasic(t *testing.T) {
	tests := []prmTestPermut{
		{num: 10000, singleTarget: false, recurs: false, keep: false},
		{num: 10000, singleTarget: true, recurs: false, keep: false},
		{num: 10, singleTarget: false, recurs: false, keep: false},
		{num: 10, singleTarget: true, recurs: false, keep: false},
		{num: 10000, singleTarget: false, recurs: true, keep: true},
		{num: 10000, singleTarget: true, recurs: true, keep: false},
		{num: 10, singleTarget: false, recurs: true, keep: true},
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
		} else {
			name += "/non-recurs"
		}
		if test.keep {
			name += "/keep-src"
		} else {
			name += "/remove-src"
		}
		name = name[1:]
		t.Run(name, test.do)
	}
}

func (test *prmTestPermut) do(t *testing.T) {
	const subdir = "subdir" // to promote recursively
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
	subdirFQN := filepath.Join(tempDir, subdir)
	err = cos.CreateDir(subdirFQN)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	// generate ngen files in tempDir and tempDir/subdir, respectively
	// (with total = 2 * ngen)
	ngen := to - from + 1
	tlog.Logf("Generating %d files...\n", ngen*2)
	cmd := fmt.Sprintf(genfiles, from, to, tempDir, tempDir, subdir)
	_, err = exec.Command("bash", "-c", cmd).CombinedOutput()

	tassert.CheckFatal(t, err)

	// prepare request
	args := api.PromoteArgs{
		BaseParams: baseParams,
		Bck:        m.bck,
		SrcFQN:     tempDir,
		Recursive:  test.recurs,
		KeepSrc:    test.keep,
	}
	if test.singleTarget {
		target, _ := m.smap.GetRandTarget()
		tlog.Logf("Promoting via %s\n", target.StringEx())
		args.Target = target.ID()
	}

	// promote
	err = api.Promote(&args)
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

	// perform checks
	cnt, cntsub := countFiles(t, tempDir)
	if test.keep {
		tassert.Errorf(t, cnt == ngen && cntsub == ngen, "keep == true: expected cnt %d == cntsub %d == num %d gererated",
			cnt, cntsub, ngen)
	}
	if test.recurs {
		tassert.Errorf(t, len(list.Entries) == ngen*2, "expected to recurs promote %d, got %d", ngen*2, len(list.Entries))
		if !test.keep {
			tassert.Errorf(t, cnt == 0 && cntsub == 0, "keep == false recurs: expected cnt %d == cntsub %d == 0",
				cnt, cntsub)
		}
	} else {
		tassert.Errorf(t, len(list.Entries) == ngen, "expected to promote %d, got %d", ngen, len(list.Entries))
		if !test.keep {
			tassert.Errorf(t, cnt == 0 && cntsub == ngen, "keep == false non-recurs: expected cnt %d == 0, cntsub %d == %d",
				cnt, cntsub, ngen)
		}
	}
}

func countFiles(t *testing.T, dir string) (n, nsubdir int) {
	f := func(path string, de iofs.DirEntry, err error) error {
		if err == nil && de.Type().IsRegular() {
			if filepath.Dir(path) == dir {
				n++
			} else {
				nsubdir++
			}
		}
		return nil
	}
	err := filepath.WalkDir(dir, f)
	tassert.CheckFatal(t, err)
	return
}
