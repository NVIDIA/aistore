// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	iofs "io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

// TODO:
// - run under `runProviderTests`
// - use loopback dev-s
// - use nfs

type prmTests struct {
	num          int
	singleTarget bool
	recurs       bool
	deleteSrc    bool
	overwriteDst bool
	notFshare    bool
}

func TestPromote(t *testing.T) {
	tests := []prmTests{
		// short and long
		{num: 10000, singleTarget: false, recurs: false, deleteSrc: false, overwriteDst: false, notFshare: false},
		{num: 10000, singleTarget: false, recurs: true, deleteSrc: true, overwriteDst: false, notFshare: false},
		{num: 10, singleTarget: false, recurs: false, deleteSrc: true, overwriteDst: true, notFshare: false},
		// long
		{num: 10000, singleTarget: true, recurs: false, deleteSrc: false, overwriteDst: false, notFshare: false},
		{num: 10000, singleTarget: true, recurs: true, deleteSrc: false, overwriteDst: true, notFshare: false},
		{num: 10, singleTarget: true, recurs: false, deleteSrc: false, overwriteDst: false, notFshare: false},
		{num: 10, singleTarget: false, recurs: true, deleteSrc: false, overwriteDst: false, notFshare: false},
		{num: 10000, singleTarget: false, recurs: true, deleteSrc: true, overwriteDst: false, notFshare: true},
		{num: 10000, singleTarget: true, recurs: true, deleteSrc: false, overwriteDst: true, notFshare: true},
		{num: 10, singleTarget: false, recurs: true, deleteSrc: false, overwriteDst: false, notFshare: true},
	}
	if testing.Short() {
		tests = tests[0:3]
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
		if test.deleteSrc {
			name += "/delete-src"
		} else {
			name += "/keep-src"
		}
		if test.overwriteDst {
			name += "/overwrite-dst"
		} else {
			name += "/skip-existing-dst"
		}
		if test.notFshare {
			name += "/execute-autonomously"
		} else {
			name += "/collaborate-on-fshare"
		}
		name = name[1:]
		t.Run(name, test.do)
	}
}

// generate ngen files in tempdir and tempdir/subdir, respectively
var genfiles = `for f in {%d..%d}; do b=$RANDOM;
for i in {1..3}; do echo $b; done > %s/$f.test;
for i in {1..5}; do echo $b --- $b; done > %s/%s/$f.test.test;
done`

func (test *prmTests) generate(t *testing.T, from, to int, tempdir, subdir string) {
	tlog.Logf("Generating %d files...\n", test.num*2)
	cmd := fmt.Sprintf(genfiles, from, to, tempdir, tempdir, subdir)
	_, err := exec.Command("bash", "-c", cmd).CombinedOutput()
	tassert.CheckFatal(t, err)
}

func (test *prmTests) do(t *testing.T) {
	const subdir = "subdir" // to promote recursively
	var (
		m          = ioContext{t: t, bck: cmn.Bck{Provider: apc.ProviderAIS, Name: cos.RandString(10)}}
		from       = 10000
		to         = from + test.num - 1
		baseParams = tutils.BaseAPIParams()
	)
	m.initWithCleanupAndSaveState()
	tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	tempdir, err := os.MkdirTemp("", "prm")
	tassert.CheckFatal(t, err)
	subdirFQN := filepath.Join(tempdir, subdir)
	err = cos.CreateDir(subdirFQN)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(tempdir)
	})
	test.generate(t, from, to, tempdir, subdir)

	// prepare request
	args := api.PromoteArgs{
		BaseParams: baseParams,
		Bck:        m.bck,
		PromoteArgs: cluster.PromoteArgs{
			SrcFQN:         tempdir,
			Recursive:      test.recurs,
			OverwriteDst:   test.overwriteDst,
			DeleteSrc:      test.deleteSrc,
			SrcIsNotFshare: test.notFshare,
		},
	}
	var target *cluster.Snode
	if test.singleTarget {
		target, _ = m.smap.GetRandTarget()
		tlog.Logf("Promoting via %s\n", target.StringEx())
		args.DaemonID = target.ID()
	}

	// (I) do
	xactID, err := api.Promote(&args)
	tassert.CheckFatal(t, err)

	// wait and then collect snaps
	locObjs, outObjs, inObjs := test.wait(t, xactID, tempdir, target, &m)

	// list
	tlog.Logln("Listing and counting...")
	list, err := api.ListObjects(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)

	//
	// run checks
	//
	cnt, cntsub := countFiles(t, tempdir)
	if !test.deleteSrc {
		tassert.Errorf(t, cnt == test.num && cntsub == test.num,
			"delete-src == false: expected cnt (%d) == cntsub (%d) == num (%d) gererated",
			cnt, cntsub, test.num)
	}
	expNum, s := test.num, ""
	if test.recurs {
		expNum = test.num * 2
		s = " recursively"
	}

	// num promoted
	tassert.Errorf(t, len(list.Entries) == expNum, "expected to%s promote %d, got %d", s, test.num*2, len(list.Entries))

	// delete source
	if test.deleteSrc {
		if test.recurs {
			tassert.Errorf(t, cnt == 0 && cntsub == 0,
				"delete-src == true, recursive: expected cnt (%d) == cntsub (%d) == 0",
				cnt, cntsub)
		} else {
			tassert.Errorf(t, cnt == 0 && cntsub == test.num,
				"delete-src == true, non-recursive: expected cnt (%d) == 0 and cntsub (%d) == (%d)",
				cnt, cntsub, test.num)
		}
	}
	// vs xaction stats
	if xactID != "" {
		if test.singleTarget {
			tassert.Errorf(t, locObjs+outObjs == int64(expNum),
				"single-target promote: expected sum(loc+out)==%d, got (%d + %d)=%d",
				expNum, locObjs, outObjs, locObjs+outObjs)
		} else if !test.notFshare {
			tassert.Errorf(t, int(locObjs) == expNum && int(inObjs) == 0 && int(outObjs) == 0,
				"file share: expected each target to handle the entire content locally, got (loc, out, in) = (%d, %d, %d)",
				locObjs, outObjs, inObjs)
		}
	}

	// (II) do more when _not_ overwriting destination, namely:
	// delete a few promoted objects, and then immediately
	// promote them again from the original (non-deleted) source
	if test.overwriteDst || test.deleteSrc {
		return
	}
	numDel := cos.Max(len(list.Entries)/100, 2)
	idx := rand.Intn(len(list.Entries))
	if idx+numDel >= len(list.Entries) {
		idx = cos.Max(0, len(list.Entries)-numDel)
		tassert.Fatalf(t, idx+numDel < len(list.Entries), "unexpected test configuration (???)")
	}
	tlog.Logf("Deleting %d random objects\n", numDel)
	for i := 0; i < numDel; i++ {
		name := list.Entries[idx+i].Name
		err := api.DeleteObject(baseParams, m.bck, name)
		tassert.CheckFatal(t, err)
	}

	// do
	xactID, err = api.Promote(&args)
	tassert.CheckFatal(t, err)

	locObjs, outObjs, inObjs = test.wait(t, xactID, tempdir, target, &m)

	// list
	tlog.Logln("Listing and counting the 2nd time...")
	list, err = api.ListObjects(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)

	// num promoted
	tassert.Errorf(t, len(list.Entries) == expNum, "expected to%s promote %d, got %d", s, test.num*2, len(list.Entries))

	// xaction stats versus `numDel` - but note:
	// other than the selected few objects that were deleted prior to promoting the 2nd time,
	// all the rest already exists and is not expected to "show up" in the stats
	if xactID != "" {
		if test.singleTarget {
			tassert.Errorf(t, locObjs+outObjs == int64(numDel),
				"single-target promote: expected sum(loc+out)==%d, got (%d + %d)=%d",
				expNum, locObjs, outObjs, locObjs+outObjs)
		} else if !test.notFshare {
			tassert.Errorf(t, int(locObjs) == numDel && int(inObjs) == 0 && int(outObjs) == 0,
				"file share: expected each target to handle the entire content locally, got (loc, out, in) = (%d, %d, %d)",
				locObjs, outObjs, inObjs)
		}
	}
}

// wait for an xaction (if there's one) and then query all targets for stats
func (test *prmTests) wait(t *testing.T, xactID, tempdir string, target *cluster.Snode, m *ioContext) (locObjs, outObjs, inObjs int64) {
	time.Sleep(2 * time.Second)
	xargs := api.XactReqArgs{Kind: apc.ActPromote, Timeout: rebalanceTimeout}
	xname := fmt.Sprintf("%q", apc.ActPromote)
	if xactID != "" {
		xargs.ID = xactID
		xname = fmt.Sprintf("x-%s[%s]", apc.ActPromote, xactID)
		tassert.Errorf(t, cos.IsValidUUID(xactID), "expecting valid x-UUID %q", xactID)
	}

	// wait "cases" 1. through 3.
	if xactID != "" && !test.singleTarget { // 1. cluster-wide xaction
		tlog.Logf("Waiting for global %s(%s=>%s)\n", xname, tempdir, m.bck)
		notifStatus, err := api.WaitForXactionIC(baseParams, xargs)
		tassert.CheckFatal(t, err)
		if notifStatus != nil && (notifStatus.AbortedX || notifStatus.ErrMsg != "") {
			tlog.Logf("Warning: notif-status: %+v\n", notifStatus)
		}
	} else if xactID != "" && test.singleTarget { // 2. single-target xaction
		xargs.DaemonID = target.ID()
		tlog.Logf("Waiting for %s(%s=>%s) at %s\n", xname, tempdir, m.bck, target.StringEx())
		err := api.WaitForXactionNode(baseParams, xargs, xactSnapNotRunning)
		tassert.CheckFatal(t, err)
	} else { // 3. synchronous execution
		tlog.Logf("Promoting without xaction (%s=>%s)\n", tempdir, m.bck)
	}

	// collect stats
	snaps, err := api.QueryXactionSnaps(baseParams, xargs)
	tassert.CheckFatal(t, err)
	locObjs, outObjs, inObjs = snaps.ObjCounts()
	tlog.Logf("%s: (loc, out, in) = (%d, %d, %d)\n", xname, locObjs, outObjs, inObjs)
	return
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
