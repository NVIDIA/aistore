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
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

// TODO: stress notFshare

const subdir = "subdir" // to promote recursively

type prmTests struct {
	num          int
	singleTarget bool
	recurs       bool
	deleteSrc    bool
	overwriteDst bool
	notFshare    bool
}

// flow: TestPromote (tests) => runProvider x (provider tests) => test.do(bck)
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
	// see also "filtering" below
	if testing.Short() {
		tests = tests[0:3]
	}
	for _, test := range tests {
		var name string
		if test.num < 32 {
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
		t.Run(name, func(t *testing.T) { runProviderTests(t, test.do) })
	}
}

// generate ngen files in tempdir and tempdir/subdir, respectively
var genfiles = `for f in {%d..%d}; do b=$RANDOM;
for i in {1..3}; do echo $b; done > %s/$f.test;
for i in {1..5}; do echo $b --- $b; done > %s/%s/$f.test.test;
done`

func (test *prmTests) generate(t *testing.T, from, to int, tempdir, subdir string) {
	tlog.Logf("Generating %d (%d + %d) files...\n", test.num*2, test.num, test.num)
	cmd := fmt.Sprintf(genfiles, from, to, tempdir, tempdir, subdir)
	_, err := exec.Command("bash", "-c", cmd).CombinedOutput()
	tassert.CheckFatal(t, err)
}

func (test *prmTests) do(t *testing.T, bck *cluster.Bck) {
	if bck.IsCloud() {
		// NOTE: filtering out some test permutations to save time
		if testing.Short() {
			fmt := "%s is cloud bucket"
			tools.ShortSkipf(t, fmt, bck)
		}
		if strings.Contains(t.Name(), "few-files") ||
			strings.Contains(t.Name(), "single-target") ||
			strings.Contains(t.Name(), "recurs") {
			t.Skipf("skipping %s for Cloud bucket", t.Name())
		}

		// also, reducing the number of files to promote
		test.num = cos.Min(test.num, 50)
	}

	var (
		m          = ioContext{t: t, bck: bck.Clone()}
		from       = 10000
		to         = from + test.num - 1
		baseParams = tools.BaseAPIParams()
	)
	m.saveCluState(m.proxyURL)

	tempdir, err := os.MkdirTemp("", "prm")
	tassert.CheckFatal(t, err)
	subdirFQN := filepath.Join(tempdir, subdir)
	err = cos.CreateDir(subdirFQN)
	tassert.CheckFatal(t, err)

	if m.bck.IsRemote() {
		m.del()
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(tempdir)
		if m.bck.IsRemote() {
			m.del()
		}
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

	// wait for the operation to finish and collect stats
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

	// num promoted
	expNum, s := test.num, ""
	if test.recurs {
		expNum = test.num * 2
		s = " recursively"
	}
	tassert.Fatalf(t, len(list.Entries) == expNum, "expected to%s promote %d files, got %d", s, expNum, len(list.Entries))

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
	tlog.Logln("Running test case _not_ to overwrite destination...")
	l := len(list.Entries)
	numDel := cos.Max(l/100, 2)
	idx := rand.Intn(l)
	if idx+numDel >= l {
		if numDel >= l {
			idx, numDel = 0, l
		} else {
			idx = l - numDel
		}
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
	time.Sleep(4 * time.Second)
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
	xs, err := api.QueryXactionSnaps(baseParams, xargs)
	tassert.CheckFatal(t, err)
	if xactID != "" {
		locObjs, outObjs, inObjs = xs.ObjCounts(xactID)
		tlog.Logf("%s[%s]: (loc, out, in) = (%d, %d, %d)\n", xname, xactID, locObjs, outObjs, inObjs)
		return
	}
	uuids := xs.GetUUIDs()
	for _, xactID := range uuids {
		locObjs, outObjs, inObjs = xs.ObjCounts(xactID)
		tlog.Logf("%s[%s]: (loc, out, in) = (%d, %d, %d)\n", xname, xactID, locObjs, outObjs, inObjs)
	}
	return 0, 0, 0
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
