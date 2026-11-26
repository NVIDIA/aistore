// Package integration_test.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TODO -- FIXME: randomize range, check prefix for `xs.iteratePrefix`
// NOTE:          `from` is a cloud bucket, if exists
func TestCopyMultiObjSimple(t *testing.T) {
	const (
		copyCnt   = 20
		objSize   = 128
		cksumType = cos.ChecksumCesXxh
	)
	var (
		objCnt     = 2345
		proxyURL   = tools.RandomProxyURL(t)
		bckFrom    cmn.Bck
		bckTo      = cmn.Bck{Name: "cp-range-to", Provider: apc.AIS}
		baseParams = tools.BaseAPIParams(proxyURL)
		xid        string
		err        error
		exists     bool
	)
	if cliBck.IsRemote() {
		if exists, _ = tools.BucketExists(nil, proxyURL, cliBck); exists {
			bckFrom = cliBck
			objCnt = 40
		}
	}
	if !exists {
		bckFrom = cmn.Bck{Name: "cp-range-from", Provider: apc.AIS}
		tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	}
	objList := make([]string, 0, objCnt)
	tlog.Logfln("exists = %t", exists)

	tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)
	for i := range objCnt {
		objList = append(objList, fmt.Sprintf("test/a-%04d", i))
	}
	for range 5 {
		tlog.Logfln("PUT %d => %s", len(objList), bckFrom.Cname(""))
		for _, objName := range objList {
			r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cksumType})
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bckFrom,
				ObjName:    objName,
				Reader:     r,
				Size:       objSize,
			})
			tassert.CheckFatal(t, err)
		}

		rangeStart := 10 // rand.IntN(objCnt - copyCnt - 1)
		template := "test/a-" + fmt.Sprintf("{%04d..%04d}", rangeStart, rangeStart+copyCnt-1)
		tlog.Logfln("[%s] %s => %s", template, bckFrom.Cname(""), bckTo.Cname(""))

		msg := cmn.TCOMsg{ToBck: bckTo}
		msg.Template = template
		xid, err = api.CopyMultiObj(baseParams, bckFrom, &msg)
		tassert.CheckFatal(t, err)
	}

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyObjects}
	api.WaitForXaction(baseParams, &wargs)

	tlog.Logln("prefix: test/")
	msg := &apc.LsoMsg{Prefix: "test/"}
	lst, err := api.ListObjects(baseParams, bckTo, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == copyCnt, "%d != %d", copyCnt, len(lst.Entries))
	rangeStart := 10 // rand.IntN(objCnt - copyCnt - 1)
	for i := rangeStart; i < rangeStart+copyCnt; i++ {
		objName := fmt.Sprintf("test/a-%04d", i)
		err := api.DeleteObject(baseParams, bckTo, objName)
		tassert.CheckError(t, err)
		tlog.Logfln("%s", bckTo.Cname(objName))
	}
}

// one target aborts with a local cause (each peer must get that cause)
func TestCopyMultiObjPeerAbort(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2, NonRoot: true})
	testCopyPeerAbort(t, apc.ActCopyObjects)
}

func TestCopyBucketPeerAbort(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2, Long: true, NonRoot: true})
	testCopyPeerAbort(t, apc.ActCopyBck)
}

func testCopyPeerAbort(t *testing.T, kind string) {
	const (
		numOwn     = 2  // source and copy on tsi (the abort starts on tsi)
		numPerPeer = 10 // source and copy on the peers (they still run at the abort)
		size       = cos.MiB
	)
	var (
		bckFrom  = cmn.Bck{Name: "cp-abort-from" + cos.GenTie(), Provider: apc.AIS}
		bckTo    = cmn.Bck{Name: "cp-abort-to" + cos.GenTie(), Provider: apc.AIS}
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		smap     = tools.GetClusterMap(t, proxyURL)
	)
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)
	tsi, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	objNames := copyAbortNames(t, smap, bckFrom, bckTo, tsi.ID(), numOwn, numPerPeer*(smap.CountActiveTs()-1))
	for _, objName := range objNames {
		r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{BaseParams: bp, Bck: bckFrom, ObjName: objName, Reader: r})
		tassert.CheckFatal(t, err)
	}
	mpathsReadOnly(t, bp, tsi, &bckTo, fs.ObjCT)

	var xid string
	if kind == apc.ActCopyObjects {
		msg := cmn.TCOMsg{ToBck: bckTo}
		msg.ObjNames = objNames
		xid, err = api.CopyMultiObj(bp, bckFrom, &msg)
	} else {
		xid, err = api.CopyBucket(bp, bckFrom, bckTo, &apc.TCBMsg{})
	}
	tassert.CheckFatal(t, err)
	args := &xact.ArgsMsg{ID: xid, Kind: kind, Timeout: 30 * time.Second}
	snaps, err := api.WaitForSnaps(bp, args, func(snaps xact.MultiSnap) (bool, bool, error) {
		return allTargetsFinished(snaps, smap.CountActiveTs()), false, nil
	})
	tassert.CheckFatal(t, err)

	checkPeerAbortCause(t, snaps, tsi, os.ErrPermission.Error())
	if xact.Table[kind].ICMode != xact.ICNone {
		checkICAbortCause(t, bp, args, snaps, tsi)
	}
}

// HRW-select names local to tid (abort origin) and names that skip tid (peers)
func copyAbortNames(t *testing.T, smap *meta.Smap, bckFrom, bckTo cmn.Bck, tid string, numOwn, numOther int) []string {
	var (
		from, to = meta.CloneBck(&bckFrom), meta.CloneBck(&bckTo)
		own      = make([]string, 0, numOwn)
		other    = make([]string, 0, numOther)
	)
	smap.InitDigests()
	for i := 0; len(own) < numOwn || len(other) < numOther; i++ {
		objName := fmt.Sprintf("cp-abort-%d", i)
		src, err := smap.HrwObjName2T(from, objName)
		tassert.CheckFatal(t, err)
		dst, err := smap.HrwObjName2T(to, objName)
		tassert.CheckFatal(t, err)
		switch {
		case src.ID() == tid && dst.ID() == tid && len(own) < numOwn:
			own = append(own, objName)
		case src.ID() != tid && dst.ID() != tid && len(other) < numOther:
			other = append(other, objName)
		}
	}
	return append(own, other...)
}

// tco calls DemandBase.Stop before Quiesce and each target must still count the 'done' of its peers
func TestCopyMultiObjQuiesce(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, MinTargets: 2})
	var (
		bckFrom  = cmn.Bck{Name: "cp-qui-from" + cos.GenTie(), Provider: apc.AIS}
		bckTo    = cmn.Bck{Name: "cp-qui-to" + cos.GenTie(), Provider: apc.AIS}
		m        = &ioContext{t: t, num: 100, fileSize: cos.KiB, fixedSize: true, bck: bckFrom}
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)
	m.puts()
	tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)

	const progressTimeout = time.Minute
	config := tools.GetClusterConfig(t)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"timeout.send_file_time": config.Timeout.SendFile.String()})
	})
	tools.SetClusterConfig(t, cos.StrKVs{"timeout.send_file_time": progressTimeout.String()})

	msg := cmn.TCOMsg{ToBck: bckTo}
	msg.ObjNames = m.objNames
	xid, err := api.CopyMultiObj(bp, bckFrom, &msg)
	tassert.CheckFatal(t, err)

	args := &xact.ArgsMsg{ID: xid, Kind: apc.ActCopyObjects, Timeout: 2*xact.IdleDefault + 2*progressTimeout}
	snaps, err := api.WaitForSnaps(bp, args, func(snaps xact.MultiSnap) (bool, bool, error) {
		return allTargetsFinished(snaps, m.smap.CountActiveTs()), false, nil
	})
	tassert.CheckFatal(t, err)

	var first, last time.Time
	for tid, tsnaps := range snaps {
		snap := tsnaps[0]
		tassert.Errorf(t, !snap.IsAborted(), "%s: %s aborted: %s", meta.Tname(tid), xid, snap.AbortErr)
		if first.IsZero() || snap.EndTime.Before(first) {
			first = snap.EndTime
		}
		if snap.EndTime.After(last) {
			last = snap.EndTime
		}
	}
	tlog.Logfln("%s: the targets finished within %v", xid, last.Sub(first))

	list, err := api.ListObjects(bp, bckTo, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == len(m.objNames), "expected %d objects in %s, got %d",
		len(m.objNames), bckTo.Cname(""), len(list.Entries))
	tassert.Errorf(t, last.Sub(first) < progressTimeout/2, "%s: the targets finished %v apart (progress timeout %v)",
		xid, last.Sub(first), progressTimeout)
}

// reuse one TCO while a peer sends into targets that already finished their local work.
func TestCopyMultiObjSlowPeer(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, MinTargets: 2})
	const batches = 3
	var (
		from = cmn.Bck{Name: "cp-slow-from" + cos.GenTie(), Provider: apc.AIS}
		to   = cmn.Bck{Name: "cp-slow-to" + cos.GenTie(), Provider: apc.AIS}
		purl = tools.RandomProxyURL(t)
		bp   = tools.BaseAPIParams(purl)
		smap = tools.GetClusterMap(t, purl)
	)
	props := &cmn.BpropsToSet{Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumCesXxh)}}
	tools.CreateBucket(t, purl, from, props, true /*cleanup*/)
	tools.CreateBucket(t, purl, to, props, true /*cleanup*/)
	slow, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	var receiver *meta.Snode
	for id, si := range smap.Tmap {
		if id != slow.ID() && !si.InMaintOrDecomm() {
			receiver = si
			break
		}
	}

	tassert.Fatalf(t, receiver != nil, "no receiver other than %s", slow.StringEx())

	perTarget, rl := frontendRateLim(smap, time.Second)
	batchSize := perTarget + 4
	names := make([]string, 0, batches*batchSize)
	smap.InitDigests()
	for i := 0; len(names) < cap(names); i++ {
		name := fmt.Sprintf("slow-%d", i)
		src, err := smap.HrwObjName2T(meta.CloneBck(&from), name)
		tassert.CheckFatal(t, err)
		dst, err := smap.HrwObjName2T(meta.CloneBck(&to), name)
		tassert.CheckFatal(t, err)
		if src.ID() == slow.ID() && dst.ID() == receiver.ID() {
			names = append(names, name)
			tassert.CheckFatal(t, tools.PutObjRR(bp, from, name, cos.KiB, cos.ChecksumCesXxh))
		}
	}
	_, err = api.SetBucketProps(bp, from, &cmn.BpropsToSet{RateLimit: rl})
	tassert.CheckFatal(t, err)

	var xid string
	for i := range batches {
		msg := cmn.TCOMsg{ToBck: to}
		msg.ObjNames = names[i*batchSize : (i+1)*batchSize]
		id, err := api.CopyMultiObj(bp, from, &msg)
		tassert.CheckFatal(t, err)
		if xid == "" {
			xid = id
		}
		tassert.Fatalf(t, id == xid, "expected shared TCO %s, got %s", xid, id)
	}

	wantTxns := fmt.Sprintf(" txns:%d/%d ", batches, batches)
	args := &xact.ArgsMsg{ID: xid, Kind: apc.ActCopyObjects, DaemonID: receiver.ID(), Timeout: time.Minute}
	snaps, err := api.WaitForSnaps(bp, args, func(snaps xact.MultiSnap) (bool, bool, error) {
		for _, snap := range snaps[receiver.ID()] {
			if snap.IsAborted() {
				return false, false, fmt.Errorf("%s aborted: %s", xid, snap.AbortErr)
			}
			if strings.Contains(snap.CtlMsg, wantTxns) {
				return true, false, nil
			}
		}
		return false, false, nil
	})
	tassert.CheckFatal(t, err)
	snap := snaps[receiver.ID()][0]
	tassert.Fatalf(t, !strings.Contains(snap.CtlMsg, "] ["), "finished local work retained: %s", snap.CtlMsg)
	tassert.Fatalf(t, snap.Stats.InObjs < int64(len(names)), "slow peer finished before receiver's local work: %s", snap.CtlMsg)
	tlog.Logfln("%s finished all local transactions with %d/%d incoming objects received", receiver.StringEx(), snap.Stats.InObjs, len(names))

	args.DaemonID = ""
	args.Timeout = 2 * time.Minute
	tassert.CheckFatal(t, api.WaitForXaction(bp, args))
	list, err := api.ListObjects(bp, to, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(list.Entries) == len(names), "expected %d copied objects, got %d", len(names), len(list.Entries))
	for _, name := range names {
		attrs, err := api.GetObjectWithValidation(bp, to, name, nil)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, attrs.Size() == cos.KiB, "unexpected size for %s: %d", name, attrs.Size())
	}
}

func TestCopyMultiObj(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		testCopyMobj(t, bck)
	})
}

func testCopyMobj(t *testing.T, bck *meta.Bck) {
	const objCnt = 200
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		m = ioContext{
			t:       t,
			bck:     bck.Clone(),
			num:     objCnt,
			prefix:  "copy-multiobj/",
			ordered: true,
		}
		bckTo     = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		numToCopy = min(m.num/2, 13)
		fmtRange  = "%s{%d..%d}"
		// test randomization
		maybe    = mono.NanoTime()&0x1 != 0
		maybeNot = !maybe
		//
		// total tests = (num runProviderTests) * (num subtests)
		//
		subtests = []struct {
			list           bool
			createDst      bool
			evictRemoteSrc bool
		}{
			{list: true, createDst: true},
			{list: false, createDst: true},

			{list: maybe, createDst: false},
			{list: maybeNot, createDst: false, evictRemoteSrc: true},
		}
	)
	for _, test := range subtests {
		tname := "list"
		if !test.list {
			tname = "range"
		}
		if test.createDst {
			tname += "/dst"
		} else {
			tname += "/no-dst"
		}
		if m.bck.IsRemote() && test.evictRemoteSrc {
			tname += "/evict-remote-src"
		}
		t.Run(tname, func(t *testing.T) {
			m.init(true /*cleanup*/)
			if m.bck.IsCloud() {
				defer m.del()
			}
			if !bckTo.Equal(&m.bck) && bckTo.IsAIS() {
				if test.createDst {
					tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)
				} else {
					t.Cleanup(func() {
						tools.DestroyBucket(t, proxyURL, bckTo)
					})
				}
			}

			m.puts()

			if m.bck.IsRemote() && test.evictRemoteSrc {
				tlog.Logfln("evicting %s", m.bck.String())
				//
				// evict all _cached_ data from the "local" cluster
				// keep the src bucket in the "local" BMD though
				//
				err := api.EvictRemoteBucket(baseParams, m.bck, true)
				tassert.CheckFatal(t, err)
			}

			tlog.Logfln("%s: %s => %s %d objects", t.Name(), m.bck.String(), bckTo.String(), numToCopy)
			var erv atomic.Value
			if test.list {
				for i := 0; i < numToCopy && erv.Load() == nil; i++ {
					list := make([]string, 0, numToCopy)
					flst := func(i int, list []string) {
						var (
							err error
							xid string
							msg = cmn.TCOMsg{ToBck: bckTo}
						)
						msg.ObjNames = list
						if m.bck.IsRemote() && test.evictRemoteSrc {
							xid, err = api.CopyMultiObj(baseParams, m.bck, &msg, apc.FltExists)
						} else {
							xid, err = api.CopyMultiObj(baseParams, m.bck, &msg)
						}
						if err != nil {
							erv.Store(err)
						} else {
							tlog.Logfln("[%s] %2d: cp list %d objects", xid, i, numToCopy)
						}
					}
					for range numToCopy {
						list = append(list, m.objNames[rand.IntN(m.num)])
					}
					if !test.createDst && i == 0 {
						// serialize the very first batch as it entails creating destination bucket
						// behind the scenes
						flst(i, list)
						time.Sleep(time.Second)
						continue
					}
					go flst(i, list)
				}
			} else {
				for i := 0; i < numToCopy && erv.Load() == nil; i++ {
					start := rand.IntN(m.num - numToCopy)
					ftmpl := func(start int, i int) {
						var (
							err      error
							xid      string
							template = fmt.Sprintf(fmtRange, m.prefix, start, start+numToCopy-1)
							msg      = cmn.TCOMsg{ToBck: bckTo}
						)
						msg.Template = template
						if m.bck.IsRemote() && test.evictRemoteSrc {
							xid, err = api.CopyMultiObj(baseParams, m.bck, &msg, apc.FltExists)
						} else {
							xid, err = api.CopyMultiObj(baseParams, m.bck, &msg)
						}
						if err != nil {
							erv.Store(err)
						} else {
							tlog.Logfln("[%s] %2d: cp range [%s]", xid, i, template)
						}
					}
					if !test.createDst && i == 0 {
						// (ditto serialize)
						ftmpl(start, i)
						time.Sleep(time.Second)
						continue
					}
					go ftmpl(start, i)
				}
			}
			if erv.Load() != nil {
				tassert.CheckFatal(t, erv.Load().(error))
			}
			wargs := xact.ArgsMsg{Kind: apc.ActCopyObjects, Bck: m.bck}
			api.WaitForXaction(baseParams, &wargs)

			msg := &apc.LsoMsg{Prefix: m.prefix}
			msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			objList, err := api.ListObjects(baseParams, bckTo, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tlog.Logfln("Total (`ls %s/%s*`): %d objects", bckTo.String(), m.prefix, len(objList.Entries))
		})
	}
}

// copy a remote bucket where a page of S3 dir markers lists empty but still
// has a continuation token
func TestCopyRemoteEmptyPages(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Bck: cliBck, RequiredCloudProvider: apc.AWS})

	const pageSize = 4
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		bckTo    = cmn.Bck{Name: "cp-empty-pages-" + trand.String(6), Provider: apc.AIS}
		prefix   = "cp-empty-pages-" + trand.String(6) + "/"
		keys     = make([]string, 0, 4*pageSize)
		objNames = make([]string, 0, 2*pageSize)
	)
	bprops, err := api.HeadBucket(bp, cliBck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)

	cfg, err := config.LoadDefaultConfig(t.Context(), loadCredentials(t), config.WithRegion(bprops.Extra.AWS.CloudRegion))
	tassert.CheckFatal(t, err)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := os.Getenv(env.AWSEndpoint); ep != "" {
			o.BaseEndpoint, o.UsePathStyle = aws.String(ep), true
		}
	})

	// small remote pages
	_, err = api.SetBucketProps(bp, cliBck, &cmn.BpropsToSet{
		Extra: &cmn.ExtraToSet{AWS: &cmn.ExtraPropsAWSToSet{MaxPageSize: apc.Ptr[int64](pageSize)}},
	})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		_, err := api.SetBucketProps(bp, cliBck, &cmn.BpropsToSet{
			Extra: &cmn.ExtraToSet{AWS: &cmn.ExtraPropsAWSToSet{MaxPageSize: apc.Ptr(bprops.Extra.AWS.MaxPageSize)}},
		})
		tassert.CheckError(t, err)
	})

	t.Cleanup(func() {
		for _, key := range keys {
			_, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(cliBck.Name), Key: aws.String(key)})
			tassert.CheckError(t, err)
		}
	})
	// one page per group: markers, objects, markers, objects
	for _, group := range []string{"a", "b", "c", "d"} {
		for i := range pageSize {
			name := fmt.Sprintf("%s%s-%d", prefix, group, i)
			if group == "a" || group == "c" {
				name += "/"
			} else {
				objNames = append(objNames, name)
			}
			_, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String(cliBck.Name), Key: aws.String(name), Body: strings.NewReader(""),
			})
			tassert.CheckFatal(t, err)
			keys = append(keys, name)
		}
	}

	t.Cleanup(func() { tools.DestroyBucket(t, proxyURL, bckTo) })
	msg := &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{Prefix: prefix}}
	xid, err := api.CopyBucket(bp, cliBck, bckTo, msg, apc.FltExists)
	tassert.CheckFatal(t, err)
	tlog.Logfln("copying %s => %s: %s", cliBck.Cname(prefix), bckTo.String(), xid)
	tassert.Fatalf(t, strings.HasPrefix(xid, xact.PrefixTcoID), "expected x-tco job, got %q", xid)

	err = api.WaitForSnapsIdle(bp, &xact.ArgsMsg{ID: xid, Timeout: tools.CopyBucketTimeout})
	tassert.CheckFatal(t, err)

	lst, err := api.ListObjects(bp, bckTo, &apc.LsoMsg{Prefix: prefix}, api.ListArgs{})
	tassert.CheckFatal(t, err)
	copied := make([]string, 0, len(lst.Entries))
	for _, en := range lst.Entries {
		copied = append(copied, en.Name)
	}
	slices.Sort(copied)
	tassert.Fatalf(t, slices.Equal(copied, objNames), "expected %v, got %v", objNames, copied)
}
