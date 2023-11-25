// Package integration_test.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

// TODO -- FIXME: randomize range, check prefix for `xs.iteratePrefix`
// NOTE:          `from` is a cloud bucket, if exists
func TestCopyMultiObjSimple(t *testing.T) {
	const (
		copyCnt   = 20
		objSize   = 128
		cksumType = cos.ChecksumXXHash
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
	tlog.Logf("exists = %t\n", exists)

	tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)
	for i := 0; i < objCnt; i++ {
		objList = append(objList, fmt.Sprintf("test/a-%04d", i))
	}
	for i := 0; i < 5; i++ {
		tlog.Logf("PUT %d => %s\n", len(objList), bckFrom.Cname(""))
		for _, objName := range objList {
			r, _ := readers.NewRand(objSize, cksumType)
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bckFrom,
				ObjName:    objName,
				Reader:     r,
				Size:       objSize,
			})
			tassert.CheckFatal(t, err)
		}

		rangeStart := 10 // rand.Intn(objCnt - copyCnt - 1)
		template := "test/a-" + fmt.Sprintf("{%04d..%04d}", rangeStart, rangeStart+copyCnt-1)
		tlog.Logf("[%s] %s => %s\n", template, bckFrom.Cname(""), bckTo.Cname(""))

		msg := cmn.TCObjsMsg{ToBck: bckTo}
		msg.Template = template
		xid, err = api.CopyMultiObj(baseParams, bckFrom, &msg)
		tassert.CheckFatal(t, err)
	}

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyObjects}
	api.WaitForXactionIdle(baseParams, &wargs)

	tlog.Logln("prefix: test/")
	msg := &apc.LsoMsg{Prefix: "test/"}
	lst, err := api.ListObjects(baseParams, bckTo, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == copyCnt, "%d != %d", copyCnt, len(lst.Entries))
	rangeStart := 10 // rand.Intn(objCnt - copyCnt - 1)
	for i := rangeStart; i < rangeStart+copyCnt; i++ {
		objName := fmt.Sprintf("test/a-%04d", i)
		err := api.DeleteObject(baseParams, bckTo, objName)
		tassert.CheckError(t, err)
		tlog.Logf("%s\n", bckTo.Cname(objName))
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
				tlog.Logf("evicting %s\n", m.bck)
				//
				// evict all _cached_ data from the "local" cluster
				// keep the src bucket in the "local" BMD though
				//
				err := api.EvictRemoteBucket(baseParams, m.bck, true)
				tassert.CheckFatal(t, err)
			}

			tlog.Logf("%s: %s => %s %d objects\n", t.Name(), m.bck, bckTo, numToCopy)
			var erv atomic.Value
			if test.list {
				for i := 0; i < numToCopy && erv.Load() == nil; i++ {
					list := make([]string, 0, numToCopy)
					flst := func(i int, list []string) {
						var (
							err error
							xid string
							msg = cmn.TCObjsMsg{ToBck: bckTo}
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
							tlog.Logf("[%s] %2d: cp list %d objects\n", xid, i, numToCopy)
						}
					}
					for j := 0; j < numToCopy; j++ {
						list = append(list, m.objNames[rand.Intn(m.num)])
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
					start := rand.Intn(m.num - numToCopy)
					ftmpl := func(start int, i int) {
						var (
							err      error
							xid      string
							template = fmt.Sprintf(fmtRange, m.prefix, start, start+numToCopy-1)
							msg      = cmn.TCObjsMsg{ToBck: bckTo}
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
							tlog.Logf("[%s] %2d: cp range [%s]\n", xid, i, template)
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
			api.WaitForXactionIdle(baseParams, &wargs)

			msg := &apc.LsoMsg{Prefix: m.prefix}
			msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			objList, err := api.ListObjects(baseParams, bckTo, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tlog.Logf("Total (`ls %s/%s*`): %d objects\n", bckTo, m.prefix, len(objList.Entries))
		})
	}
}
