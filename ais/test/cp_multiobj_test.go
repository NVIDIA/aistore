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
	cfg, err := config.LoadDefaultConfig(t.Context(), loadCredentials(t), config.WithRegion(env.AwsDefaultRegion()))
	tassert.CheckFatal(t, err)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := os.Getenv(env.AWSEndpoint); ep != "" {
			o.BaseEndpoint, o.UsePathStyle = aws.String(ep), true
		}
	})

	bprops, err := api.HeadBucket(bp, cliBck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)

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
