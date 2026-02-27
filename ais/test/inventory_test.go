// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

func TestCreateInventorySimple(t *testing.T) {
	var (
		m = &ioContext{
			t:      t,
			num:    20, // given PageSize (below)
			bck:    cliBck,
			prefix: t.Name(),
		}
		bp = tools.BaseAPIParams()
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1, RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)
	m.remotePuts(true /*evict*/)

	msg := &apc.CreateInvMsg{
		LsoMsg: apc.LsoMsg{
			Prefix:   m.prefix,
			Props:    apc.GetPropsName,
			PageSize: 3, // forces multiple pages
		},
	}

	xid, err := api.CreateBucketInventory(bp, m.bck, msg)
	tassert.CheckFatal(t, err)

	tlog.Logfln("%s[%s] started", apc.ActCreateNBI, xid)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: tools.ListRemoteBucketTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)
}

func TestCreateInventoryPermitateOnDisk(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1, RemoteBck: true, Bck: cliBck})

	type testCase struct {
		num           int
		pageSize      int64
		pagesPerChunk int64
		name          string
	}
	tests := []testCase{
		{num: 5, pageSize: 10, pagesPerChunk: 1, name: "few-objects-large-page"},
		{num: 20, pageSize: 3, pagesPerChunk: 10, name: "multi-page-one-chunk"},
		{num: 50, pageSize: 3, pagesPerChunk: 2, name: "multi-page-multi-chunk"},
		{num: 12, pageSize: 4, pagesPerChunk: 1, name: "exact-page-boundary"},
		{num: 40, pageSize: 2, pagesPerChunk: 2, name: "small-pages-many-chunks"},
		{num: 30, pageSize: 5, pagesPerChunk: 3, name: "medium-pages-medium-chunks"},
	}

	sysBck := meta.SysBckInv().Clone()

	proxyURL := tools.GetPrimaryURL()
	initMountpaths(t, proxyURL)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				m = &ioContext{
					t:      t,
					num:    tc.num,
					bck:    cliBck,
					prefix: t.Name(),
				}
				bp = tools.BaseAPIParams()
			)
			m.init(true /*cleanup*/)
			m.remotePuts(true /*evict*/)

			msg := &apc.CreateInvMsg{
				LsoMsg: apc.LsoMsg{
					Prefix:   m.prefix,
					Props:    apc.GetPropsName,
					PageSize: tc.pageSize,
				},
				PagesPerChunk: tc.pagesPerChunk,
			}

			xid, err := api.CreateBucketInventory(bp, m.bck, msg)
			tassert.CheckFatal(t, err)

			tlog.Logfln("%s[%s] started (%s: num=%d pageSize=%d ppc=%d)",
				apc.ActCreateNBI, xid, tc.name, tc.num, tc.pageSize, tc.pagesPerChunk)

			args := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: tools.ListRemoteBucketTimeout}
			_, err = api.WaitForXactionIC(bp, &args)
			tassert.CheckFatal(t, err)

			// expected chunks: ceil(num / (pageSize * pagesPerChunk))
			entriesPerChunk := tc.pageSize * tc.pagesPerChunk
			expectedChunks := int((int64(tc.num) + entriesPerChunk - 1) / entriesPerChunk)

			tlog.Logfln("expecting %d chunk(s)", expectedChunks)

			// validate chunks on disk (local deployments only)
			if expectedChunks > 1 {
				srcUname := string(m.bck.MakeUname(""))
				invObjName := srcUname + xid

				m.validateChunksOnDisk(sysBck, invObjName, expectedChunks)
			}
		})
	}
}
