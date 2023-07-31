// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

const (
	ecObjLimit    = 256 * cos.KiB
	ecTime        = 10 * time.Minute
	rebalanceTime = 20 * time.Minute
)

type ecTest struct {
	name   string
	data   int
	parity int
}

var (
	ecTests = []ecTest{
		{"EC 1:1", 1, 1},
		{"EC 2:2", 2, 2},
	}
	objSizes = []int64{
		8 * cos.KiB, 128 * cos.KiB, // only replicas
		cos.MiB, 8 * cos.MiB, // only encoded
	}
)

func fillBucket(tb testing.TB, proxyURL string, bck cmn.Bck, objSize uint64, objCount int) {
	tlog.Logf("PUT %d objects of size %d into bucket %s...\n", objCount, objSize, bck)
	_, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
		ProxyURL:  proxyURL,
		Bck:       bck,
		ObjCnt:    objCount,
		ObjSize:   objSize,
		FixedSize: true,
		CksumType: cos.ChecksumXXHash,
	})
	tassert.CheckFatal(tb, err)
}

func BenchmarkECEncode(b *testing.B) {
	const (
		bckSize = cos.GiB
	)
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	for ecIdx, test := range ecTests {
		for szIdx, size := range objSizes {
			objCount := int(bckSize/size) + 1
			bck := cmn.Bck{
				Name:     fmt.Sprintf("bench-ec-enc-%d", len(objSizes)*ecIdx+szIdx),
				Provider: apc.AIS,
			}
			tools.CreateBucket(b, proxyURL, bck, nil, true /*cleanup*/)
			fillBucket(b, proxyURL, bck, uint64(size), objCount)

			b.Run(test.name, func(b *testing.B) {
				bckProps := &cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ObjSizeLimit: api.Int64(ecObjLimit),
						DataSlices:   api.Int(test.data),
						ParitySlices: api.Int(test.parity),
					},
				}
				_, err := api.SetBucketProps(baseParams, bck, bckProps)
				tassert.CheckFatal(b, err)

				reqArgs := xact.ArgsMsg{Kind: apc.ActECEncode, Bck: bck, Timeout: ecTime}
				_, err = api.WaitForXactionIC(baseParams, reqArgs)
				tassert.CheckFatal(b, err)
			})
		}
	}
}

func BenchmarkECRebalance(b *testing.B) {
	const (
		bckSize = 256 * cos.MiB
	)
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	for ecIdx, test := range ecTests {
		for szIdx, size := range objSizes {
			if size < 128*cos.KiB {
				continue
			}
			objCount := int(bckSize/size) + 1
			bck := cmn.Bck{
				Name:     fmt.Sprintf("bench-reb-%d", len(objSizes)*ecIdx+szIdx),
				Provider: apc.AIS,
			}
			tools.CreateBucket(b, proxyURL, bck, nil, true /*cleanup*/)

			smap, err := api.GetClusterMap(baseParams)
			tassert.CheckFatal(b, err)
			tgtLost, err := smap.GetRandTarget()
			tassert.CheckFatal(b, err)

			args := &apc.ActValRmNode{DaemonID: tgtLost.ID(), SkipRebalance: true}
			_, err = api.StartMaintenance(baseParams, args)
			tassert.CheckFatal(b, err)
			fillBucket(b, proxyURL, bck, uint64(size), objCount)

			b.Run("rebalance", func(b *testing.B) {
				bckProps := &cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ObjSizeLimit: api.Int64(ecObjLimit),
						DataSlices:   api.Int(test.data),
						ParitySlices: api.Int(test.parity),
					},
				}
				_, err := api.SetBucketProps(baseParams, bck, bckProps)
				tassert.CheckFatal(b, err)

				reqArgs := xact.ArgsMsg{Kind: apc.ActECEncode, Bck: bck, Timeout: ecTime}
				_, err = api.WaitForXactionIC(baseParams, reqArgs)
				tassert.CheckFatal(b, err)

				args := &apc.ActValRmNode{DaemonID: tgtLost.ID()}
				_, err = api.StopMaintenance(baseParams, args)
				tassert.CheckError(b, err)
				tools.WaitForRebalAndResil(b, baseParams, rebalanceTime)
			})
		}
	}
}

func BenchmarkRebalance(b *testing.B) {
	const (
		bckSize = cos.GiB
	)
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "bench-reb",
			Provider: apc.AIS,
		}
	)

	for _, size := range objSizes {
		objCount := int(bckSize/size) + 1
		tools.CreateBucket(b, proxyURL, bck, nil, true /*cleanup*/)

		smap, err := api.GetClusterMap(baseParams)
		tassert.CheckFatal(b, err)
		tgtLost, err := smap.GetRandTarget()
		tassert.CheckFatal(b, err)

		args := &apc.ActValRmNode{DaemonID: tgtLost.ID(), SkipRebalance: true}
		_, err = api.StartMaintenance(baseParams, args)
		tassert.CheckFatal(b, err)
		fillBucket(b, proxyURL, bck, uint64(size), objCount)

		b.Run("rebalance", func(b *testing.B) {
			args := &apc.ActValRmNode{DaemonID: tgtLost.ID()}
			_, err := api.StopMaintenance(baseParams, args)
			tassert.CheckError(b, err)
			tools.WaitForRebalAndResil(b, baseParams, rebalanceTime)
		})
	}
}
