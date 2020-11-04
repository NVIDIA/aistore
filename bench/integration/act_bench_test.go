// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

const (
	ecObjLimit    = 256 * cmn.KiB
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
		8 * cmn.KiB, 128 * cmn.KiB, // only replicas
		cmn.MiB, 8 * cmn.MiB, // only encoded
	}
)

func fillBucket(b *testing.B, proxyURL string, bck cmn.Bck, objSize uint64, objCount int) {
	var (
		filenameCh = make(chan string, objCount)
		errCh      = make(chan error, objCount)
	)
	tutils.Logf("PUT %d objects of size %d into bucket %s...\n", objCount, objSize, bck)
	tutils.PutRandObjs(proxyURL, bck, "", objSize, objCount, errCh, filenameCh, cmn.ChecksumXXHash, true)
	close(filenameCh)
	close(errCh)
	tassert.SelectErr(b, errCh, "put", true)
}

func BenchmarkECEncode(b *testing.B) {
	const (
		bckSize = cmn.GiB
	)
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	for ecIdx, test := range ecTests {
		for szIdx, size := range objSizes {
			objCount := int(bckSize/size) + 1
			bck := cmn.Bck{
				Name:     fmt.Sprintf("bench-ec-enc-%d", len(objSizes)*ecIdx+szIdx),
				Provider: cmn.ProviderAIS,
			}
			tutils.CreateFreshBucket(b, proxyURL, bck)
			defer tutils.DestroyBucket(b, proxyURL, bck)
			fillBucket(b, proxyURL, bck, uint64(size), objCount)

			b.Run(test.name, func(b *testing.B) {
				bckProps := cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ObjSizeLimit: api.Int64(ecObjLimit),
						DataSlices:   api.Int(test.data),
						ParitySlices: api.Int(test.parity),
					},
				}
				_, err := api.SetBucketProps(baseParams, bck, bckProps)
				tassert.CheckFatal(b, err)

				reqArgs := api.XactReqArgs{Kind: cmn.ActECEncode, Bck: bck, Latest: false, Timeout: ecTime}
				_, err = api.WaitForXaction(baseParams, reqArgs)
				tassert.CheckFatal(b, err)
			})
		}
	}
}

func BenchmarkECRebalance(b *testing.B) {
	const (
		bckSize = 256 * cmn.MiB
	)
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	for ecIdx, test := range ecTests {
		for szIdx, size := range objSizes {
			if size < 128*cmn.KiB {
				continue
			}
			objCount := int(bckSize/size) + 1
			bck := cmn.Bck{
				Name:     fmt.Sprintf("bench-reb-%d", len(objSizes)*ecIdx+szIdx),
				Provider: cmn.ProviderAIS,
			}
			tutils.CreateFreshBucket(b, proxyURL, bck)
			defer tutils.DestroyBucket(b, proxyURL, bck)

			smap, err := api.GetClusterMap(baseParams)
			tassert.CheckFatal(b, err)
			tgtLost, err := smap.GetRandTarget()
			tassert.CheckFatal(b, err)

			args := &cmn.ActValDecommision{DaemonID: tgtLost.ID(), SkipRebalance: true}
			err = tutils.UnregisterNode(proxyURL, args)
			tassert.CheckFatal(b, err)
			fillBucket(b, proxyURL, bck, uint64(size), objCount)

			b.Run("rebalance", func(b *testing.B) {
				bckProps := cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ObjSizeLimit: api.Int64(ecObjLimit),
						DataSlices:   api.Int(test.data),
						ParitySlices: api.Int(test.parity),
					},
				}
				_, err := api.SetBucketProps(baseParams, bck, bckProps)
				tassert.CheckFatal(b, err)

				reqArgs := api.XactReqArgs{Kind: cmn.ActECEncode, Bck: bck, Latest: false, Timeout: ecTime}
				_, err = api.WaitForXaction(baseParams, reqArgs)
				tassert.CheckFatal(b, err)

				_, err = tutils.JoinCluster(proxyURL, tgtLost)
				tassert.CheckError(b, err)
				tutils.WaitForRebalanceToComplete(b, baseParams, rebalanceTime)
			})
		}
	}
}

func BenchmarkRebalance(b *testing.B) {
	const (
		bckSize = cmn.GiB
	)
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "bench-reb",
			Provider: cmn.ProviderAIS,
		}
	)

	for _, size := range objSizes {
		objCount := int(bckSize/size) + 1
		tutils.CreateFreshBucket(b, proxyURL, bck)
		defer tutils.DestroyBucket(b, proxyURL, bck)

		smap, err := api.GetClusterMap(baseParams)
		tassert.CheckFatal(b, err)
		tgtLost, err := smap.GetRandTarget()
		tassert.CheckFatal(b, err)

		args := &cmn.ActValDecommision{DaemonID: tgtLost.ID(), SkipRebalance: true}
		err = tutils.UnregisterNode(proxyURL, args)
		tassert.CheckFatal(b, err)
		fillBucket(b, proxyURL, bck, uint64(size), objCount)

		b.Run("rebalance", func(b *testing.B) {
			_, err = tutils.JoinCluster(proxyURL, tgtLost)
			tassert.CheckError(b, err)
			tutils.WaitForRebalanceToComplete(b, baseParams, rebalanceTime)
		})
	}
}
