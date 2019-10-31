// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func (m *bucketMD) LoadFromFS() error {
	mpath, err := fs.Mountpaths.MpathForMetadata()
	if err != nil {
		return err
	}
	bmdFullPath := filepath.Join(mpath.Path, bmdFname)
	return cmn.LocalLoad(bmdFullPath, m)
}

var _ = Describe("BMD marshal and unmarshal", func() {
	const (
		mpath = "/tmp"
	)

	var (
		bmd *bucketMD
	)

	BeforeEach(func() {
		// Set path for proxy (it uses Confdir)
		cfg := cmn.GCO.BeginUpdate()
		cfg.Confdir = mpath
		cmn.GCO.CommitUpdate(cfg)

		bmd = newBucketMD()
		for _, provider := range []string{cmn.AIS, cmn.Cloud} {
			for i := 0; i < 10; i++ {
				bmd.add(&cluster.Bck{
					Name:     fmt.Sprintf("local%d", i),
					Provider: provider,
				}, cmn.DefaultBucketProps())
			}
		}
	})

	for _, node := range []string{cmn.Target, cmn.Proxy} {
		Describe(node, func() {
			var bmdOwner bmdOwner

			BeforeEach(func() {
				bmdOwner = newBMDOwnerTgt()
				bmdOwner.put(bmd)
			})

			It(fmt.Sprintf("should correctly save and load bmd for %s", node), func() {
				savedBMD := newBucketMD()
				Expect(savedBMD.LoadFromFS()).NotTo(HaveOccurred())
				Expect(savedBMD.BMD).To(Equal(bmd.BMD))
			})

			It(fmt.Sprintf("should correctly save and check for incorrect data for %s", node), func() {
				bmdFullPath := filepath.Join(mpath, bmdFname)
				f, err := os.OpenFile(bmdFullPath, os.O_RDWR, 0)
				Expect(err).NotTo(HaveOccurred())
				_, err = f.WriteAt([]byte("xxxxxxxxxxxx"), 10)
				Expect(err).NotTo(HaveOccurred())
				Expect(f.Close()).NotTo(HaveOccurred())

				savedBMD := newBucketMD()
				Expect(savedBMD.LoadFromFS()).To(HaveOccurred())
			})
		})
	}
})
