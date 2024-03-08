// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/core/meta"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BMD marshal and unmarshal", func() {
	const (
		mpath    = "/tmp"
		testpath = "/tmp/.ais.test.bmd"
	)

	var (
		bmd *bucketMD
		cfg *cmn.Config
	)

	BeforeEach(func() {
		// Set path for proxy (it uses ConfigDir)
		config := cmn.GCO.BeginUpdate()
		config.ConfigDir = mpath
		config.Cksum.Type = cos.ChecksumXXHash
		config.Space = cmn.SpaceConf{
			LowWM: 75, HighWM: 90, OOS: 95,
		}
		config.LRU = cmn.LRUConf{
			DontEvictTime: cos.Duration(time.Second), CapacityUpdTime: cos.Duration(time.Minute), Enabled: true,
		}
		cmn.GCO.CommitUpdate(config)
		cfg = cmn.GCO.Get()

		bmd = newBucketMD()
		for _, provider := range []string{apc.AIS, apc.AWS} {
			for i := 0; i < 10; i++ {
				var hdr http.Header
				if provider != apc.AIS {
					hdr = http.Header{apc.HdrBackendProvider: []string{provider}}
				}

				var (
					bck   = meta.NewBck(fmt.Sprintf("bucket_%d", i), provider, cmn.NsGlobal)
					props = defaultBckProps(bckPropsArgs{bck: bck, hdr: hdr})
				)
				bmd.add(bck, props)
			}
		}
	})

	for _, node := range []string{apc.Target, apc.Proxy} {
		makeBMDOwner := func() bmdOwner {
			var bowner bmdOwner
			switch node {
			case apc.Target:
				bowner = newBMDOwnerTgt()
			case apc.Proxy:
				bowner = newBMDOwnerPrx(cfg)
			}
			return bowner
		}

		Describe(node, func() {
			var bowner bmdOwner

			BeforeEach(func() {
				bowner = makeBMDOwner()
				bowner.putPersist(bmd, nil)
			})

			It("should correctly load bmd for "+node, func() {
				bowner.init()
				Expect(bowner.Get()).To(Equal(&bmd.BMD))
			})

			It("should save and load bmd using jsp methods for "+node, func() {
				bowner.init()
				bmd := bowner.get()
				for _, signature := range []bool{false, true} {
					for _, compress := range []bool{false, true} {
						for _, checksum := range []bool{false, true} {
							opts := jsp.Options{
								Compress:  compress,
								Checksum:  checksum,
								Signature: signature,
							}
							clone := bmd.clone()
							bck := meta.NewBck("abc"+cos.GenTie(), apc.AIS, cmn.NsGlobal)

							// Add bucket and save.
							clone.add(bck, defaultBckProps(bckPropsArgs{bck: bck}))
							err := jsp.Save(testpath, clone, opts, nil)
							Expect(err).NotTo(HaveOccurred())

							// Load elsewhere and check.
							loaded := newBucketMD()
							_, err = jsp.Load(testpath, loaded, opts)
							Expect(err).NotTo(HaveOccurred())
							Expect(loaded.UUID).To(BeEquivalentTo(clone.UUID))
							Expect(loaded.Version).To(BeEquivalentTo(clone.Version))
							_, present := loaded.Get(bck)
							Expect(present).To(BeTrue())
						}
					}
				}
			})
		})
	}
})
