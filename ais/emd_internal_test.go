// Package ais: internal unit tests
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/ext/etl"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EtlMD marshal and unmarshal", func() {
	const (
		mpath    = "/tmp"
		testpath = "/tmp/.ais.test.etlMD"
	)

	var (
		etlMD *etlMD
		cfg   *cmn.Config
	)

	BeforeEach(func() {
		// Set path for proxy (it uses ConfigDir)
		config := cmn.GCO.BeginUpdate()
		config.ConfigDir = mpath
		config.Cksum.Type = cos.ChecksumOneXxh
		config.Space = &cmn.SpaceConf{
			LowWM: 75, HighWM: 90, OOS: 95,
		}
		config.LRU = &cmn.LRUConf{
			DontEvictTime: cos.Duration(time.Hour), CapacityUpdTime: cos.Duration(time.Minute), Enabled: true,
		}
		cmn.GCO.CommitUpdate(config)
		cmn.Rom.Set(&config.ClusterConfig)

		cfg = cmn.GCO.Get()

		etlMD = newEtlMD()
		msg := &etl.ETLSpecMsg{
			InitMsgBase: etl.InitMsgBase{
				EtlName:     "runtime-spec",
				CommTypeX:   etl.Hpush,
				InitTimeout: cos.Duration(etl.DefaultInitTimeout),
				ObjTimeout:  cos.Duration(etl.DefaultObjTimeout),
			},
			Runtime: etl.RuntimeSpec{Image: "test-runtime-image"},
		}
		etlMD.Add(msg, etl.Running, make(etl.PodMap, 4)) // Running stage expects no-nil pod map
	})

	for _, node := range []string{apc.Target, apc.Proxy} {
		makeEtlMDOwner := func() etlOwner {
			var eowner etlOwner
			switch node {
			case apc.Target:
				eowner = newEtlMDOwnerTgt()
			case apc.Proxy:
				eowner = newEtlMDOwnerPrx(cfg)
			}
			return eowner
		}

		Describe(node, func() {
			var eowner etlOwner

			BeforeEach(func() {
				eowner = makeEtlMDOwner()
				eowner.putPersist(etlMD, nil)
			})

			It("should correctly load etlMD for "+node, func() {
				eowner.init()
				Expect(eowner.Get()).To(Equal(&etlMD.MD))
			})

			It("should save and load etlMD using jsp methods for "+node, func() {
				eowner.init()
				etlMD := eowner.get()
				for _, signature := range []bool{false, true} {
					for _, compress := range []bool{false, true} {
						for _, checksum := range []bool{false, true} {
							opts := jsp.Options{
								Compress:  compress,
								Checksum:  checksum,
								Signature: signature,
							}
							clone := etlMD.clone()
							msg := &etl.ETLSpecMsg{
								InitMsgBase: etl.InitMsgBase{
									EtlName:   "test-spec",
									CommTypeX: etl.Hpush,
								},
								Runtime: etl.RuntimeSpec{
									Image: "test-image",
								},
							}

							// Add and save.
							preVersion := clone.version()
							clone.add(msg, etl.Running, make(etl.PodMap, 4)) // Running stage expects no-nil pod map
							err := jsp.Save(testpath, clone, opts, nil)
							Expect(err).NotTo(HaveOccurred())

							// Load elsewhere and check.
							loaded := newEtlMD()
							_, err = jsp.Load(testpath, loaded, opts)
							Expect(err).NotTo(HaveOccurred())
							Expect(loaded.Version).To(BeEquivalentTo(clone.Version))
							Expect(loaded.Version - 1).To(BeEquivalentTo(preVersion)) // Version should be incremented
							_, present := loaded.Get(msg.Name())
							Expect(present).To(BeTrue())

							// Delete and save.
							preVersion = clone.version()
							clone.del(msg.Name())
							err = jsp.Save(testpath, clone, opts, nil)
							Expect(err).NotTo(HaveOccurred())

							// Load elsewhere and check.
							loaded = newEtlMD()
							_, err = jsp.Load(testpath, loaded, opts)
							Expect(err).NotTo(HaveOccurred())
							Expect(loaded.Version).To(BeEquivalentTo(clone.Version))
							Expect(loaded.Version - 1).To(BeEquivalentTo(preVersion)) // Version should be incremented
							_, present = loaded.Get(msg.Name())
							Expect(present).To(BeFalse())
						}
					}
				}
			})
		})
	}
})
