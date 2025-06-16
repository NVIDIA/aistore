// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/ext/etl"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEtlMDDeepCopy(t *testing.T) {
	etlMD := newEtlMD()
	etlMD.Add(&etl.ETLSpecMsg{
		InitMsgBase: etl.InitMsgBase{
			EtlName:   "test-spec",
			CommTypeX: etl.Hpush,
		},
		Runtime: etl.RuntimeSpec{
			Image: "test-image",
		},
	})
	clone := etlMD.clone()
	s1 := string(cos.MustMarshal(etlMD))
	s2 := string(cos.MustMarshal(clone))
	if s1 == "" || s2 == "" || s1 != s2 {
		t.Log(s1)
		t.Log(s2)
		t.Fatal("marshal(etlmd) != marshal(clone(etlmd))")
	}
}

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
		config.Space = cmn.SpaceConf{
			LowWM: 75, HighWM: 90, OOS: 95,
		}
		config.LRU = cmn.LRUConf{
			DontEvictTime: cos.Duration(time.Second), CapacityUpdTime: cos.Duration(time.Minute), Enabled: true,
		}
		cmn.GCO.CommitUpdate(config)
		cfg = cmn.GCO.Get()

		etlMD = newEtlMD()
		for _, initType := range []string{etl.CodeType, etl.SpecType} {
			for i := range 5 {
				var msg etl.InitMsg
				if initType == etl.CodeType {
					msg = &etl.ETLSpecMsg{
						InitMsgBase: etl.InitMsgBase{
							EtlName:   "test-spec",
							CommTypeX: etl.Hpush,
						},
						Runtime: etl.RuntimeSpec{
							Image: "test-image",
						},
					}
				} else {
					msg = &etl.InitSpecMsg{
						InitMsgBase: etl.InitMsgBase{
							EtlName:   fmt.Sprintf("init-spec-%d", i),
							CommTypeX: etl.Hpush,
						},
						Spec: []byte(fmt.Sprintf("test spec - %d", i)),
					}
				}
				etlMD.Add(msg)
			}
		}
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

							// Add bucket and save.
							clone.Add(msg)
							err := jsp.Save(testpath, clone, opts, nil)
							Expect(err).NotTo(HaveOccurred())

							// Load elsewhere and check.
							loaded := newEtlMD()
							_, err = jsp.Load(testpath, loaded, opts)
							Expect(err).NotTo(HaveOccurred())
							Expect(loaded.Version).To(BeEquivalentTo(clone.Version))
							_, present := loaded.Get(msg.Name())
							Expect(present).To(BeTrue())
						}
					}
				}
			})
		})
	}
})
