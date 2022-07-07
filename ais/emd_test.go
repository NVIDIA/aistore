// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/etl"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEtlMDDeepCopy(t *testing.T) {
	etlMD := newEtlMD()
	etlMD.Add(&etl.InitCodeMsg{
		InitMsgBase: etl.InitMsgBase{
			IDX:       "init-code",
			CommTypeX: etl.PushCommType,
		},
		Code: []byte("print('hello')"),
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
		config.Cksum.Type = cos.ChecksumXXHash
		config.Space = cmn.SpaceConf{
			LowWM: 75, HighWM: 90, OOS: 95,
		}
		config.LRU = cmn.LRUConf{
			DontEvictTime: cos.Duration(time.Second), CapacityUpdTime: cos.Duration(time.Minute), Enabled: true,
		}
		cmn.GCO.CommitUpdate(config)
		cfg = cmn.GCO.Get()

		etlMD = newEtlMD()
		for _, initType := range []string{apc.ETLInitCode, apc.ETLInitSpec} {
			for i := 0; i < 5; i++ {
				var msg etl.InitMsg
				if initType == apc.ETLInitCode {
					msg = &etl.InitCodeMsg{
						InitMsgBase: etl.InitMsgBase{
							IDX:       fmt.Sprintf("init-code-%d", i),
							CommTypeX: etl.PushCommType,
						},
						Code: []byte(fmt.Sprintf("print('hello-%d')", i)),
					}
				} else {
					msg = &etl.InitSpecMsg{
						InitMsgBase: etl.InitMsgBase{
							IDX:       fmt.Sprintf("init-spec-%d", i),
							CommTypeX: etl.PushCommType,
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

			It(fmt.Sprintf("should correctly load etlMD for %s", node), func() {
				eowner.init()
				Expect(eowner.Get()).To(Equal(&etlMD.MD))
			})

			It(fmt.Sprintf("should save and load etlMD using jsp methods for %s", node), func() {
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
							msg := &etl.InitCodeMsg{
								InitMsgBase: etl.InitMsgBase{
									IDX:       "init-code-" + cos.GenTie(),
									CommTypeX: etl.PushCommType,
								},
								Code: []byte("print('hello')"),
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
							_, present := loaded.Get(msg.ID())
							Expect(present).To(BeTrue())
						}
					}
				}
			})

			It(fmt.Sprintf("should correctly detect etlMD corruption %s", node), func() {
				etlMDFullPath := filepath.Join(mpath, fname.Emd)
				f, err := os.OpenFile(etlMDFullPath, os.O_RDWR, 0)
				Expect(err).NotTo(HaveOccurred())
				_, err = f.WriteAt([]byte("xxxxxxxxxxxx"), 10)
				Expect(err).NotTo(HaveOccurred())
				Expect(f.Close()).NotTo(HaveOccurred())

				fmt.Println("NOTE: error on screen is expected at this point...")
				fmt.Println("")
				eowner = makeEtlMDOwner()
				eowner.init()

				Expect(eowner.Get()).NotTo(Equal(&etlMD.MD))
			})
		})
	}
})
