// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

const (
	testIP            = "127.0.0.1"
	testDir           = "/tmp/" + cmn.DSortNameLowercase + "_tests"
	testBucket        = cmn.DSortNameLowercase + "_tests"
	globalManagerUUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
)

// interface guard
var (
	_ extract.Creator       = (*extractCreatorMock)(nil)
	_ cluster.SmapListeners = (*testSmapListeners)(nil)
)

//
// MISC FUNCTIONS
//
func getFreePorts(count int) ([]int, error) {
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return nil, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

//
// TEST SMAP
//
type testSmapListeners struct {
	sync.RWMutex
}

func (*testSmapListeners) Reg(cluster.Slistener)   {}
func (*testSmapListeners) Unreg(cluster.Slistener) {}

type testSmap struct {
	*cluster.Smap
	a *testSmapListeners
}

func newTestSmap(targets ...string) *testSmap {
	smap := &testSmap{
		&cluster.Smap{},
		&testSmapListeners{},
	}
	smap.Tmap = make(cluster.NodeMap)
	for _, target := range targets {
		smap.Tmap[target] = &cluster.Snode{}
	}
	return smap
}

func (tm *testSmap) addTarget(si *cluster.Snode) {
	tm.Tmap[si.DaemonID] = si
}

func (tm *testSmap) Get() *cluster.Smap {
	return tm.Smap
}

func (tm *testSmap) Listeners() cluster.SmapListeners {
	return tm.a
}

//
// MOCKS
//
type extractCreatorMock struct {
	useCompression bool
	createShard    func(s *extract.Shard, w io.Writer, loadContent extract.LoadContentFunc) // func to hijack CreateShard function
}

func (*extractCreatorMock) ExtractShard(*cluster.LOM, cos.ReadReaderAt, extract.RecordExtractor, bool) (int64, int, error) {
	return 0, 0, nil
}

func (ec *extractCreatorMock) CreateShard(s *extract.Shard, w io.Writer, loadContent extract.LoadContentFunc) (int64, error) {
	ec.createShard(s, w, loadContent)
	return 0, nil
}
func (*extractCreatorMock) SupportsOffset() bool      { return true }
func (ec *extractCreatorMock) UsingCompression() bool { return ec.useCompression }
func (*extractCreatorMock) MetadataSize() int64       { return 0 }

type targetNodeMock struct {
	daemonID  string
	mux       *mux.ServeMux
	s         *http.Server
	controlCh chan error
	managers  *ManagerGroup
}

type testContext struct {
	targetCnt int
	targets   []*targetNodeMock
	smap      *testSmap
	errCh     chan error
	wg        *sync.WaitGroup
}

func newTargetMock(daemonID string, smap *testSmap) *targetNodeMock {
	// Initialize dSort manager
	rs := &ParsedRequestSpec{
		Extension: cos.ExtTar,
		Algorithm: &SortAlgorithm{
			FormatType: extract.FormatTypeString,
		},
		MaxMemUsage: cos.ParsedQuantity{Type: cos.QuantityPercent, Value: 0},
		DSorterType: DSorterGeneralType,
	}

	db := dbdriver.NewDBMock()
	dsortManagers := NewManagerGroup(db, true /*skip hk*/)
	dsortManager, err := dsortManagers.Add(globalManagerUUID)
	Expect(err).ShouldNot(HaveOccurred())
	ctx.node = smap.Get().Tmap[daemonID]
	dsortManager.init(rs)
	dsortManager.unlock()

	net := smap.GetTarget(daemonID).PublicNet
	return &targetNodeMock{
		daemonID: daemonID,
		s: &http.Server{
			Addr:    fmt.Sprintf("%s:%s", net.NodeHostname, net.DaemonPort),
			Handler: http.NewServeMux(),
		},
		controlCh: make(chan error, 1),
		managers:  dsortManagers,
	}
}

func (t *targetNodeMock) setHandlers(handlers map[string]http.HandlerFunc) {
	mux := mux.NewServeMux()
	for path, handler := range handlers {
		mux.HandleFunc(path, handler)
	}

	t.mux = mux
	t.s.Handler = mux
}

func (t *targetNodeMock) setup() {
	// set default handlers
	defaultHandlers := map[string]http.HandlerFunc{
		cmn.URLPathdSortRecords.S + "/": func(w http.ResponseWriter, r *http.Request) {
			manager, _ := t.managers.Get(globalManagerUUID)
			manager.incrementReceived()
		},
	}
	t.setHandlers(defaultHandlers)

	go func() {
		t.controlCh <- t.s.ListenAndServe()
	}()
}

func (t *targetNodeMock) beforeTeardown() {
	manager, _ := t.managers.Get(globalManagerUUID)
	manager.lock()
	manager.setInProgressTo(false)
	manager.unlock()
	manager.cleanup()
}

func (t *targetNodeMock) teardown() {
	t.s.Close()
	Expect(<-t.controlCh).To(Equal(http.ErrServerClosed))
}

func (tctx *testContext) setup() {
	tctx.errCh = make(chan error, tctx.targetCnt)
	tctx.wg = &sync.WaitGroup{}

	mm = memsys.TestPageMM()

	fs.TestNew(nil)
	err := cos.CreateDir(testDir)
	Expect(err).NotTo(HaveOccurred())
	_, err = fs.Add(testDir, "daeID")
	Expect(err).NotTo(HaveOccurred())

	fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})

	config := cmn.GCO.BeginUpdate()
	config.HostNet.UseIntraControl = false
	config.HostNet.UseIntraData = false
	config.Disk.IostatTimeShort = cos.Duration(10 * time.Millisecond)
	cmn.GCO.CommitUpdate(config)

	genNodeID := func(i int) string {
		return fmt.Sprintf("target:%d", i)
	}

	// Initialize smap.
	smap := newTestSmap()
	ports, err := getFreePorts(tctx.targetCnt)
	Expect(err).ShouldNot(HaveOccurred())
	for i := 0; i < tctx.targetCnt; i++ {
		targetPort := ports[i]
		ni := cluster.NetInfo{
			NodeHostname: testIP,
			DaemonPort:   fmt.Sprintf("%d", targetPort),
			DirectURL:    fmt.Sprintf("http://%s:%d", testIP, targetPort),
		}
		di := &cluster.Snode{
			DaemonID:        genNodeID(i),
			PublicNet:       ni,
			IntraControlNet: ni,
			IntraDataNet:    ni,
		}
		smap.addTarget(di)
	}
	smap.InitDigests()

	// Create and setup target mocks.
	targets := make([]*targetNodeMock, tctx.targetCnt)
	for i := 0; i < tctx.targetCnt; i++ {
		target := newTargetMock(genNodeID(i), smap)
		target.setup()
		targets[i] = target
	}

	// Wait for all targets to start servers. Without this sleep it may happen
	// that we close server faster than it starts and many bad things can happen.
	time.Sleep(time.Millisecond * 100)

	ctx.smapOwner = smap

	// Initialize BMD owner.
	bmdMock := cluster.NewBaseBownerMock(
		cluster.NewBck(
			testBucket, cmn.ProviderAIS, cmn.NsGlobal,
			&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}},
		),
	)
	ctx.t = cluster.NewTargetMock(bmdMock)

	tctx.smap = smap
	tctx.targets = targets
}

func (tctx *testContext) teardown() {
	for _, target := range tctx.targets {
		target.beforeTeardown()
	}
	for _, target := range tctx.targets {
		target.teardown()
	}

	os.RemoveAll(testDir)
}

var _ = Describe("Distributed Sort", func() {
	sc := transport.Init()
	go sc.Run()

	Describe("participateInRecordDistribution", func() {
		Describe("Simple smoke tests", func() {
			runSmokeRecordDistribution := func(targetCnt int) {
				ctx := &testContext{
					targetCnt: targetCnt,
				}
				ctx.setup()
				defer ctx.teardown()

				for _, target := range ctx.targets {
					ctx.wg.Add(1)
					go func(target *targetNodeMock) {
						defer ctx.wg.Done()

						targetOrder := randomTargetOrder(1, ctx.smap.Tmap)
						finalTarget := targetOrder[len(targetOrder)-1]
						manager, exists := target.managers.Get(globalManagerUUID)
						Expect(exists).To(BeTrue())
						isFinal, err := manager.participateInRecordDistribution(targetOrder)
						if err != nil {
							ctx.errCh <- err
							return
						}

						if target.daemonID == finalTarget.DaemonID {
							if !isFinal {
								ctx.errCh <- fmt.Errorf("last target %q is not final", finalTarget.DaemonID)
								return
							}
						} else {
							if isFinal {
								ctx.errCh <- fmt.Errorf("non-last %q target is final %q", target.daemonID, finalTarget.DaemonID)
								return
							}
						}
					}(target)
				}

				ctx.wg.Wait()
				close(ctx.errCh)
				for err := range ctx.errCh {
					Expect(err).ShouldNot(HaveOccurred())
				}
			}

			DescribeTable(
				"testing with different number of targets",
				runSmokeRecordDistribution,
				Entry("should work with 0 targets", 0),
				Entry("should work with 2 targets", 2),
				Entry("should work with 8 targets", 8),
				Entry("should work with 32 targets", 32),
				Entry("should work with 1 target", 1),
				Entry("should work with 3 targets", 3),
				Entry("should work with 31 targets", 31),
				Entry("should work with 100 targets", 100),
			)

			Context("Checking for SortedRecords", func() {
				var tctx *testContext

				BeforeEach(func() {
					tctx = &testContext{
						targetCnt: 10,
					}
					tctx.setup()

					for _, target := range tctx.targets {
						handlers := map[string]http.HandlerFunc{
							cmn.URLPathdSortRecords.S + "/": recordsHandler(target.managers),
						}
						target.setHandlers(handlers)
					}
				})

				AfterEach(func() {
					tctx.teardown()
				})

				createRecords := func(keys ...string) *extract.Records {
					records := extract.NewRecords(len(keys))
					for _, key := range keys {
						records.Insert(&extract.Record{
							Key:  key,
							Name: key,
						})
					}
					return records
				}

				It("should report that final target has all the sorted records", func() {
					srecordsCh := make(chan *extract.Records, 1)
					for _, target := range tctx.targets {
						manager, exists := target.managers.Get(globalManagerUUID)
						Expect(exists).To(BeTrue())
						manager.lock()
						manager.setInProgressTo(true)
						manager.unlock()
					}

					for _, target := range tctx.targets {
						tctx.wg.Add(1)
						go func(target *targetNodeMock) {
							defer tctx.wg.Done()

							// For each target add sorted record
							manager, exists := target.managers.Get(globalManagerUUID)
							Expect(exists).To(BeTrue())
							manager.recManager.Records = createRecords(target.daemonID)

							targetOrder := randomTargetOrder(1, tctx.smap.Tmap)
							isFinal, err := manager.participateInRecordDistribution(targetOrder)
							if err != nil {
								tctx.errCh <- err
								return
							}

							if isFinal {
								srecordsCh <- manager.recManager.Records
							}
						}(target)
					}

					tctx.wg.Wait()
					close(tctx.errCh)
					for err := range tctx.errCh {
						Expect(err).ShouldNot(HaveOccurred())
					}

					// Get SortedRecrods
					close(srecordsCh)
					srecords := <-srecordsCh
					Expect(srecords.Len()).To(Equal(len(tctx.targets)))

					// Created expected slice of records
					keys := make([]string, len(tctx.targets))
					for idx, target := range tctx.targets {
						keys[idx] = target.daemonID
					}

					expectedRecords := createRecords(keys...)
					Expect(srecords.All()).Should(ConsistOf(expectedRecords.All()))
				})

				It("should report that final target has all the records sorted in decreasing order", func() {
					srecordsCh := make(chan *extract.Records, 1)
					for _, target := range tctx.targets {
						manager, exists := target.managers.Get(globalManagerUUID)
						Expect(exists).To(BeTrue())

						rs := &ParsedRequestSpec{
							Algorithm: &SortAlgorithm{
								Decreasing: true,
								FormatType: extract.FormatTypeString,
							},
							Extension:   cos.ExtTar,
							MaxMemUsage: cos.ParsedQuantity{Type: cos.QuantityPercent, Value: 0},
							DSorterType: DSorterGeneralType,
						}
						ctx.node = ctx.smapOwner.Get().Tmap[target.daemonID]
						manager.lock()
						err := manager.init(rs)
						manager.unlock()
						if err != nil {
							tctx.errCh <- err
							return
						}

						// For each target add sorted record
						manager.recManager.Records = createRecords(target.daemonID)
					}

					for _, target := range tctx.targets {
						tctx.wg.Add(1)
						go func(target *targetNodeMock) {
							manager, exists := target.managers.Get(globalManagerUUID)
							Expect(exists).To(BeTrue())

							defer tctx.wg.Done()
							targetOrder := randomTargetOrder(1, tctx.smap.Tmap)
							isFinal, err := manager.participateInRecordDistribution(targetOrder)
							if err != nil {
								tctx.errCh <- err
								return
							}

							if isFinal {
								manager, exists := target.managers.Get(globalManagerUUID)
								Expect(exists).To(BeTrue())
								srecordsCh <- manager.recManager.Records
							}
						}(target)
					}

					tctx.wg.Wait()
					close(tctx.errCh)
					for err := range tctx.errCh {
						Expect(err).ShouldNot(HaveOccurred())
					}

					// Get SortedRecrods
					close(srecordsCh)
					srecords := <-srecordsCh
					Expect(srecords.Len()).To(Equal(len(tctx.targets)))

					// Created expected slice of records
					keys := make([]string, len(tctx.targets))
					for idx, target := range tctx.targets {
						keys[idx] = target.daemonID
					}

					expectedRecords := createRecords(keys...)
					Expect(srecords.All()).Should(ConsistOf(expectedRecords.All()))
				})
			})
		})
	})
})
