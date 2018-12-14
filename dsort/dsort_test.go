// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dsort/extract"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/transport"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

const (
	testIP            = "127.0.0.1"
	testDir           = "/tmp"
	testBucket        = "dsort_tests"
	globalManagerUUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
)

var (
	// Used as a compile-time check for correct interface implementation.
	_ extract.ExtractCreator = &extractCreatorMock{}
	_ cluster.SmapListeners  = &testSmapListeners{}
)

//
// MISC FUNCTIONS
//
func getFreePorts(count int) ([]int, error) {
	var ports []int
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

func (a *testSmapListeners) Reg(sl cluster.Slistener) {}

func (a *testSmapListeners) Unreg(sl cluster.Slistener) {}

type testSmap struct {
	*cluster.Smap
	a *testSmapListeners
}

func newTestSmap(targets ...string) *testSmap {
	smap := &testSmap{
		&cluster.Smap{},
		&testSmapListeners{},
	}
	smap.Tmap = make(map[string]*cluster.Snode)
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

func (ec *extractCreatorMock) ExtractShard(fqn string, f *io.SectionReader, extractor extract.RecordExtractor, toDisk bool) (int64, int, error) {
	return 0, 0, nil
}
func (ec *extractCreatorMock) CreateShard(s *extract.Shard, w io.Writer, loadContent extract.LoadContentFunc) (int64, error) {
	ec.createShard(s, w, loadContent)
	return 0, nil
}
func (ec *extractCreatorMock) UsingCompression() bool { return ec.useCompression }
func (ec *extractCreatorMock) MetadataSize() int64    { return 0 }

type targetMock struct {
	daemonID  string
	mux       *http.ServeMux
	s         *http.Server
	controlCh chan error
	managers  *ManagerGroup
}

type testContext struct {
	targetCnt int
	targets   []*targetMock
	smap      *testSmap
	errCh     chan error
	wg        *sync.WaitGroup
}

func newTargetMock(daemonID string, smap *testSmap) *targetMock {
	// Initialize dsort manager
	rs := &ParsedRequestSpec{
		Extension: extTar,
		Algorithm: &SortAlgorithm{
			FormatType: extract.FormatTypeString,
		},
	}

	dsortManagers := NewManagerGroup()
	dsortManager, err := dsortManagers.Add(globalManagerUUID)
	Expect(err).ShouldNot(HaveOccurred())
	ctx.node = smap.Get().Tmap[daemonID]
	dsortManager.init(rs)
	dsortManager.unlock()

	net := smap.GetTarget(daemonID).PublicNet
	return &targetMock{
		daemonID: daemonID,
		s: &http.Server{
			Addr:    fmt.Sprintf("%s:%s", net.NodeIPAddr, net.DaemonPort),
			Handler: http.NewServeMux(),
		},
		controlCh: make(chan error, 1),
		managers:  dsortManagers,
	}
}

func (t *targetMock) setHandlers(handlers map[string]http.HandlerFunc) {
	mux := http.NewServeMux()
	for path, handler := range handlers {
		mux.HandleFunc(path, handler)
	}

	t.mux = mux
	t.s.Handler = mux
}

func (t *targetMock) setup() {
	// set default handlers
	defaultHandlers := map[string]http.HandlerFunc{
		cmn.URLPath(cmn.Version, cmn.Sort, cmn.Records) + "/": func(w http.ResponseWriter, r *http.Request) {
			manager, _ := t.managers.Get(globalManagerUUID)
			manager.incrementReceived()
		},
	}
	t.setHandlers(defaultHandlers)

	go func() {
		t.controlCh <- t.s.ListenAndServe()
	}()
}

func (t *targetMock) beforeTeardown() {
	manager, _ := t.managers.Get(globalManagerUUID)
	manager.setInProgressTo(false)
	manager.cleanup()
}

func (t *targetMock) teardown() {
	t.s.Close()
	Expect(<-t.controlCh).To(Equal(http.ErrServerClosed))
}

func (tctx *testContext) setup() {
	tctx.errCh = make(chan error, tctx.targetCnt)
	tctx.wg = &sync.WaitGroup{}

	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.Add(testDir)

	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})

	config := cmn.GCO.BeginUpdate()
	config.Net.UseIntraControl = false
	config.Net.UseIntraData = false
	cmn.GCO.CommitUpdate(config)

	genNodeID := func(i int) string {
		return fmt.Sprintf("target:%d", i)
	}

	// Initialize smap
	smap := newTestSmap()
	ports, err := getFreePorts(tctx.targetCnt)
	Expect(err).ShouldNot(HaveOccurred())
	for i := 0; i < tctx.targetCnt; i++ {
		targetPort := ports[i]
		ni := cluster.NetInfo{
			NodeIPAddr: testIP,
			DaemonPort: fmt.Sprintf("%d", targetPort),
			DirectURL:  fmt.Sprintf("http://%s:%d", testIP, targetPort),
		}
		di := &cluster.Snode{
			DaemonID:        genNodeID(i),
			PublicNet:       ni,
			IntraControlNet: ni,
			IntraDataNet:    ni,
		}
		smap.addTarget(di)
	}

	// Create and setup target mocks
	targets := make([]*targetMock, tctx.targetCnt)
	for i := 0; i < tctx.targetCnt; i++ {
		target := newTargetMock(genNodeID(i), smap)
		target.setup()
		targets[i] = target
	}

	// Wait for all targets to start servers. Without this sleep it may happen
	// that we close server faster than it starts and many bad things can happen.
	time.Sleep(time.Millisecond * 100)

	ctx.smap = smap
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

	os.RemoveAll(filepath.Join(testDir, testBucket))
	fs.Mountpaths = nil
}

var _ = Describe("Distributed Sort", func() {
	Describe("participateInRecordDistribution", func() {
		Describe("Simple smoke tests", func() {
			var runSmokeRecordDistribution = func(targetCnt int) {
				ctx := &testContext{
					targetCnt: targetCnt,
				}
				ctx.setup()
				defer ctx.teardown()

				for _, target := range ctx.targets {
					ctx.wg.Add(1)
					go func(target *targetMock) {
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
							cmn.URLPath(cmn.Version, cmn.Sort, cmn.Records) + "/": recordsHandler(target.managers),
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
							Key:         key,
							ContentPath: key,
						})
					}
					return records
				}

				It("should report that final target has all the sorted records", func() {
					srecordsCh := make(chan *extract.Records, 1)
					for _, target := range tctx.targets {
						manager, exists := target.managers.Get(globalManagerUUID)
						Expect(exists).To(BeTrue())
						manager.setInProgressTo(true)
					}

					for _, target := range tctx.targets {
						tctx.wg.Add(1)
						go func(target *targetMock) {
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
							Extension: extTar,
						}
						ctx.node = ctx.smap.Get().Tmap[target.daemonID]
						err := manager.init(rs)
						if err != nil {
							tctx.errCh <- err
							return
						}

						// For each target add sorted record
						manager.recManager.Records = createRecords(target.daemonID)
					}

					for _, target := range tctx.targets {
						tctx.wg.Add(1)
						go func(target *targetMock) {
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

		Describe("distributeShardRecords", func() {
			var (
				tctx *testContext
				rs   *ParsedRequestSpec

				mtx    sync.Mutex
				shards []*extract.Shard
			)

			BeforeEach(func() {
				shards = nil
				tctx = &testContext{
					targetCnt: 10,
				}
				tctx.setup()

				rs = &ParsedRequestSpec{
					Extension:     extTar,
					Bucket:        testBucket,
					IsLocalBucket: true,
					OutputFormat: &parsedOutputTemplate{
						Prefix: "superprefix",
						Suffix: "supersuffix",
						Step:   1,
						End:    10000000,
					},
					Algorithm: &SortAlgorithm{
						FormatType: extract.FormatTypeString,
					},
					ExtractConcLimit: 10,
					CreateConcLimit:  10,
				}

				for _, target := range tctx.targets {
					handlers := map[string]http.HandlerFunc{
						cmn.URLPath(cmn.Version, cmn.Sort, cmn.Shards) + "/": shardsHandler(target.managers),
					}
					target.setHandlers(handlers)
					transport.SetMux(cmn.NetworkPublic, target.mux)

					manager, exists := target.managers.Get(globalManagerUUID)
					Expect(exists).To(BeTrue())
					ctx.node = ctx.smap.Get().Tmap[target.daemonID]
					err := manager.init(rs)
					Expect(err).ShouldNot(HaveOccurred())
					err = manager.initStreams()
					Expect(err).ShouldNot(HaveOccurred())
					manager.extractCreator = &extractCreatorMock{
						useCompression: false,
						createShard: func(s *extract.Shard, _ io.Writer, _ extract.LoadContentFunc) {
							mtx.Lock()
							shards = append(shards, s)
							mtx.Unlock()
						},
					}
				}
			})

			AfterEach(func() {
				tctx.teardown()
			})

			It("should distribute shard records without an error when there are no shards", func() {
				manager, exists := tctx.targets[0].managers.Get(globalManagerUUID)
				Expect(exists).To(BeTrue())
				err := manager.distributeShardRecords(100)
				Expect(err).ShouldNot(HaveOccurred())
			})

			type dsrArgs struct {
				recordCnt  int
				recordSize int64
				shardSize  int64

				expectedShardCnt  int
				expectedShardSize int64
			}

			testDistributeShardCreation := func(args dsrArgs) {
				rs.OutputFormat.End = args.expectedShardCnt

				finalTarget := tctx.targets[0]
				manager, exists := finalTarget.managers.Get(globalManagerUUID)
				Expect(exists).To(BeTrue())
				manager.recManager.Records = extract.NewRecords(args.recordCnt)
				keys := make([]string, args.recordCnt)
				for i := 0; i < args.recordCnt; i++ {
					key := fmt.Sprintf("%s%d%s%s", rs.OutputFormat.Prefix, i, rs.OutputFormat.Suffix, rs.Extension)
					keys[i] = key

					f, err := cmn.CreateFile(filepath.Join(testDir, fs.ObjectType, "local", testBucket, key))
					Expect(err).ShouldNot(HaveOccurred())
					err = f.Close()
					Expect(err).ShouldNot(HaveOccurred())

					manager.recManager.Records.Insert(&extract.Record{
						Key:         key,
						ContentPath: key,
						Objects: []*extract.RecordObj{&extract.RecordObj{
							Size:      args.recordSize,
							Extension: rs.Extension,
						}},
					})
				}

				err := manager.distributeShardRecords(args.shardSize)
				Expect(err).ShouldNot(HaveOccurred())

				for _, target := range tctx.targets {
					tctx.wg.Add(1)
					go func(target *targetMock) {
						defer tctx.wg.Done()
						manager, exists := target.managers.Get(globalManagerUUID)
						Expect(exists).To(BeTrue())
						tctx.errCh <- manager.createShardsLocally()
					}(target)
				}
				tctx.wg.Wait()
				close(tctx.errCh)
				for err := range tctx.errCh {
					Expect(err).ShouldNot(HaveOccurred())
				}

				Expect(shards).To(HaveLen(args.expectedShardCnt))

				var totalShardsSize int64 = 0
				foundLastShard := false
				for _, shard := range shards {
					// We allow for one shard (last) to have smaller size than expected
					if shard.Size < args.expectedShardSize {
						// Ensure that we have only one such shard
						Expect(foundLastShard).To(BeFalse())
						foundLastShard = true
					} else {
						Expect(shard.Size).To(Equal(args.expectedShardSize))
					}
					Expect(shard.Name).To(HavePrefix(rs.OutputFormat.Prefix))
					Expect(shard.Name).To(HaveSuffix("%s%s", rs.OutputFormat.Suffix, rs.Extension))

					// Ensure that all shards records have good key (there is duplicates)
					for _, shardRecord := range shard.Records.All() {
						exists := false
						for _, key := range keys {
							if shardRecord.Key == key {
								exists = true
								break
							}
						}

						Expect(exists).To(BeTrue())
					}

					totalShardsSize += shard.Size
				}

				totalRecordsSize := int64(args.recordCnt) * args.recordSize
				Expect(totalShardsSize).To(Equal(totalRecordsSize))
			}

			DescribeTable(
				"distribute shard records",
				testDistributeShardCreation,
				Entry(
					"single record with big size and small shard size",
					dsrArgs{
						recordCnt:  1,
						recordSize: 20000,
						shardSize:  1,

						expectedShardCnt:  1,
						expectedShardSize: 20000,
					},
				),
				Entry(
					"small number of records, record size and shard size",
					dsrArgs{
						recordCnt:  7,
						recordSize: 13,
						shardSize:  19,

						expectedShardCnt:  4,
						expectedShardSize: 26,
					},
				),
				Entry(
					"average number of records, record size and shard size",
					dsrArgs{
						recordCnt:  61,
						recordSize: 950,
						shardSize:  2050,

						expectedShardCnt:  21,
						expectedShardSize: 2850,
					},
				),
				Entry(
					"large number of records and shards",
					dsrArgs{
						recordCnt:  100,
						recordSize: 10000,
						shardSize:  20000,

						expectedShardCnt:  50,
						expectedShardSize: 20000,
					},
				),
			)
		})
	})
})
