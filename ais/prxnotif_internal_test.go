// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type nopHB struct{}

func (*nopHB) HeardFrom(string, int64) int64 { return 0 }
func (*nopHB) TimedOut(string) bool          { return false }
func (*nopHB) reg(string)                    {}
func (*nopHB) set(time.Duration) bool        { return false }

var _ hbTracker = (*nopHB)(nil)

var _ = Describe("Notifications xaction test", func() {
	// NOTE: constants and functions declared inside 'Describe' to avoid cluttering of `ais` namespace.
	const (
		target1ID = "target1"
		target2ID = "target2"
		pDaemonID = "primary-id"
	)

	cos.InitShortID(0)
	xid := cos.GenUUID()

	// helper functions
	var (
		mockNode = func(id, daeType string) *meta.Snode {
			server := discoverServerDefaultHandler(1, 1)
			info := serverTCPAddr(server.URL)
			return newSnode(id, daeType, info, info, info)
		}

		getNodeMap = func(ids ...string) (snodes meta.NodeMap) {
			snodes = make(meta.NodeMap, len(ids))
			for _, id := range ids {
				snodes[id] = mockNode(id, apc.Target)
			}
			return
		}

		mockProxyRunner = func(name string) *proxy {
			tracker := &mock.StatsTracker{}
			p := &proxy{
				htrun: htrun{
					si:     mockNode(name, apc.Proxy),
					statsT: tracker,
				},
			}
			g.client.data = &http.Client{}
			g.client.control = &http.Client{}

			palive := newPalive(p, tracker, atomic.NewBool(true))
			palive.keepalive.hb = &nopHB{}
			p.keepalive = palive
			return p
		}

		testNotifs = func() *notifs {
			n := &notifs{
				p:   mockProxyRunner(pDaemonID),
				nls: newListeners(),
				fin: newListeners(),
			}
			smap := &smapX{Smap: meta.Smap{Version: 1}}
			n.p.htrun.owner.smap = newSmapOwner(cmn.GCO.Get())
			n.p.htrun.owner.smap.put(smap)
			n.p.htrun.startup.cluster = *atomic.NewInt64(1)
			return n
		}

		baseXact = func(xid string, counts ...int64) *core.Snap {
			var (
				objCount  int64
				byteCount int64
			)
			if len(counts) > 0 {
				objCount = counts[0]
			}
			if len(counts) > 1 {
				byteCount = counts[1]
			}
			return &core.Snap{
				ID: xid,
				Stats: core.Stats{
					Bytes: byteCount,
					Objs:  objCount,
				}}
		}

		finishedXact = func(xid string, counts ...int64) (snap *core.Snap) {
			snap = baseXact(xid, counts...)
			snap.EndTime = time.Now()
			return
		}

		abortedXact = func(xid string, counts ...int64) (snap *core.Snap) {
			snap = finishedXact(xid, counts...)
			snap.AbortedX = true
			return
		}

		notifRequest = func(daeID, xid, notifKind string, stats *core.Snap) *http.Request {
			nm := core.NotifMsg{
				UUID:     xid,
				Data:     cos.MustMarshal(stats),
				AbortedX: stats.AbortedX,
			}
			body := bytes.NewBuffer(cos.MustMarshal(nm))
			req := httptest.NewRequest(http.MethodPost, apc.URLPathNotifs.Join(notifKind), body)
			req.Header = make(http.Header)
			req.Header.Add(apc.HdrSenderID, daeID)
			return req
		}

		checkRequest = func(n *notifs, req *http.Request, expectedStatus int) []byte {
			writer := httptest.NewRecorder()
			n.handler(writer, req)
			resp := writer.Result()
			respBody, _ := cos.ReadAllN(resp.Body, resp.ContentLength)
			resp.Body.Close()
			Expect(resp.StatusCode).To(BeEquivalentTo(expectedStatus))
			return respBody
		}
	)

	var (
		n       *notifs
		smap    = &smapX{}
		nl1     nl.Listener
		targets = getNodeMap(target1ID, target2ID)
	)

	BeforeEach(func() {
		n = testNotifs()
		nl1 = xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
	})

	Describe("handleMsg", func() {
		It("should add node to finished set on receiving finished stats", func() {
			Expect(nl1.FinCount()).To(BeEquivalentTo(0))
			snap := finishedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap)}
			n._finished(nl1, targets[target1ID], msg)
			Expect(nl1.ActiveNotifiers().Contains(target1ID)).To(BeFalse())
			Expect(nl1.Finished()).To(BeFalse())
		})

		It("should set error when source sends an error message", func() {
			Expect(nl1.Err()).To(BeNil())
			snap := finishedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap), ErrMsg: "some error"}
			n._finished(nl1, targets[target1ID], msg)
			Expect(msg.ErrMsg).To(BeEquivalentTo(nl1.Err().Error()))
			Expect(nl1.ActiveNotifiers().Contains(target1ID)).To(BeFalse())
		})

		It("should finish when all the Notifiers finished", func() {
			Expect(nl1.FinCount()).To(BeEquivalentTo(0))
			n.add(nl1)
			snap := finishedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap)}
			n._finished(nl1, targets[target1ID], msg)
			n._finished(nl1, targets[target2ID], msg)
			Expect(nl1.FinCount()).To(BeEquivalentTo(len(targets)))
			Expect(nl1.Finished()).To(BeTrue())
		})

		It("should be done if xaction Aborted", func() {
			snap := abortedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap), AbortedX: snap.AbortedX}
			n._finished(nl1, targets[target1ID], msg)
			Expect(nl1.Aborted()).To(BeTrue())
			Expect(nl1.Err()).NotTo(BeNil())
		})

		It("should update local stats upon progress", func() {
			var (
				initObjCount     int64 = 5
				initByteCount    int64 = 30
				updatedObjCount  int64 = 10
				updatedByteCount int64 = 120
			)

			statsFirst := baseXact(xid, initObjCount, initByteCount)
			statsProgress := baseXact(xid, updatedObjCount, updatedByteCount)

			// Handle fist set of stats
			msg := &core.NotifMsg{Data: cos.MustMarshal(statsFirst)}
			nl1.Lock()
			n._progress(nl1, targets[target1ID], msg)
			nl1.Unlock()
			val, _ := nl1.NodeStats().Load(target1ID)
			snap, ok := val.(*core.Snap)
			Expect(ok).To(BeTrue())
			Expect(snap.Stats.Objs).To(BeEquivalentTo(initObjCount))
			Expect(snap.Stats.Bytes).To(BeEquivalentTo(initByteCount))

			// Next a Finished notification with stats
			msg = &core.NotifMsg{Data: cos.MustMarshal(statsProgress)}
			n._finished(nl1, targets[target1ID], msg)
			val, _ = nl1.NodeStats().Load(target1ID)
			snap, ok = val.(*core.Snap)
			Expect(ok).To(BeTrue())
			Expect(snap.Stats.Objs).To(BeEquivalentTo(updatedObjCount))
			Expect(snap.Stats.Bytes).To(BeEquivalentTo(updatedByteCount))
		})
	})

	Describe("ListenSmapChanged", func() {
		It("should mark xaction Aborted when node not in smap", func() {
			notifiers := getNodeMap(target1ID, target2ID)
			nl1 = xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, notifiers)
			n = testNotifs()
			n.add(nl1)

			// Update smap, remove a target
			smap := n.p.owner.smap.get()
			smap.Tmap = getNodeMap(target1ID) // target 2 removed
			smap.Version++
			n.p.owner.smap.put(smap)

			n.ListenSmapChanged()
			Expect(nl1.Finished()).To(BeTrue())
			Expect(nl1.Aborted()).To(BeTrue())
		})
	})

	Describe("handler", func() {
		It("should mark xaction finished when done", func() {
			stats := finishedXact(xid)
			n.add(nl1)

			request := notifRequest(target1ID, xid, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// Second target sends progress
			request = notifRequest(target2ID, xid, apc.Progress, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should not be marked finished on progress notification
			Expect(nl1.Finished()).To(BeFalse())

			// Second target finished
			request = notifRequest(target2ID, xid, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished
			Expect(nl1.Finished()).To(BeTrue())
		})

		It("should accept finished notifications after a target aborts", func() {
			stats := finishedXact(xid)
			abortStats := abortedXact(xid)
			n.add(nl1)

			// First target aborts an xaction
			request := notifRequest(target1ID, xid, apc.Finished, abortStats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished when an xaction aborts
			Expect(nl1.Finished()).To(BeTrue())
			Expect(nl1.FinCount()).To(BeEquivalentTo(1))

			// Second target sends finished stats
			request = notifRequest(target2ID, xid, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)
			Expect(nl1.Finished()).To(BeTrue())
			Expect(nl1.FinCount()).To(BeEquivalentTo(2))
		})
	})

	Describe("Sharding", func() {
		It("should distribute listeners across shards based on UUID", func() {
			n = testNotifs()

			// Create multiple listeners with different UUIDs
			numListeners := 160
			listeners := make([]nl.Listener, numListeners)
			for i := range numListeners {
				xid := cos.GenUUID()
				targets := getNodeMap(target1ID)
				listeners[i] = xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
				n.add(listeners[i])
			}

			// Verify distribution across shards
			totalCount := 0
			shardCounts := make([]int, shimcnt)
			for i := range shimcnt {
				shardCounts[i] = len(n.nls.all[i].m)
				totalCount += shardCounts[i]
			}

			// Verify all listeners were added
			Expect(totalCount).To(Equal(numListeners))

			// With random UUIDs, each shard should have *some* listeners
			// Don't check for perfect distribution, just verify spreading
			emptyShards := 0
			for _, count := range shardCounts {
				if count == 0 {
					emptyShards++
				}
			}
			// With 160 listeners, expect most shards to have listeners
			Expect(emptyShards).To(BeNumerically("<", shimcnt/2),
				"Too many empty shards - distribution may be broken")

		})

		It("should correctly route operations to the right shard", func() {
			n = testNotifs()
			xid1 := cos.GenUUID()
			xid2 := cos.GenUUID()

			targets := getNodeMap(target1ID)
			nl1 := xact.NewXactNL(xid1, apc.ActECEncode, &smap.Smap, targets)
			nl2 := xact.NewXactNL(xid2, apc.ActECEncode, &smap.Smap, targets)

			// Add both
			n.add(nl1)
			n.add(nl2)

			// Verify they're in correct shards
			idx1 := n.nls.index(xid1)
			idx2 := n.nls.index(xid2)

			_, exists1 := n.nls.all[idx1].m[xid1]
			Expect(exists1).To(BeTrue())

			_, exists2 := n.nls.all[idx2].m[xid2]
			Expect(exists2).To(BeTrue())

			// Verify entry() returns correct listeners
			entry1, exists1 := n.nls.entry(xid1)
			Expect(exists1).To(BeTrue())
			Expect(entry1.UUID()).To(Equal(xid1))

			entry2, exists2 := n.nls.entry(xid2)
			Expect(exists2).To(BeTrue())
			Expect(entry2.UUID()).To(Equal(xid2))
		})

		It("should handle concurrent operations on different shards", func() {
			n = testNotifs()
			numGoroutines := 50
			opsPerGoroutine := 20

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			// in parallel
			for range numGoroutines {
				go func() {
					defer wg.Done()
					defer GinkgoRecover()

					for range opsPerGoroutine {
						xid := cos.GenUUID()
						targets := getNodeMap(target1ID)
						nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)

						// add
						err := n.add(nl)
						Expect(err).To(BeNil())

						// lookup
						entry, exists := n.nls.entry(xid)
						Expect(exists).To(BeTrue())
						Expect(entry.UUID()).To(Equal(xid))

						// delete
						ok := n.del(nl, false)
						Expect(ok).To(BeTrue())

						// verify
						_, exists = n.nls.entry(xid)
						Expect(exists).To(BeFalse())
					}
				}()
			}

			wg.Wait()

			// check all cleaned up
			Expect(n.nls.l.Load()).To(BeEquivalentTo(0))
			for i := range shimcnt {
				Expect(len(n.nls.all[i].m)).To(BeZero())
			}
		})

		It("should maintain consistency under concurrent read/write", func() {
			n = testNotifs()
			numReaders := 10
			numWriters := 5
			duration := 100 * time.Millisecond

			// Seed with some listeners
			seedListeners := make(map[string]nl.Listener, 20)
			for range 20 {
				xid := cos.GenUUID()
				targets := getNodeMap(target1ID)
				nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
				n.add(nl)
				seedListeners[xid] = nl
			}

			var wg sync.WaitGroup
			stop := make(chan struct{})

			// Launch readers
			wg.Add(numReaders)
			for range numReaders {
				go func() {
					defer wg.Done()
					defer GinkgoRecover()

					for {
						select {
						case <-stop:
							return
						default:
							// Random read operations
							for xid := range seedListeners {
								entry, exists := n.nls.entry(xid)
								if exists {
									Expect(entry.UUID()).To(Equal(xid))
								}
							}
						}
					}
				}()
			}

			// writers
			wg.Add(numWriters)
			for range numWriters {
				go func() {
					defer wg.Done()
					defer GinkgoRecover()

					for {
						select {
						case <-stop:
							return
						default:
							// add new listener
							xid := cos.GenUUID()
							targets := getNodeMap(target1ID)
							nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
							n.add(nl)

							// delete it
							n.del(nl, false)
						}
					}
				}()
			}

			// let it run
			time.Sleep(duration)
			close(stop)
			wg.Wait()

			// check
			for xid := range seedListeners {
				entry, exists := n.nls.entry(xid)
				Expect(exists).To(BeTrue())
				Expect(entry.UUID()).To(Equal(xid))
			}
		})

		It("should correctly handle housekeep with sharded locks", func() {
			n = testNotifs()

			// add some finished listeners with old timestamps
			numOldListeners := 10

			for range numOldListeners {
				xid := cos.GenUUID()
				targets := getNodeMap(target1ID)
				nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
				// set as old/finished
				nl.SetStats(target1ID, &core.Snap{
					ID:      xid,
					EndTime: time.Now().Add(-24 * time.Hour),
				})
				n.fin.add(nl, false)
			}

			initialCount := n.fin.l.Load()
			Expect(initialCount).To(BeEquivalentTo(numOldListeners))

			// housekeep (should clean up old entries)
			n.housekeep(time.Now().UnixNano())

			// verify housekeep
			finalCount := n.fin.l.Load()
			Expect(finalCount).To(BeNumerically("<", initialCount), "Housekeep should have cleaned up old listeners")
		})

	})
})

//
// micro-benches ------- TODO: isolate from TestMain() and side effects - see ais/tgtobj_internal_test.go :TODO
//

type fakeNL struct {
	id        string
	owner     string
	notifiers meta.NodeMap
}

func newFakeNL(id string) nl.Listener { // helper
	return &fakeNL{
		id:        id,
		notifiers: meta.NodeMap{"t": &meta.Snode{DaeID: "t"}},
	}
}

func (*fakeNL) Callback(nl.Listener, int64)                               {}
func (*fakeNL) UnmarshalStats([]byte) (any, bool, bool, error)            { return nil, false, false, nil }
func (*fakeNL) Lock()                                                     {}
func (*fakeNL) Unlock()                                                   {}
func (*fakeNL) RLock()                                                    {}
func (*fakeNL) RUnlock()                                                  {}
func (f *fakeNL) Notifiers() meta.NodeMap                                 { return f.notifiers }
func (*fakeNL) Kind() string                                              { return "bench" }
func (*fakeNL) Cause() string                                             { return "" }
func (*fakeNL) Bcks() []*cmn.Bck                                          { return nil }
func (*fakeNL) AddErr(error)                                              {}
func (*fakeNL) Err() error                                                { return nil }
func (*fakeNL) ErrCnt() int                                               { return 0 }
func (f *fakeNL) UUID() string                                            { return f.id }
func (*fakeNL) SetAborted()                                               {}
func (*fakeNL) Aborted() bool                                             { return false }
func (*fakeNL) Status() *nl.Status                                        { return nil }
func (*fakeNL) SetStats(string, any)                                      {}
func (*fakeNL) NodeStats() *nl.NodeStats                                  { return nil }
func (*fakeNL) QueryArgs() cmn.HreqArgs                                   { return cmn.HreqArgs{} }
func (*fakeNL) EndTime() int64                                            { return 0 }
func (*fakeNL) SetAddedTime()                                             {}
func (*fakeNL) AddedTime() int64                                          { return 0 }
func (*fakeNL) Finished() bool                                            { return false }
func (f *fakeNL) Name() string                                            { return f.id }
func (f *fakeNL) String() string                                          { return f.id }
func (f *fakeNL) GetOwner() string                                        { return f.owner }
func (f *fakeNL) SetOwner(s string)                                       { f.owner = s }
func (*fakeNL) LastUpdated(*meta.Snode) int64                             { return 0 }
func (*fakeNL) ProgressInterval() time.Duration                           { return 0 }
func (f *fakeNL) ActiveNotifiers() meta.NodeMap                           { return f.notifiers }
func (*fakeNL) FinCount() int                                             { return 0 }
func (f *fakeNL) ActiveCount() int                                        { return len(f.notifiers) }
func (*fakeNL) HasFinished(*meta.Snode) bool                              { return false }
func (*fakeNL) MarkFinished(*meta.Snode)                                  {}
func (*fakeNL) NodesTardy(time.Duration) (nodes meta.NodeMap, tardy bool) { return nil, false }

func BenchmarkShardedAdd(b *testing.B) {
	cos.InitShortID(0)
	n := &notifs{
		nls: newListeners(),
		fin: newListeners(),
	}
	smap := &smapX{}
	targets := make(meta.NodeMap, 1)
	targets["target1"] = &meta.Snode{DaeID: "target1"}

	b.ResetTimer()
	for range b.N {
		xid := cos.GenUUID()
		nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
		n.nls.add(nl, false)
	}
}

func BenchmarkShardedEntry(b *testing.B) {
	cos.InitShortID(0)
	n := &notifs{
		nls: newListeners(),
		fin: newListeners(),
	}
	smap := &smapX{}
	targets := make(meta.NodeMap, 1)
	targets["target1"] = &meta.Snode{DaeID: "target1"}

	// Seed with listeners
	xids := make([]string, 1000)
	for i := range xids {
		xid := cos.GenUUID()
		xids[i] = xid
		nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
		n.nls.add(nl, false)
	}

	b.ResetTimer()
	for i := range b.N {
		xid := xids[i%len(xids)]
		_, _ = n.nls.entry(xid)
	}
}

func BenchmarkShardedConcurrentMixed(b *testing.B) {
	cos.InitShortID(0)
	n := &notifs{
		nls: newListeners(),
		fin: newListeners(),
	}
	smap := &smapX{}
	targets := make(meta.NodeMap, 1)
	targets["target1"] = &meta.Snode{DaeID: "target1"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			xid := cos.GenUUID()
			nl := xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)

			// add
			n.nls.add(nl, false)
			// lookup
			_, _ = n.nls.entry(xid)
			// delete
			n.nls.del(nl, false)
		}
	})
}

// last byte controls shard (indexer uses last byte & shimask)
func makeIDs(count, shard int) []string {
	ids := make([]string, count)
	base := []byte("benchxxxxxxxxxxxxxxxxxxxxxxxx") // long enough
	for i := range count {
		b := make([]byte, len(base))
		copy(b, base)
		b[len(b)-1] = byte((i << 4) | (shard & 0x0f))
		ids[i] = string(b)
	}
	return ids
}

func makeIDsSpread(count int) []string {
	ids := make([]string, count)
	base := []byte("benchxxxxxxxxxxxxxxxxxxxxxxxx")
	for i := range count {
		b := make([]byte, len(base))
		copy(b, base)
		b[len(b)-1] = byte(i) // uniform across 16 shards
		ids[i] = string(b)
	}
	return ids
}

func BenchmarkShardedAdd_Spread(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	ids := makeIDsSpread(b.N << 2)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		n.nls.add(newFakeNL(ids[i]), false)
	}
}

func BenchmarkShardedAddDel_Spread(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	const ring = 1 << 14
	ids := makeIDsSpread(ring)
	mask := ring - 1
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		id := ids[i&mask]
		nl := newFakeNL(id)
		n.nls.add(nl, false)
		n.nls.del(nl, false)
	}
}

func BenchmarkShardedAddDel_HotShard(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	const ring = 1 << 14
	ids := makeIDs(ring, 7)
	mask := ring - 1
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		id := ids[i&mask]
		nl := newFakeNL(id)
		n.nls.add(nl, false)
		n.nls.del(nl, false)
	}
}

func BenchmarkShardedEntry_Spread(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	const num = 4096
	ids := makeIDsSpread(num)
	for _, id := range ids {
		n.nls.add(newFakeNL(id), false)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_, _ = n.nls.entry(ids[i&(num-1)])
	}
}

func BenchmarkShardedEntry_HotShard(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	const num = 4096
	ids := makeIDs(num, 12)
	for _, id := range ids {
		n.nls.add(newFakeNL(id), false)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_, _ = n.nls.entry(ids[i&(num-1)])
	}
}

func BenchmarkShardedConcurrentMixed_Spread(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		const ring = 1 << 12
		ids := makeIDsSpread(ring)
		mask := ring - 1
		i := 0
		for pb.Next() {
			id := ids[i&mask]
			i++
			nl := newFakeNL(id)
			n.nls.add(nl, false)
			_, _ = n.nls.entry(id)
			n.nls.del(nl, false)
		}
	})
}

func BenchmarkShardedConcurrentMixed_HotShard(b *testing.B) {
	n := &notifs{nls: newListeners(), fin: newListeners()}
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		const ring = 1 << 12
		ids := makeIDs(ring, 3)
		mask := ring - 1
		i := 0
		for pb.Next() {
			id := ids[i&mask]
			i++
			nl := newFakeNL(id)
			n.nls.add(nl, false)
			_, _ = n.nls.entry(id)
			n.nls.del(nl, false)
		}
	})
}
