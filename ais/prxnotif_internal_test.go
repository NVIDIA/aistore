// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"net/http"
	"net/http/httptest"
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
			req.Header.Add(apc.HdrCallerID, daeID)
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
		nl      nl.Listener
		targets = getNodeMap(target1ID, target2ID)
	)

	BeforeEach(func() {
		n = testNotifs()
		nl = xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, targets)
	})

	Describe("handleMsg", func() {
		It("should add node to finished set on receiving finished stats", func() {
			Expect(nl.FinCount()).To(BeEquivalentTo(0))
			snap := finishedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap)}
			n._finished(nl, targets[target1ID], msg)
			Expect(nl.ActiveNotifiers().Contains(target1ID)).To(BeFalse())
			Expect(nl.Finished()).To(BeFalse())
		})

		It("should set error when source sends an error message", func() {
			Expect(nl.Err()).To(BeNil())
			snap := finishedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap), ErrMsg: "some error"}
			n._finished(nl, targets[target1ID], msg)
			Expect(msg.ErrMsg).To(BeEquivalentTo(nl.Err().Error()))
			Expect(nl.ActiveNotifiers().Contains(target1ID)).To(BeFalse())
		})

		It("should finish when all the Notifiers finished", func() {
			Expect(nl.FinCount()).To(BeEquivalentTo(0))
			n.add(nl)
			snap := finishedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap)}
			n._finished(nl, targets[target1ID], msg)
			n._finished(nl, targets[target2ID], msg)
			Expect(nl.FinCount()).To(BeEquivalentTo(len(targets)))
			Expect(nl.Finished()).To(BeTrue())
		})

		It("should be done if xaction Aborted", func() {
			snap := abortedXact(xid)
			msg := &core.NotifMsg{Data: cos.MustMarshal(snap), AbortedX: snap.AbortedX}
			n._finished(nl, targets[target1ID], msg)
			Expect(nl.Aborted()).To(BeTrue())
			Expect(nl.Err()).NotTo(BeNil())
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
			nl.Lock()
			n._progress(nl, targets[target1ID], msg)
			nl.Unlock()
			val, _ := nl.NodeStats().Load(target1ID)
			snap, ok := val.(*core.Snap)
			Expect(ok).To(BeTrue())
			Expect(snap.Stats.Objs).To(BeEquivalentTo(initObjCount))
			Expect(snap.Stats.Bytes).To(BeEquivalentTo(initByteCount))

			// Next a Finished notification with stats
			msg = &core.NotifMsg{Data: cos.MustMarshal(statsProgress)}
			n._finished(nl, targets[target1ID], msg)
			val, _ = nl.NodeStats().Load(target1ID)
			snap, ok = val.(*core.Snap)
			Expect(ok).To(BeTrue())
			Expect(snap.Stats.Objs).To(BeEquivalentTo(updatedObjCount))
			Expect(snap.Stats.Bytes).To(BeEquivalentTo(updatedByteCount))
		})
	})

	Describe("ListenSmapChanged", func() {
		It("should mark xaction Aborted when node not in smap", func() {
			notifiers := getNodeMap(target1ID, target2ID)
			nl = xact.NewXactNL(xid, apc.ActECEncode, &smap.Smap, notifiers)
			n = testNotifs()
			n.add(nl)

			// Update smap, remove a target
			smap := n.p.owner.smap.get()
			smap.Tmap = getNodeMap(target1ID) // target 2 removed
			smap.Version++
			n.p.owner.smap.put(smap)

			n.ListenSmapChanged()
			Expect(nl.Finished()).To(BeTrue())
			Expect(nl.Aborted()).To(BeTrue())
		})
	})

	Describe("handler", func() {
		It("should mark xaction finished when done", func() {
			stats := finishedXact(xid)
			n.add(nl)

			request := notifRequest(target1ID, xid, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// Second target sends progress
			request = notifRequest(target2ID, xid, apc.Progress, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should not be marked finished on progress notification
			Expect(nl.Finished()).To(BeFalse())

			// Second target finished
			request = notifRequest(target2ID, xid, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished
			Expect(nl.Finished()).To(BeTrue())
		})

		It("should accept finished notifications after a target aborts", func() {
			stats := finishedXact(xid)
			abortStats := abortedXact(xid)
			n.add(nl)

			// First target aborts an xaction
			request := notifRequest(target1ID, xid, apc.Finished, abortStats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished when an xaction aborts
			Expect(nl.Finished()).To(BeTrue())
			Expect(nl.FinCount()).To(BeEquivalentTo(1))

			// Second target sends finished stats
			request = notifRequest(target2ID, xid, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)
			Expect(nl.Finished()).To(BeTrue())
			Expect(nl.FinCount()).To(BeEquivalentTo(2))
		})
	})
})
