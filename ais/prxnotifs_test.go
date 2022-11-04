// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type dummyKT struct{}

func (*dummyKT) HeardFrom(string, bool)            {}
func (*dummyKT) TimedOut(string) bool              { return false }
func (*dummyKT) changed(uint8, time.Duration) bool { return false }

var _ KeepaliveTracker = (*dummyKT)(nil)

var _ = Describe("Notifications xaction test", func() {
	// NOTE: constants and functions declared inside 'Describe' to avoid cluttering of `ais` namespace.
	const (
		target1ID = "target1"
		target2ID = "target2"
		pDaemonID = "primary-id"
	)

	cos.InitShortID(0)
	xactID := cos.GenUUID()

	// helper functions
	var (
		mockNode = func(id, daeType string) *cluster.Snode {
			server := discoverServerDefaultHandler(1, 1)
			info := serverTCPAddr(server.URL)
			return cluster.NewSnode(id, daeType, info, info, info)
		}

		getNodeMap = func(ids ...string) (snodes cluster.NodeMap) {
			snodes = make(cluster.NodeMap, len(ids))
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
			p.client.data = &http.Client{}
			p.client.control = &http.Client{}
			palive := newPalive(p, tracker, atomic.NewBool(true))
			palive.keepalive.kt = &dummyKT{}
			p.keepalive = palive
			return p
		}

		testNotifs = func() *notifs {
			n := &notifs{
				p:   mockProxyRunner(pDaemonID),
				nls: newListeners(),
				fin: newListeners(),
			}
			smap := &smapX{Smap: cluster.Smap{Version: 1}}
			n.p.htrun.owner.smap = newSmapOwner(cmn.GCO.Get())
			n.p.htrun.owner.smap.put(smap)
			n.p.htrun.startup.cluster = *atomic.NewInt64(1)
			return n
		}

		baseXact = func(xactID string, counts ...int64) *xact.SnapExt {
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
			return &xact.SnapExt{Snap: xact.Snap{
				ID: xactID,
				Stats: xact.Stats{
					Bytes: byteCount,
					Objs:  objCount,
				},
			}}
		}

		finishedXact = func(xactID string, counts ...int64) (snap *xact.SnapExt) {
			snap = baseXact(xactID, counts...)
			snap.EndTime = time.Now()
			return
		}

		abortedXact = func(xactID string, counts ...int64) (snap *xact.SnapExt) {
			snap = finishedXact(xactID, counts...)
			snap.AbortedX = true
			return
		}

		notifRequest = func(daeID, xactID, notifKind string, stats any) *http.Request {
			nm := cluster.NotifMsg{
				UUID: xactID,
				Data: cos.MustMarshal(stats),
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
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			Expect(resp.StatusCode).To(BeEquivalentTo(expectedStatus))
			return respBody
		}
	)

	var (
		n       *notifs
		smap    = &smapX{}
		nl      nl.NotifListener
		targets = getNodeMap(target1ID, target2ID)
	)

	BeforeEach(func() {
		n = testNotifs()
		nl = xact.NewXactNL(xactID, apc.ActECEncode, &smap.Smap, targets)
	})

	Describe("handleMsg", func() {
		It("should add node to finished set on receiving finished stats", func() {
			Expect(nl.FinCount()).To(BeEquivalentTo(0))
			snap := finishedXact(xactID)
			err := n.handleFinished(nl, targets[target1ID], cos.MustMarshal(snap), nil)
			Expect(err).To(BeNil())
			Expect(nl.ActiveNotifiers().Contains(target1ID)).To(BeFalse())
			Expect(nl.Finished()).To(BeFalse())
		})

		It("should set error when source sends an error message", func() {
			Expect(nl.Err()).To(BeNil())
			snap := finishedXact(xactID)
			srcErr := errors.New("some error")
			err := n.handleFinished(nl, targets[target1ID], cos.MustMarshal(snap), srcErr)
			Expect(err).To(BeNil())
			Expect(srcErr).To(BeEquivalentTo(nl.Err()))
			Expect(nl.ActiveNotifiers().Contains(target1ID)).To(BeFalse())
		})

		It("should finish when all the Notifiers finished", func() {
			Expect(nl.FinCount()).To(BeEquivalentTo(0))
			n.add(nl)
			snap := finishedXact(xactID)
			n.handleFinished(nl, targets[target1ID], cos.MustMarshal(snap), nil)
			err := n.handleFinished(nl, targets[target2ID], cos.MustMarshal(snap), nil)
			Expect(err).To(BeNil())
			Expect(nl.FinCount()).To(BeEquivalentTo(len(targets)))
			Expect(nl.Finished()).To(BeTrue())
		})

		It("should be done if xaction Aborted", func() {
			snap := abortedXact(xactID)
			err := n.handleFinished(nl, targets[target1ID], cos.MustMarshal(snap), nil)
			Expect(err).To(BeNil())
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

			statsFirst := baseXact(xactID, initObjCount, initByteCount)
			statsProgress := baseXact(xactID, updatedObjCount, updatedByteCount)

			// Handle fist set of stats
			err := n.handleProgress(nl, targets[target1ID], cos.MustMarshal(statsFirst), nil)
			Expect(err).To(BeNil())
			val, _ := nl.NodeStats().Load(target1ID)
			snap, ok := val.(*xact.SnapExt)
			Expect(ok).To(BeTrue())
			Expect(snap.Stats.Objs).To(BeEquivalentTo(initObjCount))
			Expect(snap.Stats.Bytes).To(BeEquivalentTo(initByteCount))

			// Next a Finished notification with stats
			err = n.handleFinished(nl, targets[target1ID], cos.MustMarshal(statsProgress), nil)
			Expect(err).To(BeNil())
			val, _ = nl.NodeStats().Load(target1ID)
			snap, ok = val.(*xact.SnapExt)
			Expect(ok).To(BeTrue())
			Expect(snap.Stats.Objs).To(BeEquivalentTo(updatedObjCount))
			Expect(snap.Stats.Bytes).To(BeEquivalentTo(updatedByteCount))
		})
	})

	Describe("ListenSmapChanged", func() {
		It("should mark xaction Aborted when node not in smap", func() {
			notifiers := getNodeMap(target1ID, target2ID)
			nl = xact.NewXactNL(xactID, apc.ActECEncode, &smap.Smap, notifiers)
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
			stats := finishedXact(xactID)
			n.add(nl)

			request := notifRequest(target1ID, xactID, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// Second target sends progress
			request = notifRequest(target2ID, xactID, apc.Progress, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should not be marked finished on progress notification
			Expect(nl.Finished()).To(BeFalse())

			// Second target finished
			request = notifRequest(target2ID, xactID, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished
			Expect(nl.Finished()).To(BeTrue())
		})

		It("should accept finished notifications after a target aborts", func() {
			stats := finishedXact(xactID)
			abortStats := abortedXact(xactID)
			n.add(nl)

			// First target aborts an xaction
			request := notifRequest(target1ID, xactID, apc.Finished, abortStats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished when an xaction aborts
			Expect(nl.Finished()).To(BeTrue())
			Expect(nl.FinCount()).To(BeEquivalentTo(1))

			// Second target sends finished stats
			request = notifRequest(target2ID, xactID, apc.Finished, stats)
			checkRequest(n, request, http.StatusOK)
			Expect(nl.Finished()).To(BeTrue())
			Expect(nl.FinCount()).To(BeEquivalentTo(2))
		})
	})
})
