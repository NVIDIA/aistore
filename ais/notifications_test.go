// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Notifications xaction test", func() {
	// Note: constants and functions declared inside 'Describe' to avoid cluttering of `ais` namespace
	const (
		target1ID = "target1"
		target2ID = "target2"
		pDaemonID = "primary-id"
		xactID    = "xact-id"
	)

	// helper functions
	var (
		mockNode = func(id, daeType string) *cluster.Snode {
			server := discoverServerDefaultHandler(1, 1)
			info := serverTCPAddr(server.URL)
			return newSnode(id, httpProto, daeType, info, &net.TCPAddr{}, &net.TCPAddr{})
		}

		getNodeMap = func(ids ...string) (snodes cluster.NodeMap) {
			snodes = make(cluster.NodeMap, len(ids))
			for _, id := range ids {
				snodes[id] = mockNode(id, cmn.Target)
			}
			return
		}

		mockProxyRunner = func(name string) *proxyrunner {
			tracker := &stats.TrackerMock{}
			p := &proxyrunner{
				httprunner: httprunner{
					si:               mockNode(name, cmn.Proxy),
					statsT:           tracker,
					httpclientGetPut: &http.Client{},
					httpclient:       &http.Client{},
				},
			}
			p.keepalive = newProxyKeepaliveRunner(p, tracker, atomic.NewBool(true))
			return p
		}

		testNotifs = func() *notifs {
			n := &notifs{
				p:   mockProxyRunner(pDaemonID),
				fin: make(map[string]nl.NotifListener, 2),
				m:   make(map[string]nl.NotifListener, 2),
			}
			smap := &smapX{Smap: cluster.Smap{Version: 1}}
			n.p.httprunner.owner.smap = newSmapOwner()
			n.p.httprunner.owner.smap.put(smap)
			n.p.httprunner.startup.cluster = *atomic.NewBool(true)
			return n
		}

		baseXact = func(xactID string, counts ...int64) *xaction.BaseXactStatsExt {
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
			return &xaction.BaseXactStatsExt{BaseXactStats: xaction.BaseXactStats{
				IDX:         xactID,
				BytesCountX: byteCount,
				ObjCountX:   objCount,
			}}
		}

		finishedXact = func(xactID string, counts ...int64) (stats *xaction.BaseXactStatsExt) {
			stats = baseXact(xactID, counts...)
			stats.EndTimeX = time.Now()
			return
		}

		abortedXact = func(xactID string, counts ...int64) (stats *xaction.BaseXactStatsExt) {
			stats = finishedXact(xactID, counts...)
			stats.AbortedX = true
			return
		}

		notifRequest = func(daeID, xactID, notifKind string, stats interface{}) *http.Request {
			nm := cluster.NotifMsg{
				UUID: xactID,
				Data: cmn.MustMarshal(stats),
			}
			body := bytes.NewBuffer(cmn.MustMarshal(nm))
			req := httptest.NewRequest(http.MethodPost, cmn.JoinWords(cmn.Version, cmn.Notifs, notifKind),
				body)
			req.Header = make(http.Header)
			req.Header.Add(cmn.HeaderCallerID, daeID)
			return req
		}

		checkRequest = func(n *notifs, req *http.Request, expectedStatus int) []byte {
			writer := httptest.NewRecorder()
			n.handler(writer, req)
			resp := writer.Result()
			respBody, _ := ioutil.ReadAll(resp.Body)
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
		nl = xaction.NewXactNL(xactID, &smap.Smap, targets, cmn.ActECEncode)
	})

	Describe("handleMsg", func() {
		It("should add node to finished set on receiving finished stats", func() {
			Expect(nl.FinCount()).To(BeEquivalentTo(0))
			stats := finishedXact(xactID)
			err := n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), nil)
			Expect(err).To(BeNil())
			Expect(nl.FinNotifiers().Contains(target1ID)).To(BeTrue())
			Expect(nl.Finished()).To(BeFalse())
		})

		It("should set error when source sends an error message", func() {
			Expect(nl.Err()).To(BeNil())
			stats := finishedXact(xactID)
			srcErr := errors.New("some error")
			err := n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), srcErr)
			Expect(err).To(BeNil())
			Expect(srcErr).To(BeEquivalentTo(nl.Err()))
			Expect(nl.FinNotifiers().Contains(target1ID)).To(BeTrue())
		})

		It("should finish when all the Notifiers finished", func() {
			Expect(nl.FinCount()).To(BeEquivalentTo(0))
			stats := finishedXact(xactID)
			n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), nil)
			err := n.handleFinished(nl, targets[target2ID], cmn.MustMarshal(stats), nil)
			Expect(err).To(BeNil())
			Expect(nl.FinCount()).To(BeEquivalentTo(len(targets)))
			Expect(nl.Finished()).To(BeTrue())
		})

		It("should be done if xaction Aborted", func() {
			stats := abortedXact(xactID)
			err := n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), nil)
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
			err := n.handleProgress(nl, targets[target1ID], cmn.MustMarshal(statsFirst), nil)
			Expect(err).To(BeNil())
			val, _ := nl.NodeStats().Load(target1ID)
			statsXact, ok := val.(*xaction.BaseXactStatsExt)
			Expect(ok).To(BeTrue())
			Expect(statsXact.ObjCount()).To(BeEquivalentTo(initObjCount))
			Expect(statsXact.BytesCount()).To(BeEquivalentTo(initByteCount))

			// Next a Finished notification with stats
			err = n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(statsProgress), nil)
			Expect(err).To(BeNil())
			val, _ = nl.NodeStats().Load(target1ID)
			statsXact, ok = val.(*xaction.BaseXactStatsExt)
			Expect(ok).To(BeTrue())
			Expect(statsXact.ObjCount()).To(BeEquivalentTo(updatedObjCount))
			Expect(statsXact.BytesCount()).To(BeEquivalentTo(updatedByteCount))
		})
	})

	Describe("ListenSmapChanged", func() {
		It("should mark xaction Aborted when node not in smap", func() {
			notifiers := getNodeMap(target1ID, target2ID)
			nl = xaction.NewXactNL(xactID, &smap.Smap, notifiers, cmn.ActECEncode)
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

			request := notifRequest(target1ID, xactID, cmn.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// Second target sends progress
			request = notifRequest(target2ID, xactID, cmn.Progress, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should not be marked finished on progress notification
			Expect(nl.Finished()).To(BeFalse())

			// Second target finished
			request = notifRequest(target2ID, xactID, cmn.Finished, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished
			Expect(nl.Finished()).To(BeTrue())
		})

		// Error cases
		Context("erroneous cases handler", func() {
			It("should respond with error if xactionID not found", func() {
				request := notifRequest(target1ID, xactID, cmn.Finished, nil)
				checkRequest(n, request, http.StatusNotFound)
			})

			It("should respond with error if requested with invalid path", func() {
				request := notifRequest(target1ID, "", "invalid-kind", nil)
				checkRequest(n, request, http.StatusBadRequest)
			})

			It("should respond with error if requested if invalid data provided in body", func() {
				request := notifRequest(target1ID, xactID, cmn.Finished, nil)
				// set invalid body
				request.Body = ioutil.NopCloser(bytes.NewBuffer([]byte{}))

				respBody := checkRequest(n, request, http.StatusBadRequest)
				Expect(string(respBody)).To(ContainSubstring("json-unmarshal"))
			})

			It("should responding with StatusGone for finished notifications", func() {
				notifiers := getNodeMap(target1ID)
				nl = xaction.NewXactNL(xactID, &smap.Smap, notifiers, cmn.ActECEncode)
				n.add(nl)

				// Send a Finished notification
				request := notifRequest(target1ID, xactID, cmn.Finished, finishedXact(xactID))
				checkRequest(n, request, http.StatusOK)

				// Send a notification after finishing
				request = notifRequest(target1ID, xactID, cmn.Progress, nil)
				checkRequest(n, request, http.StatusGone)
			})
		})
	})
})
