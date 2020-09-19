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
	"github.com/NVIDIA/aistore/stats"
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
		getNodeMap = func(ids ...string) (snodes cluster.NodeMap) {
			snodes = make(cluster.NodeMap, len(ids))
			for _, id := range ids {
				server := discoverServerDefaultHandler(1, 1)
				info := serverTCPAddr(server.URL)
				snodes[id] = newSnode(id, httpProto, cmn.Target, info, &net.TCPAddr{}, &net.TCPAddr{})
			}
			return
		}

		testNotifs = func() *notifs {
			n := &notifs{
				p: &proxyrunner{
					httprunner: httprunner{
						si:               getNodeMap(pDaemonID)[pDaemonID],
						statsT:           &stats.TrackerMock{},
						httpclientGetPut: &http.Client{},
						httpclient:       &http.Client{},
					}},
				fin: make(map[string]notifListener, 2),
				m:   make(map[string]notifListener, 2),
			}
			smap := &smapX{Smap: cluster.Smap{Version: 1}}
			n.p.httprunner.owner.smap = newSmapOwner()
			n.p.httprunner.owner.smap.put(smap)
			n.p.httprunner.startup.cluster = *atomic.NewBool(true)
			return n
		}

		baseXact = func(xactID string, counts ...int64) *cmn.BaseXactStatsExt {
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
			return &cmn.BaseXactStatsExt{BaseXactStats: cmn.BaseXactStats{
				IDX:         xactID,
				BytesCountX: byteCount,
				ObjCountX:   objCount,
			}}
		}

		finishedXact = func(xactID string, counts ...int64) (stats *cmn.BaseXactStatsExt) {
			stats = baseXact(xactID, counts...)
			stats.EndTimeX = time.Now()
			return
		}

		abortedXact = func(xactID string, counts ...int64) (stats *cmn.BaseXactStatsExt) {
			stats = finishedXact(xactID, counts...)
			stats.AbortedX = true
			return
		}

		notifRequest = func(daeID, xactID, notifKind string, notifTy int32, stats interface{}) *http.Request {
			nm := cmn.NotifMsg{
				UUID: xactID,
				Ty:   notifTy,
				Data: cmn.MustMarshal(stats),
			}
			body := bytes.NewBuffer(cmn.MustMarshal(nm))
			req := httptest.NewRequest(http.MethodPost, cmn.JoinWords(cmn.Version, cmn.Notifs, notifKind), body)
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
		nl      notifListener
		targets = getNodeMap(target1ID, target2ID)
	)

	BeforeEach(func() {
		n = testNotifs()
		nl = newXactNL(xactID, smap, targets, notifXact, cmn.ActECEncode)
	})

	Describe("handleMsg", func() {
		It("should add node to finished set on receiving finished stats", func() {
			Expect(nl.finCount()).To(BeEquivalentTo(0))
			stats := finishedXact(xactID)
			err := n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), nil)
			Expect(err).To(BeNil())
			Expect(nl.finNotifiers().Contains(target1ID)).To(BeTrue())
			Expect(nl.finished()).To(BeFalse())
		})

		It("should set error when source sends an error message", func() {
			Expect(nl.err()).To(BeNil())
			stats := finishedXact(xactID)
			srcErr := errors.New("some error")
			err := n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), srcErr)
			Expect(err).To(BeNil())
			Expect(srcErr).To(BeEquivalentTo(nl.err()))
			Expect(nl.finNotifiers().Contains(target1ID)).To(BeTrue())
		})

		It("should finish when all the notifiers finished", func() {
			Expect(nl.finCount()).To(BeEquivalentTo(0))
			stats := finishedXact(xactID)
			n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), nil)
			err := n.handleFinished(nl, targets[target2ID], cmn.MustMarshal(stats), nil)
			Expect(err).To(BeNil())
			Expect(nl.finCount()).To(BeEquivalentTo(len(targets)))
			Expect(nl.finished()).To(BeTrue())
		})

		It("should be done if xaction Aborted", func() {
			stats := abortedXact(xactID)
			err := n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(stats), nil)
			Expect(err).To(BeNil())
			Expect(nl.aborted()).To(BeTrue())
			Expect(nl.err()).NotTo(BeNil())
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
			statsXact, ok := nl.nodeStats()[target1ID].(*cmn.BaseXactStatsExt)
			Expect(ok).To(BeTrue())
			Expect(statsXact.ObjCount()).To(BeEquivalentTo(initObjCount))
			Expect(statsXact.BytesCount()).To(BeEquivalentTo(initByteCount))

			// Next a Finished notification with stats
			err = n.handleFinished(nl, targets[target1ID], cmn.MustMarshal(statsProgress), nil)
			Expect(err).To(BeNil())
			statsXact, ok = nl.nodeStats()[target1ID].(*cmn.BaseXactStatsExt)
			Expect(ok).To(BeTrue())
			Expect(statsXact.ObjCount()).To(BeEquivalentTo(updatedObjCount))
			Expect(statsXact.BytesCount()).To(BeEquivalentTo(updatedByteCount))
		})
	})

	Describe("ListenSmapChanged", func() {
		It("should mark xaction aborted when node not in smap", func() {
			notifiers := getNodeMap(target1ID, target2ID)
			nl = newXactNL(xactID, smap, notifiers, notifXact, cmn.ActECEncode)
			n = testNotifs()
			n.add(nl)

			// Update smap, remove a target
			smap := n.p.owner.smap.get()
			smap.Tmap = getNodeMap(target1ID) // target 2 removed
			smap.Version++
			n.p.owner.smap.put(smap)

			n.ListenSmapChanged()
			Expect(nl.finished()).To(BeTrue())
			Expect(nl.aborted()).To(BeTrue())
		})
	})

	Describe("handler", func() {
		It("should mark xaction finished when done", func() {
			stats := finishedXact(xactID)
			n.add(nl)

			request := notifRequest(target1ID, xactID, cmn.Finished, notifXact, stats)
			checkRequest(n, request, http.StatusOK)

			// Second target sends progress
			request = notifRequest(target2ID, xactID, cmn.Progress, notifXact, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should not be marked finished on progress notification
			Expect(nl.finished()).To(BeFalse())

			// Second target finished
			request = notifRequest(target2ID, xactID, cmn.Finished, notifXact, stats)
			checkRequest(n, request, http.StatusOK)

			// `nl` should be marked finished
			Expect(nl.finished()).To(BeTrue())
		})

		// Error cases
		Context("erroneous cases handler", func() {
			It("should respond with error if xactionID not found", func() {
				request := notifRequest(target1ID, xactID, cmn.Finished, notifXact, nil)
				checkRequest(n, request, http.StatusNotFound)
			})

			It("should respond with error if requested with invalid path", func() {
				request := notifRequest(target1ID, "", "invalid-kind", notifInvalid, nil)
				checkRequest(n, request, http.StatusBadRequest)
			})

			It("should respond with error if requested if invalid data provided in body", func() {
				request := notifRequest(target1ID, xactID, cmn.Finished, notifXact, nil)
				// set invalid body
				request.Body = ioutil.NopCloser(bytes.NewBuffer([]byte{}))

				respBody := checkRequest(n, request, http.StatusBadRequest)
				Expect(string(respBody)).To(ContainSubstring("json-unmarshal"))
			})

			It("should responding with StatusGone for finished notifications", func() {
				notifiers := getNodeMap(target1ID)
				nl = newXactNL(xactID, smap, notifiers, notifXact, cmn.ActECEncode)
				n.add(nl)

				// Send a Finished notification
				request := notifRequest(target1ID, xactID, cmn.Finished, notifXact, finishedXact(xactID))
				checkRequest(n, request, http.StatusOK)

				// Send a notification after finishing
				request = notifRequest(target1ID, xactID, cmn.Progress, notifXact, nil)
				checkRequest(n, request, http.StatusGone)
			})
		})
	})
})
