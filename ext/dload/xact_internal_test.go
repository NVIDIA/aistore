// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestDownloadRejectedHandoff(t *testing.T) {
	// Download uses the package store. Keep this test serial and restore it
	// without starting the database or housekeeping goroutine.
	previousStore := g.store
	g.store = &infoStore{dljobs: make(map[string]*dljob)}
	t.Cleanup(func() { g.store = previousStore })

	// No receiver: the handoff must time out regardless of scheduling or
	// how many jobs the dispatcher would normally admit.
	xdl := &Xact{dispatcher: &dispatcher{workCh: make(chan jobif)}}
	job := &singleDlJob{sliceDlJob: sliceDlJob{
		baseDlJob: baseDlJob{id: "rejected-handoff", xdl: xdl},
	}}

	resp, status, err := xdl.Download(job)
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected HTTP 429, got %d (response %v, error %v)", status, resp, err)
	}
	if err == nil {
		t.Fatal("rejected handoff returned a nil error; the HTTP handler would panic")
	}
	if resp != nil {
		t.Errorf("expected no success response on rejection, got %v", resp)
	}
	if !cmn.IsErrTooManyRequests(err) {
		t.Fatalf("expected a too-many-requests error, got %T: %v", err, err)
	}
}

func TestDownloadRejectedHandoffStopsThrottler(t *testing.T) {
	previousStore := g.store
	g.store = &infoStore{dljobs: make(map[string]*dljob)}
	t.Cleanup(func() { g.store = previousStore })

	xdl := &Xact{dispatcher: &dispatcher{workCh: make(chan jobif)}}
	job := &singleDlJob{sliceDlJob: sliceDlJob{
		baseDlJob: baseDlJob{id: "rejected-throttled", xdl: xdl},
	}}
	job.throt.init(Limits{BytesPerHour: 60 * 1024 * 1024}) // starts goroutine + ticker
	t.Cleanup(job.throt.stop)

	if _, status, _ := xdl.Download(job); status != http.StatusTooManyRequests {
		t.Fatalf("expected HTTP 429, got %d", status)
	}
	select {
	case <-job.throt.stopCh.Listen():
	default:
		t.Fatal("rejected handoff left the job's throttler running")
	}
}
