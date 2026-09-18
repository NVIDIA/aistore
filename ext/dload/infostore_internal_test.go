// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/hk"
)

func TestDownloadHandoffJobLifecycle(t *testing.T) {
	previousStore := g.store
	g.store = &infoStore{dljobs: make(map[string]*dljob)}
	t.Cleanup(func() { g.store = previousStore })

	xdl := &Xact{dispatcher: &dispatcher{workCh: make(chan jobif)}}
	job := &singleDlJob{sliceDlJob: sliceDlJob{
		baseDlJob: baseDlJob{id: "handoff-lifecycle", xdl: xdl},
	}}

	// With no receiver the submission must time out. Rejection must leave
	// neither a status record nor a phantom running job in list responses.
	_, status, _ := xdl.Download(job)
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected HTTP 429, got %d", status)
	}
	if _, err := g.store.getJob(job.ID()); !errors.Is(err, errJobNotFound) {
		t.Fatalf("rejected job remains in the store: %v", err)
	}
	for _, activeOnly := range []bool{false, true} {
		if jobs := g.store.getList(&request{onlyActive: activeOnly}); len(jobs) != 0 {
			t.Fatalf("rejected job remains in list (activeOnly=%t): %d jobs", activeOnly, len(jobs))
		}
	}

	// A subsequent successful handoff must retain its record, already visible
	// when the dispatcher receives the job.
	received := make(chan error, 1)
	stopReceiver := make(chan struct{})
	defer close(stopReceiver)
	store := g.store
	go func() {
		select {
		case accepted := <-xdl.dispatcher.workCh:
			_, err := store.getJob(accepted.ID())
			received <- err
		case <-stopReceiver:
		}
	}()
	resp, status, err := xdl.Download(job)
	if status != http.StatusOK || err != nil || resp != job.ID() {
		t.Fatalf("expected successful handoff, got (%v, %d, %v)", resp, status, err)
	}
	if err := <-received; err != nil {
		t.Fatalf("accepted job was not registered before handoff: %v", err)
	}
}

func TestDownloadHousekeepPreservesLiveJobs(t *testing.T) {
	driver, err := kvdb.NewBuntDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	store := &infoStore{
		downloaderDB: newDownloadDB(driver),
		dljobs:       make(map[string]*dljob),
	}
	now := time.Now()
	for _, id := range []string{"running", "aborting", "recent", "expired"} {
		store.dljobs[id] = &dljob{id: id, startedTime: now.Add(-2 * hk.DayInterval)}
	}
	// An abort request is not completion: workers may still update this job.
	store.dljobs["aborting"].aborted.Store(true)
	store.dljobs["recent"].finishedTime.Store(now)
	store.dljobs["expired"].finishedTime.Store(now.Add(-2 * hk.DayInterval))

	store.housekeep(0)

	for _, id := range []string{"running", "aborting", "recent"} {
		if _, err := store.getJob(id); err != nil {
			t.Errorf("housekeeping removed %s job: %v", id, err)
		}
	}
	if _, err := store.getJob("expired"); !errors.Is(err, errJobNotFound) {
		t.Errorf("housekeeping retained expired job: %v", err)
	}
}
