// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// Smoke tests for xactions

func TestXactionRenewLRU(t *testing.T) {
	xactions := newXactions()
	defer xactions.abortAll()

	ch := make(chan *lru.Xaction, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			ch <- xactions.renewLRU()
			wg.Done()
		}()
	}

	wg.Wait()
	close(ch)

	notNilCount := 0
	for xact := range ch {
		if xact != nil {
			notNilCount++
		}
	}

	tassert.Errorf(t, notNilCount == 1, "expected just one LRU xaction to be created, got %d", notNilCount)
}

func TestXactionRenewPrefetch(t *testing.T) {
	xactions := newXactions()
	defer xactions.abortAll()

	ch := make(chan *xactPrefetch, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			ch <- xactions.renewPrefetch(nil)
			wg.Done()
		}()
	}

	wg.Wait()
	close(ch)

	notNilCount := 0
	for xact := range ch {
		if xact != nil {
			notNilCount++
		}
	}

	tassert.Errorf(t, notNilCount == 1, "expected just one Prefetch xaction to be created, got %d", notNilCount)
}

func TestXactionRenewEvictDelete(t *testing.T) {
	xactions := newXactions()
	defer xactions.abortAll()

	ch := make(chan *xactEvictDelete, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			ch <- xactions.renewEvictDelete(true)
			wg.Done()
		}()
	}

	wg.Wait()
	close(ch)

	res := make(map[*xactEvictDelete]struct{}, 10)
	for xact := range ch {
		if xact != nil {
			res[xact] = struct{}{}
		}
	}

	tassert.Errorf(t, len(res) > 0, "expected some EvictDelete xactions to be created, got %d", len(res))
}
