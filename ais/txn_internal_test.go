// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
)

const testMaxHostBusy = 20 * time.Second

func testTxnConfig() *cmn.Config {
	config := &cmn.Config{}
	config.Timeout.MaxHostBusy = cos.Duration(testMaxHostBusy)
	return config
}

// returns the txn interface (not the concrete type) so callers exercise it exactly
// as production does - notably started(), which is interface-dispatched everywhere.
func newETLInitTxn(initTimeout time.Duration) txn {
	msg := &etl.InitSpecMsg{InitMsgBase: etl.InitMsgBase{InitTimeout: cos.Duration(initTimeout)}}
	c := &txnSrv{t: t, uuid: "test-etl-init", msg: &actMsgExt{ActMsg: apc.ActMsg{Action: apc.ActETLInline}}}
	return newTxnETLInit(c, msg)
}

// a representative non-ETL txn: inherits txnBase.beginTimeout unchanged.
func newCreateBucketTxn() txn {
	bck := meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)
	c := &txnSrv{t: t, bck: bck, uuid: "test-create-bck", msg: &actMsgExt{ActMsg: apc.ActMsg{Action: apc.ActCreateBck}}}
	return newTxnCreateBucket(c)
}

// beginTimeout: ETL respects init_timeout (+ slack); everything else is generic.
func TestBeginTimeout(t *testing.T) {
	config := testTxnConfig()
	slack := txnTimeoutMult * testMaxHostBusy // begin->commit slack added on top of init_timeout
	generic := txnTimeoutMult * testMaxHostBusy

	tests := []struct {
		name   string
		txn    txn
		expect time.Duration
	}{
		{"non-etl-create-bucket", newCreateBucketTxn(), generic},
		{"etl-5m", newETLInitTxn(5 * time.Minute), 5*time.Minute + slack},
		{"etl-default-45s", newETLInitTxn(etl.DefaultInitTimeout), etl.DefaultInitTimeout + slack},
		{"etl-zero-falls-back", newETLInitTxn(0), generic}, // guard: never below generic
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.txn.beginTimeout(config); got != tc.expect {
				t.Fatalf("beginTimeout = %v, want %v", got, tc.expect)
			}
		})
	}
}

// checkTimeout (begin phase): the actual GC decision. This is the regression guard:
// pre-fix, the deadline was a flat 2*max_host_busy (40s) for ALL txns, so a slow ETL
// init was reaped mid-bootstrap. Post-fix, ETL init survives until init_timeout+slack.
func TestCheckTimeoutBeginPhaseETL(t *testing.T) {
	config := testTxnConfig()
	begin := time.Now()
	slack := txnTimeoutMult * testMaxHostBusy

	tests := []struct {
		name        string
		initTimeout time.Duration
		elapsed     time.Duration
		wantReaped  bool
	}{
		// 5m init_timeout, still booting at 2m. Old code reaped at 40s.
		{"etl-5m-at-2m-survives", 5 * time.Minute, 2 * time.Minute, false},
		// boundary: just inside vs just past init_timeout+slack.
		{"etl-5m-just-inside", 5 * time.Minute, 5*time.Minute + slack - time.Second, false},
		{"etl-5m-just-past", 5 * time.Minute, 5*time.Minute + slack + time.Second, true},
		// even a default (45s) ETL init must outlive the old 40s flat deadline.
		{"etl-default-at-41s-survives", etl.DefaultInitTimeout, 41 * time.Second, false},
		{"etl-default-past-deadline", etl.DefaultInitTimeout, etl.DefaultInitTimeout + slack + time.Second, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txn := newETLInitTxn(tc.initTimeout)
			txn.started(apc.Begin2PC, begin)
			err, _ := checkTimeout(txn, begin.Add(tc.elapsed), config)
			if tc.wantReaped {
				if err == nil {
					t.Fatalf("expected GC reap at elapsed=%v (init_timeout=%v), got nil", tc.elapsed, tc.initTimeout)
				}
				if !strings.Contains(err.Error(), "[begin - start-commit] timeout") {
					t.Fatalf("unexpected reap error: %v", err)
				}
			} else if err != nil {
				t.Fatalf("ETL init must NOT be reaped at elapsed=%v (init_timeout=%v): %v", tc.elapsed, tc.initTimeout, err)
			}
		})
	}
}

// checkTimeout (begin phase): non-ETL transactions keep the generic deadline (unchanged
// behavior). Uses an ETL txn with init_timeout=0 as the stand-in for the default path,
// since beginTimeout then returns exactly 2*max_host_busy like every txnBase-backed type.
func TestCheckTimeoutBeginPhaseGeneric(t *testing.T) {
	config := testTxnConfig()
	begin := time.Now()
	deadline := txnTimeoutMult * testMaxHostBusy // 40s

	txn := newETLInitTxn(0) // init_timeout=0 -> generic beginTimeout (== txnBase default)
	txn.started(apc.Begin2PC, begin)

	if err, _ := checkTimeout(txn, begin.Add(deadline-time.Second), config); err != nil {
		t.Fatalf("generic txn within %v deadline must survive: %v", deadline, err)
	}
	if err, _ := checkTimeout(txn, begin.Add(deadline+time.Second), config); err == nil {
		t.Fatalf("generic txn past %v deadline must be reaped", deadline)
	}
}

// checkTimeout (commit phase): the post-commit branch is type-agnostic and must remain
// governed by max_host_busy regardless of the begin-phase change above.
func TestCheckTimeoutCommitPhaseUnaffected(t *testing.T) {
	config := testTxnConfig()
	begin := time.Now()

	txn := newETLInitTxn(5 * time.Minute) // long init_timeout must NOT leak into commit phase
	txn.started(apc.Begin2PC, begin)
	txn.started(apc.Commit2PC, begin)

	// under 2*max_host_busy after commit -> not reaped
	if err, _ := checkTimeout(txn, begin.Add(txnTimeoutMult*testMaxHostBusy-time.Second), config); err != nil {
		t.Fatalf("commit-phase txn within deadline must survive: %v", err)
	}
	// just past 2*max_host_busy -> flagged even though init_timeout=5m (proves no leak)
	err, _ := checkTimeout(txn, begin.Add(txnTimeoutMult*testMaxHostBusy+time.Second), config)
	if err == nil || !strings.Contains(err.Error(), "commit is taking too long") {
		t.Fatalf("commit-phase must use max_host_busy, not init_timeout; got: %v", err)
	}
	// past gcTxnsTimeoutMult*max_host_busy -> terminal commit-phase reap
	err, _ = checkTimeout(txn, begin.Add(gcTxnsTimeoutMult*testMaxHostBusy+time.Second), config)
	if err == nil || !strings.Contains(err.Error(), "[commit - done] timeout") {
		t.Fatalf("expected [commit - done] timeout, got: %v", err)
	}
}
