// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"testing"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestPostNewWorkOrderEpochs(t *testing.T) {
	oldParams, oldGetter, oldWorkCh := runParams, objnameGetter, workCh
	oldPosted, oldTotal := epochReadsPosted, totalWOs
	oldGet, oldBatch, oldMpd := getPending, getBatchPending, mpdStreamPending
	t.Cleanup(func() {
		runParams, objnameGetter, workCh = oldParams, oldGetter, oldWorkCh
		epochReadsPosted, totalWOs = oldPosted, oldTotal
		getPending, getBatchPending, mpdStreamPending = oldGet, oldBatch, oldMpd
	})

	names := []string{"a", "b", "c", "d"}
	runParams = &params{workloadParams: workloadParams{numEpochs: 1}}
	runParams.epochReadLimit = int64(runParams.numEpochs) * int64(len(names))

	objnameGetter = &namegetter.PermShuffle{}
	workCh = make(chan *workOrder, 1)

	for _, test := range []struct {
		name                  string
		op, batchSize, mpdPct int
		want                  []int
	}{
		{name: "GET", op: opGet, want: []int{1, 1, 1, 1}},
		{name: "MPD stream", op: opGetMpdStream, mpdPct: 100, want: []int{1, 1, 1, 1}},
		{name: "GetBatch with full batches", op: opGetBatch, batchSize: 2, want: []int{2, 2}},
		{name: "GetBatch with remainder", op: opGetBatch, batchSize: 3, want: []int{3, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runParams.getBatchSize = test.batchSize
			runParams.mpdStreamPct = test.mpdPct
			objnameGetter.Init(names, cos.NowRand())
			epochReadsPosted = 0 // postNewWorkOrder increments this counter.

			for i, want := range test.want {
				before := epochReadsPosted
				tassert.CheckFatal(t, postNewWorkOrder())
				wo := <-workCh
				defer freeWO(wo)
				tassert.Fatalf(t, wo.op == test.op, "operation = %d, want %d", wo.op, test.op)
				if wo.op == opGetBatch {
					got := len(wo.moss.In)
					tassert.Fatalf(t, got == want, "batch %d: got %d objects, want %d", i, got, want)
				}
				posted := epochReadsPosted - before
				tassert.Fatalf(t, posted == int64(want), "batch %d: posted %d reads, want %d", i, posted, want)
			}
			tassert.Fatalf(t, epochReadsPosted == runParams.epochReadLimit,
				"total reads: got %d, want %d", epochReadsPosted, runParams.epochReadLimit)
		})
	}
}

// shouldUsePercentage: verifies percentages outside the open interval (0, 100)
// short-circuit without consulting the hashed selection.
func TestShouldUsePercentage(t *testing.T) {
	for _, pct := range []int{-1, 0} {
		if shouldUsePercentage(pct) {
			t.Fatalf("shouldUsePercentage(%d) returned true", pct)
		}
	}
	for _, pct := range []int{100, 101} {
		if !shouldUsePercentage(pct) {
			t.Fatalf("shouldUsePercentage(%d) returned false", pct)
		}
	}
}

// encodeArchName/decodeArchName: verifies object and archive paths survive a round trip.
func TestArchiveNameRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		objName  string
		archPath string
	}{
		{name: "object only", objName: "object"},
		{name: "archive member", objName: "shards/data.tar", archPath: "images/001.jpg"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := encodeArchName(test.objName, test.archPath)
			objName, archPath := decodeArchName(encoded)
			if objName != test.objName || archPath != test.archPath {
				t.Fatalf("round trip = (%q, %q), expected (%q, %q)",
					objName, archPath, test.objName, test.archPath)
			}
		})
	}
}

// newGetWorkOrder: with '-readoff=random', objects not larger than readlen are read from offset 0;
// larger objects get offsets in [0, size-readlen] that are not always 0.
func TestNewGetWorkOrderRandReadOff(t *testing.T) {
	const readLen = cos.KiB
	oldParams, oldRnd, oldSizes, oldGetter := runParams, readOffRnd, objSizes, objnameGetter
	t.Cleanup(func() {
		runParams, readOffRnd, objSizes, objnameGetter = oldParams, oldRnd, oldSizes, oldGetter
	})
	runParams = &params{readParams: readParams{readOffRandom: true, readLen: readLen}}
	readOffRnd = cos.NowRand()
	objSizes = map[string]int64{"smaller": readLen / 2, "equal": readLen, "larger": 4 * readLen}
	objnameGetter = &namegetter.Random{}
	objnameGetter.Init([]string{"smaller", "equal", "larger"}, cos.NowRand())

	var nonZero bool
	for range 100 {
		wo, err := newGetWorkOrder(opGet)
		if err != nil {
			t.Fatal(err)
		}
		size := objSizes[wo.objName]
		switch {
		case size <= readLen:
			if wo.readOff != 0 {
				t.Fatalf("%s (size %d): offset %d, expected 0", wo.objName, size, wo.readOff)
			}
		case wo.readOff < 0 || wo.readOff > size-readLen:
			t.Fatalf("%s (size %d): offset %d out of range [0, %d]", wo.objName, size, wo.readOff, size-readLen)
		case wo.readOff > 0:
			nonZero = true
		}
		freeWO(wo)
	}
	if !nonZero {
		t.Fatal("expected at least one non-zero offset for objects larger than readlen")
	}
}
