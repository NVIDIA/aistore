// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"testing"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
	"github.com/NVIDIA/aistore/cmn/cos"
)

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
