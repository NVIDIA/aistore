// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import "testing"

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
