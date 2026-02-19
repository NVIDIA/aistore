// Package cos_test: unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// computeCRC32C computes CRC32C checksum using cos methods
func computeCRC32C(t *testing.T, data []byte) uint32 {
	t.Helper()
	h := cos.NewCksumHash(cos.ChecksumCRC32C)
	h.H.Write(data)
	h.Finalize()
	val, err := strconv.ParseUint(h.Value(), 16, 32)
	if err != nil {
		t.Fatalf("computeCRC32C: failed to parse checksum value %q: %v", h.Value(), err)
	}
	return uint32(val)
}

// randomBytes generates random bytes of given length using the provided rng
func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

func TestCRC32CCombine(t *testing.T) {
	tests := []struct {
		name string
		a, b []byte
	}{
		{"simple", []byte("hello"), []byte("world")},
		{"empty_b", []byte("hello"), []byte{}},
		{"empty_a", []byte{}, []byte("world")},
		{"both_empty", []byte{}, []byte{}},
		{"single_byte", []byte{0x42}, []byte{0x43}},
		{"large", make([]byte, 10000), make([]byte, 20000)},
	}

	// Fill large test data
	for i := range tests[5].a {
		tests[5].a[i] = byte(i % 256)
	}
	for i := range tests[5].b {
		tests[5].b[i] = byte((i * 7) % 256)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Compute CRC32C of concatenated data directly
			combined := make([]byte, 0, len(tc.a)+len(tc.b))
			combined = append(combined, tc.a...)
			combined = append(combined, tc.b...)
			expected := computeCRC32C(t, combined)

			// Compute CRC32C of parts and combine
			crc1 := computeCRC32C(t, tc.a)
			crc2 := computeCRC32C(t, tc.b)
			got := cos.CRC32CCombine(crc1, crc2, int64(len(tc.b)))

			if got != expected {
				t.Errorf("CRC32CCombine(%08x, %08x, %d) = %08x, want %08x",
					crc1, crc2, len(tc.b), got, expected)
			}
		})
	}
}

func TestCRC32CCombineMultipleParts(t *testing.T) {
	parts := [][]byte{
		[]byte("part1"),
		[]byte("part2-longer"),
		[]byte("p3"),
		[]byte("part4-even-longer-data"),
	}

	// Compute expected: CRC32C of all parts concatenated
	var all []byte
	for _, p := range parts {
		all = append(all, p...)
	}
	expected := computeCRC32C(t, all)

	// Compute by combining parts one by one
	var combined uint32
	for _, p := range parts {
		crc := computeCRC32C(t, p)
		combined = cos.CRC32CCombine(combined, crc, int64(len(p)))
	}

	if combined != expected {
		t.Errorf("Multi-part combine = %08x, want %08x", combined, expected)
	}
}

// TestCRC32CCombineRandomized tests CRC32C combine with random data and random split points
func TestCRC32CCombineRandomized(t *testing.T) {
	const (
		iterations = 100
		maxSize    = 100000 // 100KB max
	)

	rng := rand.New(rand.NewPCG(42, 42)) // deterministic for reproducibility

	for i := range iterations {
		// Generate random data of random size
		size := rng.IntN(maxSize) + 1
		data := randomBytes(rng, size)

		// Pick a random split point
		splitAt := rng.IntN(size + 1) // can be 0 or size (edge cases)
		a, b := data[:splitAt], data[splitAt:]

		// Method 1: Direct CRC32C of whole data
		expected := computeCRC32C(t, data)

		// Method 2: Combine CRC32C of parts
		crc1 := computeCRC32C(t, a)
		crc2 := computeCRC32C(t, b)
		got := cos.CRC32CCombine(crc1, crc2, int64(len(b)))

		if got != expected {
			t.Fatalf("iteration %d: size=%d split=%d: CRC32CCombine(%08x, %08x, %d) = %08x, want %08x",
				i, size, splitAt, crc1, crc2, len(b), got, expected)
		}
	}
}

// TestCRC32CCombineAssociativity verifies that combining is associative:
// combine(combine(A,B), C) == combine(A, combine(B,C)) when done correctly
func TestCRC32CCombineAssociativity(t *testing.T) {
	const iterations = 50

	rng := rand.New(rand.NewPCG(456, 456))

	for i := range iterations {
		// Generate 3 random parts
		a := randomBytes(rng, rng.IntN(5000)+1)
		b := randomBytes(rng, rng.IntN(5000)+1)
		c := randomBytes(rng, rng.IntN(5000)+1)

		// Direct computation
		var all []byte
		all = append(all, a...)
		all = append(all, b...)
		all = append(all, c...)
		expected := computeCRC32C(t, all)

		crcA := computeCRC32C(t, a)
		crcB := computeCRC32C(t, b)
		crcC := computeCRC32C(t, c)

		// Method 1: ((A + B) + C)
		ab := cos.CRC32CCombine(crcA, crcB, int64(len(b)))
		result1 := cos.CRC32CCombine(ab, crcC, int64(len(c)))

		// Method 2: Compute CRC of (A+B) directly, then combine with C
		abData := append(append([]byte{}, a...), b...)
		crcAB := computeCRC32C(t, abData)
		result2 := cos.CRC32CCombine(crcAB, crcC, int64(len(c)))

		if result1 != expected {
			t.Fatalf("iteration %d: method1 ((A+B)+C) = %08x, want %08x", i, result1, expected)
		}
		if result2 != expected {
			t.Fatalf("iteration %d: method2 (AB+C) = %08x, want %08x", i, result2, expected)
		}
	}
}
