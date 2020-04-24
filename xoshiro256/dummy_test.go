// Package xoshiro256 implements the xoshiro256** RNG
// no-copyright
package xoshiro256

import "testing"

func TestXoshiro256Hash(t *testing.T) {
	tests := []struct {
		input    uint64
		expected uint64
	}{
		{4573842, 5026071747115404967},
		{0, 1905207664160064169},
	}

	for _, test := range tests {
		if Hash(test.input) != test.expected {
			t.Errorf("wrong hash for %d, expected: %d", test.input, test.expected)
		}
	}
}
