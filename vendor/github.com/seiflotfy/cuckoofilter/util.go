package cuckoo

import (
	metro "github.com/dgryski/go-metro"
)

func getAltIndex(fp byte, i uint, numBuckets uint) uint {
	hash := uint(metro.Hash64([]byte{fp}, 1337))
	return (i ^ hash) % numBuckets
}

func getFingerprint(data []byte) byte {
	fp := byte(metro.Hash64(data, 1335)%255 + 1)
	return fp
}

// getIndicesAndFingerprint returns the 2 bucket indices and fingerprint to be used
func getIndicesAndFingerprint(data []byte, numBuckets uint) (uint, uint, byte) {
	hash := metro.Hash64(data, 1337)
	f := getFingerprint(data)
	i1 := uint(hash) % numBuckets
	i2 := getAltIndex(f, i1, numBuckets)
	return i1, i2, f
}

func getNextPow2(n uint64) uint {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return uint(n)
}
