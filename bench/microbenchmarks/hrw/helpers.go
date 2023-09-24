// Package hrw provides a way to benchmark different HRW variants.
// See /bench/hrw/README.md for more info.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package hrw

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/OneOfOne/xxhash"
)

type hashFuncs struct {
	name      string
	hashF     func(string, []node) int
	countObjs []int
}

const (
	objNameLen = 50
	fqnMaxLen  = 128
)

// Duplicated on purpose to avoid dependency on any AIStore code.
func randFileName(src *rand.Rand, nameLen int) string {
	const (
		letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	b := make([]byte, nameLen)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := nameLen-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func similarFileName(bucketName string, objNum int) string {
	paddedLen := objNameLen - len(strconv.Itoa(objNum)) - len("set-")
	objectName := fmt.Sprintf("set-%0*d", paddedLen, objNum)
	return bucketName + "/" + objectName
}

// Duplicated on purpose to avoid dependency on any AIStore code.
func randNodeID(randGen *rand.Rand) string {
	randIP := ""
	for i := 0; i < 3; i++ {
		randIP += strconv.Itoa(randGen.Intn(255)) + "."
	}
	randIP += strconv.Itoa(randGen.Intn(255))
	cksum := xxhash.Checksum32S(cos.UnsafeB(randIP), xxHashSeed)
	nodeID := strconv.Itoa(int(cksum & 0xfffff))
	randPort := strconv.Itoa(randGen.Intn(65535))
	return nodeID + "_" + randPort
}

func randNodeIDs(numNodes int, randGen *rand.Rand) []node {
	nodes := make([]node, numNodes)
	for i := 0; i < numNodes; i++ {
		id := randNodeID(randGen)
		xhash := xxhash.NewS64(xxHashSeed)
		xhash.WriteString(id)
		seed := xxhash.Checksum64S(cos.UnsafeB(id), xxHashSeed)
		nodes[i] = node{
			id:          id,
			idDigestInt: xorshift64(seed),
			idDigestXX:  xhash,
		}
	}
	return nodes
}

func get3DSlice(numRoutines, numFuncs, numNodes int) [][][]int {
	perRoutine := make([][][]int, numRoutines)
	for w := 0; w < numRoutines; w++ {
		perFunc := make([][]int, numFuncs)
		for p := range perFunc {
			perFunc[p] = make([]int, numNodes)
		}
		perRoutine[w] = perFunc
	}
	return perRoutine
}

func xorshift64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
