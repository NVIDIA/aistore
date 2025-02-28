// Package hrw provides a way to benchmark different HRW variants.
// See /bench/hrw/README.md for more info.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package hrw

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
	onexxh "github.com/OneOfOne/xxhash"
)

const xxHashSeed = 1103515245

type node struct {
	id          string
	idDigestInt uint64
	idDigestXX  *onexxh.XXHash64
}

func hrwXXHash(key string, nodes []node) int {
	var maxCksum uint64
	var destIdx int
	for idx, node := range nodes {
		cksum := onexxh.Checksum64S(cos.UnsafeB(node.id+":"+key), xxHashSeed)
		if cksum > maxCksum {
			maxCksum = cksum
			destIdx = idx
		}
	}

	return destIdx
}

// This turns out to be actually slower than the case without the append.
// Possible reason is that in the case without the append - the method
// `Checksum64S` would iterate over the entire string just once.
// Whereas in this case with append, there are a few additional steps:
// 1. Copying the object pointed by the stored pointer.
// 2. Writing the key to the current hash. `WriteString`
// 3. Now, iterating from the beginning to compute the entire hash. `Sum64`
func hrwXXHashWithAppend(key string, nodes []node) int {
	var maxCksum uint64
	var destIdx int
	for idx, node := range nodes {
		// node.hash equals Checksum64S(node.id, xxHashSeed)
		xxhashNode := *node.idDigestXX
		xxhashNode.WriteString(":" + key)
		cksum := xxhashNode.Sum64()
		if cksum > maxCksum {
			maxCksum = cksum
			destIdx = idx
		}
	}

	return destIdx
}

func hrwHybridXXHashXorshift(key string, nodes []node) int {
	keyHash := onexxh.Checksum64S(cos.UnsafeB(":"+key), xxHashSeed)

	var maxCksum uint64
	var destIdx int
	for idx, node := range nodes {
		// node.hash equals Checksum64S(node.id, xxHashSeed)
		cksum := xorshift64(node.idDigestInt ^ keyHash)
		if cksum > maxCksum {
			maxCksum = cksum
			destIdx = idx
		}
	}

	return destIdx
}

func hrwHybridXXHashXoshiro256(key string, nodes []node) int {
	keyHash := onexxh.Checksum64S(cos.UnsafeB(":"+key), xxHashSeed)

	var maxCksum uint64
	var destIdx int
	for idx, node := range nodes {
		// node.hash equals Checksum64S(node.id, xxHashSeed)
		cksum := xoshiro256.Hash(node.idDigestInt ^ keyHash)
		if cksum > maxCksum {
			maxCksum = cksum
			destIdx = idx
		}
	}

	return destIdx
}
