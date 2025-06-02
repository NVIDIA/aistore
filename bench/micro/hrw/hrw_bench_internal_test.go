//go:build hrw

// Package hrw provides a way to benchmark different HRW variants.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */

package hrw

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var resultInt int

func BenchmarkHRW(b *testing.B) {
	randGen := cos.NowRand()

	hashFuncs := []hashFuncs{
		{name: "hrwXXHash", hashF: hrwXXHash},
		{name: "hrwXXHashWithAppend", hashF: hrwXXHashWithAppend},
		{name: "hrwXXHash+XorShift", hashF: hrwHybridXXHashXorshift},
		{name: "hrwXXHash+Xoshiro", hashF: hrwHybridXXHashXoshiro256},
	}

	// Length of name: {256, 512, 1024}
	for e := uint(8); e <= 10; e++ {
		nameLen := 1 << e
		// Number of nodes: {16, 32, 64}
		for k := uint(4); k <= 6; k++ {
			numNodes := 1 << k
			nodes := randNodeIDs(numNodes, randGen)
			fileName := randFileName(randGen, nameLen)
			for _, hashFunc := range hashFuncs {
				b.Run(fmt.Sprintf("%s/%d/%d", hashFunc.name, numNodes, nameLen), func(b *testing.B) {
					var nodeID int
					for b.Loop() {
						// Record the result to prevent the compiler
						// eliminating the function call.
						nodeID = hashFunc.hashF(fileName, nodes)
					}
					// Store the result to a package level variable so the
					// compiler cannot eliminate the Benchmark itself.
					resultInt = nodeID
				})
			}
		}
	}
}

func TestEqualDistribution(t *testing.T) {
	// Testing for the previous approach as well as the new approach
	// since if both fail the test then the hash algorithm isn't the cause.
	hashFs := []hashFuncs{
		{name: "hrwXXHash+Xoshiro", hashF: hrwHybridXXHashXoshiro256},
		{name: "hrwXXHash", hashF: hrwXXHash},
	}

	seed := time.Now().UnixNano()
	t.Logf("Seed: %d", seed)

	numRoutines := 1000
	objsPerRoutine := 1000
	// We don't want to run for a large number of objects since it takes longer.
	totalObjs := numRoutines * objsPerRoutine // 1 million objects
	threshold := 0.0025 * float64(totalObjs)  // 0.25% = +/- 2.5k objects

	for _, useSimilarNames := range []bool{true, false} {
		// Number of nodes: {4, 8, 16, 32, 64}
		for k := uint(2); k <= 6; k++ {
			numNodes := 1 << k

			for idx := range hashFs {
				hashFs[idx].countObjs = make([]int, numNodes)
			}

			objDist := get3DSlice(numRoutines, len(hashFs), numNodes)

			var wg sync.WaitGroup
			wg.Add(numRoutines)
			for r := range numRoutines {
				go func(r int, hashFs []hashFuncs, dist [][]int) {
					defer wg.Done()
					invokeHashFunctions(seed+int64(r+10), objsPerRoutine, numNodes, useSimilarNames, hashFs, dist)
				}(r, hashFs, objDist[r])
			}
			wg.Wait()

			mean := float64(totalObjs) / float64(numNodes)

			maxDiff := -1.0
			for f, hashFunc := range hashFs {
				sum := 0
				for r := range numRoutines {
					for idx := range numNodes {
						hashFunc.countObjs[idx] += objDist[r][f][idx]
						sum += objDist[r][f][idx]
					}
				}
				if sum != totalObjs {
					t.Fatalf("Expected objects: %d, Actual objects: %d", totalObjs, sum)
				}

				for _, c := range hashFunc.countObjs {
					diff := math.Abs(mean - float64(c))
					if diff > threshold {
						t.Errorf("Name: %s, Nodes: %d, Mean: %f, Actual: %d, Threshold: %f, Diff: %f",
							hashFunc.name, numNodes, mean, c, threshold, diff)
					}
					if diff > maxDiff {
						maxDiff = diff
					}
				}
				t.Logf("Name: %s, MaxDiff: %f, Threshold: %f\n", hashFunc.name, maxDiff, threshold)
			}
		}

		// When you run this test multiple times using `-count x` then GC is not called until all the runs finish.
		// This will end up consuming a lot of RAM. Thus, calling GC explicitly.
		runtime.GC()
	}
}

func invokeHashFunctions(seed int64, numObjs, numNodes int, useSimilarNames bool, hashFuncs []hashFuncs, dist [][]int) {
	randGen := rand.New(cos.NewRandSource(uint64(seed)))
	nodes := randNodeIDs(numNodes, randGen)
	bucketName := randFileName(randGen, fqnMaxLen-objNameLen)
	for n := range numObjs {
		var fileName string
		if useSimilarNames {
			fileName = similarFileName(bucketName, n)
		} else {
			nameLen := randGen.IntN((1<<10)-1) + 1
			fileName = randFileName(randGen, nameLen)
		}
		for idx, hashFunc := range hashFuncs {
			dist[idx][hashFunc.hashF(fileName, nodes)]++
		}
	}
}
