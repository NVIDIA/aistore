package hrw_bench

import (
	"sync"
	"testing"

	"math/rand"
	"time"

	"fmt"
)

var resultInt int

func BenchmarkHRW(b *testing.B) {
	randGen := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
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
			nodes := getRandNodeIds(numNodes, randGen)
			fileName := randFileName(randGen, nameLen)
			for _, hashFunc := range hashFuncs {
				b.Run(fmt.Sprintf("%s/%d/%d", hashFunc.name, numNodes, nameLen), func(b *testing.B) {
					var nodeId int
					for n := 0; n < b.N; n++ {
						// Record the result to prevent the compiler
						// eliminating the function call.
						nodeId = hashFunc.hashF(fileName, nodes)
					}
					// Store the result to a package level variable so the
					// compiler cannot eliminate the Benchmark itself.
					resultInt = nodeId
				})
			}
		}
	}
}

func TestEqualDistribution(t *testing.T) {
	hashFs := []hashFuncs{
		{name: "hrwXXHash", hashF: hrwXXHash},
		{name: "hrwXXHashWithAppend", hashF: hrwXXHashWithAppend},
		{name: "hrwXXHash+XorShift", hashF: hrwHybridXXHashXorshift},
		{name: "hrwXXHash+Xoshiro", hashF: hrwHybridXXHashXoshiro256},
	}

	numRoutines := 1000
	objsPerRoutine := 1000
	totalObjs := numRoutines * objsPerRoutine

	// Number of nodes: {16, 32, 64}
	for k := uint(4); k <= 6; k++ {
		numNodes := 1 << k

		for idx := range hashFs {
			hashFs[idx].countObjs = make([]int, numNodes)
		}

		objDist := get3DSlice(numRoutines, len(hashFs), numNodes)

		var wg sync.WaitGroup
		wg.Add(numRoutines)
		for r := 0; r < numRoutines; r++ {
			go func(numObjs int, numNodes int, hashFs []hashFuncs, dist [][]int) {
				defer wg.Done()
				invokeHashFunctions(numObjs, numNodes, hashFs, dist)
			}(objsPerRoutine, numNodes, hashFs, objDist[r])
		}
		wg.Wait()

		for f, hashFunc := range hashFs {
			sum := 0
			for r := 0; r < numRoutines; r++ {
				for idx := 0; idx < numNodes; idx++ {
					hashFunc.countObjs[idx] += objDist[r][f][idx]
					sum += objDist[r][f][idx]
				}
			}
			if sum != totalObjs {
				t.Fatalf("Expected objects: %d, Actual objects: %d", totalObjs, sum)
			}
		}

		for _, hashFunc := range hashFs {
			writeOutputToFile(t, hashFunc, numNodes)
		}
	}
}

func invokeHashFunctions(numObjs, numNodes int, hashFuncs []hashFuncs, dist [][]int) {
	randGen := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	nodes := getRandNodeIds(numNodes, randGen)
	for n := 0; n < numObjs; n++ {
		nameLen := randGen.Intn((1<<10)-1) + 1
		fileName := randFileName(randGen, nameLen)
		for idx, hashFunc := range hashFuncs {
			dist[idx][hashFunc.hashF(fileName, nodes)]++
		}
	}
}
