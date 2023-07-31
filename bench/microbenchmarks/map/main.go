// Package main
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// =============Preamble================
// This benchmark tests the performance of regular maps versus sync.Maps focusing on the
// primitive operations of get, put, and delete while under contention. For the use case of
// AIStore, we want the read times to be as fast as possible.
//
// The design of the benchmark is as follows:
// 1. Fill the map to a max size (default 1000000)
// 2. Randomly get X (default 200000) elements from the map where p% (default 10%) of them will be a random store
// 3. Randomly delete (max size - min size) elements from the map
// All of these operations will happen concurrently with step 2 having the option to spawn more
// go routines (default 8). Additionally, in all cases, the elements being read from the maps,
// put into the maps, and deleted from the maps will be the same between the two maps.
//
// Example of invocation:
// go run main.go -numread 300000 -maxsize 1000000 -minsize 50000 -putpct 10 -workers 8
// go run main.go -numread 100000 -maxsize 1000000 -minsize 50000 -putpct 20 -workers 20 -shrink=false (no deletion)

// ===========Results===================
// From the results of the benchmark, sync.Map does appear to out perform regular maps for read operations
// when there is contention from multiple go routines. The performance of sync.Map for put and delete operations are usually slower than
// regular maps, but if we expect > 80% of the requests to be reads, sync.Map will be faster than regular maps.
// Overall, if we expect that the maps will have much higher read requests than put/delete requests, sync.Maps are
// the better option.

type (
	benchMap interface {
		get(int) int
		put(node)
		delete(int)
		mapType() string
	}
	node struct {
		key, val int
	}

	// Maps
	regMap struct {
		m map[int]int
		// Regular map RWMutex
		mu sync.RWMutex
	}

	syncMap struct {
		m sync.Map
	}
	params struct {
		maxSize    int  // Max size of map
		minSize    int  // Shrink size of map
		numread    int  // Number of random reads
		iterations int  // Number of iterations to run
		putpct     int  // Percentage of puts
		workers    int  // Number of Read/Write Goroutines
		doShrink   bool // Enable shrinking (Deleting) of keys in map
	}
)

var (
	// Params
	runParams params
	flagUsage bool

	nodeList []node
	readList [][]int
	delList  [][]int
)

// =================sync.Map===================
func newSyncMap() *syncMap {
	return &syncMap{}
}

func (m *syncMap) get(key int) int {
	val, _ := m.m.Load(key)
	if val == nil {
		return 0
	}
	return val.(int)
}

func (m *syncMap) put(n node) {
	m.m.Store(n.key, n.val)
}

func (m *syncMap) delete(key int) {
	m.m.Delete(key)
}

func (*syncMap) mapType() string {
	return "Sync.Map"
}

// =================Regular Map===================
func newRegMap() *regMap {
	return &regMap{make(map[int]int), sync.RWMutex{}}
}

func (m *regMap) get(key int) int {
	m.mu.RLock()
	val := m.m[key]
	m.mu.RUnlock()
	return val
}

func (m *regMap) put(n node) {
	m.mu.Lock()
	m.m[n.key] = n.val
	m.mu.Unlock()
}

func (m *regMap) delete(key int) {
	m.mu.Lock()
	delete(m.m, key)
	m.mu.Unlock()
}

func (*regMap) mapType() string {
	return "Reg.Map"
}

func parseCmdLine() (p params, err error) {
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // Discard flags of imported packages

	// Command line options
	f.BoolVar(&flagUsage, "usage", false, "Show extensive help menu with examples and exit")
	f.BoolVar(&p.doShrink, "shrink", true, "Enable shrink (deletion) of map keys")
	f.IntVar(&p.maxSize, "maxsize", 1000000, "Maximum size of map")
	f.IntVar(&p.minSize, "minsize", 500000, "Shrink size of map")
	f.IntVar(&p.numread, "numread", 300000, "Number of random reads")
	f.IntVar(&p.iterations, "iter", 10, "Number of iterations to run")
	f.IntVar(&p.putpct, "putpct", 10, "Percentage of puts")
	f.IntVar(&p.workers, "workers", 8, "Number of Read/Write go-routines")

	f.Parse(os.Args[1:])

	if len(os.Args[1:]) == 0 {
		fmt.Println("Flags: ")
		f.PrintDefaults()
		fmt.Println()
		os.Exit(0)
	}

	os.Args = []string{os.Args[0]}
	flag.Parse() // Called so that imported packages don't compain

	if p.minSize > p.maxSize {
		return params{}, fmt.Errorf("minsize %d greater than maxsize %d", p.minSize, p.maxSize)
	}

	if p.putpct < 0 || p.putpct > 100 {
		return params{}, fmt.Errorf("invalid putpct: %d", p.putpct)
	}

	return
}

func init() {
	var err error
	runParams, err = parseCmdLine()
	if err != nil {
		fmt.Fprintln(os.Stdout, "err: "+err.Error())
		os.Exit(2)
	}

	fmt.Printf("Map size: %d\t Min size: %d\t Read count: %d\t Iterations: %d\t Put Pct: %d\t Workers: %d\n\n",
		runParams.maxSize, runParams.minSize, runParams.numread, runParams.iterations, runParams.putpct, runParams.workers)
	nodeList = createNodeList(runParams.maxSize)
	readList = createReadDelList(nodeList, runParams.numread, runParams.iterations)
	delList = createReadDelList(nodeList, runParams.maxSize-runParams.minSize, runParams.iterations)
}

/* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
func randInt(r int) int {
	r ^= r << 13
	r ^= r >> 17
	r ^= r << 5
	return r & 0x7fffffff
}

// Create a node struct where key and value are the same (it should not matter)
func createNodeList(size int) (nodeList []node) {
	nodeList = make([]node, 0, size)
	for i := 0; i < size; i++ {
		nodeList = append(nodeList, node{key: i, val: randInt(i)})
	}
	return
}

// Creates a list of keys (int)
func createList(nodeList []node, size int) (keys []int) {
	nodeLen := len(nodeList)
	keys = make([]int, 0, size)

	for i := 0; i < size; i++ {
		keys = append(keys, cos.NowRand().Intn(nodeLen))
	}
	return
}

// Creates a list of list of keys
func createReadDelList(nodeList []node, size, iterations int) (keysList [][]int) {
	wg := &sync.WaitGroup{}

	keysList = make([][]int, iterations)
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(index int) {
			keysList[index] = createList(nodeList, size)
			wg.Done()
		}(i)
	}
	wg.Wait()
	return
}

func fill(m benchMap, nodeList []node) time.Duration {
	start := time.Now()
	for _, ele := range nodeList {
		m.put(ele)
	}
	return time.Since(start)
}

func shrink(m benchMap, keys []int) time.Duration {
	start := time.Now()
	for _, ele := range keys {
		m.delete(ele)
	}
	return time.Since(start)
}

func readNWrite(m benchMap, threshold int, keys []int, dur chan time.Duration) {
	start := time.Now()
	for _, key := range keys {
		// Randomly have some puts
		if key%100 >= (100-threshold) || key%100 <= (-100+threshold) {
			m.put(node{key: key, val: key})
		} else {
			m.get(key)
		}
	}
	dur <- time.Since(start)
}

// Benchmark
// 1. Fill map
// 2. Concurrently run X go routines that reads and occasionally writes
// 3. Shrink
func bench(m benchMap, nodeList []node, readList, delList [][]int, iterations, threshold, workers int, doShrink bool) {
	var (
		wg           = &sync.WaitGroup{}
		fillDur      time.Duration
		shrinkDur    time.Duration
		readWriteDur time.Duration
		totDur       time.Duration

		totFil    time.Duration
		totRW     time.Duration
		totShrink time.Duration
		totTot    time.Duration
	)
	fmt.Printf("Fill, Read + Random Write and Shrink with %s\n", m.mapType())

	for i := 0; i < iterations; i++ {
		start := time.Now()
		readDurCh := make(chan time.Duration, workers)
		wg.Add(1)
		go func() {
			fillDur = fill(m, nodeList)
			wg.Done()
		}()

		for j := 0; j < workers; j++ {
			wg.Add(1)
			go func(idx int) {
				readNWrite(m, threshold, readList[idx], readDurCh)
				wg.Done()
			}(i)
		}

		if doShrink {
			wg.Add(1)
			go func(idx int) {
				shrinkDur = shrink(m, delList[idx])
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(readDurCh)
		for dur := range readDurCh {
			readWriteDur += dur
		}
		totDur = time.Since(start)

		averageRW := time.Duration(int(readWriteDur) / workers)
		fmt.Printf("%s | Fill: %s\t Read+Write: %s\t Shrink: %s\t Total: %s\n",
			m.mapType(), fillDur, averageRW, shrinkDur, totDur)
		readWriteDur = 0
		totFil += fillDur
		totRW += averageRW
		totShrink += shrinkDur
		totTot += totDur
	}
	fmt.Println()
	fmt.Printf("%s | Avg. Fill: %s\t Avg. Read+Write: %s\t Avg. Shrink: %s\t Avg. Total: %s\n",
		m.mapType(),
		time.Duration(int(totFil)/iterations),
		time.Duration(int(totRW)/iterations),
		time.Duration(int(totShrink)/iterations),
		time.Duration(int(totTot)/iterations))
	fmt.Println()
}

func main() {
	sMap := newSyncMap()
	rMap := newRegMap()

	bench(sMap, nodeList, readList, delList, runParams.iterations, runParams.putpct, runParams.workers, runParams.doShrink)
	bench(rMap, nodeList, readList, delList, runParams.iterations, runParams.putpct, runParams.workers, runParams.doShrink)
}
