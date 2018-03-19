/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// 'dfcloader' is a load generator for DFC.
// It sends HTTP requests to a proxy server fronting targets.
// Run with -help for usage information.

// Examples:
// 1. No put or get, just clean up:
//    dfcloader -bucket=liding-dfc -duration 0s -totalputsize=0
// 2. Time based local bucket put only:
//    dfcloader -bucket=liding-dfc -duration 10s -numworkers=3 -minsize=1024 -maxsize=1048 -pctput=100 -local=true
// 3. Put limit based cloud bucket mixed put(30%) and get(70%):
//    dfcloader -bucket=liding-dfc -duration 0s -numworkers=3 -minsize=1024 -maxsize=1048 -pctput=30 -local=false -totalputsize=10240

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/cmd/dfcloader/stats"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

const (
	opPut = iota
	opGet

	myName = "loader"
)

type (
	workOrder struct {
		op       int
		proxyURL string
		bucket   string
		isLocal  bool
		objName  string // In the format of 'virtual dir' + "/" + objname
		size     int64
		err      error
		start    time.Time
		end      time.Time
	}

	params struct {
		proxyURL          string
		isLocal           bool
		bucket            string
		putPct            int           // % of puts, rest are gets
		duration          time.Duration // Stops after run at least this long
		putSizeUpperBound int64         // Stops after written at least this much data
		minSize           int
		maxSize           int
		numWorkers        int
		cleanUp           bool
		statsShowInterval int
		readerType        string
		usingSG           bool
		usingFile         bool
		tmpDir            string // Only used when usingFile is true
	}
)

var (
	runParams            params
	nonDeterministicRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	workOrders           chan *workOrder
	workOrderResults     chan *workOrder
	intervalStats        stats.Stats
	accumulatedStats     stats.Stats
	allObjects           []string // All objects created under virtual directory myName
	statsPrintHeader     = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
)

func parseCmdLine() (params, error) {
	var p params

	// Command line options
	ip := flag.String("ip", "localhost", "IP address for proxy server")
	port := flag.Int("port", 8080, "Port number for proxy server")
	flag.IntVar(&p.statsShowInterval, "statsinterval", 10, "Interval to show stats in seconds; 0 = disabled")
	flag.StringVar(&p.bucket, "bucket", "nvdfc", "Bucket name")
	flag.BoolVar(&p.isLocal, "local", true, "True if using local bucket")
	flag.DurationVar(&p.duration, "duration", time.Minute, "How long to run the test; 0 = Unbounded."+
		"If duration is 0 and totalputsize is also 0, it is a no op.")
	flag.IntVar(&p.numWorkers, "numworkers", 10, "Number of go routines sending requests in parallel")
	flag.IntVar(&p.putPct, "pctput", 0, "Percentage of put requests")
	flag.StringVar(&p.tmpDir, "tmpdir", "/tmp/dfc", "Local temporary directory used to store temporary files")
	flag.Int64Var(&p.putSizeUpperBound, "totalputsize", 0, "Stops after total put size exceeds this (in KB); 0 = no limit")
	flag.BoolVar(&p.cleanUp, "cleanup", true, "True if clean up after run")
	flag.IntVar(&p.minSize, "minsize", 1024, "Minimal object size in KB")
	flag.IntVar(&p.maxSize, "maxsize", 1048576, "Maximal object size in KB")
	flag.StringVar(&p.readerType, "readertype", readers.ReaderTypeSG,
		fmt.Sprintf("Type of reader. {%s(default) | %s | %s | %s", readers.ReaderTypeSG,
			readers.ReaderTypeFile, readers.ReaderTypeInMem, readers.ReaderTypeRand))

	flag.Parse()
	p.usingSG = p.readerType == readers.ReaderTypeSG
	p.usingFile = p.readerType == readers.ReaderTypeFile

	// Sanity check
	if p.maxSize < p.minSize {
		return params{}, fmt.Errorf("Invalid option: min and max size (%d, %d)", p.minSize, p.maxSize)
	}

	if p.putPct < 0 || p.putPct > 100 {
		return params{}, fmt.Errorf("Invalid option: put percent %d", p.putPct)
	}

	if p.statsShowInterval < 0 {
		return params{}, fmt.Errorf("Invalid option: stats show interval %d", p.statsShowInterval)
	}

	p.proxyURL = "http://" + *ip + ":" + strconv.Itoa(*port)
	p.putSizeUpperBound *= 1024
	return p, nil
}

func main() {
	var (
		wg  sync.WaitGroup
		err error
	)

	runParams, err = parseCmdLine()
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	// If neither duration nor put upper bound is specified, it is a no op.
	// This can be used as a cleaup only run (no put no get).
	if runParams.duration == 0 {
		if runParams.putSizeUpperBound == 0 {
			if runParams.cleanUp {
				cleanUp()
			}

			return
		}

		runParams.duration = time.Duration(math.MaxInt64)
	}

	if runParams.usingFile {
		err = dfc.CreateDir(runParams.tmpDir + "/" + myName)
		if err != nil {
			fmt.Println("Failed to create local test directory", runParams.tmpDir, "err = ", err)
			return
		}
	}

	if runParams.isLocal {
		err := client.CreateLocalBucket(runParams.proxyURL, runParams.bucket)
		if err != nil {
			fmt.Println("Failed to create local bucket", runParams.bucket, "err = ", err)
			return
		}
	}

	err = bootStrap()
	if err != nil {
		fmt.Println("Failed to boot strap, err = ", err)
		return
	}

	if runParams.putPct == 0 && len(allObjects) == 0 {
		fmt.Println("Nothing to read, bucket is empty")
		return
	}

	fmt.Printf("Found %d existing objects\n", len(allObjects))
	logRunParams(runParams, os.Stdout)

	workOrders = make(chan *workOrder, runParams.numWorkers)
	workOrderResults = make(chan *workOrder, runParams.numWorkers)
	for i := 0; i < runParams.numWorkers; i++ {
		wg.Add(1)
		go worker(workOrders, workOrderResults, &wg)
	}

	var statsTicker *time.Ticker
	timer := time.NewTimer(runParams.duration)

	if runParams.statsShowInterval == 0 {
		statsTicker = time.NewTicker(time.Duration(math.MaxInt64))
	} else {
		statsTicker = time.NewTicker(time.Second * time.Duration(runParams.statsShowInterval))
	}

	tsStart := time.Now()
	intervalStats = stats.NewStats(tsStart)
	accumulatedStats = stats.NewStats(tsStart)

	statsWriter := os.Stdout
	writeStatsHeader(statsWriter)

	// Get the workers started
	for i := 0; i < runParams.numWorkers; i++ {
		if runParams.putPct == 0 {
			workOrders <- newGetWorkOrder()
		} else {
			workOrders <- newPutWorkOrder()
		}
	}

L:
	for {
		if runParams.putSizeUpperBound != 0 &&
			accumulatedStats.TotalPutBytes() >= runParams.putSizeUpperBound {
			break
		}

		select {
		case <-timer.C:
			break L
		case wo := <-workOrderResults:
			completeWorkOrder(wo)
			newWorkOrder()
		case <-statsTicker.C:
			accumulatedStats.Aggregate(intervalStats)
			writeStats(statsWriter, false /* final */, intervalStats, accumulatedStats)
			intervalStats = stats.NewStatsNow()
		default:
			// Do nothing
		}
	}

	timer.Stop()
	statsTicker.Stop()
	close(workOrders)
	wg.Wait() // wait until all workers are done (notified by closing work order channel)

	// Process left over work orders
	close(workOrderResults)
	for wo := range workOrderResults {
		completeWorkOrder(wo)
	}

	fmt.Printf("\nActual run duration: %v\n", time.Now().Sub(tsStart))
	accumulatedStats.Aggregate(intervalStats)
	writeStats(statsWriter, true /* final */, intervalStats, accumulatedStats)

	if runParams.cleanUp {
		cleanUp()
	}
}

// lonRunParams show run parameters in json format
func logRunParams(p params, to *os.File) {
	b, _ := json.MarshalIndent(struct {
		URL           string `json:"proxy"`
		IsLocal       bool   `json:"local"`
		Bucket        string `json:"bucket"`
		Duration      string `json:"duration"`
		MaxPutBytes   int64  `json:"put upper bound"`
		PutPct        int    `json:"put %"`
		MinSize       int    `json:"minimal object size in KB"`
		MaxSize       int    `json:"maximal object size in KB"`
		NumWorkers    int    `json:"# workers"`
		StatsInterval string `json:"stats interval"`
		Backing       string `json:"backed by"`
		Cleanup       bool   `json:"cleanup"`
	}{
		URL:           p.proxyURL,
		IsLocal:       p.isLocal,
		Bucket:        p.bucket,
		Duration:      p.duration.String(),
		MaxPutBytes:   p.putSizeUpperBound,
		PutPct:        p.putPct,
		MinSize:       p.minSize,
		MaxSize:       p.maxSize,
		NumWorkers:    p.numWorkers,
		StatsInterval: time.Duration(time.Second * time.Duration(runParams.statsShowInterval)).String(),
		Backing:       p.readerType,
		Cleanup:       p.cleanUp,
	}, "", "   ")

	fmt.Printf("Run configuration:\n%s\n\n", string(b))
}

// prettyNumber converts a number to format like 1,234,567
func prettyNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%s,%03d", prettyNumber(n/1000), n%1000)
}

// prettyNumBytes converts number of bytes to something like 4.7G, 2.8K, etc
func prettyNumBytes(n int64) string {
	const (
		_ = 1 << (10 * iota)
		KB
		MB
		GB
		TB
		PB
	)

	table := []struct {
		v int64
		n string
	}{
		{PB, "PB"},
		{TB, "TB"},
		{GB, "GB"},
		{MB, "MB"},
		{KB, "KB"},
	}

	for _, t := range table {
		if n > t.v {
			return fmt.Sprintf("%.2f%s", float64(n)/float64(t.v), t.n)
		}
	}

	// Less than 1KB
	return fmt.Sprintf("%dB", n)
}

// prettyDuration converts an interger representing a time in nano second to a string
func prettyDuration(t int64) string {
	if t > 1000000000 {
		return fmt.Sprintf("%.2fs", float64(t)/float64(1000000000))
	}
	return fmt.Sprintf("%.2fms", float64(t)/float64(1000000))
}

// prettyLatency combines three latency min, avg and max into a string
func prettyLatency(min, avg, max int64) string {
	return fmt.Sprintf("%-11s%-11s%-11s", prettyDuration(min), prettyDuration(avg), prettyDuration(max))
}

func prettyTimeStamp() string {
	return time.Now().String()[11:19]
}

// writeStatusHeader writes stats header to the writter.
func writeStatsHeader(to *os.File) {
	fmt.Fprintln(to)
	fmt.Fprintf(to, statsPrintHeader,
		"Time", "OP", "Count", "Total Bytes", "Latency(min, avg, max)", "Throughput", "Error")
}

// writeStatus writes stats to the writter.
// if final = true, writes the total; otherwise writes the interval stats
func writeStats(to *os.File, final bool, s, t stats.Stats) {
	p := fmt.Fprintf
	pn := prettyNumber
	pb := prettyNumBytes
	pl := prettyLatency
	pt := prettyTimeStamp
	if final {
		writeStatsHeader(to)
		p(to, statsPrintHeader, pt(), "Put",
			pn(t.TotalPuts()),
			pb(t.TotalPutBytes()),
			pl(t.MinPutLatency(), t.AvgPutLatency(), t.MaxPutLatency()),
			pb(t.PutThroughput(time.Now())),
			pn(t.TotalErrPuts()))
		p(to, statsPrintHeader, pt(), "Get",
			pn(t.TotalGets()),
			pb(t.TotalGetBytes()),
			pl(t.MinGetLatency(), t.AvgGetLatency(), t.MaxGetLatency()),
			pb(t.GetThroughput(time.Now())),
			pn(t.TotalErrGets()))
	} else {
		// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
		if s.TotalPuts() != 0 {
			p(to, statsPrintHeader, pt(), "Put",
				pn(s.TotalPuts())+"("+pn(t.TotalPuts())+")",
				pb(s.TotalPutBytes())+"("+pb(t.TotalPutBytes())+")",
				pl(s.MinPutLatency(), s.AvgPutLatency(), s.MaxPutLatency()),
				pb(s.PutThroughput(time.Now()))+"("+pb(t.PutThroughput(time.Now()))+")",
				pn(s.TotalErrPuts())+"("+pn(t.TotalErrPuts())+")")
		}
		if s.TotalGets() != 0 {
			p(to, statsPrintHeader, pt(), "Get",
				pn(s.TotalGets())+"("+pn(t.TotalGets())+")",
				pb(s.TotalGetBytes())+"("+pb(t.TotalGetBytes())+")",
				pl(s.MinGetLatency(), s.AvgGetLatency(), s.MaxGetLatency()),
				pb(s.GetThroughput(time.Now()))+"("+pb(t.GetThroughput(time.Now()))+")",
				pn(s.TotalErrGets())+"("+pn(t.TotalErrGets())+")")
		}
	}
}

func newPutWorkOrder() *workOrder {
	var size int

	if runParams.maxSize == runParams.minSize {
		size = runParams.minSize
	} else {
		size = nonDeterministicRand.Intn(runParams.maxSize-runParams.minSize) + runParams.minSize
	}

	return &workOrder{
		proxyURL: runParams.proxyURL,
		bucket:   runParams.bucket,
		isLocal:  runParams.isLocal,
		op:       opPut,
		objName:  myName + "/" + client.FastRandomFilename(nonDeterministicRand, 32),
		size:     int64(size * 1024),
	}
}

func newGetWorkOrder() *workOrder {
	n := len(allObjects)
	if n == 0 {
		return nil
	}

	return &workOrder{
		proxyURL: runParams.proxyURL,
		bucket:   runParams.bucket,
		isLocal:  runParams.isLocal,
		op:       opGet,
		objName:  allObjects[nonDeterministicRand.Intn(n)],
	}
}

func newWorkOrder() {
	var wo *workOrder

	if nonDeterministicRand.Intn(99) < runParams.putPct {
		wo = newPutWorkOrder()
	} else {
		wo = newGetWorkOrder()
	}

	if wo != nil {
		workOrders <- wo
	}
}

func completeWorkOrder(wo *workOrder) {
	delta := wo.end.Sub(wo.start)

	switch wo.op {
	case opGet:
		if wo.err == nil {
			intervalStats.AddGet(wo.size, delta)
		} else {
			fmt.Println("Get failed: ", wo.err)
			intervalStats.AddErrGet()
		}
	case opPut:
		if wo.err == nil {
			allObjects = append(allObjects, wo.objName)
			intervalStats.AddPut(wo.size, delta)
		} else {
			fmt.Println("Put failed: ", wo.err)
			intervalStats.AddErrPut()
		}
	default:
		// Should not be here
	}
}

func cleanUp() {
	fmt.Println(prettyTimeStamp() + " Clean up ...")

	var wg sync.WaitGroup
	f := func(objs []string, wg *sync.WaitGroup) {
		defer wg.Done()

		for _, obj := range objs {
			err := client.Del(runParams.proxyURL, runParams.bucket, obj, nil /* wg */, nil /* errch */, true /* silent */)
			if err != nil {
				fmt.Println("delete err ", err)
			}
			if runParams.usingFile {
				err := os.Remove(runParams.tmpDir + "/" + obj)
				if err != nil {
					fmt.Println("delete local file err ", err)
				}
			}
		}
	}

	w := runParams.numWorkers
	n := len(allObjects) / w
	for i := 0; i < w; i++ {
		wg.Add(1)
		go f(allObjects[i*n:(i+1)*n], &wg)
	}

	if len(allObjects)%w != 0 {
		wg.Add(1)
		go f(allObjects[n*w:], &wg)
	}

	wg.Wait()

	if runParams.isLocal {
		client.DestroyLocalBucket(runParams.proxyURL, runParams.bucket)
	}

	fmt.Println(prettyTimeStamp() + " Clean up done")
}

// bootStrap boot straps existing objects in the bucket
func bootStrap() error {
	var err error
	allObjects, err = client.ListObjects(runParams.proxyURL, runParams.bucket, myName)
	return err
}
