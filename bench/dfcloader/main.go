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

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/bench/dfcloader/stats"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/stats/statsd"
	"github.com/NVIDIA/dfcpub/tutils"
)

const (
	opPut = iota
	opGet
	opConfig

	myName = "loader"
)

type (
	workOrder struct {
		op        int
		proxyURL  string
		bucket    string
		isLocal   bool
		objName   string // In the format of 'virtual dir' + "/" + objname
		size      int64
		err       error
		start     time.Time
		end       time.Time
		latencies tutils.HTTPLatencies
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
		verifyHash        bool // verify xxHash during get
		cleanUp           bool
		statsShowInterval int
		readerType        string
		usingSG           bool
		usingFile         bool
		tmpDir            string // only used when usingFile is true
		loaderID          int    // when multiple of instances of loader running on the same host
		statsdPort        int
		batchSize         int  // batch is used for bootstraping(list) and delete
		getConfig         bool // true if only run get proxy config request
	}

	// sts records accumulated puts/gets information.
	sts struct {
		put       stats.HTTPReq
		get       stats.HTTPReq
		getConfig stats.HTTPReq
	}
)

var (
	runParams            params
	nonDeterministicRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	workOrders           chan *workOrder
	workOrderResults     chan *workOrder
	intervalStats        sts
	accumulatedStats     sts
	allObjects           []string // All objects created under virtual directory myName
	statsPrintHeader     = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
	statsdC              statsd.Client
	getPending           int64
	putPending           int64
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
	flag.BoolVar(&p.verifyHash, "verifyhash", false, "True if verify xxhash during get")
	flag.IntVar(&p.minSize, "minsize", 1024, "Minimal object size in KB")
	flag.IntVar(&p.maxSize, "maxsize", 1048576, "Maximal object size in KB")
	flag.StringVar(&p.readerType, "readertype", tutils.ReaderTypeSG,
		fmt.Sprintf("Type of reader. {%s(default) | %s | %s | %s", tutils.ReaderTypeSG,
			tutils.ReaderTypeFile, tutils.ReaderTypeInMem, tutils.ReaderTypeRand))
	flag.IntVar(&p.loaderID, "loaderid", 1, "ID to identify a loader when multiple instances of loader running on the same host")
	flag.IntVar(&p.statsdPort, "statsdport", 8125, "UDP port number for local statsd server")
	flag.IntVar(&p.batchSize, "batchsize", 100, "List and delete batch size")
	flag.BoolVar(&p.getConfig, "getconfig", false, "True if send get proxy config requests only")

	flag.Parse()
	p.usingSG = p.readerType == tutils.ReaderTypeSG
	p.usingFile = p.readerType == tutils.ReaderTypeFile

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

// newStats returns a new stats object with given time as the starting point
func newStats(t time.Time) sts {
	return sts{
		put:       stats.NewHTTPReq(t),
		get:       stats.NewHTTPReq(t),
		getConfig: stats.NewHTTPReq(t),
	}
}

// aggregate adds another sts to self
func (s *sts) aggregate(other sts) {
	s.get.Aggregate(other.get)
	s.put.Aggregate(other.put)
	s.getConfig.Aggregate(other.getConfig)
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
		err = cmn.CreateDir(runParams.tmpDir + "/" + myName)
		if err != nil {
			fmt.Println("Failed to create local test directory", runParams.tmpDir, "err = ", err)
			return
		}
	}

	if runParams.isLocal {
		exists, err := tutils.DoesLocalBucketExist(runParams.proxyURL, runParams.bucket)
		if err != nil {
			fmt.Println("Failed to get local bucket lists", runParams.bucket, "err = ", err)
			return
		}

		if !exists {
			baseParams := tutils.BaseAPIParams(runParams.proxyURL)
			err := api.CreateLocalBucket(baseParams, runParams.bucket)
			if err != nil {
				fmt.Println("Failed to create local bucket", runParams.bucket, "err = ", err)
				return
			}
		}
	}

	if !runParams.getConfig {
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
	}

	logRunParams(runParams, os.Stdout)

	host, err := os.Hostname()
	if err != nil {
		fmt.Println("Failed to get host name", err)
		return
	}

	statsdC, err = statsd.New("localhost", runParams.statsdPort,
		fmt.Sprintf("dfcloader.%s-%d", host, runParams.loaderID))
	if err != nil {
		fmt.Println("Failed to connect to statd, running without statsd")
	}
	defer statsdC.Close()

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
	intervalStats = newStats(tsStart)
	accumulatedStats = newStats(tsStart)

	statsWriter := os.Stdout
	writeStatsHeader(statsWriter)

	// Get the workers started
	for i := 0; i < runParams.numWorkers; i++ {
		if runParams.getConfig {
			workOrders <- newGetConfigWorkOrder()
		} else {
			if runParams.putPct == 0 {
				workOrders <- newGetWorkOrder()
			} else {
				workOrders <- newPutWorkOrder()
			}
		}
	}

L:
	for {
		if runParams.putSizeUpperBound != 0 &&
			accumulatedStats.put.TotalBytes() >= runParams.putSizeUpperBound {
			break
		}

		select {
		case <-timer.C:
			break L
		case wo := <-workOrderResults:
			completeWorkOrder(wo)
			newWorkOrder()
		case <-statsTicker.C:
			accumulatedStats.aggregate(intervalStats)
			writeStats(statsWriter, false /* final */, intervalStats, accumulatedStats)
			intervalStats = newStats(time.Now())
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

	fmt.Printf("\nActual run duration: %v\n", time.Since(tsStart))
	accumulatedStats.aggregate(intervalStats)
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
func writeStats(to *os.File, final bool, s, t sts) {
	p := fmt.Fprintf
	pn := prettyNumber
	pb := prettyNumBytes
	pl := prettyLatency
	pt := prettyTimeStamp
	if final {
		writeStatsHeader(to)
		p(to, statsPrintHeader, pt(), "Put",
			pn(t.put.Total()),
			pb(t.put.TotalBytes()),
			pl(t.put.MinLatency(), t.put.AvgLatency(), t.put.MaxLatency()),
			pb(t.put.Throughput(t.put.Start(), time.Now())),
			pn(t.put.TotalErrs()))
		p(to, statsPrintHeader, pt(), "Get",
			pn(t.get.Total()),
			pb(t.get.TotalBytes()),
			pl(t.get.MinLatency(), t.get.AvgLatency(), t.get.MaxLatency()),
			pb(t.get.Throughput(t.get.Start(), time.Now())),
			pn(t.get.TotalErrs()))
		p(to, statsPrintHeader, pt(), "CFG",
			pn(t.getConfig.Total()),
			pb(t.getConfig.TotalBytes()),
			pl(t.getConfig.MinLatency(), t.getConfig.AvgLatency(), t.getConfig.MaxLatency()),
			pb(t.getConfig.Throughput(t.getConfig.Start(), time.Now())),
			pn(t.getConfig.TotalErrs()))
	} else {
		// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
		if s.put.Total() != 0 {
			p(to, statsPrintHeader, pt(), "Put",
				pn(s.put.Total())+"("+pn(t.put.Total())+" "+pn(putPending)+" "+pn(int64(len(workOrderResults)))+")",
				pb(s.put.TotalBytes())+"("+pb(t.put.TotalBytes())+")",
				pl(s.put.MinLatency(), s.put.AvgLatency(), s.put.MaxLatency()),
				pb(s.put.Throughput(s.put.Start(), time.Now()))+"("+pb(t.put.Throughput(t.put.Start(), time.Now()))+")",
				pn(s.put.TotalErrs())+"("+pn(t.put.TotalErrs())+")")
		}
		if s.get.Total() != 0 {
			p(to, statsPrintHeader, pt(), "Get",
				pn(s.get.Total())+"("+pn(t.get.Total())+" "+pn(getPending)+" "+pn(int64(len(workOrderResults)))+")",
				pb(s.get.TotalBytes())+"("+pb(t.get.TotalBytes())+")",
				pl(s.get.MinLatency(), s.get.AvgLatency(), s.get.MaxLatency()),
				pb(s.get.Throughput(s.get.Start(), time.Now()))+"("+pb(t.get.Throughput(t.get.Start(), time.Now()))+")",
				pn(s.get.TotalErrs())+"("+pn(t.get.TotalErrs())+")")
		}
		if s.getConfig.Total() != 0 {
			p(to, statsPrintHeader, pt(), "CFG",
				pn(s.getConfig.Total())+"("+pn(t.getConfig.Total())+")",
				pb(s.getConfig.TotalBytes())+"("+pb(t.getConfig.TotalBytes())+")",
				pl(s.getConfig.MinLatency(), s.getConfig.AvgLatency(), s.getConfig.MaxLatency()),
				pb(s.getConfig.Throughput(s.getConfig.Start(), time.Now()))+"("+pb(t.getConfig.Throughput(t.getConfig.Start(), time.Now()))+")",
				pn(s.getConfig.TotalErrs())+"("+pn(t.getConfig.TotalErrs())+")")
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

	putPending++
	return &workOrder{
		proxyURL: runParams.proxyURL,
		bucket:   runParams.bucket,
		isLocal:  runParams.isLocal,
		op:       opPut,
		objName:  myName + "/" + tutils.FastRandomFilename(nonDeterministicRand, 32),
		size:     int64(size * 1024),
	}
}

func newGetWorkOrder() *workOrder {
	n := len(allObjects)
	if n == 0 {
		return nil
	}

	getPending++
	return &workOrder{
		proxyURL: runParams.proxyURL,
		bucket:   runParams.bucket,
		isLocal:  runParams.isLocal,
		op:       opGet,
		objName:  allObjects[nonDeterministicRand.Intn(n)],
	}
}

func newGetConfigWorkOrder() *workOrder {
	return &workOrder{
		proxyURL: runParams.proxyURL,
		op:       opConfig,
	}
}

func newWorkOrder() {
	var wo *workOrder

	if runParams.getConfig {
		wo = newGetConfigWorkOrder()
	} else {
		if nonDeterministicRand.Intn(99) < runParams.putPct {
			wo = newPutWorkOrder()
		} else {
			wo = newGetWorkOrder()
		}
	}

	if wo != nil {
		workOrders <- wo
	}
}

func completeWorkOrder(wo *workOrder) {
	delta := wo.end.Sub(wo.start)

	switch wo.op {
	case opGet:
		getPending--
		statsdC.Send("get",
			statsd.Metric{
				Type:  statsd.Gauge,
				Name:  "pending",
				Value: getPending,
			},
		)
		if wo.err == nil {
			intervalStats.get.Add(wo.size, delta)
			statsdC.Send("get",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency",
					Value: float64(delta / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "throughput",
					Value: wo.size,
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxyconn",
					Value: float64(wo.latencies.ProxyConn / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxy",
					Value: float64(wo.latencies.Proxy / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.targetconn",
					Value: float64(wo.latencies.TargetConn / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.target",
					Value: float64(wo.latencies.Target / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.posthttp",
					Value: float64(wo.latencies.PostHTTP / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxyheader",
					Value: float64(wo.latencies.ProxyWroteHeader / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxyrequest",
					Value: float64(wo.latencies.ProxyWroteRequest / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxyresponse",
					Value: float64(wo.latencies.ProxyFirstResponse / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.targetheader",
					Value: float64(wo.latencies.TargetWroteHeader / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.targetrequest",
					Value: float64(wo.latencies.TargetWroteRequest / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.targetresponse",
					Value: float64(wo.latencies.TargetFirstResponse / time.Millisecond),
				},
			)
		} else {
			fmt.Println("Get failed: ", wo.err)
			intervalStats.get.AddErr()
			statsdC.Send("get",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "error",
					Value: 1,
				},
			)
		}
	case opPut:
		putPending--
		statsdC.Send("put",
			statsd.Metric{
				Type:  statsd.Gauge,
				Name:  "pending",
				Value: putPending,
			},
		)
		if wo.err == nil {
			allObjects = append(allObjects, wo.objName)
			intervalStats.put.Add(wo.size, delta)
			statsdC.Send("put",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency",
					Value: float64(delta / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "throughput",
					Value: wo.size,
				},
			)
		} else {
			fmt.Println("Put failed: ", wo.err)
			intervalStats.put.AddErr()
			statsdC.Send("put",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "error",
					Value: 1,
				},
			)
		}
	case opConfig:
		if wo.err == nil {
			intervalStats.getConfig.Add(1, delta)
			statsdC.Send("getconfig",
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "count",
					Value: 1,
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency",
					Value: float64(delta / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxyconn",
					Value: float64(wo.latencies.ProxyConn / time.Millisecond),
				},
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "latency.proxy",
					Value: float64(wo.latencies.Proxy / time.Millisecond),
				},
			)
		} else {
			fmt.Println("Get config failed: ", wo.err)
			intervalStats.getConfig.AddErr()
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

		t := len(objs)
		b := cmn.Min(t, runParams.batchSize)
		n := t / b
		for i := 0; i < n; i++ {
			err := tutils.DeleteList(runParams.proxyURL, runParams.bucket, objs[i*b:(i+1)*b], true /* wait */, 0 /* wait forever */)
			if err != nil {
				fmt.Println("delete err ", err)
			}
		}

		if t%b != 0 {
			err := tutils.DeleteList(runParams.proxyURL, runParams.bucket, objs[n*b:], true /* wait */, 0 /* wait forever */)
			if err != nil {
				fmt.Println("delete err ", err)
			}
		}

		if runParams.usingFile {
			for _, obj := range objs {
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
		baseParams := tutils.BaseAPIParams(runParams.proxyURL)
		api.DestroyLocalBucket(baseParams, runParams.bucket)
	}

	fmt.Println(prettyTimeStamp() + " Clean up done")
}

// bootStrap boot straps existing objects in the bucket
func bootStrap() error {
	var err error
	allObjects, err = tutils.ListObjects(runParams.proxyURL, runParams.bucket, myName, 0)
	return err
}
