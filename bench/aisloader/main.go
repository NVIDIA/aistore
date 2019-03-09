/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// 'aisloader' is a load generator for AIStore.
// It sends HTTP requests to a proxy server fronting targets.
// Run with -help for usage information.

// Examples:
// 1. No put or get, just clean up:
//    aisloader -bucket=nvais -duration 0s -totalputsize=0
// 2. Time based local bucket put only:
//    aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1024 -maxsize=1048 -pctput=100 -bckprovider=local
// 3. Put limit based cloud bucket mixed put(30%) and get(70%):
//    aisloader -bucket=nvais -duration 0s -numworkers=3 -minsize=1024 -maxsize=1048 -pctput=30 -bckprovider=cloud -totalputsize=10240

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	opPut = iota
	opGet
	opConfig

	myName        = "loader"
	dockerEnvFile = "/tmp/docker_ais/deploy.env" // filepath of Docker deployment config
)

type (
	workOrder struct {
		op          int
		proxyURL    string
		bucket      string
		bckProvider string
		objName     string // In the format of 'virtual dir' + "/" + objname
		size        int64
		err         error
		start       time.Time
		end         time.Time
		latencies   tutils.HTTPLatencies
	}

	params struct {
		seed int64 // random seed, 0=current time

		putSizeUpperBound int64
		minSize           int64
		maxSize           int64

		readOff int64 // read offset \
		readLen int64 // read length / to test range read

		putSizeUpperBoundStr string // Stops after written at least this much data
		minSizeStr           string
		maxSizeStr           string

		readOffStr string // read offset \
		readLenStr string // read length / to test range read

		duration time.Duration // Stops after run at least this long

		proxyURL    string
		bckProvider string //If bucket is "local" or "cloud" bucket
		bucket      string

		readerType  string
		tmpDir      string // only used when usingFile is true
		statsOutput string

		statsdIP          string
		statsdPort        int
		statsShowInterval int

		loaderID   int // when multiple of instances of loader running on the same host
		putPct     int // % of puts, rest are gets
		numWorkers int
		batchSize  int // batch is used for bootstraping(list) and delete

		verifyHash bool // verify xxHash during get
		cleanUp    bool
		usingSG    bool
		usingFile  bool
		getConfig  bool // true if only run get proxy config request
		jsonFormat bool

		stopable       bool // if true, will stop when `stop` is sent through stdin
		statsdRequired bool

		// bucket-related options
		bPropsStr string
		bProps    cmn.BucketProps
	}

	// sts records accumulated puts/gets information.
	sts struct {
		put       stats.HTTPReq
		get       stats.HTTPReq
		getConfig stats.HTTPReq
	}

	jsonStats struct {
		Start      time.Time     `json:"start_time"` // time current stats started
		Cnt        int64         `json:"count"`      // total # of requests
		Bytes      int64         `json:"bytes"`      // total bytes by all requests
		Errs       int64         `json:"errors"`     // number of failed requests
		Latency    int64         `json:"latency"`    // Average request latency in nanoseconds
		Duration   time.Duration `json:"duration"`
		MinLatency int64         `json:"min_latency"`
		MaxLatency int64         `json:"max_latency"`
		Throughput int64         `json:"throughput"`
	}
)

var (
	runParams        params
	rnd              *rand.Rand
	workOrders       chan *workOrder
	workOrderResults chan *workOrder
	intervalStats    sts
	accumulatedStats sts
	allObjects       []string // All objects created under virtual directory myName
	statsPrintHeader = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
	statsdC          statsd.Client
	getPending       int64
	putPending       int64

	flagUsage bool

	ip   string
	port string

	envVars      = tutils.ParseEnvVariables(dockerEnvFile) // Gets the fields from the .env file from which the docker was deployed
	dockerHostIP = envVars["PRIMARYHOSTIP"]                // Host IP of primary cluster
	dockerPort   = envVars["PORT"]
)

func printUsage(f *flag.FlagSet) {
	fmt.Println("Flags: ")
	f.PrintDefaults()
	fmt.Println()

	fmt.Println("Examples: ")
	examples := []string{
		"  aisloader -bucket=nvais -duration 0s -totalputsize=0",
		"\tNo PUT or GET, just clean up",

		"  aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1024 -maxsize=1048 -pctput=100 -bckprovider=local",
		"\tTime based local bucket PUT only. Bucket is cleaned up by default",

		"  aisloader -bucket=nvais -duration 0s -numworkers=3 -minsize=1024 -maxsize=1048 -pctput=30 -bckprovider=cloud -totalputsize=10240",
		"\tPut limit based cloud bucket mixed PUT(30%) and GET(70%). Bucket is cleaned up by default",

		"  aisloader -bucket=nvais -cleanup=false -duration 10s -numworkers=3 -pctput=100 -bckprovider=local",
		"  aisloader -bucket=nvais -duration 5s -numworkers=3 -pctput=0 -bckprovider=local",
		"\tFirst command PUTs into local bucket with clean up disabled, leaving the bucket for the second command to test only GET performance",
	}
	for i := 0; i < len(examples); i++ {
		fmt.Println(examples[i])
	}
	fmt.Println()

	fmt.Println("Additional Commands: ")
	fmt.Println("aisloader usage        Shows this menu")
	fmt.Println()
}

func parseCmdLine() (params, error) {
	var (
		p   params
		err error
	)

	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError) //Discard flags of imported packages

	// Command line options
	f.BoolVar(&flagUsage, "usage", false, "Show extensive help menu with examples and exit")
	f.StringVar(&ip, "ip", "localhost", "IP address for proxy server")
	f.StringVar(&port, "port", "8080", "Port number for proxy server")
	f.IntVar(&p.statsShowInterval, "statsinterval", 10, "Interval to show stats in seconds; 0 = disabled")
	f.StringVar(&p.bucket, "bucket", "nvais", "Bucket name")
	f.StringVar(&p.bckProvider, "bckprovider", cmn.LocalBs, "'local' if using local bucket, otherwise 'cloud'")
	f.DurationVar(&p.duration, "duration", time.Minute, "How long to run the test; 0 = Unbounded."+
		"If duration is 0 and totalputsize is also 0, it is a no op.")
	f.IntVar(&p.numWorkers, "numworkers", 10, "Number of go routines sending requests in parallel")
	f.IntVar(&p.putPct, "pctput", 0, "Percentage of put requests, bucket must not be empty if zero")
	f.StringVar(&p.tmpDir, "tmpdir", "/tmp/ais", "Local temporary directory used to store temporary files")
	f.StringVar(&p.putSizeUpperBoundStr, "totalputsize", "0", "Stops after total put size exceeds this, can specify multiplicative suffix; 0 = no limit")
	f.BoolVar(&p.cleanUp, "cleanup", true, "Determines if aisloader cleans up the files it creates when run is finished, true by default.")
	f.BoolVar(&p.verifyHash, "verifyhash", false, "If set, the contents of the downloaded files are verified using the xxhash in response headers during GET requests.")
	f.StringVar(&p.minSizeStr, "minsize", "", "Minimal object size, can specify multiplicative suffix")
	f.StringVar(&p.maxSizeStr, "maxsize", "", "Maximal object size, can specify multiplicative suffix")
	f.StringVar(&p.readerType, "readertype", tutils.ReaderTypeSG,
		fmt.Sprintf("Type of reader: %s(default) | %s | %s", tutils.ReaderTypeSG, tutils.ReaderTypeFile, tutils.ReaderTypeRand))
	f.IntVar(&p.loaderID, "loaderid", 1, "ID to identify a loader when multiple instances of loader running on the same host")
	f.StringVar(&p.statsdIP, "statsdip", "localhost", "IP for statsd server")
	f.IntVar(&p.statsdPort, "statsdport", 8125, "UDP port number for statsd server")
	f.BoolVar(&p.statsdRequired, "check-statsd", false, "If set, checks if statsd is running before run")
	f.IntVar(&p.batchSize, "batchsize", 100, "List and delete batch size")
	f.StringVar(&p.bPropsStr, "bprops", "", "Set local bucket properties(a JSON string in API SetBucketProps format)")
	f.Int64Var(&p.seed, "seed", 0, "Seed for random source, 0=use current time")
	f.BoolVar(&p.jsonFormat, "json", false, "Determines if the output should be printed in JSON format")
	f.StringVar(&p.readOffStr, "readoff", "", "Read range offset, can specify multiplicative suffix")
	f.StringVar(&p.readLenStr, "readlen", "", "Read range length, can specify multiplicative suffix; 0 = read the entire object")

	//Advanced Usage
	f.BoolVar(&p.getConfig, "getconfig", false, "If set, aisloader tests reading the configuration of the proxy instead of the usual GET/PUT requests.")
	f.StringVar(&p.statsOutput, "stats-output", "", "Determines where the stats should be printed to. Default stdout")
	f.BoolVar(&p.stopable, "stopable", false, "if true, will stop on receiving CTRL+C, prevents aisloader being a no op")

	f.Parse(os.Args[1:])

	if len(os.Args[1:]) == 0 {
		printUsage(f)
		os.Exit(0)
	}

	os.Args = []string{os.Args[0]}
	flag.Parse() //Called so that imported packages don't compain

	if flagUsage || f.NArg() != 0 && f.Arg(0) == "usage" {
		printUsage(f)
		os.Exit(0)
	}

	p.usingSG = p.readerType == tutils.ReaderTypeSG
	p.usingFile = p.readerType == tutils.ReaderTypeFile

	if p.seed == 0 {
		p.seed = time.Now().UnixNano()
	}
	rnd = rand.New(rand.NewSource(p.seed))

	if p.putSizeUpperBoundStr != "" {
		if p.putSizeUpperBound, err = cmn.S2B(p.putSizeUpperBoundStr); err != nil {
			return params{}, fmt.Errorf("failed to parse total put size %s: %v", p.putSizeUpperBoundStr, err)
		}
	}

	if p.minSizeStr != "" {
		if p.minSize, err = cmn.S2B(p.minSizeStr); err != nil {
			return params{}, fmt.Errorf("failed to parse min size %s: %v", p.minSizeStr, err)
		}
	} else {
		p.minSize = cmn.MiB
	}

	if p.maxSizeStr != "" {
		if p.maxSize, err = cmn.S2B(p.maxSizeStr); err != nil {
			return params{}, fmt.Errorf("failed to parse max size %s: %v", p.maxSizeStr, err)
		}
	} else {
		p.maxSize = cmn.GiB
	}

	// Sanity check
	if p.maxSize < p.minSize {
		return params{}, fmt.Errorf("invalid option: min and max size (%d, %d)", p.minSize, p.maxSize)
	}

	if p.putPct < 0 || p.putPct > 100 {
		return params{}, fmt.Errorf("invalid option: put percent %d", p.putPct)
	}

	if p.statsShowInterval < 0 {
		return params{}, fmt.Errorf("invalid option: stats show interval %d", p.statsShowInterval)
	}

	if p.readOffStr != "" {
		if p.readOff, err = cmn.S2B(p.readOffStr); err != nil {
			return params{}, fmt.Errorf("failed to parse read offset %s: %v", p.readOffStr, err)
		}
	}
	if p.readLenStr != "" {
		if p.readLen, err = cmn.S2B(p.readLenStr); err != nil {
			return params{}, fmt.Errorf("failed to parse read length %s: %v", p.readLenStr, err)
		}
	}

	if p.bPropsStr != "" {
		var bprops cmn.BucketProps
		jsonStr := strings.TrimRight(p.bPropsStr, ",")
		if !strings.HasPrefix(jsonStr, "{") {
			jsonStr = "{" + strings.TrimRight(jsonStr, ",") + "}"
		}

		if err := json.Unmarshal([]byte(jsonStr), &bprops); err != nil {
			return params{}, fmt.Errorf("failed to parse bucket properties: %v", err)
		}

		p.bProps = bprops
		if p.bProps.EC.Enabled {
			// fill EC defaults
			if p.bProps.EC.ParitySlices == 0 {
				p.bProps.EC.ParitySlices = 2
			}
			if p.bProps.EC.DataSlices == 0 {
				p.bProps.EC.DataSlices = 2
			}

			if p.bProps.EC.ParitySlices < 1 || p.bProps.EC.ParitySlices > 32 {
				return params{}, fmt.Errorf(
					"invalid number of parity slices: %d, it must be between 1 and 32",
					p.bProps.EC.ParitySlices)
			}
			if p.bProps.EC.DataSlices < 1 || p.bProps.EC.DataSlices > 32 {
				return params{}, fmt.Errorf(
					"invalid number of data slices: %d, it must be between 1 and 32",
					p.bProps.EC.DataSlices)
			}
		}

		if p.bProps.Mirror.Enabled {
			// fill mirror default properties
			if p.bProps.Mirror.UtilThresh == 0 {
				p.bProps.Mirror.UtilThresh = 5
			}
			if p.bProps.Mirror.Copies == 0 {
				p.bProps.Mirror.Copies = 2
			}
			if p.bProps.Mirror.Copies != 2 {
				return params{}, fmt.Errorf(
					"invalid number of mirror copies: %d, it must equal 2",
					p.bProps.Mirror.Copies)
			}
		}
	}
	// For Dry-Run on Docker
	if tutils.DockerRunning() && ip == "localhost" {
		ip = dockerHostIP
	}

	if tutils.DockerRunning() && port == "8080" {
		port = dockerPort
	}

	p.proxyURL = "http://" + ip + ":" + port
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
		fmt.Fprintln(os.Stdout, "err: "+err.Error())
		os.Exit(2)
	}

	// If neither duration nor put upper bound is specified, it is a no op.
	// Note that stopable prevents being a no op
	// This can be used as a cleaup only run (no put no get).
	if runParams.duration == 0 {
		if runParams.putSizeUpperBound == 0 && !runParams.stopable {
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

	if runParams.bckProvider == cmn.LocalBs {
		exists, err := tutils.DoesLocalBucketExist(runParams.proxyURL, runParams.bucket)
		if err != nil {
			fmt.Println("Failed to get local bucket lists", runParams.bucket, "err = ", err)
			return
		}

		baseParams := tutils.BaseAPIParams(runParams.proxyURL)
		if !exists {
			err := api.CreateLocalBucket(baseParams, runParams.bucket)
			if err != nil {
				fmt.Println("Failed to create local bucket", runParams.bucket, "err = ", err)
				return
			}
		}

		oldProps, err := api.HeadBucket(baseParams, runParams.bucket)
		if err != nil {
			fmt.Printf("Failed to read bucket %s properties: %v\n", runParams.bucket, err)
			return
		}
		change := false
		if runParams.bProps.EC.Enabled != oldProps.EC.Enabled {
			if !runParams.bProps.EC.Enabled {
				oldProps.EC.Enabled = false
			} else {
				oldProps.EC = cmn.ECConf{
					Enabled:      true,
					ObjSizeLimit: runParams.bProps.EC.ObjSizeLimit,
					DataSlices:   runParams.bProps.EC.DataSlices,
					ParitySlices: runParams.bProps.EC.ParitySlices,
				}
			}
			change = true
		}
		if runParams.bProps.Mirror.Enabled != oldProps.Mirror.Enabled {
			if runParams.bProps.Mirror.Enabled {
				oldProps.Mirror.Enabled = runParams.bProps.Mirror.Enabled
				oldProps.Mirror.Copies = runParams.bProps.Mirror.Copies
				oldProps.Mirror.UtilThresh = runParams.bProps.Mirror.UtilThresh
			} else {
				oldProps.Mirror.Enabled = false
			}
			change = true
		}
		if change {
			if err = api.SetBucketProps(baseParams, runParams.bucket, *oldProps); err != nil {
				fmt.Printf("Failed to enable EC for the bucket %s properties: %v\n", runParams.bucket, err)
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

	osSigChan := make(chan os.Signal, 2)
	if runParams.stopable {
		signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)
	}

	logRunParams(runParams, os.Stdout)

	host, err := os.Hostname()
	if err != nil {
		fmt.Println("Failed to get host name", err)
		return
	}

	statsdC, err = statsd.New(runParams.statsdIP, runParams.statsdPort, fmt.Sprintf("aisloader.%s-%d", host, runParams.loaderID))
	if err != nil {
		fmt.Printf("%s", "Failed to connect to statsd server")
		if runParams.statsdRequired {
			fmt.Println("... aborting")
			os.Exit(1)
		} else {
			fmt.Println("... proceeding anyway")
		}
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

	if runParams.statsOutput != "" {
		f, err := cmn.CreateFile(runParams.statsOutput)
		if err != nil {
			fmt.Println("Failed to create stats out file")
		}

		statsWriter = f
	}

	preWriteStats(statsWriter, runParams.jsonFormat)

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
			if runParams.statsShowInterval == 0 && runParams.putSizeUpperBound != 0 {
				accumulatedStats.aggregate(intervalStats)
				intervalStats = newStats(time.Now())
			}
			newWorkOrder()
		case <-statsTicker.C:
			if runParams.statsShowInterval > 0 {
				accumulatedStats.aggregate(intervalStats)
				writeStats(statsWriter, runParams.jsonFormat, false /* final */, intervalStats, accumulatedStats)
				intervalStats = newStats(time.Now())
			}
		case <-osSigChan:
			if runParams.stopable {
				break L
			}
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
	writeStats(statsWriter, runParams.jsonFormat, true /* final */, intervalStats, accumulatedStats)
	postWriteStats(statsWriter, runParams.jsonFormat)
	if runParams.cleanUp {
		cleanUp()
	}
}

// lonRunParams show run parameters in json format
func logRunParams(p params, to *os.File) {
	b, _ := json.MarshalIndent(struct {
		Seed          int64  `json:"seed"`
		URL           string `json:"proxy"`
		BckProvider   string `json:"bprovider"`
		Bucket        string `json:"bucket"`
		Duration      string `json:"duration"`
		MaxPutBytes   int64  `json:"put upper bound"`
		PutPct        int    `json:"put %"`
		MinSize       int64  `json:"minimal object size in Bytes"`
		MaxSize       int64  `json:"maximal object size in Bytes"`
		NumWorkers    int    `json:"# workers"`
		StatsInterval string `json:"stats interval"`
		Backing       string `json:"backed by"`
		Cleanup       bool   `json:"cleanup"`
	}{
		Seed:          p.seed,
		URL:           p.proxyURL,
		BckProvider:   p.bckProvider,
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

func preWriteStats(to *os.File, jsonFormat bool) {
	fmt.Fprintln(to)
	if !jsonFormat {
		fmt.Fprintf(to, statsPrintHeader,
			"Time", "OP", "Count", "Total Bytes", "Latency(min, avg, max)", "Throughput", "Error")
	} else {
		fmt.Fprintln(to, "[")
	}
}

func postWriteStats(to *os.File, jsonFormat bool) {
	if jsonFormat {
		fmt.Fprintln(to)
		fmt.Fprintln(to, "]")
	}
}

func writeFinalStats(to *os.File, jsonFormat bool, s sts) {
	if !jsonFormat {
		writeHumanReadibleFinalStats(to, s)
	} else {
		writeStatsJSON(to, s, false)
	}
}

func writeIntervalStats(to *os.File, jsonFormat bool, s, t sts) {
	if !jsonFormat {
		writeHumanReadibleIntervalStats(to, s, t)
	} else {
		writeStatsJSON(to, s)
	}
}

func jsonStatsFromReq(r stats.HTTPReq) *jsonStats {
	stats := &jsonStats{
		Cnt:        r.Total(),
		Bytes:      r.TotalBytes(),
		Start:      r.Start(),
		Duration:   time.Since(r.Start()),
		Errs:       r.TotalErrs(),
		Latency:    r.AvgLatency(),
		MinLatency: r.MinLatency(),
		MaxLatency: r.MaxLatency(),
		Throughput: r.Throughput(r.Start(), time.Now()),
	}

	return stats
}

func writeStatsJSON(to *os.File, s sts, withcomma ...bool) {
	stats := struct {
		Get *jsonStats `json:"get"`
		Put *jsonStats `json:"put"`
		Cfg *jsonStats `json:"cfg"`
	}{
		Get: jsonStatsFromReq(s.get),
		Put: jsonStatsFromReq(s.put),
		Cfg: jsonStatsFromReq(s.getConfig),
	}
	jsonOutput, err := json.Marshal(stats)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error during converting stats to json, skipping")
	}

	fmt.Fprintln(to)
	fmt.Fprint(to, string(jsonOutput))
	// print comma by default
	if len(withcomma) == 0 || withcomma[0] {
		fmt.Fprint(to, ",")
	}
}

func writeHumanReadibleIntervalStats(to *os.File, s, t sts) {
	p := fmt.Fprintf
	pn := prettyNumber
	pb := prettyNumBytes
	pl := prettyLatency
	pt := prettyTimeStamp

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

func writeHumanReadibleFinalStats(to *os.File, t sts) {
	p := fmt.Fprintf
	pn := prettyNumber
	pb := prettyNumBytes
	pl := prettyLatency
	pt := prettyTimeStamp
	preWriteStats(to, false)
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
}

// writeStatus writes stats to the writter.
// if final = true, writes the total; otherwise writes the interval stats
func writeStats(to *os.File, jsonFormat, final bool, s, t sts) {
	if final {
		writeFinalStats(to, jsonFormat, t)
	} else {
		// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
		writeIntervalStats(to, jsonFormat, s, t)
	}
}

func newPutWorkOrder() *workOrder {
	var size int64

	if runParams.maxSize == runParams.minSize {
		size = runParams.minSize
	} else {
		size = rnd.Int63n(runParams.maxSize-runParams.minSize) + runParams.minSize
	}

	putPending++
	return &workOrder{
		proxyURL:    runParams.proxyURL,
		bucket:      runParams.bucket,
		bckProvider: runParams.bckProvider,
		op:          opPut,
		objName:     myName + "/" + tutils.FastRandomFilename(rnd, 32),
		size:        size,
	}
}

func newGetWorkOrder() *workOrder {
	n := len(allObjects)
	if n == 0 {
		return nil
	}

	getPending++
	return &workOrder{
		proxyURL:    runParams.proxyURL,
		bucket:      runParams.bucket,
		bckProvider: runParams.bckProvider,
		op:          opGet,
		objName:     allObjects[rnd.Intn(n)],
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
		if rnd.Intn(99) < runParams.putPct {
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
	delta := cmn.TimeDelta(wo.end, wo.start)

	metrics := make([]statsd.Metric, 0)
	if wo.err == nil {
		metrics = []statsd.Metric{
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
				Value: cmn.MaxF64(float64(wo.latencies.TargetFirstResponse/time.Millisecond), 0),
			},
		}
	}

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
			)
			if len(metrics) != 0 {
				statsdC.Send("get", metrics...)
			}
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
			if len(metrics) != 0 {
				statsdC.Send("put", metrics...)
			}
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
			err := api.DeleteList(tutils.BaseAPIParams(runParams.proxyURL), runParams.bucket, runParams.bckProvider,
				objs[i*b:(i+1)*b], true /* wait */, 0 /* wait forever */)
			if err != nil {
				fmt.Println("delete err ", err)
			}
		}

		if t%b != 0 {
			err := api.DeleteList(tutils.BaseAPIParams(runParams.proxyURL), runParams.bucket, runParams.bckProvider,
				objs[n*b:], true /* wait */, 0 /* wait forever */)
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

	if runParams.bckProvider == cmn.LocalBs {
		baseParams := tutils.BaseAPIParams(runParams.proxyURL)
		api.DestroyLocalBucket(baseParams, runParams.bucket)
	}

	fmt.Println(prettyTimeStamp() + " Clean up done")
}

// bootStrap boot straps existing objects in the bucket
func bootStrap() error {
	var err error

	allObjects, err = tutils.ListObjectsFast(runParams.proxyURL, runParams.bucket, runParams.bckProvider, myName)
	return err
}
