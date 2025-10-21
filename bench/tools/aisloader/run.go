// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

// `aisloader` is a benchmarking tool to measure storage performance. It's a load
// generator that can be used to benchmark and stress-test AIStore
// or any S3-compatible backend.
//
// In fact, aisloader can list, write, and read S3(*) buckets _directly_, which
// makes it quite useful, convenient, and easy to use benchmark tool to compare
// storage performance with AIStore in front of S3 vs _without_.
//
// (*) aisloader can be further easily extended to work directly with any
// Cloud storage including, but not limited to, AIS-supported GCP, OCI, and Azure.
//
// In addition, `aisloader` generates synthetic workloads that mimic training and
// inference workloads - the capability that allows to run benchmarks in isolation
// avoiding compute-side bottlenecks and the associated complexity (to analyze those).
//
// For usage, run: `aisloader`, or `aisloader usage`, or `aisloader --help`,
// or see examples.go in this package.

package aisloader

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats/statsd"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/xact"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	myName           = "loader"
	randomObjNameLen = 32

	wo2FreeSize  = 4096
	wo2FreeDelay = 3*time.Second + time.Millisecond

	ua = "aisloader"

	defaultClusterIP   = "localhost"
	defaultClusterIPv4 = "127.0.0.1"
)

type (
	// sts records accumulated puts/gets information.
	sts struct {
		statsd   stats.Metrics
		put      stats.HTTPReq
		get      stats.HTTPReq
		getBatch stats.HTTPReq
		putMPU   stats.HTTPReq // multipart upload operations
	}

	jsonStats struct {
		Start      time.Time     `json:"start_time"`   // time current stats started
		Cnt        int64         `json:"count,string"` // total # of requests
		Bytes      int64         `json:"bytes,string"` // total bytes by all requests
		Errs       int64         `json:"errors"`       // number of failed requests
		Latency    int64         `json:"latency"`      // Average request latency in nanoseconds
		Duration   time.Duration `json:"duration"`
		MinLatency int64         `json:"min_latency"`
		MaxLatency int64         `json:"max_latency"`
		Throughput int64         `json:"throughput,string"`
	}
)

var (
	runParams        *params
	rnd              *rand.Rand
	intervalStats    sts
	accumulatedStats sts
	objnameGetter    namegetter.Basic
	statsPrintHeader = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
	statsdC          *statsd.Client
	getPending       int64
	putPending       int64
	getBatchPending  int64
	traceHTTPSig     atomic.Bool

	flagUsage   bool
	flagVersion bool
	flagQuiet   bool

	etlInitSpec *etl.InitSpecMsg
	etlName     string

	useRandomObjName bool
	objNameCnt       atomic.Uint64

	suffixIDMaskLen uint
	suffixID        uint64

	numGets atomic.Int64

	gmm      *memsys.MMSA
	stopping atomic.Bool

	ip          string
	port        string
	envEndpoint string

	s3svc *s3.Client // s3 client - see s3ListObjects

	s3Endpoint     string
	s3Profile      string
	s3UsePathStyle bool

	loggedUserToken string
)

var (
	workCh  chan *workOrder
	resCh   chan *workOrder
	wo2Free []*workOrder
)

var _version, _buildtime string

// main function
func Start(version, buildtime string) (err error) {
	_version, _buildtime = version, buildtime

	// global and parsed/validated
	runParams = &params{}

	// discard flags of imported packages
	// define and add aisloader's own flags
	// parse flags
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	addCmdLine(f, runParams)

	// validate and finish initialization
	if err = _init(runParams); err != nil {
		return err
	}

	// print arguments unless quiet
	if !flagQuiet && !runParams.getLoaderID {
		printArguments(f)
	}

	if runParams.getLoaderID {
		fmt.Printf("0x%x\n", suffixID)
		if useRandomObjName {
			fmt.Printf("Warning: loaderID 0x%x used only for StatsD, not for object names!\n", suffixID)
		}
		return nil
	}

	// If none of duration, epochs, or put upper bound is specified, it is a no op.
	// Note that stoppable prevents being a no op
	// This can be used as a cleanup only run (no put no get).
	if runParams.duration.Val == 0 {
		if runParams.putSizeUpperBound == 0 && runParams.numEpochs == 0 && !runParams.stoppable {
			if runParams.cleanUp.Val {
				cleanup()
			}
			return nil
		}

		runParams.duration.Val = time.Duration(math.MaxInt64)
	}

	if runParams.readerType == readers.TypeFile {
		if err := cos.CreateDir(runParams.tmpDir + "/" + myName); err != nil {
			return fmt.Errorf("failed to create local test directory %q, err = %s", runParams.tmpDir, err.Error())
		}
	}

	// usage is currently limited to selecting a random proxy (gateway)
	// to access AIStore (done for every I/O request)
	if runParams.randomProxy {
		runParams.smap, err = api.GetClusterMap(runParams.bp)
		if err != nil {
			return fmt.Errorf("failed to get cluster map: %v", err)
		}
	}
	loggedUserToken, err = authn.LoadToken(runParams.tokenFile)
	if err != nil && runParams.tokenFile != "" {
		return err
	}
	runParams.bp.Token = loggedUserToken
	runParams.bp.UA = ua

	var created bool
	if err := setupBucket(runParams, &created); err != nil {
		return err
	}

	if isDirectS3() {
		if err := initS3Svc(); err != nil {
			return err
		}
	} else {
		if s3UsePathStyle {
			return errors.New("cannot use '-s3-use-path-style' without '-s3endpoint'")
		}
	}

	// list objects, or maybe not
	switch {
	case created:
		if runParams.putPct < 100 {
			return errors.New("new bucket, expecting 100% PUT")
		}
		objnameGetter = &namegetter.Random{}
		objnameGetter.Init([]string{}, rnd)
	case !runParams.skipList:
		names, err := listObjects()
		if err != nil {
			return err
		}
		var (
			l               = len(names)
			ng, isPermBased = newNameGetter(names)
		)
		if runParams.putPct < 100 && isPermBased && l < 2 {
			mixed := cos.Ternary(runParams.putPct > 0, " mixed", " read-only")
			return fmt.Errorf("need at least 2 existing objects for epochs-based%s workload (got %d)", mixed, l)
		}
		if runParams.putPct == 0 && l == 0 {
			if runParams.subDir == "" {
				return errors.New("the bucket is empty, cannot run 100% read benchmark")
			}
			return errors.New("no objects with prefix '" + runParams.subDir + "' in the bucket, cannot run 100% read benchmark")
		}

		// initialize global name-getter and announce
		objnameGetter = ng
		objnameGetter.Init(names, rnd)
		fmt.Printf("Found %s existing object%s\n\n", cos.FormatBigInt(l), cos.Plural(l))

	default:
		objnameGetter = &namegetter.Random{}
		objnameGetter.Init([]string{}, rnd)
	}

	printRunParams(runParams)
	if runParams.dryRun { // dry-run so just print the configurations and exit
		os.Exit(0)
	}

	if runParams.cleanUp.Val {
		v := "destroyed"
		if !runParams.bck.IsAIS() {
			v = "emptied"
		}
		fmt.Printf("BEWARE: cleanup is enabled, bucket %s will be %s upon termination!\n", runParams.bck.String(), v)
		time.Sleep(time.Second)
	}

	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get host name: %s", err.Error())
	}
	prefixC := fmt.Sprintf("aisloader.%s-%x", host, suffixID)
	statsdC, err = statsd.New(runParams.statsdIP, runParams.statsdPort, prefixC, runParams.statsdProbe)
	if err != nil {
		fmt.Printf("%s", "Failed to connect to StatsD server")
		time.Sleep(time.Second)
	}
	defer statsdC.Close()

	// init housekeeper and memsys;
	// empty config to use memsys constants;
	// alternatively: "memsys": { "min_free": "2gb", ... }
	hk.Init(true /*run*/)
	go hk.HK.Run()
	hk.WaitStarted()

	config := &cmn.Config{}
	config.Log.Level = "3"
	// memsys.Init(prefixC, prefixC, config) // TODO: revisit
	gmm = memsys.PageMM()
	gmm.RegWithHK()

	if etlInitSpec != nil {
		fmt.Println(now(), "Starting ETL...")
		etlName, err = api.ETLInit(runParams.bp, etlInitSpec)
		if err != nil {
			return fmt.Errorf("failed to initialize ETL: %v", err)
		}
		fmt.Println(now(), etlName, "started")

		defer func() {
			fmt.Println(now(), "Stopping ETL", etlName)
			if err := api.ETLStop(runParams.bp, etlName); err != nil {
				fmt.Printf("%s Failed to stop ETL %s: %v\n", now(), etlName, err)
				return
			}
			fmt.Println(now(), etlName, "stopped")
		}()
	}

	workCh = make(chan *workOrder, runParams.numWorkers)
	resCh = make(chan *workOrder, runParams.numWorkers)
	wg := &sync.WaitGroup{}
	for range runParams.numWorkers {
		wg.Add(1)
		go worker(workCh, resCh, wg, &numGets)
	}
	if runParams.putPct != 0 {
		wo2Free = make([]*workOrder, 0, wo2FreeSize)
	}

	timer := time.NewTimer(runParams.duration.Val)

	var statsTicker *time.Ticker
	if runParams.statsShowInterval == 0 {
		statsTicker = time.NewTicker(math.MaxInt64)
	} else {
		statsTicker = time.NewTicker(time.Second * time.Duration(runParams.statsShowInterval))
	}

	tsStart := time.Now()
	intervalStats = newStats(tsStart)
	accumulatedStats = newStats(tsStart)

	statsWriter := os.Stdout

	if runParams.statsOutput != "" {
		f, err := cos.CreateFile(runParams.statsOutput)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create stats-out %q: %v\n", runParams.statsOutput, err)
		}

		statsWriter = f
	}

	osSigChan := make(chan os.Signal, 2)
	if runParams.stoppable {
		signal.Notify(osSigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	} else {
		signal.Notify(osSigChan, syscall.SIGHUP)
	}

	preWriteStats(statsWriter, runParams.jsonFormat)

	// Get the workers started
	for range runParams.numWorkers {
		if err = postNewWorkOrder(); err != nil {
			break
		}
	}
	if err != nil {
		goto Done
	}

MainLoop:
	for runParams.putSizeUpperBound == 0 || accumulatedStats.put.TotalBytes() < runParams.putSizeUpperBound {
		if runParams.numEpochs > 0 { // if defined
			if numGets.Load() > int64(runParams.numEpochs)*int64(objnameGetter.Len()) {
				break
			}
		}

		// Prioritize showing stats otherwise we will dropping the stats intervals.
		select {
		case <-statsTicker.C:
			accumulatedStats.aggregate(&intervalStats)
			writeStats(statsWriter, runParams.jsonFormat, false /* final */, &intervalStats, &accumulatedStats)
			sendStatsdStats(&intervalStats)
			intervalStats = newStats(time.Now())
		default:
		}

		select {
		case <-timer.C:
			break MainLoop
		case wo := <-resCh:
			completeWorkOrder(wo, false)
			freeWO(wo)
			if runParams.statsShowInterval == 0 && runParams.putSizeUpperBound != 0 {
				accumulatedStats.aggregate(&intervalStats)
				intervalStats = newStats(time.Now())
			}
			if err := postNewWorkOrder(); err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				break MainLoop
			}
		case <-statsTicker.C:
			accumulatedStats.aggregate(&intervalStats)
			writeStats(statsWriter, runParams.jsonFormat, false /* final */, &intervalStats, &accumulatedStats)
			sendStatsdStats(&intervalStats)
			intervalStats = newStats(time.Now())
		case sig := <-osSigChan:
			switch sig {
			case syscall.SIGHUP:
				msg := "Detailed latency info is "
				if traceHTTPSig.Toggle() {
					msg += "disabled"
				} else {
					msg += "enabled"
				}
				fmt.Println(msg)
			default:
				if runParams.stoppable {
					break MainLoop
				}
			}
		}
	}

Done:
	timer.Stop()
	statsTicker.Stop()
	close(workCh)
	wg.Wait() // wait until all workers complete their work

	// Process left over work orders
	close(resCh)
	for wo := range resCh {
		completeWorkOrder(wo, true)
	}

	finalizeStats(statsWriter)
	fmt.Printf("Stats written to %s\n", statsWriter.Name())

	// Verify chunked objects if multipart upload was used
	if runParams.multipartChunks > 0 && runParams.putPct > 0 {
		chunkedLs, err := api.ListObjects(runParams.bp, runParams.bck, &apc.LsoMsg{Props: apc.GetPropsChunked}, api.ListArgs{})
		if err != nil {
			fmt.Println("failed to list chunked objects: ", err)
		} else {
			var actualChunked int64
			for _, entry := range chunkedLs.Entries {
				if entry.Flags&apc.EntryIsChunked != 0 {
					actualChunked++
				}
			}
			if actualChunked != accumulatedStats.putMPU.Total() {
				fmt.Printf("WARNING: Expected %d chunked objects but found %d\n", accumulatedStats.putMPU.Total(), actualChunked)
			}
		}
	}

	if runParams.cleanUp.Val {
		cleanup()
	}

	fmt.Printf("\nActual run duration: %v\n", time.Since(tsStart))

	return err
}

func isDirectS3() bool {
	debug.Assert(flag.Parsed())
	return s3Endpoint != ""
}

func loaderMaskFromTotalLoaders(totalLoaders uint64) uint {
	// take first bigger power of 2, then take first bigger or equal number
	// divisible by 4. This makes loaderID more visible in hex object name
	return ceilAlign(fastLog2Ceil(totalLoaders), 4)
}

func printArguments(set *flag.FlagSet) {
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)

	fmt.Fprint(w, "==== COMMAND LINE ARGUMENTS ====\n")
	fmt.Fprint(w, "=========== DEFAULTS ===========\n")
	set.VisitAll(func(f *flag.Flag) {
		if f.Value.String() == f.DefValue {
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", f.Name, f.Value.String())
		}
	})
	fmt.Fprint(w, "============ CUSTOM ============\n")
	set.VisitAll(func(f *flag.Flag) {
		if f.Value.String() != f.DefValue {
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", f.Name, f.Value.String())
		}
	})
	fmt.Fprintf(w, "HTTP trace:\t%v\n", runParams.traceHTTP)
	fmt.Fprint(w, "=================================\n\n")
	w.Flush()
}

// newStats returns a new stats object with given time as the starting point
func newStats(t time.Time) sts {
	return sts{
		put:      stats.NewHTTPReq(t),
		get:      stats.NewHTTPReq(t),
		putMPU:   stats.NewHTTPReq(t),
		getBatch: stats.NewHTTPReq(t),
		// StatsD is deprecated
		statsd: stats.NewStatsdMetrics(t),
	}
}

// aggregate adds another sts to self
func (s *sts) aggregate(other *sts) {
	s.get.Aggregate(other.get)
	s.put.Aggregate(other.put)
	s.putMPU.Aggregate(other.putMPU)
	s.getBatch.Aggregate(other.getBatch)
}

func setupBucket(runParams *params, created *bool) error {
	if strings.Contains(runParams.bck.Name, apc.BckProviderSeparator) {
		bck, objName, err := cmn.ParseBckObjectURI(runParams.bck.Name, cmn.ParseURIOpts{})
		if err != nil {
			return err
		}
		if objName != "" {
			return fmt.Errorf("expecting bucket name or a bucket URI with no object name in it: %s => [%v, %s]",
				runParams.bck.String(), bck.String(), objName)
		}
		if runParams.bck.Provider != apc.AIS /*cmdline default*/ && runParams.bck.Provider != bck.Provider {
			return fmt.Errorf("redundant and different bucket provider: %q vs %q in %s",
				runParams.bck.Provider, bck.Provider, bck.String())
		}
		runParams.bck = bck
	}

	const cachedText = "--cached option (to list \"cached\" objects only) "

	if isDirectS3() {
		if apc.ToScheme(runParams.bck.Provider) != apc.S3Scheme {
			return fmt.Errorf("option --s3endpoint requires s3 bucket (have %s)", runParams.bck.String())
		}
		if runParams.cached {
			return errors.New(cachedText + "cannot be used together with --s3endpoint (direct S3 access)")
		}
	}
	if runParams.putPct == 100 && runParams.cached {
		return errors.New(cachedText + "is incompatible with 100% PUT workload")
	}
	if runParams.bck.Provider != apc.AIS {
		return nil
	}
	if runParams.cached && !runParams.bck.IsRemote() {
		return fmt.Errorf(cachedText+"applies to remote buckets only (have %s)", runParams.bck.Cname(""))
	}
	if runParams.listDirs && !runParams.bck.IsRemote() {
		return fmt.Errorf("--list-dirs option applies to remote buckets only (have %s)", runParams.bck.Cname(""))
	}

	//
	// ais:// or ais://@remais
	//
	if runParams.bck.Name == "" {
		runParams.bck.Name = cos.CryptoRandS(8)
		fmt.Printf("New bucket name %q\n", runParams.bck.Name)
	}
	exists, err := api.QueryBuckets(runParams.bp, cmn.QueryBcks(runParams.bck), apc.FltPresent)
	if err != nil {
		return fmt.Errorf("%s not found: %v", runParams.bck.String(), err)
	}
	if !exists {
		if err := api.CreateBucket(runParams.bp, runParams.bck, nil); err != nil {
			return fmt.Errorf("failed to create %s: %v", runParams.bck.String(), err)
		}
		*created = true
	}
	if runParams.bPropsStr == "" {
		return nil
	}
	propsToUpdate := cmn.BpropsToSet{}
	// update bucket props if bPropsStr is set
	oldProps, err := api.HeadBucket(runParams.bp, runParams.bck, true /* don't add */)
	if err != nil {
		return fmt.Errorf("failed to read bucket %s properties: %v", runParams.bck.String(), err)
	}
	change := false
	if runParams.bProps.EC.Enabled != oldProps.EC.Enabled {
		propsToUpdate.EC = &cmn.ECConfToSet{
			Enabled:      apc.Ptr(runParams.bProps.EC.Enabled),
			ObjSizeLimit: apc.Ptr[int64](runParams.bProps.EC.ObjSizeLimit),
			DataSlices:   apc.Ptr(runParams.bProps.EC.DataSlices),
			ParitySlices: apc.Ptr(runParams.bProps.EC.ParitySlices),
		}
		change = true
	}
	if runParams.bProps.Mirror.Enabled != oldProps.Mirror.Enabled {
		propsToUpdate.Mirror = &cmn.MirrorConfToSet{
			Enabled: apc.Ptr(runParams.bProps.Mirror.Enabled),
			Copies:  apc.Ptr[int64](runParams.bProps.Mirror.Copies),
			Burst:   apc.Ptr(runParams.bProps.Mirror.Burst),
		}
		change = true
	}
	if change {
		if _, err = api.SetBucketProps(runParams.bp, runParams.bck, &propsToUpdate); err != nil {
			return fmt.Errorf("failed to enable EC for the bucket %s properties: %v", runParams.bck.String(), err)
		}
	}
	return nil
}

func getIDFromString(val string, hashLen uint) uint64 {
	hash := onexxh.Checksum64S(cos.UnsafeB(val), cos.MLCG32)
	// keep just the loaderIDHashLen bytes
	hash <<= 64 - hashLen
	hash >>= 64 - hashLen
	return hash
}

func sendStatsdStats(s *sts) {
	s.statsd.SendAll(statsdC)
}

func cleanup() {
	stopping.Store(true)
	time.Sleep(time.Second)
	fmt.Println(now() + " Cleaning up...")
	if objnameGetter != nil {
		// `objnameGetter` has been actually assigned to/initialized.
		var (
			w       = runParams.numWorkers
			objsLen = objnameGetter.Len()
			n       = objsLen / w
			wg      = &sync.WaitGroup{}
		)
		for i := range w {
			wg.Add(1)
			go cleanupObjs(objnameGetter.Names()[i*n:(i+1)*n], wg)
		}
		if objsLen%w != 0 {
			wg.Add(1)
			go cleanupObjs(objnameGetter.Names()[n*w:], wg)
		}
		wg.Wait()
	}

	if runParams.bck.IsAIS() {
		api.DestroyBucket(runParams.bp, runParams.bck)
	}
	fmt.Println(now() + " Done")
}

func cleanupObjs(objs []string, wg *sync.WaitGroup) {
	defer wg.Done()

	t := len(objs)
	if t == 0 {
		return
	}

	// Only delete objects if it's not an AIS bucket (because otherwise we just go ahead
	// and remove the bucket itself)
	if runParams.bck.IsRemote() {
		b := min(t, runParams.evictBatchSize)
		n := t / b
		for i := range n {
			evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: objs[i*b : (i+1)*b]}}
			xid, err := api.DeleteMultiObj(runParams.bp, runParams.bck, evdMsg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "delete multi-obj err:", err)
			}
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects}
			if _, err = api.WaitForXactionIC(runParams.bp, &args); err != nil {
				fmt.Fprintln(os.Stderr, "wait for xaction err:", err)
			}
		}

		if t%b != 0 {
			evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: objs[n*b:]}}
			xid, err := api.DeleteMultiObj(runParams.bp, runParams.bck, evdMsg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "delete err:", err)
			}
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects}
			if _, err = api.WaitForXactionIC(runParams.bp, &args); err != nil {
				fmt.Fprintln(os.Stderr, "wait for xaction err:", err)
			}
		}
	}

	if runParams.readerType == readers.TypeFile {
		for _, obj := range objs {
			if err := os.Remove(runParams.tmpDir + "/" + obj); err != nil {
				fmt.Println("delete local file err:", err)
			}
		}
	}
}

func objNamesFromFile() (names []string, err error) {
	var fh *os.File
	if fh, err = os.Open(runParams.fileList); err != nil {
		return
	}
	names = make([]string, 0, 1024)
	scanner := bufio.NewScanner(fh)
	regex := regexp.MustCompile(`\s`)
	for scanner.Scan() {
		n := strings.TrimSpace(scanner.Text())
		if strings.Contains(n, " ") || regex.MatchString(n) {
			continue
		}
		names = append(names, n)
	}
	fh.Close()
	return
}

func listObjects() (names []string, err error) {
	switch {
	case runParams.fileList != "":
		names, err = objNamesFromFile()
	case isDirectS3():
		names, err = s3ListObjects()
	default:
		names, err = listObjectNames(runParams)
	}
	return
}

func newNameGetter(names []string) (ng namegetter.Basic, isPermBased bool) {
	var (
		readOnly  = runParams.putPct == 0
		epoching  = runParams.numEpochs > 0
		threshold = runParams.permShuffleMax
		n         = len(names)
	)
	switch {
	case !readOnly && !epoching:
		ng = &namegetter.Random{}
	case !readOnly && epoching:
		ng = &namegetter.RandomUnique{}
	case n > int(threshold) && n > namegetter.AffineMinN:
		// O(1) memory, strict per-epoch
		ng = &namegetter.PermAffinePrime{}
		isPermBased = true
	default:
		// Fisher-Yates shuffle: https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
		ng = &namegetter.PermShuffle{}
		isPermBased = true
	}
	return
}

// returns smallest number divisible by `align` that is greater or equal `val`
func ceilAlign(val, align uint) uint {
	mod := val % align
	if mod != 0 {
		val += align - mod
	}
	return val
}
