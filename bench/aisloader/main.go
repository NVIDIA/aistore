/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// AIS loader (aisloader) is a tool to measure storage performance. It's a load
// generator that has been developed (and is currently used) to benchmark and
// stress-test AIStore(tm) but can be easily extended for any S3-compatible backend.
// For usage, run: `aisloader` or `aisloader usage` or `aisloader --help`.

// Examples:
// 1. Destroy existing ais bucket. If the bucket is Cloud-based, delete all objects:
//    aisloader -bucket=nvais -duration 0s -totalputsize=0
//    aisloader -bucket=nvais -provider=cloud -cleanup=true -duration 0s -totalputsize=0
// 2. Timed (for 1h) 100% GET from a Cloud bucket, no cleanup:
//    aisloader -bucket=nvaws -duration 1h -numworkers=30 -pctput=0 -provider=cloud -cleanup=false
// 3. Time-based PUT into ais bucket, random objects names:
//    aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais
// 4. Mixed 30% PUT and 70% GET to/from Cloud bucket. PUT will generate random object names and is limited by 10GB total size:
//    aisloader -bucket=nvaws -duration 0s -numworkers=3 -minsize=1MB -maxsize=1MB -pctput=30 -provider=cloud -totalputsize=10G
// 5. PUT 2000 objects with names that look like hex({0..2000}{loaderid})
//    aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000
// 6. Use random object names and loaderID for reporting stats
//    aisloader -loaderid=10
// 7. PUT objects with names based on loaderID and total number of loaders; names: hex({0..}{loaderid})
//    aisloader -loaderid=10 -loadernum=20
// 8. PUT objects with names passed on hash of loaderID of length -loaderidhashlen; names: hex({0..}{hash(loaderid)})
//    aisloader -loaderid=loaderstring -loaderidhashlen=8
// 9. Does nothing but prints loaderID:
//    aisloader -getloaderid (0x0)
//    aisloader -loaderid=10 -getloaderid (0xa)
//    aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid (0xdb)

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/containers"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/aisloader/namegetter"
	"github.com/NVIDIA/aistore/bench/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/OneOfOne/xxhash"
)

const (
	opPut = iota
	opGet
	opConfig

	myName           = "loader"
	dockerEnvFile    = "/tmp/docker_ais/deploy.env" // filepath of Docker deployment config
	randomObjNameLen = 32
)

var (
	version = "1.0"
	build   string
)

type (
	workOrder struct {
		op        int
		proxyURL  string
		bucket    string
		provider  string
		objName   string // In the format of 'virtual dir' + "/" + objName
		size      int64
		err       error
		start     time.Time
		end       time.Time
		latencies tutils.HTTPLatencies
	}

	//nolint:maligned
	params struct {
		seed              int64 // random seed; UnixNano() if omitted
		putSizeUpperBound int64
		minSize           int64
		maxSize           int64
		readOff           int64 // read offset
		readLen           int64 // read length
		loaderCnt         uint64
		maxputs           uint64
		putShards         uint64

		loaderID             string // used with multiple loader instances generating objects in parallel
		proxyURL             string
		provider             string // "ais" or "cloud"
		bucket               string
		readerType           string
		tmpDir               string // used only when usingFile
		statsOutput          string
		statsdIP             string
		bPropsStr            string
		putSizeUpperBoundStr string // stop after writing that amount of data
		minSizeStr           string
		maxSizeStr           string
		readOffStr           string // read offset
		readLenStr           string // read length
		subDir               string
		duration             cmn.DurationExt // stop after the run for at least that much
		cleanUp              cmn.BoolExt

		bProps cmn.BucketProps

		statsdPort        int
		statsShowInterval int
		putPct            int // % of puts, rest are gets
		numWorkers        int
		batchSize         int // batch is used for bootstraping(list) and delete
		loaderIDHashLen   uint
		numEpochs         uint

		getLoaderID    bool
		randomObjName  bool
		uniqueGETs     bool
		verifyHash     bool // verify xxhash during get
		usingSG        bool
		usingFile      bool
		getConfig      bool // true: load control plane (read proxy config)
		jsonFormat     bool
		stoppable      bool // true: terminate by Ctrl-C
		statsdRequired bool
		dryRun         bool // true: print configuration and parameters that aisloader will use at runtime
		traceHTTP      bool // true: trace http latencies as per tutils.HTTPLatencies & https://golang.org/pkg/net/http/httptrace
	}

	// sts records accumulated puts/gets information.
	sts struct {
		put       stats.HTTPReq
		get       stats.HTTPReq
		getConfig stats.HTTPReq
		statsd    statsd.Metrics
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
	runParams        params
	rnd              *rand.Rand
	workOrders       chan *workOrder
	workOrderResults chan *workOrder
	intervalStats    sts
	accumulatedStats sts
	bucketObjsNames  namegetter.ObjectNameGetter
	statsPrintHeader = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
	statsdC          statsd.Client
	getPending       int64
	putPending       int64
	traceHTTPSig     atomic.Bool

	flagUsage   bool
	flagVersion bool

	ip   string
	port string

	useRandomObjName bool
	objNameCnt       atomic.Uint64

	suffixIDMaskLen uint
	suffixID        uint64

	numGets atomic.Int64

	envVars      = tutils.ParseEnvVariables(dockerEnvFile) // Gets the fields from the .env file from which the docker was deployed
	dockerHostIP = envVars["PRIMARY_HOST_IP"]              // Host IP of primary cluster
	dockerPort   = envVars["PORT"]
)

func (wo *workOrder) String() string {
	var errstr, opName string
	switch wo.op {
	case opGet:
		opName = http.MethodGet
	case opPut:
		opName = http.MethodPut
	case opConfig:
		opName = "CONFIG"
	}

	if wo.err != nil {
		errstr = ", error: " + wo.err.Error()
	}

	return fmt.Sprintf("WO: %s/%s, start:%s end:%s, size: %d, type: %s%s",
		wo.bucket, wo.objName, wo.start.Format(time.StampMilli), wo.end.Format(time.StampMilli), wo.size, opName, errstr)
}

func loaderMaskFromTotalLoaders(totalLoaders uint64) uint {
	// take first bigger power of 2, then take first bigger or equal number
	// divisible by 4. This makes loaderID more visible in hex object name
	return cmn.CeilAlign(cmn.FastLog2Ceil(totalLoaders), 4)
}

func parseCmdLine() (params, error) {
	var (
		p   params
		err error
	)

	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // discard flags of imported packages

	f.BoolVar(&flagUsage, "usage", false, "Show command-line options, usage, and examples")
	f.BoolVar(&flagVersion, "version", false, "Show aisloader version")
	f.StringVar(&ip, "ip", "localhost", "AIS proxy/gateway IP address or hostname")
	f.StringVar(&port, "port", "8080", "AIS proxy/gateway port")
	f.IntVar(&p.statsShowInterval, "statsinterval", 10, "Interval in seconds to print performance counters; 0 - disabled")
	f.StringVar(&p.bucket, "bucket", "nvais", "Bucket name")
	f.StringVar(&p.provider, "provider", cmn.AIS, "ais - for AIS bucket, cloud - for Cloud bucket; other supported values include \"gcp\" and \"aws\", for Amazon and Google clouds, respectively")

	cmn.DurationExtVar(f, &p.duration, "duration", time.Minute,
		"Benchmark duration (0 - run forever or until Ctrl-C). Note that if both duration and totalputsize are 0 (zeros), aisloader will have nothing to do.\n"+
			"If not specified and totalputsize > 0, aisloader runs until totalputsize reached. Otherwise aisloader runs until first of duration and "+
			"totalputsize reached")

	f.IntVar(&p.numWorkers, "numworkers", 10, "Number of goroutine workers operating on AIS in parallel")
	f.IntVar(&p.putPct, "pctput", 0, "Percentage of PUTs in the aisloader-generated workload")
	f.StringVar(&p.tmpDir, "tmpdir", "/tmp/ais", "Local directory to store temporary files")
	f.StringVar(&p.putSizeUpperBoundStr, "totalputsize", "0", "Stop PUT workload once cumulative PUT size reaches or exceeds this value (can contain standard multiplicative suffix K, MB, GiB, etc.; 0 - unlimited")
	cmn.BoolExtVar(f, &p.cleanUp, "cleanup", "true: remove bucket upon benchmark termination; default false for cloud buckets")
	f.BoolVar(&p.verifyHash, "verifyhash", false, "true: checksum-validate GET: recompute object checksums and validate it against the one received with the GET metadata")
	f.StringVar(&p.minSizeStr, "minsize", "", "Minimum object size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.maxSizeStr, "maxsize", "", "Maximum object size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.readerType, "readertype", tutils.ReaderTypeSG,
		fmt.Sprintf("Type of reader: %s(default) | %s | %s", tutils.ReaderTypeSG, tutils.ReaderTypeFile, tutils.ReaderTypeRand))
	f.StringVar(&p.loaderID, "loaderid", "0", "ID to identify a loader among multiple concurrent instances")
	f.StringVar(&p.statsdIP, "statsdip", "localhost", "StatsD IP address or hostname")
	f.IntVar(&p.statsdPort, "statsdport", 8125, "StatsD UDP port")
	f.BoolVar(&p.statsdRequired, "check-statsd", false, "true: prior to benchmark make sure that StatsD is reachable")
	f.IntVar(&p.batchSize, "batchsize", 100, "Batch size to list and delete")
	f.StringVar(&p.bPropsStr, "bprops", "", "JSON string formatted as per the SetBucketPropsMsg API and containing bucket properties to apply")
	f.Int64Var(&p.seed, "seed", 0, "Random seed to achieve deterministic reproducible results (0 - use current time in nanoseconds)")
	f.BoolVar(&p.jsonFormat, "json", false, "true: print the output in JSON")
	f.StringVar(&p.readOffStr, "readoff", "", "Read range offset (can contain multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.readLenStr, "readlen", "", "Read range length (can contain multiplicative suffix; 0 - GET full object)")
	f.Uint64Var(&p.maxputs, "maxputs", 0, "Maximum number of objects to PUT")
	f.UintVar(&p.numEpochs, "epochs", 0, "Number of \"epochs\" to run whereby each epoch entails full pass through the entire listed bucket")

	//
	// object naming
	//
	f.Uint64Var(&p.loaderCnt, "loadernum", 0,
		"total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen")
	f.BoolVar(&p.getLoaderID, "getloaderid", false,
		"true: print stored/computed unique loaderID aka aisloader identifier and exit")
	f.UintVar(&p.loaderIDHashLen, "loaderidhashlen", 0,
		"Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum")
	f.BoolVar(&p.randomObjName, "randomname", true,
		"true: generate object names of 32 random characters. This option is ignored when loadernum is defined")
	f.StringVar(&p.subDir, "subdir", "", "Virtual destination directory for all aisloader-generated objects")
	f.Uint64Var(&p.putShards, "putshards", 0, "Spread generated objects over this many subdirectories (max 100k)")
	f.BoolVar(&p.uniqueGETs, "uniquegets", true, "true: GET objects randomly and equally. Meaning, make sure *not* to GET some objects more frequently than the others")

	//
	// advanced usage
	//
	f.BoolVar(&p.getConfig, "getconfig", false, "true: generate control plane load by reading AIS proxy configuration (that is, instead of reading/writing data exercise control path)")
	f.StringVar(&p.statsOutput, "stats-output", "", "filename to log statistics (empty string translates as standard output (default))")
	f.BoolVar(&p.stoppable, "stoppable", false, "true: stop upon CTRL-C")
	f.BoolVar(&p.dryRun, "dry-run", false, "true: show the configuration and parameters that aisloader will use for benchmark")
	f.BoolVar(&p.traceHTTP, "trace-http", false, "true: trace HTTP latencies") // see tutils.HTTPLatencies

	f.Parse(os.Args[1:])

	if len(os.Args[1:]) == 0 {
		printUsage(f)
		os.Exit(0)
	}

	os.Args = []string{os.Args[0]}
	flag.Parse() // Called so that imported packages don't complain

	if flagUsage || f.NArg() != 0 && f.Arg(0) == "usage" {
		printUsage(f)
		os.Exit(0)
	}
	if flagVersion || f.NArg() != 0 && f.Arg(0) == "version" {
		fmt.Printf("version %s, build %s\n", version, build)
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
			return params{}, fmt.Errorf("failed to parse total PUT size %s: %v", p.putSizeUpperBoundStr, err)
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

	if !p.duration.IsSet && p.putSizeUpperBound != 0 {
		// user specified putSizeUpperBound, but not duration, override default 1 minute
		// and run aisloader until putSizeUpperBound is reached
		p.duration.Val = time.Duration(cmn.MaxInt64)
	}

	// Sanity check
	if p.maxSize < p.minSize {
		return params{}, fmt.Errorf("invalid option: min and max size (%d, %d)", p.minSize, p.maxSize)
	}

	if p.putPct < 0 || p.putPct > 100 {
		return params{}, fmt.Errorf("invalid option: PUT percent %d", p.putPct)
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

	if p.loaderID == "" {
		return params{}, fmt.Errorf("loaderID can't be empty")
	}

	loaderID, parseErr := strconv.ParseUint(p.loaderID, 10, 64)
	if p.loaderCnt == 0 && p.loaderIDHashLen == 0 {
		if p.randomObjName {
			useRandomObjName = true
			if parseErr != nil {
				return params{}, errors.New("loaderID as string only allowed when using loaderIDHashLen")
			}
			// don't have to set suffixIDLen as userRandomObjName = true
			suffixID = loaderID
		} else {
			// stats will be using loaderID
			// but as suffixIDMaskLen = 0, object names will be just consecutive numbers
			suffixID = loaderID
			suffixIDMaskLen = 0
		}
	} else {
		if p.loaderCnt > 0 && p.loaderIDHashLen > 0 {
			return params{}, fmt.Errorf("loadernum and loaderIDHashLen can't be > 0 at the same time")
		}

		if p.loaderIDHashLen > 0 {
			if p.loaderIDHashLen == 0 || p.loaderIDHashLen > 63 {
				return params{}, fmt.Errorf("loaderIDHashLen has to be larger than 0 and smaller than 64")
			}

			suffixIDMaskLen = cmn.CeilAlign(p.loaderIDHashLen, 4)
			suffixID = getIDFromString(p.loaderID, suffixIDMaskLen)
		} else {
			// p.loaderCnt > 0
			if parseErr != nil {
				return params{}, fmt.Errorf("loadername has to be a number when using loadernum")
			}
			if loaderID > p.loaderCnt {
				return params{}, fmt.Errorf("loaderid has to be smaller than loadernum")
			}

			suffixIDMaskLen = loaderMaskFromTotalLoaders(p.loaderCnt)
			suffixID = loaderID
		}
	}

	if p.subDir != "" {
		p.subDir = filepath.Clean(p.subDir)
		if p.subDir[0] == '/' {
			return params{}, fmt.Errorf("object name prefix can't start with /")
		}
	}

	if p.putShards > 100000 {
		return params{}, fmt.Errorf("putshards should not exceed 100000")
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
				p.bProps.EC.ParitySlices = 1
			}
			if p.bProps.EC.DataSlices == 0 {
				p.bProps.EC.DataSlices = 1
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

	if !p.cleanUp.IsSet {
		p.cleanUp.Val = cmn.IsProviderAIS(p.provider)
	}

	// For Dry-Run on Docker
	if containers.DockerRunning() && ip == "localhost" {
		ip = dockerHostIP
	}

	if containers.DockerRunning() && port == "8080" {
		port = dockerPort
	}

	traceHTTPSig.Store(p.traceHTTP)

	p.proxyURL = "http://" + ip + ":" + port

	// Don't print arguments when just getting loaderID
	if !p.getLoaderID {
		printArguments(f)
	}
	return p, nil
}

func printArguments(set *flag.FlagSet) {
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)

	_, _ = fmt.Fprintf(w, "==== COMMAND LINE ARGUMENTS ====\n")
	_, _ = fmt.Fprintf(w, "=========== DEFAULTS ===========\n")
	set.VisitAll(func(f *flag.Flag) {
		if f.Value.String() == f.DefValue {
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", f.Name, f.Value.String())
		}
	})
	_, _ = fmt.Fprintf(w, "============ CUSTOM ============\n")
	set.VisitAll(func(f *flag.Flag) {
		if f.Value.String() != f.DefValue {
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", f.Name, f.Value.String())
		}
	})
	fmt.Fprintf(w, "HTTP trace:\t%v\n", runParams.traceHTTP)
	_, _ = fmt.Fprintf(w, "=================================\n\n")
	_ = w.Flush()
}

// newStats returns a new stats object with given time as the starting point
func newStats(t time.Time) sts {
	return sts{
		put:       stats.NewHTTPReq(t),
		get:       stats.NewHTTPReq(t),
		getConfig: stats.NewHTTPReq(t),
		statsd:    statsd.NewStatsdMetrics(t),
	}
}

// aggregate adds another sts to self
func (s *sts) aggregate(other sts) {
	s.get.Aggregate(other.get)
	s.put.Aggregate(other.put)
	s.getConfig.Aggregate(other.getConfig)
}

func setupBucket(runParams *params) error {
	if runParams.provider != cmn.AIS || runParams.getConfig {
		return nil
	}
	baseParams := tutils.BaseAPIParams(runParams.proxyURL)
	exists, err := api.DoesBucketExist(baseParams, runParams.bucket)
	if err != nil {
		return fmt.Errorf("failed to get ais bucket lists to check for %s, err = %v", runParams.bucket, err)
	}

	if !exists {
		err := api.CreateBucket(baseParams, runParams.bucket)
		if err != nil {
			return fmt.Errorf("failed to create ais bucket %s, err = %s", runParams.bucket, err)
		}
	}

	if runParams.bPropsStr == "" {
		return nil
	}
	// update bucket props if bPropsStr is set
	oldProps, err := api.HeadBucket(baseParams, runParams.bucket)
	if err != nil {
		return fmt.Errorf("failed to read bucket %s properties: %v", runParams.bucket, err)
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
		if err = api.SetBucketPropsMsg(baseParams, runParams.bucket, *oldProps); err != nil {
			return fmt.Errorf("failed to enable EC for the bucket %s properties: %v", runParams.bucket, err)
		}
	}
	return nil
}

func getIDFromString(val string, hashLen uint) uint64 {
	hash := xxhash.ChecksumString64S(val, cmn.MLCG32)
	// leave just right loaderIDHashLen bytes
	hash <<= (64 - hashLen)
	hash >>= (64 - hashLen)
	return hash
}

func main() {
	var (
		wg  = &sync.WaitGroup{}
		err error
	)

	runParams, err = parseCmdLine()
	if err != nil {
		cmn.ExitInfof("%s", err)
	}

	if runParams.getLoaderID {
		fmt.Printf("0x%x\n", suffixID)
		if useRandomObjName {
			fmt.Printf("Warning: loaderID 0x%x used only for StatsD, not for object names!\n", suffixID)
		}
		return
	}

	// If neither duration nor put upper bound is specified, it is a no op.
	// Note that stoppable prevents being a no op
	// This can be used as a cleaup only run (no put no get).
	if runParams.duration.Val == 0 {
		if runParams.putSizeUpperBound == 0 && !runParams.stoppable {
			if runParams.cleanUp.Val {
				cleanUp()
			}

			return
		}

		runParams.duration.Val = time.Duration(math.MaxInt64)
	}

	if runParams.usingFile {
		err = cmn.CreateDir(runParams.tmpDir + "/" + myName)
		if err != nil {
			fmt.Println("Failed to create local test directory", runParams.tmpDir, "err = ", err)
			return
		}
	}

	if err := setupBucket(&runParams); err != nil {
		cmn.ExitInfof("%s", err)
	}

	if !runParams.getConfig {
		err = bootStrap()
		if err != nil {
			return
		}

		objsLen := bucketObjsNames.Len()
		if runParams.putPct == 0 && objsLen == 0 {
			fmt.Println("Nothing to read, bucket is empty")
			return
		}

		fmt.Printf("Found %d existing objects\n", objsLen)
	}

	printRunParams(runParams)
	if runParams.dryRun { // dry-run so just print the configurations and exit
		os.Exit(0)
	}

	if runParams.cleanUp.Val {
		fmt.Printf("BEWARE: cleanup is enabled, bucket %s will be destroyed after the run!\n", runParams.bucket)
	}

	host, err := os.Hostname()
	if err != nil {
		fmt.Println("Failed to get host name", err)
		return
	}

	statsdC, err = statsd.New(runParams.statsdIP, runParams.statsdPort, fmt.Sprintf("aisloader.%s-%x", host, suffixID))
	if err != nil {
		fmt.Printf("%s", "Failed to connect to StatsD server")
		if runParams.statsdRequired {
			cmn.ExitInfof("... aborting")
		} else {
			fmt.Println("... proceeding anyway")
		}
	}
	defer statsdC.Close()

	workOrders = make(chan *workOrder, runParams.numWorkers)
	workOrderResults = make(chan *workOrder, runParams.numWorkers)
	for i := 0; i < runParams.numWorkers; i++ {
		wg.Add(1)
		go worker(workOrders, workOrderResults, wg, &numGets)
	}

	timer := time.NewTimer(runParams.duration.Val)

	var statsTicker *time.Ticker
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

	osSigChan := make(chan os.Signal, 2)
	if runParams.stoppable {
		signal.Notify(osSigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	} else {
		signal.Notify(osSigChan, syscall.SIGHUP)
	}

	preWriteStats(statsWriter, runParams.jsonFormat)

	// Get the workers started
	for i := 0; i < runParams.numWorkers; i++ {
		if err = postNewWorkOrder(); err != nil {
			break
		}
	}

	if err != nil {
		goto Done
	}

MainLoop:
	for {
		if runParams.putSizeUpperBound != 0 &&
			accumulatedStats.put.TotalBytes() >= runParams.putSizeUpperBound {
			break
		}

		if runParams.numEpochs > 0 { // if defined
			if numGets.Load() > int64(runParams.numEpochs)*int64(bucketObjsNames.Len()) {
				break
			}
		}

		// Prioritize showing stats otherwise we will dropping the stats intervals.
		if runParams.statsShowInterval > 0 {
			select {
			case <-statsTicker.C:
				accumulatedStats.aggregate(intervalStats)
				writeStats(statsWriter, runParams.jsonFormat, false /* final */, intervalStats, accumulatedStats)
				sendStatsdStats(&intervalStats)
				intervalStats = newStats(time.Now())
			default:
				break
			}
		}

		select {
		case <-timer.C:
			break MainLoop
		case wo := <-workOrderResults:
			completeWorkOrder(wo)
			if runParams.statsShowInterval == 0 && runParams.putSizeUpperBound != 0 {
				accumulatedStats.aggregate(intervalStats)
				intervalStats = newStats(time.Now())
			}

			if err := postNewWorkOrder(); err != nil {
				_, _ = fmt.Fprint(os.Stderr, err.Error())
				break MainLoop
			}
		case <-statsTicker.C:
			if runParams.statsShowInterval > 0 {
				accumulatedStats.aggregate(intervalStats)
				writeStats(statsWriter, runParams.jsonFormat, false /* final */, intervalStats, accumulatedStats)
				sendStatsdStats(&intervalStats)
				intervalStats = newStats(time.Now())
			}
		case sig := <-osSigChan:
			switch sig {
			case syscall.SIGHUP:
				msg := "Collecting detailed latency info is "
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
	close(workOrders)
	wg.Wait() // wait until all workers are done (notified by closing work order channel)

	// Process left over work orders
	close(workOrderResults)
	for wo := range workOrderResults {
		completeWorkOrder(wo)
	}

	fmt.Printf("\nActual run duration: %v\n", time.Since(tsStart))
	finalizeStats(statsWriter)
	if runParams.cleanUp.Val {
		cleanUp()
	}
}

func sendStatsdStats(s *sts) {
	s.statsd.SendAll(&statsdC)
}

func newPutWorkOrder() (*workOrder, error) {
	objName, err := generatePutObjectName()
	if err != nil {
		return nil, err
	}

	var size int64
	if runParams.maxSize == runParams.minSize {
		size = runParams.minSize
	} else {
		size = rnd.Int63n(runParams.maxSize-runParams.minSize) + runParams.minSize
	}

	putPending++
	return &workOrder{
		proxyURL: runParams.proxyURL,
		bucket:   runParams.bucket,
		provider: runParams.provider,
		op:       opPut,
		objName:  objName,
		size:     size,
	}, nil
}

func generatePutObjectName() (string, error) {
	cnt := objNameCnt.Inc()
	if runParams.maxputs != 0 && cnt-1 == runParams.maxputs {
		return "", fmt.Errorf("number of PUT objects reached maxputs limit (%d)", runParams.maxputs)
	}

	var (
		comps [3]string
		idx   = 0
	)

	if runParams.subDir != "" {
		comps[idx] = runParams.subDir
		idx++
	}

	if runParams.putShards != 0 {
		comps[idx] = fmt.Sprintf("%05x", cnt%runParams.putShards)
		idx++
	}

	if useRandomObjName {
		comps[idx] = cmn.RandStringWithSrc(rnd, randomObjNameLen)
		idx++
	} else {
		objectNumber := (cnt - 1) << suffixIDMaskLen
		objectNumber |= suffixID
		comps[idx] = strconv.FormatUint(objectNumber, 16)
		idx++
	}

	return path.Join(comps[0:idx]...), nil
}

func newGetWorkOrder() (*workOrder, error) {
	if bucketObjsNames.Len() == 0 {
		return nil, fmt.Errorf("no objects in bucket")
	}

	getPending++
	return &workOrder{
		proxyURL: runParams.proxyURL,
		bucket:   runParams.bucket,
		provider: runParams.provider,
		op:       opGet,
		objName:  bucketObjsNames.ObjName(),
	}, nil
}

func newGetConfigWorkOrder() *workOrder {
	return &workOrder{
		proxyURL: runParams.proxyURL,
		op:       opConfig,
	}
}

func postNewWorkOrder() (err error) {
	var wo *workOrder

	if runParams.getConfig {
		wo = newGetConfigWorkOrder()
	} else {
		if rnd.Intn(99) < runParams.putPct {
			if wo, err = newPutWorkOrder(); err != nil {
				return err
			}
		} else {
			if wo, err = newGetWorkOrder(); err != nil {
				return err
			}
		}
	}

	cmn.Assert(wo != nil)
	workOrders <- wo
	return nil
}

func validateWorkOrder(wo *workOrder, delta time.Duration) error {
	if wo.op == opGet || wo.op == opPut {
		if delta == 0 {
			return fmt.Errorf("%s has the same start time as end time", wo)
		}
	}
	return nil
}

func completeWorkOrder(wo *workOrder) {
	delta := cmn.TimeDelta(wo.end, wo.start)

	if wo.err == nil && traceHTTPSig.Load() {
		var lat *statsd.MetricLatsAgg
		switch wo.op {
		case opGet:
			lat = &intervalStats.statsd.GetLat
		case opPut:
			lat = &intervalStats.statsd.PutLat
		}
		if lat != nil {
			lat.Add("latency.proxyconn", wo.latencies.ProxyConn)
			lat.Add("latency.proxy", wo.latencies.Proxy)
			lat.Add("latency.targetconn", wo.latencies.TargetConn)
			lat.Add("latency.target", wo.latencies.Target)
			lat.Add("latency.posthttp", wo.latencies.PostHTTP)
			lat.Add("latency.proxyheader", wo.latencies.ProxyWroteHeader)
			lat.Add("latency.proxyrequest", wo.latencies.ProxyWroteRequest)
			lat.Add("latency.targetheader", wo.latencies.TargetWroteHeader)
			lat.Add("latency.proxyresponse", wo.latencies.ProxyFirstResponse)
			lat.Add("latency.targetrequest", wo.latencies.TargetWroteRequest)
			lat.Add("latency.targetresponse", wo.latencies.TargetFirstResponse)
		}
	}

	if err := validateWorkOrder(wo, delta); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] %s", err.Error())
		return
	}

	switch wo.op {
	case opGet:
		getPending--
		intervalStats.statsd.Get.AddPending(getPending)
		if wo.err == nil {
			intervalStats.get.Add(wo.size, delta)
			intervalStats.statsd.Get.Add(wo.size, delta)
		} else {
			fmt.Println("GET failed: ", wo.err)
			intervalStats.statsd.Get.AddErr()
			intervalStats.get.AddErr()
		}
	case opPut:
		putPending--
		intervalStats.statsd.Put.AddPending(putPending)
		if wo.err == nil {
			bucketObjsNames.AddObjName(wo.objName)
			intervalStats.put.Add(wo.size, delta)
			intervalStats.statsd.Put.Add(wo.size, delta)
		} else {
			fmt.Println("PUT failed: ", wo.err)
			intervalStats.put.AddErr()
			intervalStats.statsd.Put.AddErr()
		}
	case opConfig:
		if wo.err == nil {
			intervalStats.getConfig.Add(1, delta)
			intervalStats.statsd.Config.Add(delta, wo.latencies.Proxy, wo.latencies.ProxyConn)
		} else {
			fmt.Println("GET config failed: ", wo.err)
			intervalStats.getConfig.AddErr()
		}
	default:
		// Should not be here
	}
}

func cleanUp() {
	var wg sync.WaitGroup
	fmt.Println(prettyTimeStamp() + " Cleaning up ...")
	if bucketObjsNames != nil {
		// bucketObjsNames has been actually assigned to/initialized
		w := runParams.numWorkers
		objsLen := bucketObjsNames.Len()
		n := objsLen / w
		for i := 0; i < w; i++ {
			wg.Add(1)
			go _cleanup(bucketObjsNames.Names()[i*n:(i+1)*n], &wg)
		}
		if objsLen%w != 0 {
			wg.Add(1)
			go _cleanup(bucketObjsNames.Names()[n*w:], &wg)
		}
		wg.Wait()
	}

	if cmn.IsProviderAIS(runParams.provider) {
		baseParams := tutils.BaseAPIParams(runParams.proxyURL)
		api.DestroyBucket(baseParams, runParams.bucket)
	}
	fmt.Println(prettyTimeStamp() + " Done")
}

func _cleanup(objs []string, wg *sync.WaitGroup) {
	defer wg.Done()

	t := len(objs)
	if t == 0 {
		return
	}
	b := cmn.Min(t, runParams.batchSize)
	n := t / b
	for i := 0; i < n; i++ {
		err := api.DeleteList(tutils.BaseAPIParams(runParams.proxyURL), runParams.bucket, runParams.provider,
			objs[i*b:(i+1)*b], true /* wait */, 0 /* wait forever */)
		if err != nil {
			fmt.Println("delete err ", err)
		}
	}

	if t%b != 0 {
		err := api.DeleteList(tutils.BaseAPIParams(runParams.proxyURL), runParams.bucket, runParams.provider,
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

// bootStrap boot straps existing objects in the bucket
func bootStrap() error {
	names, err := tutils.ListObjectsFast(runParams.proxyURL, runParams.bucket, runParams.provider, "")
	if err != nil {
		fmt.Printf("Failed to list bucket %s(%s), proxy %s, err: %v\n",
			runParams.bucket, runParams.provider, runParams.proxyURL, err)
		return err
	}

	if runParams.subDir != "" {
		filteredNames := names[:0]

		for _, name := range names {
			if strings.HasPrefix(name, runParams.subDir) {
				filteredNames = append(filteredNames, name)
			}
		}

		names = filteredNames
	}

	if !runParams.uniqueGETs {
		bucketObjsNames = &namegetter.RandomNameGetter{}
	} else {
		bucketObjsNames = &namegetter.RandomUniqueNameGetter{}

		// Permutation strategies seem to be always better (they use more memory though)
		if runParams.putPct == 0 {
			bucketObjsNames = &namegetter.PermutationUniqueNameGetter{}

			// number from benchmarks: aisloader/tests/objNamegetter_test.go
			// After 50k overhead on new go routine and WaitGroup becomes smaller than benefits
			if len(names) > 50000 {
				bucketObjsNames = &namegetter.PermutationUniqueImprovedNameGetter{}
			}
		}
	}
	bucketObjsNames.Init(names, rnd)
	return err
}
