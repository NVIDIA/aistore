/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// AIS loader (aisloader) is a tool to measure storage performance. It's a load
// generator that has been developed (and is currently used) to benchmark and
// stress-test AIStore(tm) but can be easily extended for any S3-compatible backend.
// For usage, run: `aisloader` or `aisloader usage` or `aisloader --help`.

// Examples:
// 1. Destroy existing local bucket. If the bucket is Cloud-based, delete all objects:
//    aisloader -bucket=nvais -duration 0s -totalputsize=0
//    aisloader -bucket=nvais -bckprovider=cloud -cleanup=true -duration 0s -totalputsize=0
// 2. Timed (for 1h) 100% GET from a Cloud bucket, no cleanup:
//    aisloader -bucket=nvaws -duration 1h -numworkers=30 -pctput=0 -bckprovider=cloud -cleanup=false
// 3. Time-based PUT into local bucket, random objects names:
//    aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -bckprovider=local
// 4. Mixed 30% PUT and 70% GET to/from Cloud bucket. PUT will generate random object names and is limited by 10GB total size:
//    aisloader -bucket=nvaws -duration 0s -numworkers=3 -minsize=1MB -maxsize=1MB -pctput=30 -bckprovider=cloud -totalputsize=10G
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
	"io"
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/aisloader/namegetter"
	"github.com/NVIDIA/aistore/bench/aisloader/stats"
	"github.com/NVIDIA/aistore/cluster"
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

var examples = []string{
	"1. Cleanup (i.e., empty) existing bucket:",
	"# aisloader -bucket=nvais -duration 0s -totalputsize=0\n",
	"# aisloader -bucket=nvais -bckprovider=cloud -cleanup=true -duration 0s -totalputsize=0\n",

	"2. Time-based 100% PUT into local bucket. Upon exit the bucket is emptied (by default):",
	"# aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -bckprovider=local\n",

	"3. Timed (for 1h) 100% GET from a Cloud bucket, no cleanup:",
	"# aisloader -bucket=nvaws -duration 1h -numworkers=30 -pctput=0 -bckprovider=cloud -cleanup=false\n",

	"4. Mixed 30%/70% PUT and GET of variable-size objects to/from a Cloud bucket.\n   PUT will generate random object names and is limited by the 10GB total size.\n   Cleanup is not disabled, which means that upon completion all generated objects will be deleted:",
	"# aisloader -bucket=nvaws -duration 0s -numworkers=3 -minsize=1024 -maxsize=1MB -pctput=30 -bckprovider=cloud -totalputsize=10G\n",

	"5. PUT 1GB total into a local bucket with cleanup disabled, object size = 1MB, duration unlimited:",
	"# aisloader -bucket=nvais -cleanup=false -totalputsize=1G -duration=0 -minsize=1MB -maxsize=1MB -numworkers=8 -pctput=100 -bckprovider=local\n",

	"6. 100% GET from a local bucket:",
	"# aisloader -bucket=nvais -duration 5s -numworkers=3 -pctput=0 -bckprovider=local\n",

	"7. PUT 2000 objects named as `aisloader/hex({0..2000}{loaderid})`:",
	"# aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -objNamePrefix=\"aisloader\"\n",

	"8. Use random object names and loaderID to report statistics:",
	"# aisloader -loaderid=10\n",

	"9. PUT objects with random name generation being based on the specified loaderID and the total number of concurrent aisloaders:",
	"# aisloader -loaderid=10 -loadernum=20\n",

	"10. Same as above except that loaderID is computed by the aisloader as hash(loaderstring) & 0xff:",
	"# aisloader -loaderid=loaderstring -loaderidhashlen=8\n",

	"11. Print loaderID and exit (all 3 examples below) with the resulting loaderID shown on the right:",
	"# aisloader -getloaderid (0x0)",
	"# aisloader -loaderid=10 -getloaderid (0xa)",
	"# aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid (0xdb)",
}
var (
	version = "1.0"
	build   string
)

type (
	workOrder struct {
		op          int
		proxyURL    string
		bucket      string
		bckProvider string
		objName     string // In the format of 'virtual dir' + "/" + objName
		size        int64
		err         error
		start       time.Time
		end         time.Time
		latencies   tutils.HTTPLatencies
	}

	params struct {
		seed              int64 // random seed; UnixNano() if omitted
		putSizeUpperBound int64
		minSize           int64
		maxSize           int64
		readOff           int64 // read offset
		readLen           int64 // read length
		loaderCnt         uint64
		maxputs           uint64

		loaderID             string // used with multiple loader instances generating objects in parallel
		proxyURL             string
		bckProvider          string // "local" or "cloud"
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
	}

	// sts records accumulated puts/gets information.
	sts struct {
		put       stats.HTTPReq
		get       stats.HTTPReq
		getConfig stats.HTTPReq
		statsd    statsd.Metrics
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
	bucketObjsNames  namegetter.ObjectNameGetter
	statsPrintHeader = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
	statsdC          statsd.Client
	getPending       int64
	putPending       int64

	flagUsage   bool
	flagVersion bool

	ip   string
	port string

	useRandomObjName bool
	objNameCnt       atomic.Uint64

	suffixIDMaskLen uint
	suffixID        uint64

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

func printUsage(f *flag.FlagSet) {
	fmt.Println("\nAbout")
	fmt.Println("=====")
	fmt.Printf("AIS loader (aisloader v%s, build %s) is a tool to measure storage performance.\n", version, build)
	fmt.Println("It's a load generator that has been developed (and is currently used) to benchmark and")
	fmt.Println("stress-test AIStore(tm) but can be easily extended for any S3-compatible backend.")
	fmt.Println("For usage, run: `aisloader` or `aisloader usage` or `aisloader --help`.")
	fmt.Println("Further details at https://github.com/NVIDIA/aistore/blob/master/docs/howto_benchmark.md")

	fmt.Println("\nCommand-line options")
	fmt.Println("====================")
	f.PrintDefaults()
	fmt.Println()

	fmt.Println("\nExamples")
	fmt.Println("========")
	for i := 0; i < len(examples); i++ {
		fmt.Println(examples[i])
	}
}

func loaderMaskFromTotalLoaders(totalLoaders uint64) uint {
	// take first bigger power of 2:
	// example: for loaderCnt 13, mask length should be 4 (2^4 == 16 > 13)
	res := cmn.CeilAlign(cmn.FastLog2Ceil(totalLoaders), 4)

	// if result is not divisible by 4 add reduntant bytes
	// so objectname is better readable in hex representation
	// (then each aisloader has unique last characters, not just bytes)
	return res + 4 - (res % 4)
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
	f.StringVar(&p.bckProvider, "bckprovider", cmn.LocalBs, "local - for local bucket, cloud - for Cloud based bucket")

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
	f.BoolVar(&p.uniqueGETs, "uniquegets", true, "true: GET objects randomly and equally. Meaning, make sure *not* to GET some objects more frequently than the others")

	//
	// advanced usage
	//
	f.BoolVar(&p.getConfig, "getconfig", false, "true: generate control plane load by reading AIS proxy configuration (that is, instead of reading/writing data exercise control path)")
	f.StringVar(&p.statsOutput, "stats-output", "", "filename to log statistics (empty string translates as standard output (default))")
	f.BoolVar(&p.stoppable, "stoppable", false, "true: stop upon CTRL-C")
	f.BoolVar(&p.dryRun, "dry-run", false, "true: show the configuration and parameters that aisloader will use for benchmark")

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
			if p.loaderIDHashLen <= 0 || p.loaderIDHashLen > 63 {
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

	if !p.cleanUp.IsSet {
		p.cleanUp.Val = p.bckProvider == cmn.LocalBs
	}

	// For Dry-Run on Docker
	if tutils.DockerRunning() && ip == "localhost" {
		ip = dockerHostIP
	}

	if tutils.DockerRunning() && port == "8080" {
		port = dockerPort
	}

	p.proxyURL = "http://" + ip + ":" + port

	// Don't print arguments when just getting loaderID
	if !p.getLoaderID {
		printArguments(f)
	}
	return p, nil
}

func printArguments(set *flag.FlagSet) {
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)

	_, _ = fmt.Fprintf(w, "\n==== COMMAND LINE ARGUMENTS ============\n")
	_, _ = fmt.Fprintf(w, "======== DEFAULTS ========\n")
	set.VisitAll(func(f *flag.Flag) {
		if f.Value.String() == f.DefValue {
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", f.Name, f.Value.String())
		}
	})
	_, _ = fmt.Fprintf(w, "======== CUSTOM ==========\n")
	set.Visit(func(f *flag.Flag) {
		if f.Value.String() != f.DefValue {
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", f.Name, f.Value.String())
		}
	})
	_, _ = fmt.Fprintf(w, "========================================\n\n")
	_ = w.Flush()
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

func setupBucket(runParams *params) error {
	if runParams.bckProvider != cmn.LocalBs || runParams.getConfig {
		return nil
	}
	baseParams := tutils.BaseAPIParams(runParams.proxyURL)
	exists, err := api.DoesLocalBucketExist(baseParams, runParams.bucket)
	if err != nil {
		return fmt.Errorf("failed to get local bucket lists to check for %s, err = %v", runParams.bucket, err)
	}

	if !exists {
		err := api.CreateLocalBucket(baseParams, runParams.bucket)
		if err != nil {
			return fmt.Errorf("failed to create local bucket %s, err = %s", runParams.bucket, err)
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
	hash := xxhash.ChecksumString64S(val, cluster.MLCG32)
	// leave just right loaderIDHashLen bytes
	hash <<= (64 - hashLen)
	hash >>= (64 - hashLen)
	return hash
}

func main() {
	var (
		wg  sync.WaitGroup
		err error
	)

	runParams, err = parseCmdLine()
	if err != nil {
		cmn.ExitInfof("%s", err)
	}

	if runParams.getLoaderID {
		fmt.Printf("0x%x\n", suffixID)
		if useRandomObjName {
			fmt.Printf("Warning: loaderID 0x%x used only for statsd, not for object names!\n", suffixID)
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
			fmt.Println("Failed to boot strap, err = ", err)
			return
		}

		objsLen := bucketObjsNames.Len()
		if runParams.putPct == 0 && objsLen == 0 {
			fmt.Println("Nothing to read, bucket is empty")
			return
		}

		fmt.Printf("Found %d existing objects\n", objsLen)
	}

	osSigChan := make(chan os.Signal, 2)
	if runParams.stoppable {
		signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)
	}

	logRunParams(runParams)

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
		fmt.Printf("%s", "Failed to connect to statsd server")
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
		go worker(workOrders, workOrderResults, &wg)
	}

	var statsTicker *time.Ticker
	timer := time.NewTimer(runParams.duration.Val)

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
				var wo *workOrder
				wo, err = newPutWorkOrder()
				if err != nil {
					break
				}
				workOrders <- wo
			}
		}
	}

	if err != nil {
		goto DONE
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

			if err := newWorkOrder(); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, err.Error())
				break L
			}
		case <-statsTicker.C:
			if runParams.statsShowInterval > 0 {
				accumulatedStats.aggregate(intervalStats)
				writeStats(statsWriter, runParams.jsonFormat, false /* final */, intervalStats, accumulatedStats)
				sendStatsdStats(&intervalStats)
				intervalStats = newStats(time.Now())
			}
		case <-osSigChan:
			if runParams.stoppable {
				break L
			}
		default:
			// Do nothing
		}
	}

DONE:
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
	if runParams.cleanUp.Val {
		cleanUp()
	}
}

// lonRunParams show run parameters in json format
func logRunParams(p params) {
	b, _ := json.MarshalIndent(struct {
		Seed          int64  `json:"seed"`
		URL           string `json:"proxy"`
		BckProvider   string `json:"bprovider"`
		Bucket        string `json:"bucket"`
		Duration      string `json:"duration"`
		MaxPutBytes   int64  `json:"PUT upper bound"`
		PutPct        int    `json:"% PUT"`
		MinSize       int64  `json:"minimum object size (bytes)"`
		MaxSize       int64  `json:"maximum object size (bytes)"`
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
		StatsInterval: (time.Duration(runParams.statsShowInterval) * time.Second).String(),
		Backing:       p.readerType,
		Cleanup:       p.cleanUp.Val,
	}, "", "   ")

	fmt.Printf("Runtime configuration:\n%s\n\n", string(b))
	if p.dryRun {
		os.Exit(0)
	}
	time.Sleep(time.Second)
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
	if n == 0 {
		return "-"
	}

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

func prettySpeed(n int64) string {
	if n == 0 {
		return "-"
	}
	return prettyNumBytes(n) + "/s"
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

func preWriteStats(to io.Writer, jsonFormat bool) {
	fmt.Fprintln(to)
	if !jsonFormat {
		fmt.Fprintf(to, statsPrintHeader,
			"Time", "OP", "Count", "Total Bytes", "Latency(min, avg, max)", "Throughput", "Error")
	} else {
		fmt.Fprintln(to, "[")
	}
}

func postWriteStats(to io.Writer, jsonFormat bool) {
	if jsonFormat {
		fmt.Fprintln(to)
		fmt.Fprintln(to, "]")
	}
}

func writeFinalStats(to io.Writer, jsonFormat bool, s sts) {
	if !jsonFormat {
		writeHumanReadibleFinalStats(to, s)
	} else {
		writeStatsJSON(to, s, false)
	}
}

func writeIntervalStats(to io.Writer, jsonFormat bool, s, t sts) {
	if !jsonFormat {
		writeHumanReadibleIntervalStats(to, s, t)
	} else {
		writeStatsJSON(to, s)
	}
}

func jsonStatsFromReq(r stats.HTTPReq) *jsonStats {
	jStats := &jsonStats{
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

	return jStats
}

func writeStatsJSON(to io.Writer, s sts, withcomma ...bool) {
	jStats := struct {
		Get *jsonStats `json:"get"`
		Put *jsonStats `json:"put"`
		Cfg *jsonStats `json:"cfg"`
	}{
		Get: jsonStatsFromReq(s.get),
		Put: jsonStatsFromReq(s.put),
		Cfg: jsonStatsFromReq(s.getConfig),
	}
	jsonOutput, err := json.Marshal(jStats)
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

func writeHumanReadibleIntervalStats(to io.Writer, s, t sts) {
	p := fmt.Fprintf
	pn := prettyNumber
	pb := prettyNumBytes
	pl := prettyLatency
	pt := prettyTimeStamp

	workOrderResLen := int64(len(workOrderResults))
	// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
	if s.put.Total() != 0 {
		p(to, statsPrintHeader, pt(), "PUT",
			pn(s.put.Total())+"("+pn(t.put.Total())+" "+pn(putPending)+" "+pn(workOrderResLen)+")",
			pb(s.put.TotalBytes())+"("+pb(t.put.TotalBytes())+")",
			pl(s.put.MinLatency(), s.put.AvgLatency(), s.put.MaxLatency()),
			pb(s.put.Throughput(s.put.Start(), time.Now()))+"("+pb(t.put.Throughput(t.put.Start(), time.Now()))+")",
			pn(s.put.TotalErrs())+"("+pn(t.put.TotalErrs())+")")
	}
	if s.get.Total() != 0 {
		p(to, statsPrintHeader, pt(), "GET",
			pn(s.get.Total())+"("+pn(t.get.Total())+" "+pn(getPending)+" "+pn(workOrderResLen)+")",
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

func writeHumanReadibleFinalStats(to io.Writer, t sts) {
	p := fmt.Fprintf
	pn := prettyNumber
	pb := prettyNumBytes
	ps := prettySpeed
	pl := prettyLatency
	pt := prettyTimeStamp
	preWriteStats(to, false)
	p(to, statsPrintHeader, pt(), "PUT",
		pn(t.put.Total()),
		pb(t.put.TotalBytes()),
		pl(t.put.MinLatency(), t.put.AvgLatency(), t.put.MaxLatency()),
		ps(t.put.Throughput(t.put.Start(), time.Now())),
		pn(t.put.TotalErrs()))
	p(to, statsPrintHeader, pt(), "GET",
		pn(t.get.Total()),
		pb(t.get.TotalBytes()),
		pl(t.get.MinLatency(), t.get.AvgLatency(), t.get.MaxLatency()),
		ps(t.get.Throughput(t.get.Start(), time.Now())),
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
func writeStats(to io.Writer, jsonFormat, final bool, s, t sts) {
	if final {
		writeFinalStats(to, jsonFormat, t)
	} else {
		// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
		writeIntervalStats(to, jsonFormat, s, t)
	}
}

func sendStatsdStats(s *sts) {
	s.statsd.SendAll(&statsdC)
}

func newPutWorkOrder() (*workOrder, error) {
	objName, err := putObjectname()
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
		proxyURL:    runParams.proxyURL,
		bucket:      runParams.bucket,
		bckProvider: runParams.bckProvider,
		op:          opPut,
		objName:     objName,
		size:        size,
	}, nil
}

func putObjectname() (string, error) {
	cnt := objNameCnt.Inc()
	if runParams.maxputs != 0 && cnt-1 == runParams.maxputs {
		return "", fmt.Errorf("number of PUT objects reached maxputs limit (%d)", runParams.maxputs)
	}

	var baseObjName string

	if useRandomObjName {
		baseObjName = tutils.FastRandomFilename(rnd, randomObjNameLen)
	} else {
		objectNumber := (cnt - 1) << suffixIDMaskLen
		objectNumber |= suffixID
		baseObjName = strconv.FormatUint(objectNumber, 16)
	}

	if runParams.subDir == "" {
		return baseObjName, nil
	}
	return path.Join(runParams.subDir, baseObjName), nil
}

func newGetWorkOrder() *workOrder {
	if bucketObjsNames.Len() == 0 {
		return nil
	}

	getPending++
	return &workOrder{
		proxyURL:    runParams.proxyURL,
		bucket:      runParams.bucket,
		bckProvider: runParams.bckProvider,
		op:          opGet,
		objName:     bucketObjsNames.ObjName(),
	}
}

func newGetConfigWorkOrder() *workOrder {
	return &workOrder{
		proxyURL: runParams.proxyURL,
		op:       opConfig,
	}
}

func newWorkOrder() (err error) {
	var wo *workOrder

	if runParams.getConfig {
		wo = newGetConfigWorkOrder()
	} else {
		if rnd.Intn(99) < runParams.putPct {
			if wo, err = newPutWorkOrder(); err != nil {
				return err
			}
		} else {
			wo = newGetWorkOrder()
		}
	}

	if wo != nil {
		workOrders <- wo
	}
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

	if wo.err == nil {
		intervalStats.statsd.General.Add("latency.proxyconn", wo.latencies.ProxyConn)
		intervalStats.statsd.General.Add("latency.proxy", wo.latencies.Proxy)
		intervalStats.statsd.General.Add("latency.targetconn", wo.latencies.TargetConn)
		intervalStats.statsd.General.Add("latency.target", wo.latencies.Target)
		intervalStats.statsd.General.Add("latency.posthttp", wo.latencies.PostHTTP)
		intervalStats.statsd.General.Add("latency.proxyheader", wo.latencies.ProxyWroteHeader)
		intervalStats.statsd.General.Add("latency.proxyrequest", wo.latencies.ProxyWroteRequest)
		intervalStats.statsd.General.Add("latency.targetheader", wo.latencies.TargetWroteHeader)
		intervalStats.statsd.General.Add("latency.proxyresponse", wo.latencies.ProxyFirstResponse)
		intervalStats.statsd.General.Add("latency.targetrequest", wo.latencies.TargetWroteRequest)
		intervalStats.statsd.General.Add("latency.targetresponse", wo.latencies.TargetFirstResponse)
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
	fmt.Println(prettyTimeStamp() + " Cleaning up ...")

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

	if bucketObjsNames != nil {
		// bucketObjsNames has been actually assigned to/initialized
		w := runParams.numWorkers
		objsLen := bucketObjsNames.Len()
		n := objsLen / w
		for i := 0; i < w; i++ {
			wg.Add(1)
			go f(bucketObjsNames.Names()[i*n:(i+1)*n], &wg)
		}

		if objsLen%w != 0 {
			wg.Add(1)
			go f(bucketObjsNames.Names()[n*w:], &wg)
		}

		wg.Wait()
	}

	if runParams.bckProvider == cmn.LocalBs {
		baseParams := tutils.BaseAPIParams(runParams.proxyURL)
		api.DestroyLocalBucket(baseParams, runParams.bucket)
	}

	fmt.Println(prettyTimeStamp() + " Done")
}

// bootStrap boot straps existing objects in the bucket
func bootStrap() error {
	names, err := tutils.ListObjectsFast(runParams.proxyURL, runParams.bucket, runParams.bckProvider, "")

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
