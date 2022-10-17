// Package aisloader
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */

// AIS loader (aisloader) is a tool to measure storage performance. It's a load
// generator that has been developed to benchmark and stress-test AIStore
// but can be easily extended to benchmark any S3-compatible backend.
// For usage, run: `aisloader`, or `aisloader usage`, or `aisloader --help`.

// Examples:
// 1. Destroy existing ais bucket:
//    a) aisloader -bucket=ais://abc -duration 0s -totalputsize=0 -cleanup=true
//    Delete all objects in a given AWS-based bucket:
//    b) aisloader -bucket=aws://nvais -cleanup=true -duration 0s -totalputsize=0
// 2. Timed (for 1h) 100% GET from a Cloud bucket (no cleanup):
//    aisloader -bucket=aws://mybucket -duration 1h -numworkers=30 -pctput=0 -cleanup=false
// 3. Time-based PUT into ais bucket, random objects names:
//    aisloader -bucket=abc -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais -cleanup=true
// 4. Mixed 30% PUT and 70% GET to/from a Cloud bucket. PUT will generate random object names and is limited by 10GB total size:
//    aisloader -bucket=gs://nvgs -duration 0s -numworkers=3 -minsize=1MB -maxsize=1MB -pctput=30 -totalputsize=10G -cleanup=false
// 5. PUT 2000 objects with names that look like hex({0..2000}{loaderid})
//    aisloader -bucket=ais://abc -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -cleanup=false
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

package aisloader

import (
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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/bench/aisloader/namegetter"
	"github.com/NVIDIA/aistore/bench/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const (
	opPut = iota
	opGet
	opConfig

	myName           = "loader"
	randomObjNameLen = 32

	wo2FreeSize  = 4096
	wo2FreeDelay = 3*time.Second + time.Millisecond

	ua = "aisloader"
)

type (
	workOrder struct {
		op        int
		proxyURL  string
		bck       cmn.Bck
		objName   string // In the format of 'virtual dir' + "/" + objName
		size      int64
		err       error
		start     time.Time
		end       time.Time
		latencies httpLatencies
		cksumType string
		sgl       *memsys.SGL
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
		putShards         uint64
		statsdPort        int
		statsShowInterval int
		putPct            int // % of puts, rest are gets
		numWorkers        int
		batchSize         int // batch is used for bootstraping(list) and delete
		loaderIDHashLen   uint
		numEpochs         uint

		duration cos.DurationExt // stop after the run for at least that much

		bp api.BaseParams

		bck    cmn.Bck
		bProps cmn.BucketProps

		loaderID             string // used with multiple loader instances generating objects in parallel
		proxyURL             string
		readerType           string
		tmpDir               string // used only when usingFile
		statsOutput          string
		cksumType            string
		statsdIP             string
		bPropsStr            string
		putSizeUpperBoundStr string // stop after writing that amount of data
		minSizeStr           string
		maxSizeStr           string
		readOffStr           string // read offset
		readLenStr           string // read length
		subDir               string
		tokenFile            string

		etlName     string // name of a ETL to apply to each object. Omitted when etlSpecPath specified.
		etlSpecPath string // Path to a ETL spec to apply to each object.

		cleanUp cos.BoolExt // cleanup i.e. remove and destroy everything created during bench

		statsdProbe   bool
		getLoaderID   bool
		randomObjName bool
		uniqueGETs    bool
		verifyHash    bool // verify xxhash during get
		getConfig     bool // true: load control plane (read proxy config)
		jsonFormat    bool
		stoppable     bool // true: terminate by Ctrl-C
		dryRun        bool // true: print configuration and parameters that aisloader will use at runtime
		traceHTTP     bool // true: trace http latencies as per httpLatencies & https://golang.org/pkg/net/http/httptrace
	}

	// sts records accumulated puts/gets information.
	sts struct {
		put       stats.HTTPReq
		get       stats.HTTPReq
		getConfig stats.HTTPReq
		statsd    stats.Metrics
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
	wo2Free          []*workOrder
	intervalStats    sts
	accumulatedStats sts
	bucketObjsNames  namegetter.ObjectNameGetter
	statsPrintHeader = "%-10s%-6s%-22s\t%-22s\t%-36s\t%-22s\t%-10s\n"
	statsdC          *statsd.Client
	getPending       int64
	putPending       int64
	traceHTTPSig     atomic.Bool

	flagUsage   bool
	flagVersion bool

	etlInitSpec *etl.InitSpecMsg
	etlID       string

	useRandomObjName bool
	objNameCnt       atomic.Uint64

	suffixIDMaskLen uint
	suffixID        uint64

	numGets atomic.Int64

	gmm      *memsys.MMSA
	stopping atomic.Bool

	ip          string
	port        string
	aisEndpoint string
	envEndpoint string

	loggedUserToken string
)

var _version, _buildtime string

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
		wo.bck, wo.objName, wo.start.Format(time.StampMilli), wo.end.Format(time.StampMilli), wo.size, opName, errstr)
}

func loaderMaskFromTotalLoaders(totalLoaders uint64) uint {
	// take first bigger power of 2, then take first bigger or equal number
	// divisible by 4. This makes loaderID more visible in hex object name
	return cos.CeilAlign(cos.FastLog2Ceil(totalLoaders), 4)
}

func parseCmdLine() (params, error) {
	var (
		p   params
		err error
	)

	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError) // discard flags of imported packages

	f.BoolVar(&flagUsage, "usage", false, "Show command-line options, usage, and examples")
	f.BoolVar(&flagVersion, "version", false, "Show aisloader version")
	f.DurationVar(&transportArgs.Timeout, "timeout", 10*time.Minute, "Client HTTP timeout - used in LIST/GET/PUT/DELETE")
	f.IntVar(&p.statsShowInterval, "statsinterval", 10, "Interval in seconds to print performance counters; 0 - disabled")
	f.StringVar(&p.bck.Name, "bucket", "", "Bucket name or bucket URI. If empty, a bucket with random name will be created")
	f.StringVar(&p.bck.Provider, "provider", apc.AIS,
		"ais - for AIS bucket, \"aws\", \"azure\", \"gcp\", \"hdfs\"  for Azure, Amazon, Google, and HDFS clouds respectively")
	f.StringVar(&ip, "ip", "localhost", "AIS proxy/gateway IP address or hostname")
	f.StringVar(&port, "port", "8080", "AIS proxy/gateway port")

	cos.DurationExtVar(f, &p.duration, "duration", time.Minute,
		"Benchmark duration (0 - run forever or until Ctrl-C). Note that if both duration and totalputsize are 0 (zeros), aisloader will have nothing to do.\n"+
			"If not specified and totalputsize > 0, aisloader runs until totalputsize reached. Otherwise aisloader runs until first of duration and "+
			"totalputsize reached")

	f.IntVar(&p.numWorkers, "numworkers", 10, "Number of goroutine workers operating on AIS in parallel")
	f.IntVar(&p.putPct, "pctput", 0, "Percentage of PUTs in the aisloader-generated workload")
	f.StringVar(&p.tmpDir, "tmpdir", "/tmp/ais", "Local directory to store temporary files")
	f.StringVar(&p.putSizeUpperBoundStr, "totalputsize", "0",
		"Stop PUT workload once cumulative PUT size reaches or exceeds this value (can contain standard multiplicative suffix K, MB, GiB, etc.; 0 - unlimited")
	cos.BoolExtVar(f, &p.cleanUp, "cleanup", "true: remove bucket upon benchmark termination (mandatory: must be specified)")
	f.BoolVar(&p.verifyHash, "verifyhash", false,
		"true: checksum-validate GET: recompute object checksums and validate it against the one received with the GET metadata")
	f.StringVar(&p.minSizeStr, "minsize", "", "Minimum object size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.maxSizeStr, "maxsize", "", "Maximum object size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.readerType, "readertype", readers.ReaderTypeSG,
		fmt.Sprintf("Type of reader: %s(default) | %s | %s | %s", readers.ReaderTypeSG, readers.ReaderTypeFile, readers.ReaderTypeRand, readers.ReaderTypeTar))
	f.StringVar(&p.loaderID, "loaderid", "0", "ID to identify a loader among multiple concurrent instances")
	f.StringVar(&p.statsdIP, "statsdip", "localhost", "StatsD IP address or hostname")
	f.StringVar(&p.tokenFile, "tokenfile", "", "authentication token (FQN)") // see also: AIS_AUTHN_TOKEN_FILE
	f.IntVar(&p.statsdPort, "statsdport", 8125, "StatsD UDP port")
	f.BoolVar(&p.statsdProbe, "test-probe StatsD server prior to benchmarks", false, "when enabled probes StatsD server prior to running")
	f.IntVar(&p.batchSize, "batchsize", 100, "Batch size to list and delete")
	f.StringVar(&p.bPropsStr, "bprops", "", "JSON string formatted as per the SetBucketProps API and containing bucket properties to apply")
	f.Int64Var(&p.seed, "seed", 0, "Random seed to achieve deterministic reproducible results (0 - use current time in nanoseconds)")
	f.BoolVar(&p.jsonFormat, "json", false, "Defines whether to print output in JSON format")
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
	f.BoolVar(&p.uniqueGETs, "uniquegets", true,
		"true: GET objects randomly and equally. Meaning, make sure *not* to GET some objects more frequently than the others")

	//
	// advanced usage
	//
	f.BoolVar(&p.getConfig, "getconfig", false,
		"true: generate control plane load by reading AIS proxy configuration (that is, instead of reading/writing data exercise control path)")
	f.StringVar(&p.statsOutput, "stats-output", "", "filename to log statistics (empty string translates as standard output (default))")
	f.BoolVar(&p.stoppable, "stoppable", false, "true: stop upon CTRL-C")
	f.BoolVar(&p.dryRun, "dry-run", false, "true: show the configuration and parameters that aisloader will use for benchmark")
	f.BoolVar(&p.traceHTTP, "trace-http", false, "true: trace HTTP latencies") // see httpLatencies
	f.StringVar(&p.cksumType, "cksum-type", cos.ChecksumXXHash, "cksum type to use for put object requests")

	// ETL
	f.StringVar(&p.etlName, "etl", "", "name of an ETL applied to each object on GET request. One of '', 'tar2tf', 'md5', 'echo'")
	f.StringVar(&p.etlSpecPath, "etl-spec", "", "path to an ETL spec to be applied to each object on GET request.")

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
		fmt.Printf("version %s (build %s)\n", _version, _buildtime)
		os.Exit(0)
	}
	if !p.cleanUp.IsSet && p.bck.Name != "" {
		fmt.Println("\nNote: `-cleanup` is a mandatory (required) option defining whether to destroy bucket upon completion of the benchmark.")
		fmt.Println("      The option must be specified in the command line.")
		os.Exit(1)
	}

	if p.seed == 0 {
		p.seed = mono.NanoTime()
	}
	rnd = rand.New(rand.NewSource(p.seed))

	if p.putSizeUpperBoundStr != "" {
		if p.putSizeUpperBound, err = cos.S2B(p.putSizeUpperBoundStr); err != nil {
			return params{}, fmt.Errorf("failed to parse total PUT size %s: %v", p.putSizeUpperBoundStr, err)
		}
	}

	if p.minSizeStr != "" {
		if p.minSize, err = cos.S2B(p.minSizeStr); err != nil {
			return params{}, fmt.Errorf("failed to parse min size %s: %v", p.minSizeStr, err)
		}
	} else {
		p.minSize = cos.MiB
	}

	if p.maxSizeStr != "" {
		if p.maxSize, err = cos.S2B(p.maxSizeStr); err != nil {
			return params{}, fmt.Errorf("failed to parse max size %s: %v", p.maxSizeStr, err)
		}
	} else {
		p.maxSize = cos.GiB
	}

	if !p.duration.IsSet {
		if p.putSizeUpperBound != 0 {
			// user specified putSizeUpperBound, but not duration, override default 1 minute
			// and run aisloader until putSizeUpperBound is reached
			p.duration.Val = time.Duration(math.MaxInt64)
		} else {
			fmt.Printf("\nDuration not specified - running for %v\n\n", p.duration.Val)
		}
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
		if p.readOff, err = cos.S2B(p.readOffStr); err != nil {
			return params{}, fmt.Errorf("failed to parse read offset %s: %v", p.readOffStr, err)
		}
	}
	if p.readLenStr != "" {
		if p.readLen, err = cos.S2B(p.readLenStr); err != nil {
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

			suffixIDMaskLen = cos.CeilAlign(p.loaderIDHashLen, 4)
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

	if err := cos.ValidateCksumType(p.cksumType); err != nil {
		return params{}, err
	}

	if p.etlName != "" && p.etlSpecPath != "" {
		return params{}, fmt.Errorf("etl and etl-spec flag can't be set both")
	}

	if p.etlSpecPath != "" {
		fh, err := os.Open(p.etlSpecPath)
		if err != nil {
			return params{}, err
		}
		etlSpec, err := io.ReadAll(fh)
		fh.Close()
		if err != nil {
			return params{}, err
		}
		etlInitSpec, err = tetl.SpecToInitMsg(etlSpec)
		if err != nil {
			return params{}, err
		}
	}

	if p.etlName != "" {
		etlSpec, err := tetl.GetTransformYaml(p.etlName)
		if err != nil {
			return params{}, err
		}
		etlInitSpec, err = tetl.SpecToInitMsg(etlSpec)
		if err != nil {
			return params{}, err
		}
	}

	if p.bPropsStr != "" {
		var bprops cmn.BucketProps
		jsonStr := strings.TrimRight(p.bPropsStr, ",")
		if !strings.HasPrefix(jsonStr, "{") {
			jsonStr = "{" + strings.TrimRight(jsonStr, ",") + "}"
		}

		if err := jsoniter.Unmarshal([]byte(jsonStr), &bprops); err != nil {
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
			if p.bProps.Mirror.Burst == 0 {
				p.bProps.Mirror.Burst = 512
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

	aisEndpoint = "http://" + ip + ":" + port
	envEndpoint = os.Getenv(env.AIS.Endpoint)
	if envEndpoint != "" {
		aisEndpoint = envEndpoint
	}

	traceHTTPSig.Store(p.traceHTTP)

	scheme, address := cmn.ParseURLScheme(aisEndpoint)
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "https" {
		return params{}, fmt.Errorf("unknown scheme %q", scheme)
	}

	p.proxyURL = scheme + "://" + address

	transportArgs.UseHTTPS = scheme == "https"
	httpClient = cmn.NewClient(transportArgs)

	// NOTE: auth token is assigned below when we execute the very first API call
	p.bp = api.BaseParams{Client: httpClient, URL: p.proxyURL}

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
		statsd:    stats.NewStatsdMetrics(t),
	}
}

// aggregate adds another sts to self
func (s *sts) aggregate(other sts) {
	s.get.Aggregate(other.get)
	s.put.Aggregate(other.put)
	s.getConfig.Aggregate(other.getConfig)
}

func setupBucket(runParams *params) error {
	if runParams.bck.Provider != apc.AIS || runParams.getConfig {
		return nil
	}
	if runParams.bck.Name == "" {
		runParams.bck.Name = cos.RandStringStrong(8)
		fmt.Printf("New bucket name %q\n", runParams.bck.Name)
	} else if nbck, objName, err := cmn.ParseBckObjectURI(runParams.bck.Name, cmn.ParseURIOpts{}); err == nil {
		if objName != "" {
			return fmt.Errorf("expecting bucket name or a bucket URI with no object name in it: %s => [%v, %s]",
				runParams.bck, nbck, objName)
		}
		runParams.bck = nbck
	}
	exists, err := api.QueryBuckets(runParams.bp, cmn.QueryBcks(runParams.bck), apc.FltExists)
	if err != nil {
		return fmt.Errorf("%s not found: %v", runParams.bck, err)
	}
	if !exists {
		if err := api.CreateBucket(runParams.bp, runParams.bck, nil); err != nil {
			return fmt.Errorf("failed to create %s: %v", runParams.bck, err)
		}
	}
	if runParams.bPropsStr == "" {
		return nil
	}
	propsToUpdate := cmn.BucketPropsToUpdate{}
	// update bucket props if bPropsStr is set
	oldProps, err := api.HeadBucket(runParams.bp, runParams.bck, true /* don't add */)
	if err != nil {
		return fmt.Errorf("failed to read bucket %s properties: %v", runParams.bck, err)
	}
	change := false
	if runParams.bProps.EC.Enabled != oldProps.EC.Enabled {
		propsToUpdate.EC = &cmn.ECConfToUpdate{
			Enabled:      api.Bool(runParams.bProps.EC.Enabled),
			ObjSizeLimit: api.Int64(runParams.bProps.EC.ObjSizeLimit),
			DataSlices:   api.Int(runParams.bProps.EC.DataSlices),
			ParitySlices: api.Int(runParams.bProps.EC.ParitySlices),
		}
		change = true
	}
	if runParams.bProps.Mirror.Enabled != oldProps.Mirror.Enabled {
		propsToUpdate.Mirror = &cmn.MirrorConfToUpdate{
			Enabled: api.Bool(runParams.bProps.Mirror.Enabled),
			Copies:  api.Int64(runParams.bProps.Mirror.Copies),
			Burst:   api.Int(runParams.bProps.Mirror.Burst),
		}
		change = true
	}
	if change {
		if _, err = api.SetBucketProps(runParams.bp, runParams.bck, &propsToUpdate); err != nil {
			return fmt.Errorf("failed to enable EC for the bucket %s properties: %v", runParams.bck, err)
		}
	}
	return nil
}

func getIDFromString(val string, hashLen uint) uint64 {
	hash := xxhash.ChecksumString64S(val, cos.MLCG32)
	// leave just right loaderIDHashLen bytes
	hash <<= 64 - hashLen
	hash >>= 64 - hashLen
	return hash
}

func Start(version, buildtime string) (err error) {
	wg := &sync.WaitGroup{}
	_version, _buildtime = version, buildtime
	runParams, err = parseCmdLine()
	if err != nil {
		return err
	}

	if runParams.getLoaderID {
		fmt.Printf("0x%x\n", suffixID)
		if useRandomObjName {
			fmt.Printf("Warning: loaderID 0x%x used only for StatsD, not for object names!\n", suffixID)
		}
		return nil
	}

	// If neither duration nor put upper bound is specified, it is a no op.
	// Note that stoppable prevents being a no op
	// This can be used as a cleanup only run (no put no get).
	if runParams.duration.Val == 0 {
		if runParams.putSizeUpperBound == 0 && !runParams.stoppable {
			if runParams.cleanUp.Val {
				cleanup()
			}
			return nil
		}

		runParams.duration.Val = time.Duration(math.MaxInt64)
	}

	if runParams.readerType == readers.ReaderTypeFile {
		if err := cos.CreateDir(runParams.tmpDir + "/" + myName); err != nil {
			return fmt.Errorf("failed to create local test directory %q, err = %s", runParams.tmpDir, err.Error())
		}
	}

	loggedUserToken = authn.LoadToken(runParams.tokenFile)
	runParams.bp.Token = loggedUserToken
	runParams.bp.UA = ua
	if err := setupBucket(&runParams); err != nil {
		return err
	}

	if !runParams.getConfig {
		if err := bootstrap(); err != nil {
			return err
		}

		objsLen := bucketObjsNames.Len()
		if runParams.putPct == 0 && objsLen == 0 {
			return errors.New("nothing to read, bucket is empty")
		}

		fmt.Printf("Found %d existing objects\n", objsLen)
	}

	printRunParams(runParams)
	if runParams.dryRun { // dry-run so just print the configurations and exit
		os.Exit(0)
	}

	if runParams.cleanUp.Val {
		fmt.Printf("BEWARE: cleanup is enabled, bucket %s will be destroyed upon termination!\n", runParams.bck)
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
	hk.Init(&stopping)
	go hk.DefaultHK.Run()
	hk.WaitStarted()

	memsys.Init(prefixC, prefixC, &cmn.Config{})
	gmm = memsys.PageMM()
	gmm.RegWithHK()

	if etlInitSpec != nil {
		fmt.Println(prettyTimestamp() + " Waiting for an ETL to start...")
		etlID, err = api.ETLInit(runParams.bp, etlInitSpec)
		if err != nil {
			return fmt.Errorf("failed to initialize ETL: %v", err)
		}
		fmt.Println(prettyTimestamp() + " ETL started")

		defer func() {
			fmt.Println(prettyTimestamp() + " Stopping the ETL...")
			if err := api.ETLStop(runParams.bp, etlID); err != nil {
				fmt.Printf("%s Failed to stop the ETL: %v\n", prettyTimestamp(), err)
				return
			}
			fmt.Println(prettyTimestamp() + " ETL stopped")
		}()
	}

	workOrders = make(chan *workOrder, runParams.numWorkers)
	workOrderResults = make(chan *workOrder, runParams.numWorkers)
	for i := 0; i < runParams.numWorkers; i++ {
		wg.Add(1)
		go worker(workOrders, workOrderResults, wg, &numGets)
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
		select {
		case <-statsTicker.C:
			accumulatedStats.aggregate(intervalStats)
			writeStats(statsWriter, runParams.jsonFormat, false /* final */, intervalStats, accumulatedStats)
			sendStatsdStats(&intervalStats)
			intervalStats = newStats(time.Now())
		default:
			break
		}

		select {
		case <-timer.C:
			break MainLoop
		case wo := <-workOrderResults:
			completeWorkOrder(wo, false)
			if runParams.statsShowInterval == 0 && runParams.putSizeUpperBound != 0 {
				accumulatedStats.aggregate(intervalStats)
				intervalStats = newStats(time.Now())
			}
			if err := postNewWorkOrder(); err != nil {
				_, _ = fmt.Fprint(os.Stderr, err.Error())
				break MainLoop
			}
		case <-statsTicker.C:
			accumulatedStats.aggregate(intervalStats)
			writeStats(statsWriter, runParams.jsonFormat, false /* final */, intervalStats, accumulatedStats)
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
	close(workOrders)
	wg.Wait() // wait until all workers complete their work

	// Process left over work orders
	close(workOrderResults)
	for wo := range workOrderResults {
		completeWorkOrder(wo, true)
	}

	fmt.Printf("\nActual run duration: %v\n", time.Since(tsStart))
	finalizeStats(statsWriter)
	if runParams.cleanUp.Val {
		cleanup()
	}

	return err
}

func sendStatsdStats(s *sts) {
	s.statsd.SendAll(statsdC)
}

func newPutWorkOrder() (*workOrder, error) {
	objName, err := generatePutObjectName()
	if err != nil {
		return nil, err
	}
	size := runParams.minSize
	if runParams.maxSize != runParams.minSize {
		size = rnd.Int63n(runParams.maxSize-runParams.minSize) + runParams.minSize
	}
	putPending++
	return &workOrder{
		proxyURL:  runParams.proxyURL,
		bck:       runParams.bck,
		op:        opPut,
		objName:   objName,
		size:      size,
		cksumType: runParams.cksumType,
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
		comps[idx] = cos.RandStringWithSrc(rnd, randomObjNameLen)
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
		bck:      runParams.bck,
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
	if runParams.getConfig {
		workOrders <- newGetConfigWorkOrder()
		return
	}

	var wo *workOrder
	if rnd.Intn(99) < runParams.putPct {
		if wo, err = newPutWorkOrder(); err != nil {
			return err
		}
	} else {
		if wo, err = newGetWorkOrder(); err != nil {
			return err
		}
	}
	workOrders <- wo
	return
}

func validateWorkOrder(wo *workOrder, delta time.Duration) error {
	if wo.op == opGet || wo.op == opPut {
		if delta == 0 {
			return fmt.Errorf("%s has the same start time as end time", wo)
		}
	}
	return nil
}

func completeWorkOrder(wo *workOrder, terminating bool) {
	delta := timeDelta(wo.end, wo.start)

	if wo.err == nil && traceHTTPSig.Load() {
		var lat *stats.MetricLatsAgg
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
		if wo.sgl == nil || terminating {
			return
		}

		now, l := time.Now(), len(wo2Free)
		debug.Assert(!wo.end.IsZero())
		// free previously executed PUT SGLs
		for i := 0; i < l; i++ {
			if terminating {
				return
			}
			w := wo2Free[i]
			// delaying freeing sgl for `wo2FreeDelay`
			// (background at https://github.com/golang/go/issues/30597)
			if now.Sub(w.end) < wo2FreeDelay {
				break
			}
			if w.sgl != nil && !w.sgl.IsNil() {
				w.sgl.Free()
				copy(wo2Free[i:], wo2Free[i+1:])
				i--
				l--
				wo2Free = wo2Free[:l]
			}
		}
		// append to free later
		wo2Free = append(wo2Free, wo)
	case opConfig:
		if wo.err == nil {
			intervalStats.getConfig.Add(1, delta)
			intervalStats.statsd.Config.Add(delta, wo.latencies.Proxy, wo.latencies.ProxyConn)
		} else {
			fmt.Println("GET config failed: ", wo.err)
			intervalStats.getConfig.AddErr()
		}
	default:
		debug.Assert(false) // Should never be here
	}
}

func cleanup() {
	stopping.Store(true)
	time.Sleep(time.Second)
	fmt.Println(prettyTimestamp() + " Cleaning up...")
	if bucketObjsNames != nil {
		// `bucketObjsNames` has been actually assigned to/initialized.
		var (
			w       = runParams.numWorkers
			objsLen = bucketObjsNames.Len()
			n       = objsLen / w
			wg      = &sync.WaitGroup{}
		)
		for i := 0; i < w; i++ {
			wg.Add(1)
			go cleanupObjs(bucketObjsNames.Names()[i*n:(i+1)*n], wg)
		}
		if objsLen%w != 0 {
			wg.Add(1)
			go cleanupObjs(bucketObjsNames.Names()[n*w:], wg)
		}
		wg.Wait()
	}

	if runParams.bck.IsAIS() {
		api.DestroyBucket(runParams.bp, runParams.bck)
	}
	fmt.Println(prettyTimestamp() + " Done")
}

func cleanupObjs(objs []string, wg *sync.WaitGroup) {
	defer wg.Done()

	t := len(objs)
	if t == 0 {
		return
	}

	// Only clean up the objects if it's not AIS bucket (the whole bucket is
	// removed if it's AIS)
	if !runParams.bck.IsAIS() {
		b := cos.Min(t, runParams.batchSize)
		n := t / b
		for i := 0; i < n; i++ {
			xactID, err := api.DeleteList(runParams.bp, runParams.bck, objs[i*b:(i+1)*b])
			if err != nil {
				fmt.Println("delete err ", err)
			}
			args := api.XactReqArgs{ID: xactID, Kind: apc.ActDeleteObjects}
			if _, err = api.WaitForXactionIC(runParams.bp, args); err != nil {
				fmt.Println("wait for xaction err ", err)
			}
		}

		if t%b != 0 {
			xactID, err := api.DeleteList(runParams.bp, runParams.bck, objs[n*b:])
			if err != nil {
				fmt.Println("delete err ", err)
			}
			args := api.XactReqArgs{ID: xactID, Kind: apc.ActDeleteObjects}
			if _, err = api.WaitForXactionIC(runParams.bp, args); err != nil {
				fmt.Println("wait for xaction err ", err)
			}
		}
	}

	if runParams.readerType == readers.ReaderTypeFile {
		for _, obj := range objs {
			if err := os.Remove(runParams.tmpDir + "/" + obj); err != nil {
				fmt.Println("delete local file err ", err)
			}
		}
	}
}

// bootstrap bootstraps existing objects in the bucket.
func bootstrap() error {
	names, err := listObjectNames(runParams.bp, runParams.bck, "")
	if err != nil {
		fmt.Printf("Failed to list_objects %s, proxy %s, err: %v\n",
			runParams.bck, runParams.proxyURL, err)
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

			// Number from benchmarks: aisloader/tests/objnamegetter_test.go
			// After 50k overhead on new goroutine and WaitGroup becomes smaller than benefits
			if len(names) > 50000 {
				bucketObjsNames = &namegetter.PermutationUniqueImprovedNameGetter{}
			}
		}
	}
	bucketObjsNames.Init(names, rnd)
	return err
}
