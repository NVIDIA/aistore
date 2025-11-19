// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tetl"

	jsoniter "github.com/json-iterator/go"
)

type (
	// Cluster connection and API configuration
	clusterParams struct {
		smap        *meta.Smap     // cluster map
		bp          api.BaseParams // base parameters for API calls
		proxyURL    string         // resolved proxy URL
		tokenFile   string         // authentication token file path
		randomProxy bool           // select random gateway for each request
	}

	// Workload timing and intensity
	// NOTE: defines name-getter type via (putPct, numEpochs, permShuffleMax) and number of objects in the bucket
	workloadParams struct {
		duration             DurationExt // benchmark duration
		putSizeUpperBoundStr string      // total PUT size limit (unparsed)
		tmpDir               string      // temp directory for file-based readers
		readerType           string      // reader type: sg(default), file, rand, tar (PUT only)
		putSizeUpperBound    int64       // total PUT size limit (bytes)
		seed                 int64       // random seed for reproducibility
		maxputs              uint64      // max number of PUT operations
		numWorkers           int         // number of concurrent workers
		putPct               int         // percentage of PUT operations (rest are GETs)
		updateExistingPct    int         // percentage of GETs followed by PUT updates
		numEpochs            uint        // number of full passes through bucket (enables permutation name-getter)
		permShuffleMax       uint        // threshold for switching to affine name-getter (memory vs compute tradeoff)
	}

	// Object size constraints and integrity
	sizeCksumParams struct {
		maxSizeStr string // max object size (unparsed)
		minSizeStr string // min object size (unparsed)
		cksumType  string // checksum algorithm
		maxSize    int64  // max object size (bytes)
		minSize    int64  // min object size (bytes)
		verifyHash bool   // recompute and validate checksums on GET
	}

	// archives aka shards
	// NOTE:
	// aisloader currently uses a subset of readers.Arch functionality.
	// Supported fields are listed below; the remaining ones are not yet wired in:
	//   - Names:     explicit file names (aisloader generates synthetic names)
	//   - RecExt:    in-archive filename (aka, recordi) extension (defaults to .txt)
	//   - TarFormat: tar format override (uses archive package defaults while readers.Arch supports GNU, USTAR, etc.)
	//   - RandNames: random numeric names (aisloader uses its own naming)
	//   - RandDir:   random directory layers
	//   - MaxLayers: max directory depth (not applicable)
	//   - Seed:      always auto-generated from mono.NanoTime()

	archParams struct {
		format   string // archive format (default: .tar)
		prefix   string // optional prefix inside archive (e.g., "trunk-", "a/b/c/trunk-")
		numFiles int    // number of archived files inside shard (PUT only)
		minSzStr string // min file size
		maxSzStr string // max --/--

		// resolved
		minSz int64
		maxSz int64

		pct int // broadly: percentage of archive workload (affects both reading and writing when positive)
	}

	// Object naming strategy and distribution
	namingParams struct {
		subDir        string // virtual directory prefix for objects
		fileList      string // file containing object names to use
		numVirtDirs   uint64 // spread generated objects across that many virtual subdirectories (< 100k or "%05x")
		randomObjName bool   // generate random 32-char object names
		skipList      bool   // skip listing bucket before 100% PUT workload
	}

	// Read operation configuration
	readParams struct {
		readLenStr     string // read range length (unparsed)
		readOffStr     string // read range offset (unparsed)
		readLen        int64  // read range length (bytes, 0 = full object)
		readOff        int64  // read range offset (bytes)
		evictBatchSize int    // batch size for list and evict operations
		getBatchSize   int    // use GetBatch API instead of GetObject (ML workloads)
		latest         bool   // check metadata and GET latest version from remote bucket
		cached         bool   // list only cached objects from remote bucket
		listDirs       bool   // list virtual subdirectories (remote buckets only)
		continueOnErr  bool   // [GetBatch] ignore missing files and/or objects - include them under "__404__/" prefix and keep going
	}

	// Multipart upload settings
	multipartParams struct {
		multipartChunks int // number of chunks per multipart upload (0 = disabled)
		multipartPct    int // percentage of PUTs using multipart (0-100)
	}

	// ETL (Extract, Transform, Load) configuration
	etlParams struct {
		etlName     string // predefined ETL to apply (e.g., 'tar2tf', 'md5', 'echo')
		etlSpecPath string // custom ETL spec file path
	}

	// Bucket properties configuration
	bucketParams struct {
		bck       cmn.Bck    // target bucket
		bPropsStr string     // bucket properties JSON string
		bProps    cmn.Bprops // parsed bucket properties
	}

	// Loader instance identification for fleet coordination
	loaderParams struct {
		loaderID        string // unique identifier for this loader instance
		loaderCnt       uint64 // total number of concurrent loader instances
		loaderIDHashLen uint   // bit length of generated loader ID (0-63)
		getLoaderID     bool   // print loader ID and exit
	}

	// Statistics and monitoring configuration
	statsParams struct {
		statsOutput       string // output file for statistics (empty = stdout)
		statsdIP          string // StatsD server IP or hostname (deprecated)
		statsShowInterval int    // stats print interval in seconds (0 = disabled)
		statsdPort        int    // StatsD UDP port (deprecated)
		statsdProbe       bool   // probe StatsD server before starting (deprecated)
		jsonFormat        bool   // output stats in JSON format
	}

	// Miscellaneous (cleanup, dry-run, et al.)
	miscParams struct {
		cleanUp   BoolExt // destroy/empty bucket on termination
		stoppable bool    // allow termination via Ctrl-C
		dryRun    bool    // print config and exit without running
		traceHTTP bool    // trace HTTP request latencies
	}

	// Main configuration: all parameter sections
	params struct {
		clusterParams   // cluster connection
		bucketParams    // bucket + properties
		workloadParams  // timing, workers, name-getter config
		sizeCksumParams // object size + integrity
		archParams      // one of the AIS-supported formats and related defining parameters
		namingParams    // object naming strategy
		readParams      // read operations
		multipartParams // multipart upload
		etlParams       // ETL config
		loaderParams    // fleet coordination
		statsParams     // monitoring
		miscParams      // other
	}
)

func addCmdLine(f *flag.FlagSet, p *params) {
	// Meta flags (not in params struct)
	f.BoolVar(&flagUsage, "usage", false, "show command-line options, usage, and examples")
	f.BoolVar(&flagVersion, "version", false, "show aisloader version")
	f.BoolVar(&flagQuiet, "quiet", false, "when starting to run, do not print command line arguments, default settings, and usage examples")
	f.DurationVar(&cargs.Timeout, "timeout", 10*time.Minute, "client HTTP timeout - used in LIST/GET/PUT/DELETE")

	// ============ Cluster ============
	f.StringVar(&ip, "ip", defaultClusterIP, "AIS proxy/gateway IP address or hostname")
	f.StringVar(&port, "port", "8080", "AIS proxy/gateway port")
	f.StringVar(&p.tokenFile, "tokenfile", "", "authentication token (FQN)")
	f.BoolVar(&p.randomProxy, "randomproxy", false, "when true, select random gateway (\"proxy\") to execute I/O request")

	// S3 direct (NOTE: with no AIStore in-between)
	f.StringVar(&s3Endpoint, "s3endpoint", "", "S3 endpoint to read/write s3 bucket directly (with no AIStore)")
	f.StringVar(&s3Profile, "s3profile", "", "other then default S3 config profile referencing alternative credentials")
	f.BoolVar(&s3UsePathStyle, "s3-use-path-style", false, "use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY. Should only be used with 's3endpoint' option")

	// ============ Bucket properties ============
	f.StringVar(&p.bck.Name, "bucket", "", "bucket name or bucket URI. If empty, a bucket with random name will be created")
	f.StringVar(&p.bck.Provider, "provider", apc.AIS,
		"ais - for AIS bucket, \"aws\", \"azure\", \"gcp\", \"oci\" for Azure, Amazon, Google, and Oracle clouds, respectively")

	f.StringVar(&p.bPropsStr, "bprops", "", "JSON string formatted as per the SetBucketProps API and containing bucket properties to apply")

	// ============ Workload ============
	DurationExtVar(f, &p.duration, "duration", time.Minute,
		"Benchmark duration (0 - run forever or until Ctrl-C). \n"+
			"If not specified and totalputsize > 0, aisloader runs until totalputsize reached. Otherwise aisloader runs until first of duration and "+
			"totalputsize reached")
	f.IntVar(&p.numWorkers, "numworkers", 10, "number of goroutine workers operating on AIS in parallel")
	f.UintVar(&p.numEpochs, "epochs", 0, "number of \"epochs\" to run whereby each epoch entails full pass through the entire listed bucket")
	f.UintVar(&p.permShuffleMax, "perm-shuffle-max", 100_000, "max names for shuffle-based name-getter (above this uses O(1) memory affine)")
	f.IntVar(&p.putPct, "pctput", 0, "percentage of PUTs in the aisloader-generated workload")
	f.IntVar(&p.updateExistingPct, "pctupdate", 0,
		"percentage of GET requests that are followed by a PUT \"update\" (i.e., creation of a new version of the object)")
	f.Uint64Var(&p.maxputs, "maxputs", 0, "maximum number of objects to PUT")
	f.StringVar(&p.putSizeUpperBoundStr, "totalputsize", "0",
		"stop PUT workload once cumulative PUT size reaches or exceeds this value (can contain standard multiplicative suffix K, MB, GiB, etc.; 0 - unlimited")
	f.Int64Var(&p.seed, "seed", 0, "random seed to achieve deterministic reproducible results (0 - use current time in nanoseconds)")

	f.StringVar(&p.tmpDir, "tmpdir", "/tmp/ais", "local directory to store temporary files")
	f.StringVar(&p.readerType, "readertype", readers.SG,
		fmt.Sprintf("[advanced usage only] type of reader: %s(default) | %s | %s", readers.SG, readers.File, readers.Rand))

	// ============ Object sizing and checksumming ============
	f.StringVar(&p.minSizeStr, "minsize", "", "minimum object size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.maxSizeStr, "maxsize", "", "maximum object size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.cksumType, "cksum-type", cos.ChecksumOneXxh, "cksum type to use for put object requests")
	f.BoolVar(&p.verifyHash, "verifyhash", false,
		"when true, checksum-validate GET: recompute object checksums and validate it against the one received with the GET metadata")

	// ============ Archives (Shards) ============
	f.StringVar(&p.archParams.format, "arch.format", "", "archive format (one of the 4 supported by AIStore; default TAR)")
	f.StringVar(&p.archParams.prefix, "arch.prefix", "", "optional prefix inside archive (e.g., \"trunk\" or \"a/b/c/trunk-\")")
	f.IntVar(&p.archParams.numFiles, "arch.num-files", 0, "number of archived files per shard (PUT only; default gets computed from sizes)")
	f.StringVar(&p.archParams.minSzStr, "arch.minsize", "", "minimum file size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.archParams.maxSzStr, "arch.maxsize", "", "maximum file size (with or without multiplicative suffix K, MB, GiB, etc.)")
	f.IntVar(&p.archParams.pct, "arch.pct", 0, "when writing: percentage of shards vs plain objects; when reading: include archived files in the GET pool (ratio is ultimately determined by dataset)")

	// ============ Naming ============
	f.StringVar(&p.subDir, "subdir", "", "For GET requests, '-subdir' is a prefix that may or may not be an actual _virtual directory_;\n"+
		"For PUTs, '-subdir' is a virtual destination directory for all aisloader-generated objects;\n"+
		"See also:\n"+
		"\t- closely related CLI '--prefix' option: "+cmn.GitHubHome+"/blob/main/docs/cli/object.md\n"+
		"\t- virtual directories:                   "+cmn.GitHubHome+"/blob/main/docs/howto_virt_dirs.md")
	f.Uint64Var(&p.numVirtDirs, "num-subdirs", 0, "spread generated objects over this many virtual subdirectories (< 100k)")
	f.BoolVar(&p.randomObjName, "randomname", true,
		"when true, generate object names of 32 random characters. This option is ignored when loadernum is defined")
	f.StringVar(&p.fileList, "filelist", "", "local or locally-accessible text file containing object names (for subsequent reading)")
	f.BoolVar(&p.skipList, "skiplist", false, "when true, skip listing objects in a bucket before running 100% PUT workload")

	// ============ Read operations ============
	f.StringVar(&p.readOffStr, "readoff", "", "read range offset (can contain multiplicative suffix K, MB, GiB, etc.)")
	f.StringVar(&p.readLenStr, "readlen", "", "read range length (can contain multiplicative suffix; 0 - GET full object)")
	f.IntVar(&p.getBatchSize, "get-batchsize", 0, "GetBatch (ML endpoint)")
	f.BoolVar(&p.latest, "latest", false, "when true, check in-cluster metadata and possibly GET the latest object version from the associated remote bucket")
	f.BoolVar(&p.cached, "cached", false, "list in-cluster objects - only those objects from a remote bucket that are present (\"cached\")")
	f.BoolVar(&p.listDirs, "list-dirs", false, "list virtual subdirectories (remote buckets only)")
	f.IntVar(&p.evictBatchSize, "evict-batchsize", 1000, "batch size to list and evict the _next_ batch of remote objects")
	f.BoolVar(&p.continueOnErr, "cont-on-err", false, "GetBatch: ignore missing files and/or objects - include them under \"__404__/\" prefix and keep going")

	// ============ Multipart ============
	f.IntVar(&p.multipartChunks, "multipart-chunks", 0, "number of chunks for multipart upload (0 - disabled, >0 - use multipart upload with specified number of chunks)")
	f.IntVar(&p.multipartPct, "pctmultipart", 0, "percentage of PUT operations that use multipart upload (0-100, only applies when multipart-chunks > 0)")

	// ============ ETL ============
	f.StringVar(&p.etlName, "etl", "", "name of an ETL applied to each object on GET request. One of '', 'tar2tf', 'md5', 'echo'")
	f.StringVar(&p.etlSpecPath, "etl-spec", "", "path to an ETL spec to be applied to each object on GET request.")

	// ============ Loader instance ============
	f.StringVar(&p.loaderID, "loaderid", "0", "ID to identify a loader among multiple concurrent instances")
	f.Uint64Var(&p.loaderCnt, "loadernum", 0,
		"total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen")
	f.UintVar(&p.loaderIDHashLen, "loaderidhashlen", 0,
		"Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum")
	f.BoolVar(&p.getLoaderID, "getloaderid", false,
		"when true, print stored/computed unique loaderID aka aisloader identifier and exit")

	// ============ Stats ============
	f.IntVar(&p.statsShowInterval, "statsinterval", 10, "interval in seconds to print performance counters; 0 - disabled")
	f.StringVar(&p.statsOutput, "stats-output", "", "filename to log statistics (empty string translates as standard output (default))")
	f.StringVar(&p.statsdIP, "statsdip", "localhost", "StatsD IP address or hostname (NOTE: deprecated)")
	f.IntVar(&p.statsdPort, "statsdport", 8125, "StatsD UDP port (NOTE: deprecated)")
	f.BoolVar(&p.statsdProbe, "test-probe StatsD server prior to benchmarks", false, "when enabled probes StatsD server prior to running (NOTE: deprecated)")
	f.BoolVar(&p.jsonFormat, "json", false, "when true, print output in JSON format")

	// ============ Miscellaneous ============
	BoolExtVar(f, &p.cleanUp, "cleanup", "when true, remove bucket upon benchmark termination (must be specified for AIStore buckets)")
	f.BoolVar(&p.stoppable, "stoppable", false, "when true, stop upon CTRL-C")
	f.BoolVar(&p.dryRun, "dry-run", false, "when true, show the configuration and parameters that aisloader will use for benchmark")
	f.BoolVar(&p.traceHTTP, "trace-http", false, "when true, trace HTTP latencies")

	//
	// Parse and validate
	//
	orig := f.Usage
	f.Usage = func() {
		fmt.Println("Run `aisloader` (for inline help), `aisloader version` (for version), or see 'docs/aisloader.md' for details and usage examples.")
	}
	f.Parse(os.Args[1:])
	f.Usage = orig

	if len(os.Args[1:]) == 0 {
		printUsage(f)
		os.Exit(0)
	}

	os.Args = []string{os.Args[0]}
	flag.Parse() // Called so that imported packages don't complain

	if flagUsage || (f.NArg() != 0 && (f.Arg(0) == "usage" || f.Arg(0) == "help")) {
		printUsage(f)
		os.Exit(0)
	}
	if flagVersion || (f.NArg() != 0 && f.Arg(0) == "version") {
		fmt.Printf("version %s (build %s)\n", _version, _buildtime)
		os.Exit(0)
	}
}

// parse/validate command line and finish initialization
func initParams(p *params) (err error) {
	// '--s3endpoint' takes precedence
	if s3Endpoint == "" {
		if ep := os.Getenv(env.AWSEndpoint); ep != "" {
			s3Endpoint = ep
		}
	}
	if p.bck.Name != "" {
		if p.cleanUp.Val && isDirectS3() {
			return errors.New("direct S3 access via '-s3endpoint': option '-cleanup' is not supported yet")
		}
		if !p.cleanUp.IsSet && !isDirectS3() {
			fmt.Println("\nNote: `-cleanup` is a required option. Beware! When -cleanup=true the bucket will be destroyed upon completion of the benchmark.")
			fmt.Println("      The option must be specified in the command line, e.g.: `--cleanup=false`")
			os.Exit(1)
		}
	}

	if p.seed == 0 {
		p.seed = mono.NanoTime()
	}
	rnd = rand.New(cos.NewRandSource(uint64(p.seed)))

	//
	// parse sizes
	//
	if p.putSizeUpperBound, err = _parseSize(p.putSizeUpperBoundStr, "totalputsize", 0); err != nil {
		return err
	}

	if p.minSize, err = _parseSize(p.minSizeStr, "minsize", cos.MiB); err != nil {
		return err
	}
	if p.maxSize, err = _parseSize(p.maxSizeStr, "maxsize", cos.GiB); err != nil {
		return err
	}
	if p.minSizeStr == "" && p.maxSizeStr != "" {
		p.minSize = min(p.minSize, p.maxSize)
	}
	if p.maxSizeStr == "" && p.minSizeStr != "" {
		p.maxSize = max(p.minSize, p.maxSize)
	}

	if p.readOff, err = _parseSize(p.readOffStr, "readoff", 0); err != nil {
		return err
	}
	if p.readLen, err = _parseSize(p.readLenStr, "readlen", 0); err != nil {
		return err
	}
	if p.minSz, err = _parseSize(p.minSzStr, "arch.minsize", 0); err != nil {
		return err
	}
	if p.maxSz, err = _parseSize(p.maxSzStr, "arch.maxsize", 0); err != nil {
		return err
	}

	if !p.duration.IsSet {
		if p.putSizeUpperBound != 0 || p.numEpochs != 0 {
			// user specified putSizeUpperBound or numEpochs, but not duration, override default 1 minute
			// and run aisloader until other threshold is reached
			p.duration.Val = time.Duration(math.MaxInt64)
		} else {
			fmt.Printf("\nDuration not specified - running for %v\n\n", p.duration.Val)
		}
	}

	// validate
	if err := p.validate(); err != nil {
		return err
	}

	if p.bPropsStr != "" {
		var bprops cmn.Bprops
		jsonStr := strings.TrimRight(p.bPropsStr, ",")
		if !strings.HasPrefix(jsonStr, "{") {
			jsonStr = "{" + strings.TrimRight(jsonStr, ",") + "}"
		}

		if err := jsoniter.Unmarshal([]byte(jsonStr), &bprops); err != nil {
			return fmt.Errorf("failed to parse bucket properties: %v", err)
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
				return fmt.Errorf(
					"invalid number of parity slices: %d, it must be between 1 and 32",
					p.bProps.EC.ParitySlices)
			}
			if p.bProps.EC.DataSlices < 1 || p.bProps.EC.DataSlices > 32 {
				return fmt.Errorf(
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
				return fmt.Errorf(
					"invalid number of mirror copies: %d, it must equal 2",
					p.bProps.Mirror.Copies)
			}
		}
	}

	var useHTTPS bool
	if !isDirectS3() {
		// AIS endpoint: http://ip:port _or_ AIS_ENDPOINT env
		aisEndpoint := "http://" + ip + ":" + port

		// see also: tlsArgs
		envEndpoint = os.Getenv(env.AisEndpoint)
		if envEndpoint != "" {
			if ip != "" && ip != defaultClusterIP && ip != defaultClusterIPv4 {
				return fmt.Errorf("'%s=%s' environment and '--ip=%s' command-line are mutually exclusive",
					env.AisEndpoint, envEndpoint, ip)
			}
			aisEndpoint = envEndpoint
		}

		traceHTTPSig.Store(p.traceHTTP)

		scheme, address := cmn.ParseURLScheme(aisEndpoint)
		if scheme == "" {
			scheme = "http"
		}
		if scheme != "http" && scheme != "https" {
			return fmt.Errorf("invalid AIStore endpoint %q: unknown URI scheme %q", aisEndpoint, scheme)
		}

		// TODO: validate against cluster map (see api.GetClusterMap below)
		p.proxyURL = scheme + "://" + address
		useHTTPS = scheme == "https"
	}

	p.bp = api.BaseParams{URL: p.proxyURL}
	if useHTTPS {
		// environment to override client config
		cmn.EnvToTLS(&sargs)
		p.bp.Client = cmn.NewClientTLS(cargs, sargs, false /*intra-cluster*/)
	} else {
		p.bp.Client = cmn.NewClient(cargs)
	}

	// NOTE: auth token is assigned below when we execute the very first API call
	return nil
}

func _parseSize(valueStr, name string, defaultVal int64) (int64, error) {
	if valueStr == "" {
		return defaultVal, nil
	}
	v, err := cos.ParseSize(valueStr, cos.UnitsIEC)
	if err != nil {
		return 0, fmt.Errorf("failed to parse '--%s=%s': %v", name, valueStr, err)
	}
	return v, nil
}

func (p *params) validate() error {
	if p.maxSize < p.minSize {
		return fmt.Errorf("invalid option: min and max size (%d, %d), respectively", p.minSize, p.maxSize)
	}

	if p.putPct < 0 || p.putPct > 100 {
		return fmt.Errorf("invalid option: PUT percent %d", p.putPct)
	}
	if p.updateExistingPct < 0 || p.updateExistingPct > 100 {
		return fmt.Errorf("invalid %d percentage of GET requests that are followed by a PUT \"update\"", p.putPct)
	}

	if p.multipartChunks < 0 {
		return fmt.Errorf("invalid option: multipart-chunks %d (must be >= 0)", p.multipartChunks)
	}
	if p.multipartPct < 0 || p.multipartPct > 100 {
		return fmt.Errorf("invalid option: pctmultipart %d (must be 0-100)", p.multipartPct)
	}
	if p.multipartChunks == 0 && p.multipartPct != 0 {
		fmt.Fprintf(os.Stderr, "Warning: pctmultipart=%d is ignored when multipart-chunks=0\n", p.multipartPct)
	}

	if p.skipList && p.fileList != "" {
		fmt.Fprintln(os.Stderr, "Warning: '-skiplist' option is ignored when '-filelist' is specified")
		p.skipList = false
	}
	if p.skipList && p.putPct < 100 {
		return errors.New("invalid option: '-skiplist' is only valid for 100% PUT workloads")
	}

	// direct s3 access vs other command line
	if isDirectS3() {
		if p.randomProxy {
			return errors.New("command line options '-s3endpoint' and '-randomproxy' are mutually exclusive")
		}
		if ip != "" && ip != defaultClusterIP && ip != defaultClusterIPv4 {
			return errors.New("command line options '-s3endpoint' and '-ip' are mutually exclusive")
		}
		if port != "" && port != "8080" {
			return errors.New("command line options '-s3endpoint' and '-port' are mutually exclusive")
		}
		if p.traceHTTP {
			return errors.New("direct S3 access via '-s3endpoint': HTTP tracing is not supported yet")
		}
		if p.cleanUp.Val {
			return errors.New("direct S3 access via '-s3endpoint': '-cleanup' option is not supported yet")
		}
		if p.verifyHash {
			return errors.New("direct S3 access via '-s3endpoint': '-verifyhash' option is not supported yet")
		}
		if p.readOffStr != "" || p.readLenStr != "" {
			return errors.New("direct S3 access via '-s3endpoint': Read range is not supported yet")
		}
		if p.multipartChunks > 0 {
			return errors.New("direct S3 access via '-s3endpoint': multipart upload is not supported yet")
		}
	}

	if p.getBatchSize != 0 {
		const s = "'--get-batchsize'"
		if p.getBatchSize < 1 || p.getBatchSize > 1000 {
			return fmt.Errorf("invalid %s value %d (must be 1-1000)", s, p.getBatchSize)
		}
		if p.readOff != 0 || p.readLen != 0 {
			return fmt.Errorf("option %s cannot be used with readoff/readlen", s)
		}
		if p.verifyHash {
			return fmt.Errorf("option %s cannot be used with verifyhash", s)
		}
		if p.etlName != "" || p.etlSpecPath != "" {
			return fmt.Errorf("option %s cannot be used with ETL", s)
		}
		if isDirectS3() {
			return fmt.Errorf("option %s requires AIStore (S3 does not provide GetBatch)", s)
		}
	}

	if p.statsShowInterval < 0 {
		return fmt.Errorf("invalid option: stats show interval %d", p.statsShowInterval)
	}

	if p.loaderID == "" {
		return errors.New("loaderID can't be empty")
	}

	loaderID, parseErr := strconv.ParseUint(p.loaderID, 10, 64)
	if p.loaderCnt == 0 && p.loaderIDHashLen == 0 {
		if p.randomObjName {
			useRandomObjName = true
			if parseErr != nil {
				return errors.New("loaderID as string only allowed when using loaderIDHashLen")
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
			return errors.New("loadernum and loaderIDHashLen can't be > 0 at the same time")
		}

		if p.loaderIDHashLen > 0 {
			if p.loaderIDHashLen == 0 || p.loaderIDHashLen > 63 {
				return errors.New("loaderIDHashLen has to be larger than 0 and smaller than 64")
			}

			suffixIDMaskLen = ceilAlign(p.loaderIDHashLen, 4)
			suffixID = getIDFromString(p.loaderID, suffixIDMaskLen)
		} else {
			// p.loaderCnt > 0
			if parseErr != nil {
				return errors.New("loadername has to be a number when using loadernum")
			}
			if loaderID > p.loaderCnt {
				return errors.New("loaderid has to be smaller than loadernum")
			}

			suffixIDMaskLen = loaderMaskFromTotalLoaders(p.loaderCnt)
			suffixID = loaderID
		}
	}

	if p.subDir != "" {
		p.subDir = filepath.Clean(p.subDir)
		if p.subDir[0] == '/' {
			return errors.New("object name prefix can't start with /")
		}
	}

	if p.numVirtDirs >= 100_000 {
		return fmt.Errorf("'--num-subdirs' (%d) must be a non-negative number in the open interval [0, 100k)", p.numVirtDirs)
	}

	if err := cos.ValidateCksumType(p.cksumType); err != nil {
		return err
	}

	if p.etlName != "" && p.etlSpecPath != "" {
		return errors.New("etl and etl-spec flag can't be set both")
	}

	if p.etlSpecPath != "" {
		fh, err := os.Open(p.etlSpecPath)
		if err != nil {
			return err
		}
		etlSpec, err := cos.ReadAll(fh)
		fh.Close()
		if err != nil {
			return err
		}
		etlInitSpec, err = tetl.SpecToInitMsg(etlSpec)
		if err != nil {
			return err
		}
	}

	if p.etlName != "" {
		etlSpec, err := tetl.GetTransformYaml(p.etlName)
		if err != nil {
			return err
		}
		etlInitSpec, err = tetl.SpecToInitMsg(etlSpec)
		if err != nil {
			return err
		}
	}

	// validate archive
	if p.archParams.pct != 0 {
		if p.archParams.pct < 0 || p.archParams.pct > 100 {
			return fmt.Errorf("invalid option: arch.pct %d (must be 0-100)", p.archParams.pct)
		}
		if s3Endpoint != "" {
			return errors.New("archive operations require AIStore cluster; not supported with '-s3endpoint' (direct S3 access)")
		}

		// TODO:
		// - currently, we create each part independently with fresh readers
		// - multipart upload of shards requires a special streaming chunk writer
		if p.multipartPct > 0 {
			return errors.New("multipart uploads and archive operations are mutually exclusive (not supported yet)")
		}

		mime := cos.NonZero(p.archParams.format, archive.ExtTar)
		arch := &readers.Arch{
			Mime:    mime,
			Prefix:  p.archParams.prefix,
			MinSize: p.archParams.minSz,
			MaxSize: p.archParams.maxSz,
			Num:     p.archParams.numFiles,
		}
		if err := arch.Init(p.maxSize); err != nil {
			return err
		}
	}

	return nil
}
