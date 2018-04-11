// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends._test
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // profile
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

type Test struct {
	name   string
	method func(*testing.T)
}

const (
	RestAPIDaemonSuffix   = "/v1/daemon"
	TestLocalBucketName   = "TESTLOCALBUCKET"
	RenameLocalBucketName = "renamebucket"
	RenameDir             = "/tmp/dfc/rename"
	RenameStr             = "rename"
	ListRangeDir          = "/tmp/dfc/listrange"
	ListRangeStr          = "__listrange"
)

var (
	GetConfigMsg         = dfc.GetMsg{GetWhat: dfc.GetWhatConfig}
	GetStatsMsg          = dfc.GetMsg{GetWhat: dfc.GetWhatStats}
	CreateLocalBucketMsg = dfc.ActionMsg{Action: dfc.ActCreateLB}
	DeleteLocalBucketMsg = dfc.ActionMsg{Action: dfc.ActDestroyLB}
	GetSmapMsg           = dfc.GetMsg{GetWhat: dfc.GetWhatSmap}
	RenameMsg            = dfc.ActionMsg{Action: dfc.ActRename}
	HighWaterMark        = uint32(80)
	LowWaterMark         = uint32(60)
	UpdTime              = time.Second * 20
	configRegression     = map[string]string{
		"stats_time":         fmt.Sprintf("%v", UpdTime),
		"dont_evict_time":    fmt.Sprintf("%v", UpdTime),
		"capacity_upd_time":  fmt.Sprintf("%v", UpdTime),
		"startup_delay_time": fmt.Sprintf("%v", UpdTime),
		"lowwm":              fmt.Sprintf("%d", LowWaterMark),
		"highwm":             fmt.Sprintf("%d", HighWaterMark),
		"passthru":           "true",
		"lru_enabled":        "true",
	}
	httpclient = &http.Client{}
	tests      = []Test{
		Test{"Local Bucket", regressionLocalBuckets},
		Test{"Cloud Bucket", regressionCloudBuckets},
		Test{"Stats", regressionStats},
		Test{"Config", regressionConfig},
		Test{"Rebalance", regressionRebalance},
		Test{"LRU", regressionLRU},
		Test{"Rename", regressionRename},
		Test{"RW stress", regressionRWStress},
		Test{"PrefetchList", regressionPrefetchList},
		Test{"PrefetchRange", regressionPrefetchRange},
		Test{"DeleteList", regressionDeleteList},
		Test{"DeleteRange", regressionDeleteRange},
	}
	abortonerr      = false
	readerType      = readers.ReaderTypeSG
	failLRU         = ""
	prefetchPrefix  = "__bench/test-"
	prefetchRegex   = "^\\d22\\d?"
	prefetchRange   = "0:2000"
	PhysMemSizeWarn = uint64(7 * 1024) // MBs

	// Following flags are set by parse()
	usingSG   bool // True if using SGL as reader backing memory
	usingFile bool // True if using file as reader backing
)

func init() {
	flag.BoolVar(&abortonerr, "abortonerr", abortonerr, "abort on error")
	flag.StringVar(&prefetchPrefix, "prefetchprefix", prefetchPrefix, "Prefix for Prefix-Regex Prefetch")
	flag.StringVar(&prefetchRegex, "prefetchregex", prefetchRegex, "Regex for Prefix-Regex Prefetch")
	flag.StringVar(&prefetchRange, "prefetchrange", prefetchRange, "Range for Prefix-Regex Prefetch")
	flag.StringVar(&readerType, "readertype", readers.ReaderTypeSG,
		fmt.Sprintf("Type of reader. {%s(default) | %s | %s | %s", readers.ReaderTypeSG,
			readers.ReaderTypeFile, readers.ReaderTypeInMem, readers.ReaderTypeRand))
}

func Test_regression(t *testing.T) {
	parse()

	if err := client.Tcping(proxyurl); err != nil {
		tlogf("%s: %v\n", proxyurl, err)
		os.Exit(1)
	}

	tlogf("=== abortonerr = %v, proxyurl = %s\n\n", abortonerr, proxyurl)

	if err := dfc.CreateDir(LocalDestDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalDestDir, err)
	}
	if err := dfc.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}
	// get them early to LRU later
	errch := make(chan error, 100)
	getRandomFiles(0, 0, 20, clibucket, t, nil, errch)
	selectErr(errch, "get", t, false)
	if t.Failed() {
		failLRU = "LRU: need a cloud bucket with at least 20 objects"
	}
	// run tests
	for _, test := range tests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}
}

func regressionCloudBuckets(t *testing.T) {
	regressionBucket(httpclient, t, clibucket)
}

func regressionLocalBuckets(t *testing.T) {
	bucket := TestLocalBucketName
	createLocalBucket(httpclient, t, bucket)
	time.Sleep(time.Second * 2) // FIXME: must be deterministic
	regressionBucket(httpclient, t, bucket)
	destroyLocalBucket(httpclient, t, bucket)
}

func regressionBucket(httpclient *http.Client, t *testing.T, bucket string) {
	var (
		numPuts  = 10
		filesput = make(chan string, numPuts)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
		sgl      *dfc.SGLIO
		filesize = uint64(1024)
	)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}
	putRandomFiles(0, baseseed+2, filesize, numPuts, bucket, t, nil, errch, filesput, SmokeDir, SmokeStr, "", false, sgl)
	close(filesput)
	selectErr(errch, "put", t, false)
	getRandomFiles(0, 0, numPuts, bucket, t, nil, errch)
	selectErr(errch, "get", t, false)
	for fname := range filesput {
		if usingFile {
			err := os.Remove(SmokeDir + "/" + fname)
			if err != nil {
				t.Error(err)
			}
		}

		wg.Add(1)
		go client.Del(proxyurl, bucket, "smoke/"+fname, wg, errch, false)
	}
	wg.Wait()
	selectErr(errch, "delete", t, abortonerr)
	close(errch)
}

func regressionStats(t *testing.T) {
	smap := getClusterMap(httpclient, t)
	stats := getClusterStats(httpclient, t)

	for k, v := range stats.Target {
		tdstats := getDaemonStats(httpclient, t, smap.Smap[k].DirectURL)
		tdcapstats := tdstats["capacity"].(map[string]interface{})
		dcapstats := v.Capacity
		for fspath, fstats := range dcapstats {
			tfstats := tdcapstats[fspath].(map[string]interface{})
			used, err := tfstats["used"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Used")
			}
			avail, err := tfstats["avail"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Avail")
			}
			usedpct, err := tfstats["usedpct"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Usedpct")
			}
			if uint64(used) != fstats.Used || uint64(avail) != fstats.Avail || uint32(usedpct) != fstats.Usedpct {
				t.Errorf("Stats are different when queried from Target and Proxy: "+
					"Used: %v, %v | Available:  %v, %v | Percentage: %v, %v",
					tfstats["used"], fstats.Used, tfstats["avail"], fstats.Avail, tfstats["usedpct"], fstats.Usedpct)
			}
			if fstats.Usedpct > HighWaterMark {
				t.Error("Used Percentage above High Watermark")
			}
		}
	}
}

func regressionConfig(t *testing.T) {
	oconfig := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	olruconfig := oconfig["lru_config"].(map[string]interface{})
	orebconfig := oconfig["rebalance_conf"].(map[string]interface{})
	oproxyconfig := oconfig["proxy"].(map[string]interface{})

	for k, v := range configRegression {
		setConfig(k, v, proxyurl+"/v1/cluster", httpclient, t)
	}

	nconfig := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	nlruconfig := nconfig["lru_config"].(map[string]interface{})
	nrebconfig := nconfig["rebalance_conf"].(map[string]interface{})
	nproxyconfig := nconfig["proxy"].(map[string]interface{})

	if nconfig["stats_time"] != configRegression["stats_time"] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nconfig["stats_time"], configRegression["stats_time"])
	} else {
		o := oconfig["stats_time"].(string)
		setConfig("stats_time", o, proxyurl+"/v1/cluster", httpclient, t)
	}
	if nlruconfig["dont_evict_time"] != configRegression["dont_evict_time"] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig["dont_evict_time"], configRegression["dont_evict_time"])
	} else {
		o := olruconfig["dont_evict_time"].(string)
		setConfig("dont_evict_time", o, proxyurl+"/v1/cluster", httpclient, t)
	}
	if nlruconfig["capacity_upd_time"] != configRegression["capacity_upd_time"] {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig["capacity_upd_time"], configRegression["capacity_upd_time"])
	} else {
		o := olruconfig["capacity_upd_time"].(string)
		setConfig("capacity_upd_time", o, proxyurl+"/v1/cluster", httpclient, t)
	}
	if nrebconfig["startup_delay_time"] != configRegression["startup_delay_time"] {
		t.Errorf("StartupDelayTime was not set properly: %v, should be: %v",
			nrebconfig["startup_delay_time"], configRegression["startup_delay_time"])
	} else {
		o := orebconfig["startup_delay_time"].(string)
		setConfig("startup_delay_time", o, proxyurl+"/v1/cluster", httpclient, t)
	}
	if hw, err := strconv.Atoi(configRegression["highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig["highwm"] != float64(hw) {
		t.Errorf("HighWatermark was not set properly: %.0f, should be: %d",
			nlruconfig["highwm"], hw)
	} else {
		o := olruconfig["highwm"].(float64)
		setConfig("highwm", strconv.Itoa(int(o)), proxyurl+"/v1/cluster", httpclient, t)
	}
	if lw, err := strconv.Atoi(configRegression["lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig["lowwm"] != float64(lw) {
		t.Errorf("LowWatermark was not set properly: %.0f, should be: %d",
			nlruconfig["lowwm"], lw)
	} else {
		o := olruconfig["lowwm"].(float64)
		setConfig("lowwm", strconv.Itoa(int(o)), proxyurl+"/v1/cluster", httpclient, t)
	}
	if pt, err := strconv.ParseBool(configRegression["passthru"]); err != nil {
		t.Fatalf("Error parsing Passthru: %v", err)
	} else if nproxyconfig["passthru"] != pt {
		t.Errorf("Proxy Passthru was not set properly: %v, should be %v",
			nproxyconfig["passthru"], pt)
	} else {
		o := oproxyconfig["passthru"].(bool)
		setConfig("passthru", strconv.FormatBool(o), proxyurl+"/v1/cluster", httpclient, t)
	}
	if pt, err := strconv.ParseBool(configRegression["lru_enabled"]); err != nil {
		t.Fatalf("Error parsing LRUEnabled: %v", err)
	} else if nlruconfig["lru_enabled"] != pt {
		t.Errorf("LRUEnabled was not set properly: %v, should be %v",
			nlruconfig["lru_enabled"], pt)
	} else {
		o := olruconfig["lru_enabled"].(bool)
		setConfig("lru_enabled", strconv.FormatBool(o), proxyurl+"/v1/cluster", httpclient, t)
	}
}

func regressionLRU(t *testing.T) {
	if failLRU != "" {
		t.Errorf(failLRU)
		t.Fail()
		return
	}
	var (
		errch   = make(chan error, 100)
		usedpct = uint32(100)
	)
	//
	// remember targets' watermarks
	//
	smap := getClusterMap(httpclient, t)
	lwms := make(map[string]interface{})
	hwms := make(map[string]interface{})
	bytesEvictedOrig := make(map[string]int64)
	filesEvictedOrig := make(map[string]int64)
	for k, di := range smap.Smap {
		cfg := getConfig(di.DirectURL+RestAPIDaemonSuffix, httpclient, t)
		lrucfg := cfg["lru_config"].(map[string]interface{})
		lwms[k] = lrucfg["lowwm"]
		hwms[k] = lrucfg["highwm"]
	}
	// add a few more
	getRandomFiles(0, 0, 3, clibucket, t, nil, errch)
	selectErr(errch, "get", t, true)
	//
	// find out min usage %% across all targets
	//
	stats := getClusterStats(httpclient, t)
	for k, v := range stats.Target {
		bytesEvictedOrig[k], filesEvictedOrig[k] = v.Core.Bytesevicted, v.Core.Filesevicted
		for _, c := range v.Capacity {
			usedpct = min(usedpct, c.Usedpct)
		}
	}
	tlogf("LRU: current min space usage in the cluster: %d%%\n", usedpct)
	var (
		lowwm  = usedpct - 5
		highwm = usedpct - 1
	)
	if int(lowwm) < 10 {
		t.Errorf("The current space usage is too low (%d) for the LRU to be tested", lowwm)
		t.Fail()
		return
	}
	oconfig := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	if t.Failed() {
		return
	}
	//
	// all targets: set new watermarks; restore upon exit
	//
	olruconfig := oconfig["lru_config"].(map[string]interface{})
	defer func() {
		setConfig("dont_evict_time", olruconfig["dont_evict_time"].(string), proxyurl+"/v1/cluster", httpclient, t)
		setConfig("capacity_upd_time", olruconfig["capacity_upd_time"].(string), proxyurl+"/v1/cluster", httpclient, t)
		setConfig("highwm", fmt.Sprint(olruconfig["highwm"]), proxyurl+"/v1/cluster", httpclient, t)
		setConfig("lowwm", fmt.Sprint(olruconfig["lowwm"]), proxyurl+"/v1/cluster", httpclient, t)
		for k, di := range smap.Smap {
			setConfig("highwm", fmt.Sprint(hwms[k]), di.DirectURL+RestAPIDaemonSuffix, httpclient, t)
			setConfig("lowwm", fmt.Sprint(lwms[k]), di.DirectURL+RestAPIDaemonSuffix, httpclient, t)
		}
	}()
	//
	// cluster-wide reduce dont-evict-time
	//
	dontevicttimestr := "30s"
	capacityupdtimestr := "5s"
	sleeptime, err := time.ParseDuration(oconfig["stats_time"].(string)) // to make sure the stats get updated
	if err != nil {
		t.Fatalf("Failed to parse stats_time: %v", err)
	}
	setConfig("dont_evict_time", dontevicttimestr, proxyurl+"/v1/cluster", httpclient, t)
	setConfig("capacity_upd_time", capacityupdtimestr, proxyurl+"/v1/cluster", httpclient, t)
	if t.Failed() {
		return
	}
	setConfig("lowwm", fmt.Sprint(lowwm), proxyurl+"/v1/cluster", httpclient, t)
	if t.Failed() {
		return
	}
	setConfig("highwm", fmt.Sprint(highwm), proxyurl+"/v1/cluster", httpclient, t)
	if t.Failed() {
		return
	}
	waitProgressBar("LRU: ", sleeptime/2)
	getRandomFiles(0, 0, 1, clibucket, t, nil, errch)
	waitProgressBar("LRU: ", sleeptime/2)
	//
	// results
	//
	stats = getClusterStats(httpclient, t)
	testFsPaths := oconfig["test_fspaths"].(map[string]interface{})
	for k, v := range stats.Target {
		bytes := v.Core.Bytesevicted - bytesEvictedOrig[k]
		tlogf("Target %s: evicted %d files - %.2f MB (%dB) total\n",
			k, v.Core.Filesevicted-filesEvictedOrig[k], float64(bytes)/1000/1000, bytes)
		//
		// testingFSPpaths() - cannot reliably verify space utilization by tmpfs
		//
		if testFsPaths["count"].(float64) > 0 {
			continue
		}
		for mpath, c := range v.Capacity {
			if c.Usedpct < lowwm-1 || c.Usedpct > lowwm+1 {
				t.Errorf("Target %s failed to reach lwm %d%%: mpath %s, used space %d%%", k, lowwm, mpath, c.Usedpct)
			}
		}
	}
}

func regressionRebalance(t *testing.T) {
	var (
		sid      string
		numPuts  = 40
		filesput = make(chan string, numPuts)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
		sgl      *dfc.SGLIO
		filesize = uint64(1024 * 128)
	)
	filesSentOrig := make(map[string]int64)
	bytesSentOrig := make(map[string]int64)
	filesRecvOrig := make(map[string]int64)
	bytesRecvOrig := make(map[string]int64)
	stats := getClusterStats(httpclient, t)
	for k, v := range stats.Target {
		bytesSentOrig[k], filesSentOrig[k], bytesRecvOrig[k], filesRecvOrig[k] =
			v.Core.Numsentbytes, v.Core.Numsentfiles, v.Core.Numrecvbytes, v.Core.Numrecvfiles
	}

	//
	// step 1. config
	//
	oconfig := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	orebconfig := oconfig["rebalance_conf"].(map[string]interface{})
	defer func() {
		setConfig("startup_delay_time", orebconfig["startup_delay_time"].(string), proxyurl+"/v1/cluster", httpclient, t)
	}()
	//
	// cluster-wide reduce startup_delay_time
	//
	startupdelaytimestr := "20s"
	setConfig("startup_delay_time", startupdelaytimestr, proxyurl+"/v1/cluster", httpclient, t) // NOTE: 1 second
	if t.Failed() {
		return
	}
	waitProgressBar("Rebalance: ", time.Second*10)

	//
	// step 2. unregister random target
	//
	smap := getClusterMap(httpclient, t)
	l := len(smap.Smap)
	if l < 3 { // NOTE: proxy is counted; FIXME: will have to be fixed for "multi-proxies"...
		if l == 0 {
			t.Fatal("DFC cluster is empty - zero targets")
		} else {
			t.Fatalf("Must have 2 or more targets in the cluster, have only %d", l)
		}
	}
	for sid = range smap.Smap {
		break
	}
	unregisterTarget(sid, t)
	tlogf("Unregistered %s: cluster size = %d (targets)\n", sid, l-1)
	//
	// step 3. put random files => (cluster - 1)
	//
	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}
	putRandomFiles(0, baseseed, filesize, numPuts, clibucket, t, nil, errch, filesput, SmokeDir, SmokeStr, "", false, sgl)
	selectErr(errch, "put", t, false)

	//
	// step 4. register back
	//
	registerTarget(sid, &smap, t)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		smap = getClusterMap(httpclient, t)
		if len(smap.Smap) == l {
			break
		}
	}
	if len(smap.Smap) != l {
		t.Errorf("Re-registration timed out: target %s, original num targets %d\n", sid, l)
		return
	}
	tlogf("Re-registered %s: the cluster is now back to %d targets\n", sid, l)
	//
	// step 5. wait for rebalance to run its course
	//
	waitProgressBar("Rebalance: ", time.Second*10)
	//
	// step 6. statistics
	//
	stats = getClusterStats(httpclient, t)
	var bsent, fsent, brecv, frecv int64
	for k, v := range stats.Target {
		bsent += v.Core.Numsentbytes - bytesSentOrig[k]
		fsent += v.Core.Numsentfiles - filesSentOrig[k]
		brecv += v.Core.Numrecvbytes - bytesRecvOrig[k]
		frecv += v.Core.Numrecvfiles - filesRecvOrig[k]
	}

	//
	// step 7. cleanup
	//
	close(filesput) // to exit for-range
	for fname := range filesput {
		if usingFile {
			err := os.Remove(SmokeDir + "/" + fname)
			if err != nil {
				t.Error(err)
			}
		}

		wg.Add(1)
		go client.Del(proxyurl, clibucket, "smoke/"+fname, wg, errch, false)
	}
	wg.Wait()
	selectErr(errch, "delete", t, abortonerr)
	close(errch)
	if !t.Failed() && testing.Verbose() {
		fmt.Printf("Rebalance: sent     %.2f MB in %d files\n", float64(bsent)/1000/1000, fsent)
		fmt.Printf("           received %.2f MB in %d files\n", float64(brecv)/1000/1000, frecv)
	}
}

func regressionRename(t *testing.T) {
	var (
		req       *http.Request
		r         *http.Response
		injson    []byte
		err       error
		numPuts   = 10
		filesput  = make(chan string, numPuts)
		errch     = make(chan error, numPuts)
		basenames = make([]string, 0, numPuts) // basenames
		bnewnames = make([]string, 0, numPuts) // new basenames
		sgl       *dfc.SGLIO
	)
	// create & put
	createLocalBucket(httpclient, t, RenameLocalBucketName)
	defer func() {
		// cleanup
		wg := &sync.WaitGroup{}
		for _, fname := range bnewnames {
			wg.Add(1)
			go client.Del(proxyurl, RenameLocalBucketName, RenameStr+"/"+fname, wg, errch, false)
		}

		if usingFile {
			for _, fname := range basenames {
				err = os.Remove(RenameDir + "/" + fname)
				if err != nil {
					t.Errorf("Failed to remove file %s: %v", fname, err)
				}
			}
		}

		wg.Wait()
		selectErr(errch, "delete", t, false)
		close(errch)
		destroyLocalBucket(httpclient, t, RenameLocalBucketName)
	}()

	time.Sleep(time.Second * 5)

	if err = dfc.CreateDir(RenameDir); err != nil {
		t.Errorf("Error creating dir: %v", err)
	}

	if usingSG {
		sgl = dfc.NewSGLIO(1024 * 1024)
		defer sgl.Free()
	}

	putRandomFiles(0, baseseed+1, 0, numPuts, RenameLocalBucketName, t, nil, nil, filesput, RenameDir, RenameStr, "", false, sgl)
	selectErr(errch, "put", t, false)
	close(filesput)
	for fname := range filesput {
		basenames = append(basenames, fname)
	}

	// rename
	for _, fname := range basenames {
		RenameMsg.Name = RenameStr + "/" + fname + ".renamed" // objname
		bnewnames = append(bnewnames, fname+".renamed")       // base name
		injson, err = json.Marshal(RenameMsg)
		if err != nil {
			t.Fatalf("Failed to marshal RenameMsg: %v", err)
		}
		req, err = http.NewRequest("POST", proxyurl+"/v1/files/"+RenameLocalBucketName+"/"+RenameStr+"/"+fname, bytes.NewBuffer(injson))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		r, err = httpclient.Do(req)
		if r != nil {
			r.Body.Close()
		}
		s := fmt.Sprintf("Rename %s/%s => %s", RenameStr, fname, RenameMsg.Name)
		if testfail(err, s, r, nil, t) {
			destroyLocalBucket(httpclient, t, RenameLocalBucketName)
			return
		}
		tlogln(s)
	}

	// get renamed objects
	waitProgressBar("Rename/move: ", time.Second*5)
	for _, fname := range bnewnames {
		client.Get(proxyurl, RenameLocalBucketName, RenameStr+"/"+fname, nil, errch, false, false)
	}
	selectErr(errch, "get", t, false)
}

func regressionPrefetchList(t *testing.T) {
	var (
		toprefetch    = make(chan string, numfiles)
		netprefetches = int64(0)
	)

	// Skip the test when given a local bucket
	props, err := client.HeadBucket(proxyurl, clibucket)
	if err != nil {
		t.Errorf("Could not execute HeadBucket Request: %v", err)
		return
	}
	if props.CloudProvider == dfc.ProviderDfc {
		t.Skipf("Cannot prefetch from local bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := getClusterMap(httpclient, t)
	for _, v := range smap.Smap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
		}
		netprefetches -= npf
	}

	// 2. Get keys to prefetch
	n := int64(getMatchingKeys(match, clibucket, []chan string{toprefetch}, nil, t))
	close(toprefetch) // to exit for-range
	files := make([]string, 0)
	for i := range toprefetch {
		files = append(files, i)
	}

	// 3. Evict those objects from the cache and prefetch them
	tlogf("Evicting and Prefetching %d objects\n", len(files))
	err = client.EvictList(proxyurl, clibucket, files, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = client.PrefetchList(proxyurl, clibucket, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred.
	for _, v := range smap.Smap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
		}
		netprefetches += npf
	}
	if netprefetches != n {
		t.Errorf("Did not prefetch all files: Missing %d of %d\n", (n - netprefetches), n)
	}
}

func regressionPrefetchRange(t *testing.T) {
	var (
		netprefetches = int64(0)
		err           error
		rmin, rmax    int64
		re            *regexp.Regexp
	)

	// Skip the test when given a local bucket
	props, err := client.HeadBucket(proxyurl, clibucket)
	if err != nil {
		t.Errorf("Could not execute HeadBucket Request: %v", err)
		return
	}
	if props.CloudProvider == dfc.ProviderDfc {
		t.Skipf("Cannot prefetch from local bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := getClusterMap(httpclient, t)
	for _, v := range smap.Smap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
		}
		netprefetches -= npf
	}

	// 2. Parse arguments
	if prefetchRange != "" {
		ranges := strings.Split(prefetchRange, ":")
		if rmin, err = strconv.ParseInt(ranges[0], 10, 64); err != nil {
			t.Errorf("Error parsing range min: %v", err)
		}
		if rmax, err = strconv.ParseInt(ranges[1], 10, 64); err != nil {
			t.Errorf("Error parsing range max: %v", err)
		}
	}

	// 3. Discover the number of items we expect to be prefetched
	if re, err = regexp.Compile(prefetchRegex); err != nil {
		t.Errorf("Error compiling regex: %v", err)
	}
	msg := dfc.GetMsg{}
	objsToFilter := testListBucketAll(t, clibucket, msg)
	files := make([]string, 0)
	if objsToFilter != nil {
		for _, be := range objsToFilter.Entries {
			if oname := strings.TrimPrefix(be.Name, prefetchPrefix); oname != be.Name {
				s := re.FindStringSubmatch(oname)
				if s == nil {
					continue
				}
				if i, err := strconv.ParseInt(s[0], 10, 64); err != nil && s[0] != "" {
					continue
				} else if s[0] == "" || (rmin == 0 && rmax == 0) || (i >= rmin && i <= rmax) {
					files = append(files, be.Name)
				}
			}
		}
	}

	// 4. Evict those objects from the cache, and then prefetch them.
	tlogf("Evicting and Prefetching %d objects\n", len(files))
	err = client.EvictRange(proxyurl, clibucket, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = client.PrefetchRange(proxyurl, clibucket, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred.
	for _, v := range smap.Smap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
		}
		netprefetches += npf
	}
	if netprefetches != int64(len(files)) {
		t.Errorf("Did not prefetch all files: Missing %d of %d\n",
			(int64(len(files)) - netprefetches), len(files))
	}
}

func regressionDeleteRange(t *testing.T) {
	var (
		err            error
		prefix         = ListRangeStr + "/tstf-"
		quarter        = numfiles / 4
		third          = numfiles / 3
		smallrangesize = third - quarter + 1
		smallrange     = fmt.Sprintf("%d:%d", quarter, third)
		bigrange       = fmt.Sprintf("0:%d", numfiles)
		regex          = "\\d?\\d"
		wg             = &sync.WaitGroup{}
		errch          = make(chan error, numfiles)
	)
	// 1. Put files to delete:
	for i := 0; i < numfiles; i++ {
		r, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go client.PutAsync(wg, proxyurl, r, clibucket, fmt.Sprintf("%s%d", prefix, i), errch, false /* silent */)
	}
	wg.Wait()
	selectErr(errch, "put", t, true)

	// 2. Delete the small range of objects:
	err = client.DeleteRange(proxyurl, clibucket, prefix, regex, smallrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that the correct files have been deleted
	msg := &dfc.GetMsg{GetPrefix: prefix}
	bktlst, err := client.ListBucket(proxyurl, clibucket, msg)
	if len(bktlst.Entries) != numfiles-smallrangesize {
		t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), numfiles-smallrangesize)
	}
	filemap := make(map[string]*dfc.BucketEntry)
	for _, entry := range bktlst.Entries {
		filemap[entry.Name] = entry
	}
	for i := 0; i < numfiles; i++ {
		keyname := fmt.Sprintf("%s%d", prefix, i)
		_, ok := filemap[keyname]
		if ok && i >= quarter && i <= third {
			t.Errorf("File exists that should have been deleted: %s", keyname)
		} else if !ok && (i < quarter || i > third) {
			t.Errorf("File does not exist that should not have been deleted: %s", keyname)
		}
	}

	// 4. Delete the big range of objects:
	err = client.DeleteRange(proxyurl, clibucket, prefix, regex, bigrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Check to see that all the files have been deleted
	bktlst, err = client.ListBucket(proxyurl, clibucket, msg)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}
}

func regressionDeleteList(t *testing.T) {
	var (
		err    error
		prefix = ListRangeStr + "/tstf-"
		wg     = &sync.WaitGroup{}
		errch  = make(chan error, numfiles)
		files  = make([]string, 0)
	)
	// 1. Put files to delete:
	for i := 0; i < numfiles; i++ {
		r, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			t.Fatal(err)
		}

		keyname := fmt.Sprintf("%s%d", prefix, i)

		wg.Add(1)
		go client.PutAsync(wg, proxyurl, r, clibucket, keyname, errch, false /* silent */)
		files = append(files, keyname)

	}
	wg.Wait()
	selectErr(errch, "put", t, true)

	// 2. Delete the objects
	err = client.DeleteList(proxyurl, clibucket, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that all the files have been deleted.
	msg := &dfc.GetMsg{GetPrefix: prefix}
	bktlst, err := client.ListBucket(proxyurl, clibucket, msg)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}
}

//========
//
// Helpers
//
//========

func waitProgressBar(prefix string, wait time.Duration) {
	ticker := time.NewTicker(time.Second * 5)
	tlogf(prefix)
	idx := 1
waitloop:
	for range ticker.C {
		elapsed := time.Second * 2 * time.Duration(idx)
		if elapsed >= wait {
			tlogln("")
			break waitloop
		}
		tlogf("----%d%%", (elapsed * 100 / wait))
		idx++
	}
}

func unregisterTarget(sid string, t *testing.T) {
	var (
		req *http.Request
		r   *http.Response
		err error
	)
	req, err = http.NewRequest("DELETE", proxyurl+"/v1/cluster"+"/"+"daemon"+"/"+sid, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	if r != nil {
		r.Body.Close()
	}
	if testfail(err, fmt.Sprintf("Unregister target %s", sid), r, nil, t) {
		return
	}
	time.Sleep(time.Second * 3)
}

func registerTarget(sid string, smap *dfc.Smap, t *testing.T) {
	var (
		req *http.Request
		r   *http.Response
		err error
	)
	si := smap.Smap[sid]
	req, err = http.NewRequest("POST", si.DirectURL+"/v1/daemon", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	if r != nil {
		r.Body.Close()
	}
	if t.Failed() || testfail(err, fmt.Sprintf("Register target %s", sid), r, nil, t) {
		return
	}
}

func createLocalBucket(httpclient *http.Client, t *testing.T, bucket string) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(CreateLocalBucketMsg)
	if err != nil {
		t.Fatalf("Failed to marshal CreateLocalBucketMsg: %v", err)
	}
	req, err = http.NewRequest("POST", proxyurl+"/v1/files/"+bucket, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Create Local Bucket", r, nil, t) {
		return
	}
}

func destroyLocalBucket(httpclient *http.Client, t *testing.T, bucket string) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(DeleteLocalBucketMsg)
	if err != nil {
		t.Fatalf("Failed to marshal CreateLocalBucketMsg: %v", err)
	}
	req, err = http.NewRequest("DELETE", proxyurl+"/v1/files/"+bucket, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Delete Local Bucket", r, nil, t) {
		return
	}
}

func getClusterStats(httpclient *http.Client, t *testing.T) (stats dfc.ClusterStats) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(GetStatsMsg)
	if err != nil {
		t.Fatalf("Failed to marshal GetStatsMsg: %v", err)
	}
	req, err = http.NewRequest("GET", proxyurl+"/v1/cluster", bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Get configuration", r, nil, t) {
		return
	}
	var b []byte
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Failed to read response body")
	}
	err = json.Unmarshal(b, &stats)
	if err != nil {
		t.Fatalf("Failed to unmarshal Dfconfig: %v", err)
	}
	return
}

func getDaemonStats(httpclient *http.Client, t *testing.T, URL string) (stats map[string]interface{}) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(GetStatsMsg)
	if err != nil {
		t.Fatalf("Failed to marshal GetStatsMsg: %v", err)
	}
	req, err = http.NewRequest("GET", URL+RestAPIDaemonSuffix, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Get configuration", r, nil, t) {
		return
	}
	var b []byte
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Failed to read response body")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	// If this isn't used, json.Unmarshal converts uint32s to floats, losing precision
	err = dec.Decode(&stats)
	if err != nil {
		t.Fatalf("Failed to unmarshal Dfconfig: %v", err)
	}
	return
}

func getClusterMap(httpclient *http.Client, t *testing.T) (smap dfc.Smap) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(GetSmapMsg)
	if err != nil {
		t.Fatalf("Failed to marshal GetStatsMsg: %v", err)
	}
	req, err = http.NewRequest("GET", proxyurl+"/v1/cluster", bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Get configuration", r, nil, t) {
		return
	}
	var b []byte
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("Failed to read response body")
	}
	err = json.Unmarshal(b, &smap)
	if err != nil {
		t.Fatalf("Failed to unmarshal Dfconfig: %v", err)
	}
	return
}

func getConfig(URL string, httpclient *http.Client, t *testing.T) (dfcfg map[string]interface{}) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(GetConfigMsg)
	if err != nil {
		t.Fatalf("Failed to marshal GetConfigMsg: %v", err)
	}
	req, err = http.NewRequest("GET", URL, bytes.NewBuffer(injson))
	if err != nil {
		t.Errorf("Failed to create request: %v", err)
	}
	r, err = httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Get configuration", r, nil, t) {
		return
	}

	var b []byte
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Errorf("Failed to read response body")
		return
	}
	err = json.Unmarshal(b, &dfcfg)
	if err != nil {
		t.Errorf("Failed to unmarshal config: %v", err)
		return
	}
	return
}
func setConfig(name, value, URL string, httpclient *http.Client, t *testing.T) {
	SetConfigMsg := dfc.ActionMsg{Action: dfc.ActSetConfig,
		Name:  name,
		Value: value,
	}

	injson, err := json.Marshal(SetConfigMsg)
	if err != nil {
		t.Errorf("Failed to marshal SetConfig Message: %v", err)
		return
	}
	req, err := http.NewRequest("PUT", URL, bytes.NewBuffer(injson))
	if err != nil {
		t.Errorf("Failed to create request: %v", err)
		return
	}
	r, err := httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if err != nil {
		t.Errorf("Failed to execute SetConfig request: %v", err)
		return
	}
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func selectErr(errch chan error, verb string, t *testing.T, errisfatal bool) {
	select {
	case err := <-errch:
		if errisfatal {
			t.Fatalf("Failed to %s files: %v", verb, err)
		} else {
			t.Errorf("Failed to %s files: %v", verb, err)
		}
	default:
	}
}

func tlogf(msg string, args ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stdout, msg, args...)
	}
}

func tlogln(msg string) {
	tlogf(msg + "\n")
}
