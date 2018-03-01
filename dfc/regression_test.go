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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
)

type Test struct {
	name   string
	method func(*testing.T)
}

const (
	RestAPIClusterPath     = ProxyURL + "/v1/cluster"
	RestAPIDaemonPath      = ProxyURL + "/v1/daemon"
	RestAPILocalBucketPath = ProxyURL + "/v1/files/"
	RestAPIDaemonSuffix    = "/v1/daemon"
	TestLocalBucketName    = "TESTLOCALBUCKET"
	RenameLocalBucketName  = "renamebucket"
	RenameDir              = "/tmp/dfc/rename"
	RenameStr              = "rename"
)

var (
	GetConfigMsg         = dfc.GetMsg{GetWhat: dfc.GetWhatConfig}
	GetStatsMsg          = dfc.GetMsg{GetWhat: dfc.GetWhatStats}
	CreateLocalBucketMsg = dfc.ActionMsg{Action: dfc.ActCreateLB}
	GetSmapMsg           = dfc.GetMsg{GetWhat: dfc.GetWhatSmap}
	RenameMsg            = dfc.ActionMsg{Action: dfc.ActRename}
	HighWaterMark        = uint32(80)
	LowWaterMark         = uint32(60)
	UpdTime              = time.Second * 20
	configRegression     = map[string]string{
		"stats_time":      fmt.Sprintf("%v", UpdTime),
		"dont_evict_time": fmt.Sprintf("%v", UpdTime),
		"lowwm":           fmt.Sprintf("%d", LowWaterMark),
		"highwm":          fmt.Sprintf("%d", HighWaterMark),
		"no_xattrs":       "false",
		"passthru":        "true",
		"lru_enabled":     "true",
	}
	abortonerr = false
	client     = &http.Client{}
	tests      = []Test{
		Test{"Local Bucket", regressionLocalBuckets},
		Test{"Cloud Bucket", regressionCloudBuckets},
		Test{"Stats", regressionStats},
		Test{"Config", regressionConfig},
		Test{"Rebalance", regressionRebalance},
		Test{"LRU", regressionLRU},
		Test{"Rename", regressionRename},
		Test{"RW stress", regressionRWStress},
	}
)

func init() {
	flag.BoolVar(&abortonerr, "abortonerr", abortonerr, "abort on error")
}

func Test_regression(t *testing.T) {
	flag.Parse()
	tlogf("=== abortonerr = %v\n\n", abortonerr)

	if err := dfc.CreateDir(LocalDestDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalDestDir, err)
	}
	if err := dfc.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}

	for _, test := range tests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}
}

func regressionCloudBuckets(t *testing.T) {
	regressionBucket(client, t, clibucket)
}

func regressionLocalBuckets(t *testing.T) {
	bucket := TestLocalBucketName
	createLocalBucket(client, t, bucket)
	time.Sleep(time.Second * 2) // FIXME: must be deterministic
	regressionBucket(client, t, bucket)
	destroyLocalBucket(client, t, bucket)
}

func regressionBucket(client *http.Client, t *testing.T, bucket string) {
	var (
		numPuts  = 10
		filesput = make(chan string, numPuts)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
	)
	putRandomFiles(0, baseseed+2, uint64(1024), numPuts, bucket, t, nil, errch, filesput, SmokeDir, smokestr)
	close(filesput) // to exit for-range
	selectErr(errch, "put", t, false)
	getRandomFiles(0, 0, numPuts, bucket, t, nil, errch)
	selectErr(errch, "get", t, false)
	for fname := range filesput {
		err := os.Remove(SmokeDir + "/" + fname)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go del(bucket, "smoke/"+fname, wg, errch, false)
	}
	wg.Wait()
	selectErr(errch, "delete", t, abortonerr)
	close(errch)
}

func regressionStats(t *testing.T) {
	smap := getClusterMap(client, t)
	stats := getClusterStats(client, t)

	for k, v := range stats.Target {
		tdstats := getDaemonStats(client, t, smap.Smap[k].DirectURL)
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
	oconfig := getConfig(RestAPIDaemonPath, client, t)
	olruconfig := oconfig["lru_config"].(map[string]interface{})
	oproxyconfig := oconfig["proxy"].(map[string]interface{})

	for k, v := range configRegression {
		setConfig(k, v, RestAPIClusterPath, client, t)
	}

	nconfig := getConfig(RestAPIDaemonPath, client, t)
	nlruconfig := nconfig["lru_config"].(map[string]interface{})
	nproxyconfig := nconfig["proxy"].(map[string]interface{})

	if nconfig["stats_time"] != configRegression["stats_time"] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nconfig["stats_time"], configRegression["stats_time"])
	} else {
		o := oconfig["stats_time"].(string)
		setConfig("stats_time", o, RestAPIClusterPath, client, t)
	}
	if nlruconfig["dont_evict_time"] != configRegression["dont_evict_time"] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig["dont_evict_time"], configRegression["dont_evict_time"])
	} else {
		o := olruconfig["dont_evict_time"].(string)
		setConfig("dont_evict_time", o, RestAPIClusterPath, client, t)
	}
	if hw, err := strconv.Atoi(configRegression["highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig["highwm"] != float64(hw) {
		t.Errorf("HighWatermark was not set properly: %.0f, should be: %d",
			nlruconfig["highwm"], hw)
	} else {
		o := olruconfig["highwm"].(float64)
		setConfig("highwm", strconv.Itoa(int(o)), RestAPIClusterPath, client, t)
	}
	if lw, err := strconv.Atoi(configRegression["lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig["lowwm"] != float64(lw) {
		t.Errorf("LowWatermark was not set properly: %.0f, should be: %d",
			nlruconfig["lowwm"], lw)
	} else {
		o := olruconfig["lowwm"].(float64)
		setConfig("lowwm", strconv.Itoa(int(o)), RestAPIClusterPath, client, t)
	}
	if nx, err := strconv.ParseBool(configRegression["no_xattrs"]); err != nil {
		t.Fatalf("Error parsing NoXattrs: %v", err)
	} else if nconfig["no_xattrs"] != nx {
		t.Errorf("NoXattrs was not set properly: %v, should be: %v",
			nconfig["no_xattrs"], nx)
	} else {
		o := oconfig["no_xattrs"].(bool)
		setConfig("no_xattrs", strconv.FormatBool(o), RestAPIClusterPath, client, t)
	}
	if pt, err := strconv.ParseBool(configRegression["passthru"]); err != nil {
		t.Fatalf("Error parsing Passthru: %v", err)
	} else if nproxyconfig["passthru"] != pt {
		t.Errorf("Proxy Passthru was not set properly: %v, should be %v",
			nproxyconfig["passthru"], pt)
	} else {
		o := oproxyconfig["passthru"].(bool)
		setConfig("passthru", strconv.FormatBool(o), RestAPIClusterPath, client, t)
	}
	if pt, err := strconv.ParseBool(configRegression["lru_enabled"]); err != nil {
		t.Fatalf("Error parsing LRUEnabled: %v", err)
	} else if nlruconfig["lru_enabled"] != pt {
		t.Errorf("LRUEnabled was not set properly: %v, should be %v",
			nlruconfig["lru_enabled"], pt)
	} else {
		o := olruconfig["lru_enabled"].(bool)
		setConfig("lru_enabled", strconv.FormatBool(o), RestAPIClusterPath, client, t)
	}
}

func regressionLRU(t *testing.T) {
	var (
		errch   = make(chan error, 100)
		usedpct = uint32(100)
	)
	//
	// remember targets' watermarks
	//
	smap := getClusterMap(client, t)
	lwms := make(map[string]interface{})
	hwms := make(map[string]interface{})
	bytesEvictedOrig := make(map[string]int64)
	filesEvictedOrig := make(map[string]int64)
	for k, di := range smap.Smap {
		cfg := getConfig(di.DirectURL+RestAPIDaemonSuffix, client, t)
		lrucfg := cfg["lru_config"].(map[string]interface{})
		lwms[k] = lrucfg["lowwm"]
		hwms[k] = lrucfg["highwm"]
	}
	//
	// add some files
	//
	getRandomFiles(0, 0, 20, clibucket, t, nil, errch)
	selectErr(errch, "get", t, true)
	//
	// find out min usage %% across all targets
	//
	stats := getClusterStats(client, t)
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
		t.Skipf("The current space usage is too low (%d) for the LRU to be tested", lowwm)
		return
	}
	oconfig := getConfig(RestAPIDaemonPath, client, t)
	if t.Failed() {
		return
	}
	//
	// all targets: set new watermarks; restore upon exit
	//
	olruconfig := oconfig["lru_config"].(map[string]interface{})
	defer func() {
		setConfig("dont_evict_time", olruconfig["dont_evict_time"].(string), RestAPIClusterPath, client, t)
		setConfig("highwm", fmt.Sprint(olruconfig["highwm"]), RestAPIClusterPath, client, t)
		setConfig("lowwm", fmt.Sprint(olruconfig["lowwm"]), RestAPIClusterPath, client, t)
		for k, di := range smap.Smap {
			setConfig("highwm", fmt.Sprint(hwms[k]), di.DirectURL+RestAPIDaemonSuffix, client, t)
			setConfig("lowwm", fmt.Sprint(lwms[k]), di.DirectURL+RestAPIDaemonSuffix, client, t)
		}
	}()
	//
	// cluster-wide reduce dont-evict-time
	//
	dontevicttimestr := "20s"                                            // 2 * stats_time; FIXME: race vs smoke test
	sleeptime, err := time.ParseDuration(oconfig["stats_time"].(string)) // to make sure the stats get updated
	if err != nil {
		t.Fatalf("Failed to parse stats_time: %v", err)
	}
	setConfig("dont_evict_time", dontevicttimestr, RestAPIClusterPath, client, t) // NOTE: 1 second
	if t.Failed() {
		return
	}
	setConfig("lowwm", fmt.Sprint(lowwm), RestAPIClusterPath, client, t)
	if t.Failed() {
		return
	}
	setConfig("highwm", fmt.Sprint(highwm), RestAPIClusterPath, client, t)
	if t.Failed() {
		return
	}
	waitProgressBar("LRU: ", sleeptime)
	getRandomFiles(0, 0, 1, clibucket, t, nil, errch)
	waitProgressBar("LRU: ", sleeptime)
	//
	// results
	//
	stats = getClusterStats(client, t)
	test_fspaths := oconfig["test_fspaths"].(map[string]interface{})
	for k, v := range stats.Target {
		bytes := v.Core.Bytesevicted - bytesEvictedOrig[k]
		tlogf("Target %s: evicted %d files - %.2f MB (%dB) total\n",
			k, v.Core.Filesevicted-filesEvictedOrig[k], float64(bytes)/1000/1000, bytes)
		//
		// testingFSPpaths() - cannot reliably verify space utilization by tmpfs
		//
		if test_fspaths["count"].(float64) > 0 {
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
		numPuts  = 30
		filesput = make(chan string, numPuts)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
	)
	filesSentOrig := make(map[string]int64)
	bytesSentOrig := make(map[string]int64)
	filesRecvOrig := make(map[string]int64)
	bytesRecvOrig := make(map[string]int64)
	stats := getClusterStats(client, t)
	for k, v := range stats.Target {
		bytesSentOrig[k], filesSentOrig[k], bytesRecvOrig[k], filesRecvOrig[k] =
			v.Core.Numsentbytes, v.Core.Numsentfiles, v.Core.Numrecvbytes, v.Core.Numrecvfiles
	}
	//
	// step 1. unregister random target
	//
	smap := getClusterMap(client, t)
	l := len(smap.Smap)
	if l < 2 {
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
	// step 2. put random files => (cluster - 1)
	//
	putRandomFiles(0, baseseed, uint64(1024*128), numPuts, clibucket, t, nil, errch, filesput, SmokeDir, smokestr)
	selectErr(errch, "put", t, false)

	//
	// step 3. register back
	//
	registerTarget(sid, &smap, t)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		smap = getClusterMap(client, t)
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
	// step 4. wait for rebalance to run its course
	//
	waitProgressBar("Rebalance: ", time.Second*10)
	//
	// step 5. statistics
	//
	stats = getClusterStats(client, t)
	var bsent, fsent, brecv, frecv int64
	for k, v := range stats.Target {
		bsent += v.Core.Numsentbytes - bytesSentOrig[k]
		fsent += v.Core.Numsentfiles - filesSentOrig[k]
		brecv += v.Core.Numrecvbytes - bytesRecvOrig[k]
		frecv += v.Core.Numrecvfiles - filesRecvOrig[k]
	}

	//
	// step 6. cleanup
	//
	close(filesput) // to exit for-range
	for fname := range filesput {
		err := os.Remove(SmokeDir + "/" + fname)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go del(clibucket, "smoke/"+fname, wg, errch, false)
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
		basenames = make([]string, 0, numPuts) // basenames generated by putRandomFiles
		bnewnames = make([]string, 0, numPuts) // new basenames
	)
	// create & put
	createLocalBucket(client, t, RenameLocalBucketName)
	defer func() {
		// cleanup
		wg := &sync.WaitGroup{}
		for _, fname := range bnewnames {
			wg.Add(1)
			go del(RenameLocalBucketName, RenameStr+"/"+fname, wg, errch, false)
		}
		for _, fname := range basenames {
			err = os.Remove(RenameDir + "/" + fname)
			if err != nil {
				t.Errorf("Failed to remove file %s: %v", fname, err)
			}
		}
		wg.Wait()
		selectErr(errch, "delete", t, false)
		close(errch)
		destroyLocalBucket(client, t, RenameLocalBucketName)
	}()

	time.Sleep(time.Second * 5)

	if err = dfc.CreateDir(RenameDir); err != nil {
		t.Errorf("Error creating dir: %v", err)
	}
	putRandomFiles(0, baseseed+1, 0, numPuts, RenameLocalBucketName, t, nil, nil, filesput, RenameDir, RenameStr)
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
		req, err = http.NewRequest("POST", RestAPILocalBucketPath+RenameLocalBucketName+"/"+RenameStr+"/"+fname, bytes.NewBuffer(injson))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		r, err = client.Do(req)
		if r != nil {
			r.Body.Close()
		}
		s := fmt.Sprintf("Rename %s/%s => %s", RenameStr, fname, RenameMsg.Name)
		if testfail(err, s, r, nil, t) {
			destroyLocalBucket(client, t, RenameLocalBucketName)
			return
		}
		tlogln(s)
	}

	// get renamed objects
	waitProgressBar("Rename/move: ", time.Second*5)
	for _, fname := range bnewnames {
		get(RenameLocalBucketName, RenameStr+"/"+fname, nil, errch, false)
	}
	selectErr(errch, "get", t, false)
}

// helper (likely to be used)
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
	req, err = http.NewRequest("DELETE", RestAPIClusterPath+"/"+"daemon"+"/"+sid, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
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
	r, err = client.Do(req)
	if r != nil {
		r.Body.Close()
	}
	if t.Failed() || testfail(err, fmt.Sprintf("Register target %s", sid), r, nil, t) {
		return
	}
}

func createLocalBucket(client *http.Client, t *testing.T, bucket string) {
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
	req, err = http.NewRequest("POST", RestAPILocalBucketPath+bucket, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Create Local Bucket", r, nil, t) {
		return
	}
}

func destroyLocalBucket(client *http.Client, t *testing.T, bucket string) {
	var (
		req *http.Request
		r   *http.Response
		err error
	)
	req, err = http.NewRequest("DELETE", RestAPILocalBucketPath+bucket, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, "Delete Local Bucket", r, nil, t) {
		return
	}
}

func getClusterStats(client *http.Client, t *testing.T) (stats dfc.ClusterStats) {
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
	req, err = http.NewRequest("GET", RestAPIClusterPath, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
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

func getDaemonStats(client *http.Client, t *testing.T, URL string) (stats map[string]interface{}) {
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
	r, err = client.Do(req)
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

func getClusterMap(client *http.Client, t *testing.T) (smap dfc.Smap) {
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
	req, err = http.NewRequest("GET", RestAPIClusterPath, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
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

func getConfig(URL string, client *http.Client, t *testing.T) (dfcfg map[string]interface{}) {
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
	r, err = client.Do(req)
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
func setConfig(name, value, URL string, client *http.Client, t *testing.T) {
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
	r, err := client.Do(req)
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
