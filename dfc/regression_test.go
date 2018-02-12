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

const (
	RestAPIDaemonDelete    = ProxyURL + "/v1/cluster/daemon/"
	RestAPIClusterPath     = ProxyURL + "/v1/cluster"
	RestAPIDaemonPath      = ProxyURL + "/v1/daemon"
	RestAPILocalBucketPath = ProxyURL + "/v1/files/"
	RestAPIDaemonSuffix    = "/v1/daemon"
	TestLocalBucketName    = "TESTLOCALBUCKET"
)

var (
	GetConfigMsg         = dfc.GetMsg{GetWhat: dfc.GetWhatConfig}
	GetStatsMsg          = dfc.GetMsg{GetWhat: dfc.GetWhatStats}
	CreateLocalBucketMsg = dfc.ActionMsg{Action: dfc.ActCreateLB}
	GetSmapMsg           = dfc.GetMsg{GetWhat: dfc.GetWhatSmap}
	SyncmapMsg           = dfc.ActionMsg{Action: dfc.ActSyncSmap}
	RebalanceMsg         = dfc.ActionMsg{Action: dfc.ActionRebalance}
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
	}
	abortonerr       = true
	regressionFailed = false

	client = &http.Client{}
)

func init() {
	flag.BoolVar(&abortonerr, "abortonerr", abortonerr, "abort on error")
}

func Test_regression(t *testing.T) {
	flag.Parse()

	fmt.Fprintf(os.Stdout, "=== abortonerr = %v\n\n", abortonerr)

	if err := dfc.CreateDir(LocalRootDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalRootDir, err)
	}
	if err := dfc.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}
	t.Run("Local Buckets", regressionLocalBuckets)
	t.Run("Cloud Bucket", regressionCloudBuckets)
	t.Run("Stats", regressionStats)
	t.Run("Config", regressionConfig)
	t.Run("Sync&Rebalance", regressionSyncRebalance)

	// FIXME: LRU won't delete anything that is newer that (time.Now() - dontevicttime)
	t.Run("LRU", regressionLRU)
}

func regressionSyncRebalance(t *testing.T) {
	syncSmaps(client, t)
	rebalanceCluster(client, t)
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
	if abortonerr && t.Failed() {
		regressionFailed = true
	}
}

func regressionBucket(client *http.Client, t *testing.T, bucket string) {
	var (
		filesput = make(chan string, 10)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
		numPuts  = 10
	)
	putRandomFiles(0, 0, uint64(1024), numPuts, bucket, t, nil, errch, filesput)
	close(filesput) // to exit for-range
	selectErr(errch, "put", t)
	getRandomFiles(0, 0, numPuts, bucket, t, nil, errch)
	selectErr(errch, "get", t)
	for fname := range filesput {
		err := os.Remove(SmokeDir + "/" + fname)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go del(bucket, "smoke/"+fname, wg, errch)
	}
	wg.Wait()
	selectErr(errch, "delete", t)
	close(errch)
	if abortonerr && t.Failed() {
		regressionFailed = true
	}
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
	if abortonerr && t.Failed() {
		regressionFailed = true
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
	if lw, err := strconv.Atoi(configRegression["lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig["lowwm"] != float64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nlruconfig["lowwm"], lw)
	} else {
		o := olruconfig["lowwm"].(float64)
		setConfig("lowwm", strconv.Itoa(int(o)), RestAPIClusterPath, client, t)
	}
	if hw, err := strconv.Atoi(configRegression["highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig["highwm"] != float64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nlruconfig["highwm"], hw)
	} else {
		o := olruconfig["highwm"].(float64)
		setConfig("highwm", strconv.Itoa(int(o)), RestAPIClusterPath, client, t)
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

	if abortonerr && t.Failed() {
		regressionFailed = true
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
	selectErr(errch, "get", t)
	if t.Failed() {
		return
	}
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
	fmt.Fprintf(os.Stdout, "LRU: current min space usage in the cluster: %d%%\n", usedpct)
	var (
		lowwm  = usedpct - 5
		highwm = usedpct - 1
	)
	if int(lowwm) < 10 {
		t.Errorf("The current space usage is too low (%d) for the LRU to be tested", lowwm)
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
	defer setConfig("dont_evict_time", olruconfig["dont_evict_time"].(string), RestAPIClusterPath, client, t)
	defer func() {
		for k, di := range smap.Smap {
			setConfig("highwm", fmt.Sprint(hwms[k]), di.DirectURL+RestAPIDaemonSuffix, client, t)
			setConfig("lowwm", fmt.Sprint(lwms[k]), di.DirectURL+RestAPIDaemonSuffix, client, t)
		}
	}()
	//
	// cluster-wide reduce dont-evict-time
	//
	dontevicttimestr := "1s"
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
	for k, v := range stats.Target {
		bytes := v.Core.Bytesevicted - bytesEvictedOrig[k]
		fmt.Fprintf(os.Stdout, "Target %s: evicted %d files - %.2f MB (%dB) total\n",
			k, v.Core.Filesevicted-filesEvictedOrig[k], float64(bytes)/1000/1000, bytes)

		for mpath, c := range v.Capacity {
			if c.Usedpct < lowwm-1 || c.Usedpct > lowwm+1 {
				t.Errorf("Target %s failed to reach lwm %d%%: mpath %s, used space %d%%", k, lowwm, mpath, c.Usedpct)
			}
		}
	}
}

// helper (likely to be used)
func waitProgressBar(prefix string, wait time.Duration) {
	ticker := time.NewTicker(time.Second * 5)
	fmt.Fprintf(os.Stdout, prefix)
waitloop:
	for i := 1; ; i++ {
		select {
		case <-ticker.C:
			if regressionFailed {
				return
			}
			elapsed := time.Second * 2 * time.Duration(i)
			if elapsed >= wait {
				fmt.Fprintf(os.Stdout, "\n")
				break waitloop
			}
			fmt.Fprintf(os.Stdout, "----%d%%", (elapsed * 100 / wait))
		}
	}
}

func syncSmaps(client *http.Client, t *testing.T) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(SyncmapMsg)
	if err != nil {
		t.Fatalf("Failed to marshal SyncmapMsg: %v", err)
	}
	req, err = http.NewRequest("PUT", RestAPIClusterPath, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, fmt.Sprintf("Synchronize Smaps"), r, nil, t) {
		return
	}
}

func rebalanceCluster(client *http.Client, t *testing.T) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	injson, err = json.Marshal(RebalanceMsg)
	if err != nil {
		t.Fatalf("Failed to marshal RebalanceMsg: %v", err)
	}
	req, err = http.NewRequest("PUT", RestAPIClusterPath, bytes.NewBuffer(injson))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	r, err = client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if testfail(err, fmt.Sprintf("Initiate Rebalance"), r, nil, t) {
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
	if testfail(err, fmt.Sprintf("Create Local Bucket"), r, nil, t) {
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
	if testfail(err, fmt.Sprintf("Delete Local Bucket"), r, nil, t) {
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
	if testfail(err, fmt.Sprintf("Get configuration"), r, nil, t) {
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
	if testfail(err, fmt.Sprintf("Get configuration"), r, nil, t) {
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
	if testfail(err, fmt.Sprintf("Get configuration"), r, nil, t) {
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
	if testfail(err, fmt.Sprintf("Get configuration"), r, nil, t) {
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
		t.Errorf("Failed to unmarshal Dfconfig: %v", err)
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

func selectErr(errch chan error, verb string, t *testing.T) {
	select {
	case err := <-errch:
		if abortonerr {
			t.Fatalf("Failed to %s files: %v", verb, err)
		} else {
			t.Errorf("Failed to %s files: %v", verb, err)
		}
	default:
	}
}
