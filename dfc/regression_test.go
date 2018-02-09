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

	HighWaterMark = uint32(80)
	LowWaterMark  = uint32(60)

	configSettings = [6]string{"stats_time", "dont_evict_time", "lowwm", "highwm", "no_xattrs", "passthru"}
	configValues   = [6]string{"20s", "20s", fmt.Sprint(LowWaterMark), fmt.Sprint(HighWaterMark), "true", "true"}
	client         = &http.Client{}
)

func init() {
}

func Test_regression(t *testing.T) {
	flag.Parse()

	t.Run("Local Buckets", regressionLocalBuckets)
	t.Run("Cloud Bucket", regressionCloudBuckets)
	t.Run("Stats", regressionStats)
	t.Run("Config", regressionConfig)
	t.Run("Sync&Rebalance", regressionSyncRebalance)
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
	time.Sleep(1500 * time.Millisecond)
	regressionBucket(client, t, bucket)
	destroyLocalBucket(client, t, bucket)
}

func regressionBucket(client *http.Client, t *testing.T, bucket string) {
	var (
		filesput = make(chan string, 10)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
	)
	wg.Add(1)
	go putRandomFiles(0, 0, 1024, 10, bucket, t, wg, errch, filesput)
	wg.Wait()
	selectErr(errch, t)
	wg.Add(1)
	close(filesput)
	go getRandomFiles(0, 0, 10, bucket, t, wg, errch)
	wg.Wait()
	selectErr(errch, t)
	for file := range filesput {
		err := os.Remove(SmokeDir + file)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go del(bucket, "smoke/"+file, wg, errch)
	}
	wg.Wait()
	selectErr(errch, t)
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
	_ = oconfig

	for i := 0; i < len(configSettings); i++ {
		setConfig(configSettings[i], configValues[i], RestAPIDaemonPath, client, t)
	}

	nconfig := getConfig(RestAPIDaemonPath, client, t)
	lruconfig := nconfig["lru_config"].(map[string]interface{})
	proxyconfig := nconfig["proxy"].(map[string]interface{})

	if nconfig["stats_time"] != configValues[0] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nconfig["stats_time"], configValues[0])
	}
	if lruconfig["dont_evict_time"] != configValues[1] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			lruconfig["dont_evict_time"], configValues[1])
	}
	if lw, err := strconv.ParseFloat(configValues[2], 64); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if lruconfig["lowwm"] != lw {
		t.Errorf("LowWatermark was not set properly: %v, should be: %v",
			lruconfig["lowwm"], lw)
	}
	if hw, err := strconv.ParseFloat(configValues[3], 64); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if lruconfig["highwm"] != hw {
		t.Errorf("HighWatermark was not set properly: %v, should be: %v",
			lruconfig["highwm"], hw)
	}
	if nx, err := strconv.ParseBool(configValues[4]); err != nil {
		t.Fatalf("Error parsing NoXattrs: %v", err)
	} else if nconfig["no_xattrs"] != nx {
		t.Errorf("NoXattrs was not set properly: %v, should be: %v",
			nconfig["no_xattrs"], nx)
	}
	if pt, err := strconv.ParseBool(configValues[5]); err != nil {
		t.Fatalf("Error parsing Passthru: %v", err)
	} else if proxyconfig["passthru"] != pt {
		t.Errorf("Proxy Passthru was not set properly: %v, should be %v",
			proxyconfig["passthru"], pt)
	}
}

func regressionLRU(t *testing.T) {
	var (
		errch   = make(chan error, 100)
		wg      = &sync.WaitGroup{}
		usedpct = uint32(100)
	)
	smap := getClusterMap(client, t)
	lwms := make([]interface{}, len(smap.Smap))
	hwms := make([]interface{}, len(smap.Smap))
	i := 0
	for _, di := range smap.Smap {
		cfg := getConfig(di.DirectURL+RestAPIDaemonSuffix, client, t)
		lrucfg := cfg["lru_config"].(map[string]interface{})
		lwms[i] = lrucfg["lowwm"]
		hwms[i] = lrucfg["highwm"]
		i++
	}
	stats := getClusterStats(client, t)
	for _, v := range stats.Target {
		for _, c := range v.Capacity {
			usedpct = min(usedpct, c.Usedpct)
		}
	}
	var (
		lowwm  = usedpct - 3
		highwm = usedpct - 1
	)
	for _, di := range smap.Smap {
		setConfig("lowwm", fmt.Sprint(lowwm), di.DirectURL+RestAPIDaemonSuffix, client, t)
		setConfig("highwm", fmt.Sprint(highwm), di.DirectURL+RestAPIDaemonSuffix, client, t)
	}
	wg.Add(1)
	go getRandomFiles(0, 0, 10, clibucket, t, wg, errch)
	wg.Wait()
	selectErr(errch, t)
	// Give time for the proxy to execute LRU
	time.Sleep(20 * time.Second)
	stats = getClusterStats(client, t)
	for _, v := range stats.Target {
		for _, c := range v.Capacity {
			if c.Usedpct < lowwm {
				t.Errorf("Usedpct %v, below low watermark %v", c.Usedpct, lowwm)
			} else if c.Usedpct > highwm {
				t.Errorf("Usedpct %v above high watermark %v", c.Usedpct, highwm)
			}
		}
	}
	i = 0
	for _, di := range smap.Smap {
		setConfig("highwm", fmt.Sprint(hwms[i]), di.DirectURL+RestAPIDaemonSuffix, client, t)
		setConfig("lowwm", fmt.Sprint(lwms[i]), di.DirectURL+RestAPIDaemonSuffix, client, t)
		i++
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

func selectErr(errch chan error, t *testing.T) {
	select {
	case err := <-errch:
		t.Errorf("Failed to put files: %v", err)
	default:
	}
}
