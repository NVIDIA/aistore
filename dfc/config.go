/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/golang/glog"
)

// dfconfig structure specifies Configuration Parameters for DFC Instance
// (Proxy Client or Storage Server) in JSON format.
// Config Parameters are specified during DFC service instantiation.
//  These Parameter overrides default paramemters.
// TODO Get and Set Config Parameter functionality/interface(s).
type dfconfig struct {
	ID            string        `json:"id"`
	Logdir        string        `json:"logdir"`
	Loglevel      string        `json:"loglevel"`
	CloudProvider string        `json:"cloudprovider"`
	StatsTime     time.Duration `json:"stats_time"`
	HttpTimeout   time.Duration `json:"http_timeout"`
	Listen        listenconfig  `json:"listen"`
	Proxy         proxyconfig   `json:"proxy"`
	S3            s3config      `json:"s3"`
	Cache         cacheconfig   `json:"cache"`
}

const (
	amazoncloud = "aws"
	googlecloud = "gcp"
)

// s3config specifies  Amazon S3 specific configuration parameters
type s3config struct {
	Maxconcurrdownld uint32 `json:"maxconcurrdownld"` // Concurent Download for a session.
	Maxconcurrupld   uint32 `json:"maxconcurrupld"`   // Concurrent Upload for a session.
	Maxpartsize      uint64 `json:"maxpartsize"`      // Maximum part size for Upload and Download used for buffering.
}

// caching configuration
type cacheconfig struct {
	CachePath       string        `json:"cachepath"`       // caching path
	CachePathCount  int           `json:"cachepathcount"`  // num cache paths
	ErrorThreshold  int           `json:"errorthreshold"`  // error threshold for the specific cache path to become unusable
	FSLowWaterMark  uint32        `json:"fslowwatermark"`  // capacity usage low watermark
	FSHighWaterMark uint32        `json:"fshighwatermark"` // capacity usage high watermark
	DontEvictTime   time.Duration `json:"dont_evict_time"` // eviction is not permitted during [atime, atime + dont]
}

// daemon listenig params
type listenconfig struct {
	Proto string `json:"proto"` // Prototype : tcp, udp
	Port  string `json:"port"`  // Listening port.
}

// proxyconfig specifies proxy's well-known address as http://<ipaddress>:<portnumber>
type proxyconfig struct {
	URL      string `json:"url"`      // used to register caching servers
	Passthru bool   `json:"passthru"` // false: get then redirect, true (default): redirect right away
}

// Load and validate daemon's config
func initconfigparam(configfile, loglevel, role string, statstime time.Duration) error {
	getConfig(configfile)

	err := flag.Lookup("log_dir").Value.Set(ctx.config.Logdir)
	if err != nil {
		// Non-fatal as it'll be placing it directly under the /tmp
		glog.Errorf("Failed to flag-set glog dir %q, err: %v", ctx.config.Logdir, err)
	}
	if glog.V(3) {
		glog.Infof("Logdir %q Proto %s Port %s ID %s loglevel %s",
			ctx.config.Logdir, ctx.config.Listen.Proto,
			ctx.config.Listen.Port, ctx.config.ID, ctx.config.Loglevel)
	}
	for i := 0; i < ctx.config.Cache.CachePathCount; i++ {
		mpath := ctx.config.Cache.CachePath + dfcStoreMntPrefix + strconv.Itoa(i)
		err = createdir(mpath)
		if err != nil {
			glog.Errorf("Failed to create cachedir %q, err: %v", mpath, err)
			return err
		}
		// Create DFC signature file

		dfile := mpath + dfcSignatureFileName
		// Always write signature file, We may want data to have some instance specific
		// timing or stateful information.
		data := []byte("dfcsignature \n")
		err := ioutil.WriteFile(dfile, data, 0644)
		if err != nil {
			glog.Errorf("Failed to create signature file %q, err: %v", dfile, err)
			return err
		}

	}
	err = createdir(ctx.config.Logdir)
	if err != nil {
		glog.Errorf("Failed to create Logdir %q, err: %v", ctx.config.Logdir, err)
		return err
	}

	// CLI override
	if statstime != 0 {
		ctx.config.StatsTime = statstime
	}
	if loglevel != "" {
		err = flag.Lookup("v").Value.Set(loglevel)
		ctx.config.Loglevel = loglevel
	} else {
		err = flag.Lookup("v").Value.Set(ctx.config.Loglevel)
	}
	if err != nil {
		//  Not fatal as it will use default logging level
		glog.Errorf("Failed to set loglevel %v", err)
	}

	glog.Infof("============== ")
	glog.Infof("============== Verbosity: %s Config: %q Role: %s StatsTime %v",
		ctx.config.Loglevel, configfile, role, ctx.config.StatsTime)
	glog.Infof("============== ")
	glog.Flush()
	return err
}

// Helper function to Create specified directory. It will also create complete path, not
// just short path.(similar to mkdir -p)
func createdir(dirname string) error {
	var err error
	_, err = os.Stat(dirname)
	if err != nil {
		// Create bucket-path directory for non existent paths.
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				glog.Errorf("Failed to create dir %q, err: %v", dirname, err)
			}
		} else {
			glog.Errorf("Failed to stat %s, err: %v", dirname, err)
		}
	}
	return err
}

// Read JSON config file and unmarshal json content into config struct.
func getConfig(fpath string) {
	raw, err := ioutil.ReadFile(fpath)
	if err != nil {
		glog.Errorf("Failed to read config %q, err: %v", fpath, err)
		os.Exit(1)
	}
	err = json.Unmarshal(raw, &ctx.config)
	if err != nil {
		glog.Errorf("Failed to json-unmarshal config %q, err: %v", fpath, err)
		os.Exit(1)
	}
}
