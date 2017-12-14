// CopyRight Notice: All rights reserved
//
//

package dfc

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/golang/glog"
)

// dfconfig structure specifies Configuration Parameters for DFC Instance
// (Proxy Client or Storage Server) in JSON format.
// Config Parameters are specified during DFC service instantiation.
//  These Parameter overrides default paramemters.
// TODO Get and Set Config Parameter functionality/interface(s).
type dfconfig struct {
	ID            string       `json:"id"`
	Logdir        string       `json:"logdir"`
	Loglevel      string       `json:"loglevel"`
	CloudProvider string       `json:"cloudprovider"`
	Listen        listenconfig `json:"listen"`
	Proxy         proxyconfig  `json:"proxy"`
	S3            s3config     `json:"s3"`
	Cache         cacheconfig  `json:"cache"`
}

// Need to define structure for each cloud vendor like S3 , Azure, Cloud etc
// AWS S3 configurable parameters

// s3config specifies  Amazon S3 specific configuration parameters.
type s3config struct {
	Maxconcurrdownld uint32 `json:"maxconcurrdownld"` // Concurent Download for a session.
	Maxconcurrupld   uint32 `json:"maxconcurrupld"`   // Concurrent Upload for a session.
	Maxpartsize      uint64 `json:"maxpartsize"`      // Maximum part size for Upload and Download used for buffering.
}

// cacheconfig specifies caching specific parameters.
type cacheconfig struct {

	// CachePath specifies caching path(location) on DFC instance for cached objects.
	// It can be nil .
	CachePath string `json:"cachepath"`

	// CachePathCount specifies number of cache paths for DFC storage instance. It is to emulate
	// MultiMountPoint support. It can be zero.
	CachePathCount int `json:"cachepathcount"`

	// ErrorThreshold specifies errorthreshold for specific cache path.DFC will set cachepath to be unusable
	// if errorcount is > errorthreshold.
	ErrorThreshold int `json:"errorthreshold"`

	// FSCheck frequency specifies frequency to run FSCheck thread . It is specified in minutes.
	FSCheckfreq uint32 `json:"fscheckfreq"`

	// LowWaterMark is specified in %, FS Usage need to be higher than LowWaterMark for FSCheck thread to purge old cached data.
	FSLowWaterMark uint32 `json:"fslowwatermark"`

	// HighWaterMark is specified in %. FSCheck thread will purge old data aggressively if FS Usage is more than HighWaterMark.
	FSHighWaterMark uint32 `json:"fshighwatermark"`
}

// listenconfig specifies listner Parameter for DFC instance.
// User can specify Port and Protocol(TCP/UDP) as part of listenconfig.
type listenconfig struct {
	Proto string `json:"proto"` // Prototype : tcp, udp
	Port  string `json:"port"`  // Listening port.
}

// proxyconfig specifies well-known address for http proxy as http://<ipaddress>:<portnumber>
type proxyconfig struct {
	URL      string `json:"url"`      // used to register caching servers
	Passthru bool   `json:"passthru"` // false: get then redirect, true (default): redirect right away
}

// Read JSON Config file and populate DFC Instance's config parameters.
// We currently support only one configuration per JSON file.
func initconfigparam(configfile, loglevel, role string) error {
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
	// Argument specified at commandline or through flags has highest precedence.
	if loglevel != "" {
		err = flag.Lookup("v").Value.Set(loglevel)
	} else {
		err = flag.Lookup("v").Value.Set(ctx.config.Loglevel)
	}
	if err != nil {
		//  Not fatal as it will use default logging level
		glog.Errorf("Failed to set loglevel %v", err)
	}

	glog.Infof("============== ")
	glog.Infof("============== Log level: %s Config: %s Role: %s", flag.Lookup("v").Value.String(), configfile, role)
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
