/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/golang/glog"
)

// TODO: AWS specific initialization
type awsif struct {
}

// TODO: GCP specific initialization
type gcpif struct {
}

type cinterface interface {
	listbucket(http.ResponseWriter, string)
	getobj(http.ResponseWriter, string, string, string)
}

// storhdlr implements the target's REST API. It checks wheather the key exists locally,
// and downloads the corresponding object otherwise
func storhdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodGet)
	stats := getstorstats()
	//
	// parse and validate
	//
	split := strings.SplitN(html.EscapeString(r.URL.Path), "/", 5)
	apitems := split[1:]
	if !checkRestAPI(w, r, apitems, 3, apiversion, apiresfiles) {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	bktname, keyname := apitems[2], ""
	if len(apitems) > 3 {
		keyname = apitems[3]
	}
	atomic.AddInt64(&stats.numget, 1)
	//
	// list the bucket and return
	//
	if len(keyname) == 0 {
		getcloudif().listbucket(w, bktname)
		return
	}
	//
	// get from the bucket
	//
	mpath := doHashfindMountPath(bktname + "/" + keyname)
	assert(len(mpath) > 0) // see mountpath.enabled
	fname := mpath + "/" + bktname + "/" + keyname
	_, err := os.Stat(fname)
	if os.IsNotExist(err) {
		atomic.AddInt64(&stats.numcoldget, 1)
		glog.Infof("Bucket %s key %s fqn %q is not cached", bktname, keyname, fname)
		getcloudif().getobj(w, mpath, bktname, keyname)
	} else if glog.V(2) {
		glog.Infof("Bucket %s key %s fqn %q is cached", bktname, keyname, fname)
	}
	file, err := os.Open(fname)
	if err != nil {
		glog.Errorf("Failed to open %q, err: %v", fname, err)
		checksetmounterror(fname)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		atomic.AddInt64(&stats.numerr, 1)
	} else {
		defer file.Close()
		// TODO: optimize. Currently the file gets downloaded and stored locally
		//       _prior_ to sending http response back to the requesting client
		_, err := io.Copy(w, file)
		if err != nil {
			glog.Errorf("Failed to copy %q to http response, err: %v", fname, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			atomic.AddInt64(&stats.numerr, 1)
		} else if glog.V(3) {
			glog.Infof("Copied %q to http response", fname)
		}
	}
	glog.Flush()
}

//===========================================================================
//
// registration w/ proxy
//
//===========================================================================
func registerwithproxy() (rerr error) {
	httpClient = createHTTPClient()
	proxyURL := ctx.config.Proxy.URL
	data := url.Values{}
	ipaddr, err := getipaddr()
	if err != nil {
		return err
	}
	// name/value: IP, port and ID as part of the target's registration
	data.Set(IP, ipaddr)
	data.Add(PORT, string(ctx.config.Listen.Port))
	data.Add(ID, ctx.config.ID)

	u, _ := url.ParseRequestURI(string(proxyURL))
	//
	// REST API
	//
	u.Path = "/" + apiversion + "/" + apirestargets + "/"
	urlStr := u.String()
	if glog.V(3) {
		glog.Infof("URL %q", urlStr)
	}
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewBufferString(data.Encode()))
	if err != nil {
		glog.Errorf("Unexpected failure to create POST request, err: %v", err)
		return err
	}
	req.Header.Add("Authorization", "auth_token=\"XXXXXXX\"")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	// send http request
	response, err := httpClient.Do(req)
	if err != nil && response == nil {
		glog.Errorf("Failed to register with proxy, err: %v", err)
		return err
	}
	// cleanup
	defer func() {
		err = response.Body.Close()
		if err != nil {
			rerr = err
		}
	}()
	// check if the work was actually done
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		glog.Errorf("Couldn't parse response body, err: %v", err)
		return err
	}
	return nil
}

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif cinterface // Interface for multiple cloud
}

// start target runner
func (r *targetrunner) run() error {
	// FIXME cleanup: unreg is missing
	if err := registerwithproxy(); err != nil {
		glog.Fatalf("Failed to register with proxy, err: %v", err)
		return err
	}
	// Local mount points have precedence over cachePath settings
	var err error
	if ctx.mountpaths, err = parseProcMounts(procMountsPath); err != nil {
		glog.Fatalf("Failed to parse %s, err: %v", procMountsPath, err)
		return err
	}
	if len(ctx.mountpaths) == 0 {
		glog.Infof("Warning: configuring %d mp-s for testing", ctx.config.Cache.CachePathCount)

		// Use CachePath from config file if set
		if ctx.config.Cache.CachePath == "" || ctx.config.Cache.CachePathCount < 1 {
			errstr := fmt.Sprintf("Invalid configuration: CachePath %q CachePathCount %d",
				ctx.config.Cache.CachePath, ctx.config.Cache.CachePathCount)
			glog.Error(errstr)
			err := errors.New(errstr)
			return err
		}
		ctx.mountpaths = emulateCachepathMounts()
	} else {
		glog.Infof("Found %d mp-s", len(ctx.mountpaths))
	}

	// init mps in the stats
	initusedstats()

	// cloud provider
	assert(ctx.config.CloudProvider == amazoncloud || ctx.config.CloudProvider == googlecloud)
	if ctx.config.CloudProvider == amazoncloud {
		// TODO: AWS initialization (sessions)
		r.cloudif = &awsif{}

	} else {
		r.cloudif = &gcpif{}
	}
	//
	// REST API: register storage target's handler(s) and start listening
	//
	r.httprunner.registerhdlr("/"+apiversion+"/"+apiresfiles+"/", storhdlr)
	r.httprunner.registerhdlr("/", invalhdlr)
	return r.httprunner.run()
}

// stop gracefully
func (r *targetrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	r.httprunner.stop(err)
}
