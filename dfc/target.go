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
	listbucket(http.ResponseWriter, string) error
	getobj(http.ResponseWriter, string, string, string) error
}

// storhdlr implements the target's REST API: checks if the named fobject
// exists locallyi. If not, it downloads it to cache and (always)
// sends it back via http
func storhdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodGet)
	stats := getstorstats()
	//
	// parse and validate REST API
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
	mpath := hrwMpath(bktname + "/" + keyname)
	assert(len(mpath) > 0) // see mountpath.enabled
	fname := mpath + "/" + bktname + "/" + keyname
	_, err := os.Stat(fname)
	if os.IsNotExist(err) {
		atomic.AddInt64(&stats.numcoldget, 1)
		glog.Infof("Bucket %s key %s fqn %q is not cached", bktname, keyname, fname)
		//
		// TODO: do getcloudif().getobj() and write http response in parallel
		//
		if err = getcloudif().getobj(w, mpath, bktname, keyname); err != nil {
			return
		}
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
		// NOTE: the following copyBuffer() call is equaivalent to:
		// 	rt, _ := w.(io.ReaderFrom)
		// 	written, err := rt.ReadFrom(file) ==> sendfile path
		written, err := copyBuffer(w, file)
		if err != nil {
			glog.Errorf("Failed to copy %q to http response, err: %v", fname, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			atomic.AddInt64(&stats.numerr, 1)
		} else if glog.V(3) {
			glog.Infof("Copied %q to http response (size %.2f MB)", fname, float64(written)/1000/1000)
		}
	}
	glog.Flush()
}

func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: maxidleconns},
		Timeout:   requesttimeout,
	}
	return client
}

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif    cinterface   // Interface for multiple cloud
	httpclient *http.Client // http client for intra-cluster comm
}

// start target runner
func (r *targetrunner) run() error {
	// init
	r.httprunner.init()
	r.httpclient = createHTTPClient()

	// FIXME cleanup unreg
	if err := r.register(); err != nil {
		glog.Errorf("Failed to register with proxy, err: %v", err)
		return err
	}
	// local mp-s have precedence over cachePath
	var err error
	ctx.mountpaths = make(map[string]mountPath, 4)
	if err = parseProcMounts(procMountsPath); err != nil {
		glog.Errorf("Failed to parse %s, err: %v", procMountsPath, err)
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
		emulateCachepathMounts()
	} else {
		glog.Infof("Found %d mp-s", len(ctx.mountpaths))
	}

	// init per-mp usage stats
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
	r.unregister()
	r.httprunner.stop(err)
}

// registration
func (r *targetrunner) register() error {
	data := url.Values{}
	// name/value: IP, port and ID as part of the target's registration
	data.Set(nodeIPAddr, r.ipaddr)
	data.Add(daemonPort, ctx.config.Listen.Port)
	data.Add(daemonID, r.sid)

	u, _ := url.ParseRequestURI(ctx.config.Proxy.URL)
	//
	// REST API
	//
	u.Path = "/" + apiversion + "/" + apirestargets + "/"
	urlStr := u.String()
	if glog.V(3) {
		glog.Infof("URL %q", urlStr)
	}
	method := http.MethodPost
	action := "register with"
	req, err := http.NewRequest(method, urlStr, bytes.NewBufferString(data.Encode()))
	if err != nil {
		glog.Errorf("Unexpected failure to create %s request, err: %v", method, err)
		return err
	}
	req.Header.Add("Authorization", "auth_token=\"XXXXXXX\"")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	// send http request
	response, err := r.httpclient.Do(req)
	if err != nil || response == nil {
		glog.Errorf("Failed to %s proxy, err: %v", action, err)
		return err
	}
	// cleanup
	defer func() {
		if response != nil {
			err = response.Body.Close()
		}
	}()
	// check if the work was actually done
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		glog.Errorf("Couldn't parse response body, err: %v", err)
		return err
	}
	return err
}

// unregistration
func (r *targetrunner) unregister() error {
	data := url.Values{}
	u, _ := url.ParseRequestURI(ctx.config.Proxy.URL)
	//
	// REST API
	//
	u.Path = "/" + apiversion + "/" + apirestargets + "/" + daemonID + "/" + r.sid
	urlStr := u.String()
	if glog.V(3) {
		glog.Infof("URL %q", urlStr)
	}
	method := http.MethodDelete
	req, err := http.NewRequest(method, urlStr, bytes.NewBufferString(data.Encode()))
	if err != nil {
		glog.Errorf("Unexpected failure to create %s request, err: %v", method, err)
		return err
	}
	// send http request
	response, err := r.httpclient.Do(req)
	if err != nil || response == nil {
		// proxy may have already shut down
		glog.Infof("Failed to unregister from proxy, err: %v", err)
		return err
	}
	// cleanup
	defer func() {
		if response != nil {
			err = response.Body.Close()
		}
	}()
	// check if the work was actually done
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		glog.Errorf("Couldn't parse response body, err: %v", err)
		return err
	}
	return err
}
