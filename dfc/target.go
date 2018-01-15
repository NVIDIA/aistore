/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"syscall"

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

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif cinterface // multi-cloud vendor support
	stats   *Storstats
	smap    *Smap
}

// start target runner
func (t *targetrunner) run() error {
	// init
	t.httprunner.init()
	t.stats = getstorstats() // TODO consistency: use instead of getstorstats()
	t.smap = &Smap{}

	// FIXME cleanup unreg
	if err := t.register(); err != nil {
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
		t.cloudif = &awsif{}

	} else {
		t.cloudif = &gcpif{}
	}
	//
	// REST API: register storage target's handler(s) and start listening
	//
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", t.filehdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, t.daemonhdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon+"/", t.daemonhdlr) // FIXME
	t.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Storage target is ready, ID=%s", t.si.DaemonID)
	return t.httprunner.run()
}

// stop gracefully
func (t *targetrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.name, err)
	t.unregister()
	t.httprunner.stop(err)
}

// target registration with proxy
func (t *targetrunner) register() error {
	jsbytes, err := json.Marshal(t.si)
	if err != nil {
		glog.Errorf("Unexpected failure to json-marshal %+v, err: %v", t.si, err)
		return err
	}
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	_, err = t.call(url, http.MethodPost, jsbytes)
	return err
}

func (t *targetrunner) unregister() error {
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	url += "/" + Rdaemon + "/" + t.si.DaemonID
	_, err := t.call(url, http.MethodDelete, nil)
	return err
}

//==============
//
// http handlers
//
//==============

// handler for: "/"+Rversion+"/"+Rfiles+"/"
// checks if the named fobject; if not, downloads it and (always)
// sends it back via http
func (t *targetrunner) filehdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodGet) // TODO
	//
	// parse and validate REST API
	//
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		statsAdd(&t.stats.Numerr, 1)
		return
	}
	bktname, keyname := apitems[0], ""
	if len(apitems) > 1 {
		keyname = apitems[1]
	}
	statsAdd(&t.stats.Numget, 1)
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
		statsAdd(&t.stats.Numcoldget, 1)
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
		statsAdd(&t.stats.Numerr, 1)
	} else {
		defer file.Close()
		// NOTE: the following copyBuffer() call is equaivalent to:
		// 	rt, _ := w.(io.ReaderFrom)
		// 	written, err := rt.ReadFrom(file) ==> sendfile path
		written, err := copyBuffer(w, file)
		if err != nil {
			glog.Errorf("Failed to copy %q to http response, err: %v", fname, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			statsAdd(&t.stats.Numerr, 1)
		} else if glog.V(3) {
			glog.Infof("Copied %q to http(%.2f MB)", fname, float64(written)/1000/1000)
		}
	}
	glog.Flush()
}

// handler for: "/"+Rversion+"/"+Rdaemon
func (t *targetrunner) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpget(w, r)
	case http.MethodPut:
		t.httpput(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

func (t *targetrunner) httpput(w http.ResponseWriter, r *http.Request) {
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		statsAdd(&t.stats.Numerr, 1)
		return
	}
	// PUT '{Smap}' /v1/daemon/syncsmap => target(s)
	if len(apitems) > 0 && apitems[0] == Rsyncsmap {
		curversion := t.smap.Version
		var smap *Smap
		if readJson(w, r, &smap) != nil {
			return
		}
		if curversion < smap.Version {
			glog.Infof("syncsmap: new version %d (old %d) - rebalance?", smap.Version, curversion)
			for id, si := range smap.Smap {
				if id == t.si.DaemonID {
					glog.Infoln("target:", si, "<= self")
				} else {
					glog.Infoln("target:", si)
				}
			}
			glog.Flush()
			t.smap = smap
		}
		return
	}

	var msg ActionMsg
	if readJson(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActionShutdown:
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) httpget(w http.ResponseWriter, r *http.Request) {
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		statsAdd(&t.stats.Numerr, 1)
		return
	}
	var msg GetMsg
	if readJson(w, r, &msg) != nil {
		return
	}
	var (
		jsbytes []byte
		err     error
	)
	switch msg.What {
	case GetConfig:
		jsbytes, err = json.Marshal(t.si)
		assert(err == nil, err)
	case GetStats:
		var stats Storstats
		getstorstatsrunner().syncstats(&stats)
		jsbytes, err = json.Marshal(stats)
		assert(err == nil, err)
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsbytes)
}
