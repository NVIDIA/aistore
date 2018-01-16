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
	smap    *Smap
}

// start target runner
func (t *targetrunner) run() error {
	// init
	t.httprunner.init(getstorstats())
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

// "/"+Rversion+"/"+Rfiles
func (t *targetrunner) filehdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpfilget(w, r)
	case http.MethodPut:
		t.httpfilput(w, r)
	default:
		invalhdlr(w, r)
	}
}

// "/"+Rversion+"/"+Rfiles+"/"+bucket [+"/"+fname]
// checks if the object exists locally (if not, downloads it)
// and sends it back via http
func (t *targetrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restApiItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname := apitems[0], ""
	if len(apitems) > 1 {
		objname = apitems[1]
	}
	t.statsif.add("numget", 1)
	//
	// list the bucket and return
	//
	if len(objname) == 0 {
		getcloudif().listbucket(w, bucket)
		return
	}
	//
	// get from the bucket
	//
	fqn := t.fqn(bucket, objname)
	_, err := os.Stat(fqn)
	if os.IsNotExist(err) {
		t.statsif.add("numcoldget", 1)
		glog.Infof("Bucket %s key %s fqn %q is not cached", bucket, objname, fqn)
		//
		// TODO: do getcloudif().getobj() and write http response in parallel
		//
		if err = getcloudif().getobj(w, fqn, bucket, objname); err != nil {
			return
		}
	} else if glog.V(2) {
		glog.Infof("Bucket %s key %s fqn %q is cached", bucket, objname, fqn)
	}
	file, err := os.Open(fqn)
	if err != nil {
		glog.Errorf("Failed to open %q, err: %v", fqn, err)
		checksetmounterror(fqn)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		t.statsif.add("numerr", 1)
	} else {
		defer file.Close()
		// NOTE: the following copyBuffer() call is equaivalent to:
		// 	rt, _ := w.(io.ReaderFrom)
		// 	written, err := rt.ReadFrom(file) ==> sendfile path
		written, err := copyBuffer(w, file)
		if err != nil {
			glog.Errorf("Failed to copy %q to http response, err: %v", fqn, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			t.statsif.add("numerr", 1)
		} else if glog.V(3) {
			glog.Infof("Copied %q to http(%.2f MB)", fqn, float64(written)/1000/1000)
		}
	}
	glog.Flush()
}

// '{CopyMsg}' "/"+Rversion+"/"+Rfiles+"/"+bucket+"/"+fname
func (t *targetrunner) httpfilput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restApiItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	var msg CopyMsg
	if t.readJson(w, r, &msg) != nil {
		return
	}
	if t.si.DaemonID != msg.FromID && t.si.DaemonID != msg.ToID {
		s := fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
			t.si.DaemonID, msg.FromID, msg.ToID)
		t.statsif.add("numerr", 1)
		invalmsghdlr(w, r, s)
		return
	}

	t.statsif.add("numput", 1)

	glog.Infoln(bucket, objname)
}

//
// Cloud bucket + object => (local hashed path, fully qualified filename)
//
func (t *targetrunner) fqn(bucket, objname string) string {
	mpath := hrwMpath(bucket + "/" + objname)
	assert(len(mpath) > 0) // FIXME; see mountpath.enabled
	return mpath + "/" + bucket + "/" + objname
}

//===========================
//
// control plane
//
//===========================

// "/"+Rversion+"/"+Rdaemon
func (t *targetrunner) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpdaeget(w, r)
	case http.MethodPut:
		t.httpdaeput(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

func (t *targetrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restApiItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	// PUT '{Smap}' /v1/daemon/syncsmap => target(s)
	if len(apitems) > 0 && apitems[0] == Rsyncsmap {
		curversion := t.smap.Version
		var smap *Smap
		if t.readJson(w, r, &smap) != nil {
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
	if t.readJson(w, r, &msg) != nil {
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

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restApiItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	var msg GetMsg
	if t.readJson(w, r, &msg) != nil {
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
