// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
)

const (
	Objstateattr = "user.objstate" // Object's state
	MD5attr      = "user.MD5"      // Object's MD5
	XAttrInvalid = "0"
	XAttrValid   = "1"
)

const directput = 5

// TODO: AWS specific initialization
type awsif struct {
}

// TODO: GCP specific initialization
type gcpif struct {
}

type cinterface interface {
	listbucket(w http.ResponseWriter, bucket string, msg *GetMsg) error
	getobj(w http.ResponseWriter, fqn, bucket, objname string) (file *os.File, err error)
	putobj(r *http.Request, w http.ResponseWriter, fqn, bucket, objname, md5sum string) error
}

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif   cinterface // multi-cloud vendor support
	smap      *Smap
	xactinp   *xactInProgress
	starttime time.Time
	lbmap     *lbmap
}

// start target runner
func (t *targetrunner) run() error {
	// init
	t.httprunner.init(getstorstats())
	t.smap = &Smap{}
	t.xactinp = newxactinp()
	// local (cache-only) buckets
	t.lbmap = &lbmap{LBmap: make(map[string]string)}

	if err := t.register(); err != nil {
		glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
		if match, _ := regexp.MatchString("connection refused", err.Error()); match {
			glog.Errorf("Target %s: retrying registration ...", t.si.DaemonID)
			time.Sleep(time.Second * 3)
			if err = t.register(); err != nil {
				glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
				glog.Errorf("Target %s is terminating", t.si.DaemonID)
				return err
			}
			glog.Errorf("Success: target %s registered OK", t.si.DaemonID)
		} else {
			return err
		}
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
	if ctx.config.LegacyMode {
		glog.Infof("DFC Target is running in legacy Mode")
	}
	//
	// REST API: register storage target's handler(s) and start listening
	//
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", t.filehdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, t.daemonhdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon+"/", t.daemonhdlr) // FIXME
	t.httprunner.registerhdlr("/", invalhdlr)
	glog.Infof("Target %s is ready", t.si.DaemonID)
	glog.Flush()
	t.starttime = time.Now()
	return t.httprunner.run()
}

// stop gracefully
func (t *targetrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.name, err)
	sleep := t.xactinp.abortAll()
	t.unregister()
	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
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

// "/"+Rversion+"/"+Rfiles+"/"+bucket [+"/"+objname]
//
// checks if the object exists locally (if not, downloads it)
// and sends it back via http
func (t *targetrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname := apitems[0], ""
	if len(apitems) > 1 {
		objname = apitems[1]
	}
	if strings.Contains(bucket, "/") {
		s := fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		invalmsghdlr(w, r, s)
		t.statsif.add("numerr", 1)
		return
	}
	//
	// list the bucket and return
	//
	if len(objname) == 0 {
		t.statsif.add("numlist", 1)
		var msg GetMsg
		t.readJSON(w, r, &msg)
		getcloudif().listbucket(w, bucket, &msg)
		return
	}
	//
	// get the object from the bucket
	//
	t.statsif.add("numget", 1)
	fqn := t.fqn(bucket, objname)
	var file *os.File
	_, err := os.Stat(fqn)
	if os.IsNotExist(err) || isinvalidobj(fqn) {
		t.statsif.add("numcoldget", 1)
		glog.Infof("Bucket %s key %s fqn %q is not cached or invalid", bucket, objname, fqn)
		// TODO: do getcloudif().getobj() and write http response in parallel
		if file, err = getcloudif().getobj(w, fqn, bucket, objname); err != nil {
			return
		}
		//
		// NOTE: alternatively: close it, handle errors, and if ok reopen for read
		//
		ret, err := file.Seek(0, 0)
		assert(ret == 0)
		assert(err == nil)
	} else if file, err = os.Open(fqn); err != nil {
		s := fmt.Sprintf("Failed to open local file %q, err: %v", fqn, err)
		t.statsif.add("numerr", 1)
		invalmsghdlr(w, r, s)
		return
	}
	defer file.Close()
	// NOTE: copyBuffer()
	written, err := copyBuffer(w, file)
	if err != nil {
		glog.Errorf("Failed to copy %q to http, err: %v", fqn, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		t.statsif.add("numerr", 1)
	} else if glog.V(3) {
		glog.Infof("Copied %q to http(%.2f MB)", fqn, float64(written)/1000/1000)
	}
	glog.Flush()
}

// "/"+Rversion+"/"+Rfiles+"/"+"from_id"+"/"+ID+"to_id"+"/"+bucket+"/"+objname
func (t *targetrunner) httpfilput(w http.ResponseWriter, r *http.Request) {
	var (
		s, fqn, from, to, bucket, objname string
		err                               error
		finfo                             os.FileInfo
		written                           int64
	)
	apitems := t.restAPIItems(r.URL.Path, 9)
	if len(apitems) == directput {
		if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
			return
		}
		bucket, objname = apitems[0], ""
		if len(apitems) > 1 {
			objname = strings.Join(apitems[1:], "/")
		}
		fqn = t.fqn(bucket, objname)

		md5 := r.Header.Get("Content-MD5")
		assert(md5 != "")
		if err = getcloudif().putobj(r, w, fqn, bucket, objname, md5); err != nil {
			return
		}

	} else {

		if apitems = t.checkRestAPI(w, r, apitems, 6, Rversion, Rfiles); apitems == nil {
			return
		}
		if apitems[0] != Rfrom || apitems[2] != Rto {
			s = fmt.Sprintf("File copy: missing %s and/or %s in the URL: %+v", Rfrom, Rto, apitems)
			goto merr
		}
		from, to, bucket, objname = apitems[1], apitems[3], apitems[4], apitems[5]
		if glog.V(3) {
			glog.Infof("File copy: from %q bucket %q objname %q => to %q", from, bucket, objname, to)
		}
		if t.si.DaemonID != from && t.si.DaemonID != to {
			s = fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
				t.si.DaemonID, from, to)
			goto merr
		}
		fqn = t.fqn(bucket, objname)
		finfo, err = os.Stat(fqn)
		if t.si.DaemonID == from {
			//
			// the source
			//
			if os.IsNotExist(err) {
				s = fmt.Sprintf("File copy: %s does not exist at the source %s", fqn, t.si.DaemonID)
				goto merr
			}
			si, ok := t.smap.Smap[to]
			if !ok {
				s = fmt.Sprintf("File copy: unknown destination %s (do syncsmap?)", to)
				goto merr
			}
			if s = t.sendfile(r.Method, bucket, objname, si); s != "" {
				goto merr
			}
			if glog.V(3) {
				glog.Infof("Sent %q to %s (size %.2f MB)", fqn, to, float64(finfo.Size())/1000/1000)
			}
		} else {
			//
			// the destination
			//
			if os.IsExist(err) {
				s = fmt.Sprintf("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
				return // not an error, nothing to do
			}

			md5 := r.Header.Get("Content-MD5")
			assert(md5 != "", err)
			if written, err = ReceiveFile(fqn, r.Body, md5); err != nil {
				s = fmt.Sprintf("File copy: failed to receive %q from %s", fqn, from)
				goto merr
			}
			if glog.V(3) {
				glog.Infof("Received %q from %s (size %.2f MB)", fqn, from, float64(written)/1000/1000)
			}
			t.statsif.add("numrecvfile", 1)
		}
	}
	return
merr:
	t.statsif.add("numerr", 1)
	invalmsghdlr(w, r, s)
}

func (t *targetrunner) sendfile(method, bucket, objname string, destsi *daemonInfo) string {
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	fqn := t.fqn(bucket, objname)
	url := destsi.DirectURL + "/" + Rversion + "/" + Rfiles + "/"
	url += Rfrom + "/" + fromid + "/" + Rto + "/" + toid + "/"
	url += bucket + "/" + objname
	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()
	// calculate MD5 for file and send it as part of header
	md5, errstr := CalculateMD5(file)
	if errstr != "" {
		return fmt.Sprintf("Failed to CalulateMD5, err: %v", errstr)
	}

	request, err := http.NewRequest(method, url, file)
	assert(err == nil, err)
	request.Header.Set("Content-MD5", md5)
	response, err := t.httpclient.Do(request)
	if err != nil {
		return fmt.Sprintf("Failed to copy %q from %s, err: %v", t.si.DaemonID, fqn, err)
	}
	if response != nil {
		ioutil.ReadAll(response.Body)
		response.Body.Close()
	}
	t.statsif.add("numsendfile", 1)
	return ""
}

//
// Cloud bucket + object => (local hashed path, fully qualified filename)
//
func (t *targetrunner) fqn(bucket, objname string) string {
	mpath := hrwMpath(bucket + "/" + objname)
	assert(len(mpath) > 0) // FIXME; see mountpath.enabled
	return mpath + "/" + bucket + "/" + objname
}

func (t *targetrunner) splitfqn(fqn string) (mpath, bucket, objname string) {
	for path := range ctx.mountpaths {
		if !strings.HasPrefix(fqn, path+"/") {
			continue
		}
		bo := fqn[len(path+"/"):]
		b_o := strings.SplitN(bo, "/", 2)
		mpath, bucket, objname = path, b_o[0], b_o[1]
		return
	}
	assert(false)
	return
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
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	// PUT '{Smap}' /v1/daemon/(syncsmap|rebalance)
	if len(apitems) > 0 && (apitems[0] == Rsyncsmap || apitems[0] == Rebalance) {
		t.httpdaeput_smap(w, r, apitems)
		return
	}
	// PUT '{lbmap}' /v1/daemon/localbuckets
	if len(apitems) > 0 && apitems[0] == Rsynclb {
		t.readJSON(w, r, t.lbmap)
		glog.Infof("lbmap: %+v", t.lbmap)
		return
	}
	var msg ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActShutdown:
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		invalmsghdlr(w, r, s)
	}
}
func (t *targetrunner) httpdaeput_smap(w http.ResponseWriter, r *http.Request, apitems []string) {
	curversion := t.smap.Version
	var smap *Smap
	if t.readJSON(w, r, &smap) != nil {
		return
	}
	if curversion == smap.Version {
		return
	}
	if curversion > smap.Version {
		glog.Errorf("Warning: attempt to downgrade Smap verion %d to %d", curversion, smap.Version)
		return
	}
	glog.Infof("%s: got new version %d (old %d)", apitems[0], smap.Version, curversion)
	var existentialQ bool // to assert
	for id, si := range smap.Smap {
		if id == t.si.DaemonID {
			existentialQ = true
			glog.Infoln("target:", si, "<= self")
		} else {
			glog.Infoln("target:", si)
		}
	}
	glog.Flush()
	assert(existentialQ)
	t.smap = smap
	if apitems[0] == Rsyncsmap {
		// FIXME: must trigger delayed xactRebalance (TODO)
		return
	}
	// xaction
	go t.runRebalance()
}

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	var msg GetMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	var (
		jsbytes []byte
		err     error
	)
	switch msg.GetWhat {
	case GetWhatConfig:
		jsbytes, err = json.Marshal(t.si)
		assert(err == nil, err)
	case GetWhatStats:
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
