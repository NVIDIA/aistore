// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
)

// xattrs
const (
	HashAttr     = "user.obj.dfchash" // Object's XXHash
	XAttrInvalid = "0"
	XAttrValid   = "1"
)

const DFC_CONTENT_HASH = "Content-HASH"

type mountPath struct {
	Path string
	Fsid syscall.Fsid
}

type fipair struct {
	relname string
	os.FileInfo
}
type allfinfos struct {
	finfos     []fipair
	rootLength int
}

//===========================================================================
//
// target runner
//
//===========================================================================
type targetrunner struct {
	httprunner
	cloudif    cloudif // multi-cloud vendor support
	smap       *Smap
	proxysi    *daemonInfo
	xactinp    *xactInProgress
	starttime  time.Time
	lbmap      *lbmap
	rtnamemap  *rtnamemap
	buffers    *buffers // 128K default
	buffers32k *buffers // 32K
	buffers4k  *buffers // 4K
}

// start target runner
func (t *targetrunner) run() error {
	t.httprunner.init(getstorstats())
	t.httprunner.kalive = gettargetkalive()
	t.smap = &Smap{}                                 // cluster map
	t.xactinp = newxactinp()                         // extended actions
	t.lbmap = &lbmap{LBmap: make(map[string]string)} // local (cache-only) buckets
	t.rtnamemap = newrtnamemap(128)                  // lock/unlock name
	t.buffers = newbuffers(128 * 1024)
	t.buffers32k = newbuffers(32 * 1024)
	t.buffers4k = newbuffers(4 * 1024)

	if err, status := t.register(); err != nil {
		glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
		if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout { // AA: use status
			glog.Errorf("Target %s: retrying registration...", t.si.DaemonID)
			time.Sleep(time.Second * 3)
			if err, status = t.register(); err != nil {
				glog.Errorf("Target %s failed to register with proxy, err: %v", t.si.DaemonID, err)
				glog.Errorf("Target %s is terminating", t.si.DaemonID)
				return err
			}
			glog.Errorf("Success: target %s joined the cluster", t.si.DaemonID)
		} else {
			return err
		}
	}
	// fill-in mpaths
	ctx.mountpaths = make(map[string]*mountPath, len(ctx.config.FSpaths))
	if t.testingFSPpaths() {
		glog.Infof("Warning: configuring %d fspaths for testing", ctx.config.TestFSP.Count)
		t.testCachepathMounts()
	} else {
		t.fspath2mpath()
		t.mpath2Fsid() // enforce FS uniqueness
	}
	for mpath := range ctx.mountpaths {
		cloudbctsfqn := mpath + "/" + ctx.config.CloudBuckets
		if err := CreateDir(cloudbctsfqn); err != nil {
			glog.Fatalf("FATAL: cannot create cloud buckets dir %q, err: %v", cloudbctsfqn, err)
		}
		localbctsfqn := mpath + "/" + ctx.config.LocalBuckets
		if err := CreateDir(localbctsfqn); err != nil {
			glog.Fatalf("FATAL: cannot create local buckets dir %q, err: %v", localbctsfqn, err)
		}
	}

	// cloud provider
	if ctx.config.CloudProvider == amazoncloud {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		assert(ctx.config.CloudProvider == googlecloud)
		t.cloudif = &gcpimpl{t}
	}
	if ctx.config.NoXattrs {
		glog.Infof("Warning: running with xattrs disabled")
		glog.Flush()
	}
	// init capacity
	rr := getstorstatsrunner()
	rr.initCapacity()
	//
	// REST API: register storage target's handler(s) and start listening
	//
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", t.filehdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon, t.daemonhdlr)
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rdaemon+"/", t.daemonhdlr) // FIXME
	t.httprunner.registerhdlr("/"+Rversion+"/"+Rpush+"/", t.pushhdlr)
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
	close(t.rtnamemap.abrt)
	if t.httprunner.h != nil {
		t.unregister() // ignore errors
	}
	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
}

// target registration with proxy
func (t *targetrunner) register() (err error, status int) {
	jsbytes, err := json.Marshal(t.si)
	if err != nil {
		return fmt.Errorf("Unexpected failure to json-marshal %+v, err: %v", t.si, err), 0
	}
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	_, err, _, status = t.call(t.proxysi, url, http.MethodPost, jsbytes)
	return
}

func (t *targetrunner) unregister() (err error, status int) {
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	url += "/" + Rdaemon + "/" + t.si.DaemonID
	_, err, _, status = t.call(t.proxysi, url, http.MethodDelete, nil)
	return
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
	case http.MethodDelete:
		t.httpfildelete(w, r)
	case http.MethodPost:
		t.httpfilrename(w, r)
	default:
		invalhdlr(w, r)
	}
}

// "/"+Rversion+"/"+Rfiles+"/"+bucket [+"/"+objname]
//
// checks if the object exists locally (if not, downloads it)
// and sends it back via http
func (t *targetrunner) httpfilget(w http.ResponseWriter, r *http.Request) {
	var (
		md5hash                             string
		coldget, exclusive                  bool
		bucket, objname, fqn, uname, errstr string
		size                                int64
		errcode                             int
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname, errstr = apitems[0], "", ""
	if len(apitems) > 1 {
		objname = apitems[1]
	}
	if strings.Contains(bucket, "/") {
		errstr = fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	//
	// list the bucket and return
	//
	if len(objname) == 0 {
		t.listbucket(w, r, bucket)
		return
	}
	//
	// serialize on the name
	//
	fqn, uname, exclusive = t.fqn(bucket, objname), bucket+objname, true
	t.rtnamemap.lockname(uname, exclusive, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer func(locktype *bool) { t.rtnamemap.unlockname(uname, *locktype) }(&exclusive)

	//
	// get the object from the bucket
	//
	if coldget, size, errstr = t.getchecklocal(w, r, bucket, objname, fqn); errstr != "" {
		return
	}
	if coldget {
		if md5hash, size, errstr, errcode = getcloudif().getobj(fqn, bucket, objname); errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
		t.statsif.add("numcoldget", 1)
	}
	//
	// downgrade lock(name)
	//
	exclusive = false
	t.rtnamemap.downgradelock(uname)
	//
	// local file => http response
	//
	file, err := os.Open(fqn)
	if err != nil {
		if os.IsPermission(err) {
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
			t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
		} else {
			errstr = fmt.Sprintf("Failed to open local file %s, err: %v", fqn, err)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
		}
		return
	}

	defer file.Close()
	if !coldget {
		md5binary, errstr := Getxattr(fqn, HashAttr)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		md5hash = string(md5binary)
	}
	if md5hash != "" {
		w.Header().Add(DFC_CONTENT_HASH, md5hash)
	}
	// buffer pooling
	var buffs buffif
	if size > t.buffers32k.fixedsize {
		buffs = t.buffers
	} else if size > t.buffers4k.fixedsize {
		buffs = t.buffers32k
	} else {
		assert(size != 0, "Unexpected: zero size "+fqn)
		buffs = t.buffers4k
	}
	buf := buffs.alloc()
	defer buffs.free(buf)
	// copy
	written, err := io.CopyBuffer(w, file, buf)
	if err != nil {
		errstr = fmt.Sprintf("Failed to send file %s, err: %v", fqn, err)
		t.invalmsghdlr(w, r, errstr)
	} else if glog.V(3) {
		glog.Infof("GET: sent %s (%.2f MB)", fqn, float64(written)/1000/1000)
	}
	t.statsif.add("numget", 1)
}

func (t *targetrunner) getchecklocal(w http.ResponseWriter, r *http.Request, bucket, objname, fqn string) (coldget bool, size int64, errstr string) {
	finfo, err := os.Stat(fqn)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			if t.islocalBucket(bucket) {
				errstr = fmt.Sprintf("GET local: file %s (local bucket %s, object %s) does not exist",
					fqn, bucket, objname)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return
			}
			coldget = true
		case os.IsPermission(err):
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", fqn)
			t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
			return
		default:
			errstr = fmt.Sprintf("Failed to fstat %s, err: %v", fqn, err)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			return
		}
	} else {
		size = finfo.Size()
	}
	return
}

// "/"+Rversion+"/"+Rpush+"/"+bucket
func (t *targetrunner) pushhdlr(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, "push"); apitems == nil {
		return
	}
	bucket := apitems[0]

	if strings.Contains(bucket, "/") {
		s := fmt.Sprintf("Invalid bucket name %s (contains '/')", bucket)
		t.invalmsghdlr(w, r, s)
		t.statsif.add("numerr", 1)
		return
	}

	if pusher, ok := w.(http.Pusher); ok {
		objnamebytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			s := fmt.Sprintf("Could not read Request Body: %v", err)
			t.invalmsghdlr(w, r, s)
			t.statsif.add("numerr", 1)
			return
		}

		objnames := make([]string, 0)
		err = json.Unmarshal(objnamebytes, &objnames)
		if err != nil {
			s := fmt.Sprintf("Could not unmarshal objnames: %v", err)
			t.invalmsghdlr(w, r, s)
			t.statsif.add("numerr", 1)
			return
		}

		for _, objname := range objnames {
			err := pusher.Push("/v1/files/"+bucket+"/"+objname, nil)
			if err != nil {
				t.invalmsghdlr(w, r, "Error Pushing "+"/v1/files/"+bucket+"/"+objname+": "+err.Error())
				return
			}
		}
	} else {
		t.invalmsghdlr(w, r, "Pusher Unavailable - could not push files.")
		return
	}

	w.Write([]byte("Pushed Object List "))
}

func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string) {
	msg := &GetMsg{}
	if t.readJSON(w, r, msg) != nil {
		return
	}
	if !t.islocalBucket(bucket) {
		jsbytes, errstr, errcode := getcloudif().listbucket(bucket, msg)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
		} else {
			if errstr := t.writeJSON(w, r, jsbytes, "listbucket"); errstr == "" {
				t.statsif.add("numlist", 1)
			}
		}
		return
	}
	// local bucket
	allfinfos := allfinfos{make([]fipair, 0, 128), 0}
	for mpath := range ctx.mountpaths {
		localbucketfqn := mpath + "/" + ctx.config.LocalBuckets + "/" + bucket
		allfinfos.rootLength = len(localbucketfqn)
		if err := filepath.Walk(localbucketfqn, allfinfos.listwalkf); err != nil {
			glog.Errorf("Failed to traverse mpath %q, err: %v", mpath, err)
		}
	}
	t.statsif.add("numlist", 1)
	var reslist = BucketList{Entries: make([]*BucketEntry, 0, len(allfinfos.finfos))}
	for _, fi := range allfinfos.finfos {
		if msg.GetPrefix != "" && !strings.HasPrefix(fi.relname, msg.GetPrefix) {
			continue
		}

		entry := &BucketEntry{}
		entry.Name = fi.relname

		if strings.Contains(msg.GetProps, GetPropsSize) {
			entry.Size = fi.Size()
		}
		if strings.Contains(msg.GetProps, GetPropsCtime) {
			t := fi.ModTime()
			switch msg.GetTimeFormat {
			case "":
				fallthrough
			case RFC822:
				entry.Ctime = t.Format(time.RFC822)
			default:
				entry.Ctime = t.Format(msg.GetTimeFormat)
			}
		}
		if strings.Contains(msg.GetProps, GetPropsChecksum) {
			fqn := t.fqn(bucket, fi.relname)
			md5hex, errstr := Getxattr(fqn, HashAttr)
			if errstr != "" {
				glog.Infoln(errstr)
			} else {
				md5 := hex.EncodeToString(md5hex)
				entry.Checksum = md5
			}
		}
		// TODO: other GetMsg props TBD
		reslist.Entries = append(reslist.Entries, entry)
	}
	jsbytes, err := json.Marshal(reslist)
	assert(err == nil, err)
	_ = t.writeJSON(w, r, jsbytes, "listbucket")
}

func (all *allfinfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("listwalkf callback invoked with err: %v", err)
		return err
	}
	if osfi.IsDir() {
		// Listbucket doesn't need to return directories
		return nil
	}
	relname := fqn[all.rootLength:]
	all.finfos = append(all.finfos, fipair{relname, osfi})
	return nil
}

func (t *targetrunner) httpfilput(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 9)
	if len(apitems) > 4 && apitems[2] == Rfrom || apitems[4] == Rto {
		//
		// Rebalance: "/"+Rversion+"/"+Rfiles+"/"+"from_id"+"/"+ID+"to_id"+"/"+bucket+"/"+objname
		//
		if apitems = t.checkRestAPI(w, r, apitems, 6, Rversion, Rfiles); apitems == nil {
			return
		}
		from, to, bucket, objname := apitems[1], apitems[3], apitems[4], apitems[5]
		if len(apitems) > 6 {
			objname = strings.Join(apitems[5:], "/")
		}
		size, errstr := t.dorebalance(r, from, to, bucket, objname)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		t.statsif.add("numrecvfiles", 1)
		t.statsif.add("numrecvbytes", size)
	} else {
		// PUT: "/"+Rversion+"/"+Rfiles+"/"+bucket+"/"+objname
		if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
			return
		}
		bucket, objname := apitems[0], ""
		if len(apitems) > 1 {
			objname = strings.Join(apitems[1:], "/")
		}
		errstr, errcode := t.doput(w, r, bucket, objname)
		if errstr != "" {
			if errcode == 0 {
				t.invalmsghdlr(w, r, errstr)
			} else {
				t.invalmsghdlr(w, r, errstr, errcode)
			}
			return
		}
		t.statsif.add("numput", 1)
	}
	return
}

func (t *targetrunner) doput(w http.ResponseWriter, r *http.Request, bucket string, objname string) (errstr string, errcode int) {
	var (
		err     error
		file    *os.File
		md5hash string
		hdhash  string
	)
	fqn := t.fqn(bucket, objname)
	putfqn := fmt.Sprintf("%s.%d", fqn, time.Now().UnixNano())
	hdhash = r.Header.Get("Content-HASH")
	if hdhash == "" {
		glog.Infof("Warning: empty Content-HASH")
	}

	defer r.Body.Close()
	glog.Infof("PUT: %s => %s", fqn, putfqn)

	// optimize out if the checksums do match
	if hdhash != "" {
		file, err := os.Open(fqn)
		if err == nil {
			md5 := md5.New()
			buf := t.buffers.alloc()
			md5hash, _ = ComputeFileMD5(file, buf, md5)
			t.buffers.free(buf)
			// not a critical error
			if err = file.Close(); err != nil {
				glog.Warningf("Unexpected failure to close %s once md5-ed, err: %v", fqn, err)
			}
			if md5hash == hdhash {
				glog.Infof("Existing %s is valid: new PUT is a no-op", fqn)
				return
			}
		} else {
			glog.Warningf("Unexpected failure to open local valid %s, err: %v", fqn, err)
		}
	}
	if md5hash, _, errstr = t.receiveFileAndFinalize(putfqn, objname, hdhash, r.Body); errstr != "" {
		return
	}
	// putfqn is received
	defer func() {
		if errstr != "" {
			if err = os.Remove(putfqn); err != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, putfqn, err)
			}
		}
	}()

	if hdhash != "" && md5hash != "" && md5hash != hdhash {
		errstr = fmt.Sprintf("Invalid checksum: %s md5 %s... does not match user's %s...", fqn, md5hash[:8], hdhash[:8])
		return
	}
	if !t.islocalBucket(bucket) {
		if file, err = os.Open(putfqn); err != nil {
			errstr = fmt.Sprintf("Failed to re-open %s err: %v", putfqn, err)
			return
		}
		if errstr, errcode = getcloudif().putobj(file, bucket, objname); errstr != "" {
			_ = file.Close()
			return
		}
		if err = file.Close(); err != nil {
			// not failing as the cloud part is done
			glog.Errorf("Unexpected failure to close an already PUT file %s, err: %v", putfqn, err)
		}
	}
	// serialize on the name - and rename
	errstr = t.safeRename(bucket, objname, putfqn, fqn)
	return
}

func (t *targetrunner) safeRename(bucket, objname, putfqn, fqn string) (errstr string) {
	uname := bucket + objname
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	if err := os.Rename(putfqn, fqn); err != nil {
		errstr = fmt.Sprintf("Unexpected failure to rename %s => %s, err: %v", putfqn, fqn, err)
	} else {
		glog.Infof("PUT done: %s <= %s", fqn, putfqn)
	}
	t.rtnamemap.unlockname(uname, true)
	return
}

func (t *targetrunner) dorebalance(r *http.Request, from, to, bucket, objname string) (size int64, errstr string) {
	if t.si.DaemonID != from && t.si.DaemonID != to {
		errstr = fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
			t.si.DaemonID, from, to)
		return
	}
	fqn := t.fqn(bucket, objname)

	if t.si.DaemonID == from {
		//
		// the source
		//
		uname := bucket + objname
		t.rtnamemap.lockname(uname, false, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
		defer t.rtnamemap.unlockname(uname, false)

		finfo, err := os.Stat(fqn)
		if glog.V(3) {
			glog.Infof("Rebalance from %q: bucket %q objname %q => to %q", from, bucket, objname, to)
		}
		if err != nil && os.IsNotExist(err) {
			errstr = fmt.Sprintf("File copy: %s does not exist at the source %s", fqn, t.si.DaemonID)
			return
		}
		si, ok := t.smap.Smap[to]
		if !ok {
			errstr = fmt.Sprintf("File copy: unknown destination %s (do syncsmap?)", to)
			return
		}
		size = finfo.Size()
		if errstr = t.sendfile(r.Method, bucket, objname, si, size, ""); errstr != "" {
			return
		}
		if glog.V(3) {
			glog.Infof("Sent %q to %s (size %.2f MB)", fqn, to, float64(finfo.Size())/1000/1000)
		}
	} else {
		//
		// the destination
		//
		if glog.V(3) {
			glog.Infof("Rebalance to %q: bucket %q objname %q <= from %q", to, bucket, objname, from)
		}
		putfqn := fmt.Sprintf("%s.%d", fqn, time.Now().UnixNano())
		_, err := os.Stat(fqn)
		if err != nil && os.IsExist(err) {
			glog.Infof("File copy: %s already exists at the destination %s", fqn, t.si.DaemonID)
			return // not an error, nothing to do
		}
		// write file with extended attributes .
		if _, size, errstr = t.receiveFileAndFinalize(putfqn, objname, "", r.Body); errstr != "" {
			return
		}
		errstr = t.safeRename(bucket, objname, putfqn, fqn)
	}
	return
}

func (t *targetrunner) httpfildelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		msg                     ActionMsg
		evict                   bool
		errcode                 int
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname = apitems[0], ""
	if len(apitems) > 1 {
		objname = strings.Join(apitems[1:], "/")
	}
	fqn := t.fqn(bucket, objname)
	uname := bucket + objname
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if !t.islocalBucket(bucket) {
		b, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err == nil && len(b) > 0 {
			err = json.Unmarshal(b, &msg)
			if err == nil {
				evict = (msg.Action == ActEvict)
			}
		}
		if !evict {
			errstr, errcode = getcloudif().deleteobj(bucket, objname)
			t.statsif.add("numdelete", 1)
		}
	}
	if errstr != "" {
		if errcode == 0 {
			t.invalmsghdlr(w, r, errstr)
		} else {
			t.invalmsghdlr(w, r, errstr, errcode)
		}
		return
	}
	finfo, err := os.Stat(fqn)
	if err != nil {
		if os.IsNotExist(err) && t.islocalBucket(bucket) && !evict {
			errstr = fmt.Sprintf("DELETE local: file %s (local bucket %s, object %s) does not exist",
				fqn, bucket, objname)
		}
	} else {
		if err := os.Remove(fqn); err != nil {
			errstr = err.Error()
		} else if evict {
			t.statsif.add("bytesevicted", finfo.Size())
			t.statsif.add("filesevicted", 1)
		}
	}
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
}

func (t *targetrunner) httpfilrename(w http.ResponseWriter, r *http.Request) {
	var (
		msg    ActionMsg
		errstr string
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 2, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket, objname := apitems[0], strings.Join(apitems[1:], "/")
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	if msg.Action != ActRename {
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
		return
	}
	newobjname := msg.Name
	fqn, uname := t.fqn(bucket, objname), bucket+objname
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	finfo, err := os.Stat(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (local bucket %s, object %s), err: %v",
			fqn, bucket, objname, err)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	si := hrwTarget(bucket+"/"+newobjname, t.smap)
	// local rename
	if si.DaemonID == t.si.DaemonID {
		newfqn := t.fqn(bucket, newobjname)
		dirname := filepath.Dir(newfqn)
		if err := CreateDir(dirname); err != nil {
			errstr = fmt.Sprintf("Unexpected failure to create local dir %s, err: %v", dirname, err)
			t.invalmsghdlr(w, r, errstr)
		} else if err := os.Rename(fqn, newfqn); err != nil {
			errstr = fmt.Sprintf("Failed to rename %s => %s, err: %v", fqn, newfqn, err)
			t.invalmsghdlr(w, r, errstr)
		} else {
			t.statsif.add("numrename", 1)
			if glog.V(3) {
				glog.Infof("Renamed %s => %s", fqn, newfqn)
			}
		}
		return
	}
	// move/migrate
	glog.Infof("Migrating [%s %s => %s] %s => %s", bucket, objname, newobjname, t.si.DaemonID, si.DaemonID)

	if errstr = t.sendfile(http.MethodPut, bucket, objname, si, finfo.Size(), newobjname); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
}

func (t *targetrunner) sendfile(method, bucket, objname string, destsi *daemonInfo, size int64, newobjname string) string {
	if newobjname == "" {
		newobjname = objname
	}
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	url := destsi.DirectURL + "/" + Rversion + "/" + Rfiles + "/"
	url += Rfrom + "/" + fromid + "/" + Rto + "/" + toid + "/"
	url += bucket + "/" + newobjname

	fqn := t.fqn(bucket, objname)
	file, err := os.Open(fqn)
	if err != nil {
		return fmt.Sprintf("Failed to open %q, err: %v", fqn, err)
	}
	defer file.Close()

	// md5 - FIXME: use xxhash here and elsewhere - FIXME: must be configurable and optional
	// + buffer pooling
	md5 := md5.New()
	var buffs buffif
	if size > t.buffers32k.fixedsize {
		buffs = t.buffers
	} else if size > t.buffers4k.fixedsize {
		buffs = t.buffers32k
	} else {
		assert(size != 0, "Unexpected: zero size "+fqn)
		buffs = t.buffers4k
	}
	buf := buffs.alloc()
	md5hash, errstr := ComputeFileMD5(file, buf, md5)
	buffs.free(buf)
	if errstr != "" {
		return errstr
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return fmt.Sprintf("Unexpected fseek failure when sending %q from %s, err: %v", fqn, t.si.DaemonID, err)
	}
	//
	// do send
	//
	request, err := http.NewRequest(method, url, file)
	assert(err == nil, err)
	request.Header.Set("Content-HASH", md5hash)
	response, err := t.httpclient.Do(request)
	if err != nil {
		return fmt.Sprintf("Failed to send %q from %s, err: %v", t.si.DaemonID, fqn, err)
	}
	if response != nil {
		ioutil.ReadAll(response.Body)
		response.Body.Close()
	}
	t.statsif.add("numsentfiles", 1)
	t.statsif.add("numsentbytes", size)
	return ""
}

//
// Cloud bucket + object => (local hashed path, fully qualified filename)
//
func (t *targetrunner) fqn(bucket, objname string) string {
	mpath := hrwMpath(bucket + "/" + objname)
	if t.islocalBucket(bucket) {
		return mpath + "/" + ctx.config.LocalBuckets + "/" + bucket + "/" + objname
	}
	return mpath + "/" + ctx.config.CloudBuckets + "/" + bucket + "/" + objname
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
	case http.MethodPost:
		t.httpdaepost(w, r)
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
		t.httpdaeput_lbmap(w, r, apitems)
		return
	}
	//
	// other PUT /daemon actions
	//
	var msg ActionMsg
	if t.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		if errstr := t.setconfig(msg.Name, msg.Value); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
		if msg.Name == "lru_enabled" && msg.Value == "false" {
			_, lruxact := t.xactinp.find(ActLRU)
			if lruxact != nil {
				if glog.V(3) {
					glog.Infof("Aborting LRU due to lru_enabled config change")
				}
				lruxact.abort()
			}
		}
	case ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) httpdaeput_smap(w http.ResponseWriter, r *http.Request, apitems []string) {
	curversion := t.smap.Version
	var newsmap *Smap
	if t.readJSON(w, r, &newsmap) != nil {
		return
	}
	if curversion == newsmap.Version {
		return
	}
	if curversion > newsmap.Version {
		glog.Errorf("Warning: attempt to downgrade Smap verion %d to %d", curversion, newsmap.Version)
		return
	}
	glog.Infof("%s: new Smap version %d (old %d)", apitems[0], newsmap.Version, curversion)

	// check whether this target is present in the new Smap
	// rebalance? (nothing to rebalance if the new map is a strict subset of the old)
	// assign proxysi
	// log
	existentialQ, isSubset := false, true
	for id, si := range newsmap.Smap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			glog.Infoln("target:", si, "<= self")
		} else {
			glog.Infoln("target:", si)
		}
		if _, ok := t.smap.Smap[id]; !ok {
			isSubset = false
		}
	}
	assert(existentialQ)

	t.smap, t.proxysi = newsmap, newsmap.ProxySI
	if apitems[0] == Rsyncsmap {
		return
	}
	if isSubset {
		glog.Infoln("nothing to rebalance: new Smap is a strict subset of the old")
		return
	}
	// xaction
	go t.runRebalance()
}

func (t *targetrunner) httpdaeput_lbmap(w http.ResponseWriter, r *http.Request, apitems []string) {
	curversion := t.lbmap.Version
	newlbmap := &lbmap{LBmap: make(map[string]string)}
	if t.readJSON(w, r, newlbmap) != nil {
		return
	}
	if curversion == newlbmap.Version {
		return
	}
	if curversion > newlbmap.Version {
		glog.Errorf("Warning: attempt to downgrade lbmap verion %d to %d", curversion, newlbmap.Version)
		return
	}
	glog.Infof("%s: new lbmap version %d (old %d)", apitems[0], newlbmap.Version, curversion)
	// destroylb
	for bucket := range t.lbmap.LBmap {
		_, ok := newlbmap.LBmap[bucket]
		if !ok {
			glog.Infof("Destroy local bucket %s", bucket)
			for mpath := range ctx.mountpaths {
				localbucketfqn := mpath + "/" + ctx.config.LocalBuckets + "/" + bucket
				if err := os.RemoveAll(localbucketfqn); err != nil {
					glog.Errorf("Failed to destroy local bucket dir %q, err: %v", localbucketfqn, err)
				}
			}
		}
	}
	t.lbmap = newlbmap
	for mpath := range ctx.mountpaths {
		for bucket := range t.lbmap.LBmap {
			localbucketfqn := mpath + "/" + ctx.config.LocalBuckets + "/" + bucket
			if err := CreateDir(localbucketfqn); err != nil {
				glog.Errorf("Failed to create local bucket dir %q, err: %v", localbucketfqn, err)
			}
		}
	}
	return
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
		jsbytes, err = json.Marshal(ctx.config)
		assert(err == nil, err)
	case GetWhatSmap:
		jsbytes, err = json.Marshal(t.si)
		assert(err == nil, err)
	case GetWhatStats:
		rr := getstorstatsrunner()
		rr.Lock()
		jsbytes, err = json.Marshal(rr)
		rr.Unlock()
		assert(err == nil, err)
	default:
		s := fmt.Sprintf("Unexpected GetMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
	}
	_ = t.writeJSON(w, r, jsbytes, "httpdaeget")
}

// management interface to register (unregistered) self
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	if err, status := t.register(); err != nil {
		s := fmt.Sprintf("Target %s failed to register with proxy, status %d, err: %v", t.si.DaemonID, status, err)
		t.invalmsghdlr(w, r, s)
		return
	}
	if glog.V(3) {
		glog.Infof("Registered self %s", t.si.DaemonID)
	}
}

//==============================================================================
//
// target's utilities and helpers
//
//==============================================================================
func (t *targetrunner) islocalBucket(bucket string) bool {
	_, ok := t.lbmap.LBmap[bucket]
	return ok
}

func (t *targetrunner) testingFSPpaths() bool {
	return ctx.config.TestFSP.Count > 0
}

func (t *targetrunner) fqn2bckobj(fqn string) (bucket, objname string, ok bool) {
	fn := func(path string) bool {
		if strings.HasPrefix(fqn, path) {
			rempath := fqn[len(path):]
			items := strings.SplitN(rempath, "/", 2)
			bucket, objname = items[0], items[1]
			return true
		}
		return false
	}
	for mpath := range ctx.mountpaths {
		if fn(mpath + "/" + ctx.config.CloudBuckets + "/") {
			ok = len(objname) > 0
			return
		}
		if fn(mpath + "/" + ctx.config.LocalBuckets + "/") {
			assert(t.islocalBucket(bucket))
			ok = len(objname) > 0
			return
		}
	}
	return
}

func (t *targetrunner) fspath2mpath() {
	for fp := range ctx.config.FSpaths {
		if _, err := os.Stat(fp); err != nil {
			glog.Fatalf("FATAL: fspath %q does not exist, err: %v", fp, err)
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(fp, &statfs); err != nil {
			glog.Fatalf("FATAL: cannot statfs fspath %q, err: %v", fp, err)
		}

		mp := &mountPath{Path: fp, Fsid: statfs.Fsid}
		_, ok := ctx.mountpaths[mp.Path]
		assert(!ok)
		ctx.mountpaths[mp.Path] = mp
	}
}

// create local directories to test multiple fspaths
func (t *targetrunner) testCachepathMounts() {
	var instpath string
	if ctx.config.TestFSP.Instance > 0 {
		instpath = ctx.config.TestFSP.Root + strconv.Itoa(ctx.config.TestFSP.Instance) + "/"
	} else {
		// e.g. when docker
		instpath = ctx.config.TestFSP.Root
	}
	for i := 0; i < ctx.config.TestFSP.Count; i++ {
		var mpath string
		if ctx.config.TestFSP.Count > 1 {
			mpath = instpath + strconv.Itoa(i+1)
		} else {
			mpath = instpath[0 : len(instpath)-1]
		}
		if err := CreateDir(mpath); err != nil {
			glog.Fatalf("FATAL: cannot create test cache dir %q, err: %v", mpath, err)
			return
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, &statfs); err != nil {
			glog.Fatalf("FATAL: cannot statfs mpath %q, err: %v", mpath, err)
			return
		}
		mp := &mountPath{Path: mpath, Fsid: statfs.Fsid}
		_, ok := ctx.mountpaths[mp.Path]
		assert(!ok)
		ctx.mountpaths[mp.Path] = mp
	}
}

func (t *targetrunner) mpath2Fsid() (fsmap map[syscall.Fsid]string) {
	fsmap = make(map[syscall.Fsid]string, len(ctx.mountpaths))
	for _, mountpath := range ctx.mountpaths {
		mp2, ok := fsmap[mountpath.Fsid]
		if ok {
			if !t.testingFSPpaths() {
				glog.Fatalf("FATAL: duplicate FSID %v: mpath1 %q, mpath2 %q", mountpath.Fsid, mountpath.Path, mp2)
			}
			continue
		}
		fsmap[mountpath.Fsid] = mountpath.Path
	}
	return
}

// on err closes and removes the file; othwerise closes and returns the size
// FIXME: md5 must be optional/configurable as well as xxhash
// omd5 can be empty for multipart object or when PUTting
func (t *targetrunner) receiveFileAndFinalize(fqn, objname, omd5 string, reader io.ReadCloser) (md5hash string, written int64, errstr string) {
	var (
		err  error
		file *os.File
	)
	if file, errstr = initobj(fqn); errstr != "" {
		errstr = fmt.Sprintf("Failed to create and initialize %s, err: %s", fqn, errstr)
		return
	}
	buf := t.buffers.alloc()
	defer func() {
		t.buffers.free(buf)
		if errstr == "" {
			return
		}
		if err = file.Close(); err != nil {
			glog.Errorf("Nested: failed to close received file %s, err: %v", fqn, err)
		}
		// FIXME: immediately remove evidence..
		if err = os.Remove(fqn); err != nil {
			glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, fqn, err)
		}
	}()
	if ctx.config.CksumConfig.ValidateColdGet {
		md5 := md5.New()
		if written, errstr = ReceiveFile(file, reader, buf, md5); errstr != "" {
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash = hex.EncodeToString(hashInBytes)
		if omd5 != "" && omd5 != md5hash {
			errstr = fmt.Sprintf("Checksum mismatch: object %s MD5 %s... != received file %s MD5 %s...)",
				objname, omd5[:8], fqn, md5hash[:8])
			return
		}
	} else {
		if written, errstr = ReceiveFile(file, reader, buf); errstr != "" {
			return
		}
	}
	if errstr = finalizeobj(fqn, []byte(md5hash)); errstr != "" {
		return
	}
	if err = file.Close(); err != nil {
		errstr = fmt.Sprintf("Failed to close received file %s, err: %v", fqn, err)
	}
	return
}

//
// pre- and post- wrappers
//
func initobj(fqn string) (file *os.File, errstr string) {
	var err error
	file, err = Createfile(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Unable to create file %s, err: %v", fqn, err)
		return nil, errstr
	}
	return file, ""
}

func finalizeobj(fqn string, md5sum []byte) (errstr string) {
	if ctx.config.NoXattrs {
		return
	}
	if len(md5sum) == 0 {
		return
	}
	errstr = Setxattr(fqn, HashAttr, md5sum)
	return
}
