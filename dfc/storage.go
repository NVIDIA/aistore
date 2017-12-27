/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/golang/glog"
)

const (
	fslash           = "/"
	s3skipTokenToKey = 3
)

// TODO Fillin AWS specific initialization
type awsif struct {
}

// TODO Fillin GCP specific initialization
type gcpif struct {
}

type cinterface interface {
	listbucket(http.ResponseWriter, string)
	getobj(http.ResponseWriter, string, string, string)
}

// Servhdlr function serves request coming to listening port of DFC's Storage Server.
// It supports GET method only and return 405 error for non supported Methods.
// This function checks wheather key exists locally or not. If key does not exist locally
// it prepares session and download objects from S3 to path on local host.
func servhdlr(w http.ResponseWriter, r *http.Request) {
	stats := getstorstats()
	switch r.Method {
	case "GET":
		atomic.AddInt64(&stats.numget, 1)
		cnt := strings.Count(html.EscapeString(r.URL.Path), fslash)
		s := strings.Split(html.EscapeString(r.URL.Path), fslash)
		bktname := s[1]
		if cnt == 1 {
			// Get with only bucket name will imply getting list of objects from bucket.
			getcloudif().listbucket(w, bktname)
		} else {
			// Expecting /<bucketname>/keypath
			s := strings.SplitN(html.EscapeString(r.URL.Path), fslash, s3skipTokenToKey)
			keyname := s[2]
			mpath := doHashfindMountPath(bktname + fslash + keyname)
			assert(len(mpath) > 0) // see mountpath.enabled

			fname := mpath + fslash + bktname + fslash + keyname
			// Check wheather filename exists in local directory or not
			_, err := os.Stat(fname)
			if os.IsNotExist(err) {
				atomic.AddInt64(&stats.numcoldget, 1)
				glog.Infof("Bucket %s key %s fqn %q is not cached", bktname, keyname, fname)
				getcloudif().getobj(w, mpath, bktname, keyname)
			} else if glog.V(2) {
				glog.Infof("Bucket %s key %s fqn %q *is* cached", bktname, keyname, fname)
			}
			file, err := os.Open(fname)
			if err != nil {
				glog.Errorf("Failed to open file %q, err: %v", fname, err)
				checksetmounterror(fname)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				atomic.AddInt64(&stats.numerr, 1)
			} else {
				defer file.Close()

				// TODO: optimize. Currently the file gets downloaded and stored locally
				//       _prior_ to sending http response back to the requesting client
				_, err := io.Copy(w, file)
				if err != nil {
					glog.Errorf("Failed to copy data to http response for fname %q, err: %v", fname, err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					atomic.AddInt64(&stats.numerr, 1)
				} else {
					glog.Infof("Copied %q to http response\n", fname)
				}
			}
		}
	case "POST":
	case "PUT":
	case "DELETE":
	default:
		glog.Errorf("Invalid request from %s: %s %q", r.RemoteAddr, r.Method, r.URL)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed)+": "+r.Method,
			http.StatusMethodNotAllowed)

	}
	glog.Flush()
}

//===========================================================================
//
// storage runner
//
//===========================================================================
type storagerunner struct {
	httprunner
	cloudif cinterface // Interface for multiple cloud
}

// start storage runner
func (r *storagerunner) run() error {
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
	return r.runhandler(servhdlr)
}

// stop gracefully
func (r *storagerunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	r.httprunner.stop(err)
}
