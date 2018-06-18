// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

var runRebalanceOnce = &sync.Once{}

type xrebpathrunner struct {
	t         *targetrunner
	mpathplus string
	xreb      *xactRebalance
	wg        *sync.WaitGroup
	newsmap   *Smap
	aborted   bool
}

func (t *targetrunner) runRebalance(newsmap *Smap, newtargetid string) {
	//
	// first, check whether all the Smap-ed targets are up and running
	//
	glog.Infof("rebalance: Smap ver %d, newtargetid=%s", newsmap.version(), newtargetid)
	from := "?" + URLParamFromID + "=" + t.si.DaemonID
	for sid, si := range newsmap.Tmap {
		if sid == t.si.DaemonID {
			continue
		}
		url := si.DirectURL + "/" + Rversion + "/" + Rhealth
		url += from
		pollstarted, ok := time.Now(), false
		timeout := kalivetimeout
		for {
			res := t.call(nil, si, url, http.MethodGet, nil, timeout)
			if res.err == nil {
				ok = true
				break
			}

			if res.status > 0 {
				glog.Infof("%s is offline with status %d, err: %v", sid, res.status, res.err)
			} else {
				glog.Infof("%s is offline, err: %v", sid, res.err)
			}
			timeout = time.Duration(float64(timeout)*1.5 + 0.5)
			if timeout > ctx.config.Timeout.MaxKeepalive {
				timeout = ctx.config.Timeout.Default
			}
			if time.Since(pollstarted) > ctx.config.Rebalance.DestRetryTime {
				break
			}
			time.Sleep(proxypollival * 2)
		}
		if !ok {
			glog.Errorf("Not starting rebalancing x-action: target %s appears to be offline", sid)
			return
		}
	}

	// find and abort in-progress x-action if exists and if its smap version is lower
	// start new x-action unless the one for the current version is already in progress
	xreb := t.xactinp.renewRebalance(newsmap.Version, t)
	if xreb == nil {
		return
	}
	pmarker := t.xactinp.rebalanceInProgress()
	file, err := CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	glog.Infoln(xreb.tostring())
	wg := &sync.WaitGroup{}
	allr := make([]*xrebpathrunner, 0, len(ctx.mountpaths.Available)*2)
	for mpath := range ctx.mountpaths.Available {
		rc := &xrebpathrunner{t: t, mpathplus: makePathCloud(mpath), xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		go rc.oneRebalance()
		allr = append(allr, rc)

		rl := &xrebpathrunner{t: t, mpathplus: makePathLocal(mpath), xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		go rl.oneRebalance()
		allr = append(allr, rl)
	}
	wg.Wait()
	if pmarker != "" {
		var aborted bool
		for _, r := range allr {
			if r.aborted {
				aborted = true
				break
			}
		}
		if !aborted {
			if err := os.Remove(pmarker); err != nil {
				glog.Errorf("Failed to remove rebalance-in-progress mark %s, err: %v", pmarker, err)
			}
		}
	}
	if newtargetid == t.si.DaemonID {
		glog.Infof("rebalance: %s <= self", newtargetid)
		t.pollRebalancingDone(newsmap) // until the cluster is fully rebalanced - see t.httpobjget
	}
	xreb.etime = time.Now()
	glog.Infoln(xreb.tostring())
	t.xactinp.del(xreb.id)
}

func (t *targetrunner) pollRebalancingDone(newsmap *Smap) {
	for {
		time.Sleep(time.Minute) // FIXME: must be smarter
		count := 0
		for sid, si := range newsmap.Tmap {
			if sid == t.si.DaemonID {
				continue
			}
			url := si.DirectURL + "/" + Rversion + "/" + Rhealth
			res := t.call(nil, si, url, http.MethodGet, nil)
			// retry once
			if res.err == context.DeadlineExceeded {
				res = t.call(nil, si, url, http.MethodGet, nil, kalivetimeout*2)
			}

			if res.err != nil {
				glog.Errorf("Failed to call %s, err: %v - assuming down/unavailable", sid, res.err)
				continue
			}

			status := &thealthstatus{}
			err := json.Unmarshal(res.outjson, status)
			if err == nil {
				if status.IsRebalancing {
					time.Sleep(proxypollival * 2)
					count++
				}
			} else {
				glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]",
					url, err, string(res.outjson))
			}
		}
		if glog.V(4) {
			glog.Infof("in-progress count=%d (targets)", count)
		}
		if count == 0 {
			break
		}
	}
}

//=========================
//
// rebalance-runner methods
//
//=========================

func (rcl *xrebpathrunner) oneRebalance() {
	if err := filepath.Walk(rcl.mpathplus, rcl.rebwalkf); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rcl.mpathplus, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rcl.mpathplus, err)
		}
	}
	rcl.wg.Done()
}

// the walking callback is execited by the LRU xaction
// (notice the receiver)
func (rcl *xrebpathrunner) rebwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("rebwalkf invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if iswork, _ := rcl.t.isworkfile(fqn); iswork {
		return nil
	}
	// abort?
	select {
	case <-rcl.xreb.abrt:
		err = fmt.Errorf("%s aborted, exiting rebwalkf path %s", rcl.xreb.tostring(), rcl.mpathplus)
		glog.Infoln(err)
		glog.Flush()
		rcl.aborted = true
		return err
	default:
		break
	}
	// rebalance maybe
	bucket, objname, errstr := rcl.t.fqn2bckobj(fqn)
	if errstr != "" {
		glog.Warningf("%s - skipping...", errstr)
		return nil
	}
	si, errstr := HrwTarget(bucket, objname, rcl.newsmap)
	if errstr != "" {
		return fmt.Errorf(errstr)
	}
	if si.DaemonID == rcl.t.si.DaemonID {
		return nil
	}

	// do rebalance
	glog.Infof("%s/%s %s => %s", bucket, objname, rcl.t.si.DaemonID, si.DaemonID)
	if errstr = rcl.t.sendfile(http.MethodPut, bucket, objname, si, osfi.Size(), "", ""); errstr != "" {
		glog.Infof("Failed to rebalance %s/%s: %s", bucket, objname, errstr)
	} else {
		// FIXME: TODO: delay the removal or (even) rely on the LRU
		if err := os.Remove(fqn); err != nil {
			glog.Errorf("Failed to delete %s after it has been moved, err: %v", fqn, err)
		}
	}
	return nil
}
