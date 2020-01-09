// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: This request is internal so we can have asserts there.
// [METHOD] /v1/download
func (t *targetrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	if !t.verifyProxyRedirection(w, r, cmn.Download) {
		return
	}
	var (
		response   interface{}
		respErr    error
		statusCode int
	)

	downloaderXact, err := xaction.Registry.RenewDownloader(t, t.statsif)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodPost:
		id := r.URL.Query().Get(cmn.URLParamID)
		cmn.Assert(id != "")

		dlJob, err := t.parseStartDownloadRequest(r, id)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Downloading: %s", dlJob.ID())
		}

		response, respErr, statusCode = downloaderXact.Download(dlJob)

	case http.MethodGet:
		payload := &cmn.DlAdminBody{}
		if err := cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		cmn.AssertNoErr(payload.Validate(false /*requireID*/))

		if payload.ID != "" {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Getting status of download: %s", payload)
			}
			response, respErr, statusCode = downloaderXact.JobStatus(payload.ID)
		} else {
			var regex *regexp.Regexp
			if payload.Regex != "" {
				if regex, err = regexp.CompilePOSIX(payload.Regex); err != nil {
					cmn.InvalidHandlerWithMsg(w, r, err.Error())
					return
				}
			}
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Listing downloads")
			}
			response, respErr, statusCode = downloaderXact.ListJobs(regex)
		}

	case http.MethodDelete:
		payload := &cmn.DlAdminBody{}
		if err = cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		cmn.AssertNoErr(payload.Validate(true))

		items, err := cmn.MatchRESTItems(r.URL.Path, 1, false, cmn.Version, cmn.Download)
		cmn.AssertNoErr(err)

		switch items[0] {
		case cmn.Abort:
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Aborting download: %s", payload)
			}
			response, respErr, statusCode = downloaderXact.AbortJob(payload.ID)
		case cmn.Remove:
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Removing download: %s", payload)
			}
			response, respErr, statusCode = downloaderXact.RemoveJob(payload.ID)
		default:
			cmn.AssertMsg(false, fmt.Sprintf("Invalid action for DELETE request: %s (expected either %s or %s).", items[0], cmn.Abort, cmn.Remove))
			return
		}

	default:
		cmn.AssertMsg(false, fmt.Sprintf("Invalid http method %s; expected one of %s, %s, %s", r.Method, http.MethodGet, http.MethodPost, http.MethodDelete))
		return
	}

	if statusCode >= http.StatusBadRequest {
		cmn.InvalidHandlerWithMsg(w, r, respErr.Error(), statusCode)
		return
	}

	if response != nil {
		b := cmn.MustMarshal(response)
		_, err = w.Write(b)
		if err != nil {
			glog.Errorf("Failed to write to http response: %s.", err.Error())
		}
	}
}

// parseStartDownloadRequest translates external http request into internal representation: DlJob interface
// based on different type of request DlJob might of different type which implements the interface
func (t *targetrunner) parseStartDownloadRequest(r *http.Request, id string) (downloader.DlJob, error) {
	var (
		// link -> objname
		objects cmn.SimpleKVs
		query   = r.URL.Query()

		payload        = &cmn.DlBase{}
		singlePayload  = &cmn.DlSingleBody{}
		rangePayload   = &cmn.DlRangeBody{}
		multiPayload   = &cmn.DlMultiBody{}
		cloudPayload   = &cmn.DlCloudBody{}
		objectsPayload interface{}

		description string
		bckIsAIS    bool
		fromCloud   bool
	)

	payload.InitWithQuery(query)

	singlePayload.InitWithQuery(query)
	rangePayload.InitWithQuery(query)
	multiPayload.InitWithQuery(query)
	cloudPayload.InitWithQuery(query)

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	if err := singlePayload.Validate(); err == nil {
		if objects, err = singlePayload.ExtractPayload(); err != nil {
			return nil, err
		}
		description = singlePayload.Describe()
	} else if err := rangePayload.Validate(); err == nil {
		// FIXME: rangePayload still evaluates all of the objects on this line
		// it means that if range is 0-3mln, we will create 3mln objects right away
		// this should not be the case and we should create them on the demand
		// NOTE: size of objects to be downloaded by a target will be unknown
		// So proxy won't be able to sum sizes from all targets when calculating total size
		// This should be taken care of somehow, as total is easy to know from range template anyway
		if objects, err = rangePayload.ExtractPayload(); err != nil {
			return nil, err
		}
		description = rangePayload.Describe()
	} else if err := multiPayload.Validate(b); err == nil {
		if err := jsoniter.Unmarshal(b, &objectsPayload); err != nil {
			return nil, err
		}

		if objects, err = multiPayload.ExtractPayload(objectsPayload); err != nil {
			return nil, err
		}
		description = multiPayload.Describe()
	} else if err := cloudPayload.Validate(bckIsAIS); err == nil {
		//
		// TODO -- FIXME: bckIsAIS must be removed; init bck (below) and check conditions&errors
		//
		bck := &cluster.Bck{Name: cloudPayload.Bucket, Provider: cloudPayload.Provider}
		baseJob := downloader.NewBaseDlJob(id, bck, cloudPayload.Timeout, payload.Description)
		return downloader.NewCloudBucketDlJob(t.contextWithAuth(r.Header), t, baseJob, cloudPayload.Prefix, cloudPayload.Suffix)
	} else {
		return nil, errors.New("input does not match any of the supported formats (single, range, multi, cloud)")
	}

	if payload.Description == "" {
		payload.Description = description
	}

	bck := &cluster.Bck{Name: payload.Bucket, Provider: payload.Provider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
			return nil, err
		}
	}
	input, err := downloader.BuildDownloaderInput(t, id, bck, payload, objects, fromCloud)
	if err != nil {
		return nil, err
	}

	return downloader.NewSliceDlJob(input.ID, input.Objs, bck, payload.Timeout, payload.Description), nil
}
