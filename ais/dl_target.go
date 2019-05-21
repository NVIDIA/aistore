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
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// NOTE: This request is internal so we can have asserts there.
// [METHOD] /v1/download
func (t *targetrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	if !t.verifyProxyRedirection(w, r, cmn.Download) {
		return
	}

	var (
		response interface{}
		respErr  error

		statusCode = http.StatusBadRequest
	)

	downloader, err := t.xactions.renewDownloader(t)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodPost:
		id := r.URL.Query().Get(cmn.URLParamID)
		cmn.Assert(id != "")

		payload, err := t.parseStartDownloadRequest(r, id)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Downloading: %s", payload)
		}
		response, respErr, statusCode = downloader.Download(payload)

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
			response, respErr, statusCode = downloader.JobStatus(payload.ID)
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
			response, respErr, statusCode = downloader.ListJobs(regex)
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
			response, respErr, statusCode = downloader.AbortJob(payload.ID)
		case cmn.Remove:
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Removing download: %s", payload)
			}
			response, respErr, statusCode = downloader.RemoveJob(payload.ID)
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
		b, err := jsoniter.Marshal(response)
		cmn.AssertNoErr(err)
		_, err = w.Write(b)
		if err != nil {
			glog.Errorf("Failed to write to http response: %s.", err.Error())
		}
	}
}

func (t *targetrunner) parseStartDownloadRequest(r *http.Request, id string) (*cmn.DlBody, error) {
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
		bucket      string
		bckIsLocal  bool
		fromCloud   bool
	)

	payload.InitWithQuery(query)
	bucket = payload.Bucket

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
	} else if err := cloudPayload.Validate(bckIsLocal); err == nil {
		bckEntries, err := t.listCloudBucket(r.Header, bucket, cloudPayload.Prefix, cloudPayload.Suffix)
		if err != nil {
			return nil, err
		}
		if len(bckEntries) == 0 {
			return nil, fmt.Errorf("input does not match any object in cloud bucket %s", bucket)
		}

		objects = make(cmn.SimpleKVs, len(bckEntries))
		for _, entry := range bckEntries {
			objects[entry] = ""
		}
		fromCloud = true
		description = cloudPayload.Describe()
	} else {
		return nil, errors.New("input does not match any of the supported formats (single, range, multi, cloud)")
	}

	if payload.Description == "" {
		payload.Description = description
	}

	return t.buildDownloaderInput(id, payload, objects, fromCloud)
}

func (t *targetrunner) buildDownloaderInput(id string, payload *cmn.DlBase, objects cmn.SimpleKVs, cloud bool) (*cmn.DlBody, error) {
	var (
		smap   = t.smapowner.get()
		dlBody = &cmn.DlBody{ID: id}
	)
	dlBody.Bucket = payload.Bucket
	dlBody.BckProvider = payload.BckProvider
	dlBody.Timeout = payload.Timeout
	dlBody.Description = payload.Description

	// Filter out objects that will be handled by other targets
	for objName, link := range objects {
		// Make sure that objName doesn't contain "?query=smth" suffix.
		objName, err := normalizeObjName(objName)
		if err != nil {
			return nil, err
		}
		// Make sure that link contains protocol (absence of protocol can result in errors).
		link = cmn.PrependProtocol(link)

		si, errstr := hrwTarget(payload.Bucket, objName, smap)
		if errstr != "" {
			return nil, fmt.Errorf(errstr)
		}
		if si.DaemonID != t.si.DaemonID {
			continue
		}

		dlObj := cmn.DlObj{
			Objname:   objName,
			Link:      link,
			FromCloud: cloud,
		}
		dlBody.Objs = append(dlBody.Objs, dlObj)
	}

	return dlBody, nil
}

func (t *targetrunner) listCloudBucket(header http.Header, bucket, prefix, suffix string) ([]string, error) {
	msg := cmn.SelectMsg{
		Prefix:     prefix,
		PageMarker: "",
		Fast:       true,
	}

	objects := make([]string, 0, 1024)
	for {
		bytes, err, _ := getcloudif().listbucket(t.contextWithAuth(header), bucket, &msg)
		if err != nil {
			return nil, fmt.Errorf("error listing cloud bucket %s: %v", bucket, err)
		}
		curBckEntries := &cmn.BucketList{}
		if err := jsoniter.Unmarshal(bytes, curBckEntries); err != nil {
			return nil, fmt.Errorf("error unmarshalling BucketList: %v", err)
		}

		// Take only entries with matching suffix
		for _, entry := range curBckEntries.Entries {
			if strings.HasSuffix(entry.Name, suffix) {
				objects = append(objects, entry.Name)
			}
		}

		msg.PageMarker = curBckEntries.PageMarker
		if msg.PageMarker == "" {
			break
		}
	}

	return objects, nil
}
