// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
)

func (t *targetrunner) objectInNextTier(nextTierURL, bucket, objname string) (in bool, errstr string, errcode int) {
	url := nextTierURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname)
	url += fmt.Sprintf("?%s=true", cmn.URLParamCheckCached)

	resp, err := t.httprunner.httpclientLongTimeout.Head(url)
	if err != nil {
		errstr = err.Error()
		return
	}
	if resp.StatusCode >= http.StatusBadRequest {
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return
		}
		errcode = resp.StatusCode
		_, err := ioutil.ReadAll(resp.Body)
		errstr = fmt.Sprintf("Failed to get %s/%s from %s tier, status %d", bucket, objname, nextTierURL, resp.StatusCode)
		if err != nil {
			errstr += fmt.Sprintf(" (Failed to read response, err: %v)", err)
		}
		resp.Body.Close()
		return
	}
	in = true
	resp.Body.Close()
	return
}

func (t *targetrunner) getObjectNextTier(nextTierURL, bucket, objname, fqn string) (props *cluster.LOM, errstr string, errcode int) {
	var url = nextTierURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname)

	resp, err := t.httprunner.httpclientLongTimeout.Get(url)
	if err != nil {
		errstr = err.Error()
		return
	}

	if resp.StatusCode >= http.StatusBadRequest {
		errcode = resp.StatusCode
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errstr = err.Error()
			resp.Body.Close()
			return
		}
		errstr = fmt.Sprintf(
			"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, next tier URL: %s",
			resp.StatusCode, string(b), bucket, objname, nextTierURL,
		)
		resp.Body.Close()
		return
	}

	props = &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr = props.Fill(0); errstr != "" {
		resp.Body.Close()
		return
	}
	_, props.Nhobj, props.Size, errstr = t.receive(fqn, props, "", resp.Body)
	resp.Body.Close()
	return
}

// TODO: this function is currently unused should be placed in `put`
func (t *targetrunner) putObjectNextTier(nextTierURL, bucket, object string, body io.ReadCloser,
	reopenBody func() (io.ReadCloser, error)) (errstr string, errcode int) {
	var url = nextTierURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)

	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		errstr = fmt.Sprintf("failed to create new HTTP request, err: %v", err)
		return
	}
	req.GetBody = reopenBody
	resp, err := t.httprunner.httpclientLongTimeout.Do(req)
	if err != nil {
		errstr = err.Error()
		return
	}

	if resp.StatusCode >= http.StatusBadRequest {
		errcode = resp.StatusCode
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errstr = err.Error()
		} else {
			errstr = fmt.Sprintf(
				"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, next tier URL: %s",
				resp.StatusCode, string(b), bucket, object, nextTierURL,
			)
		}
	}

	resp.Body.Close()
	return
}
