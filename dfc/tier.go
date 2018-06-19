// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

func (t *targetrunner) objectInNextTier(nextURL, bucket, objName string) (in bool, errstr string, errcode int) {
	var url = nextURL + URLPath(Rversion, Robjects, bucket, objName) + fmt.Sprintf(
		"?%s=true", URLParamCheckCached)

	r, err := t.httprunner.httpclientLongTimeout.Head(url)
	if err != nil {
		errstr = err.Error()
		return
	}
	if r.StatusCode >= http.StatusBadRequest {
		if r.StatusCode == http.StatusNotFound {
			r.Body.Close()
			return
		}
		errcode = r.StatusCode
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errstr = fmt.Sprintf("failed to read response body, err: %s", err)
		} else {
			errstr = fmt.Sprintf(
				"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, next tier URL: %s",
				r.StatusCode, string(b), bucket, objName, nextURL)
		}
		r.Body.Close()
		return
	}
	in = true
	r.Body.Close()
	return
}

func (t *targetrunner) getObjectNextTier(nextURL, bucket, objName, fqn string) (p *objectProps, errstr string, errcode int) {
	var url = nextURL + URLPath(Rversion, Robjects, bucket, objName)

	r, err := t.httprunner.httpclientLongTimeout.Get(url)
	if err != nil {
		errstr = err.Error()
		return
	}

	if r.StatusCode >= http.StatusBadRequest {
		errcode = r.StatusCode
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errstr = err.Error()
			r.Body.Close()
			return
		}
		errstr = fmt.Sprintf(
			"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, next tier URL: %s",
			r.StatusCode, string(b), bucket, objName, nextURL)
		r.Body.Close()
		return
	}

	p = &objectProps{}
	_, p.nhobj, p.size, errstr = t.receive(fqn, objName, "", nil, r.Body)
	r.Body.Close()
	return
}

func (t *targetrunner) putObjectNextTier(nextURL, bucket, objName string, body io.Reader) (errstr string, errcode int) {
	var url = nextURL + URLPath(Rversion, Robjects, bucket, objName)

	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		errstr = fmt.Sprintf("failed to create new HTTP request, err: %v", err)
		return
	}

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
				resp.StatusCode, string(b), bucket, objName, nextURL)
		}
	}
	resp.Body.Close()
	return
}
