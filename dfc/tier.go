// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func (t *targetrunner) objectInNextTier(nextURL, bucket, objname string) (in bool, errstr string, errcode int) {
	var url = nextURL + URLPath(Rversion, Robjects, bucket, objname) + fmt.Sprintf(
		"?%s=true", URLParamCheckCached)

	r, err := t.httprunner.httpclientLongTimeout.Head(url)
	if err != nil {
		errstr = err.Error()
		return
	}
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		if r.StatusCode == http.StatusNotFound {
			return
		}
		errcode = r.StatusCode
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errstr = fmt.Sprintf("failed to read response body, err = %s", err)
			return
		}
		errstr = fmt.Sprintf(
			"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, next tier URL: %s",
			r.StatusCode, string(b), bucket, objname, nextURL)
		return
	}
	in = true
	return
}

func (t *targetrunner) getObjectNextTier(nextURL, bucket, objname, fqn string) (p *objectProps, errstr string, errcode int) {
	var url = nextURL + URLPath(Rversion, Robjects, bucket, objname)

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
			return
		}
		errstr = fmt.Sprintf(
			"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, next tier URL: %s",
			r.StatusCode, string(b), bucket, objname, nextURL)
		return
	}

	p = &objectProps{}
	_, p.nhobj, p.size, errstr = t.receive(fqn, false, objname, "", nil, r.Body)
	return
}
