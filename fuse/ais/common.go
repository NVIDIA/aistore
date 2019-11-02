// Package ais implements an AIStore client.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	httpTransportTimeout = 60 * time.Second  // FIXME: Too long?
	httpClientTimeout    = 300 * time.Second // FIXME: Too long?
)

//////////
// ERRORS
//////////

const (
	IOErrorKindBucket = "bucket"
	IOErrorKindObject = "object"
)

type IOError struct {
	kind   string
	object string
	op     string
	err    error
}

func (e *IOError) Error() string {
	return fmt.Sprintf("IOError: %s op %s %s: %v",
		e.kind, e.op, e.object, e.err)
}

func newIOError(err error, kind string, op string, object string) error {
	return &IOError{
		kind:   kind,
		op:     op,
		object: object,
		err:    err,
	}
}

func newBucketIOError(err error, op string, object ...string) error {
	if len(object) > 0 {
		return newIOError(err, IOErrorKindBucket, op, object[0])
	}
	return newIOError(err, IOErrorKindBucket, op, "")
}

func newObjectIOError(err error, op string, object string) error {
	return newIOError(err, IOErrorKindObject, op, object)
}

///////////
// HELPERS
///////////

func emptyBuffer() cmn.ReadOpenCloser {
	return cmn.NopOpener(
		ioutil.NopCloser(
			bytes.NewReader([]byte{}),
		),
	)
}

func apiParams(clusterURL string) *api.BaseParams {
	httpTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: httpTransportTimeout,
		}).DialContext,
	}

	httpClient := &http.Client{
		Timeout:   httpClientTimeout,
		Transport: httpTransport,
	}

	return &api.BaseParams{
		Client: httpClient,
		URL:    clusterURL,
	}
}
