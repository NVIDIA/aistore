// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/tutils/tassert"
)

var (
	transformMessage = []byte("Written by the transformerServer")
)

func testCommunication(t *testing.T, commType string) {
	transformerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(transformMessage)
	}))
	defer transformerServer.Close()

	comm := makeCommunicator(commType, transformerServer.URL, nil)
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		comm.DoTransform(w, r, nil, "")
	}))
	defer targetServer.Close()

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetServer.URL, http.StatusMovedPermanently)
	}))
	defer proxyServer.Close()

	resp, err := http.Get(proxyServer.URL)
	tassert.CheckFatal(t, err)

	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	tassert.CheckFatal(t, err)

	tassert.Errorf(
		t, reflect.DeepEqual(b, transformMessage),
		"CommType: %s, Expected transformed msg: %q, got: %q", commType, transformMessage, b,
	)
}

func TestCommunication(t *testing.T) {
	tests := []string{
		pushPullCommType,
		redirectCommType,
	}

	for _, commType := range tests {
		t.Run(commType, func(t *testing.T) {
			testCommunication(t, commType)
		})
	}
}
