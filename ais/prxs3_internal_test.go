// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestDecodeS3XML(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("<Delete><Object><Key>foo</Key></Object></Delete>"))
	var decoded s3.Delete
	ecode, err := decodeS3XML(req, &decoded, maxDeleteXMLSize)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, ecode == 0 && len(decoded.Object) == 1 && decoded.Object[0].Key == "foo",
		"unexpected result: status=%d, decoded=%+v", ecode, decoded)
}

func TestDecodeS3XMLRejectsDeclaredOversize(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.ContentLength = maxVersioningXMLSize + 1
	ecode, err := decodeS3XML(req, &s3.VersioningConfiguration{}, maxVersioningXMLSize)
	tassert.Fatalf(t, err != nil && ecode == http.StatusRequestEntityTooLarge,
		"expected status %d, got %d (%v)", http.StatusRequestEntityTooLarge, ecode, err)
}

func TestDecodeS3XMLRejectsStreamedOversize(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", cos.NopReader(maxVersioningXMLSize+1))
	ecode, err := decodeS3XML(req, &s3.VersioningConfiguration{}, maxVersioningXMLSize)
	tassert.Fatalf(t, err != nil && ecode == http.StatusRequestEntityTooLarge,
		"expected status %d, got %d (%v)", http.StatusRequestEntityTooLarge, ecode, err)
}
