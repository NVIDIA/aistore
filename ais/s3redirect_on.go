//go:build !s3rproxy

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// s3Redirect() HTTP-redirects to a designated node in a cluster. See also:
// * docs/s3compat.md
// * Makefile (and look for `s3rproxy` build tag)
// * ais/s3redirect_on.go
func (*proxy) s3Redirect(w http.ResponseWriter, _ *http.Request, _ *cluster.Snode, redirectURL, bucket string) {
	h := w.Header()
	h.Set(cos.HdrLocation, redirectURL)
	h.Set(cos.HdrContentType, "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusTemporaryRedirect)
	ep := ExtractEndpoint(redirectURL)
	body := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
		"<Error><Code>TemporaryRedirect</Code><Message>Redirect</Message>" +
		"<Endpoint>" + ep + "</Endpoint>" +
		"<Bucket>" + bucket + "</Bucket></Error>"
	fmt.Fprint(w, body)
}

// ExtractEndpoint extracts an S3 endpoint from the full URL path.
// Endpoint is a host name with port and root URL path (if exists).
// E.g. for AIS `http://localhost:8080/s3/bck1/obj1` the endpoint
// would be `localhost:8080/s3`
func ExtractEndpoint(path string) string {
	ep := path
	if idx := strings.Index(ep, "/"+apc.S3); idx > 0 {
		ep = ep[:idx+3]
	}
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return ep
}
