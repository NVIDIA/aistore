// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core/meta"
)

// s3Redirect performs reverse proxy call or HTTP-redirects to a designated node
// in a cluster based on feature flag. See also: docs/s3compat.md
func (p *proxy) s3Redirect(w http.ResponseWriter, r *http.Request, si *meta.Snode, redirectURL, bucket string) {
	if cmn.GCO.Get().Features.IsSet(feat.S3ReverseProxy) {
		p.reverseNodeRequest(w, r, si)
	} else {
		h := w.Header()
		h.Set(cos.HdrLocation, redirectURL)
		h.Set(cos.HdrContentType, "text/xml; charset=utf-8")
		h.Set(cos.HdrServer, s3.AISServer)
		w.WriteHeader(http.StatusTemporaryRedirect)
		ep := extractEndpoint(redirectURL)
		body := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
			"<Error><Code>TemporaryRedirect</Code><Message>Redirect</Message>" +
			"<Endpoint>" + ep + "</Endpoint>" +
			"<Bucket>" + bucket + "</Bucket></Error>"
		fmt.Fprint(w, body)
	}
}

// extractEndpoint extracts an S3 endpoint from the full URL path.
// Endpoint is a host name with port and root URL path (if exists).
// E.g. for AIS `http://localhost:8080/s3/bck1/obj1` the endpoint
// would be `localhost:8080/s3`
func extractEndpoint(path string) string {
	ep := path
	if idx := strings.Index(ep, "/"+apc.S3); idx > 0 {
		ep = ep[:idx+3]
	}
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return ep
}
