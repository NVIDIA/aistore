// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	AISRegion = "ais"
	AISSever  = "AIS"

	// versioning
	URLParamVersioning  = "versioning" // URL parameter
	URLParamMultiDelete = "delete"
	versioningEnabled   = "Enabled"
	versioningDisabled  = "Suspended"

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"
	// TODO: can it be omitted? // storageClass = "STANDARD"

	// Headers
	headerETag    = "ETag"
	headerVersion = "x-amz-version-id"
	HeaderObjSrc  = "x-amz-copy-source"

	headerAtime = "Last-Modified"
)

// ExtractEndpoint extracts an S3 endpoint from the full URL path.
// Endpoint is a host name with port and root URL path(if it exists).
// E.g. for AIS `http://localhost:8080/s3/bck1/obj1` the endpoint
// is `localhost:8080/s3`
func ExtractEndpoint(path string) string {
	ep := path
	if idx := strings.Index(ep, "/"+cmn.S3); idx > 0 {
		ep = ep[:idx+3]
	}
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return ep
}

func MakeRedirectBody(newPath, bucket string) string {
	ep := ExtractEndpoint(newPath)
	body := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
		"<Error><Code>TemporaryRedirect</Code><Message>Redirect</Message>" +
		"<Endpoint>" + ep + "</Endpoint>" +
		"<Bucket>" + bucket + "</Bucket></Error>"
	return body
}
