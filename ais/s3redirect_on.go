// +build !alwaysrproxy
// +build !s3rproxy

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/ais/s3compat"
	"github.com/NVIDIA/aistore/cluster"
)

// s3Redirect() will return a redirect to target to fulfill the client's request.
// If `alwaysrproxy` or `s3rproxy` build tags are specified, this behavior changes.
// See s3redirect_off.go for more information.
func (p *proxyrunner) s3Redirect(w http.ResponseWriter, r *http.Request, si *cluster.Snode, redirectURL, bck string) {
	s3compat.SendRedirect(w, redirectURL, bck)
}
