// +build alwaysrproxy s3rproxy

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
)

func init() {
	glog.Errorln("Warning: Using the `s3rproxy` build tag will reduce performance for s3 requests!")
}

// If `alwaysrproxy` or `s3rproxy` is specified, s3Redirect() will perform a
// reverse proxy request to the target instead of returning a redirect to the
// target. This will significantly impact performance, but allows for better
// compatibility with services that do not support redirection.
func (p *proxyrunner) s3Redirect(w http.ResponseWriter, r *http.Request, si *cluster.Snode, redirectURL, bck string) {
	p.reverseNodeRequest(w, r, si)
}
