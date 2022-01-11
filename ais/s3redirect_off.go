//go:build s3rproxy
// +build s3rproxy

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"os"

	"github.com/NVIDIA/aistore/cluster"
)

func init() {
	fmt.Fprintln(os.Stderr, "Warning *****")
	fmt.Fprintln(os.Stderr, "Warning: reverse-proxying datapath requests might adversely affect performance!")
	fmt.Fprintln(os.Stderr, "Warning *****")
}

// Perform reverse-proxy request to a designated clustered node.
// NOTE: it is strongly recommended _not_ to use `s3rproxy` build tag, if possible.
// See also:
// * docs/s3compat.md
// * Makefile (for `s3rproxy` build tag)
// * ais/s3redirect_on.go
func (p *proxy) s3Redirect(w http.ResponseWriter, r *http.Request, si *cluster.Snode, _, _ string) {
	p.reverseNodeRequest(w, r, si)
}
