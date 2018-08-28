/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package api

import (
	"fmt"
	"path"
)

// URLPath returns a HTTP URL path by joining all segments with "/"
func URLPath(segments ...string) string {
	return path.Join("/", path.Join(segments...))
}

// query-able xactions
func ValidateXactionQueryable(kind string) (errstr string) {
	if kind == XactionRebalance || kind == XactionPrefetch {
		return
	}
	return fmt.Sprintf("Invalid xaction '%s', expecting one of [%s, %s]", kind, XactionRebalance, XactionPrefetch)
}
