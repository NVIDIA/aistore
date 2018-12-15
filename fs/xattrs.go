/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
)

// helper: get checksum; only xxhash is currently supported/expected
func GetXattrCksum(fqn string, algo string) (cksum, errstr string) {
	var b []byte
	cmn.Assert(algo == cmn.ChecksumXXHash, fmt.Sprintf("Unsupported checksum algorithm '%s'", algo))
	if b, errstr = GetXattr(fqn, cmn.XattrXXHashVal); errstr != "" {
		return
	}
	if b == nil {
		glog.Warningf("%s is not checksummed", fqn)
		return
	}
	cksum = string(b)
	return
}
