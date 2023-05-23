// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */

package archive

import (
	"archive/tar"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
)

const TarBlockSize = 512 // Size of each block in a tar stream

// package-local copy of config feature flags (a change requires restart)
var features feat.Flags

func Init(f feat.Flags) { features = f }

// set auxiliary bits in TAR header
// common for all ais-created/appended TARs
// - currently, not using os.Getuid/gid (or user.Current) to set Uid/Gid, and
// - not calling standard tar.FileInfoHeader(finfo-of-the-file-to-archive) as well
// - see also: /usr/local/go/src/archive/tar/common.go
func SetAuxTarHeader(hdr *tar.Header) {
	hdr.Mode = int64(cos.PermRWRR)
}
