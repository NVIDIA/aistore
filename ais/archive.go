// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"archive/tar"
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func extractTar(reader io.Reader, relname string, lom *cluster.LOM) (*io.LimitedReader, error) {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				err = cmn.NewNotFoundError("file %q in archive %q/%q", relname, lom.Bucket(), lom.ObjName)
			}
			return nil, err
		}
		if hdr.Name != relname {
			continue
		}
		return &io.LimitedReader{R: reader, N: hdr.Size}, nil
	}
}
