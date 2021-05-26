// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"archive/tar"
	"io"
)

func extractTar(reader io.Reader, relname string) (*io.LimitedReader, error) {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		if hdr.Name != relname {
			continue
		}
		return &io.LimitedReader{R: reader, N: hdr.Size}, nil
	}
}
