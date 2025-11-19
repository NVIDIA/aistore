// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/pierrec/lz4/v4"
)

// TODO (feature): support non-standard file extensions (see NOTE below)

// archived file entry
type Entry struct {
	Name string
	Size int64 // uncompressed size
}

func List(fqn string) ([]*Entry, error) {
	var (
		lst   []*Entry
		finfo os.FileInfo
	)
	fh, err := os.Open(fqn)
	if err != nil {
		return nil, err
	}
	mime, err := MimeFile(fh, nil /*NOTE: not reading file magic*/, "", fqn)
	if err != nil {
		return nil, err
	}
	switch mime {
	case ExtTar:
		lst, err = lsTar(fh)
	case ExtTgz, ExtTarGz:
		lst, err = lsTgz(fh)
	case ExtZip:
		finfo, err = os.Stat(fqn)
		if err == nil {
			lst, err = lsZip(fh, finfo.Size())
		}
	case ExtTarLz4:
		lst, err = lsLz4(fh)
	default:
		debug.Assert(false, mime)
	}
	cos.Close(fh)
	if err != nil {
		return nil, err
	}
	// paging requires them sorted
	sort.Slice(lst, func(i, j int) bool { return lst[i].Name < lst[j].Name })
	return lst, nil
}

// list: tar, tgz, zip, msgpack
func lsTar(reader io.Reader) (lst []*Entry, _ error) {
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return lst, nil // ok
			}
			return nil, err
		}
		if hdr.FileInfo().IsDir() {
			continue
		}
		e := &Entry{Name: hdr.Name, Size: hdr.Size}
		lst = append(lst, e)
	}
}

func lsTgz(reader io.Reader) ([]*Entry, error) {
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return lsTar(gzr)
}

func lsZip(readerAt cos.ReadReaderAt, size int64) (lst []*Entry, err error) {
	var zr *zip.Reader
	if zr, err = zip.NewReader(readerAt, size); err != nil {
		return
	}
	for _, f := range zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		e := &Entry{
			Name: f.FileHeader.Name,
			Size: int64(f.FileHeader.UncompressedSize64),
		}
		lst = append(lst, e)
	}
	return
}

func lsLz4(reader io.Reader) ([]*Entry, error) {
	lzr := lz4.NewReader(reader)
	return lsTar(lzr)
}

// Split a path at the first archive extension boundary, e.g.:
// "a/b/c/shard.tar/dir/file.bin" -> ("a/b/c/shard.tar", "dir/file.bin")
// "plain/object/path" -> ("plain/object/path", "").
//
// NOTE: Assumes archive extensions (.tar, .zip, etc. `FileExtensions`)
// do not appear in non-archive path components.
// This holds for lsmsg `apc.LsArchDir` results where
// the first matching extension marks the actual archive boundary.

func SplitAtExtension(path string) (shardName, fileName string) {
	if path == "" {
		return "", ""
	}
	for _, ext := range FileExtensions {
		i := strings.Index(path, ext+"/")
		if i < 0 {
			continue
		}
		return path[:i+len(ext)], path[i+len(ext)+1:]
	}
	return path, ""
}
