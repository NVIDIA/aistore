// Package archive
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/vmihailenco/msgpack"
)

// archived file
type Entry struct {
	Name string
	Size uint64 // uncompressed size
}

func List(fqn string) ([]*Entry, error) {
	var arch string
	for _, ext := range FileExtensions {
		if strings.HasSuffix(fqn, ext) {
			arch = ext
			break
		}
	}
	if arch == "" {
		return nil, nil
	}
	// list the archive content
	var (
		archList []*Entry
		finfo    os.FileInfo
	)
	f, err := os.Open(fqn)
	if err == nil {
		switch arch {
		case ExtTar:
			archList, err = listTar(f)
		case ExtTgz, ExtTarTgz:
			archList, err = listTgz(f)
		case ExtZip:
			finfo, err = os.Stat(fqn)
			if err == nil {
				archList, err = listZip(f, finfo.Size())
			}
		case ExtMsgpack:
			archList, err = listMsgpack(f)
		default:
			debug.Assert(false, arch)
		}
	}
	f.Close()
	if err != nil {
		return nil, err
	}
	// Files in archive can be in arbitrary order, but paging requires them sorted
	sort.Slice(archList, func(i, j int) bool { return archList[i].Name < archList[j].Name })
	return archList, nil
}

// list: tar, tgz, zip, msgpack
func listTar(reader io.Reader) ([]*Entry, error) {
	fileList := make([]*Entry, 0, 8)
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return fileList, nil
			}
			return nil, err
		}
		if hdr.FileInfo().IsDir() {
			continue
		}
		e := &Entry{Name: hdr.Name, Size: uint64(hdr.Size)}
		fileList = append(fileList, e)
	}
}

func listTgz(reader io.Reader) ([]*Entry, error) {
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return listTar(gzr)
}

func listZip(readerAt cos.ReadReaderAt, size int64) ([]*Entry, error) {
	zr, err := zip.NewReader(readerAt, size)
	if err != nil {
		return nil, err
	}
	fileList := make([]*Entry, 0, 8)
	for _, f := range zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		e := &Entry{
			Name: f.FileHeader.Name,
			Size: f.FileHeader.UncompressedSize64,
		}
		fileList = append(fileList, e)
	}
	return fileList, nil
}

func listMsgpack(readerAt cos.ReadReaderAt) ([]*Entry, error) {
	var (
		dst any
		dec = msgpack.NewDecoder(readerAt)
	)
	err := dec.Decode(&dst)
	if err != nil {
		return nil, err
	}
	out, ok := dst.(map[string]any)
	if !ok {
		debug.FailTypeCast(dst)
		return nil, fmt.Errorf("unexpected type (%T)", dst)
	}
	fileList := make([]*Entry, 0, len(out))
	for fullname, v := range out {
		vout := v.([]byte)
		e := &Entry{Name: fullname, Size: uint64(len(vout))}
		fileList = append(fileList, e)
	}
	return fileList, nil
}
