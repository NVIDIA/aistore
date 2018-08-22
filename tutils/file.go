/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package tutils provides common low-level utilities for all dfcpub unit and integration tests
package tutils

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"time"
)

func addFileToTar(tw *tar.Writer, path string, fileSize int) error {
	var file bytes.Buffer
	b := make([]byte, fileSize)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	if _, err := file.Write(b); err != nil {
		return err
	}

	header := new(tar.Header)
	header.Name = path
	header.Size = int64(fileSize)
	header.Typeflag = tar.TypeReg
	// write the header to the tarball archive
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	// copy the file data to the tarball
	if _, err := io.Copy(tw, &file); err != nil {
		return err
	}

	return nil
}

func addFileToZip(tw *zip.Writer, path string, fileSize int) error {
	b := make([]byte, fileSize)
	if _, err := rand.Read(b); err != nil {
		return err
	}

	header := new(zip.FileHeader)
	header.Name = path
	header.Comment = path
	header.UncompressedSize64 = uint64(fileSize)
	w, err := tw.CreateHeader(header)
	if err != nil {
		return err
	}

	if _, err := w.Write(b); err != nil {
		return err
	}

	return nil
}

// CreateTarWithRandomFiles creates tar with specified number of files. Tar
// is also gziped if necessary.
func CreateTarWithRandomFiles(tarName string, gzipped bool, fileCnt int, fileSize int) error {
	var (
		gzw *gzip.Writer
		tw  *tar.Writer
	)

	extension := ".tar"
	if gzipped {
		extension += ".gz"
	}

	// set up the output file
	name := tarName + extension
	tarball, err := os.Create(name)
	if err != nil {
		return err
	}
	defer tarball.Close()

	if gzipped {
		// set up the gzip writer
		gzw = gzip.NewWriter(tarball)
		defer gzw.Close()
		tw = tar.NewWriter(gzw)
	} else {
		tw = tar.NewWriter(tarball)
	}
	defer tw.Close()

	for i := 0; i < fileCnt; i++ {
		fileName := fmt.Sprintf("%d.txt", mrand.Int()) // generate random names
		if err := addFileToTar(tw, fileName, fileSize); err != nil {
			return err
		}
	}

	return nil
}

func CreateZipWithRandomFiles(zipName string, fileCnt, fileSize int) error {
	var (
		zw *zip.Writer
	)

	extension := ".zip"
	name := zipName + extension
	z, err := os.Create(name)
	if err != nil {
		return err
	}
	defer z.Close()

	zw = zip.NewWriter(z)
	defer zw.Close()

	for i := 0; i < fileCnt; i++ {
		fileName := fmt.Sprintf("%d.txt", mrand.Int()) // generate random names
		if err := addFileToZip(zw, fileName, fileSize); err != nil {
			return err
		}
	}

	return nil
}

type dummyFile struct {
	name string
	size int64
}

func newDummyFile(name string, size int64) *dummyFile {
	return &dummyFile{
		name: name,
		size: size,
	}
}

func (f *dummyFile) Name() string       { return f.name }
func (f *dummyFile) Size() int64        { return f.size }
func (f *dummyFile) Mode() os.FileMode  { return 0 }
func (f *dummyFile) ModTime() time.Time { return time.Now() }
func (f *dummyFile) IsDir() bool        { return false }
func (f *dummyFile) Sys() interface{}   { return nil }

// GetFileInfosFromTarBuffer returns all file infos contained in buffer which
// assumably is tar or gzipped tar.
func GetFileInfosFromTarBuffer(buffer bytes.Buffer, gzipped bool) ([]os.FileInfo, error) {
	var tr *tar.Reader
	if gzipped {
		gzr, err := gzip.NewReader(&buffer)
		if err != nil {
			return nil, err
		}
		tr = tar.NewReader(gzr)
	} else {
		tr = tar.NewReader(&buffer)
	}

	files := []os.FileInfo{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}

		if err != nil {
			return nil, err
		}

		files = append(files, newDummyFile(hdr.Name, hdr.Size))
	}

	return files, nil
}

// GetFileInfosFromZipBuffer returns all file infos contained in buffer which
// assumably is zip.
func GetFileInfosFromZipBuffer(buffer bytes.Buffer) ([]os.FileInfo, error) {
	reader := bytes.NewReader(buffer.Bytes())
	zr, err := zip.NewReader(reader, int64(buffer.Len()))
	if err != nil {
		return nil, err
	}

	files := []os.FileInfo{}
	for _, file := range zr.File {
		files = append(files, file.FileInfo())
	}

	return files, nil
}
