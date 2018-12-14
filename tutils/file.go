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
	"path/filepath"
	"strconv"
	"time"

	"github.com/NVIDIA/dfcpub/dsort/extract"
)

type (
	FileContent struct {
		Name    string
		Ext     string
		Content []byte
	}
)

func addFileToTar(tw *tar.Writer, path string, fileSize int, buf []byte) error {
	var file bytes.Buffer
	if buf == nil {
		buf = make([]byte, fileSize)
		if _, err := rand.Read(buf); err != nil {
			return err
		}
	}

	if _, err := file.Write(buf); err != nil {
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
		if err := addFileToTar(tw, fileName, fileSize, nil); err != nil {
			return err
		}
	}

	return nil
}

func CreateTarWithCustomFiles(tarName string, fileCnt, fileSize int, customFileType string, customFileExt string) error {
	// set up the output file
	extension := ".tar"
	name := tarName + extension
	tarball, err := os.Create(name)
	if err != nil {
		return err
	}
	defer tarball.Close()
	tw := tar.NewWriter(tarball)
	defer tw.Close()

	for i := 0; i < fileCnt; i++ {
		fileName := fmt.Sprintf("%d", mrand.Int()) // generate random names
		if err := addFileToTar(tw, fileName+".txt", fileSize, nil); err != nil {
			return err
		}

		var buf []byte
		// random content
		switch customFileType {
		case extract.FormatTypeInt:
			buf = []byte(strconv.Itoa(mrand.Int()))
		case extract.FormatTypeString:
			buf = []byte(fmt.Sprintf("%d-%d", mrand.Int(), mrand.Int()))
		case extract.FormatTypeFloat:
			buf = []byte(fmt.Sprintf("%d.%d", mrand.Int(), mrand.Int()))
		default:
			return fmt.Errorf("invalid custom file type: %q", customFileType)
		}

		if err := addFileToTar(tw, fileName+customFileExt, len(buf), buf); err != nil {
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

// GetFilesFromTarBuffer returns all file infos contained in buffer which
// assumably is tar or gzipped tar.
func GetFilesFromTarBuffer(buffer bytes.Buffer, extension string) ([]FileContent, error) {
	tr := tar.NewReader(&buffer)

	files := []FileContent{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}

		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		fExt := filepath.Ext(hdr.Name)
		if extension == fExt {
			if _, err := io.CopyN(&buf, tr, hdr.Size); err != nil {
				return nil, err
			}
		}

		files = append(files, FileContent{Name: hdr.Name, Ext: fExt, Content: buf.Bytes()})
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
