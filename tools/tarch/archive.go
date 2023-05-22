// Package archive provides common low-level utilities for testing archives
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tarch

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
	"github.com/NVIDIA/aistore/tools/cryptorand"
	"github.com/pierrec/lz4/v3"
)

type (
	FileContent struct {
		Name    string
		Ext     string
		Content []byte
	}
	dummyFile struct {
		name string
		size int64
	}
)

func newDummyFile(name string, size int64) *dummyFile {
	return &dummyFile{
		name: name,
		size: size,
	}
}

func (f *dummyFile) Name() string     { return f.name }
func (f *dummyFile) Size() int64      { return f.size }
func (*dummyFile) Mode() os.FileMode  { return 0 }
func (*dummyFile) ModTime() time.Time { return time.Now() }
func (*dummyFile) IsDir() bool        { return false }
func (*dummyFile) Sys() any           { return nil }

// adds a given buf to a tar or tar.gz or fills-out random fileSize bytes and adds anyway
func addBufferToTar(tw *tar.Writer, path string, fileSize int, buf []byte) (err error) {
	var b bytes.Buffer
	if buf == nil {
		buf = make([]byte, fileSize)
		if _, err = cryptorand.Read(buf); err != nil {
			return
		}
	}
	if _, err = b.Write(buf); err != nil {
		return
	}
	header := new(tar.Header)
	header.Name = path
	header.Size = int64(fileSize)
	header.Typeflag = tar.TypeReg
	// write the header to the tarball archive
	if err = tw.WriteHeader(header); err != nil {
		return
	}
	// copy buffer to the tarball
	_, err = io.CopyBuffer(tw, &b, buf)
	return
}

// adds random-filled fileSize to a zip
func addRndToZip(tw *zip.Writer, path string, fileSize int) (err error) {
	var (
		w io.Writer
		b = make([]byte, fileSize)
	)
	if _, err = cryptorand.Read(b); err != nil {
		return
	}
	header := new(zip.FileHeader)
	header.Name = path
	header.Comment = path
	header.UncompressedSize64 = uint64(fileSize)
	w, err = tw.CreateHeader(header)
	if err != nil {
		return
	}
	_, err = w.Write(b)
	return
}

// CreateTarWithRandomFiles creates tar with specified number of files. Tar is also gzipped if necessary.
func CreateTarWithRandomFiles(tarName, ext string, fileCnt, fileSize int, duplication bool,
	recordExts []string, randomNames []string) error {
	var tw *tar.Writer

	// set up the output file
	tarball, err := cos.CreateFile(tarName)
	if err != nil {
		return err
	}
	defer tarball.Close()

	switch ext {
	case archive.ExtTgz, archive.ExtTarTgz:
		gzw := gzip.NewWriter(tarball)
		defer gzw.Close()
		tw = tar.NewWriter(gzw)
	case archive.ExtTarLz4:
		lzw := lz4.NewWriter(tarball)
		defer lzw.Close()
		tw = tar.NewWriter(lzw)
	default:
		if ext != archive.ExtTar {
			return fmt.Errorf("tools: unexpected file extension %q", ext)
		}
		tw = tar.NewWriter(tarball)
	}
	defer tw.Close()

	var (
		prevFileName string
		dupIndex     = rand.Intn(fileCnt-1) + 1
	)
	if len(recordExts) == 0 {
		recordExts = []string{".txt"}
	}
	for i := 0; i < fileCnt; i++ {
		var randomName int
		if randomNames == nil {
			randomName = rand.Int()
		}
		for _, ext := range recordExts {
			var fileName string
			if randomNames == nil {
				fileName = fmt.Sprintf("%d%s", randomName, ext) // generate random names
				if dupIndex == i && duplication {
					fileName = prevFileName
				}
			} else {
				fileName = randomNames[i]
			}
			if err := addBufferToTar(tw, fileName, fileSize, nil); err != nil {
				return err
			}
			prevFileName = fileName
		}
	}

	return nil
}

func CreateTarWithCustomFilesToWriter(w io.Writer, fileCnt, fileSize int, customFileType, customFileExt string, missingKeys bool) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	for i := 0; i < fileCnt; i++ {
		fileName := fmt.Sprintf("%d", rand.Int()) // generate random names
		if err := addBufferToTar(tw, fileName+".txt", fileSize, nil); err != nil {
			return err
		}
		// If missingKeys enabled we should only add keys randomly
		if !missingKeys || (missingKeys && rand.Intn(2) == 0) {
			var buf []byte
			// random content
			switch customFileType {
			case extract.FormatTypeInt:
				buf = []byte(strconv.Itoa(rand.Int()))
			case extract.FormatTypeString:
				buf = []byte(fmt.Sprintf("%d-%d", rand.Int(), rand.Int()))
			case extract.FormatTypeFloat:
				buf = []byte(fmt.Sprintf("%d.%d", rand.Int(), rand.Int()))
			default:
				return fmt.Errorf("invalid custom file type: %q", customFileType)
			}
			if err := addBufferToTar(tw, fileName+customFileExt, len(buf), buf); err != nil {
				return err
			}
		}
	}
	return nil
}

func CreateTarWithCustomFiles(tarName string, fileCnt, fileSize int, customFileType, customFileExt string, missingKeys bool) error {
	// set up the output file
	tarball, err := cos.CreateFile(tarName)
	if err != nil {
		return err
	}
	defer tarball.Close()

	return CreateTarWithCustomFilesToWriter(tarball, fileCnt, fileSize, customFileType, customFileExt, missingKeys)
}

func CreateZipWithRandomFiles(zipName string, fileCnt, fileSize int, randomNames []string) error {
	var zw *zip.Writer
	z, err := cos.CreateFile(zipName)
	if err != nil {
		return err
	}
	defer z.Close()

	zw = zip.NewWriter(z)
	defer zw.Close()

	for i := 0; i < fileCnt; i++ {
		var fileName string
		if randomNames == nil {
			fileName = fmt.Sprintf("%d.txt", rand.Int()) // generate random names
		} else {
			fileName = randomNames[i]
		}
		if err := addRndToZip(zw, fileName, fileSize); err != nil {
			return err
		}
	}

	return nil
}

// GetFileInfosFromTarBuffer returns all file infos contained in buffer which
// presumably is tar or gzipped tar.
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

	var files []os.FileInfo //nolint:prealloc // cannot determine the size
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
// presumably is tar or gzipped tar.
func GetFilesFromTarBuffer(buffer bytes.Buffer, extension string) ([]FileContent, error) {
	tr := tar.NewReader(&buffer)

	var files []FileContent //nolint:prealloc // cannot determine the size
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}

		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		fExt := extract.Ext(hdr.Name)
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
// presumably is zip.
func GetFileInfosFromZipBuffer(buffer bytes.Buffer) ([]os.FileInfo, error) {
	reader := bytes.NewReader(buffer.Bytes())
	zr, err := zip.NewReader(reader, int64(buffer.Len()))
	if err != nil {
		return nil, err
	}

	files := make([]os.FileInfo, len(zr.File))
	for idx, file := range zr.File {
		files[idx] = file.FileInfo()
	}

	return files, nil
}
