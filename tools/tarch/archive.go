// Package archive provides common low-level utilities for testing archives
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tarch

import (
	"archive/tar"
	"bytes"
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

func addBufferToArch(aw archive.Writer, path string, fileSize int, buf []byte) (err error) {
	if buf == nil {
		buf = make([]byte, fileSize)
		if _, err = cryptorand.Read(buf); err != nil {
			return
		}
	}
	reader := bytes.NewBuffer(buf)
	oah := cos.SimpleOAH{Size: int64(fileSize)}
	return aw.Write(path, oah, reader)
}

func CreateArchRandomFiles(shardName string, tarFormat tar.Format, ext string, fileCnt, fileSize int,
	dup bool, recExts, randNames []string) error {
	wfh, err := cos.CreateFile(shardName)
	if err != nil {
		return err
	}

	aw := archive.NewWriter(ext, wfh, nil, &archive.Opts{TarFormat: tarFormat})
	defer func() {
		aw.Fini()
		wfh.Close()
	}()

	var (
		prevFileName string
		dupIndex     = rand.Intn(fileCnt-1) + 1
	)
	if len(recExts) == 0 {
		recExts = []string{".txt"}
	}
	for i := 0; i < fileCnt; i++ {
		var randomName int
		if randNames == nil {
			randomName = rand.Int()
		}
		for _, ext := range recExts {
			var fileName string
			if randNames == nil {
				fileName = fmt.Sprintf("%d%s", randomName, ext) // generate random names
				if dupIndex == i && dup {
					fileName = prevFileName
				}
			} else {
				fileName = randNames[i]
			}
			if err := addBufferToArch(aw, fileName, fileSize, nil); err != nil {
				return err
			}
			prevFileName = fileName
		}
	}
	return nil
}

func CreateArchCustomFilesToW(w io.Writer, tarFormat tar.Format, ext string, fileCnt, fileSize int,
	customFileType, customFileExt string, missingKeys bool) error {
	aw := archive.NewWriter(ext, w, nil, &archive.Opts{TarFormat: tarFormat})
	defer aw.Fini()
	for i := 0; i < fileCnt; i++ {
		fileName := fmt.Sprintf("%d", rand.Int()) // generate random names
		if err := addBufferToArch(aw, fileName+".txt", fileSize, nil); err != nil {
			return err
		}
		// If missingKeys enabled we should only add keys randomly
		if !missingKeys || (missingKeys && rand.Intn(2) == 0) {
			var buf []byte
			// random content
			switch customFileType {
			case extract.ContentKeyInt:
				buf = []byte(strconv.Itoa(rand.Int()))
			case extract.ContentKeyString:
				buf = []byte(fmt.Sprintf("%d-%d", rand.Int(), rand.Int()))
			case extract.ContentKeyFloat:
				buf = []byte(fmt.Sprintf("%d.%d", rand.Int(), rand.Int()))
			default:
				return fmt.Errorf("invalid custom file type: %q", customFileType)
			}
			if err := addBufferToArch(aw, fileName+customFileExt, len(buf), buf); err != nil {
				return err
			}
		}
	}
	return nil
}

func CreateArchCustomFiles(shardName string, tarFormat tar.Format, ext string, fileCnt, fileSize int,
	customFileType, customFileExt string, missingKeys bool) error {
	wfh, err := cos.CreateFile(shardName)
	if err != nil {
		return err
	}
	defer wfh.Close()
	return CreateArchCustomFilesToW(wfh, tarFormat, ext, fileCnt, fileSize, customFileType, customFileExt, missingKeys)
}

func newArchReader(mime string, buffer *bytes.Buffer) (ar archive.Reader, err error) {
	if mime == archive.ExtZip {
		// zip is special
		readerAt := bytes.NewReader(buffer.Bytes())
		ar, err = archive.NewReader(mime, readerAt, int64(buffer.Len()))
	} else {
		ar, err = archive.NewReader(mime, buffer)
	}
	return
}

func GetFilesFromArchBuffer(mime string, buffer bytes.Buffer, extension string) ([]FileContent, error) {
	var (
		files   = make([]FileContent, 0, 10)
		ar, err = newArchReader(mime, &buffer)
	)
	if err != nil {
		return nil, err
	}
	rcb := func(filename string, reader cos.ReadCloseSizer, hdr any) (bool, error) {
		var (
			buf bytes.Buffer
			ext = cos.Ext(filename)
		)
		defer reader.Close()
		if extension == ext {
			if _, err := io.Copy(&buf, reader); err != nil {
				return true, err
			}
		}
		files = append(files, FileContent{Name: filename, Ext: ext, Content: buf.Bytes()})
		return false, nil
	}

	_, err = ar.Range("", rcb)
	return files, err
}

func GetFileInfosFromArchBuffer(buffer bytes.Buffer, mime string) ([]os.FileInfo, error) {
	var (
		files   = make([]os.FileInfo, 0, 10)
		ar, err = newArchReader(mime, &buffer)
	)
	if err != nil {
		return nil, err
	}
	rcb := func(filename string, reader cos.ReadCloseSizer, hdr any) (bool, error) {
		files = append(files, newDummyFile(filename, reader.Size()))
		reader.Close()
		return false, nil
	}
	_, err = ar.Range("", rcb)
	return files, err
}

///////////////
// dummyFile //
///////////////

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
