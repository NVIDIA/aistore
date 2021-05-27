// Package archive provides common low-level utilities for testing archives
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dsort/extract"
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
	if _, err := io.CopyBuffer(tw, &file, buf); err != nil {
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
// is also gzipped if necessary.
func CreateTarWithRandomFiles(tarName string, gzipped bool, fileCnt, fileSize int, duplication bool,
	recordExts []string, randomNames []string) error {
	var (
		gzw *gzip.Writer
		tw  *tar.Writer
	)

	extension := cos.ExtTar
	if gzipped {
		extension = cos.ExtTarTgz
	}

	// set up the output file
	name := tarName + extension
	tarball, err := cos.CreateFile(name)
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

	prevFileName := ""
	dupIndex := rand.Intn(fileCnt-1) + 1

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
			if err := addFileToTar(tw, fileName, fileSize, nil); err != nil {
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
		if err := addFileToTar(tw, fileName+".txt", fileSize, nil); err != nil {
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

			if err := addFileToTar(tw, fileName+customFileExt, len(buf), buf); err != nil {
				return err
			}
		}
	}

	return nil
}

func CreateTarWithCustomFiles(tarName string, fileCnt, fileSize int, customFileType, customFileExt string, missingKeys bool) error {
	// set up the output file
	extension := cos.ExtTar
	name := tarName + extension
	tarball, err := cos.CreateFile(name)
	if err != nil {
		return err
	}
	defer tarball.Close()

	return CreateTarWithCustomFilesToWriter(tarball, fileCnt, fileSize, customFileType, customFileExt, missingKeys)
}

func CreateZipWithRandomFiles(zipName string, fileCnt, fileSize int) error {
	var zw *zip.Writer

	extension := cos.ExtZip
	name := zipName + extension
	z, err := cos.CreateFile(name)
	if err != nil {
		return err
	}
	defer z.Close()

	zw = zip.NewWriter(z)
	defer zw.Close()

	for i := 0; i < fileCnt; i++ {
		fileName := fmt.Sprintf("%d.txt", rand.Int()) // generate random names
		if err := addFileToZip(zw, fileName, fileSize); err != nil {
			return err
		}
	}

	return nil
}
