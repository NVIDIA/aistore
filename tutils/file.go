// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

var trashPattern = "/" + fs.TrashDir + "/"

type (
	FileContent struct {
		Name    string
		Ext     string
		Content []byte
	}

	DirTreeDesc struct {
		InitDir string // directory where the tree is created (can be empty)
		Dirs    int    // number of (initially empty) directories at each depth (we recurse into single directory at each depth)
		Files   int    // number of files at each depth
		Depth   int    // depth of tree/nesting
		Empty   bool   // determines if there is a file somewhere in the directories
	}
)

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

	var files []os.FileInfo // nolint:prealloc // cannot determine the size
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

	var files []FileContent // nolint:prealloc // cannot determine the size
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

func RandomObjDir(dirLen, maxDepth int) (dir string) {
	depth := rand.Intn(maxDepth)
	for i := 0; i < depth; i++ {
		dir = filepath.Join(dir, GenRandomString(dirLen))
	}
	return
}

func SetXattrCksum(fqn string, cksum *cmn.Cksum, t cluster.Target) error {
	lom := &cluster.LOM{T: t, FQN: fqn}
	_ = lom.Init(cmn.Bck{})
	_ = lom.LoadMetaFromFS()

	lom.SetCksum(cksum)
	return lom.Persist()
}

func CheckPathExists(t *testing.T, path string, dir bool) {
	if fi, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else {
		if dir && !fi.IsDir() {
			t.Fatalf("expected path %q to be directory", path)
		} else if !dir && fi.IsDir() {
			t.Fatalf("expected path %q to not be directory", path)
		}
	}
}

func CheckPathNotExists(t *testing.T, path string) {
	if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func PrepareDirTree(tb testing.TB, desc DirTreeDesc) (string, []string) {
	fileNames := make([]string, 0, 100)
	topDirName, err := ioutil.TempDir(desc.InitDir, "")
	tassert.CheckFatal(tb, err)

	nestedDirectoryName := topDirName
	for depth := 1; depth <= desc.Depth; depth++ {
		names := make([]string, 0, desc.Dirs)
		for i := 1; i <= desc.Dirs; i++ {
			name, err := ioutil.TempDir(nestedDirectoryName, "")
			tassert.CheckFatal(tb, err)
			names = append(names, name)
		}
		for i := 1; i <= desc.Files; i++ {
			f, err := ioutil.TempFile(nestedDirectoryName, "")
			tassert.CheckFatal(tb, err)
			fileNames = append(fileNames, f.Name())
			f.Close()
		}
		sort.Strings(names)
		if desc.Dirs > 0 {
			// We only recurse into last directory.
			nestedDirectoryName = names[len(names)-1]
		}
	}

	if !desc.Empty {
		f, err := ioutil.TempFile(nestedDirectoryName, "")
		tassert.CheckFatal(tb, err)
		fileNames = append(fileNames, f.Name())
		f.Close()
	}
	return topDirName, fileNames
}

func IsTrashDir(path string) bool {
	return strings.Contains(path, trashPattern)
}

func FilesEqual(file1, file2 string) (bool, error) {
	f1, err := ioutil.ReadFile(file1)
	if err != nil {
		return false, err
	}
	f2, err := ioutil.ReadFile(file2)
	if err != nil {
		return false, err
	}
	return bytes.Equal(f1, f2), nil
}
