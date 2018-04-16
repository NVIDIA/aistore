/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"golang.org/x/net/webdav"
)

func TestName(t *testing.T) {
	n := "/"
	o, err := newFile(n)
	if err != nil || o.typ != Root {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket/"
	o, err = newFile(n)
	if err != nil || o.typ != Bucket || o.bucket != "bucket" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket"
	o, err = newFile(n)
	if err != nil || o.typ != Bucket || o.bucket != "bucket" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket/dir/"
	o, err = newFile(n)
	if err != nil || o.typ != Directory || o.bucket != "bucket" || o.prefix != "dir/" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket/dir1/dir2/"
	o, err = newFile(n)
	if err != nil || o.typ != Directory || o.bucket != "bucket" || o.prefix != "dir1/dir2/" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket/file"
	o, err = newFile(n)
	if err != nil || o.typ != Object || o.bucket != "bucket" || o.prefix != "file" ||
		o.fi.name != "file" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket/dir/file"
	o, err = newFile(n)
	if err != nil || o.typ != Object || o.bucket != "bucket" || o.prefix != "dir/file" ||
		o.fi.name != "file" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "/bucket/dir1/dir2/file"
	o, err = newFile(n)
	if err != nil || o.typ != Object || o.bucket != "bucket" || o.prefix != "dir1/dir2/file" ||
		o.fi.name != "file" {
		t.Fatalf("Failed to parse %s, %v", n, o)
	}

	n = "bucket/dir/file"
	o, err = newFile(n)
	if err == nil {
		t.Fatalf("Successfully parsed bad name %s, %v", n, o)
	}

	n = "//"
	o, err = newFile(n)
	if err == nil {
		t.Fatalf("Successfully parsed bad name %s, %v", n, o)
	}

	n = "/bucket/dir//dir/file/"
	o, err = newFile(n)
	if err == nil {
		t.Fatalf("Successfully parsed bad name %s, %v", n, o)
	}
}

func TestGroup(t *testing.T) {
	act := groupBucketEntries(t, "", []*dfc.BucketEntry{
		{Name: "dir/file1", Size: 16},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	act = groupBucketEntries(t, "", []*dfc.BucketEntry{
		{Name: "dir/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
		{Name: "dir/file1", Size: 16},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	act = groupBucketEntries(t, "", []*dfc.BucketEntry{
		{Name: "dir/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
		{Name: "file1", Size: 164},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})

	act = groupBucketEntries(t, "", []*dfc.BucketEntry{
		{Name: "dir/file1", Size: 16},
		{Name: "file1", Size: 164},
		{Name: "dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})

	act = groupBucketEntries(t, "", []*dfc.BucketEntry{
		{Name: "file1", Size: 164},
		{Name: "dir/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	act = groupBucketEntries(t, "", []*dfc.BucketEntry{
		{Name: "file1", Size: 164},
		{Name: "dir/dir2/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	act = groupBucketEntries(t, "loader/", []*dfc.BucketEntry{
		{Name: "loader/file1", Size: 164},
		{Name: "loader/dir/dir2/file1", Size: 16},
		{Name: "loader/dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	objs := []*dfc.BucketEntry{
		{Name: "file1", Size: 164},
		{Name: "file1/dir2/file1", Size: 16},
	}

	_, err := group(objs, "")
	if err == nil {
		t.Fatalf("Failed to detect duplicate dir and file %v", objs)
	}

	objs = []*dfc.BucketEntry{
		{Name: "file1", Size: 164},
		{Name: "file1", Size: 16},
	}

	_, err = group(objs, "")
	if err == nil {
		t.Fatalf("Failed to detect duplicate file keys %v", objs)
	}
}

func groupBucketEntries(t *testing.T, prefix string, entries []*dfc.BucketEntry) []os.FileInfo {
	act, err := group(entries, prefix)
	if err != nil {
		t.Fatalf("Failed to group %v", entries)
	}

	return act
}

func cmpFileInfos(t *testing.T, act []os.FileInfo, exp []os.FileInfo) {
	if len(exp) != len(act) {
		t.Fatalf("FileInfo compare failed: act = %v, exp= %v", act, exp)
	}

	for i := 0; i < len(exp); i++ {
		if exp[i].Name() != act[i].Name() || exp[i].Size() != act[i].Size() {
			t.Fatalf("FileInfo compare failed: act = %v, exp= %v", act[i], exp[i])
		}

		if !exp[i].ModTime().IsZero() &&
			(exp[i].ModTime().Before(startTime) || exp[i].ModTime().After(time.Now())) {
			t.Fatalf("FileInfo compare failed: exp= %v", exp[i])
		}
	}
}

const (
	rootDir        = "/tmp/dfc"
	bucket         = "webdavTestBucket"
	bucketFullName = "/" + bucket + "/"
	file1          = bucketFullName + "testfile1"
	file2          = bucketFullName + "testfile2"
	file3          = bucketFullName + "dir1/testfile"
	file4          = bucketFullName + "dir1/dir2/testfile"
	file5          = bucketFullName + "dir2/dir/testfile"
	file6          = bucketFullName + "dir3/dir/testfile"
	file7          = bucketFullName + "dir"        // file name has same prefix as directories
	file8          = bucketFullName + "testfile/x" // dir name has same prefix as files
	content1       = "Hello world from webdav!"
	content2       = "Hello world from DFC!"
)

var (
	rootDirExists = false
	testBuf       = make([]byte, 1024)
	startTime     = time.Now()
)

// doesBucketExist reads all buckets by reading root directory, returns true if the bucket's
// name is included in the list.
func doesBucketExist(t *testing.T, fs webdav.FileSystem, bucket string) bool {
	buckets, err := readDir(t, fs, "/")
	if err != nil {
		return false // Note: Assume read error = bucket doesn't exist
	}

	for _, b := range buckets {
		if b.Name() == bucket {
			return true
		}
	}

	return false
}

// readTestRoot reads the test root directory and returns a list of files/directories in the directory.
// if the directory doesn't not exists, it is created (and removed after test)
func readTestRoot(t *testing.T) []string {
	fi, err := os.Stat(rootDir)
	if err != nil {
		e, ok := err.(*os.PathError)
		if !ok || e.Err != os.ErrNotExist {
			t.Fatalf("Failed to stat test root, err = %v", err)
		}
	} else {
		rootDirExists = true
	}

	if !fi.IsDir() {
		t.Fatalf("%s exists but not a directory", rootDir)
	}

	var files []string
	fis, err := ioutil.ReadDir(rootDir)
	if err != nil {
		t.Fatalf("Failed to read test directory, err = %v", err)
	}

	for _, file := range fis {
		files = append(files, file.Name())
	}

	return files
}

// put opens an object and writes a string at an offset (relative to begin of file)
func put(t *testing.T, fs webdav.FileSystem, name string, offset int64, content string) {
	f, err := fs.OpenFile(nil, name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Fatalf("Failed to create file, err = %v", err)
	}

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		t.Fatalf("Failed to seek, err = %v", err)
	}

	n, err := f.Write([]byte(content))
	if n != len(content) {
		t.Fatalf("Failed to write to file, err = %v", err)
	}

	f.Close()
	if err != nil {
		t.Fatalf("Failed to close file, err = %v", err)
	}
}

// get reads a file and returns its content as a string
func get(t *testing.T, fs webdav.FileSystem, name string) string {
	f, err := fs.OpenFile(nil, name, os.O_RDONLY, os.ModePerm)
	if err != nil {
		t.Fatalf("Failed to open file, err = %v", err)
	}

	n, err := f.Read(testBuf)
	if err != nil {
		t.Fatalf("Failed to read file, err = %v", err)
	}

	f.Close()
	if err != nil {
		t.Fatalf("Failed to close file, err = %v", err)
	}

	return string(testBuf[:n])
}

func readDir(t *testing.T, fs webdav.FileSystem, bucketFullName string) ([]os.FileInfo, error) {
	dir, err := fs.OpenFile(nil, bucketFullName, 0 /* flag */, 0 /* perm */)
	if err != nil {
		t.Fatalf("Failed to open root")
	}

	return dir.Readdir(100)
}

// Note: A DFC instance is required in order to run this test.
// This requirement can be removed after proxy/target can be started in unit test
func TestFS(t *testing.T) {
	existingFilesInTestRoot := readTestRoot(t)
	fs := NewFS(url.URL{Scheme: "http", Host: "127.0.0.1:8080"}, rootDir)

	// clean up
	defer func() {
		err := fs.RemoveAll(nil, bucketFullName)
		if err != nil {
			t.Fatalf("Failed to remove test bucket, err = %v", err)
		}

		// make sure all webdav created temporary files are removed.
		if !rootDirExists {
			os.Remove(rootDir)
		} else {
			files := readTestRoot(t)
			if !reflect.DeepEqual(existingFilesInTestRoot, files) {
				t.Fatalf("Test failed to clean up all temporary files")
			}
		}
	}()

	// stat, open and close root
	fi, err := fs.Stat(nil, "/")
	if err != nil {
		t.Fatalf("Failed to stat root, err = %v", err)
	}

	if !fi.IsDir() {
		t.Fatalf("Stat root returned not directory")
	}

	f, err := fs.OpenFile(nil, "/", 0, 0)
	if err != nil {
		t.Fatalf("Failed to open root, err = %v", err)
	}

	f.Close()

	if doesBucketExist(t, fs, bucket) {
		t.Fatalf("webdav test bucket already exists")
	}

	// creates test bucket
	err = fs.Mkdir(nil, bucketFullName, os.ModePerm)
	if err != nil {
		t.Fatalf("Failed to create bucket, err = %v", err)
	}

	// verify bucket now exists
	if !doesBucketExist(t, fs, bucket) {
		t.Fatalf("Newly created bucket doesn't exist")
	}

	// creates existing bucket should fail
	err = fs.Mkdir(nil, bucketFullName, os.ModePerm)
	if err == nil {
		t.Fatalf("Failed to find existing bucket, err = %v", err)
	}

	{
		// put new object
		put(t, fs, file1, 0, content1)

		err := fs.RemoveAll(nil, file1[0:len(file1)-1])
		if err == nil {
			t.Fatalf("Opened non existing object which has prefix of an existing object as its name")
		}

		// open an object and close it without touching it(read or write or seek)
		f, err := fs.OpenFile(nil, file1, 0, 0)
		if err != nil {
			t.Fatalf("Failed to open root, err = %v", err)
		}

		f.Close()

		// get and verify content
		read := get(t, fs, file1)
		if strings.Compare(read, content1) != 0 {
			t.Fatalf("Failed to verify test file's content, exp = %s, act = %s",
				content1, read)
		}

		// append
		put(t, fs, file1, int64(len(content1)), content2)
		read = get(t, fs, file1)
		if strings.Compare(read, content1+content2) != 0 {
			t.Fatalf("Failed to verify test file's content, exp = %s, act = %s",
				content1+content2, read)
		}

		// overwrite
		put(t, fs, file1, int64(len(content1)), "h")
		read = get(t, fs, file1)
		if strings.Compare(read, content1+content2) <= 0 {
			t.Fatalf("Failed to verify test file's content")
		}

		if strings.Compare(strings.ToUpper(read), strings.ToUpper(content1+content2)) != 0 {
			t.Fatalf("Failed to verify test file's content, exp = %s, act = %s",
				content1+content2, read)
		}
	}

	{
		// readdir
		put(t, fs, file2, 0, content1)
		put(t, fs, file3, 0, content2)
		put(t, fs, file4, 0, content1)
		put(t, fs, file5, 0, content2)
		put(t, fs, file6, 0, content1)

		// stat a directory
		fi, err = fs.Stat(nil, bucketFullName+"dir1")
		if err != nil {
			t.Fatalf("Failed to stat directory, err = %v", err)
		}

		fis, _ := readDir(t, fs, bucketFullName)
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir1", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir3", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile1", size: int64(len(content1) + len(content2)), mode: defaultFileMode},
			&fileInfo{name: "testfile2", size: int64(len(content1)), mode: defaultFileMode},
		})

		fis, _ = readDir(t, fs, bucketFullName+"dir1/")
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile", size: int64(len(content2)), mode: defaultFileMode},
		})

		fis, _ = readDir(t, fs, bucketFullName+"dir3/dir/")
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "testfile", size: int64(len(content1)), mode: defaultFileMode},
		})

		fis, _ = readDir(t, fs, bucketFullName+"dir2/")
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		})
	}

	{
		// rename
		fs.Rename(nil, file1, file1+"_rn")
		fis, _ := readDir(t, fs, bucketFullName)
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir1", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir3", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile1_rn", size: int64(len(content1) + len(content2)), mode: defaultFileMode},
			&fileInfo{name: "testfile2", size: int64(len(content1)), mode: defaultFileMode},
		})

		err = fs.Rename(nil, bucketFullName, "anything")
		if err == nil {
			t.Fatalf("Rename a bucket should fail")
		}

		err = fs.Rename(nil, bucketFullName+"dir1/", "anything")
		if err == nil {
			t.Fatalf("Rename a directory should fail")
		}

		err = fs.Rename(nil, bucketFullName+"dir1", "anything")
		if err == nil {
			t.Fatalf("Rename a directory should fail")
		}

		err = fs.Rename(nil, bucketFullName+"404notfound", "anything")
		if err == nil {
			t.Fatalf("Rename a non existing file should fail")
		}

		err = fs.Rename(nil, file2, bucketFullName+"dir1")
		if err == nil {
			t.Fatalf("Rename a file to an existing directory name should fail")
		}

		err = fs.Rename(nil, file5, file6)
		if err == nil {
			t.Fatalf("Rename a file to an existing file name should fail")
		}
	}

	{
		// read
		f, err := fs.OpenFile(nil, file2, os.O_RDONLY, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s", file2)
		}

		n, err := f.Read(testBuf)
		if err != nil {
			t.Fatalf("Failed to read file, err = %v", err)
		}

		if string(testBuf[:n]) != content1 {
			t.Fatalf("Failed to verify file content, exp = %s act = %s", string(testBuf[:n]), content1)
		}

		f.Close()

		// append
		f, err = fs.OpenFile(nil, file2, os.O_RDWR, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s", file2)
		}

		_, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatalf("Failed to seek to end of file, err = %v", err)
		}

		n, err = f.Write([]byte(content2))
		if err != nil || n != len(content2) {
			t.Fatalf("Failed to append file, err = %v", err)
		}

		f.Seek(0, io.SeekStart)
		n, _ = f.Read(testBuf)
		if string(testBuf[:n]) != content1+content2 {
			t.Fatalf("Failed to verify file content, exp = %s act = %s", string(testBuf[:n]),
				content1+content2)
		}

		f.Close()

		// overwrite
		f, err = fs.OpenFile(nil, file2, os.O_RDWR, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s", file2)
		}

		n, err = f.Write([]byte(strings.ToLower(content1)))
		if err != nil || n != len(content1) {
			t.Fatalf("Failed to append file, err = %v", err)
		}

		f.Seek(0, io.SeekStart)
		n, _ = f.Read(testBuf)
		if string(testBuf[:n]) != strings.ToLower(content1)+content2 {
			t.Fatalf("Failed to verify file content, exp = %s act = %s", string(testBuf[:n]),
				strings.ToLower(content1)+content2)
		}

		f.Close()
	}

	{
		// remove
		err = fs.RemoveAll(nil, "/")
		if err == nil {
			t.Fatalf("Remove root should fail")
		}

		err = fs.RemoveAll(nil, file1)
		if err == nil {
			t.Fatalf("Remove a non existing file should fail")
		}

		fs.RemoveAll(nil, file2)
		fis, _ := readDir(t, fs, bucketFullName)
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir1", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir3", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile1_rn", size: int64(len(content1) + len(content2)), mode: defaultFileMode},
		})

		fs.RemoveAll(nil, bucketFullName+"dir1/dir2/")
		fis, _ = readDir(t, fs, bucketFullName)
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir1", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir3", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile1_rn", size: int64(len(content1) + len(content2)), mode: defaultFileMode},
		})
	}

	{
		// same prefix
		put(t, fs, file7, 0, content1)
		readDir(t, fs, bucketFullName)

		put(t, fs, file8, 0, content1)
		readDir(t, fs, bucketFullName)
	}
}
