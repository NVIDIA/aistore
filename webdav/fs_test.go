/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/webdav"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestName(t *testing.T) {
	tcs := []struct {
		name string
		f    *File
		err  error
	}{
		{
			"",
			nil,
			os.ErrInvalid,
		},
		{
			"/",
			&File{
				name:   "/",
				typ:    Root,
				bucket: "",
				path:   "",
				fi: fileInfo{
					name: "",
					mode: defaultDirMode,
				}},
			nil,
		},
		{
			"/bucket",
			&File{
				name:   "/bucket",
				typ:    Bucket,
				bucket: "bucket",
				path:   "",
				fi: fileInfo{
					name: "",
					mode: defaultDirMode,
				}},
			nil,
		},
		{
			"/bucket/dir",
			&File{
				name:   "/bucket/dir",
				typ:    Object,
				bucket: "bucket",
				path:   "",
				fi: fileInfo{
					name: "dir",
					mode: defaultFileMode,
				}},
			nil,
		},
		{
			"/bucket/dir/file",
			&File{
				name:   "/bucket/dir/file",
				typ:    Object,
				bucket: "bucket",
				path:   "dir",
				fi: fileInfo{
					name: "file",
					mode: defaultFileMode,
				}},
			nil,
		},
		{
			"/bucket/../file",
			&File{
				name:   "/bucket/../file",
				typ:    Object,
				bucket: "bucket",
				path:   "",
				fi: fileInfo{
					name: "file",
					mode: defaultFileMode,
				}},
			nil,
		},
		{
			"/bucket/../../file",
			&File{
				name:   "/bucket/../../file",
				typ:    Object,
				bucket: "bucket",
				path:   "",
				fi: fileInfo{
					name: "file",
					mode: defaultFileMode,
				}},
			nil,
		},
		{
			"/bucket/dir1/dir2/file",
			&File{
				name:   "/bucket/dir1/dir2/file",
				typ:    Object,
				bucket: "bucket",
				path:   "dir1/dir2",
				fi: fileInfo{
					name: "file",
					mode: defaultFileMode,
				}},
			nil,
		},
		{
			"bucket/dir1/dir2/file",
			nil,
			os.ErrInvalid,
		},
	}

	for _, tc := range tcs {
		f, err := parseResource(tc.name)

		if tc.err != err || !reflect.DeepEqual(f, tc.f) {
			t.Fatalf("Failed to parse resource %s, exp = %v(err %v), act = %v(err %v)", tc.name, tc.f, tc.err, f, err)
		}
	}
}

func TestGroup(t *testing.T) {
	act := groupBucketEntries("", []*cmn.BucketEntry{
		{Name: "dir/file1", Size: 16},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	act = groupBucketEntries("", []*cmn.BucketEntry{
		{Name: "dir/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
		{Name: "dir/file1", Size: 16},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
	})

	act = groupBucketEntries("", []*cmn.BucketEntry{
		{Name: "dir/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
		{Name: "file1", Size: 164},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})

	act = groupBucketEntries("", []*cmn.BucketEntry{
		{Name: "dir/file1", Size: 16},
		{Name: "file1", Size: 164},
		{Name: "dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})

	act = groupBucketEntries("", []*cmn.BucketEntry{
		{Name: "file1", Size: 164},
		{Name: "dir/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})

	act = groupBucketEntries("", []*cmn.BucketEntry{
		{Name: "file1", Size: 164},
		{Name: "dir/dir2/file1", Size: 16},
		{Name: "dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})

	act = groupBucketEntries("loader", []*cmn.BucketEntry{
		{Name: "loader/file1", Size: 164},
		{Name: "loader/dir/dir2/file1", Size: 16},
		{Name: "loader/dir/file2", Size: 160},
	})

	cmpFileInfos(t, act, []os.FileInfo{
		&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
		&fileInfo{name: "file1", size: 164, mode: defaultFileMode},
	})
}

func groupBucketEntries(prefix string, entries []*cmn.BucketEntry) []os.FileInfo {
	act := group(entries, prefix)
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
	dir            = "dir"
	dir1           = "dir1"
	dir2           = "dir2"
	dir3           = "dir3"
	dirTest        = "testfile"
	content1       = "Hello world from webdav!"
	content2       = "Hello world from DFC!"
)

var (
	rootDirExists = false
	testBuf       = make([]byte, 1024)
	startTime     = time.Now()
	file1Path     = path.Join(bucketFullName, "testfile1")
	file2Path     = path.Join(bucketFullName, "testfile2")
	file3Path     = path.Join(bucketFullName, dir1, "testfile")
	file4Path     = path.Join(bucketFullName, dir1, dir2, "testfile")
	file5Path     = path.Join(bucketFullName, dir2, dir, "testfile")
	file6Path     = path.Join(bucketFullName, dir3, dir, "testfile")
	file7Path     = path.Join(bucketFullName, dir)          // file name has same prefix as directories
	file8Path     = path.Join(bucketFullName, dirTest, "x") // dir name has same prefix as files
	dir1Path      = path.Join(bucketFullName, dir1)
	dir2Path      = path.Join(bucketFullName, dir2)
	dir3Path      = path.Join(bucketFullName, dir3)
	dir12Path     = path.Join(bucketFullName, dir1, dir2)
	dir20Path     = path.Join(bucketFullName, dir2, dir)
	dir30Path     = path.Join(bucketFullName, dir3, dir)
	dirTestPath   = path.Join(bucketFullName, dirTest)
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

	fis, err := ioutil.ReadDir(rootDir)
	if err != nil {
		t.Fatalf("Failed to read test directory, err = %v", err)
	}

	files := make([]string, len(fis))
	for idx, file := range fis {
		files[idx] = file.Name()
	}

	return files
}

// put opens an object and writes a string at an offset (relative to begin of file)
func put(t *testing.T, fs webdav.FileSystem, name string, offset int64, content string) {
	f, err := fs.OpenFile(nil, name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Fatalf("Failed to create file %s, err = %v", name, err)
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

func readDir(t *testing.T, fs webdav.FileSystem, pth string) ([]os.FileInfo, error) {
	dir, err := fs.OpenFile(nil, pth, 0 /* flag */, 0 /* perm */)
	if err != nil {
		t.Fatalf("Failed to open directory %s, err = %v", pth, err)
	}

	return dir.Readdir(100)
}

// Note: A DFC instance is required in order to run this test.
// This requirement can be removed after proxy/target can be started in unit test
// Also assumes the localhost:8080 is one of the proxy
func TestFS(t *testing.T) {
	if tutils.DockerRunning() {
		t.Skip("TestFS requires direct access to local filesystem, not compatible with docker")
	}
	leader, err := tutils.GetPrimaryProxy("http://127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}

	leader = strings.TrimPrefix(leader, "http://")
	existingFilesInTestRoot := readTestRoot(t)
	fs := NewFS(url.URL{Scheme: "http", Host: leader}, rootDir)

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

	{
		// basics
		err := fs.Mkdir(nil, "notfullpath", 0)
		if err == nil {
			t.Fatalf("Relative path is not supported")
		}

		_, err = fs.OpenFile(nil, "notfullpath", 0, 0)
		if err == nil {
			t.Fatalf("Relative path is not supported")
		}

		err = fs.RemoveAll(nil, "notfullpath")
		if err == nil {
			t.Fatalf("Relative path is not supported")
		}

		err = fs.Rename(nil, "notfullpath", "")
		if err == nil {
			t.Fatalf("Relative path is not supported")
		}

		_, err = fs.Stat(nil, "notfullpath")
		if err == nil {
			t.Fatalf("Relative path is not supported")
		}
	}

	if fs.Mkdir(nil, separator, os.ModePerm) == nil {
		t.Fatalf("Mkdir of root should fail")
	}

	// stat, open and close root
	fi, err := fs.Stat(nil, "/")
	if err != nil {
		t.Fatalf("Failed to stat root, err = %v", err)
	}

	if !fi.IsDir() {
		t.Fatalf("Stat root returned not directory")
	}

	fi, err = fs.Stat(nil, "/unknownbucket")
	if err == nil {
		t.Fatalf("Stat non-existing bucket should fail")
	}

	f, err := fs.OpenFile(nil, "/", 0, 0)
	if err != nil {
		t.Fatalf("Failed to open root, err = %v", err)
	}

	f.Close()

	if doesBucketExist(t, fs, bucket) {
		t.Fatalf("webdav test bucket already exists")
	}

	if err = fs.Mkdir(nil, bucketFullName, os.ModePerm); err != nil {
		t.Fatalf("Failed to create bucket, err = %v", err)
	}

	if !doesBucketExist(t, fs, bucket) {
		t.Fatalf("Newly created bucket doesn't exist")
	}

	fi, err = fs.Stat(nil, bucketFullName)
	if err != nil || fi.Name() != "" {
		t.Fatalf("Failed to stat bucket %v, %s", err, fi.Name())
	}

	if fs.Mkdir(nil, bucketFullName, os.ModePerm) == nil {
		t.Fatalf("Failed to find existing bucket, err = %v", err)
	}

	_, err = fs.OpenFile(nil, "/404", os.O_RDWR, os.ModePerm)
	if err == nil {
		t.Fatalf("Open non-existing bucket should fail")
	}

	if fs.Mkdir(nil, path.Join(bucketFullName, "404dir/404"), os.ModePerm) == nil {
		t.Fatalf("Mkdir with non-existing path should fail")
	}

	{
		_, err := fs.OpenFile(nil, path.Join(bucketFullName+"404", "file"), 0, 0)
		if err == nil {
			t.Fatalf("Opened file in a non-existing bucket")
		}

		_, err = fs.OpenFile(nil, path.Join(bucketFullName, "dir404", "file"), 0, 0)
		if err == nil {
			t.Fatalf("Opened file in a non-existing directory")
		}

		// put new object
		put(t, fs, file1Path, 0, content1)

		err = fs.RemoveAll(nil, file1Path[0:len(file1Path)-1])
		if err == nil {
			t.Fatalf("Remove non-existing object which has prefix of an existing object as its name")
		}

		// open an object and close it without touching it(read or write or seek)
		f, err = fs.OpenFile(nil, file1Path, 0, 0)
		if err != nil {
			t.Fatalf("Failed to open file, err = %v", err)
		}

		f.Close()

		// get and verify content
		read := get(t, fs, file1Path)
		if strings.Compare(read, content1) != 0 {
			t.Fatalf("Failed to verify test file's content, exp = %s, act = %s",
				content1, read)
		}

		// append
		put(t, fs, file1Path, int64(len(content1)), content2)
		read = get(t, fs, file1Path)
		if strings.Compare(read, content1+content2) != 0 {
			t.Fatalf("Failed to verify test file's content, exp = %s, act = %s",
				content1+content2, read)
		}

		// overwrite
		put(t, fs, file1Path, int64(len(content1)), "h")
		read = get(t, fs, file1Path)
		if strings.Compare(read, content1+content2) <= 0 {
			t.Fatalf("Failed to verify test file's content")
		}

		if strings.Compare(strings.ToUpper(read), strings.ToUpper(content1+content2)) != 0 {
			t.Fatalf("Failed to verify test file's content, exp = %s, act = %s",
				content1+content2, read)
		}
	}

	{
		// readdir/mkdir
		put(t, fs, file2Path, 0, content1)

		err := fs.Mkdir(nil, dir1Path, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to create directory, err = %v", err)
		}

		err = fs.Mkdir(nil, dir1Path, os.ModePerm)
		if err == nil {
			t.Fatalf("Created duplicated directory")
		}

		put(t, fs, file3Path, 0, content2)

		err = fs.Mkdir(nil, dir12Path, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to create directory, err = %v", err)
		}

		err = fs.Mkdir(nil, dir12Path, os.ModePerm)
		if err == nil {
			t.Fatalf("Created duplicated directory")
		}

		put(t, fs, file4Path, 0, content1)
		if fs.Mkdir(nil, file4Path, os.ModePerm) == nil {
			t.Fatalf("Mkdir with existing path should fail")
		}

		fs.Mkdir(nil, dir2Path, os.ModePerm)

		fs.Mkdir(nil, dir20Path, os.ModePerm)
		put(t, fs, file5Path, 0, content2)
		fs.Mkdir(nil, dir3Path, os.ModePerm)
		fs.Mkdir(nil, dir30Path, os.ModePerm)
		put(t, fs, file6Path, 0, content1)

		// stat a directory
		fi, err = fs.Stat(nil, bucketFullName+"dir1")
		if err != nil {
			t.Fatalf("Failed to stat directory, err = %v", err)
		}

		// add afew in memory only directories
		p1 := path.Join(bucketFullName, "dir2inmemdir")
		fs.Mkdir(nil, p1, os.ModePerm)

		fis, _ := readDir(t, fs, bucketFullName)
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir1", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir2inmemdir", size: 0, mode: defaultDirMode},
			&fileInfo{name: "dir3", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile1", size: int64(len(content1) + len(content2)), mode: defaultFileMode},
			&fileInfo{name: "testfile2", size: int64(len(content1)), mode: defaultFileMode},
		})

		fs.RemoveAll(nil, p1)

		fis, _ = readDir(t, fs, bucketFullName+"dir1/")
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir2", size: 0, mode: defaultDirMode},
			&fileInfo{name: "testfile", size: int64(len(content2)), mode: defaultFileMode},
		})

		fis, _ = readDir(t, fs, bucketFullName+"dir3/dir/")
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "testfile", size: int64(len(content1)), mode: defaultFileMode},
		})

		p2 := path.Join(bucketFullName, "dir2/inmemdir2")
		fs.Mkdir(nil, p2, os.ModePerm)
		p3 := path.Join(bucketFullName, "dir2/inmemdir1")
		fs.Mkdir(nil, p3, os.ModePerm)

		fis, _ = readDir(t, fs, bucketFullName+"dir2/")
		cmpFileInfos(t, fis, []os.FileInfo{
			&fileInfo{name: "dir", size: 0, mode: defaultDirMode},
			&fileInfo{name: "inmemdir1", size: 0, mode: defaultDirMode},
			&fileInfo{name: "inmemdir2", size: 0, mode: defaultDirMode},
		})

		fs.RemoveAll(nil, p2)
		fs.RemoveAll(nil, p3)

		fs.Mkdir(nil, path.Join(bucketFullName, "dir2/a"), os.ModePerm)
		fs.Mkdir(nil, path.Join(bucketFullName, "dir2/a/b"), os.ModePerm)
		fs.Mkdir(nil, path.Join(bucketFullName, "dir2/a/b/c"), os.ModePerm)
		fs.Mkdir(nil, path.Join(bucketFullName, "dir2/a/b/c/d"), os.ModePerm)
		fis, err = readDir(t, fs, bucketFullName+"dir2/a")
		if err != nil {
			t.Fatal("Failed to read multi level directory")
		}
	}

	{
		// rename
		err = fs.Rename(nil, file1Path, path.Join(bucketFullName+"404", "file"))
		if err == nil {
			t.Fatalf("Rename a file to a non-existing bucket should fail")
		}

		err = fs.Rename(nil, file1Path, path.Join(bucketFullName, "404dir/file"))
		if err == nil {
			t.Fatalf("Rename a file to a non-existing dir should fail")
		}

		err = fs.Rename(nil, file1Path, "404dir/file")
		if err == nil {
			t.Fatalf("Rename failed to detect relative file path")
		}

		fs.Rename(nil, file1Path, file1Path+"_rn")
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

		err = fs.Rename(nil, file2Path, bucketFullName+"dir1")
		if err == nil {
			t.Fatalf("Rename a file to an existing directory name should fail")
		}

		err = fs.Rename(nil, file5Path, file6Path)
		if err == nil {
			t.Fatalf("Rename a file to an existing file name should fail")
		}
	}

	{
		// read
		f, err := fs.OpenFile(nil, file2Path, os.O_RDONLY, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s", file2Path)
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
		f, err = fs.OpenFile(nil, file2Path, os.O_RDWR, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s", file2Path)
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

		f, err = fs.OpenFile(nil, file2Path, os.O_RDONLY, os.ModePerm)
		n, err = f.Write([]byte(strings.ToLower(content1)))
		if err == nil {
			t.Fatalf("Wrote to a file without write permission")
		}

		f.Close()

		// overwrite
		f, err = fs.OpenFile(nil, file2Path, os.O_RDWR, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s", file2Path)
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
		// open flags
		fn := file2Path
		_, err = fs.Stat(nil, fn)
		if err != nil {
			t.Fatalf("Failed to open file %s for flag tests", fn)
		}

		_, err = fs.OpenFile(nil, fn, os.O_CREATE|os.O_EXCL, os.ModePerm)
		if err == nil {
			t.Fatalf("Opened existing file with both O_CREATE and O_EXCL flags")
		}

		_, err = fs.OpenFile(nil, fn+"404", os.O_RDWR, os.ModePerm)
		if err == nil {
			t.Fatalf("Opened non-existing file without O_CREATE")
		}
	}

	{
		// truncate
		fn := file2Path
		fi, _ := fs.Stat(nil, fn)
		if fi.Size() == 0 {
			t.Fatalf("Failed to prepare file for truncation")
		}

		f, err = fs.OpenFile(nil, fn, os.O_TRUNC|os.O_RDONLY, os.ModePerm)
		if err == nil {
			t.Fatalf("Opened file for truncation with read only flag")
		}

		f, err = fs.OpenFile(nil, fn, os.O_TRUNC|os.O_RDWR, os.ModePerm)
		if err != nil {
			t.Fatalf("Failed to open file %s, err = %v", fn, err)
		}

		f.Close()

		fi, _ = fs.Stat(nil, fn)
		if fi.Size() != 0 {
			t.Fatalf("Failed to truncate file")
		}
	}

	{
		// remove
		err = fs.RemoveAll(nil, "/404bucket")
		if err == nil {
			t.Fatalf("Remove non-existing bucket should fail")
		}

		err = fs.RemoveAll(nil, "/")
		if err == nil {
			t.Fatalf("Remove root should fail")
		}

		p := path.Join(bucketFullName, "remove")
		fs.Mkdir(nil, p, os.ModePerm)
		err := fs.RemoveAll(nil, p)
		if err != nil {
			t.Fatalf("Remove an existing directory should not fail")
		}

		_, err = fs.Stat(nil, p)
		if err == nil {
			t.Fatalf("Stat a removed directory should fail")
		}

		err = fs.RemoveAll(nil, file1Path)
		if err == nil {
			t.Fatalf("Remove a non existing file should fail")
		}

		fs.RemoveAll(nil, file2Path)
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
		put(t, fs, file7Path, 0, content1)
		readDir(t, fs, bucketFullName)

		fs.Mkdir(nil, dirTestPath, os.ModePerm)
		put(t, fs, file8Path, 0, content1)
		readDir(t, fs, bucketFullName)
	}

	{
		// check existing 'on disk' objects
		_, err = fs.Stat(nil, dir30Path)
		if err != nil {
			t.Fatalf("Stat existing file with multi level directory should not fail, err = %v", err)
		}

		fs1 := NewFS(url.URL{Scheme: "http", Host: leader}, rootDir)
		_, err = fs1.Stat(nil, dir30Path) // file6Path)
		if err != nil {
			t.Fatalf("Stat existing file with multi level directory should not fail, err = %v", err)
		}
	}
}

func TestINode(t *testing.T) {
	{
		root := newRoot()
		_, err := root.addChild("dir1")
		if err != nil {
			t.Fatal("Failed to create non existing directory")
		}

		_, err = root.addChild("dir1")
		if err != os.ErrExist {
			t.Fatal("Failed to detect existing directory")
		}

		root.deleteChild("dir2")
		root.deleteChild("dir1")
		root.deleteChild("dir1")
	}

	{
		root := newRoot()
		_, err := root.addChild("dir1")
		if err != nil {
			t.Fatal("Failed to create non existing directory")
		}

		dir3, err := root.addPath("dir2/dir3")
		if err != nil {
			t.Fatal("Failed to add path")
		}

		_, err = root.addChild("dir2")
		if err != os.ErrExist {
			t.Fatal("Failed to detect existing directory")
		}

		dir2 := root.find("dir2")
		if dir2 == nil {
			t.Fatal("Failed to find an existing directory")
		}

		if root.find("dir2/dir3") == nil {
			t.Fatal("Failed to find an existing directory")
		}

		if dir3 != root.find("dir2/dir3") {
			t.Fatal("Failed to find an existing directory")
		}
	}

	{
		root := newRoot()
		root.addChild("dir1")
		if root.find("dir2") != nil {
			t.Fatal("Found non-existing directory")
		}

		dir1 := root.find("dir1")
		if dir1 == nil {
			t.Fatal("Failed to find an existing directory")
		}

		dir1.addChild("dir11")
		dir11 := dir1.find("dir2")
		if dir11 != nil {
			t.Fatal("Found non-existing directory")
		}

		dir11 = dir1.find("/dir11")
		if dir11 == nil {
			t.Fatal("Failed to find absoluate path from non-root")
		}

		dir11 = dir1.find("dir11")
		if dir11 == nil {
			t.Fatal("Failed to find an existing directory")
		}

		dir11 = root.find("dir1/dir11")
		if dir11 == nil {
			t.Fatal("Failed to find an existing directory")
		}

		if dir11.find("dir1/dir11") != nil {
			t.Fatal("Found non-existing directory")
		}

		if dir11.find("a/b/c") != nil {
			t.Fatal("Found non-existing directory")
		}

		if root.find("dir1/dir111") != nil {
			t.Fatal("Found non-existing directory")
		}

		n := dir1.find("dir11")
		if n == nil || n.name != "dir11" {
			t.Fatal("Failed to find existing directory")
		}

		n = root.find("dir1/dir11")
		if n == nil || n.name != "dir11" {
			t.Fatal("Failed to find existing directory")
		}

		dir1.deleteChild("dir11")
		if root.find("dir1/dir11") != nil {
			t.Fatal("Found non-existing directory")
		}

		if root.find("dir1") == nil || root.find("dir1/") == nil || root.find("dir1") == nil {
			t.Fatal("Failed to find an existing directory")
		}

		root.addChild("dir2")
		dir2 := root.find("dir2")
		dir2.addChild("dir21")
		dir2.addChild("dir22")
		dir2.addChild("dir23")
		dir22 := root.find("dir2/dir22")
		if dir22 == nil || dir22.name != "dir22" {
			t.Fatal("Failed to find existing directory")
		}

		dir22.addChild("dir221")
		n = dir2.find("dir22")
		if n == nil || n.name != "dir22" {
			t.Fatal("Failed to find existing directory")
		}

		if root.find("dir2/dir224") != nil {
			t.Fatal("Found non-existing directory")
		}

		if root.find("dir28") != nil {
			t.Fatal("Found non-existing directory")
		}
	}
}
