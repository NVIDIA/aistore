// DFC's implementation of go webdav's FileSystem
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"golang.org/x/net/webdav"
)

type (
	// FileSystem presents a DFC backend as a file system
	FileSystem struct {
		proxy    *proxyServer
		localDir string // local directory where DFC objects are temporarily stored while they are being used for read/write
	}

	// File represents a bucket or an object in DFC; it implements interface webdav.File
	File struct {
		fs     *FileSystem // back pointer to file system
		name   string      // original name when a file operation is called
		flag   int         // original flag when open is called
		perm   os.FileMode // original perm when open is called
		exists bool        // true if the object or directory already exists when the file is opened

		typ    int
		bucket string
		prefix string

		// Runtime information
		fi    fileInfo
		pos   int
		dirty bool

		// For 'Object' only; full path of the local copy of a DFC object while it is open for read or write;
		// read/write goes to this file and flushed to DFC when file is closed if it is dirty
		// local file only exists on first time a file is read from or written to; handle != nil implies a local file exists.
		localPath string
		handle    *os.File
	}

	fileInfo struct {
		name    string
		size    int64
		mode    os.FileMode
		modTime time.Time
	}
)

var (
	_ webdav.FileSystem = &FileSystem{}
	_ webdav.File       = &File{}
	_ os.FileInfo       = &fileInfo{}
)

const (
	Root = iota
	Bucket
	Directory
	Object

	defaultFileMode os.FileMode = 0660
	defaultDirMode  os.FileMode = defaultFileMode | os.ModeDir
)

// Mkdir creates a new bucket or verifies whether a directory exists or not
func (fs *FileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	op := "mkdir"
	webdavLog(logLevelDFC, "%-15s: %-70s perm = %-30v", "FS "+op, name, perm)

	f, err := fs.newFile(name)
	if err != nil {
		return &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
	}

	switch f.typ {
	case Bucket:
		if fs.proxy.doesBucketExist(f.bucket) {
			return &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrExist,
			}
		}

		return fs.proxy.createBucket(f.bucket)

	case Directory:
		n := strings.TrimSuffix(f.prefix, "/")
		exists, _, err := fs.proxy.doesObjectExist(f.bucket, n)
		if err != nil {
			return err
		}

		if exists {
			return &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrExist,
			}
		}

		return nil

	default:
		return &os.PathError{
			Op:   op,
			Path: name,
			Err:  os.ErrInvalid,
		}
	}
}

// OpenFile opens a file or a directory and returns a File.
// If it is an object, a local file may be created depends on the flag.
func (fs *FileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	op := "openfile"
	webdavLog(logLevelDFC, "%-15s: %-70s flag = %-10d perm = %-30v", "FS "+op, name, flag, perm)

	f, err := fs.newFile(name)
	if err != nil {
		return nil, &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
	}

	f.fs = fs
	switch f.typ {
	case Object:
		f.flag = flag
		f.perm = perm

		if f.exists {
			if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
				return nil, &os.PathError{
					Op:   op,
					Path: name,
					Err:  os.ErrExist,
				}
			}

			if flag&os.O_TRUNC != 0 {
				if flag&(os.O_RDWR|os.O_WRONLY) == 0 {
					return nil, &os.PathError{
						Op:   op,
						Path: name,
						Err:  os.ErrInvalid,
					}
				}

				err := f.createLocalFile(perm)
				if err != nil {
					return nil, err
				}

				f.dirty = true
			}
		} else {
			if flag&os.O_CREATE != 0 {
				err := f.createLocalFile(perm)
				if err != nil {
					return nil, err
				}

				f.dirty = true
			} else {
				return nil, &os.PathError{
					Op:   op,
					Path: name,
					Err:  os.ErrNotExist,
				}
			}
		}

		return f, nil

	default:
		return f, nil
	}
}

// RemoveAll removes things depends on the 'name':
// 1. Root:      error, can't remove root
// 2. Bucket:    deletes all objects from the bucket, and deletes the bucket
// 3. Directory: deletes all objects under the directory (with the same prefix)
// 4. Object:    deletes the single object
func (fs *FileSystem) RemoveAll(ctx context.Context, name string) error {
	op := "removeall"
	webdavLog(logLevelDFC, "%-15s: %-70s", "FS "+op, name)

	f, err := fs.newFile(name)
	if err != nil {
		return &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
	}

	switch f.typ {
	case Root:
		return &os.PathError{
			Op:   op,
			Path: name,
			Err:  os.ErrInvalid,
		}

	case Bucket:
		if !fs.proxy.doesBucketExist(f.bucket) {
			return &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
		}

		return fs.proxy.deleteBucket(f.bucket)

	case Directory:
		names, err := fs.proxy.listObjectsNames(f.bucket, f.prefix)
		if err != nil {
			return err
		}

		return fs.proxy.deleteObjects(f.bucket, names)

	case Object:
		exists, _, err := fs.proxy.doesObjectExist(f.bucket, f.prefix)
		if err != nil {
			return err
		}

		if !exists {
			return &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
		}

		return fs.proxy.deleteObject(f.bucket, f.prefix)

	default:
		return &os.PathError{
			Op:   op,
			Path: name,
			Err:  os.ErrInvalid,
		}
	}
}

// Rename renames an object; rename root/bucket/directory is not supported.
func (fs *FileSystem) Rename(ctx context.Context, oldName, newName string) error {
	op := "rename"
	webdavLog(logLevelDFC, "%-15s: %-70s %-50s", "FS "+op, oldName, newName)

	oldf, err := fs.newFile(oldName)
	if err != nil {
		return &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  err,
		}
	}

	if oldf.typ != Object {
		return &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  os.ErrInvalid,
		}
	}

	// old file has to exist
	exists, _, err := fs.proxy.doesObjectExist(oldf.bucket, oldf.prefix)
	if err != nil {
		return err
	}

	if !exists {
		return &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  os.ErrNotExist,
		}
	}

	newf, err := fs.newFile(newName)
	if err != nil {
		return &os.PathError{
			Op:   op,
			Path: newName,
			Err:  err,
		}
	}

	if newf.typ != Object {
		return &os.PathError{
			Op:   op,
			Path: newName,
			Err:  os.ErrInvalid,
		}
	}

	// new file should not already exist
	exists, _, err = fs.proxy.doesObjectExist(newf.bucket, newf.prefix)
	if err != nil {
		return err
	}

	if exists {
		return &os.PathError{
			Op:   op,
			Path: newName,
			Err:  os.ErrExist,
		}
	}

	// get old
	localPath := fs.localFileName()
	h, err := os.Create(localPath)
	if err != nil {
		return err
	}

	defer func() {
		h.Close()
		os.Remove(localPath)
	}()

	err = fs.proxy.getObject(oldf.bucket, oldf.prefix, h)
	if err != nil {
		return err
	}

	_, err = h.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = fs.proxy.putObject(localPath, newf.bucket, newf.prefix)
	if err != nil {
		return err
	}

	return fs.proxy.deleteObject(oldf.bucket, oldf.prefix)
}

// Stat returns a file info
func (fs *FileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	op := "stat"
	webdavLog(logLevelDFC, "%-15s: %-70s", "FS "+op, name)

	f, err := fs.newFile(name)
	if err != nil {
		return nil, &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
	}

	f.fs = fs
	if f.typ == Object {
		if !f.exists {
			return nil, &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
		}

		// fall through
	}

	return f.Stat()
}

// newFile is a wrapper of the generic name parser newFile(), it checks whether a name without ending "/"
// is actually an existing directory.
func (fs *FileSystem) newFile(name string) (*File, error) {
	f, err := newFile(name)
	if err != nil {
		return nil, err
	}

	if f.typ != Object {
		return f, nil
	}

	exists, info, err := fs.proxy.doesObjectExist(f.bucket, f.prefix)
	if err != nil {
		return nil, err
	}

	if !exists {
		return f, nil
	}

	if info.IsDir() {
		f.typ = Directory
		f.fi.mode |= os.ModeDir
	} else {
		f.exists = true
		f.fi.size = info.Size()
		f.fi.modTime = info.ModTime()
	}

	return f, nil
}

// NewFS returns a DFC file system
func NewFS(url url.URL, localDir string) webdav.FileSystem {
	return &FileSystem{
		&proxyServer{url.String()},
		localDir,
	}
}

// newFile parses a name of an object and returns a File represents the object.
// WebDAV object to DFC conversion
// +--------------------------+------------+--------------+------------------+------------+
// | Name                     | Type       | Bucket Name  | Prefix           | BaseName   |
// +--------------------------+------------+--------------+------------------+------------+
// | "/"                      | Root       | N/A          | N/A              | N/A        |
// | "/bucket"                | Bucket     | bucket       | N/A              | N/A        |
// | "/bucket/dir/"           | Dir        | bucket       | dir/             | N/A        |
// | "/bucket/dir1/dir2/"     | Dir        | bucket       | dir1/dir2/       | N/A        |
// | "/bucket/dir/file"       | File       | bucket       | dir/file         | file       |
// | "/bucket/dir1/dir2/file" | File       | bucket       | dir1/dir2/file   | file       |
// +--------------------------+------------+--------------+------------------+------------+
// Note: When there is no trailing "/", it can mean either a file or a diretory, based on the operation, caller needs to treat it accordingly.
func newFile(name string) (*File, error) {
	if !strings.HasPrefix(name, "/") {
		return nil, os.ErrInvalid
	}

	if name == "/" {
		return &File{
			name: "/",
			typ:  Root,
			fi: fileInfo{
				name: "/",
				mode: defaultDirMode,
			},
		}, nil
	}

	// Verifies none of the parts between two "/"s are empty
	n := strings.TrimPrefix(name, "/")
	n = strings.TrimSuffix(n, "/")
	parts := strings.Split(n, "/")
	if len(parts) == 0 {
		return nil, os.ErrInvalid
	}

	for _, p := range parts {
		if len(p) < 1 {
			return nil, os.ErrInvalid
		}
	}

	parts = strings.Split(name, "/")
	if len(parts) == 2 {
		return &File{
			name:   name,
			typ:    Bucket,
			bucket: parts[1],
			fi: fileInfo{
				name: parts[1],
				mode: defaultDirMode,
			},
		}, nil
	}

	if strings.HasSuffix(name, "/") {
		if len(parts) == 3 {
			return &File{
				name:   name,
				typ:    Bucket,
				bucket: parts[1],
				fi: fileInfo{
					name: parts[1],
					mode: defaultDirMode,
				},
			}, nil
		}

		return &File{
			name:   name,
			typ:    Directory,
			bucket: parts[1],
			prefix: name[len(parts[1])+2:],
			fi: fileInfo{
				name: parts[1],
				mode: defaultDirMode,
			},
		}, nil
	}

	return &File{
		name:   name,
		typ:    Object,
		bucket: parts[1],
		prefix: name[len(parts[1])+2:],
		fi: fileInfo{
			name: parts[len(parts)-1],
			mode: defaultFileMode,
		},
	}, nil
}

// Close closes a previously opened local file, call proxy put if the object is updated
func (f *File) Close() error {
	webdavLog(logLevelDFC, "%-15s: %-70s", "File Close", f.name)

	if f.typ != Object {
		// No op for root/bucket/directory
		return nil
	}

	if f.handle == nil {
		// local file is not always opened
		return nil
	}

	err := f.handle.Close()
	if err != nil {
		return err
	}

	if f.dirty {
		err = f.fs.proxy.putObject(f.localPath, f.bucket, f.prefix)
	}

	os.Remove(f.localPath)
	f.handle = nil
	return err
}

// Read reads from a bucket or an object
// If it is the first time the file is accessed for read/write, create local file
func (f *File) Read(b []byte) (n int, err error) {
	webdavLog(logLevelDFC, "%-15s: %-70s # bytes = %d", "File Read", f.name, len(b))

	if f.handle == nil {
		err := f.downlaodFile()
		if err != nil {
			return 0, err
		}

		// fall through
	}

	return f.handle.Read(b)
}

// Write Writes to a bucket or an object
// If it is the first time the file is accessed for read/write, create local file
func (f *File) Write(data []byte) (n int, err error) {
	webdavLog(logLevelDFC, "%-15s: %-70s # bytes = %d", "File Write", f.name, len(data))

	if f.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, os.ErrPermission
	}

	if f.handle == nil {
		err := f.downlaodFile()
		if err != nil {
			return 0, err
		}

		// fall through
	}

	f.dirty = true
	return f.handle.Write(data)
}

// Readdir returns a slice of file info
func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	webdavLog(logLevelDFC, "%-15s: %-70s count = %d pos = %d", "File Readdir", f.name, count, f.pos)

	var fis []os.FileInfo
	switch f.typ {
	case Root:
		buckets, err := f.fs.proxy.listBuckets(true /* local */)
		if err != nil {
			return nil, err
		}

		for _, b := range buckets {
			fis = append(fis, &fileInfo{
				name: b,
				mode: defaultDirMode,
			})
		}

		return f.list(count, fis)

	case Bucket, Directory:
		objs, err := f.fs.proxy.listObjectsDetails(f.bucket, f.prefix, 0 /* limit */)
		if err != nil {
			return nil, err
		}

		fis, err := group(objs, f.prefix)
		if err != nil {
			return nil, err
		}

		return f.list(count, fis)

	default:
		return nil, os.ErrInvalid
	}
}

// Seek moves current position of an object
func (f *File) Seek(offset int64, whence int) (int64, error) {
	if f.handle == nil {
		err := f.downlaodFile()
		if err != nil {
			return 0, err
		}

		// fall through
	}

	return f.handle.Seek(offset, whence)
}

// Stat returns file info an object
func (f *File) Stat() (os.FileInfo, error) {
	return &f.fi, nil
}

func (f *File) createLocalFile(perm os.FileMode) error {
	var err error

	if f.handle != nil {
		panic(fmt.Errorf("creating an already opened file"))
	}

	f.localPath = f.fs.localFileName()
	f.handle, err = os.Create(f.localPath)
	if err != nil {
		return err
	}

	return f.handle.Chmod(perm)
}

// downlaodFile creates a local file, get the file from DFC.
func (f *File) downlaodFile() error {
	if f.handle != nil {
		panic(fmt.Errorf("downloading an already opened file"))
	}

	err := f.createLocalFile(f.perm)
	if err != nil {
		return err
	}

	err = f.fs.proxy.getObject(f.bucket, f.prefix, f.handle)
	if err != nil {
		return err
	}

	_, err = f.handle.Seek(0, io.SeekStart)
	return err
}

// list is a common function for list file/directory.
// it takes a slice of file info, returns a portion of it for webdav clients to show.
// portion is determined based on total number of file infos, the count and current position of 'File'.
func (f *File) list(count int, fis []os.FileInfo) ([]os.FileInfo, error) {
	old := f.pos
	if old >= len(fis) {
		if count > 0 {
			return nil, io.EOF
		}

		return nil, nil
	}

	if count > 0 {
		f.pos += count
		if f.pos > len(fis) {
			f.pos = len(fis)
		}
	} else {
		f.pos = len(fis)
		old = 0
	}

	return fis[old:f.pos], nil
}

func (fi *fileInfo) Name() string {
	return fi.name
}

func (fi *fileInfo) Size() int64 {
	return fi.size
}

func (fi *fileInfo) Mode() os.FileMode {
	return fi.mode
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *fileInfo) IsDir() bool {
	return fi.mode.IsDir()
}

func (fi *fileInfo) Sys() interface{} {
	return nil
}

// group groups the objects into unique directories and files.
// for example, if input = "loader/obj1", "loader/obj2", "obj3", "loader/dir/obj4", prefix = ""
// output will be: loader(directory), obj3(file)
// prefix is removed from object name before grouping.
func group(objs []*dfc.BucketEntry, prefix string) ([]os.FileInfo, error) {
	keys := make(map[string]bool) // string = first part after split by "/", bool = true if it is a file
	var fis []os.FileInfo

	for _, o := range objs {
		n := strings.TrimPrefix(o.Name, prefix)
		parts := strings.Split(n, "/")

		if len(parts) == 1 {
			if _, ok := keys[parts[0]]; ok {
				return nil, fmt.Errorf("duplicate file name %s", o.Name)
			}

			keys[parts[0]] = true
			mTime, _ := time.Parse(time.RFC822, o.Ctime)
			mTime = mTime.UTC()
			fis = append(fis, &fileInfo{
				name:    parts[0],
				size:    o.Size,
				modTime: mTime,
				mode:    defaultFileMode,
			})

			continue
		}

		isFile, ok := keys[parts[0]]
		if ok && isFile {
			return nil, fmt.Errorf("duplicate dir and file name %s", o.Name)
		}

		if !ok {
			keys[parts[0]] = false
			fis = append(fis, &fileInfo{
				name: parts[0],
				mode: defaultDirMode,
			})
		}
	}

	return fis, nil
}

// localFileName returns a full path of a temporary file used while a DFC object is opened for read or write
func (fs *FileSystem) localFileName() string {
	return filepath.Join(fs.localDir,
		client.FastRandomFilename(rand.New(rand.NewSource(time.Now().UnixNano())), 32 /* length */))
}
