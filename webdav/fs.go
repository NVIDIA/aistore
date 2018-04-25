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
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/webdav"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

type (
	// FileSystem presents a DFC backend as a file system
	FileSystem struct {
		proxy    *proxyServer
		localDir string // local directory where DFC objects are temporarily stored while they are being used for read/write

		// in memory directory cache
		// to avoid putting webdav specific objects in DFC, this in memory directory cache helps webdav keep track of directories
		// that do not have any objects in DFC. this is in memory only and is not persisted, it will get lost if webdav dies or shutdown.
		// first level directories are buckets.
		// to extent the usage beyond empty directories only, this can be used for caching all buckets and directories, this will
		// help speed up bucket and directory look ups.
		// downside is concurrent access and transaction protection, for example, access to DFC not through webdav.
		// concurrent access should be done by webdav locking.
		// transaction protection: TBD
		// if too much memory is used to cache all directories, can implement cache empty directory only, might
		// affect performance because more trips to DFC.
		// objects can also be cached; needs to implements it as when too many nodes cached, can evict
		// object nodes.
		root *inode
	}

	// File represents a bucket or an object in DFC; it implements interface webdav.File
	File struct {
		fs   *FileSystem // back pointer to file system
		name string      // original name when a file operation is called
		flag int         // original flag when open is called
		perm os.FileMode // original perm when open is called

		typ    int
		bucket string
		path   string
		prefix string // path + fi.name -> "path/base"

		// Note: Following flags are set at the time when the resourcce is requested, they don't reflects the updated status, for
		//       example, if a file doesn't exists, flag resourceExists is false; after the file is created, resourceExists is still false.
		bucketExists   bool // true if the resource's bucket exists
		pathExists     bool // true if the resource's path exists
		resourceExists bool // true if the resource exists

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

	// inode is a simple implementation of file system's inode; currently used for directory only.
	inode struct {
		name     string
		parent   *inode
		children map[string]*inode
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

	separator = "/"
)

// Mkdir creates a new bucket or verifies whether a directory exists or not
func (fs *FileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	var (
		op  = "mkdir"
		f   *File
		err error
	)

	defer func() {
		webdavLog(logLevelDFC, "%-15s: %-70s perm = %-30v err = %v", "FS "+op, name, perm, err)
	}()

	f, err = fs.newFile(name)
	if err != nil {
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
		return err
	}

	switch f.typ {
	case Root:
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  os.ErrInvalid,
		}
		return err

	case Bucket:
		if f.bucketExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrExist,
			}
			return err
		}

		err = fs.proxy.createBucket(f.bucket)
		if err != nil {
			return err
		}

		_, err = fs.root.addChild(f.bucket)
		return err

	case Object:
		// Note: Since this call is Mkdir(), Object really means it is a new directory
		if !f.bucketExists || !f.pathExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return err
		}

		if f.resourceExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrExist,
			}
			return err
		}

		parent := f.parent()
		parent.addChild(f.fi.name)
		return nil

	case Directory:
		// Since it is identified as a directory, it implies the directory already exists
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  os.ErrExist,
		}
		return err
	}

	err = fmt.Errorf("Unknown resource type for %s: %s, %d", op, name, f.typ)
	return err
}

// OpenFile opens a file or a directory and returns a File.
// If it is an object, a local file may be created depends on the flag.
func (fs *FileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	var (
		op  = "openfile"
		f   *File
		err error
	)

	defer func() {
		webdavLog(logLevelDFC, "%-15s: %-70s flag = %-10d perm = %-30v err = %v", "FS "+op, name, flag, perm, err)
	}()

	f, err = fs.newFile(name)
	if err != nil {
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
		return nil, err
	}

	switch f.typ {
	case Root, Directory:
		// Node: for directory type, if it is identified s directory, it means it already exists
		return f, nil

	case Bucket:
		if !f.bucketExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return nil, err
		}

		return f, nil

	case Object:
		if !f.bucketExists || !f.pathExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return nil, err
		}

		f.flag = flag
		f.perm = perm
		if f.resourceExists {
			if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
				err = &os.PathError{
					Op:   op,
					Path: name,
					Err:  os.ErrInvalid,
				}
				return nil, err
			}

			if flag&os.O_TRUNC != 0 {
				if flag&(os.O_RDWR|os.O_WRONLY) == 0 {
					err = &os.PathError{
						Op:   op,
						Path: name,
						Err:  os.ErrInvalid,
					}
					return nil, err
				}

				err = f.createLocalFile(perm)
				if err != nil {
					return nil, err
				}

				f.dirty = true
			}
		} else {
			if flag&os.O_CREATE != 0 {
				err = f.createLocalFile(perm)
				if err != nil {
					return nil, err
				}

				f.dirty = true
			} else {
				err = &os.PathError{
					Op:   op,
					Path: name,
					Err:  os.ErrNotExist,
				}
				return nil, err
			}
		}

		return f, nil
	}

	err = fmt.Errorf("Unknown resource type for %s: %s, %d", op, name, f.typ)
	return nil, err
}

// RemoveAll removes things depends on the 'name':
// 1. Root:      error, can't remove root
// 2. Bucket:    deletes all objects from the bucket, and deletes the bucket
// 3. Directory: deletes all objects under the directory (with the same prefix)
// 4. Object:    deletes the single object
func (fs *FileSystem) RemoveAll(ctx context.Context, name string) error {
	var (
		op  = "removeall"
		f   *File
		err error
	)

	defer func() {
		webdavLog(logLevelDFC, "%-15s: %-70s err = %v", "FS "+op, name, err)
	}()

	f, err = fs.newFile(name)
	if err != nil {
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
		return err
	}

	switch f.typ {
	case Root:
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  os.ErrInvalid,
		}
		return err

	case Bucket:
		if !f.bucketExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return err
		}

		err = fs.proxy.deleteBucket(f.bucket)
		if err != nil {
			return err
		}

		fs.root.deleteChild(f.bucket)
		return nil

	case Object:
		if !f.resourceExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return err
		}

		err = fs.proxy.deleteObject(f.bucket, f.prefix)
		return err

	case Directory:
		// if name is identified as a directory, it exists for sure
		var names []string
		names, err = fs.proxy.listObjectsNames(f.bucket, f.prefix)
		if err != nil {
			return err
		}

		if len(names) != 0 {
			// f.parent() should not return nil since the directory
			err = fs.proxy.deleteObjects(f.bucket, names)
		}

		if err == nil {
			f.parent().deleteChild(f.fi.name)
		}

		return err
	}

	err = fmt.Errorf("Unknown resource type for %s: %s, %d", op, name, f.typ)
	return err
}

// Rename renames an object; rename root/bucket/directory is not supported.
func (fs *FileSystem) Rename(ctx context.Context, oldName, newName string) error {
	var (
		op         = "rename"
		oldf, newf *File
		err        error
	)

	defer func() {
		webdavLog(logLevelDFC, "%-15s: %-70s %-50s err = %v", "FS "+op, oldName, newName, err)
	}()

	oldf, err = fs.newFile(oldName)
	if err != nil {
		err = &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  err,
		}
		return err
	}

	if oldf.typ != Object {
		err = &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  os.ErrInvalid,
		}
		return err
	}

	if !oldf.resourceExists {
		err = &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  os.ErrNotExist,
		}
		return err
	}

	newf, err = fs.newFile(newName)
	if err != nil {
		err = &os.PathError{
			Op:   op,
			Path: newName,
			Err:  err,
		}
		return err
	}

	if newf.typ != Object {
		err = &os.PathError{
			Op:   op,
			Path: newName,
			Err:  os.ErrInvalid,
		}
		return err
	}

	if newf.resourceExists {
		err = &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  os.ErrExist,
		}
		return err
	}

	if !newf.bucketExists || !newf.pathExists {
		err = &os.PathError{
			Op:   op,
			Path: oldName,
			Err:  os.ErrNotExist,
		}
		return err
	}

	// get old
	var (
		localPath string
		h         *os.File
	)
	localPath = fs.localFileName()
	h, err = os.Create(localPath)
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

	err = fs.proxy.deleteObject(oldf.bucket, oldf.prefix)
	return err
}

// Stat returns a file info
func (fs *FileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	var (
		op  = "stat"
		f   *File
		fi  os.FileInfo
		err error
	)

	defer func() {
		webdavLog(logLevelDFC, "%-15s: %-70s err = %v", "FS "+op, name, err)
	}()

	f, err = fs.newFile(name)
	if err != nil {
		err = &os.PathError{
			Op:   op,
			Path: name,
			Err:  err,
		}
		return nil, err
	}

	switch f.typ {
	case Root, Directory:
		fi, err = f.Stat()
		return fi, err

	case Bucket:
		if !f.bucketExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return nil, err
		}

		fi, err = f.Stat()
		return fi, err

	case Object:
		if !f.resourceExists {
			err = &os.PathError{
				Op:   op,
				Path: name,
				Err:  os.ErrNotExist,
			}
			return nil, err
		}

		fi, err = f.Stat()
		return fi, err
	}

	err = fmt.Errorf("Unknown resource type for %s: %s, %d", op, name, f.typ)
	return nil, err
}

// SupportDeadProp returns false to indicate FileSystem doesn't support dead properties
func (fs *FileSystem) SupportDeadProp() bool {
	return false
}

// newFile is a wrapper of the generic name parser, it does extra check and populate more fields in 'File' after parsing the resource name.
func (fs *FileSystem) newFile(name string) (*File, error) {
	f, err := parseResource(name)
	if err != nil {
		return nil, err
	}

	f.fs = fs
	f.prefix = join(f.path, f.fi.name)

	if f.typ == Root {
		return f, nil
	}

	if f.bucketNode() != nil {
		f.bucketExists = true
	} else {
		if fs.proxy.doesBucketExist(f.bucket) {
			fs.root.addChild(f.bucket)
			f.bucketExists = true
		}
	}

	if f.typ == Bucket || !f.bucketExists {
		return f, nil
	}

	if f.path != "" {
		if f.parent() != nil {
			f.pathExists = true
		} else {
			exists, _, err := fs.proxy.doesObjectExist(f.bucket, f.path)
			if err != nil {
				return nil, err
			}

			f.pathExists = exists
		}
	} else {
		f.pathExists = true
	}

	if !f.pathExists {
		return f, nil
	}

	n, _ := f.bucketNode().addPath(f.path)

	// check if it is a directory in memory; if it is not in memory and the path does exists, it will be find later
	// when dfc is consulted
	if f.parent().find(f.fi.name) != nil {
		f.typ = Directory
		f.fi.mode |= os.ModeDir
		f.resourceExists = true
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
		n.addChild(f.fi.name)
	} else {
		f.fi.size = info.Size()
		f.fi.modTime = info.ModTime()
	}

	f.resourceExists = true
	return f, err
}

// join is a helper for path's join to ignore the empty path to "."
// conversion done by path.Join()
func join(pth string, base string) string {
	if pth == "" {
		return base
	}

	return path.Join(pth, base)
}

// NewFS returns a DFC file system
func NewFS(url url.URL, localDir string) webdav.FileSystem {
	return &FileSystem{
		&proxyServer{url.String()},
		localDir,
		newRoot(),
	}
}

// parseResource parses a webdav resource and returns a File represents it.
// it can't tell whether it is a file or a directory
// +--------------------------+--------+--------+------------+------+
// | Name                     | Type   | Bucket | Path       | Base |
// +--------------------------+--------+--------+------------+------+
// | "/"                      | Root   | ""     | ""         | ""   |
// | "/bucket"                | Bucket | bucket | ""         | ""   |
// | "/bucket/dir/"           | File   | bucket | ""         | dir  |
// | "/bucket/dir1/dir2/"     | File   | bucket | dir1       | dir2 |
// | "/bucket/dir/file"       | File   | bucket | dir        | file |
// | "/bucket/dir1/dir2/file" | File   | bucket | dir1/dir2  | file |
// +--------------------------+--------+--------+------------+------+
func parseResource(name string) (*File, error) {
	// Note: path.Clean() removes anything before "..", this is not what webdav thinks.
	//       replace ".." with "/" before clean
	n := strings.Replace(name, "..", "/", -1)
	n = path.Clean(n)
	if !path.IsAbs(n) {
		return nil, os.ErrInvalid
	}

	if name == separator {
		return &File{
			name: name,
			typ:  Root,
			fi: fileInfo{
				name: "",
				mode: defaultDirMode,
			},
		}, nil
	}

	if path.Dir(n) == separator {
		return &File{
			name:   name,
			typ:    Bucket,
			bucket: path.Base(n),
			fi: fileInfo{
				name: "",
				mode: defaultDirMode,
			},
		}, nil
	}

	n = strings.TrimPrefix(n, separator)
	segs := strings.Split(n, separator)
	// guaranteed there will be at least two segment
	bucket := segs[0]
	// trim bucket name + "/" of the string to get the rest of it
	n = strings.TrimPrefix(n, bucket+separator)

	pth := path.Dir(n)
	base := path.Base(n)
	if pth == "." {
		pth = ""
	}

	return &File{
		name:   name,
		typ:    Object,
		bucket: bucket,
		path:   pth,
		fi: fileInfo{
			name: base,
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

		var n *inode
		if f.typ == Bucket {
			n = f.bucketNode()
		} else {
			n = f.inode(f.prefix)
		}

		for _, c := range n.children {
			// adding a "/" at the end to indicate to DFC this is a directory
			objs = append(objs, &dfc.BucketEntry{Name: c.name + separator})
		}

		fis := group(objs, f.prefix)
		return f.list(count, fis)
	}

	return nil, os.ErrInvalid
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

// createLocalFile creates a local file for read/write of a dfc object
func (f *File) createLocalFile(perm os.FileMode) error {
	var err error

	f.localPath = f.fs.localFileName()
	f.handle, err = os.Create(f.localPath)
	if err != nil {
		return err
	}

	return f.handle.Chmod(perm)
}

// downlaodFile creates a local file, get the file from DFC.
func (f *File) downlaodFile() error {
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

// inode search in memory directory nodes to find inode of a path relative to bucket name.
func (f *File) inode(pth string) *inode {
	return f.fs.root.find(join(f.bucket, pth))
}

// parent returns the parent inode.
func (f *File) parent() *inode {
	return f.inode(f.path)
}

// bucketNode returns the inode for the bucket that 'f' belongs to.
func (f *File) bucketNode() *inode {
	return f.inode("")
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

// newRoot returns a new root directory
func newRoot() *inode {
	return &inode{
		name:     separator,
		parent:   nil,
		children: make(map[string]*inode),
	}
}

// addChild adds 'name' as a child of node 'd' and returns the addChild node if there is no error
func (d *inode) addChild(name string) (*inode, error) {
	c, ok := d.children[name]
	if ok {
		return c, os.ErrExist
	}

	n := &inode{
		name:     name,
		parent:   d,
		children: make(map[string]*inode),
	}

	d.children[name] = n
	return n, nil
}

// deleteChild deletes node d's child 'name'
func (d *inode) deleteChild(name string) {
	delete(d.children, name)
}

// addPath adds every segments on path as a child starting from node 'd'; last node added is returned
func (d *inode) addPath(pth string) (*inode, error) {
	if path.IsAbs(pth) {
		return nil, os.ErrInvalid
	}

	if pth == "" {
		return d, nil
	}

	p := path.Clean(pth)
	segs := strings.Split(p, separator)

	n := d
	for _, s := range segs {
		n, _ = n.addChild(s)
	}

	return n, nil
}

// find returns the node corresponding to a path; nil if not exists
// the search starts from node 'd' and walks through each segment in 'pth'
// note: should not expect to look for an absoluate path; first "/" is trimmed before looking
func (d *inode) find(pth string) *inode {
	p := path.Clean(pth)
	p = strings.TrimPrefix(p, separator)
	segs := strings.Split(p, separator)

	var (
		n  = d
		ok bool
	)

	for _, s := range segs {
		n, ok = n.children[s]
		if !ok {
			return nil
		}
	}

	return n
}

// group groups the objects into unique directories and files.
// for example, if input = "loader/obj1", "loader/obj2", "obj3", "loader/dir/obj4", prefix = ""
// output will be: loader(directory), obj3(file)
// prefix is removed from object name before grouping.
// do not expect duplicate keys (upper layer should not allow that to happen)
func group(objs []*dfc.BucketEntry, prefix string) []os.FileInfo {
	keys := make(map[string]bool) // string = first part after split by "/", bool = true if it is a file
	var fis []os.FileInfo

	for _, o := range objs {
		n := strings.TrimPrefix(o.Name, prefix+separator)
		parts := strings.Split(n, separator)

		if len(parts) == 1 {
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

		_, ok := keys[parts[0]]
		if !ok {
			keys[parts[0]] = false
			fis = append(fis, &fileInfo{
				name: parts[0],
				mode: defaultDirMode,
			})
		}
	}

	sort.Sort(fiSortByName(fis))
	return fis
}

// localFileName returns a full path of a temporary file used while a DFC object is opened for read or write
func (fs *FileSystem) localFileName() string {
	return filepath.Join(fs.localDir,
		client.FastRandomFilename(rand.New(rand.NewSource(time.Now().UnixNano())), 32 /* length */))
}

type fiSortByName []os.FileInfo

func (fis fiSortByName) Len() int {
	return len(fis)
}

func (fis fiSortByName) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis fiSortByName) Less(i, j int) bool {
	return fis[i].Name() < fis[j].Name()
}
