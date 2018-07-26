// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/fs"
)

const (
	fshcNameTemplate = "DFC-TMP"
	fshcFileSize     = 10 * 1024 * 1024 // size of temporary file
	fshcMaxFileList  = 100              // maximum number of files to read by Readdir
)

// fsHealthChecker is a basic mountpath health monitor.
// When an IO error is triggered, it runs a few tests to make sure that the
// failed mountpath is healthy. Once the mountpath is considered faulty the
// mountpath is disabled and removed from the list of mountpaths utilized by DFC
//
// for mountpath definition, see fs/mountfs.go
type fsHealthChecker struct {
	namedrunner
	chStop     chan struct{}
	chFileList chan string
	fsList     map[string]*fsRunner

	// pointers to common data
	config         *fshcconf
	mountpaths     *fs.MountedFS
	fnMakeTempName func(string) string
}

type fsRunner struct {
	chStop chan struct{}
	chFile chan string
	mpath  string
}

// gets a base directory and looks for a random file inside it.
// Returns an error if any directory cannot be read
func getRandomFileName(basePath string) (string, error) {
	file, err := os.Open(basePath)
	if err != nil {
		return "", err
	}

	files, err := file.Readdir(fshcMaxFileList)
	if err == nil {
		fmap := make(map[string]os.FileInfo, len(files))
		for _, ff := range files {
			fmap[ff.Name()] = ff
		}

		// look for a non-empty random entry
		for k, info := range fmap {
			// it is a file - return its fqn
			if !info.IsDir() {
				return filepath.Join(basePath, k), nil
			}
			// it is a directory - return a random file from it
			chosen, err := getRandomFileName(filepath.Join(basePath, k))
			if chosen != "" {
				return chosen, nil
			}
			if err != nil {
				return "", err
			}
		}
	}

	if err == io.EOF {
		err = nil
	}

	return "", err
}

func newFSHealthChecker(mounts *fs.MountedFS, conf *fshcconf,
	f func(string) string) *fsHealthChecker {
	return &fsHealthChecker{
		mountpaths:     mounts,
		fnMakeTempName: f,
		chStop:         make(chan struct{}, 4),
		chFileList:     make(chan string, 32),
		config:         conf,
		fsList:         make(map[string]*fsRunner),
	}
}

func (f *fsHealthChecker) mpathChecker(r *fsRunner) {
	for {
		select {
		case filename := <-r.chFile:
			f.runMpathTest(r.mpath, filename)
		case <-r.chStop:
			return
		}
	}
}

func (f *fsHealthChecker) init() {
	availablePaths, disabledPaths := f.mountpaths.Mountpaths()
	for _, mp := range availablePaths {
		f.fsList[mp.Path] = &fsRunner{
			chStop: make(chan struct{}, 1),
			chFile: make(chan string),
			mpath:  mp.Path,
		}
	}
	for _, mp := range disabledPaths {
		f.fsList[mp.Path] = &fsRunner{
			chStop: make(chan struct{}, 1),
			chFile: make(chan string),
			mpath:  mp.Path,
		}
	}

	for _, r := range f.fsList {
		go f.mpathChecker(r)
	}
}

func (f *fsHealthChecker) onerr(fqn string) {
	if !f.config.Enabled {
		return
	}

	f.chFileList <- fqn
}

func (f *fsHealthChecker) run() error {
	glog.Infof("Starting %s", f.name)
	f.init()

	for {
		select {
		case filepath := <-f.chFileList:
			f.checkFile(filepath)
		case <-f.chStop:
			return nil
		}
	}
}

func (f *fsHealthChecker) stop(err error) {
	glog.Infof("Stopping %s, err: %v", f.name, err)
	for _, r := range f.fsList {
		r.chStop <- struct{}{}
	}

	f.chStop <- struct{}{}
	close(f.chStop)
}

func (f *fsHealthChecker) isTestPassed(mpath string, readErrors,
	writeErrors int, available bool) bool {
	glog.Infof("Tested mountpath %s(%v), read: %d of %d, write(size=%d): %d of %d",
		mpath, available,
		readErrors, f.config.ErrorLimit, fshcFileSize,
		writeErrors, f.config.ErrorLimit)
	if !available {
		return false
	}

	return readErrors < f.config.ErrorLimit && writeErrors < f.config.ErrorLimit
}

func (f *fsHealthChecker) runMpathTest(mpath, filepath string) {
	readErrs, writeErrs, exists := f.testMountpath(filepath, mpath,
		f.config.TestFileCount, fshcFileSize)

	if !f.isTestPassed(mpath, readErrs, writeErrs, exists) {
		glog.Errorf("Disabling mountpath %s...", mpath)
		f.mountpaths.DisableMountpath(mpath)
	}
}

func (f *fsHealthChecker) checkFile(filepath string) {
	mpath := fqn2mountPath(filepath)
	if mpath == "" {
		glog.Errorf("Failed to get mountpath for file %s", filepath)
		return
	}

	r, ok := f.fsList[mpath]
	if !ok {
		glog.Errorf("Invalid mountpath %s for file %s", mpath, filepath)
		return
	}

	select {
	case r.chFile <- filepath:
		// do nothing - queue is empty
	default:
		glog.Warningf("Mountpath %s test is running already", mpath)
	}
}

// reads the entire file content
func (f *fsHealthChecker) tryReadFile(fqn string, sgl *SGLIO) error {
	stat, err := os.Stat(fqn)
	if err != nil {
		return err
	}
	file, err := os.Open(fqn)
	if err != nil {
		return err
	}
	defer file.Close()

	slab := selectslab(sgl.Size())
	buf := slab.alloc()
	defer slab.free(buf)

	written, err := io.CopyBuffer(ioutil.Discard, file, buf)
	if err == nil && written < stat.Size() {
		return io.ErrShortWrite
	}

	return err
}

// creates a random file in a random directory inside a mountpath
func (f *fsHealthChecker) tryWriteFile(mountpath string, fileSize int, sgl *SGLIO) error {
	tmpdir, err := ioutil.TempDir(mountpath, fshcNameTemplate)
	if err != nil {
		glog.Errorf("Failed to create temporary directory: %v", err)
		return err
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			glog.Errorf("Failed to clean up temporary directory: %v", err)
		}
	}()
	tmpfilename := f.fnMakeTempName(filepath.Join(tmpdir, fshcNameTemplate))
	tmpfile, err := os.OpenFile(tmpfilename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		glog.Errorf("Failed to create temporary file: %v", err)
		return err
	}

	slab := selectslab(sgl.Size())
	buf := slab.alloc()
	defer func() {
		slab.free(buf)
		if err := tmpfile.Close(); err != nil {
			glog.Errorf("Failed to close tempory file %s: %v", tmpfile.Name(), err)
		}
	}()

	_, _ = rand.Read(buf)
	bytesLeft := fileSize
	bufSize := len(buf)
	for bytesLeft > 0 {
		if bytesLeft > bufSize {
			_, err = tmpfile.Write(buf)
		} else {
			_, err = tmpfile.Write(buf[:bytesLeft])
		}
		if err != nil {
			glog.Errorf("Failed to write to file %s: %v", tmpfile.Name(), err)
			return err
		}

		bytesLeft -= bufSize
	}

	return nil
}

// the core testing function: reads existing and writes temporary files on mountpath
//   1. If the filepath points to existing file, it reads this file
//   2. Reads up to maxReads files selected at random
//   3. Creates up to maxWrites temporary files
// The function returns the number of read/write errors, and if the mountpath
//   is accessible. When the specified local directory is inaccessible the
//   function returns immediately without any read/write operations
func (f *fsHealthChecker) testMountpath(filepath, mountpath string,
	maxTestFiles, fileSize int) (readFails, writeFails int, accessible bool) {
	if glog.V(4) {
		glog.Infof("Testing mountpath %s", mountpath)
	}
	if _, err := os.Stat(mountpath); err != nil {
		glog.Errorf("Mountpath %s is unavailable", mountpath)
		return 0, 0, false
	}

	sgl := NewSGLIO(0)
	defer sgl.Free()

	totalReads, totalWrites := 0, 0
	// first, read the file that causes the error, if it is defined
	if filepath != "" {
		if stat, err := os.Stat(filepath); err == nil && !stat.IsDir() {
			totalReads++
			err := f.tryReadFile(filepath, sgl)
			if err != nil {
				glog.Errorf("Checking file %s result: %#v, mountpath: %d",
					filepath, err, readFails)
				if isIOError(err) {
					readFails++
				}
			}
		}
	}

	// second, read a few more files up to maxReads files
	for totalReads < maxTestFiles {
		fqn, err := getRandomFileName(mountpath)
		if err == io.EOF {
			// no files in the mountpath
			glog.Infof("Mountpath %s contains no files", mountpath)
			break
		}
		totalReads++
		if fqn == "" {
			if isIOError(err) {
				readFails++
			}
			glog.Infof("Failed to select a random file in %s: %#v, failures: %d",
				mountpath, err, readFails)
			continue
		}
		if glog.V(4) {
			glog.Infof("Reading random file [%s]", fqn)
		}

		err = f.tryReadFile(fqn, sgl)
		if err != nil {
			glog.Errorf("Failed to read random file %s: %v", fqn, err)
			if isIOError(err) {
				readFails++
			}
		}

		glog.Infof("%s, files read: %d, failures: %d",
			mountpath, totalReads, readFails)
	}

	// third, try to creare a few random files inside the mountpath
	for totalWrites < maxTestFiles {
		totalWrites++
		err := f.tryWriteFile(mountpath, fileSize, sgl)
		if err != nil {
			glog.Errorf("Failed to create file in %s: %#v", mountpath, err)
		}
		if isIOError(err) {
			writeFails++
		}
	}

	if readFails != 0 || writeFails != 0 {
		glog.Errorf("Mountpath %s, read: %d failed of %d, write: %d failed of %d",
			mountpath, readFails, totalReads, writeFails, totalWrites)
	}

	return readFails, writeFails, true
}
