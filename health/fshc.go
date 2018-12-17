/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package health provides a basic mountpath health monitor.
package health

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
)

const (
	fshcNameTemplate = "DFC-TMP"
	fshcFileSize     = 10 * 1024 * 1024 // size of temporary file
	fshcMaxFileList  = 100              // maximum number of files to read by Readdir
)

// When an IO error is triggered, it runs a few tests to make sure that the
// failed mountpath is healthy. Once the mountpath is considered faulty the
// mountpath is disabled and removed from the list of mountpaths utilized by DFC
//
// for mountpath definition, see fs/mountfs.go
type (
	fspathDispatcher interface {
		Disable(path string, why string) (disabled, exists bool)
	}
	FSHC struct {
		cmn.NamedID
		stopCh        chan struct{}
		fileListCh    chan string
		reqCh         chan fs.ChangeReq
		mpathCheckers map[string]*mountpathChecker

		// pointers to common data
		mountpaths *fs.MountedFS
		mem2       *memsys.Mem2

		// listener is notified in case of a mountpath is disabled
		dispatcher fspathDispatcher

		// temp file name generator
		ctxResolver *fs.ContentSpecMgr
	}
	mountpathChecker struct {
		stopCh chan struct{}
		fileCh chan string
		mpath  string
	}
)

//
// public API
//

// as an fs.PathRunner
var _ fs.PathRunner = &FSHC{}

func (f *FSHC) ReqAddMountpath(mpath string)     { f.reqCh <- fs.MountpathAdd(mpath) }
func (f *FSHC) ReqRemoveMountpath(mpath string)  { f.reqCh <- fs.MountpathRem(mpath) }
func (f *FSHC) ReqEnableMountpath(mpath string)  {}
func (f *FSHC) ReqDisableMountpath(mpath string) {}

func NewFSHC(mounts *fs.MountedFS, mem2 *memsys.Mem2, ctxResolver *fs.ContentSpecMgr) *FSHC {
	return &FSHC{
		mountpaths:    mounts,
		mem2:          mem2,
		stopCh:        make(chan struct{}, 4),
		fileListCh:    make(chan string, 32),
		reqCh:         make(chan fs.ChangeReq), // NOTE: unbuffered
		mpathCheckers: make(map[string]*mountpathChecker),
		ctxResolver:   ctxResolver,
	}
}

// as a runner
func (f *FSHC) Run() error {
	glog.Infof("Starting %s", f.Getname())
	f.init()

	for {
		select {
		case filepath := <-f.fileListCh:
			f.checkFile(filepath)
		case request := <-f.reqCh:
			switch request.Action {
			case fs.Add:
				f.addmp(request.Path)
			case fs.Remove:
				f.delmp(request.Path)
			}
		case <-f.stopCh:
			return nil
		}
	}
}

func (f *FSHC) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", f.Getname(), err)
	for _, r := range f.mpathCheckers {
		r.stopCh <- struct{}{}
	}

	f.stopCh <- struct{}{}
	close(f.stopCh)
}

func (f *FSHC) OnErr(fqn string) {
	if !cmn.GCO.Get().FSHC.Enabled {
		return
	}

	f.fileListCh <- fqn
}

//
// private methods
//

func (f *FSHC) init() {
	availablePaths, disabledPaths := f.mountpaths.Get()
	for mpath := range availablePaths {
		f.addmp(mpath)
	}

	for mpath := range disabledPaths {
		f.addmp(mpath)
	}
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

func newMountpathChecker(mpath string) *mountpathChecker {
	return &mountpathChecker{
		stopCh: make(chan struct{}, 1),
		fileCh: make(chan string),
		mpath:  mpath,
	}
}

func (f *FSHC) SetDispatcher(dispatcher fspathDispatcher) {
	f.dispatcher = dispatcher
}

func (f *FSHC) runMountpathChecker(r *mountpathChecker) {
	for {
		select {
		case filename := <-r.fileCh:
			f.runMpathTest(r.mpath, filename)
		case <-r.stopCh:
			return
		}
	}
}

func (f *FSHC) delmp(mpath string) {
	mpathChecker, ok := f.mpathCheckers[mpath]
	if !ok {
		glog.Error("wanted to remove mountpath which was not registered")
		return
	}

	delete(f.mpathCheckers, mpath)
	mpathChecker.stopCh <- struct{}{} // stop mpathChecker
}

func (f *FSHC) addmp(mpath string) {
	mpathChecker := newMountpathChecker(mpath)
	f.mpathCheckers[mpath] = mpathChecker
	go f.runMountpathChecker(mpathChecker)
}

func (f *FSHC) isTestPassed(mpath string, readErrors,
	writeErrors int, available bool) (passed bool, whyFailed string) {
	config := &cmn.GCO.Get().FSHC
	glog.Infof("Tested mountpath %s(%v), read: %d of %d, write(size=%d): %d of %d",
		mpath, available,
		readErrors, config.ErrorLimit, fshcFileSize,
		writeErrors, config.ErrorLimit)
	if !available {
		return false, "Mountpath is unavailable"
	}

	passed = readErrors < config.ErrorLimit && writeErrors < config.ErrorLimit
	if !passed {
		whyFailed = fmt.Sprintf("Too many errors: %d read error(s), %d write error(s)", readErrors, writeErrors)
	}

	return passed, whyFailed
}

func (f *FSHC) runMpathTest(mpath, filepath string) {
	config := &cmn.GCO.Get().FSHC
	readErrs, writeErrs, exists := f.testMountpath(filepath, mpath, config.TestFileCount, fshcFileSize)

	if passed, why := f.isTestPassed(mpath, readErrs, writeErrs, exists); !passed {
		glog.Errorf("Disabling mountpath %s...", mpath)

		if f.dispatcher != nil {
			disabled, exists := f.dispatcher.Disable(mpath, why)
			if !disabled && exists {
				glog.Errorf("Failed to disable mountpath: %s", mpath)
			}
		}
	}
}

func (f *FSHC) checkFile(filepath string) {
	mpathInfo, _ := f.mountpaths.Path2MpathInfo(filepath)
	if mpathInfo == nil {
		glog.Errorf("Failed to get mountpath for file %s", filepath)
		return
	}
	mpath := mpathInfo.Path

	r, ok := f.mpathCheckers[mpath]
	if !ok {
		glog.Errorf("Invalid mountpath %s for file %s", mpath, filepath)
		return
	}

	select {
	case r.fileCh <- filepath:
		// do nothing - queue is empty
	default:
		glog.Warningf("Mountpath %s test is running already", mpath)
	}
}

// reads the entire file content
func (f *FSHC) tryReadFile(fqn string, sgl *memsys.SGL) error {
	stat, err := os.Stat(fqn)
	if err != nil {
		return err
	}
	file, err := os.Open(fqn)
	if err != nil {
		return err
	}
	defer file.Close()

	slab := sgl.Slab()
	buf := slab.Alloc()
	defer slab.Free(buf)

	written, err := io.CopyBuffer(ioutil.Discard, file, buf)
	if err == nil && written < stat.Size() {
		return io.ErrShortWrite
	}

	return err
}

// creates a random file in a random directory inside a mountpath
func (f *FSHC) tryWriteFile(mountpath string, fileSize int, sgl *memsys.SGL) error {
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
	tmpfilename := f.ctxResolver.GenContentFQN(filepath.Join(tmpdir, fshcNameTemplate), fs.WorkfileType, fs.WorkfileFSHC)
	tmpfile, err := os.OpenFile(tmpfilename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		glog.Errorf("Failed to create temporary file: %v", err)
		return err
	}

	slab := sgl.Slab()
	buf := slab.Alloc()
	defer func() {
		slab.Free(buf)
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
func (f *FSHC) testMountpath(filepath, mountpath string,
	maxTestFiles, fileSize int) (readFails, writeFails int, accessible bool) {
	if glog.V(4) {
		glog.Infof("Testing mountpath %s", mountpath)
	}
	if _, err := os.Stat(mountpath); err != nil {
		glog.Errorf("Mountpath %s is unavailable", mountpath)
		return 0, 0, false
	}

	sgl := f.mem2.NewSGL(0)
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
				if cmn.IsIOError(err) {
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
			if cmn.IsIOError(err) {
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
			if cmn.IsIOError(err) {
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
		if cmn.IsIOError(err) {
			writeFails++
		}
	}

	if readFails != 0 || writeFails != 0 {
		glog.Errorf("Mountpath %s, read: %d failed of %d, write: %d failed of %d",
			mountpath, readFails, totalReads, writeFails, totalWrites)
	}

	return readFails, writeFails, true
}
