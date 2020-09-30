// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package health

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	fshcNameTemplate = "AIS-TMP"
	fshcFileSize     = 10 * cmn.MiB // size of temporary file which will test writing and reading the mountpath
	fshcMaxFileList  = 100          // maximum number of files to read by Readdir
)

// When an IO error is triggered, it runs a few tests to make sure that the
// failed mountpath is healthy. Once the mountpath is considered faulty the
// mountpath is disabled and removed from the list.
//
// for mountpath definition, see fs/mountfs.go
type (
	fspathDispatcher interface {
		DisableMountpath(path, reason string) (disabled bool, err error)
	}
	FSHC struct {
		cmn.Named
		stopCh      chan struct{}
		fileListCh  chan string
		mm          *memsys.MMSA
		dispatcher  fspathDispatcher   // listener is notified upon mountpath events (disabled, etc.)
		ctxResolver *fs.ContentSpecMgr // temp filename generator
	}
)

//////////
// FSHC //
//////////

func NewFSHC(dispatcher fspathDispatcher, mm *memsys.MMSA, ctxResolver *fs.ContentSpecMgr) *FSHC {
	return &FSHC{
		mm:          mm,
		stopCh:      make(chan struct{}),
		fileListCh:  make(chan string, 100),
		dispatcher:  dispatcher,
		ctxResolver: ctxResolver,
	}
}

// as a runner
func (f *FSHC) Run() error {
	glog.Infof("Starting %s", f.GetRunName())

	for {
		select {
		case filePath := <-f.fileListCh:
			mpathInfo, _ := fs.Path2MpathInfo(filePath)
			if mpathInfo == nil {
				glog.Errorf("Failed to get mountpath for file %s", filePath)
				break
			}

			f.runMpathTest(mpathInfo.Path, filePath)
		case <-f.stopCh:
			return nil
		}
	}
}

func (f *FSHC) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", f.GetRunName(), err)
	f.stopCh <- struct{}{}
}

func (f *FSHC) OnErr(fqn string) {
	if !cmn.GCO.Get().FSHC.Enabled {
		return
	}

	f.fileListCh <- fqn
}

func (f *FSHC) isTestPassed(mpath string, readErrors, writeErrors int, available bool) (passed bool, err error) {
	config := &cmn.GCO.Get().FSHC
	glog.Infof("Tested mountpath %s(%v), read: %d of %d, write(size=%d): %d of %d",
		mpath, available,
		readErrors, config.ErrorLimit, fshcFileSize,
		writeErrors, config.ErrorLimit)

	if !available {
		return false, errors.New("mountpath is unavailable")
	}

	passed = readErrors < config.ErrorLimit && writeErrors < config.ErrorLimit
	if !passed {
		err = fmt.Errorf("too many errors: %d read error(s), %d write error(s)", readErrors, writeErrors)
	}
	return passed, err
}

func (f *FSHC) runMpathTest(mpath, filepath string) {
	var (
		passed    bool
		whyFailed error
	)

	config := &cmn.GCO.Get().FSHC
	readErrs, writeErrs, exists := f.testMountpath(filepath, mpath, config.TestFileCount, fshcFileSize)

	if passed, whyFailed = f.isTestPassed(mpath, readErrs, writeErrs, exists); passed {
		return
	}

	glog.Errorf("Disabling mountpath %s...", mpath)
	disabled, err := f.dispatcher.DisableMountpath(mpath, whyFailed.Error())
	if err != nil {
		glog.Errorf("Failed to disable mountpath: %s", err.Error())
	} else if !disabled {
		glog.Errorf("Failed to disabled mountpath: %s. Mountpath already disabled", mpath)
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
	defer cmn.Close(file)

	slab := sgl.Slab()
	buf := slab.Alloc()
	defer slab.Free(buf)

	written, err := io.CopyBuffer(ioutil.Discard, file, buf)
	if err == nil && written < stat.Size() {
		return io.ErrShortWrite
	}

	return err
}

// checks if a given mpath is disabled. d.Path is always cleaned, that is
// why d.Path is searching inside mpath and not vice versa
func (f *FSHC) isMpathDisabled(mpath string) bool {
	_, disabled := fs.Get()

	for _, d := range disabled {
		if strings.HasPrefix(mpath, d.Path) {
			return true
		}
	}

	return false
}

// creates a random file in a random directory inside a mountpath
func (f *FSHC) tryWriteFile(mountpath string, fileSize int, sgl *memsys.SGL) error {
	// Do not test a mountpath if it is already disabled. To avoid a race
	// when a lot of PUTs fails and each of them calls FSHC, FSHC disables
	// the mountpath on the first run, so all other tryWriteFile are redundant
	if f.isMpathDisabled(mountpath) {
		return nil
	}

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

	tmpFileName := f.ctxResolver.GenContentFQN(filepath.Join(tmpdir, fshcNameTemplate), fs.WorkfileType, fs.WorkfileFSHC)
	tmpFile, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		glog.Errorf("Failed to create temporary file: %v", err)
		return err
	}

	slab := sgl.Slab()
	buf := slab.Alloc()
	defer func() {
		slab.Free(buf)
		if err := tmpFile.Close(); err != nil {
			glog.Errorf("Failed to close temporary file %s: %v", tmpFile.Name(), err)
		}
	}()

	_, _ = rand.Read(buf)
	bytesLeft := fileSize
	bufSize := len(buf)
	for bytesLeft > 0 {
		if bytesLeft > bufSize {
			_, err = tmpFile.Write(buf)
		} else {
			_, err = tmpFile.Write(buf[:bytesLeft])
		}
		if err != nil {
			glog.Errorf("Failed to write to file %s: %v", tmpFile.Name(), err)
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
func (f *FSHC) testMountpath(filePath, mountpath string,
	maxTestFiles, fileSize int) (readFails, writeFails int, accessible bool) {
	if glog.V(4) {
		glog.Infof("Testing mountpath %s", mountpath)
	}
	if _, err := os.Stat(mountpath); err != nil {
		glog.Errorf("Mountpath %s is unavailable", mountpath)
		return 0, 0, false
	}

	sgl := f.mm.NewSGL(0)
	defer sgl.Free()

	totalReads, totalWrites := 0, 0

	// 1. Read the file that causes the error, if it is defined
	if filePath != "" {
		if stat, err := os.Stat(filePath); err == nil && !stat.IsDir() {
			totalReads++
			err := f.tryReadFile(filePath, sgl)
			if err != nil {
				glog.Errorf("Checking file %s result: %#v, mountpath: %d", filePath, err, readFails)
				if cmn.IsIOError(err) {
					readFails++
				}
			}
		}
	}

	// 2. Read a few more files up to maxReads files
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

		glog.Infof("%s, files read: %d, failures: %d", mountpath, totalReads, readFails)
	}

	// 3. Try to create a few random files inside the mountpath
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
			if err != nil {
				return "", err
			}
			if chosen != "" {
				return chosen, nil
			}
		}
	}
	return "", err
}
