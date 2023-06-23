// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 *
 */
package health

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

const (
	fshcFileSize    = 10 * cos.MiB // size of temporary file which will test writing and reading the mountpath
	fshcMaxFileList = 100          // maximum number of files to read by Readdir

	fshcTemp = "fshc"
)

// When an IO error is triggered, it runs a few tests to make sure that the
// failed mountpath is healthy. Once the mountpath is considered faulty the
// mountpath is disabled and removed from the list.
//
// for mountpath definition, see fs/mountfs.go
type (
	fspathDispatcher interface {
		DisableMpath(mpath, reason string) (err error)
	}
	FSHC struct {
		dispatcher fspathDispatcher // listener is notified upon mountpath events (disabled, etc.)
		fileListCh chan string
		stopCh     cos.StopCh
	}
)

//////////
// FSHC //
//////////

// interface guard
var _ cos.Runner = (*FSHC)(nil)

func NewFSHC(dispatcher fspathDispatcher) (f *FSHC) {
	f = &FSHC{dispatcher: dispatcher, fileListCh: make(chan string, 100)}
	f.stopCh.Init()
	return
}

func (*FSHC) Name() string { return "fshc" }

func (f *FSHC) Run() error {
	nlog.Infof("Starting %s", f.Name())

	for {
		select {
		case filePath := <-f.fileListCh:
			mi, err := fs.Path2Mpath(filePath)
			if err != nil {
				nlog.Errorln(err)
				break
			}

			f.runMpathTest(mi.Path, filePath)
		case <-f.stopCh.Listen():
			return nil
		}
	}
}

func (f *FSHC) Stop(err error) {
	nlog.Infof("Stopping %s, err: %v", f.Name(), err)
	f.stopCh.Close()
}

func (f *FSHC) OnErr(fqn string) {
	if !cmn.GCO.Get().FSHC.Enabled {
		return
	}
	f.fileListCh <- fqn
}

func isTestPassed(mpath string, readErrors, writeErrors int, available bool) (passed bool, err error) {
	config := &cmn.GCO.Get().FSHC
	nlog.Infof("Tested mountpath %s(%v), read: %d of %d, write(size=%d): %d of %d",
		mpath, available,
		readErrors, config.ErrorLimit, fshcFileSize,
		writeErrors, config.ErrorLimit)

	if !available {
		return false, errors.New("mountpath is unavailable")
	}

	passed = readErrors < config.ErrorLimit && writeErrors < config.ErrorLimit
	if !passed {
		err = fmt.Errorf("too many errors: %d read error%s, %d write error%s",
			readErrors, cos.Plural(readErrors), writeErrors, cos.Plural(writeErrors))
	}
	return passed, err
}

func (f *FSHC) runMpathTest(mpath, filepath string) {
	var (
		config    = cmn.GCO.Get()
		whyFailed error
		passed    bool
	)
	readErrs, writeErrs, exists := testMountpath(config, filepath, mpath, fshcFileSize)
	if passed, whyFailed = isTestPassed(mpath, readErrs, writeErrs, exists); passed {
		return
	}
	nlog.Errorf("Disabling mountpath %s...", mpath)
	if err := f.dispatcher.DisableMpath(mpath, whyFailed.Error()); err != nil {
		nlog.Errorf("Failed to disable mountpath: %s", err.Error())
	}
}

// reads the entire file content
func tryReadFile(fqn string) error {
	file, err := fs.DirectOpen(fqn, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, file); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// Creates a random file in a random directory inside a mountpath.
func tryWriteFile(mpath string, fileSize int64) error {
	const ftag = "temp file"
	// Do not test a mountpath if it is already disabled. To avoid a race
	// when a lot of PUTs fail and each one calls FSHC, FSHC disables
	// the mountpath on the first run, so all other tryWriteFile are redundant
	available, disabled := fs.Get()
	if _, ok := disabled[mpath]; ok {
		return nil
	}
	mi, ok := available[mpath]
	if !ok {
		nlog.Warningf("Tried to write %s to non-existing mountpath %q", ftag, mpath)
		return nil
	}

	tmpDir := mi.TempDir(fshcTemp)
	if err := cos.CreateDir(tmpDir); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", tmpDir, err)
	}
	tmpFileName := filepath.Join(tmpDir, "fshc-try-write-"+cos.CryptoRandS(10))
	tmpFile, err := fs.DirectOpen(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, cos.PermRWR)
	if err != nil {
		return fmt.Errorf("failed to create %s, err: %w", ftag, err)
	}

	defer func() {
		if err := tmpFile.Close(); err != nil {
			nlog.Errorf("Failed to close %s %q, err: %v", ftag, tmpFileName, err)
		}
		if err := cos.RemoveFile(tmpFileName); err != nil {
			nlog.Errorf("Failed to remove %s %q, err: %v", ftag, tmpFileName, err)
		}
	}()

	if err = cos.FloodWriter(tmpFile, fileSize); err != nil {
		return fmt.Errorf("failed to write %s %q, err: %w", ftag, tmpFileName, err)
	}
	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s %q, err: %w", ftag, tmpFileName, err)
	}
	return nil
}

// the core testing function: reads existing and writes temporary files on mountpath
//  1. If the filepath points to existing file, it reads this file
//  2. Reads up to maxReads files selected at random
//  3. Creates up to maxWrites temporary files
//
// The function returns the number of read/write errors, and if the mountpath
//
//	is accessible. When the specified local directory is inaccessible the
//	function returns immediately without any read/write operations
func testMountpath(config *cmn.Config, filePath, mountpath string, fileSize int) (readFails, writeFails int, accessible bool) {
	if config.FastV(4, cos.SmoduleFS) {
		nlog.Infof("Testing mountpath %q", mountpath)
	}
	if err := cos.Stat(mountpath); err != nil {
		nlog.Errorf("Mountpath %q is unavailable", mountpath)
		return 0, 0, false
	}

	totalReads, totalWrites := 0, 0

	// 1. Read the file that causes the error, if it is defined.
	if filePath != "" {
		if stat, err := os.Stat(filePath); err == nil && !stat.IsDir() {
			totalReads++

			if err := tryReadFile(filePath); err != nil {
				nlog.Errorf("Failed to read file (fqn: %q, read_fails: %d, err: %v)", filePath, readFails, err)
				if cos.IsIOError(err) {
					readFails++
				}
			}
		}
	}

	// 2. Read a few more files up to maxReads files.
	maxTestFiles := config.FSHC.TestFileCount
	for totalReads < maxTestFiles {
		fqn, err := getRandomFileName(mountpath)
		if err == io.EOF {
			// No files in the mountpath.
			if config.FastV(4, cos.SmoduleFS) {
				nlog.Infof("Mountpath %q contains no files", mountpath)
			}
			break
		}
		totalReads++
		if err != nil {
			if cos.IsIOError(err) {
				readFails++
			}
			nlog.Errorf("Failed to select a random file (mountpath: %q, read_fails: %d, err: %v)",
				mountpath, readFails, err,
			)
			continue
		}
		if config.FastV(4, cos.SmoduleFS) {
			nlog.Infof("Reading random file (fqn: %q)", fqn)
		}
		if err = tryReadFile(fqn); err != nil {
			nlog.Errorf("Failed to read file (fqn: %q, err: %v)", fqn, err)
			if cos.IsIOError(err) {
				readFails++
			}
		}
	}

	// 3. Try to create a few random files inside the mountpath.
	for totalWrites < maxTestFiles {
		totalWrites++
		if err := tryWriteFile(mountpath, int64(fileSize)); err != nil {
			nlog.Errorf("Failed to write file (mountpath: %q, err: %v)", mountpath, err)
			if cos.IsIOError(err) {
				writeFails++
			}
		}
	}

	if readFails != 0 || writeFails != 0 {
		nlog.Errorf("Mountpath results (mountpath: %q, read_fails: %d, total_reads: %d, write_fails: %d, total_writes: %d)",
			mountpath, readFails, totalReads, writeFails, totalWrites,
		)
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

	files, err := file.ReadDir(fshcMaxFileList)
	if err == nil {
		fmap := make(map[string]os.DirEntry, len(files))
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
