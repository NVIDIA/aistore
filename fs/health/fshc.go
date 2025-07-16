// Package health is a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

// When triggered (via `OnErr`), FSHC runs assorted tests to check health of the
// associated mountpath.
// If:
// - the mountpath appears to be unavailable, or
// - configured error limit is exceeded
// the mountpath is disabled - effectively, removed from the operation henceforth.

// constants and tunables
const (
	tmpSize     = cos.MiB // write: temp file size
	maxNumFiles = 100     // read:  upto so many existing files
)

// - compare with cmn/cos/oom
// - compare with ais/tgtspace
const (
	minTimeBetweenRuns = 4 * time.Minute
)

const (
	maxDepth = 16 // recurs read
)

type (
	disabler interface {
		DisableMpath(mi *fs.Mountpath) error // impl. ais/tgtfshc.go
	}
	FSHC struct {
		t disabler
	}
)

type (
	ror struct {
		last    int64
		running int64
	}
)

// per mountpath: recent-or-running
var all sync.Map // [mpath => ror]

func NewFSHC(t disabler) (f *FSHC) { return &FSHC{t: t} }

func (*FSHC) IsErr(err error) bool {
	return cmn.IsErrGetCap(err) || cmn.IsErrMpathCheck(err) || cos.IsIOError(err)
}

// serialize per-mountpath runs
func (f *FSHC) OnErr(mi *fs.Mountpath, fqn string) {
	var (
		now       = mono.NanoTime()
		r         = &ror{last: now, running: now}
		a, loaded = all.LoadOrStore(mi.Path, r)
	)
	if loaded {
		r = a.(*ror)
		since := time.Duration(now - ratomic.LoadInt64(&r.last))
		if since < minTimeBetweenRuns {
			nlog.Infoln("not enough time passed since the previous run [", since, "]")
			return
		}
		if !ratomic.CompareAndSwapInt64(&r.running, 0, now) {
			nlog.Infoln(mi.String(), "still running [", since, "]")
			return
		}
	}

	go run(f, mi, r, fqn, now)
}

func run(f *FSHC, mi *fs.Mountpath, r *ror, fqn string, started int64) {
	f.run(mi, fqn)

	now := mono.NanoTime()
	nlog.Warningln("runtime:", time.Duration(now-started))

	ratomic.StoreInt64(&r.last, now)
	ratomic.StoreInt64(&r.running, 0)
}

func (f *FSHC) run(mi *fs.Mountpath, fqn string) {
	var (
		serr, pass string
		cfg        = cmn.GCO.Get().FSHC
		maxerrs    = cfg.HardErrs
		numFiles   = cfg.TestFileCount
	)
	// 1. fstat
	err := cos.Stat(mi.Path)
	if err != nil {
		nlog.Errorln("fstat err #1:", err)
		time.Sleep(time.Second)
		if _, err := os.Stat(mi.Path); err != nil {
			nlog.Errorln("critical fstat err #2:", err)
			goto disable
		}
	}

	// 2. resolve FS and check it vs mi.FS
	if err = mi.CheckFS(); err != nil {
		nlog.Errorln(err)
		goto disable
	}

	// 3. mi.RescanDisks (currently, only manually via CLI)

	// double-check before reading/writing
	if !mi.IsAvail() {
		nlog.Warningln(mi.String(), "is not available, nothing to do")
	}

	// 4. read/write tests
	for i := range 2 {
		rerrs, werrs := _rw(mi, fqn, numFiles, tmpSize)

		if rerrs == 0 && werrs == 0 {
			if i == 0 {
				nlog.Infoln(mi.String(), "appears to be healthy")
				return
			}
			nlog.Infoln(mi.String(), "- no read/write errors on a 2nd pass")
			return
		}
		serr = fmt.Sprintf("(read %d, write %d (max-errors %d, write-size %s%s))",
			rerrs, werrs, maxerrs, cos.ToSizeIEC(tmpSize, 0), pass)

		if rerrs+werrs < maxerrs {
			nlog.Errorln("Warning: detected read/write errors", mi.String(), serr)
			nlog.Warningln("Warning: ignoring, _not_ disabling", mi.String())
			return
		}
		// repeat just once
		if i == 0 {
			numFiles = max(min(numFiles*2, maxNumFiles), numFiles+2)
			if numFiles >= 2*cfg.TestFileCount {
				maxerrs++
			}
			pass = ", pass two"
			time.Sleep(2 * time.Second)
		}
	}
	nlog.Errorln("exceeded I/O error limit:", serr)
	nlog.Warningln("proceeding to disable", mi.String())

disable:
	f._disable(mi)
}

func (f *FSHC) _disable(mi *fs.Mountpath) {
	if err := f.t.DisableMpath(mi); err != nil {
		nlog.Errorf("%s: failed to disable, err: %v", mi, err)
	} else {
		nlog.Infoln(mi.String(), "now disabled")
		mi.SetFlags(fs.FlagDisabledByFSHC)
	}
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
func _rw(mi *fs.Mountpath, fqn string, numFiles, fsize int) (rerrs, werrs int) {
	var numReads int

	// 1. Read the fqn that caused the error, if defined and is a file.
	if fqn != "" {
		nlog.Infoln("1. read one failed fqn:", fqn)
		if finfo, err := os.Stat(fqn); err == nil && !finfo.IsDir() {
			numReads++
			if err := _read(fqn); err != nil && !cos.IsNotExist(err) {
				nlog.Errorln(fqn+":", err)
				if cos.IsIOError(err) {
					rerrs++
				}
			}
		}
	}

	// 2. Read up to numFiles files.
	nlog.Infoln("2. read randomly up to", numFiles, "existing files")
	for numReads < numFiles {
		fqn, err := getRandFname(mi.Path, 0 /*recurs depth*/)
		if err == io.EOF {
			if numReads == 0 {
				nlog.Warningln(mi.String(), "is suspiciously empty (???)")
			}
			break
		}
		if err != nil {
			if cos.IsIOError(err) {
				rerrs++
			}
			nlog.Errorf("%s: failed to select a random file to read: (%d, %d, %v)", mi, numReads, rerrs, err)
			continue
		}
		if err = _read(fqn); err != nil {
			rerrs++
			nlog.Errorf("%s: failed to read %s (%d, %d, %v)", mi, fqn, numReads, rerrs, err)
			continue
		}
		numReads++
	}

	// Create temp dir under the mountpath (under $deleted).
	tmpDir := mi.TempDir("fshc-on-err")
	if err := cos.CreateDir(tmpDir); err != nil {
		if cos.IsIOError(err) {
			werrs++
		}
		nlog.Errorf("%s: failed to create temp dir (%d, %v)", mi, werrs, err)
		return rerrs, werrs
	}

	// 3. Generate and write numFiles files.
	nlog.Infoln("3. write", numFiles, "temp files to", tmpDir)
	for numWrites := 1; numWrites <= numFiles; numWrites++ {
		if err := _write(tmpDir, fsize); err != nil {
			if cos.IsIOError(err) {
				werrs++
			}
			nlog.Errorf("%s: %v (%d)", mi, err, werrs)
		}
	}

	// 4. Remove temp dir
	nlog.Infoln("4. remove", tmpDir)
	if err := os.RemoveAll(tmpDir); err != nil {
		if cos.IsIOError(err) {
			werrs++
		}
		nlog.Errorf("%s: %v (%d)", mi, err, werrs)
	}

	return rerrs, werrs
}

//
// helper methods
//

// Open (O_DIRECT), read, and dicard.
func _read(fqn string) error {
	file, err := fs.DirectOpen(fqn, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, file)
	errC := file.Close()
	if err == nil {
		err = errC
	}
	return err
}

// Write random file under `tmpDir`.
func _write(tmpDir string, fsize int) error {
	fname := filepath.Join(tmpDir, cos.CryptoRandS(10))
	wfh, err := fs.DirectOpen(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, cos.PermRWR)
	if err != nil {
		return err
	}

	if err = cos.FloodWriter(wfh, int64(fsize)); err != nil {
		nlog.Errorln("failed to flood-write", fname, err)
		goto cleanup
	}
	if err = wfh.Sync(); err != nil {
		nlog.Errorln("failed to fsync", fname, err)
		goto cleanup
	}

cleanup:
	if e := wfh.Close(); err == nil && e != nil {
		err = e
		nlog.Errorln("failed to fclose", fname, err)
	}
	if e := cos.RemoveFile(fname); err == nil && e != nil {
		err = e
		nlog.Errorln("failed to remove", fname, err)
	}
	return err
}

// Look up a random file to read inside `basePath`.
func getRandFname(basePath string, depth int) (string, error) {
	file, errN := os.Open(basePath)
	if errN != nil {
		return "", errN
	}

	dentries, err := file.ReadDir(maxNumFiles)
	if err != nil {
		return "", err
	}
	l := len(dentries)
	if l == 0 {
		return "", io.EOF
	}
	pos := rand.IntN(l)
	for i := range l {
		j := (pos + i) % l
		de := dentries[j]
		fqn := filepath.Join(basePath, de.Name())
		if de.Type().IsRegular() {
			return fqn, nil
		}
		if !de.IsDir() {
			nlog.Warningln("not a directory and not a file:", fqn, "(???)")
			continue
		}
		// recurs in
		depth++
		if depth > maxDepth {
			continue
		}
		fqn, err = getRandFname(fqn, depth)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return "", err
		}
		return fqn, nil
	}

	return "", io.EOF
}
