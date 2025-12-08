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
//
// For much more precise and extensive description of the functionality, please refer to
// * [docs/fshc.md](https://github.com/NVIDIA/aistore/blob/main/docs/fshc.md)

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
	singleDelayRetry = time.Second
)

const (
	maxDepth = 16 // recurs read
)

const (
	tempDir = "fshc-on-err" // to write test files
)

const (
	faulted  = "FAULTED"
	degraded = "DEGRADED"
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
			if cmn.Rom.V(4, cos.ModFS) {
				nlog.Infoln(mi.String(), "FSHC: not enough time passed since previous run:", since)
			}
			return
		}
		if !ratomic.CompareAndSwapInt64(&r.running, 0, now) {
			if cmn.Rom.V(4, cos.ModFS) {
				nlog.Infoln(mi.String(), "FSHC: run already in progress, skipping (since:", since, ")")
			}
			return
		}
	}

	go run(f, mi, r, fqn, now)
}

func run(f *FSHC, mi *fs.Mountpath, r *ror, fqn string, started int64) {
	f.run(mi, fqn)

	now := mono.NanoTime()
	nlog.Infoln(mi.String(), "FSHC runtime:", time.Duration(now-started))

	ratomic.StoreInt64(&r.last, now)
	ratomic.StoreInt64(&r.running, 0)
}

func (f *FSHC) run(mi *fs.Mountpath, fqn string) {
	var (
		froot      *os.File
		serr, pass string
		reason     = degraded
		cfg        = cmn.GCO.Get().FSHC

		// HardErrs: maximum total (read+write) IO errors allowed in a single
		// FSHC run before disabling the mountpath. See also IOErrs/IOErrTime,
		// which control how often FSHC is triggered at the stats layer.
		maxerrs  = cfg.HardErrs
		numFiles = cfg.TestFileCount
	)

	// 1. fstat
	err := cos.Stat(mi.Path)
	if shouldRetry(mi, err) {
		if _, err := os.Stat(mi.Path); err != nil {
			nlog.Errorln(mi.String(), faulted, err)
			reason = faulted
			goto disable
		}
	}

	// 2. resolve FS and check it vs mi.FS
	if err = mi.CheckFS(); err != nil {
		nlog.Errorln(err)
		reason = faulted
		goto disable
	}

	// 3. open root dir as a file; close immediately (retrying in re: network drives)
	froot, err = os.Open(mi.Path)
	if shouldRetry(mi, err) {
		if froot, err = os.Open(mi.Path); err != nil {
			nlog.Errorln(mi.String(), faulted, err)
			reason = faulted
			goto disable
		}
	}
	froot.Close()

	// mi.RescanDisks (currently, only manually via CLI)

	// double-check before reading/writing
	if !mi.IsAvail() {
		nlog.Warningln(mi.String(), "is not available, skipping FSHC run")
		return
	}

	// 4. read/write tests
	for i := range 2 {
		etag, rerrs, werrs := _rw(mi, fqn, numFiles, tmpSize, maxerrs)

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
		if etag == faulted {
			serr = fmt.Sprintf("%s %s: %s", mi, faulted, serr)
			reason = faulted
			break
		}

		if rerrs+werrs < maxerrs {
			nlog.Errorln("Warning: detected read/write errors", mi.String(), serr)
			nlog.Warningln("Warning: ignoring, _not_ disabling", mi.String())
			return
		}
		serr = "exceeded I/O error limit: " + serr
		// repeat just once
		if i == 0 {
			numFiles = cos.ClampInt(numFiles*2, numFiles+2, maxNumFiles)
			if numFiles >= 2*cfg.TestFileCount {
				maxerrs++
			}
			pass = ", pass #2"
			time.Sleep(2 * singleDelayRetry)
		}
	}
	nlog.Errorln(serr)
	nlog.Warningln("proceeding to disable", mi.String())

disable:
	f._disable(mi, reason)
}

func (f *FSHC) _disable(mi *fs.Mountpath, reason string) {
	if err := f.t.DisableMpath(mi); err != nil {
		nlog.Errorf("%s: failed to disable %s mountpath, err: %v", mi, reason, err)
	} else {
		nlog.Infoln(mi.String(), "now disabled, reason:", reason)
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
func _rw(mi *fs.Mountpath, fqn string, numFiles, fsize, maxerrs int) (etag string, rerrs, werrs int) {
	var numReads int

	// 1. Read the fqn that caused the error, if defined and is a file.
	if fqn != "" {
		nlog.Infoln("1. read one failed fqn:", fqn)
		if finfo, err := os.Stat(fqn); err == nil && !finfo.IsDir() {
			numReads++
			if err := _read(fqn); err != nil && !cos.IsNotExist(err) {
				nlog.Errorln(fqn, "[", err, "]")
				if cos.IsIOError(err) {
					rerrs++
				}
			}
		}
	}

	// 2. Read up to numFiles files.
	// Allow up to 2x iterations to account for empty dirs, symlinks, and non-regular files
	nlog.Infoln("2. read randomly up to", numFiles, "existing files")
	for i := 0; numReads < numFiles && rerrs < maxerrs && i < numFiles*2; i++ {
		fqn, etag, err := getRandFname(mi, mi.Path, 0 /*recurs depth*/)
		if err == io.EOF {
			if numReads == 0 {
				nlog.Infoln(mi.String(), "has no regular files to sample")
			}
			break
		}
		if err != nil {
			if etag == faulted {
				return etag, rerrs, 0
			}
			const efmt = "%s: %s (num-reads: %d, rerr: %d, err: %v)"
			if cos.IsIOError(err) {
				nlog.Errorf(efmt, mi, "io-error", numReads, rerrs, err)
				rerrs++
			} else {
				nlog.Warningf(efmt, mi, "failed to select existing file to read", numReads, rerrs, err)
			}
			continue
		}
		if err = _read(fqn); err != nil {
			const efmt = "%s: failed to read %s (%d, %d, %v)"
			if cos.IsIOError(err) {
				rerrs++
				nlog.Errorf(efmt, mi, fqn, numReads, rerrs, err)
			} else {
				nlog.Warningf(efmt, mi, fqn, numReads, rerrs, err)
			}
			continue
		}
		numReads++
	}

	// 3. Generate and write numFiles files.
	tmp := mi.TempDir(tempDir)
	nlog.Infoln("3. write", numFiles, "temp files to", tmp)
	if err := cos.CreateDir(tmp); err != nil { // create temp dir under the mountpath (under $deleted).
		const efmt = "%s: failed to create temp dir (%v, %q)"
		if cos.IsIOError(err) && shouldRetry(mi, err) {
			err = cos.CreateDir(tmp)
		}
		if cos.IsIOError(err) {
			etag = faulted
			werrs++
			nlog.Errorf(efmt, mi, err, etag)
		} else {
			nlog.Warningf(efmt, mi, err, etag)
		}
		return etag, rerrs, werrs
	}
	for numWrites := 1; numWrites <= numFiles && werrs < maxerrs; numWrites++ {
		if err := _write(tmp, fsize); err != nil {
			const efmt = "%s: failed to write (%d, %d, %v)"
			if cos.IsIOError(err) {
				werrs++
				nlog.Errorf(efmt, mi, numWrites, werrs, err)
			} else {
				nlog.Warningf(efmt, mi, numWrites, werrs, err)
			}
		}
	}

	// 4. Remove temp dir
	nlog.Infoln("4. remove", tmp)
	if err := os.RemoveAll(tmp); err != nil {
		if cos.IsIOError(err) {
			werrs++
		}
		nlog.Errorf("%s: %v (%d)", mi, err, werrs)
	}

	return etag, rerrs, werrs
}

//
// helper methods
//

// Open (O_DIRECT), read, and discard.
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

// traverse `dir` for a random file to read
func getRandFname(mi *fs.Mountpath, dir string, depth int) (fqn, etag string, _ error) {
	fdir, errN := os.Open(dir)
	if errN != nil {
		etag = cos.Ternary(depth == 0 && cos.IsIOError(errN), faulted, degraded)
		if etag == faulted && shouldRetry(mi, errN) {
			fdir, errN = os.Open(dir)
		}
		if errN != nil {
			return "", etag, errN
		}
	}

	dentries, err := fdir.ReadDir(maxNumFiles)

	if err != nil {
		etag = cos.Ternary(depth == 0 && cos.IsIOError(err), faulted, degraded)
		if etag == faulted && shouldRetry(mi, err) {
			dentries, err = fdir.ReadDir(maxNumFiles)
		}
		if err != nil {
			fdir.Close()
			return "", etag, err
		}
	}

	fdir.Close()

	l := len(dentries)
	if l == 0 {
		return "", "", io.EOF
	}
	pos := rand.IntN(l)
	for i := range l {
		j := (pos + i) % l
		de := dentries[j]
		fqn := filepath.Join(dir, de.Name())
		if de.Type().IsRegular() {
			return fqn, "", nil
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
		fqn, _, err = getRandFname(mi, fqn, depth)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return "", etag, err
		}
		return fqn, "", nil
	}

	return "", "", io.EOF
}

func shouldRetry(mi *fs.Mountpath, err error) bool {
	if err == nil {
		return false
	}
	nlog.Warningln(mi.String(), faulted, err, "- retrying once...")
	time.Sleep(singleDelayRetry)
	return true
}
