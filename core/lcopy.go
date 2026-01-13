// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/sys"
)

//
// LOM copy management
//

func (lom *LOM) whingeCopy() (yes bool) {
	if !lom.IsCopy() {
		return
	}
	msg := fmt.Sprintf("unexpected: %s([fqn=%s] [hrw=%s] %+v)", lom, lom.FQN, *lom.HrwFQN, lom.md.copies)
	debug.Assert(false, msg)
	nlog.Errorln(msg)
	return true
}

func (lom *LOM) HasCopies() bool { return len(lom.md.copies) > 1 }
func (lom *LOM) NumCopies() int  { return max(len(lom.md.copies), 1) } // metadata-wise

// GetCopies returns all copies
// - copies include lom.FQN aka "main repl."
// - caller must take a lock
func (lom *LOM) GetCopies() fs.MPI {
	return lom.md.copies
}

// determines whether the two LOM _structures_ represent objects that must be _copies_ of each other
// (compare with IsCopy above)
func (lom *LOM) isMirror(dst *LOM) bool {
	return lom.MirrorConf().Enabled &&
		lom.ObjName == dst.ObjName &&
		lom.Bck().Equal(dst.Bck(), true /* must have same BID*/, true /* same backend */)
}

func (lom *LOM) delCopyMd(copyFQN string) {
	delete(lom.md.copies, copyFQN)
	if len(lom.md.copies) <= 1 {
		lom.md.copies = nil
	}
}

// NOTE: used only in tests
func (lom *LOM) AddCopy(copyFQN string, mpi *fs.Mountpath) error {
	if lom.md.copies == nil {
		lom.md.copies = make(fs.MPI, 2)
	}
	lom.md.copies[copyFQN] = mpi
	lom.md.copies[lom.FQN] = lom.mi
	return lom.syncMetaWithCopies()
}

func (lom *LOM) DelCopies(copiesFQN ...string) (err error) {
	numCopies := lom.NumCopies()
	// 1. Delete all copies from the metadata
	for _, copyFQN := range copiesFQN {
		if _, ok := lom.md.copies[copyFQN]; !ok {
			return fmt.Errorf("lom %s(num: %d): copy %s does not exist", lom, numCopies, copyFQN)
		}
		lom.delCopyMd(copyFQN)
	}

	// 2. Update metadata on remaining copies, if any
	if err := lom.syncMetaWithCopies(); err != nil {
		debug.AssertNoErr(err)
		return err
	}

	// 3. Remove the copies
	for _, copyFQN := range copiesFQN {
		if err1 := cos.RemoveFile(copyFQN); err1 != nil {
			nlog.Errorln(err1) // TODO: LRU should take care of that later.
			continue
		}
	}
	return
}

func (lom *LOM) DelAllCopies() (err error) {
	copiesFQN := make([]string, 0, len(lom.md.copies))
	for copyFQN := range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		copiesFQN = append(copiesFQN, copyFQN)
	}
	return lom.DelCopies(copiesFQN...)
}

// DelExtraCopies deletes obj replicas that are not part of the lom.md.copies metadata
// (cleanup)
func (lom *LOM) DelExtraCopies(fqn ...string) (removed bool, err error) {
	if lom.whingeCopy() {
		return
	}
	avail := fs.GetAvail()
	for _, mi := range avail {
		copyFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
		if _, ok := lom.md.copies[copyFQN]; ok {
			continue
		}
		if err1 := cos.RemoveFile(copyFQN); err1 != nil {
			err = err1
			continue
		}
		if len(fqn) > 0 && fqn[0] == copyFQN {
			removed = true
		}
	}
	return
}

// syncMetaWithCopies tries to make sure that all copies have identical metadata.
// NOTE: uname for LOM must be already locked.
// NOTE: changes _may_ be made - the caller must call lom.Persist() upon return
func (lom *LOM) syncMetaWithCopies() (err error) {
	var copyFQN string
	if !lom.HasCopies() {
		return nil
	}
	if !lom.WritePolicy().IsImmediate() {
		lom.md.makeDirty()
		return nil
	}
	for {
		if copyFQN, err = lom.persistMdOnCopies(); err == nil {
			break
		}
		lom.delCopyMd(copyFQN)
		if err1 := cos.Stat(copyFQN); err1 != nil && !cos.IsNotExist(err1) {
			mi, _, err2 := fs.FQN2Mpath(copyFQN)
			if err2 != nil {
				nlog.Errorln("nested err:", err2, "fqn:", copyFQN)
			} else {
				T.FSHC(err, mi, copyFQN)
			}
		}
	}
	return
}

// RestoreObjectFromAny tries to restore the object at its default location.
// Returns true if object exists, false otherwise
// TODO: locking vs concurrent restore: consider (read-lock object + write-lock meta) split
func (lom *LOM) RestoreToLocation() (exists bool) {
	lom.Lock(true)
	if err := lom.Load(true /*cache it*/, true /*locked*/); err == nil {
		lom.Unlock(true)
		return true // nothing to do
	}
	var (
		saved     = lom.md.pushrt()
		avail     = fs.GetAvail()
		buf, slab = g.pmm.Alloc()
	)
	for path, mi := range avail {
		if path == lom.mi.Path {
			continue
		}
		fqn := mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
		if err := cos.Stat(fqn); err != nil {
			continue
		}
		dst, err := lom._restore(fqn, buf)
		if err == nil {
			lom.md = dst.md
			lom.md.poprt(saved)
			exists = true
			FreeLOM(dst)
			break
		}
		if dst != nil {
			FreeLOM(dst)
		}
	}
	lom.Unlock(true)
	slab.Free(buf)
	return exists
}

func (lom *LOM) _restore(fqn string, buf []byte) (dst *LOM, err error) {
	src := lom.CloneTo(fqn)
	defer FreeLOM(src)
	if err = src.InitFQN(fqn, lom.Bucket()); err != nil {
		return
	}
	if err = src.Load(false /*cache it*/, true /*locked*/); err != nil {
		return
	}
	// restore at default location
	return src.Copy2FQN(lom.FQN, buf)
}

// increment the object's num copies by (well) copying the former
// (compare with lom.Copy2FQN below)
// TODO: add T.FSHC() calls _after_ having disambiguated source vs destination (to blame)
func (lom *LOM) Copy(mi *fs.Mountpath, buf []byte) error {
	debug.Assert(lom.bid() != 0, lom.String())
	if err := lom._checkBucket(); err != nil {
		return err
	}
	var (
		copyFQN = mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
		dstlom  = AllocLOM(lom.ObjName)
	)
	defer FreeLOM(dstlom)
	if err := dstlom.InitFQN(copyFQN, lom.Bucket()); err != nil {
		return err
	}
	workFQN := dstlom.GenFQN(fs.WorkCT, fs.WorkfileCopy)

	// copy is a no-op if the destination exists and is identical
	errExists := cos.Stat(copyFQN)
	if errExists == nil {
		cplom := AllocLOM(lom.ObjName)
		defer FreeLOM(cplom)
		if errExists = cplom.InitFQN(copyFQN, lom.Bucket()); errExists == nil {
			if errExists = cplom.LoadMetaFromFS(); errExists == nil {
				if cplom.CheckEq(lom) == nil {
					goto add // skip copying
				}
			}
		}
	}

	// do
	if _, _, err := cos.CopyFile(lom.FQN, workFQN, buf, cos.ChecksumNone); err != nil { // TODO: checksumming
		return err
	}
	if err := cos.Rename(workFQN, copyFQN); err != nil {
		T.FSHC(err, mi, copyFQN)
		if errRemove := cos.RemoveFile(workFQN); errRemove != nil && !cos.IsNotExist(errRemove) {
			nlog.Errorln("nested err:", errRemove)
		}
		return err
	}
add:
	// add md and persist
	lom.AddCopy(copyFQN, mi)
	if err := lom.Persist(); err != nil {
		lom.delCopyMd(copyFQN)
		nlog.Errorln(err)
		return err
	}
	return lom.syncMetaWithCopies()
}

// copy object => any local destination
// usage: copying between different buckets (compare with lom.Copy() above)
// compare w/ Ufest.Relocate(lom)
func (lom *LOM) Copy2FQN(dstFQN string, buf []byte) (dst *LOM, err error) {
	debug.Assert(lom.IsLocked() >= apc.LockRead, lom.Cname(), " source not locked")

	dst = lom.CloneTo(dstFQN)
	if err = dst.InitFQN(dstFQN, nil); err == nil {
		err = lom.copy2fqn(dst, buf)
	}
	if err != nil {
		FreeLOM(dst)
		dst = nil
	}
	return
}

// copyChunks handles copying all chunks and manifest from source to destination LOM
// TODO -- FIXME:
// - copying the chunks and the manifest not respecting the destination bucket's chunks config

func (lom *LOM) copyChunks(dst *LOM, buf []byte) error {
	// Load the completed Ufest manifest from source
	srcUfest, err := NewUfest("", lom, true)
	if err != nil {
		return fmt.Errorf("failed to create Ufest for source %s: %w", lom.Cname(), err)
	}

	err = srcUfest.LoadCompleted(lom)
	if err != nil {
		return fmt.Errorf("failed to load completed manifest for %s: %w", lom.Cname(), err)
	}

	// Create a new Ufest for destination
	dstUfest, err := NewUfest("", dst, false) // don't require existing
	if err != nil {
		return fmt.Errorf("failed to create Ufest for destination %s: %w", dst.Cname(), err)
	}

	// Copy each chunk from source to destination
	srcUfest.Lock()
	defer srcUfest.Unlock()

	var (
		wg = cos.NewLimitedWaitGroup(min(sys.MaxParallelism(), 4), srcUfest.Count())
		// errs  = cos.NewErrs(srcUfest.Count())
		errCh = make(chan error, srcUfest.Count())
	)

	for i := range srcUfest.chunks {
		wg.Add(1)
		go func(srcChunk Uchunk) {
			defer wg.Done()

			dstChunk, err := dstUfest.NewChunk(int(srcChunk.Num()), dst)
			if err != nil {
				errCh <- dstUfest._undoCopy(fmt.Errorf("failed to create destination chunk %d: %w", srcChunk.Num(), err))
				return
			}

			_, _, err = cos.CopyFile(srcChunk.Path(), dstChunk.Path(), buf, srcChunk.cksum.Type())
			if err != nil {
				errCh <- dstUfest._undoCopy(fmt.Errorf("failed to copy chunk %d from %s to %s: %w",
					srcChunk.Num(), srcChunk.Path(), dstChunk.Path(), err))
				return
			}

			if srcChunk.cksum != nil {
				dstChunk.SetCksum(srcChunk.cksum.Clone())
			}

			err = dstUfest.Add(dstChunk, srcChunk.Size(), int64(srcChunk.Num()))
			if err != nil {
				errCh <- dstUfest._undoCopy(fmt.Errorf("failed to add chunk %d to destination manifest: %w", srcChunk.Num(), err))
				return
			}
		}(srcUfest.chunks[i])
	}

	wg.Wait()
	close(errCh)

	// collect up to 4 errors and join them (allocate only if needed)
	var errs []error
	for err := range errCh {
		if err == nil {
			continue
		}
		if errs == nil {
			errs = make([]error, 0, 4)
		}
		if len(errs) < 4 {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return dstUfest._undoCopy(errors.Join(errs...))
	}

	// Compute the whole checksum for the destination manifest
	wholeCksum := cos.NewCksumHash(dst.CksumConf().Type)
	if err := dstUfest.ComputeWholeChecksum(wholeCksum); err != nil {
		return dstUfest._undoCopy(fmt.Errorf("failed to compute whole checksum for destination %s: %w", dst.Cname(), err))
	}
	dst.SetCksum(&wholeCksum.Cksum)

	// Store the completed manifest for destination
	err = dstUfest.storeCompleted(dst, false /*override*/)
	if err != nil {
		return dstUfest._undoCopy(fmt.Errorf("failed to store completed manifest for destination %s: %w", dst.Cname(), err))
	}

	dst.setlmfl(lmflChunk)
	return nil
}

func (u *Ufest) _undoCopy(err error) error {
	u.removeChunks(u.lom, false /*except first*/)
	return err
}

func (lom *LOM) copy2fqn(dst *LOM, buf []byte) (err error) {
	var (
		dstCksum   *cos.CksumHash
		dstFQN     = dst.FQN
		dstCksumTy = dst.CksumType()
	)
	if dst.isMirror(lom) && lom.md.copies != nil {
		dst.md.copies = maps.Clone(lom.md.copies)
	}
	if !dst.Bck().Equal(lom.Bck(), true /*same ID*/, true /*same backend*/) {
		// The copy will be in a new bucket - completely separate object. Hence, we have to set initial version.
		dst.SetVersion(lomInitialVersion)
	}

	workFQN := dst.GenFQN(fs.WorkCT, fs.WorkfileCopy)
	_, dstCksum, err = cos.CopyFile(lom.FQN, workFQN, buf, dstCksumTy)
	if err != nil {
		return err
	}

	if err = cos.Rename(workFQN, dstFQN); err != nil {
		if errRemove := cos.RemoveFile(workFQN); errRemove != nil && !cos.IsNotExist(errRemove) {
			nlog.Errorln("nested err:", errRemove)
		}
		return err
	}
	switch {
	case lom.IsChunked():
		if err := lom.copyChunks(dst, buf); err != nil {
			return err
		}
	case dstCksumTy != cos.ChecksumNone:
		dst.SetCksum(dstCksum.Clone())
	default:
		dst.SetCksum(cos.NoneCksum)
	}

	// persist
	if lom.isMirror(dst) {
		if lom.md.copies == nil {
			lom.md.copies = make(fs.MPI, 2)
			dst.md.copies = make(fs.MPI, 2)
		}
		lom.md.copies[dstFQN], dst.md.copies[dstFQN] = dst.mi, dst.mi
		lom.md.copies[lom.FQN], dst.md.copies[lom.FQN] = lom.mi, lom.mi
		if err = lom.syncMetaWithCopies(); err != nil {
			if _, ok := lom.md.copies[dst.FQN]; !ok {
				if errRemove := cos.RemoveFile(dst.FQN); errRemove != nil {
					nlog.Errorln("nested err:", errRemove)
				}
			}
			// `lom.syncMetaWithCopies()` may have made changes notwithstanding
			if errPersist := lom.Persist(); errPersist != nil {
				nlog.Errorln("nested err:", errPersist)
			}
			return err
		}
		err = lom.Persist()
	} else if err = dst.Persist(); err != nil {
		if errRemove := cos.RemoveFile(dst.FQN); errRemove != nil {
			nlog.Errorln("nested err:", errRemove)
		}
	}
	return err
}

// load-balanced GET from replicated lom
// - picks least-utilized mountpath
// - returns (open reader + its FQN) or (nil, "")
func (lom *LOM) OpenCopy() (cos.LomReader, string) {
	debug.Assert(lom.IsLocked() > apc.LockNone, lom.Cname(), " is not locked")
	debug.Assert(!lom.IsChunked())

	if !lom.HasCopies() {
		return nil, ""
	}

	var (
		fqn   = lom.FQN
		utils = fs.GetAllMpathUtils()
		curr  = utils.Get(lom.mi.Path)
	)
	for cfqn, cmi := range lom.md.copies {
		if cfqn == lom.FQN {
			continue
		}
		if cutil := utils.Get(cmi.Path); cutil < curr {
			fqn, curr = cfqn, cutil
		}
	}
	if fqn == lom.FQN {
		return nil, ""
	}
	if lh, err := os.Open(fqn); err == nil { // (compare w/ lom.Open())
		return lh, fqn
	}
	return nil, ""
}

// returns the least-utilized mountpath that does _not_ have a copy of this `lom` yet
// (compare with leastUtilCopy())
func (lom *LOM) LeastUtilNoCopy() (mi *fs.Mountpath) {
	var (
		avail      = fs.GetAvail()
		mpathUtils = fs.GetAllMpathUtils()
		minUtil    = int64(101) // to motivate the first assignment
	)
	for mpath, mpathInfo := range avail {
		if lom.haveMpath(mpath) || mpathInfo.IsAnySet(fs.FlagWaitingDD) {
			continue
		}
		if util := mpathUtils.Get(mpath); util < minUtil {
			minUtil, mi = util, mpathInfo
		}
	}
	return
}

func (lom *LOM) haveMpath(mpath string) bool {
	if len(lom.md.copies) == 0 {
		return lom.mi.Path == mpath
	}
	for _, mi := range lom.md.copies {
		if mi.Path == mpath {
			return true
		}
	}
	return false
}

func (lom *LOM) MirrorPaths() []string {
	if !lom.HasCopies() {
		return []string{lom.mi.Path}
	}
	lom.Lock(false)
	paths := make([]string, 0, len(lom.md.copies))
	for _, mi := range lom.md.copies {
		paths = append(paths, mi.Path)
	}
	lom.Unlock(false)
	return paths
}
