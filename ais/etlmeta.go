// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const etlMDCopies = 2 // local copies

var etlMDImmSize int64

type (
	etlMD struct {
		etl.EtlMD
		_sgl  *memsys.SGL // jsp-formatted
		cksum *cos.Cksum  // EtlMD checksum
	}

	etlOwner interface {
		sync.Locker
		Get() *etl.EtlMD

		init()
		get() (etlMD *etlMD)
		putPersist(etlMD *etlMD, payload msPayload) error
		persist(clone *etlMD, payload msPayload) error
		modify(*etlMDModifier) (*etlMD, error)
	}

	etlMDModifier struct {
		pre   func(ctx *etlMDModifier, clone *etlMD) (err error)
		final func(ctx *etlMDModifier, clone *etlMD)

		msg  etl.InitMsg
		wait bool
	}

	etlMDOwnerBase struct {
		sync.Mutex
		etlMD atomic.Pointer
	}
	etlMDOwnerPrx struct {
		etlMDOwnerBase
		fpath string
	}
	etlMDOwnerTgt struct{ etlMDOwnerBase }
)

// interface guard
var (
	_ revs     = (*etlMD)(nil)
	_ etlOwner = (*etlMDOwnerPrx)(nil)
	_ etlOwner = (*etlMDOwnerTgt)(nil)
)

// c-tor
func newEtlMD() *etlMD {
	etls := make(etl.ETLs, 16)
	return &etlMD{EtlMD: etl.EtlMD{ETLs: etls}}
}

// as revs
func (*etlMD) tag() string             { return revsEtlMDTag }
func (e *etlMD) version() int64        { return e.Version }
func (*etlMD) jit(p *proxyrunner) revs { return p.owner.etlMD.get() }
func (e *etlMD) sgl() *memsys.SGL      { return e._sgl }
func (e *etlMD) marshal() []byte {
	e._sgl = e._encode()
	return e._sgl.Bytes()
}

func (e *etlMD) _encode() (sgl *memsys.SGL) {
	sgl = memsys.PageMM().NewSGL(etlMDImmSize)
	// TODO: make similar to other MD
	err := jsp.Encode(sgl, e, jsp.CCSign(cmn.MetaverEtlMD))
	debug.AssertNoErr(err)
	etlMDImmSize = cos.MaxI64(etlMDImmSize, sgl.Len())
	return
}

func (e *etlMD) clone() *etlMD {
	dst := &etlMD{}
	// deep copy
	*dst = *e
	dst.ETLs = make(etl.ETLs, len(e.ETLs))
	for id, etl := range e.ETLs {
		dst.ETLs[id] = etl
	}
	dst._sgl = nil
	return dst
}

func (e *etlMD) add(spec etl.InitMsg) {
	e.Add(spec)
	e.Version++
}

//////////////////
// etlMDOwnerBase //
//////////////////

func (eo *etlMDOwnerBase) Get() *etl.EtlMD { return &eo.get().EtlMD }
func (eo *etlMDOwnerBase) get() *etlMD     { return (*etlMD)(eo.etlMD.Load()) }
func (eo *etlMDOwnerBase) put(etlMD *etlMD) {
	eo.etlMD.Store(unsafe.Pointer(etlMD))
}

// write metasync-sent bytes directly (no json)
func (*etlMDOwnerBase) persistBytes(payload msPayload, fpath string) (done bool) {
	if payload == nil {
		return
	}
	etlMDValue := payload[revsEtlMDTag]
	if etlMDValue == nil {
		return
	}
	var (
		etlMD *etl.EtlMD
		wto   = bytes.NewBuffer(etlMDValue)
		err   = jsp.SaveMeta(fpath, etlMD, wto)
	)
	done = err == nil
	return
}

/////////////////
// etlMDOwnerPrx //
/////////////////

func newEtlMDOwnerPrx(config *cmn.Config) *etlMDOwnerPrx {
	return &etlMDOwnerPrx{fpath: filepath.Join(config.ConfigDir, cmn.EmdFname)}
}

func (eo *etlMDOwnerPrx) init() {
	etlMD := newEtlMD()
	_, err := jsp.LoadMeta(eo.fpath, etlMD)
	if err != nil {
		if !os.IsNotExist(err) {
			glog.Errorf("failed to load %s from %s, err: %v", etlMD, eo.fpath, err)
		} else {
			glog.Infof("%s does not exist at %s - initializing", etlMD, eo.fpath)
		}
	}
	eo.put(etlMD)
}

func (eo *etlMDOwnerPrx) putPersist(etlMD *etlMD, payload msPayload) (err error) {
	if !eo.persistBytes(payload, eo.fpath) {
		debug.Assert(etlMD._sgl == nil)
		etlMD._sgl = etlMD._encode()
		err = jsp.SaveMeta(eo.fpath, etlMD, etlMD._sgl)
		if err != nil {
			etlMD._sgl.Free()
			etlMD._sgl = nil
		}
	}
	if err == nil {
		eo.put(etlMD)
	}
	return
}

func (*etlMDOwnerPrx) persist(_ *etlMD, _ msPayload) (err error) { debug.Assert(false); return }

func (eo *etlMDOwnerPrx) _pre(ctx *etlMDModifier) (clone *etlMD, err error) {
	eo.Lock()
	defer eo.Unlock()
	clone = eo.get().clone()
	if err = ctx.pre(ctx, clone); err != nil {
		return
	}
	err = eo.putPersist(clone, nil)
	return
}

func (eo *etlMDOwnerPrx) modify(ctx *etlMDModifier) (clone *etlMD, err error) {
	if clone, err = eo._pre(ctx); err != nil {
		if clone._sgl != nil {
			clone._sgl.Free()
			clone._sgl = nil
		}
		return
	}
	if ctx.final != nil {
		ctx.final(ctx, clone)
	} else if clone._sgl != nil {
		clone._sgl.Free()
		clone._sgl = nil
	}
	return
}

/////////////////
// etlMDOwnerTgt //
/////////////////

func newEtlMDOwnerTgt() *etlMDOwnerTgt {
	return &etlMDOwnerTgt{}
}

func (eo *etlMDOwnerTgt) init() {
	var (
		etlMD     *etlMD
		available = fs.GetAvail()
	)
	if etlMD = loadEtlMD(available, cmn.EmdFname); etlMD != nil {
		glog.Infof("loaded %s", etlMD)
		goto finalize
	}
	etlMD = newEtlMD()
	glog.Warningf("initializing new %s", etlMD)

finalize:
	eo.put(etlMD)
}

func (eo *etlMDOwnerTgt) putPersist(etlMD *etlMD, payload msPayload) (err error) {
	if err = eo.persist(etlMD, payload); err == nil {
		eo.put(etlMD)
	}
	return
}

func (*etlMDOwnerTgt) persist(clone *etlMD, payload msPayload) (err error) {
	var (
		b   []byte
		sgl *memsys.SGL
	)
	if payload != nil {
		if etlMDValue := payload[revsEtlMDTag]; etlMDValue != nil {
			b = etlMDValue
		}
	}
	if b == nil {
		sgl = clone._encode()
		defer sgl.Free()
	}
	cnt, availCnt := fs.PersistOnMpaths(cmn.EmdFname, "" /*backup*/, clone, etlMDCopies, b, sgl)
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		glog.Errorf("Cannot store %s: %v", clone, cmn.ErrNoMountpaths)
		return
	}
	err = fmt.Errorf("failed to store %s on any of the mountpaths (%d)", clone, availCnt)
	glog.Error(err)
	return
}

func (*etlMDOwnerTgt) modify(_ *etlMDModifier) (*etlMD, error) {
	debug.Assert(false)
	return nil, nil
}

func loadEtlMD(mpaths fs.MPI, path string) (mainEtlMD *etlMD) {
	for _, mpath := range mpaths {
		etlMD := loadEtlMDFromMpath(mpath, path)
		if etlMD == nil {
			continue
		}
		if mainEtlMD != nil {
			if !mainEtlMD.cksum.Equal(etlMD.cksum) {
				cos.ExitLogf("EtlMD is different (%q): %v vs %v", mpath, mainEtlMD, etlMD)
			}
			continue
		}
		mainEtlMD = etlMD
	}
	return
}

func loadEtlMDFromMpath(mpath *fs.MountpathInfo, path string) (etlMD *etlMD) {
	var (
		fpath = filepath.Join(mpath.Path, path)
		err   error
	)
	etlMD = newEtlMD()
	etlMD.cksum, err = jsp.LoadMeta(fpath, etlMD)
	if err == nil {
		return etlMD
	}
	if !os.IsNotExist(err) {
		// Should never be NotExist error as mpi should include only mpaths with relevant etlMDs stored.
		glog.Errorf("failed to load %s from %s, err: %v", etlMD, fpath, err)
	}
	return nil
}

func hasEnoughEtlMDCopies() bool { return fs.CountPersisted(cmn.EmdFname) >= etlMDCopies }
