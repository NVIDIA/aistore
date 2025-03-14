// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const etlMDCopies = 2 // local copies

var etlMDImmSize int64

type (
	etlMD struct {
		etl.MD
		cksum *cos.Cksum
	}

	etlOwner interface {
		sync.Locker
		Get() *etl.MD

		init()
		get() (etlMD *etlMD)
		putPersist(etlMD *etlMD, payload msPayload) error
		persist(clone *etlMD, payload msPayload) error
		modify(*etlMDModifier) (*etlMD, error)
	}

	etlMDModifier struct {
		pre   func(ctx *etlMDModifier, clone *etlMD) (err error)
		final func(ctx *etlMDModifier, clone *etlMD)

		msg     etl.InitMsg
		stage   etl.Stage
		xid     string
		etlName string
		wait    bool
	}

	etlMDOwnerBase struct {
		etlMD ratomic.Pointer[etlMD]
		sync.Mutex
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
func newEtlMD() (e *etlMD) {
	e = &etlMD{}
	e.MD.Init(4)
	return
}

// as revs
func (*etlMD) tag() string       { return revsEtlMDTag }
func (e *etlMD) version() int64  { return e.Version }
func (*etlMD) uuid() string      { return "" } // TODO: add
func (*etlMD) jit(p *proxy) revs { return p.owner.etl.get() }
func (*etlMD) sgl() *memsys.SGL  { return nil }

// always remarshal (TODO: unify and optimize across all cluster-level metadata types)
func (e *etlMD) marshal() []byte {
	sgl := memsys.PageMM().NewSGL(etlMDImmSize)
	err := jsp.Encode(sgl, e, jsp.CCSign(cmn.MetaverEtlMD))
	debug.AssertNoErr(err)
	etlMDImmSize = max(etlMDImmSize, sgl.Len())
	b := sgl.ReadAll() // TODO: optimize
	sgl.Free()
	return b
}

func (e *etlMD) clone() *etlMD {
	dst := &etlMD{}
	*dst = *e
	dst.Init(len(e.ETLs))
	for id, etl := range e.ETLs {
		dst.ETLs[id] = etl
	}
	return dst
}

func (e *etlMD) add(spec etl.InitMsg, xid string, stage etl.Stage) {
	e.Add(spec, xid, stage)
	e.Version++
}

func (e *etlMD) get(id string) (msg etl.InitMsg, xid string) {
	return e.ETLs[id].InitMsg, e.ETLs[id].XactID
}

func (e *etlMD) del(id string) (exists bool) {
	_, exists = e.ETLs[id]
	delete(e.ETLs, id)
	return
}

////////////////////
// etlMDOwnerBase //
////////////////////

func (eo *etlMDOwnerBase) Get() *etl.MD { return &eo.get().MD }

func (eo *etlMDOwnerBase) get() *etlMD      { return eo.etlMD.Load() }
func (eo *etlMDOwnerBase) put(etlMD *etlMD) { eo.etlMD.Store(etlMD) }

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
		etlMD *etl.MD
		wto   = cos.NewBuffer(etlMDValue)
		err   = jsp.SaveMeta(fpath, etlMD, wto)
	)
	done = err == nil
	return
}

///////////////////
// etlMDOwnerPrx //
///////////////////

func newEtlMDOwnerPrx(config *cmn.Config) *etlMDOwnerPrx {
	return &etlMDOwnerPrx{fpath: filepath.Join(config.ConfigDir, fname.Emd)}
}

func (eo *etlMDOwnerPrx) init() {
	etlMD := newEtlMD()
	_, err := jsp.LoadMeta(eo.fpath, etlMD)
	if err != nil {
		if !os.IsNotExist(err) {
			nlog.Errorf("failed to load %s from %s, err: %v", etlMD, eo.fpath, err)
		} else {
			nlog.Infof("%s does not exist at %s - initializing", etlMD, eo.fpath)
		}
	}
	eo.put(etlMD)
}

func (eo *etlMDOwnerPrx) putPersist(etlMD *etlMD, payload msPayload) (err error) {
	if !eo.persistBytes(payload, eo.fpath) {
		err = jsp.SaveMeta(eo.fpath, etlMD, nil)
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
	etlMD := eo.get()
	clone = etlMD.clone()
	if err = ctx.pre(ctx, clone); err != nil {
		return
	}
	err = eo.putPersist(clone, nil)
	return
}

func (eo *etlMDOwnerPrx) modify(ctx *etlMDModifier) (clone *etlMD, err error) {
	if clone, err = eo._pre(ctx); err != nil {
		return
	}
	if ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return
}

///////////////////
// etlMDOwnerTgt //
///////////////////

func newEtlMDOwnerTgt() *etlMDOwnerTgt {
	return &etlMDOwnerTgt{}
}

func (eo *etlMDOwnerTgt) init() {
	var (
		etlMD     *etlMD
		available = fs.GetAvail()
	)
	if etlMD = loadEtlMD(available, fname.Emd); etlMD != nil {
		nlog.Infoln("loaded", etlMD.String())
	} else {
		etlMD = newEtlMD()
		nlog.Infoln("initializing new", etlMD.String())
	}
	eo.put(etlMD)
}

func (eo *etlMDOwnerTgt) putPersist(etlMD *etlMD, payload msPayload) (err error) {
	if err = eo.persist(etlMD, payload); err == nil {
		eo.put(etlMD)
	}
	return
}

func (*etlMDOwnerTgt) persist(clone *etlMD, payload msPayload) (err error) {
	var b []byte
	if payload != nil {
		if etlMDValue := payload[revsEtlMDTag]; etlMDValue != nil {
			b = etlMDValue
		}
	}
	if b == nil {
		b = clone.marshal()
	}
	cnt, availCnt := fs.PersistOnMpaths(fname.Emd, "" /*backup*/, clone, etlMDCopies, b, nil /*sgl*/)
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		nlog.Errorln("Cannot store", clone.String()+":", cmn.ErrNoMountpaths) // there's a bigger problem
		return
	}
	err = fmt.Errorf("failed to store %s on any of the mountpaths (%d)", clone, availCnt)
	nlog.Errorln(err)
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
		if mainEtlMD == nil {
			mainEtlMD = etlMD
			continue
		}
		if mainEtlMD.cksum.IsEmpty() {
			cos.ExitLogf("EtlMD is not checksummed (%q): %v", mpath, mainEtlMD)
		}
		if mainEtlMD.cksum.Equal(etlMD.cksum) {
			continue
		}
		if mainEtlMD.Version == etlMD.Version {
			cos.ExitLogf("EtlMD is different (%q): %v vs %v", mpath, mainEtlMD, etlMD)
		}
		nlog.Errorf("Warning: detected different EtlMD versions (%q): %v != %v", mpath, mainEtlMD, etlMD)
		if mainEtlMD.Version < etlMD.Version {
			mainEtlMD = etlMD
		}
	}
	return
}

func loadEtlMDFromMpath(mpath *fs.Mountpath, path string) (etlMD *etlMD) {
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
		nlog.Errorf("failed to load %s from %s: %v", etlMD, fpath, err)
	}
	return nil
}

func hasEnoughEtlMDCopies() bool { return fs.CountPersisted(fname.Emd) >= etlMDCopies }
