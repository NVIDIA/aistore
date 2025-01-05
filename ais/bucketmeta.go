// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// NOTE: to access bucket metadata and related structures, external
//       packages and HTTP clients must import aistore/cluster (and not ais)

// - bucketMD is a server-side extension of the meta.BMD
// - bucketMD represents buckets (that store objects) and associated metadata
// - bucketMD (instance) can be obtained via bmdOwner.get()
// - bucketMD is immutable and versioned
// - bucketMD versioning is monotonic and incremental
//
// - bucketMD typical update transaction:
//   lock -- clone() -- modify the clone -- bmdOwner.put(clone) -- unlock
//
// (*) for merges and conflict resolution, check the current version prior to put()
//     (note that version check must be protected by the same critical section)
//

const bmdCopies = 2 // local copies

type (
	bucketMD struct {
		cksum *cos.Cksum  // BMD checksum
		_sgl  *memsys.SGL // jsp-formatted
		vstr  string      // itoa(Version), to have it handy for http redirects
		meta.BMD
	}
	bmdOwner interface {
		sync.Locker
		Get() *meta.BMD

		init() bool // true when loaded previous version
		get() (bmd *bucketMD)
		put(bmd *bucketMD)
		putPersist(bmd *bucketMD, payload msPayload) error
		persist(clone *bucketMD, payload msPayload) error
		modify(*bmdModifier) (*bucketMD, error)
	}
	bmdOwnerBase struct {
		bmd ratomic.Pointer[bucketMD]
		sync.Mutex
	}
	bmdOwnerPrx struct {
		bmdOwnerBase
		fpath string
	}
	bmdOwnerTgt struct{ bmdOwnerBase }

	bmdModifier struct {
		pre   func(*bmdModifier, *bucketMD) error
		final func(*bmdModifier, *bucketMD)

		msg *apc.ActMsg

		propsToUpdate *cmn.BpropsToSet // update existing props
		revertProps   *cmn.BpropsToSet // props to revert
		setProps      *cmn.Bprops      // new props to set

		txnID string // transaction UUID
		bcks  []*meta.Bck

		wait         bool
		needReMirror bool
		needReEC     bool
		terminate    bool
		singleTarget bool
	}
)

// interface guard
var (
	_ revs        = (*bucketMD)(nil)
	_ meta.Bowner = (*bmdOwnerBase)(nil)
	_ bmdOwner    = (*bmdOwnerPrx)(nil)
	_ bmdOwner    = (*bmdOwnerTgt)(nil)
)

var bmdImmSize int64

// c-tor
func newBucketMD() *bucketMD {
	providers := make(meta.Providers, 2)
	namespaces := make(meta.Namespaces, 1)
	providers[apc.AIS] = namespaces
	buckets := make(meta.Buckets, 16)
	debug.Assert(cmn.NsGlobalUname == cmn.NsGlobal.Uname())
	namespaces[cmn.NsGlobalUname] = buckets

	return &bucketMD{BMD: meta.BMD{Providers: providers, UUID: ""}}
}

func newClusterUUID() (uuid, created string) {
	return cos.GenUUID(), time.Now().String()
}

//////////////
// bucketMD //
//////////////

func (m *bucketMD) add(bck *meta.Bck, p *cmn.Bprops) bool {
	debug.Assert(apc.IsProvider(bck.Provider))
	if _, present := m.Get(bck); present {
		return false
	}

	if m.Version == 0 {
		m.Version = 1 // on-the-fly (e.g. via PUT remote) w/ brand-new cluster
	}
	p.SetProvider(bck.Provider)
	p.BID = core.NewBID(uint64(m.Version), bck.IsAIS())
	p.Created = time.Now().UnixNano()
	bck.Props = p

	m.Add(bck)
	m.Version++

	return true
}

func (m *bucketMD) del(bck *meta.Bck) (deleted bool) {
	if !m.Del(bck) {
		return
	}
	m.Version++
	return true
}

func (m *bucketMD) set(bck *meta.Bck, p *cmn.Bprops) {
	debug.Assert(apc.IsProvider(bck.Provider))
	prevProps, present := m.Get(bck)
	if !present {
		debug.Assertf(false, "%s: not present", bck)
	}
	debug.Assert(prevProps.BID != 0)

	p.SetProvider(bck.Provider)
	p.BID = prevProps.BID

	// make sure bck.backend, if exists, references backend's own props in the BMD
	if p.BackendBck.Name != "" && p.BackendBck.Props == nil {
		if provider, err := cmn.NormalizeProvider(p.BackendBck.Provider); err == nil {
			p.BackendBck.Provider = provider
			p.BackendBck.Props, _ = m.Get((*meta.Bck)(&p.BackendBck))
		}
	}

	m.Set(bck, p)

	m.Version++
}

func (m *bucketMD) clone() *bucketMD {
	dst := &bucketMD{}

	// deep copy
	*dst = *m
	dst.Providers = make(meta.Providers, len(m.Providers))
	for provider, namespaces := range m.Providers {
		dstNamespaces := make(meta.Namespaces, len(namespaces))
		for ns, buckets := range namespaces {
			dstBuckets := make(meta.Buckets, len(buckets))
			for name, p := range buckets {
				dstProps := &cmn.Bprops{}
				*dstProps = *p
				dstBuckets[name] = dstProps
			}
			dstNamespaces[ns] = dstBuckets
		}
		dst.Providers[provider] = dstNamespaces
	}

	dst.vstr = m.vstr
	dst._sgl = nil
	return dst
}

func (m *bucketMD) validateUUID(nbmd *bucketMD, si, nsi *meta.Snode, caller string) (err error) {
	if nbmd == nil || nbmd.Version == 0 || m.Version == 0 {
		return
	}
	if !cos.IsValidUUID(m.UUID) || !cos.IsValidUUID(nbmd.UUID) {
		return
	}
	if m.UUID == nbmd.UUID {
		return
	}
	nsiname := caller
	if nsi != nil {
		nsiname = nsi.StringEx()
	} else if nsiname == "" {
		nsiname = "???"
	}
	hname := si.Name()
	// FATAL: cluster integrity error (cie)
	s := fmt.Sprintf("%s: BMDs have different UUIDs: (%s, %s) vs (%s, %s)",
		ciError(40), hname, m.StringEx(), nsiname, nbmd.StringEx())
	err = &errPrxBmdUUIDDiffer{s}
	return
}

// as revs
func (*bucketMD) tag() string       { return revsBMDTag }
func (m *bucketMD) version() int64  { return m.Version }
func (m *bucketMD) uuid() string    { return m.UUID }
func (*bucketMD) jit(p *proxy) revs { return p.owner.bmd.get() }

func (m *bucketMD) sgl() *memsys.SGL {
	if m._sgl.IsNil() {
		return nil
	}
	return m._sgl
}

func (m *bucketMD) marshal() []byte {
	m._sgl = m._encode()
	return m._sgl.Bytes()
}

func (m *bucketMD) _encode() (sgl *memsys.SGL) {
	sgl = memsys.PageMM().NewSGL(bmdImmSize)
	err := jsp.Encode(sgl, m, m.JspOpts())
	debug.AssertNoErr(err)
	bmdImmSize = max(bmdImmSize, sgl.Len())
	return
}

//////////////////
// bmdOwnerBase //
//////////////////

func (bo *bmdOwnerBase) Get() *meta.BMD       { return &bo.get().BMD }
func (bo *bmdOwnerBase) get() (bmd *bucketMD) { return bo.bmd.Load() }

func (bo *bmdOwnerBase) put(bmd *bucketMD) {
	bmd.vstr = strconv.FormatInt(bmd.Version, 10)
	bo.bmd.Store(bmd)
}

// write metasync-sent bytes directly (no json)
func (*bmdOwnerBase) persistBytes(payload msPayload, fpath string) (done bool) {
	if payload == nil {
		return
	}
	bmdValue := payload[revsBMDTag]
	if bmdValue == nil {
		return
	}
	var (
		bmd *meta.BMD
		wto = cos.NewBuffer(bmdValue)
		err = jsp.SaveMeta(fpath, bmd, wto)
	)
	done = err == nil
	return
}

/////////////////
// bmdOwnerPrx //
/////////////////

func newBMDOwnerPrx(config *cmn.Config) *bmdOwnerPrx {
	return &bmdOwnerPrx{fpath: filepath.Join(config.ConfigDir, fname.Bmd)}
}

func (bo *bmdOwnerPrx) init() (prev bool) {
	bmd, err := _loadBMD(bo.fpath)
	if err != nil {
		if !os.IsNotExist(err) {
			nlog.Errorf("failed to load %s from %s, err: %v", bmd, bo.fpath, err)
		} else {
			nlog.Infof("%s does not exist at %s - initializing", bmd, bo.fpath)
		}
	}
	bo.put(bmd)
	return
}

func (bo *bmdOwnerPrx) putPersist(bmd *bucketMD, payload msPayload) (err error) {
	if !bo.persistBytes(payload, bo.fpath) {
		debug.Assert(bmd._sgl == nil)
		bmd._sgl = bmd._encode()
		err = jsp.SaveMeta(bo.fpath, bmd, bmd._sgl)
		if err != nil {
			bmd._sgl.Free()
			bmd._sgl = nil
		}
	}
	if err == nil {
		bo.put(bmd)
	}
	return
}

func (*bmdOwnerPrx) persist(_ *bucketMD, _ msPayload) (err error) { debug.Assert(false); return }

// under lock
func (bo *bmdOwnerPrx) _pre(ctx *bmdModifier) (clone *bucketMD, err error) {
	clone = bo.get().clone()
	if err = ctx.pre(ctx, clone); err != nil || ctx.terminate {
		return
	}
	err = bo.putPersist(clone, nil)
	return
}

func (bo *bmdOwnerPrx) modify(ctx *bmdModifier) (clone *bucketMD, err error) {
	bo.Lock()
	clone, err = bo._pre(ctx)
	bo.Unlock()
	if err != nil || ctx.terminate {
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
// bmdOwnerTgt //
/////////////////

func newBMDOwnerTgt() *bmdOwnerTgt {
	return &bmdOwnerTgt{}
}

func (bo *bmdOwnerTgt) init() (prev bool) {
	var (
		bmd       *bucketMD
		available = fs.GetAvail()
	)
	if bmd = loadBMD(available, fname.Bmd); bmd != nil {
		nlog.Infof("loaded %s", bmd)
		goto finalize
	}
	if bmd = loadBMD(available, fname.BmdPrevious); bmd != nil {
		nlog.Errorf("loaded previous version of the %s (%q)", bmd, fname.BmdPrevious)
		prev = true
		goto finalize
	}
	bmd = newBucketMD()
	nlog.Warningf("initializing new %s", bmd)

finalize:
	bo.put(bmd)
	return
}

func (bo *bmdOwnerTgt) putPersist(bmd *bucketMD, payload msPayload) (err error) {
	if err = bo.persist(bmd, payload); err == nil {
		bo.put(bmd)
	}
	return
}

func (*bmdOwnerTgt) persist(clone *bucketMD, payload msPayload) (err error) {
	var (
		b   []byte
		sgl *memsys.SGL
	)
	if payload != nil {
		if bmdValue := payload[revsBMDTag]; bmdValue != nil {
			b = bmdValue
		}
	}
	if b == nil {
		sgl = clone._encode()
		defer sgl.Free()
	}
	cnt, availCnt := fs.PersistOnMpaths(fname.Bmd, fname.BmdPrevious, clone, bmdCopies, b, sgl)
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		nlog.Errorf("Cannot store %s: %v", clone, cmn.ErrNoMountpaths)
		return
	}
	err = fmt.Errorf("failed to store %s on any of the mountpaths (%d)", clone, availCnt)
	nlog.Errorln(err)
	return
}

func (*bmdOwnerTgt) modify(_ *bmdModifier) (*bucketMD, error) {
	debug.Assert(false)
	return nil, nil
}

func loadBMD(mpaths fs.MPI, path string) (mainBMD *bucketMD) {
	for _, mpath := range mpaths {
		bmd := loadBMDFromMpath(mpath, path)
		if bmd == nil {
			continue
		}
		if mainBMD == nil {
			mainBMD = bmd
			continue
		}
		if mainBMD.cksum.IsEmpty() {
			cos.ExitLogf("BMD is not checksummed (%q): %v", mpath, mainBMD)
		}
		if mainBMD.cksum.Equal(bmd.cksum) {
			continue
		}
		if mainBMD.Version == bmd.Version {
			cos.ExitLogf("BMD is different (%q): %v vs %v", mpath, mainBMD, bmd)
		}
		nlog.Errorf("Warning: detected different BMD versions (%q): %v != %v", mpath, mainBMD, bmd)
		if mainBMD.Version < bmd.Version {
			mainBMD = bmd
		}
	}
	return
}

func _loadBMD(path string) (bmd *bucketMD, err error) {
	bmd = newBucketMD()
	bmd.cksum, err = jsp.LoadMeta(path, bmd)
	if _, ok := err.(*jsp.ErrUnsupportedMetaVersion); ok {
		nlog.Errorf(cmn.FmtErrBackwardCompat, err)
	}
	return
}

func loadBMDFromMpath(mpath *fs.Mountpath, path string) (bmd *bucketMD) {
	var (
		fpath = filepath.Join(mpath.Path, path)
		err   error
	)
	bmd, err = _loadBMD(fpath)
	if err == nil {
		return bmd
	}
	if !os.IsNotExist(err) {
		// Should never be NotExist error as mpi should include only mpaths with relevant bmds stored.
		nlog.Errorf("failed to load %s from %s, err: %v", bmd, fpath, err)
	}
	return nil
}

func hasEnoughBMDCopies() bool { return fs.CountPersisted(fname.Bmd) >= bmdCopies }

//////////////////////////
// default bucket props //
//////////////////////////

type bckPropsArgs struct {
	bck *meta.Bck   // Base bucket for determining default bucket props.
	hdr http.Header // Header with remote bucket properties.
}

// Convert HEAD(bucket) response to cmn.Bprops (compare with `defaultBckProps`)
func remoteBckProps(args bckPropsArgs) (props *cmn.Bprops, err error) {
	props = &cmn.Bprops{}
	err = cmn.IterFields(props, func(tag string, field cmn.IterField) (error, bool) {
		headerName := textproto.CanonicalMIMEHeaderKey(tag)
		// skip the missing ones
		if _, ok := args.hdr[headerName]; !ok {
			return nil, false
		}
		// single-value
		return field.SetValue(args.hdr.Get(headerName), true /*force*/), false
	}, cmn.IterOpts{OnlyRead: false})
	return
}

// Used to initialize "local" bucket, in particular when there's a remote one
// (compare with `remoteBckProps` above)
// See also:
//   - github.com/NVIDIA/aistore/blob/main/docs/bucket.md#default-bucket-properties
//   - cmn.BpropsToSet
//   - cmn.Bck.DefaultProps
func defaultBckProps(args bckPropsArgs) (props *cmn.Bprops) {
	config := cmn.GCO.Get()
	props = args.bck.Bucket().DefaultProps(&config.ClusterConfig)
	props.SetProvider(args.bck.Provider)

	switch {
	case args.bck.IsAIS():
		debug.Assert(args.hdr == nil)
	case args.bck.Backend() != nil:
		debug.Assertf(args.hdr == nil, "%s, hdr=%+v", args.bck, args.hdr)
	case args.bck.IsRemote():
		debug.Assert(args.hdr != nil)
		props.Versioning.Enabled = false
		props = mergeRemoteBckProps(props, args.hdr)
	default:
		debug.Assert(false)
	}
	err := props.Validate(9999 /*targetCnt*/)
	debug.AssertNoErr(err)
	return
}

func mergeRemoteBckProps(props *cmn.Bprops, header http.Header) *cmn.Bprops {
	debug.Assert(len(header) > 0)
	switch props.Provider {
	case apc.AWS:
		props.Extra.AWS.CloudRegion = header.Get(apc.HdrS3Region)
		props.Extra.AWS.Endpoint = header.Get(apc.HdrS3Endpoint)
		props.Extra.AWS.Profile = header.Get(apc.HdrS3Profile)
	case apc.HT:
		props.Extra.HTTP.OrigURLBck = header.Get(apc.HdrOrigURLBck)
	}

	if verStr := header.Get(apc.HdrBucketVerEnabled); verStr != "" {
		versioning, err := cos.ParseBool(verStr)
		debug.AssertNoErr(err)
		props.Versioning.Enabled = versioning
	}
	return props
}

// returns (uname, nlc) pair to lock/unlock buckets
func newBckNLP(b *meta.Bck) core.NLP { return core.NewNLP(b.MakeUname("")) }
