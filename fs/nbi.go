// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

// In this source:
//  (1) NBI xattr v1 (pre-manifest)
//  (2) NBI discovery

//
// nbiXattr: native bucket inventory (NBI) on-disk metadata
//
// Versioned binary layout (big-endian via cos.BytePack):
//
// | -- FIXED-SIZE FIELDS ----------------------------------------------- | -- VAR -- |
// | ver | started | finished | ntotal | smapVer | chunks | nat | prefix |
// | u8  |  int64  |  int64   |  int64 |  int64  |  int32 | int32 | string |
//
// where:
//   - ver      : metadata version (nbiMetaVersion)
//   - started  : creation start time (UnixNano)
//   - finished : creation completion time (UnixNano)
//   - ntotal   : total number of object names in the inventory
//   - smapVer  : cluster Smap version at creation time
//   - chunks   : total number of inventory chunks (manifest.Count())
//   - nat      : number of active targets at creation time
//   - prefix   : lsmsg.Prefix used to generate the inventory
//
// Notes:
//   - `prefix` is variable-length stored last.
//   - All fixed-size fields must be appended only; do not reorder existing fields.
//   - Any post-4.3 format change must bump `nbiMetaVersion` and preserve backward compatibility.
//   - Consumers must validate `ver` meta-version before interpreting the payload.
//

const (
	nbiMetaVersion = 1
)

type nbiXattr struct {
	apc.NBIMeta
	version uint8 // set internally; not exported
}

type nbiJogger struct {
	mi     *Mountpath
	wg     *sync.WaitGroup
	mu     *sync.Mutex
	errCh  chan<- error
	out    apc.NBIInfoMap // result: shared map
	prefix string         // bck.MakeUname("")
	dir    string         // mi.makePath
	cname  string         // bck.Cname("")
}

func CollectNBI(bck *cmn.Bck) (apc.NBIInfoMap, error) {
	var (
		avail  = GetAvail()
		sysBck = meta.SysBckNBI().Bucket()
		prefix = string(bck.MakeUname(""))
		cname  = bck.Cname("")
		out    = make(apc.NBIInfoMap, 1)
		mu     sync.Mutex
		errCh  = make(chan error, len(avail))
		wg     sync.WaitGroup
	)

	for _, mi := range avail {
		wg.Add(1)
		j := &nbiJogger{
			mi:     mi,
			out:    out,
			mu:     &mu,
			errCh:  errCh,
			wg:     &wg,
			prefix: prefix,
			dir:    mi.makePathCTPrefix(sysBck, ObjCT, prefix),
			cname:  cname,
		}
		go j.run()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (j *nbiJogger) run() {
	defer j.wg.Done()

	ents, err := os.ReadDir(j.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		mfs.hc.FSHC(err, j.mi, "")
		j.errCh <- err
		return
	}

	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}

		name := ent.Name()
		fqn := filepath.Join(j.dir, name)

		meta, ok, err := getNBI(fqn)
		if err != nil {
			continue
		}

		fi, err := ent.Info()
		if err != nil {
			continue
		}

		info := &apc.NBIInfo{
			Bucket:  j.cname,
			Name:    name,
			ObjName: filepath.Join(j.prefix, name),
			Size:    fi.Size(),
		}

		if ok {
			info.NBIMeta = meta.NBIMeta
		} else {
			// TODO: indicate "likely in progress"
			info.Finished = _nbiMtime(fqn)
		}

		j.mu.Lock()
		j.out[info.ObjName] = info
		j.mu.Unlock()
	}
}

func _nbiMtime(fqn string) int64 {
	t, err := MtimeUTC(fqn)
	if err != nil {
		debug.Assertf(false, "%q: %v", fqn, err)
		return 0
	}
	return t.UnixNano()
}

// interface guard
var (
	_ cos.Packer   = (*nbiXattr)(nil)
	_ cos.Unpacker = (*nbiXattr)(nil)
)

func (x *nbiXattr) PackedSize() int {
	return 1 + 4*cos.SizeofI64 + 2*cos.SizeofI32 + cos.PackedStrLen(x.Prefix)
}

func (x *nbiXattr) Pack(packer *cos.BytePack) {
	packer.WriteUint8(x.version)
	packer.WriteInt64(x.Started)
	packer.WriteInt64(x.Finished)
	packer.WriteInt64(x.Ntotal)
	packer.WriteInt64(x.SmapVer)
	packer.WriteInt32(x.Chunks)
	packer.WriteInt32(x.Nat)
	packer.WriteString(x.Prefix)
}

func (x *nbiXattr) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if x.version, err = unpacker.ReadByte(); err != nil {
		return
	}

	if x.Started, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if x.Finished, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if x.Ntotal, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if x.SmapVer, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if x.Chunks, err = unpacker.ReadInt32(); err != nil {
		return
	}
	if x.Nat, err = unpacker.ReadInt32(); err != nil {
		return
	}

	x.Prefix, err = unpacker.ReadString()
	return
}

func getNBI(fqn string) (x *nbiXattr, ok bool, err error) {
	b, err := GetXattr(fqn, xattrNBI)
	if err != nil {
		if cos.IsErrXattrNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	x = &nbiXattr{}
	u := cos.NewUnpacker(b)
	if err = x.Unpack(u); err != nil {
		return nil, false, err
	}
	if x.version != nbiMetaVersion {
		return nil, false, fmt.Errorf("unsupported NBI metadata version %d", x.version)
	}
	return x, true, nil
}

func SetNBI(fqn string, meta *apc.NBIMeta, buf []byte) error {
	x := nbiXattr{
		NBIMeta: *meta,
		version: nbiMetaVersion,
	}
	packer := cos.NewPacker(buf, x.PackedSize())
	x.Pack(packer)

	return SetXattr(fqn, xattrNBI, packer.Bytes())
}
