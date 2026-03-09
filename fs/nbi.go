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

// (1) NBI xattr v1 (pre-manifest)
// (2) NBI discovery

const (
	nbiMetaVersion = 1
)

type nbiXattr struct {
	prefix   string
	started  int64
	finished int64
	version  uint8
}

type nbiJogger struct {
	mi     *Mountpath
	wg     *sync.WaitGroup
	mu     *sync.Mutex
	found  apc.NBIInfoMap
	errCh  chan<- error
	dir    string
	prefix string // bck.MakeUname("")
}

func CollectNBI(bck *cmn.Bck) (apc.NBIInfoMap, error) {
	var (
		avail  = GetAvail()
		sysBck = meta.SysBckNBI().Bucket()
		prefix = string(bck.MakeUname(""))
		found  = make(apc.NBIInfoMap, 1)
		mu     sync.Mutex
		errCh  = make(chan error, len(avail))
		wg     sync.WaitGroup
	)

	for _, mi := range avail {
		wg.Add(1)
		j := &nbiJogger{
			mi:     mi,
			dir:    mi.makePathCTPrefix(sysBck, ObjCT, prefix),
			found:  found,
			mu:     &mu,
			errCh:  errCh,
			wg:     &wg,
			prefix: prefix,
		}
		go j.run()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, err
	}
	if len(found) == 0 {
		return nil, nil
	}
	return found, nil
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
			Name:    name,
			ObjName: filepath.Join(j.prefix, name),
			Size:    fi.Size(),
		}
		if ok {
			info.Started = meta.started
			info.Finished = meta.finished
			info.Prefix = meta.prefix
		} else {
			info.Finished = _nbiMtime(fqn)
		}

		j.mu.Lock()
		j.found[name] = info
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

//
// nbiXattr: on-disk metadata format (versioned):
// - version (1 byte)
// - started (int64 unix-nano)
// - finished (int64 unix-nano)
// - prefix (length-prefixed string)
//

// interface guard
var (
	_ cos.Packer   = (*nbiXattr)(nil)
	_ cos.Unpacker = (*nbiXattr)(nil)
)

func (x *nbiXattr) PackedSize() int {
	return 1 + cos.SizeofI64 + cos.SizeofI64 + cos.PackedStrLen(x.prefix)
}

func (x *nbiXattr) Pack(packer *cos.BytePack) {
	packer.WriteUint8(x.version)
	packer.WriteInt64(x.started)
	packer.WriteInt64(x.finished)
	packer.WriteString(x.prefix)
}

func (x *nbiXattr) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if x.version, err = unpacker.ReadByte(); err != nil {
		return
	}
	if x.started, err = unpacker.ReadInt64(); err != nil {
		return
	}
	if x.finished, err = unpacker.ReadInt64(); err != nil {
		return
	}
	x.prefix, err = unpacker.ReadString()
	return
}

func getNBI(fqn string) (meta nbiXattr, ok bool, err error) {
	b, err := GetXattr(fqn, xattrNBI)
	if err != nil {
		if cos.IsErrXattrNotFound(err) {
			return meta, false, nil
		}
		return meta, false, err
	}

	u := cos.NewUnpacker(b)
	if err = meta.Unpack(u); err != nil {
		return meta, false, err
	}
	if meta.version != nbiMetaVersion {
		return meta, false, fmt.Errorf("unsupported NBI metadata version %d", meta.version)
	}
	return meta, true, nil
}

func SetNBI(fqn string, started, finished int64, prefix string, buf []byte) error {
	x := &nbiXattr{
		version:  nbiMetaVersion,
		started:  started,
		finished: finished,
		prefix:   prefix,
	}

	packer := cos.NewPacker(buf, x.PackedSize())
	x.Pack(packer)

	return SetXattr(fqn, xattrNBI, packer.Bytes())
}
