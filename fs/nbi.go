// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
)

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
		fi, err := os.Stat(fqn)
		if err != nil {
			continue // best effort
		}

		j.mu.Lock()
		e, ok := j.found[name]
		if !ok {
			e = &apc.NBIInfo{
				Name:    name,
				ObjName: filepath.Join(j.prefix, name),
			}
			j.found[name] = e
		}
		e.Size += fi.Size()
		if mt := fi.ModTime().UnixNano(); mt > e.Mtime {
			e.Mtime = mt
		}
		j.mu.Unlock()
	}
}
