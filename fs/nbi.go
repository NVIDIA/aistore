// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

type (
	nbiFound struct {
		err     error
		mi      *Mountpath
		invName string
		fqn     string
	}
	nbiJogger struct {
		mi  *Mountpath
		wg  *sync.WaitGroup
		ch  chan<- nbiFound
		dir string
	}
)

func findNBI(bck *cmn.Bck) (map[string]string, error) {
	var (
		avail  = GetAvail()
		sysBck = meta.SysBckNBI().Bucket()
		prefix = string(bck.MakeUname(""))
		ch     = make(chan nbiFound, len(avail))
		wg     sync.WaitGroup
	)

	for _, mi := range avail {
		wg.Add(1)
		j := &nbiJogger{
			mi:  mi,
			dir: mi.makePathCTPrefix(sysBck, ObjCT, prefix),
			ch:  ch,
			wg:  &wg,
		}
		go j.run()
	}

	wg.Wait()
	close(ch)

	found := make(map[string]string, 1)
	for res := range ch {
		if res.err != nil {
			return nil, res.err
		}
		if _, ok := found[res.invName]; !ok {
			found[res.invName] = res.fqn
		}
	}
	return found, nil
}

func (j *nbiJogger) run() {
	defer j.wg.Done()

	nlog.Errorln(">>>>> j.dir", j.dir) // DEBUG

	ents, err := os.ReadDir(j.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		j.ch <- nbiFound{err: err}
		return
	}

	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		j.ch <- nbiFound{
			invName: name,
			fqn:     filepath.Join(j.dir, name),
			mi:      j.mi,
		}
	}
}
