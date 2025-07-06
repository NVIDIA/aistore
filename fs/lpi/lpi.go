// Package lpi: local page iterator
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package lpi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/karrick/godirwalk"
)

const (
	// to fill `Page` with the entire remaining content,
	// with no limits that may otherwise be imposed by
	// page size (`Msg.Size`) and/or end-of-page (`Msg.EOP`)
	AllPages = ""
)

type (
	Page map[string]int64 // [name => size] TODO: [name => lom] for props

	Msg struct {
		EOP  string // until end-of-page marker
		Size int    // so-many entries
	}

	// local page iterator (LPI)
	// NOTE: it is caller's responsibility to serialize access _or_ take locks.
	Iter struct {
		page     Page
		smap     *meta.Smap
		root     string
		prefix   string
		rootpref string

		// runtime
		current string
		next    string
		msg     Msg
		lr      int
		lp      int
	}
)

var (
	errStop = errors.New("stop")
)

func New(root, prefix string, smap *meta.Smap) (*Iter, error) {
	// validate root
	debug.Assert(!cos.IsLastB(root, filepath.Separator), root)
	finfo, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("root fstat: %w", err)
	}
	if !finfo.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", root)
	}

	// construct
	lpi := &Iter{root: root, prefix: prefix, smap: smap}
	{
		lpi.next = root
		lpi.lr = len(root)
		if lpi.lp = len(prefix); lpi.lp > 0 {
			lpi.rootpref = filepath.Join(root, prefix)
		}
	}
	debug.Assert(lpi.lr > 1, lpi.root)
	return lpi, nil
}

func (lpi *Iter) Pos() string { return lpi.next }
func (lpi *Iter) Clear()      { clear(lpi.page) }

func (lpi *Iter) Next(msg Msg, out Page) error {
	{
		clear(out)
		lpi.page = out
		lpi.msg = msg
		lpi.current, lpi.next = lpi.next, ""
	}
	debug.Assert(strings.HasPrefix(lpi.current, lpi.root), lpi.current, " vs ", lpi.root)

	if lpi.msg.EOP != AllPages {
		if lpi.current > lpi.msg.EOP {
			return fmt.Errorf("expected (end-of-page) %q > %q (current)", lpi.msg.EOP, lpi.current)
		}
	}

	// next page
	err := godirwalk.Walk(lpi.root, &godirwalk.Options{
		Unsorted:      false,
		Callback:      lpi.Callback,
		ErrorCallback: lpi.ErrorCallback,
	})
	if err != nil && err != errStop {
		return fmt.Errorf("error gowalk-ing: %w", err)
	}

	return nil
}

func (lpi *Iter) Callback(pathname string, de *godirwalk.Dirent) (err error) {
	switch {
	case de.IsDir():
		// skip or SkipDir
		debug.Assert(!cos.IsLastB(pathname, filepath.Separator), pathname)
		if pathname == lpi.root {
			break
		}
		l := len(pathname)
		// expecting root's sub
		if l < lpi.lr+1 || pathname[:lpi.lr] != lpi.root || pathname[lpi.lr] != filepath.Separator {
			debug.Assert(false, lpi.root, " vs ", pathname)
			return filepath.SkipDir
		}
		// fast
		if l < len(lpi.current) && pathname < lpi.current[:l] {
			return filepath.SkipDir
		}
		if lpi.rootpref != "" && !cmn.DirHasOrIsPrefix(pathname, lpi.rootpref) {
			return filepath.SkipDir
		}
	case pathname < lpi.current:
		// skip
	case pathname >= lpi.current && (pathname <= lpi.msg.EOP || lpi.msg.EOP == AllPages):
		// page size limit
		if lpi.msg.Size != 0 && len(lpi.page) >= lpi.msg.Size {
			lpi.next = pathname
			err = errStop
			break
		}

		rel := pathname[lpi.lr+1:]
		debug.AssertFunc(func() bool { _, ok := lpi.page[rel]; return !ok })

		// - must contain prefix
		// - skip or SkipDir
		// - refine; see also `ObjHasPrefix`
		if lpi.lp > 0 && len(rel) >= lpi.lp {
			if s := rel[0:lpi.lp]; s != lpi.prefix {
				if s > lpi.prefix {
					return filepath.SkipDir
				}
				break
			}
		}

		// NOTE: unit test only
		if lpi.smap == nil {
			if finfo, e := os.Stat(pathname); e == nil {
				lpi.page[rel] = finfo.Size()
			}
			break
		}

		lom := core.AllocLOM("")
		err = lpi._cb(pathname, rel, lom)
		core.FreeLOM(lom)
	default:
		// next
		debug.Assert(pathname > lpi.msg.EOP && lpi.msg.EOP != AllPages)
		lpi.next = pathname
		err = errStop
	}

	return err
}

// (compare with walkInfo._cb)
func (lpi *Iter) _cb(fqn, rel string, lom *core.LOM) (err error) {
	var local bool
	if err = lom.PreInit(fqn); err != nil {
		goto rerr
	}
	debug.Assert(lom.ObjName == rel, lom.ObjName, " vs ", rel)
	if lpi.prefix != "" && !cmn.ObjHasPrefix(lom.ObjName, lpi.prefix) {
		return nil
	}
	if err = lom.PostInit(); err != nil {
		goto rerr
	}
	if _, local, err = lom.HrwTarget(lpi.smap); err != nil {
		return errStop
	}
	if !local || !lom.IsHRW() {
		// skip both
		return nil
	}
	if err = lom.Load(true, false); err != nil {
		goto rerr
	}

	// add
	lpi.page[rel] = lom.Lsize()
	return nil

rerr:
	if cmn.Rom.FastV(4, cos.SmoduleFS) {
		nlog.Warningln(lom.String(), "[", err, "]")
	}
	return nil
}

func (*Iter) ErrorCallback(pathname string, err error) godirwalk.ErrorAction {
	if err != errStop {
		nlog.Warningln("Error accessing", pathname, err)
	}
	return godirwalk.Halt
}
