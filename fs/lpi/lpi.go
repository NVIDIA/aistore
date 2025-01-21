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

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/karrick/godirwalk"
)

const (
	// to fill `Page` with the entire remaining content,
	// with no limits that may otherwise be imposed by
	// page size (`Msg.Size`) and/or end-of-page (`Msg.EOP`)
	AllPages = ""
)

type (
	Page map[string]struct{}

	Msg struct {
		EOP  string // until end-of-page marker
		Size int    // so-many entries
	}

	// local page iterator (LPI)
	// NOTE: it is caller's responsibility to serialize access _or_ take locks.
	Iter struct {
		page Page
		root string

		// runtime
		current string
		next    string
		msg     Msg
		lr      int
	}
)

var (
	errStop = errors.New("stop")
)

func New(root string) (*Iter, error) {
	finfo, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("root fstat: %v", err)
	}
	if !finfo.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", root)
	}

	lpi := &Iter{root: root}
	{
		lpi.next = lpi.root
		lpi.lr = len(lpi.root)
	}
	debug.Assert(lpi.lr > 1 && !cos.IsLastB(lpi.root, filepath.Separator), lpi.root)

	return lpi, nil
}

func (lpi *Iter) Pos() string { return lpi.next }

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
		if _, err := os.Stat(lpi.msg.EOP); err != nil {
			return fmt.Errorf("end-of-page fstat: %v", err)
		}
	}

	// do page
	err := godirwalk.Walk(lpi.root, &godirwalk.Options{
		Unsorted:      false,
		Callback:      lpi.Callback,
		ErrorCallback: lpi.ErrorCallback,
	})
	if err != nil && err != errStop {
		return fmt.Errorf("error during walk: %v", err)
	}

	return nil
}

func (lpi *Iter) Callback(pathname string, de *godirwalk.Dirent) (err error) {
	switch {
	case de.IsDir():
		// skip or SkipDir
		debug.Assert(!cos.IsLastB(pathname, filepath.Separator), pathname)
		if pathname != lpi.root {
			// TODO: assert instead
			if len(pathname) < lpi.lr+1 {
				return filepath.SkipDir
			}
			if pathname[:lpi.lr] != lpi.root || pathname[lpi.lr] != filepath.Separator {
				return filepath.SkipDir
			}

			// fast
			if l := len(pathname); l < len(lpi.current) && pathname < lpi.current[:l] {
				return filepath.SkipDir
			}
		}
	case pathname < lpi.current:
		// skip
	case pathname >= lpi.current && (pathname <= lpi.msg.EOP || lpi.msg.EOP == AllPages):
		if lpi.msg.Size != 0 && len(lpi.page) >= lpi.msg.Size {
			lpi.next = pathname
			err = errStop
			break
		}
		// add
		rel := pathname[lpi.lr+1:]

		debug.AssertFunc(func() bool {
			_, ok := lpi.page[rel]
			return !ok
		})
		lpi.page[rel] = struct{}{}
	default:
		// next
		debug.Assert(pathname > lpi.msg.EOP && lpi.msg.EOP != AllPages)
		lpi.next = pathname
		err = errStop
	}

	return err
}

func (*Iter) ErrorCallback(pathname string, err error) godirwalk.ErrorAction {
	if err != errStop {
		nlog.Warningf("Error accessing %s: %v", pathname, err)
	}
	return godirwalk.Halt
}
