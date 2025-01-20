// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

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
	allPages = ""
)

type (
	lpiPage map[string]struct{}

	lpiMsg struct {
		eop  string
		size int
	}

	LocalPageIt struct {
		page lpiPage
		root string

		// runtime
		current string
		next    string
		msg     lpiMsg
		lr      int
	}
)

var (
	errStop = errors.New("stop")
)

func newLocalPageIt(root string) (*LocalPageIt, error) {
	finfo, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("root fstat: %v", err)
	}
	if !finfo.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", root)
	}

	lpi := &LocalPageIt{root: root}
	{
		lpi.next = lpi.root
		lpi.lr = len(lpi.root)
	}
	debug.Assert(lpi.lr > 1 && !cos.IsLastB(lpi.root, filepath.Separator), lpi.root)

	return lpi, nil
}

func (lpi *LocalPageIt) do(msg lpiMsg, out lpiPage) error {
	{
		clear(out)
		lpi.page = out
		lpi.msg = msg
		lpi.current, lpi.next = lpi.next, ""
	}
	debug.Assert(strings.HasPrefix(lpi.current, lpi.root))

	if lpi.msg.eop != allPages {
		if lpi.current > lpi.msg.eop {
			return fmt.Errorf("expected (end-of-page) %q > %q (current)", lpi.msg.eop, lpi.current)
		}
		if _, err := os.Stat(lpi.msg.eop); err != nil {
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

func (lpi *LocalPageIt) Callback(pathname string, de *godirwalk.Dirent) (err error) {
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
	case pathname >= lpi.current && (pathname <= lpi.msg.eop || lpi.msg.eop == allPages):
		if lpi.msg.size != 0 && len(lpi.page) >= lpi.msg.size {
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
		debug.Assert(pathname > lpi.msg.eop && lpi.msg.eop != allPages)
		lpi.next = pathname
		err = errStop
	}

	return err
}

func (*LocalPageIt) ErrorCallback(pathname string, err error) godirwalk.ErrorAction {
	if err != errStop {
		nlog.Warningf("Error accessing %s: %v", pathname, err)
	}
	return godirwalk.Halt
}
