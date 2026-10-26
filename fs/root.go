// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// access beneath os.Root with no symlinks in any path component
// - usage: promote (root = apc.PromoteRoot)
// - os.Root confines every access; in addition, reject symlinks (documented promote contract)
// - resolve one component at a time (linear in path depth)

// stat `fqn` beneath root
func StatNoSymlinks(root *os.Root, fqn string) (os.FileInfo, error) {
	parent, base, err := resolveParent(root, fqn)
	if err != nil {
		return nil, err
	}
	defer closeSub(parent, root)
	return lstatBase(parent, base, fqn)
}

// open regular file `fqn` beneath root
func OpenNoSymlinks(root *os.Root, fqn string) (*os.File, error) {
	parent, base, err := resolveParent(root, fqn)
	if err != nil {
		return nil, err
	}
	defer closeSub(parent, root)
	fi, err := lstatBase(parent, base, fqn)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, apc.NewErrInvalidPromoteSource(fqn, "not a regular file")
	}
	fh, err := parent.Open(base)
	if err != nil {
		return nil, err
	}
	// recheck via descriptor
	if fi, err = fh.Stat(); err == nil && !fi.Mode().IsRegular() {
		err = apc.NewErrInvalidPromoteSource(fqn, "not a regular file")
	}
	if err != nil {
		fh.Close()
		return nil, err
	}
	return fh, nil
}

// remove `fqn` beneath root (unlink the last component without following it)
func RemoveNoSymlinks(root *os.Root, fqn string) error {
	parent, base, err := resolveParent(root, fqn)
	if err != nil {
		return err
	}
	defer closeSub(parent, root)
	return parent.Remove(base)
}

// walk regular files under `dir` beneath root; skip symlinks
func WalkRoot(root *os.Root, dir string, recursive bool, cb func(string, DirEntry) error) error {
	parent, base, err := resolveParent(root, dir)
	if err != nil {
		return err
	}
	defer closeSub(parent, root)
	if _, err := lstatBase(parent, base, dir); err != nil {
		return err
	}
	sub, err := parent.OpenRoot(base)
	if err != nil {
		return err
	}
	defer sub.Close()
	return iofs.WalkDir(sub.FS(), ".", func(name string, de iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			if name != "." && !recursive {
				return iofs.SkipDir
			}
			return nil
		}
		if !de.Type().IsRegular() {
			return nil
		}
		return cb(filepath.Join(dir, filepath.FromSlash(name)), de)
	})
}

// resolve all but the last component of `fqn`, one directory at a time;
// return the parent (caller closes via closeSub) and the base name
func resolveParent(root *os.Root, fqn string) (parent *os.Root, base string, _ error) {
	rel, err := filepath.Rel(root.Name(), fqn)
	if err != nil {
		return nil, "", err
	}
	if !filepath.IsLocal(rel) {
		return nil, "", apc.NewErrInvalidPromoteSource(fqn, "outside "+root.Name())
	}
	dir, base := filepath.Split(rel)
	parent = root
	if dir == "" {
		return parent, base, nil
	}
	for part := range strings.SplitSeq(strings.TrimSuffix(dir, cos.PathSeparator), cos.PathSeparator) {
		if _, err := lstatBase(parent, part, fqn); err != nil {
			closeSub(parent, root)
			return nil, "", err
		}
		sub, err := parent.OpenRoot(part)
		closeSub(parent, root)
		if err != nil {
			return nil, "", err
		}
		parent = sub
	}
	return parent, base, nil
}

func lstatBase(parent *os.Root, base, fqn string) (os.FileInfo, error) {
	fi, err := parent.Lstat(base)
	if err != nil {
		return nil, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil, apc.NewErrInvalidPromoteSource(fqn, "contains a symbolic link")
	}
	return fi, nil
}

func closeSub(r, root *os.Root) {
	if r != root {
		r.Close()
	}
}
