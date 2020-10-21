// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

func MarkerExists(marker string) bool {
	availableMpaths, _ := Get()
	for _, mp := range availableMpaths {
		path := filepath.Join(mp.Path, marker)
		if err := Access(path); err == nil {
			return true
		}
	}
	return false
}

func PutMarker(marker string) error {
	cnt, available := PersistOnMpaths(marker, nil, 1)
	if cnt == 0 {
		return fmt.Errorf("failed to persist marker %q (available mountPaths: %d)", marker, available)
	}
	return nil
}

// PersistOnMpaths persists `what` on every mountpath under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// Returns how many times it successfully stored a file.
func PersistOnMpaths(path string, what interface{}, atMost int, opts ...jsp.Options) (cnt, availCnt int) {
	var (
		options            = jsp.CksumSign()
		availableMpaths, _ = Get()
	)

	if atMost == 0 {
		atMost = len(availableMpaths)
	}
	if len(opts) > 0 {
		options = opts[0]
	}

	for _, mountPath := range availableMpaths {
		if err := PersistOnSingleMpath(mountPath, path, what, options); err != nil {
			glog.Errorf("Failed to persist %q on %q, err: %v", path, mountPath, err)
		} else {
			cnt++
		}

		if cnt == atMost {
			break
		}
	}

	return
}

func PersistOnSingleMpath(mpath *MountpathInfo, path string, what interface{}, options jsp.Options) (err error) {
	fpath := filepath.Join(mpath.Path, path)
	if what == nil {
		_, err = cmn.CreateFile(fpath)
	} else {
		err = jsp.Save(fpath, what, options)
	}
	return err
}

func FindPersisted(path, oldSuffix string) (curr, prev MPI) {
	avail, _ := Get()
	curr, prev = make(MPI, len(avail)), make(MPI, len(avail))
	for mpath, mpathInfo := range avail {
		fpath := filepath.Join(mpath, path)
		if err := Access(fpath); err == nil {
			curr[mpath] = mpathInfo
		}
		if oldSuffix == "" {
			continue
		}
		fpath += oldSuffix
		if err := Access(fpath); err == nil {
			prev[mpath] = mpathInfo
		}
	}
	return
}

func RemoveMarker(marker string) error {
	var (
		err     error
		deleted bool

		availableMpaths, _ = Get()
	)
	for _, mp := range availableMpaths {
		path := filepath.Join(mp.Path, marker)
		if errRm := os.Remove(path); errRm != nil && !os.IsNotExist(errRm) {
			glog.Errorf("failed to cleanup %q marker from %s: %v", marker, mp.Path, err)
			err = errRm
		} else if errRm == nil {
			deleted = true
		}
	}
	if err == nil && !deleted {
		err = errors.New("marker not found on any mpath")
	}
	return err
}
