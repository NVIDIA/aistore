// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const maxNumCopies = 16

func IsDirEmpty(dir string) (names []string, empty bool, err error) {
	var f *os.File
	f, err = os.Open(dir)
	if err != nil {
		return nil, false, err
	}
	defer cos.Close(f)

	// Try listing small number of files/dirs to do a quick emptiness check.
	// If seems empty try a bigger sample to determine if it actually is.
	for _, limit := range []int{10, 100, 1000, -1} {
		names, err = f.Readdirnames(limit)
		if err == io.EOF {
			return nil, true, nil
		}
		if err != nil || len(names) == 0 {
			return nil, true, err
		}
		// Firstly, check if there is any file at this level.
		dirs := names[:0]
		for _, sub := range names {
			subDir := filepath.Join(dir, sub)
			if finfo, erc := os.Stat(subDir); erc == nil {
				if !finfo.IsDir() {
					return names[:min(8, len(names))], false, nil
				}
				dirs = append(dirs, subDir)
			}
		}
		// If not, then try to recurse into each directory.
		for _, subDir := range dirs {
			if nestedNames, empty, err := IsDirEmpty(subDir); err != nil {
				return nil, false, err
			} else if !empty {
				return nestedNames, false, nil
			}
		}
		// If we've just listed all the files/dirs then exit.
		if len(names) < limit {
			break
		}
	}
	return nil, true, nil
}

func ValidateNCopies(tname string, copies int) (err error) {
	if copies < 1 || copies > maxNumCopies {
		return fmt.Errorf("%s: invalid num copies %d, must be in [1, %d] range",
			tname, copies, maxNumCopies)
	}
	avail := GetAvail()
	if num := len(avail); num < copies {
		return fmt.Errorf("%s: number of copies (%d) exceeds the number of mountpaths (%d)",
			tname, copies, num)
	}
	return nil
}
