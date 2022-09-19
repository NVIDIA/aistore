// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	fileToObj struct {
		path string
		name string
		size int64
	}
	// Struct to keep info groupped by file extension.
	// Properties start with capital for reflexion (templates)
	counter struct {
		Cnt  int64
		Size int64
	}
)
type FileToObjSlice []fileToObj

func (a FileToObjSlice) Len() int           { return len(a) }
func (a FileToObjSlice) Less(i, j int) bool { return a[i].path < a[j].path }
func (a FileToObjSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// removes base part from the path making object name from it.
// Extra step - removing leading '/' if base does not end with it
func cutPrefixFromPath(path, base string) string {
	str := strings.TrimPrefix(path, base)
	return strings.TrimPrefix(str, "/")
}

// returns only files inside the 'path' directory that matches mask. No recursion
func filesInDirByMask(path, trimPrefix, appendPrefix, mask string) ([]fileToObj, error) {
	files := make([]fileToObj, 0)
	dentries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, dent := range dentries {
		if dent.IsDir() || !dent.Type().IsRegular() {
			continue
		}
		if matched, err := filepath.Match(mask, filepath.Base(dent.Name())); !matched || err != nil {
			continue
		}
		if finfo, err := dent.Info(); err == nil {
			debug.Assert(finfo.Name() == dent.Name())
			fullPath := filepath.Join(path, dent.Name())
			fo := fileToObj{
				name: appendPrefix + cutPrefixFromPath(fullPath, trimPrefix), // empty strings ignored
				path: fullPath,
				size: finfo.Size(),
			}
			files = append(files, fo)
		}
	}
	return files, nil
}

// traverses the directory 'path' recursively and collects all files
// with names that match mask
func fileTreeByMask(path, trimPrefix, appendPrefix, mask string) ([]fileToObj, error) {
	files := make([]fileToObj, 0)
	walkf := func(fqn string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return nil
			}
			if cmn.IsErrObjNought(err) {
				return nil
			}
			return fmt.Errorf("filepath-walk invoked with err: %v", err)
		}
		if info.IsDir() {
			return nil
		}
		if matched, _ := filepath.Match(mask, filepath.Base(fqn)); !matched {
			return nil
		}

		fo := fileToObj{
			name: appendPrefix + cutPrefixFromPath(fqn, trimPrefix), // empty strings ignored
			path: fqn,
			size: info.Size(),
		}
		files = append(files, fo)
		return nil
	}

	if err := filepath.Walk(path, walkf); err != nil {
		return nil, err
	}

	return files, nil
}

// gets start path with optional wildcard at the end, base to generate
// object names, and recursive flag, returns the list of files that matches
// criteria (file path, object name, size for every file)
func generateFileList(path, trimPrefix, appendPrefix string, recursive bool) ([]fileToObj, error) {
	debug.Assert(trimPrefix == "" || strings.HasPrefix(path, trimPrefix))

	mask := ""
	info, err := os.Stat(path)
	if err != nil {
		// files from a directory by mask.
		// path must end with a mask that contains '*' or '?'
		mask = filepath.Base(path)
		if strings.Contains(mask, "*") || strings.Contains(mask, "?") {
			path = filepath.Dir(path)
			info, err = os.Stat(path)
			if err != nil {
				return nil, fmt.Errorf("path %q does not exist", path)
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("path %q is not a directory", path)
			}
		} else {
			return nil, fmt.Errorf("path %q does not exist", path)
		}
	} else if info.IsDir() {
		// the entire directory. Mask '*' is applied automatically
		mask = "*"
	} else {
		// one file without custom name.
		if trimPrefix == "" {
			// if trim prefix not specified by a caller, default is to just cat everything but base
			trimPrefix = filepath.Dir(path)
		}
		objName := cutPrefixFromPath(path, trimPrefix)

		fo := fileToObj{
			name: appendPrefix + objName,
			path: path,
			size: info.Size(),
		}

		files := []fileToObj{fo}
		return files, nil
	}

	// if trim prefix not specified by a called, cut the whole path from object name and use what's
	// left in 'mask' part of a filename
	if trimPrefix == "" {
		trimPrefix = path
	}

	if !recursive {
		return filesInDirByMask(path, trimPrefix, appendPrefix, mask)
	}

	return fileTreeByMask(path, trimPrefix, appendPrefix, mask)
}

func groupByExt(files []fileToObj) (int64, map[string]counter) {
	totalSize := int64(0)
	extSizes := make(map[string]counter, 10)
	for _, f := range files {
		totalSize += f.size
		ext := filepath.Ext(f.path)
		c := extSizes[ext]
		c.Size += f.size
		c.Cnt++
		extSizes[ext] = c
	}
	return totalSize, extSizes
}
