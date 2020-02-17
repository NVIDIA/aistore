// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
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
func cutBasePath(path, base string) string {
	str := strings.TrimPrefix(path, base)
	return strings.TrimPrefix(str, "/")
}

// returns only files inside the 'path' directory that matches mask. No recursion
func filesInDirByMask(path, base string, mask string) ([]fileToObj, error) {
	files := make([]fileToObj, 0)
	fList, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, f := range fList {
		if f.IsDir() {
			continue
		}
		if matched, _ := filepath.Match(mask, filepath.Base(f.Name())); matched {
			fullPath := filepath.Join(path, f.Name())
			fo := fileToObj{
				name: cutBasePath(fullPath, base),
				path: fullPath,
				size: f.Size(),
			}
			files = append(files, fo)
		}
	}
	return files, nil
}

// traverses the directory 'path' recursively and collects all files
// with names that match mask
func fileTreeByMask(path, base string, mask string) ([]fileToObj, error) {
	files := make([]fileToObj, 0)
	walkf := func(fqn string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return nil
			}
			return cmn.PathWalkErr(err)
		}
		if info.IsDir() {
			return nil
		}
		if matched, _ := filepath.Match(mask, filepath.Base(fqn)); !matched {
			return nil
		}

		fo := fileToObj{
			name: cutBasePath(fqn, base),
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
func generateFileList(path, base string, recursive bool) ([]fileToObj, error) {
	if base != "" && !strings.HasPrefix(path, base) {
		return nil, fmt.Errorf("Path %q must start with the base %q", path, base)
	}

	mask := ""
	info, err := os.Stat(path)
	if err != nil {
		// files from a directory by mask.
		// path must end with a mask that contains '*' or '?'
		if base == "" {
			base = filepath.Dir(path)
		}
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
			return nil, fmt.Errorf("path %q does not exists", path)
		}
	} else if info.IsDir() {
		// the entire directory. Mask '*' is applied automatically
		mask = "*"
		if base == "" {
			base = path
		}
	} else {
		// one file without custom name.
		objName := filepath.Base(path)
		if base != "" {
			objName = cutBasePath(objName, base)
		}
		fo := fileToObj{
			name: objName,
			path: path,
			size: info.Size(),
		}
		files := []fileToObj{fo}
		return files, nil
	}

	if !recursive {
		return filesInDirByMask(path, base, mask)
	}

	return fileTreeByMask(path, base, mask)
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
