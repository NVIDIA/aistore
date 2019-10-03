// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"path/filepath"
	"strings"

	"github.com/jacobsa/fuse/fuseutil"
)

const (
	separator = string(filepath.Separator)
)

func isDirName(name string) bool {
	return strings.HasSuffix(name, separator)
}

func direntNameAndType(name string) (string, fuseutil.DirentType) {
	if isDirName(name) {
		// Separator is removed because directory entries cannot contain
		// a path separator.
		return strings.TrimSuffix(name, separator), fuseutil.DT_Directory
	}
	return name, fuseutil.DT_File
}

// FIXME: Resolve name conficts (e.g. objects a/b and a/b/c --> is b a file or a directory?)
func trimObjectNames(fullNames []string, prefix string) (names []string) {
	var (
		seen = make(map[string]struct{})
	)

	for _, name := range fullNames {
		name = strings.TrimPrefix(name, prefix)

		if split := strings.Index(name, separator); split != -1 {
			name = name[:split+1]
		}

		// fullNames can contain duplicate names
		if _, found := seen[name]; found {
			continue
		}
		seen[name] = struct{}{}

		names = append(names, name)
	}

	return
}
