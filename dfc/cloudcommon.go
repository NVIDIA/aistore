// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/golang/glog"
)

const (
	fixedbufsize       = 64 * 1024
	invalidateallcache = "/tmp/failobj"
	NotExists          = "no such file or directory"
)

// Create file and initialize object state.
func initobj(fqn string) (file *os.File, errstr string) {
	var err error
	file, err = Createfile(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Unable to create file %s, err: %v", fqn, err)
		return nil, errstr
	}
	// Don't set xattribute for even new objects to maintain consistency.
	if ctx.config.NoXattrs {
		return
	}
	if errstr = Setxattr(fqn, ObjstateAttr, []byte(XAttrInvalid)); errstr != "" {
		glog.Errorf(errstr)
		// Ignoring errors
		file.Close()
		os.Remove(fqn)
		return nil, errstr
	}
	return file, ""
}

// Finalize object state.
func finalizeobj(fqn string, md5sum []byte) error {
	if ctx.config.NoXattrs {
		return nil
	}
	errstr := Setxattr(fqn, HashAttr, md5sum)
	if errstr != "" {
		glog.Errorf(errstr)
		return errors.New(errstr)
	}
	errstr = Setxattr(fqn, ObjstateAttr, []byte(XAttrValid))
	if errstr != "" {
		glog.Errorf(errstr)
		return errors.New(errstr)
	}
	return nil
}

// Return True for corrupted or invalid objects.
// Return True even for non existent object.
func isinvalidobj(fqn string) bool {
	if ctx.config.NoXattrs {
		return false
	}
	// FIXME: existence of this special file invalidates the entire cache - revisit
	_, err := os.Stat(invalidateallcache)
	if err == nil {
		return true
	}
	data, errstr := Getxattr(fqn, ObjstateAttr)
	if errstr != "" {
		if strings.Contains(errstr, NotExists) {
			return true
		} else {
			glog.Errorf("%s", errstr)
			return true
		}
	}
	if string(data) == XAttrInvalid {
		return true
	}
	return false
}
