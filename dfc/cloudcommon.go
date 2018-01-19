/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"

	"github.com/golang/glog"
)

const (
	emulateobjfailure = "/tmp/failobj"
)

// Create file and initialize object state.
func initobj(fqn string) (file *os.File, err error) {
	file, err = Createfile(fqn)
	if err != nil {
		glog.Errorf("Unable to create file %s, err: %v", fqn, err)
		return nil, err
	}

	err = Setxattr(fqn, Objstateattr, []byte(XAttrInvalid))
	if err != nil {
		glog.Errorf("Unable to set xattr %s to file %s, err: %v",
			Objstateattr, fqn, err)
		file.Close()
		return nil, err
	}
	return file, nil

}

// Finalize object state.
func finalizeobj(fqn string, md5sum []byte) error {
	err := Setxattr(fqn, MD5attr, md5sum)
	if err != nil {
		glog.Errorf("Unable to set md5 xattr %s to file %s, err: %v",
			MD5attr, fqn, err)
		return err
	}
	err = Setxattr(fqn, Objstateattr, []byte(XAttrValid))
	if err != nil {
		glog.Errorf("Unable to set valid xattr %s to file %s, err: %v",
			Objstateattr, fqn, err)
		return err
	}
	return nil
}

// Return True for corrupted or invalid objects.
func isinvalidobj(fqn string) bool {
	// Existence of file will make all cached object(s) invalid.
	_, err := os.Stat(emulateobjfailure)
	if err == nil {
		return true
	} else {
		data, err := Getxattr(fqn, Objstateattr)
		if err != nil {
			glog.Errorf("Unable to getxttr %s from file %s, err: %v", Objstateattr, fqn, err)
			return true
		}
		if string(data) == XAttrInvalid {
			return true
		}
		return false
	}
}
