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
	file, err = createfile(fqn)
	if err != nil {
		glog.Errorf("Unable to create file %s, err: %v", fqn, err)
		return nil, err
	}

	err = setxattr(fqn, objstateattr, []byte(invalid))
	if err != nil {
		glog.Errorf("Unable to set xattr %s to file %s, err: %v",
			objstateattr, fqn, err)
		file.Close()
		return nil, err
	}
	return file, nil

}

// Finalize object state.
func finalizeobj(fqn string, md5sum []byte) error {
	err := setxattr(fqn, MD5attr, md5sum)
	if err != nil {
		glog.Errorf("Unable to set md5 xattr %s to file %s, err: %v",
			MD5attr, fqn, err)
		return err
	}
	err = setxattr(fqn, objstateattr, []byte(valid))
	if err != nil {
		glog.Errorf("Unable to set valid xattr %s to file %s, err: %v",
			objstateattr, fqn, err)
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
		data, err := getxattr(fqn, objstateattr)
		if err != nil {
			glog.Errorf("Unable to getxttr %s from file %s, err: %v", objstateattr, fqn, err)
			return true
		}
		if string(data) == invalid {
			return true
		}
		return false
	}
}
