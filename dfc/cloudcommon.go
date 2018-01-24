/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/golang/glog"
)

const fixedbufsize = 64 * 1024
const (
	emulateobjfailure = "/tmp/failobj"
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
	if ctx.config.LegacyMode {
		return file, ""
	} else {
		errstr = Setxattr(fqn, Objstateattr, []byte(XAttrInvalid))
		if errstr != "" {
			glog.Errorf(errstr)
			file.Close()
			return nil, errstr
		}
		return file, ""
	}
	if glog.V(3) {
		glog.Infof("Created and initialized file %s", fqn)
	}
	return file, ""
}

// Finalize object state.
func finalizeobj(fqn string, md5sum []byte) error {
	if ctx.config.LegacyMode {
		return nil
	} else {
		errstr := Setxattr(fqn, MD5attr, md5sum)
		if errstr != "" {
			glog.Errorf(errstr)
			return errors.New(errstr)
		}
		errstr = Setxattr(fqn, Objstateattr, []byte(XAttrValid))
		if errstr != "" {
			glog.Errorf(errstr)
			return errors.New(errstr)
		}
		return nil
	}
}

// Return True for corrupted or invalid objects.
func isinvalidobj(fqn string) bool {
	if ctx.config.LegacyMode {
		return false
	} else {
		// Existence of file will make all cached object(s) invalid.
		_, err := os.Stat(emulateobjfailure)
		if err == nil {
			return true
		} else {
			data, errstr := Getxattr(fqn, Objstateattr)
			if errstr != "" {
				glog.Errorf(errstr)
				return true
			}
			if string(data) == XAttrInvalid {
				return true
			}
			return false
		}
	}
}

// on err closes and removes the file; othwerise returns the size (in bytes) while keeping the file open
func getobjto_Md5(file *os.File, fqn, objname, omd5 string, reader io.Reader) (size int64, errstr string) {
	hash := md5.New()
	writer := io.MultiWriter(file, hash)

	// was: size, err := io.Copy(writer, reader)
	size, err := copyBuffer(writer, reader)
	if err != nil {
		file.Close()
		return 0, fmt.Sprintf("Failed to download object %s as file %q, err: %v", objname, fqn, err)
	}
	hashInBytes := hash.Sum(nil)[:16]
	fmd5 := hex.EncodeToString(hashInBytes)
	if omd5 != fmd5 {
		file.Close()
		// and discard right away
		if err = os.Remove(fqn); err != nil {
			glog.Errorf("Failed to delete file %s, err: %v", fqn, err)
		}
		return 0, fmt.Sprintf("Object's %s MD5 %s... does not match %s (MD5 %s...)", objname, omd5[:8], fqn, fmd5[:8])
	} else if glog.V(3) {
		glog.Infof("Downloaded and validated %s as %s", objname, fqn)
	}
	if err = finalizeobj(fqn, hashInBytes); err != nil {
		file.Close()
		// FIXME: more logic TBD to maybe not discard
		if err = os.Remove(fqn); err != nil {
			glog.Errorf("Failed to delete file %s, err: %v", fqn, err)
		}
		return 0, fmt.Sprintf("Unable to finalize file %s, err: %v", fqn, err)
	}
	return size, ""
}

func maketeerw(r *http.Request) (io.Reader, *bytes.Buffer) {
	var bufsize int64
	if r.ContentLength > 0 {
		bufsize = fixedbufsize
	} else {
		bufsize = 0
	}
	buf := bytes.NewBuffer(make([]byte, bufsize))
	rw := io.TeeReader(r.Body, buf)
	return rw, buf
}

func truncatefile(fqn string, size int64) (errstr string) {
	glog.Infof("Setting file %s size to %v", fqn, size)
	err := os.Truncate(fqn, size)
	if err != nil {
		errstr = fmt.Sprintf("Failed to truncate file %s, err: %v", fqn, err)
		// Remove corrupted object.
		err1 := os.Remove(fqn)
		if err1 != nil {
			glog.Errorf("Failed to remove file %s, err: %v", fqn, err1)
		}
		return errstr
	}
	return ""
}
