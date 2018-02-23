// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"

	"github.com/golang/glog"
)

const (
	MAXATTRSIZE      = 1024
	MAX_COPYBUF_SIZE = 128 * 1024 // 128 KB
)

func assert(cond bool, args ...interface{}) {
	if cond {
		return
	}
	var message = "assertion failed"
	if len(args) > 0 {
		message += ": "
		for i := 0; i < len(args); i++ {
			message += fmt.Sprintf("%#v ", args[i])
		}
	}
	glog.Flush()
	glog.Fatalln(message)
}

func copyStruct(dst interface{}, src interface{}) {
	x := reflect.ValueOf(src)
	if x.Kind() == reflect.Ptr {
		starX := x.Elem()
		y := reflect.New(starX.Type())
		starY := y.Elem()
		starY.Set(starX)
		reflect.ValueOf(dst).Elem().Set(y.Elem())
	} else {
		dst = x.Interface()
	}
}

// FIXME: pick the first random IPv4 that is not loopback
func getipaddr() (ipaddr string, errstr string) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		errstr = fmt.Sprintf("Failed to get host unicast IPs, err: %v", err)
		return
	}
	found := false
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipaddr = ipnet.IP.String()
				found = true
				break
			}
		}
	}
	if !found {
		errstr = "The host does not have any IPv4 addresses"
	}
	return
}

func CreateDir(dirname string) (err error) {
	if _, err := os.Stat(dirname); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dirname, 0755); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return
}

// NOTE: receives, flushes, and closes
func ReceiveFile(file *os.File, rrbody io.Reader, buf []byte, hashes ...hash.Hash) (written int64, errstr string) {
	var (
		writer io.Writer
		err    error
	)
	if len(hashes) == 0 {
		writer = file
	} else {
		hashwriters := make([]io.Writer, len(hashes)+1)
		for i, h := range hashes {
			hashwriters[i] = h.(io.Writer)
		}
		hashwriters[len(hashes)] = file
		writer = io.MultiWriter(hashwriters...)
	}
	if buf == nil {
		written, err = io.Copy(writer, rrbody)
	} else {
		written, err = io.CopyBuffer(writer, rrbody, buf)
	}
	if err != nil {
		return written, err.Error()
	}
	return
}

func Createfile(fname string) (*os.File, error) {

	var file *os.File
	var err error
	// strips the last part from filepath
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		glog.Errorf("Failed to create local dir %s, err: %v", dirname, err)
		return nil, err
	}
	file, err = os.Create(fname)
	if err != nil {
		glog.Errorf("Unable to create file %s, err: %v", fname, err)
		return nil, err
	}

	return file, nil
}

func ComputeFileMD5(file *os.File, buf []byte, md5 hash.Hash) (csum string, errstr string) {
	var err error
	if buf == nil {
		_, err = io.Copy(md5.(io.Writer), file)
	} else {
		_, err = io.CopyBuffer(md5.(io.Writer), file, buf)
	}
	if err != nil {
		return "", fmt.Sprintf("Failed to copy buffer, err: %v", err)
	}
	hashInBytes := md5.Sum(nil)[:16]
	csum = hex.EncodeToString(hashInBytes)
	return csum, ""
}

//===========================================================================
//
// dummy io.Writer & ReadToNull() helper
//
//===========================================================================
func ReadToNull(r io.Reader) (int64, error) {
	return io.Copy(ioutil.Discard, r)
}

//===========================================================================
//
// local (config) save and restore
//
//===========================================================================
func localSave(pathname string, v interface{}) error {
	tmp := pathname + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	r := bytes.NewReader(b)
	_, err = io.Copy(file, r)
	errclose := file.Close()
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if errclose != nil {
		_ = os.Remove(tmp)
		return err
	}
	err = os.Rename(tmp, pathname)
	return err
}

func localLoad(pathname string, v interface{}) (err error) {
	file, err := os.Open(pathname)
	if err != nil {
		return
	}
	err = json.NewDecoder(file).Decode(v)
	_ = file.Close()
	return
}

func osRemove(prefix, fqn string) error {
	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("%s: removed %q", prefix, fqn)
	return nil
}
