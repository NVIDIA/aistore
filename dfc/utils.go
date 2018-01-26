// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"github.com/golang/glog"
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
func getipaddr() (string, error) {
	var ipaddr string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		glog.Errorf("Failed to get host unicast IPs, err: %v", err)
		return ipaddr, err
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipaddr = ipnet.IP.String()
				break
			}
		}
	}
	return ipaddr, err
}

// Check and Set MountPath error count and status.
func checksetmounterror(path string) {
	if getMountPathErrorCount(path) > ctx.config.Cache.ErrorThreshold {
		setMountPathStatus(path, false)
	} else {
		incrMountPathErrorCount(path)
	}

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
func ReceiveFile(fname string, rrbody io.ReadCloser, md5sum string) (written int64, err error) {
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		return 0, err
	}
	file, err := os.Create(fname)
	if err != nil {
		return 0, err
	}
	written, err = copyBuffer(file, rrbody)
	err2 := file.Close()
	if err == nil && err2 != nil {
		err = err2
		return written, err
	}
	// set extended attributes
	err = finalizeobj(fname, []byte(md5sum))
	if err != nil {
		return written, err
	}
	return
}

// copy-paste from the Go io package with a larger buffer on the read side,
// and bufio on the write (FIXME copy-paste)
func copyBuffer(dst io.Writer, src io.Reader) (written int64, err error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.
	if wt, ok := src.(io.WriterTo); ok {
		// fmt.Fprintf(os.Stdout, "use io.WriteTo\n")
		return wt.WriteTo(dst)
	}
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		// fmt.Fprintf(os.Stdout, "use io.ReadFrom\n")
		return rt.ReadFrom(src)
	}
	buf := make([]byte, 1024*128)     // buffer up to 128K for reading (FIXME)
	bufwriter := bufio.NewWriter(dst) // use bufio for writing
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := bufwriter.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	bufwriter.Flush()
	return written, err
}

func Createfile(fname string) (*os.File, error) {

	var file *os.File
	var err error
	// strips the last part from filepath
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		glog.Errorf("Failed to create local dir %s, err: %v", dirname, err)
		checksetmounterror(fname)
		return nil, err
	}
	file, err = os.Create(fname)
	if err != nil {
		glog.Errorf("Unable to create file %s, err: %v", fname, err)
		checksetmounterror(fname)
		return nil, err
	}

	return file, nil
}

// Get specific attribute for specified path.
func Getxattr(path string, attrname string) ([]byte, string) {
	// find size.
	size, err := syscall.Getxattr(path, attrname, nil)
	if err != nil {
		errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return nil, errstr
	}
	if size > 0 {
		data := make([]byte, size)
		read, err := syscall.Getxattr(path, attrname, data)
		if err != nil {
			errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
				path, attrname, err)
			return nil, errstr
		}
		return data[:read], ""
	}
	return []byte{}, ""
}

// Set specific named attribute for specific path.
func Setxattr(path string, attrname string, data []byte) (errstr string) {
	err := syscall.Setxattr(path, attrname, data, 0)
	if err != nil {
		errstr = fmt.Sprintf("Failed to set extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return
	}
	return ""
}

// Delete specific named attribute for specific path.
func Deletexattr(path string, attrname string) (errstr string) {
	err := syscall.Removexattr(path, attrname)
	if err != nil {
		errstr = fmt.Sprintf("Failed to remove extended attr for path %s attr %s, err: %v",
			path, attrname, err)
	}
	return ""
}

// Set DFC's legacy mode to specified mode.
// True will imply no support for extended attributes.
func SetLegacyMode(val bool) {
	glog.Infof("Setting Target's Legacy Mode %v", val)
	ctx.config.LegacyMode = val
}

func CalculateMD5(reader io.Reader) (csum string, errstr string) {
	hash := md5.New()
	_, err := copyBuffer(hash, reader)
	if err != nil {
		s := fmt.Sprintf("Failed to Copy buffer, err: %v", err)
		return "", s
	}
	hashInBytes := hash.Sum(nil)[:16]
	csum = hex.EncodeToString(hashInBytes)
	return csum, ""
}

//===========================================================================
//
// dummy io.Writer & ReadToNull() helper
//
//===========================================================================
type dummywriter struct {
}

func (w *dummywriter) Write(p []byte) (n int, err error) {
	n = len(p)
	return
}

func ReadToNull(r io.Reader) (int64, error) {
	w := &dummywriter{}
	return copyBuffer(w, r)
}

//===========================================================================
//
// local (config) save and restore
//
//===========================================================================
func localSave(pathname string, v interface{}) error {
	file, err := os.Create(pathname)
	if err != nil {
		return err
	}
	defer file.Close()
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	r := bytes.NewReader(b)
	_, err = io.Copy(file, r)
	assert(err == nil, err)
	return err
}

func localLoad(pathname string, v interface{}) error {
	file, err := os.Open(pathname)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(file).Decode(v)
}
