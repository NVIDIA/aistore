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
	"hash"
	"io"
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
func ReceiveFile(file *os.File, rrbody io.Reader, hashes ...hash.Hash) (written int64, errstr string) {
	var writer io.Writer
	assert(file != nil)
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
	written, err := copyBuffer(writer, rrbody)
	if err != nil {
		return written, err.Error()
	}
	return
}

// on err closes and removes the file; othwerise returns the size (in bytes).
// omd5 can be empty for multipart object or in context of put.
func ReceiveFileAndFinalize(fqn, objname, omd5 string, reader io.ReadCloser) (written int64, md5hash string, errstr string) {
	var (
		err  error
		file *os.File
	)
	if file, errstr = initobj(fqn); errstr != "" {
		errstr = fmt.Sprintf("Failed to create and initialize %s, err: %s", fqn, errstr)
		return
	}
	defer func() {
		if errstr == "" {
			return
		}
		if err = file.Close(); err != nil {
			glog.Errorf("Failed to close file %s, err: %v", fqn, err)
		}
		if err = os.Remove(fqn); err != nil {
			glog.Errorf("Nested error %s => (remove %s => err: %v", errstr, fqn, err)
		}
	}()

	md5 := md5.New()
	if written, errstr = ReceiveFile(file, reader, md5); errstr != "" {
		return
	}
	if md5hash, errstr = CalculateMD5(nil, md5); errstr != "" {
		return
	}
	if omd5 != md5hash {
		errstr = fmt.Sprintf("Object's %s MD5 %s... does not match %s (MD5 %s...)",
			objname, omd5[:8], fqn, md5hash[:8])
		return
	}
	if err = finalizeobj(fqn, []byte(md5hash)); err != nil {
		errstr = fmt.Sprintf("Unable to finalize file %s, err: %v", fqn, err)
		return
	}
	// close file
	if err = file.Close(); err != nil {
		errstr = fmt.Sprintf("Failed to close file %s, err: %v", fqn, err)
	}
	return
}

// copy-paste from the Go io package with a larger buffer on the read side,
// and bufio on the write (FIXME copy-paste)
func copyBuffer(dst io.Writer, src io.Reader) (written int64, err error) {
	var buf []byte
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
	buf = make([]byte, MAX_COPYBUF_SIZE) // buffer up to 128K for reading (FIXME)
	bufwriter := bufio.NewWriter(dst)    // use bufio for writing
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
		return nil, err
	}
	file, err = os.Create(fname)
	if err != nil {
		glog.Errorf("Unable to create file %s, err: %v", fname, err)
		return nil, err
	}

	return file, nil
}

// Set DFC's legacy mode to specified mode.
// True will imply no support for extended attributes.
func SetNoXattrs(val bool) {
	glog.Infof("Setting Target's Legacy Mode %v", val)
	ctx.config.NoXattrs = val
}

func CalculateMD5(reader io.Reader, md5 hash.Hash) (csum string, errstr string) {
	if reader != nil {
		_, err := copyBuffer(md5.(io.Writer), reader)
		if err != nil {
			s := fmt.Sprintf("Failed to Copy buffer, err: %v", err)
			return "", s
		}
	}
	assert(md5 != nil)
	hashInBytes := md5.Sum(nil)[:16]
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
	tmp := pathname + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		file.Close()
		os.Remove(tmp)
		return err
	}
	r := bytes.NewReader(b)
	_, err = io.Copy(file, r)
	errclose := file.Close()
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if errclose != nil {
		os.Remove(tmp)
		return err
	}
	err = os.Rename(tmp, pathname)
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

func osremove(prefix, fqn string) error {
	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("%s: removed %q", prefix, fqn)
	return nil
}
