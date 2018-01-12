/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"

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

func clearStruct(x interface{}) {
	p := reflect.ValueOf(x).Elem()
	p.Set(reflect.Zero(p.Type()))
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

func ReceiveFile(fname string, r *http.Response) (written int64, err error) {
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		return 0, err
	}
	fd, err := os.Create(fname)
	if err != nil {
		return 0, err
	}
	written, err = copyBuffer(fd, r.Body)
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
	buf := make([]byte, 1024*1024)    // buffer up to 1MB for reading (FIXME)
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
