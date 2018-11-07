/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package cmn provides common low-level types and utilities for all dfcpub projects
package cmn

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

var toBiBytes = map[string]int64{
	"K":   KiB,
	"KB":  KiB,
	"KIB": KiB,
	"M":   MiB,
	"MB":  MiB,
	"MIB": MiB,
	"G":   GiB,
	"GB":  GiB,
	"GIB": GiB,
	"T":   TiB,
	"TB":  TiB,
	"TIB": TiB,
}

type (
	StringSet map[string]struct{}
	SimpleKVs map[string]string
)

func S2B(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	s = strings.ToUpper(s)
	for k, v := range toBiBytes {
		if ns := strings.TrimSuffix(s, k); ns != s {
			i, err := strconv.ParseInt(strings.TrimSpace(ns), 10, 64)
			return v * i, err
		}
	}
	ns := strings.TrimSuffix(s, "B")
	i, err := strconv.ParseInt(strings.TrimSpace(ns), 10, 64)
	return i, err
}

func B2S(b int64, digits int) string {
	if b >= TiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(TiB), "TiB")
	}
	if b >= GiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(GiB), "GiB")
	}
	if b >= MiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(MiB), "MiB")
	}
	if b >= KiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(KiB), "KiB")
	}
	return fmt.Sprintf("%dB", b)
}

func (ss StringSet) String() string {
	keys := make([]string, len(ss))
	idx := 0
	for key := range ss {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func Assert(cond bool, args ...interface{}) {
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
	panic(message)
}

func StringInSlice(s string, arr []string) bool {
	for _, el := range arr {
		if el == s {
			return true
		}
	}
	return false
}

func CopyStruct(dst interface{}, src interface{}) {
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

//
// files, IO, hash
//
func CreateDir(dirname string) error {
	stat, err := os.Stat(dirname)
	if err == nil && stat.IsDir() {
		return nil
	}
	err = os.MkdirAll(dirname, 0755)
	return err
}

func CreateFile(fname string) (file *os.File, err error) {
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		return
	}
	file, err = os.Create(fname)
	return
}

func ReceiveAndChecksum(filewriter io.Writer, rrbody io.Reader,
	buf []byte, hashes ...hash.Hash) (written int64, err error) {
	var writer io.Writer
	if len(hashes) == 0 {
		writer = filewriter
	} else {
		hashwriters := make([]io.Writer, len(hashes)+1)
		for i, h := range hashes {
			hashwriters[i] = h.(io.Writer)
		}
		hashwriters[len(hashes)] = filewriter
		writer = io.MultiWriter(hashwriters...)
	}
	if buf == nil {
		written, err = io.Copy(writer, rrbody)
	} else {
		written, err = io.CopyBuffer(writer, rrbody, buf)
	}
	return
}

func ComputeXXHash(reader io.Reader, buf []byte) (csum string, errstr string) {
	var err error
	var xx hash.Hash64 = xxhash.New64()
	if buf == nil {
		_, err = io.Copy(xx.(io.Writer), reader)
	} else {
		_, err = io.CopyBuffer(xx.(io.Writer), reader, buf)
	}
	if err != nil {
		return "", fmt.Sprintf("Failed to copy buffer, err: %v", err)
	}
	hashIn64 := xx.Sum64()
	hashInBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashInBytes, hashIn64)
	csum = hex.EncodeToString(hashInBytes)
	return csum, ""
}

// as of 1.9 net/http does not appear to provide any better way..
func IsErrConnectionRefused(err error) (yes bool) {
	if uerr, ok := err.(*url.Error); ok {
		if noerr, ok := uerr.Err.(*net.OpError); ok {
			if scerr, ok := noerr.Err.(*os.SyscallError); ok {
				if scerr.Err == syscall.ECONNREFUSED {
					yes = true
				}
			}
		}
	}
	return
}

//===========================================================================
//
// local (config) save and restore - NOTE: caller is responsible to serialize
//
//===========================================================================
func LocalSave(pathname string, v interface{}) error {
	tmp := pathname + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	b, err := jsoniter.MarshalIndent(v, "", " ")
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

func LocalLoad(pathname string, v interface{}) (err error) {
	file, err := os.Open(pathname)
	if err != nil {
		return
	}
	err = jsoniter.NewDecoder(file).Decode(v)
	_ = file.Close()
	return
}
