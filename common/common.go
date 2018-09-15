/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package common provides common low-level types and utilities for all dfcpub projects
package common

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/json-iterator/go"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

type (
	StringSet map[string]struct{}
	SimpleKVs map[string]string
)

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
	glog.Fatalln(message)
}

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
