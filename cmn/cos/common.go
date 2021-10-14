// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/mono"
	jsoniter "github.com/json-iterator/go"
)

// IEC (binary) units
const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

// misc common constants
const (
	SizeofI64 = int(unsafe.Sizeof(uint64(0)))
	SizeofI32 = int(unsafe.Sizeof(uint32(0)))
	SizeofI16 = int(unsafe.Sizeof(uint16(0)))

	RFC822 = time.RFC822 // SelectMsg.TimeFormat enum

	MLCG32 = 1103515245 // xxhash seed

	PermRWR       os.FileMode = 0o640 // POSIX perms
	PermRWXRX     os.FileMode = 0o750
	configDirMode             = PermRWXRX | os.ModeDir

	JSONLocalTagOmit = "omit"
)

type (
	StringSet map[string]struct{}
	SimpleKVs map[string]string

	SysInfo struct {
		MemUsed    uint64  `json:"mem_used"`
		MemAvail   uint64  `json:"mem_avail"`
		PctMemUsed float64 `json:"pct_mem_used"`
		PctCPUUsed float64 `json:"pct_cpu_used"`
	}

	JSONRawMsgs map[string]jsoniter.RawMessage
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

// JSON is used to Marshal/Unmarshal API json messages and is initialized in init function.
var JSON jsoniter.API

func init() {
	rand.Seed(mono.NanoTime())
	rtie.Store(1013)

	jsonConf := jsoniter.Config{
		EscapeHTML:             false, // We don't send HTMLs.
		ValidateJsonRawMessage: false, // RawMessages are validated by morphing.
		// Need to be sure that we have exactly the same struct as user requested.
		DisallowUnknownFields: true,
		SortMapKeys:           true,
	}
	JSON = jsonConf.Froze()
}

//////////////////////
// JSON & JSONLocal //
//////////////////////

func MarshalToString(v interface{}) (string, error) { return JSON.MarshalToString(v) }

func MustMarshalToString(v interface{}) string {
	s, err := JSON.MarshalToString(v)
	AssertNoErr(err)
	return s
}

// MustMarshal marshals v and panics if error occurs.
func MustMarshal(v interface{}) []byte {
	b, err := JSON.Marshal(v)
	AssertNoErr(err)
	return b
}

func MorphMarshal(data, v interface{}) error {
	// `data` can be of type `map[string]interface{}` or just same type as `v`.
	// Therefore, the easiest way is to marshal the `data` again and unmarshal it
	// with hope that every field will be set correctly.
	b := MustMarshal(data)
	return JSON.Unmarshal(b, v)
}

func MustMorphMarshal(data, v interface{}) {
	err := MorphMarshal(data, v)
	AssertNoErr(err)
}

/////////////
// PARSING //
/////////////

func IsParseBool(s string) bool {
	yes, err := ParseBool(s)
	_ = err // error means false
	return yes
}

// ParseBool converts string to bool (case-insensitive):
//   y, yes, on -> true
//   n, no, off, <empty value> -> false
// strconv handles the following:
//   1, true, t -> true
//   0, false, f -> false
func ParseBool(s string) (value bool, err error) {
	if s == "" {
		return
	}
	s = strings.ToLower(s)
	switch s {
	case "y", "yes", "on":
		return true, nil
	case "n", "no", "off":
		return false, nil
	}
	return strconv.ParseBool(s)
}

// ParseEnvVariables takes in a .env file and parses its contents
func ParseEnvVariables(fpath string, delimiter ...string) map[string]string {
	m := map[string]string{}
	dlim := "="
	data, err := os.ReadFile(fpath)
	if err != nil {
		return nil
	}

	if len(delimiter) > 0 {
		dlim = delimiter[0]
	}

	paramList := strings.Split(string(data), "\n")
	for _, dat := range paramList {
		datum := strings.Split(dat, dlim)
		// key=val
		if len(datum) == 2 {
			key := strings.TrimSpace(datum[0])
			value := strings.TrimSpace(datum[1])
			m[key] = value
		}
	}
	return m
}

func ParseHexOrUint(s string) (uint64, error) {
	const hexPrefix = "0x"
	if strings.HasPrefix(s, hexPrefix) {
		return strconv.ParseUint(s[len(hexPrefix):], 16, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}

///////////////
// SimpleKVs //
///////////////

func (kv SimpleKVs) Compare(other SimpleKVs) bool {
	if len(kv) != len(other) {
		return false
	} else if len(kv) > 0 {
		return reflect.DeepEqual(kv, other)
	}
	return true
}

func (kv SimpleKVs) Keys() []string {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	return keys
}

func (kv SimpleKVs) Contains(key string) (ok bool) {
	if len(kv) == 0 {
		return false
	}
	_, ok = kv[key]
	return
}

///////////////
// StringSet //
///////////////

func NewStringSet(keys ...string) (ss StringSet) {
	ss = make(StringSet, len(keys))
	ss.Add(keys...)
	return
}

func (ss StringSet) String() string {
	keys := ss.ToSlice()
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (ss StringSet) ToSlice() []string {
	keys := make([]string, len(ss))
	idx := 0
	for key := range ss {
		keys[idx] = key
		idx++
	}
	return keys
}

func (ss StringSet) Add(keys ...string) {
	for _, key := range keys {
		ss[key] = struct{}{}
	}
}

func (ss StringSet) Contains(key string) (yes bool) {
	_, yes = ss[key]
	return
}

func (ss StringSet) Delete(key string) {
	delete(ss, key)
}

func (ss StringSet) Intersection(other StringSet) StringSet {
	result := make(StringSet)
	for key := range ss {
		if other.Contains(key) {
			result.Add(key)
		}
	}
	return result
}

func (ss StringSet) Clone() StringSet {
	result := make(StringSet, len(ss))
	for k, v := range ss {
		result[k] = v
	}
	return result
}

func (ss StringSet) All(xs ...string) bool {
	for _, x := range xs {
		if !ss.Contains(x) {
			return false
		}
	}
	return true
}

// shallow copy
func CopyStruct(dst, src interface{}) {
	x := reflect.ValueOf(src)
	Assert(x.Kind() == reflect.Ptr)
	starX := x.Elem()
	y := reflect.New(starX.Type())
	starY := y.Elem()
	starY.Set(starX)
	reflect.ValueOf(dst).Elem().Set(y.Elem())
}

func Printf(format string, a ...interface{}) {
	if flag.Parsed() {
		glog.InfoDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Printf(format+"\n", a...)
	}
}

// FreeMemToOS calls GC and returns allocated memory to OS after that
// Use to clean up memory after a huge amount of memory becomes "free" to
// return it to OS immediately without waiting for GC does it automatically
// Params:
//	d - a delay before starting memory cleanup
func FreeMemToOS(d ...time.Duration) {
	if len(d) != 0 && d[0] != 0 {
		time.Sleep(d[0])
	}
	runtime.GC()
	debug.FreeOSMemory()
}
