// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/mono"
	jsoniter "github.com/json-iterator/go"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB

	// ExtTar is tar files extension
	ExtTar = ".tar"
	// ExtTgz is short tar tgz files extension
	ExtTgz = ".tgz"
	// ExtTarTgz is tar tgz files extension
	ExtTarTgz = ".tar.gz"
	// ExtZip is zip files extension
	ExtZip = ".zip"

	// misc
	SizeofI64 = int(unsafe.Sizeof(uint64(0)))
	SizeofI32 = int(unsafe.Sizeof(uint32(0)))
	SizeofI16 = int(unsafe.Sizeof(uint16(0)))

	configHomeEnvVar = "XDG_CONFIG_HOME" // https://wiki.archlinux.org/index.php/XDG_Base_Directory
	configDirMode    = PermRWXRX | os.ModeDir

	assertMsg = "assertion failed"

	DoesNotExist = "does not exist"
	NoMountpaths = "no mountpaths"

	// NOTE: Taken from cloud.google.com/go/storage/storage.go (userAgent).
	GcsUA      = "gcloud-golang-storage/20151204"
	GithubHome = "https://github.com/NVIDIA/aistore"

	QuantityPercent = "percent"
	QuantityBytes   = "bytes"

	PermRWR   os.FileMode = 0o640
	PermRWXRX os.FileMode = 0o750
)

var (
	// JSON is used to Marshal/Unmarshal API json messages and is initialized in init function.
	JSON jsoniter.API

	EnvVars = struct {
		Endpoint string

		IsPrimary string
		PrimaryID string

		SkipVerifyCrt string
		UseHTTPS      string

		NumTarget string
		NumProxy  string
	}{
		Endpoint:      "AIS_ENDPOINT",
		IsPrimary:     "AIS_IS_PRIMARY",
		PrimaryID:     "AIS_PRIMARY_ID",
		SkipVerifyCrt: "AIS_SKIP_VERIFY_CRT",
		UseHTTPS:      "AIS_USE_HTTPS",

		// Env variables used for tests or CI
		NumTarget: "NUM_TARGET",
		NumProxy:  "NUM_PROXY",
	}
)

type (
	StringSet      map[string]struct{}
	SimpleKVs      map[string]string
	JSONRawMsgs    map[string]jsoniter.RawMessage
	SimpleKVsEntry struct {
		Key   string
		Value string
	}
	PairF32 struct {
		Min float32
		Max float32
	}
	SysInfo struct {
		MemUsed    uint64  `json:"mem_used"`
		MemAvail   uint64  `json:"mem_avail"`
		PctMemUsed float64 `json:"pct_mem_used"`
		PctCPUUsed float64 `json:"pct_cpu_used"`
	}
	CapacityInfo struct {
		Used    uint64  `json:"fs_used,string"`
		Total   uint64  `json:"fs_capacity,string"`
		PctUsed float64 `json:"pct_fs_used"`
	}
	TSysInfo struct {
		SysInfo
		CapacityInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*SysInfo  `json:"proxy"`
		Target map[string]*TSysInfo `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  JSONRawMsgs `json:"proxy"`
		Target JSONRawMsgs `json:"target"`
	}
	ParsedQuantity struct {
		Type  string
		Value uint64
	}
	DurationJSON time.Duration
	SizeJSON     int64
)

var (
	bucketReg *regexp.Regexp
	nsReg     *regexp.Regexp
)

var (
	ErrInvalidFmtFormat  = errors.New("input 'fmt' format is invalid should be 'prefix-%06d-suffix")
	ErrInvalidBashFormat = errors.New("input 'bash' format is invalid, should be 'prefix-{0001..0010..1}-suffix'")
	ErrInvalidAtFormat   = errors.New("input 'at' format is invalid, should be 'prefix-@00100-suffix'")

	ErrStartAfterEnd   = errors.New("'start' cannot be greater than 'end'")
	ErrNegativeStart   = errors.New("'start' is negative")
	ErrNonPositiveStep = errors.New("'step' is non positive number")
)

var (
	ErrInvalidQuantityUsage       = errors.New("invalid quantity, format should be '81%' or '1GB'")
	errInvalidQuantityNonNegative = errors.New("quantity should not be negative")
	ErrInvalidQuantityPercent     = errors.New("percent must be in the range (0, 100)")
	ErrInvalidQuantityBytes       = errors.New("value (bytes) must be non-negative")
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

func init() {
	// General
	rand.Seed(mono.NanoTime())
	rtie.Store(1013)

	bucketReg = regexp.MustCompile(`^[.a-zA-Z0-9_-]*$`)
	nsReg = regexp.MustCompile(`^[a-zA-Z0-9_-]*$`)

	// Config related
	GCO = &globalConfigOwner{listeners: make(map[string]ConfigListener, 4)}
	config := &Config{}
	GCO.c.Store(unsafe.Pointer(config))

	// API related
	JSON = jsoniter.Config{
		EscapeHTML:             false, // We don't send HTMLs.
		ValidateJsonRawMessage: false, // RawMessages are validated by morphing.
		// Need to be sure that we have exactly the same struct as user requested.
		DisallowUnknownFields: true,
		SortMapKeys:           true,
	}.Froze()
}

///////////////
// SimpleKVs //
///////////////

func NewSimpleKVs(entries ...SimpleKVsEntry) SimpleKVs {
	kvs := make(SimpleKVs, len(entries))
	for _, entry := range entries {
		kvs[entry.Key] = entry.Value
	}
	return kvs
}

func NewSimpleKVsFromQuery(query url.Values) SimpleKVs {
	kvs := make(SimpleKVs, len(query))
	for key := range query {
		kvs[key] = query.Get(key)
	}
	return kvs
}

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
	keys := ss.Keys()
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (ss StringSet) Keys() []string {
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

func (ss StringSet) Contains(key string) bool {
	if len(ss) == 0 {
		return false
	}
	_, ok := ss[key]
	return ok
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

// Exitf writes formatted message to STDOUT and exits with non-zero status code.
func Exitf(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, f, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

// ExitLogf is wrapper around `Exitf` with `glog` logging. It should be used
// instead `Exitf` if the `glog` was initialized.
func ExitLogf(f string, a ...interface{}) {
	glog.Errorln("Terminating...")
	glog.Errorf(f, a...)
	glog.Flush()
	Exitf(f, a...)
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

// WaitForFunc executes a function in goroutine and waits for it to finish.
// If the function runs longer than `timeLong` WaitForFunc notifies a user
// that the user should wait for the result
func WaitForFunc(f func() error, timeLong time.Duration) error {
	timer := time.NewTimer(timeLong)
	chDone := make(chan struct{}, 1)
	var err error
	go func() {
		err = f()
		chDone <- struct{}{}
	}()

loop:
	for {
		select {
		case <-timer.C:
			fmt.Println("Please wait, the operation may take some time")
		case <-chDone:
			timer.Stop()
			break loop
		}
	}

	return err
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

//////////////////////////////
// config: path, load, save //
//////////////////////////////

func homeDir() string {
	currentUser, err := user.Current()
	if err != nil {
		return os.Getenv("HOME")
	}
	return currentUser.HomeDir
}

func AppConfigPath(appName string) (configDir string) {
	// Determine the location of config directory
	if cfgHome := os.Getenv(configHomeEnvVar); cfgHome != "" {
		// $XDG_CONFIG_HOME/appName
		configDir = filepath.Join(cfgHome, appName)
	} else {
		configDir = filepath.Join(homeDir(), ".config", appName)
	}
	return
}

// ExpandPath replaces common abbreviations in file path (eg. `~` with absolute
// path to the current user home directory) and cleans the path.
func ExpandPath(path string) string {
	if path == "" || path[0] != '~' {
		return filepath.Clean(path)
	}
	if len(path) > 1 && path[1] != '/' {
		return filepath.Clean(path)
	}

	currentUser, err := user.Current()
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(filepath.Join(currentUser.HomeDir, path[1:]))
}

/////////////
// PARSING //
/////////////

func IsParseBool(s string) (yes bool) { yes, _ = ParseBool(s); return }

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

func ParseQuantity(quantity string) (ParsedQuantity, error) {
	quantity = strings.ReplaceAll(quantity, " ", "")
	idx := 0
	number := ""
	for ; idx < len(quantity) && unicode.IsDigit(rune(quantity[idx])); idx++ {
		number += string(quantity[idx])
	}

	parsedQ := ParsedQuantity{}
	if value, err := strconv.Atoi(number); err != nil {
		return parsedQ, ErrInvalidQuantityUsage
	} else if value < 0 {
		return parsedQ, errInvalidQuantityNonNegative
	} else {
		parsedQ.Value = uint64(value)
	}

	if len(quantity) <= idx {
		return parsedQ, ErrInvalidQuantityUsage
	}

	suffix := quantity[idx:]
	if suffix == "%" {
		parsedQ.Type = QuantityPercent
		if parsedQ.Value == 0 || parsedQ.Value >= 100 {
			return parsedQ, ErrInvalidQuantityPercent
		}
	} else if value, err := S2B(quantity); err != nil {
		return parsedQ, err
	} else if value < 0 {
		return parsedQ, ErrInvalidQuantityBytes
	} else {
		parsedQ.Type = QuantityBytes
		parsedQ.Value = uint64(value)
	}

	return parsedQ, nil
}

func (pq ParsedQuantity) String() string {
	switch pq.Type {
	case QuantityPercent:
		return fmt.Sprintf("%d%%", pq.Value)
	case QuantityBytes:
		return UnsignedB2S(pq.Value, 2)
	default:
		AssertMsg(false, fmt.Sprintf("Unknown quantity type: %s", pq.Type))
		return ""
	}
}

// ParseEnvVariables takes in a .env file and parses its contents
func ParseEnvVariables(fpath string, delimiter ...string) map[string]string {
	m := map[string]string{}
	dlim := "="
	data, err := ioutil.ReadFile(fpath)
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

//////////////////
// DurationJSON //
//////////////////

func (d DurationJSON) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(d.String()) }
func (d DurationJSON) String() string               { return time.Duration(d).String() }
func (d DurationJSON) IsZero() bool                 { return d == 0 }

func (d *DurationJSON) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := jsoniter.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = DurationJSON(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = DurationJSON(tmp)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

//////////////
// SizeJSON //
//////////////

func (sj SizeJSON) MarshalJSON() ([]byte, error) {
	return []byte(B2S(int64(sj), 2)), nil
}
