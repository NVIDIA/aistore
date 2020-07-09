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
	configDirMode    = 0755 | os.ModeDir

	assertMsg = "assertion failed"

	DoesNotExist = "does not exist"
	NoMountpaths = "no mountpaths"

	GcsUA     = "gcloud-golang-storage/20151204" // NOTE: taken from cloud.google.com/go/storage/storage.go (userAgent)
	GcsURL    = "http://storage.googleapis.com"
	GcsURLAlt = "http://www.googleapis.com"

	LetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	QuantityPercent = "percent"
	QuantityBytes   = "bytes"

	S3TagSepa = "!"
	TF        = "tf"
)

var (
	EnvVars = struct {
		Endpoint string

		IsPrimary string
		PrimaryID string

		SkipVerifyCrt string
		UseHTTPS      string
	}{
		Endpoint:      "AIS_ENDPOINT",
		IsPrimary:     "AIS_IS_PRIMARY",
		PrimaryID:     "AIS_PRIMARY_ID",
		SkipVerifyCrt: "AIS_SKIP_VERIFY_CRT",
		UseHTTPS:      "AIS_USE_HTTPS",
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

func (tj DurationJSON) MarshalJSON() ([]byte, error) {
	return []byte(time.Duration(tj).String()), nil
}

func (sj SizeJSON) MarshalJSON() ([]byte, error) {
	return []byte(B2S(int64(sj), 2)), nil
}

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
var (
	toBiBytes = map[string]int64{
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
)

func init() {
	// General
	rand.Seed(mono.NanoTime())
	rtie.Store(1013)

	bucketReg = regexp.MustCompile(`^[.a-zA-Z0-9_-]*$`)
	nsReg = regexp.MustCompile(`^[a-zA-Z0-9_-]*$`)

	// Config related
	config := &Config{}
	GCO.c.Store(unsafe.Pointer(config))

	// API related
	jsonAPI = jsoniter.Config{
		EscapeHTML:             true,
		ValidateJsonRawMessage: true,
		// Need to be sure that we have exactly the same struct as user requested.
		DisallowUnknownFields: true,
		SortMapKeys:           true,
	}.Froze()
}

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = LetterBytes[rand.Int63()%int64(len(LetterBytes))]
	}
	return string(b)
}

func RandStringWithSrc(src *rand.Rand, n int) string {
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(LetterBytes) {
			b[i] = LetterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

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

func IsParseBool(s string) (yes bool) { yes, _ = ParseBool(s); return }

// NOTE: not to be used in the datapath - consider instead one of the 3 flavors below
func AssertFmt(cond bool, args ...interface{}) {
	if cond {
		return
	}
	var message = assertMsg
	if len(args) > 0 {
		message += ": "
		for i := 0; i < len(args); i++ {
			message += fmt.Sprintf("%#v ", args[i])
		}
	}
	panic(message)
}

// this and the other two asserts get inlined and optimized
func Assert(cond bool) {
	if !cond {
		glog.Flush()
		panic(assertMsg)
	}
}

// NOTE: preferable usage is to have the 'if' in the calling code:
//       if (!cond) { AssertMsg(false, msg) }
// - otherwise the message (e.g. Sprintf) may get evaluated every time
func AssertMsg(cond bool, msg string) {
	if !cond {
		glog.Flush()
		panic(assertMsg + ": " + msg)
	}
}
func AssertNoErr(err error) {
	if err != nil {
		glog.Flush()
		panic(err)
	}
}

func CopyStruct(dst, src interface{}) {
	x := reflect.ValueOf(src)
	Assert(x.Kind() == reflect.Ptr)
	starX := x.Elem()
	y := reflect.New(starX.Type())
	starY := y.Elem()
	starY.Set(starX)
	reflect.ValueOf(dst).Elem().Set(y.Elem())
}

func S3ObjNameTag(s string) (objName, tag string) {
	if idx := strings.LastIndex(s, S3TagSepa); idx > 0 {
		if s[idx+1:] == TF {
			return s[:idx], s[idx+1:]
		}
	}
	return s, ""
}

func HasTarExtension(objName string) bool {
	return strings.HasSuffix(objName, ExtTar) || strings.HasSuffix(objName, ExtTarTgz) || strings.HasSuffix(objName, ExtTgz)
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

func CheckI64Range(v, low, high int64) (int64, error) {
	if v < low || v > high {
		if low == high {
			return low, fmt.Errorf("must be equal %d", low)
		}
		return low, fmt.Errorf("must be in [%d, %d] range", low, high)
	}
	return v, nil
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

/////////////
// PARSING //
/////////////

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
