// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	jsoniter "github.com/json-iterator/go"
	"github.com/teris-io/shortid"
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

	// Constant seeds for UUID generator
	uuidWorker = 1
	uuidSeed   = 17
	// Alphabet for generating UUIDs - similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"

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
	FSInfo struct {
		FSUsed     uint64  `json:"fs_used,string"`
		FSCapacity uint64  `json:"fs_capacity,string"`
		PctFSUsed  float64 `json:"pct_fs_used"`
	}
	TSysInfo struct {
		SysInfo
		FSInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*SysInfo  `json:"proxy"`
		Target map[string]*TSysInfo `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  JSONRawMsgs `json:"proxy"`
		Target JSONRawMsgs `json:"target"`
	}
	CapacityInfo struct {
		Err     error
		UsedPct int32
		High    bool
		OOS     bool
	}
	ParsedQuantity struct {
		Type  string
		Value uint64
	}
	TemplateRange struct {
		Start      int
		End        int
		Step       int
		DigitCount int
		Gap        string // characters after range (either to next range or end of string)
	}
	ParsedTemplate struct {
		Prefix string
		Ranges []TemplateRange
	}
)

var (
	rtie      atomic.Int32
	bucketReg *regexp.Regexp
	nsReg     *regexp.Regexp
)
var (
	ErrInvalidBashFormat = errors.New("input 'bash' format is invalid, should be 'prefix{0001..0010..1}suffix'")
	ErrInvalidAtFormat   = errors.New("input 'at' format is invalid, should be 'prefix@00100suffix'")

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
	rand.Seed(time.Now().UnixNano())

	sid := shortid.MustNew(uuidWorker /* worker */, uuidABC, uuidSeed /* seed */)
	// NOTE: `shortid` library uses 01/2016 as starting timestamp, maybe we
	// should fork it and change it to the newer date?
	shortid.SetDefault(sid)
	rtie.Store(1013)

	bucketReg = regexp.MustCompile(`^[.a-zA-Z0-9_-]*$`)
	nsReg = regexp.MustCompile(`^[a-zA-Z0-9_-]*$`)

	// Config related
	config := &Config{}
	GCO.c.Store(unsafe.Pointer(config))
	loadDebugMap()

	// API related
	jsonAPI = jsoniter.Config{
		EscapeHTML:             true,
		ValidateJsonRawMessage: true,
		// Need to be sure that we have exactly the same struct as user requested.
		DisallowUnknownFields: true,
		SortMapKeys:           true,
	}.Froze()
}

func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
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
	keys := make([]string, len(ss))
	idx := 0
	for key := range ss {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (ss StringSet) Add(key string) {
	ss[key] = struct{}{}
}

func (ss StringSet) Contains(key string) bool {
	_, ok := ss[key]
	return ok
}

// GenUUID generates long and unique ID (optimizes possibility of collision and
// length). It uses 8 character long strings and uses English alphabet as
// characters. This gives a collision probability of ~1/(5*10^13) which should
// suffice, even for most demanding use-cases.
func GenUUID() (uuid string) {
	return RandString(8)
}

// GenUserID generates unique and user-friendly IDs.
func GenUserID() (uuid string) {
	for {
		uuid = shortid.MustGenerate()
		if uuid[0] != '-' && uuid[0] != '_' && uuid[len(uuid)-1] != '-' && uuid[len(uuid)-1] != '_' {
			return
		}
	}
}

func S2B(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	s = strings.ToUpper(s)
	for k, v := range toBiBytes {
		if ns := strings.TrimSuffix(s, k); ns != s {
			f, err := strconv.ParseFloat(strings.TrimSpace(ns), 64)
			return int64(float64(v) * f), err
		}
	}
	ns := strings.TrimSuffix(s, "B")
	f, err := strconv.ParseFloat(strings.TrimSpace(ns), 64)
	return int64(f), err
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

func UnsignedB2S(b uint64, digits int) string {
	return B2S(int64(b), digits)
}

func TimeDelta(time1, time2 time.Time) time.Duration {
	if time1.IsZero() || time2.IsZero() {
		return 0
	}
	return time1.Sub(time2)
}

func ConvertToString(value interface{}) (valstr string, err error) {
	switch v := value.(type) {
	case string:
		valstr = v
	case bool, int, int32, int64, uint32, uint64, float32, float64:
		valstr = fmt.Sprintf("%v", v)
	default:
		err = fmt.Errorf("failed to assert type on param: %v (type %T)", value, value)
	}
	return
}

func ExitWithCode(code int) {
	glog.Flush()
	os.Exit(code)
}

func ExitInfof(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, f, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func ExitLogf(f string, a ...interface{}) {
	glog.Errorln("Terminating...")
	glog.Errorf(f, a...)
	glog.Flush()
	ExitInfof(f, a...)
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

func StrToSentence(str string) string {
	if str == "" {
		return ""
	}

	capitalized := CapitalizeString(str)

	if !strings.HasSuffix(capitalized, ".") {
		capitalized += "."
	}

	return capitalized
}

func CapitalizeString(s string) string {
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func NounEnding(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

// Either returns either lhs or rhs depending on which one is non-empty
func Either(lhs, rhs string) string {
	if lhs != "" {
		return lhs
	}
	return rhs
}

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

// DEBUG: Used for "short lifecycle" asserts
// It should be used when debugging is enabled
// for the package via `AIS_DEBUG` env variable
func Dassert(cond bool, pkg string) {
	if _, ok := CheckDebug(pkg); ok {
		Assert(cond)
	}
}

// Used for "short lifecycle" asserts (ie debugging)
func DassertMsg(cond bool, msg, pkg string) {
	if _, ok := CheckDebug(pkg); ok {
		AssertMsg(cond, msg)
	}
}

func StringInSlice(s string, arr []string) bool {
	for _, el := range arr {
		if el == s {
			return true
		}
	}
	return false
}

// StrSlicesEqual compares content of two string slices. It is replacement for
// reflect.DeepEqual because the latter returns false if slices have the same
// values but in different order.
func StrSlicesEqual(lhs, rhs []string) bool {
	if len(lhs) == 0 && len(rhs) == 0 {
		return true
	}
	if len(lhs) != len(rhs) {
		return false
	}
	total := make(map[string]bool, len(lhs))
	for _, item := range lhs {
		total[item] = true
	}
	for _, item := range rhs {
		if _, ok := total[item]; ok {
			delete(total, item)
			continue
		}
		total[item] = true
	}
	return len(total) == 0
}

func AnyHasPrefixInSlice(prefix string, arr []string) bool {
	for _, el := range arr {
		if strings.HasPrefix(el, prefix) {
			return true
		}
	}
	return false
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

func ValidateBckName(bucket string) (err error) {
	const nameErr = "may only contain letters, numbers, dashes (-), underscores (_), and dots (.)"
	if bucket == "" {
		return errors.New("bucket name is empty")
	}
	if !bucketReg.MatchString(bucket) {
		return fmt.Errorf("bucket name %s is invalid: %s", bucket, nameErr)
	}
	if strings.Contains(bucket, "..") {
		return fmt.Errorf("bucket name %s cannot contain '..'", bucket)
	}
	return
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

///////////////////////
// templates/parsing //
///////////////////////

func (pt *ParsedTemplate) Count() int64 {
	count := int64(1)
	for _, tr := range pt.Ranges {
		step := (tr.End-tr.Start)/tr.Step + 1
		count *= int64(step)
	}
	return count
}

// maxLen specifies maximum objects to be returned
func (pt *ParsedTemplate) ToSlice(maxLen ...int) []string {
	var ( // nolint:prealloc // objs is preallocated farther down
		max  = math.MaxInt64
		objs []string
	)
	if len(maxLen) > 0 && maxLen[0] >= 0 {
		max = maxLen[0]
		objs = make([]string, 0, max)
	} else {
		objs = make([]string, 0, pt.Count())
	}

	getNext := pt.Iter()
	i := 0
	for objName, hasNext := getNext(); hasNext && i < max; objName, hasNext = getNext() {
		objs = append(objs, objName)
		i++
	}
	return objs
}

func (pt *ParsedTemplate) Iter() func() (string, bool) {
	rangesCount := len(pt.Ranges)
	at := make([]int, rangesCount)

	for i, tr := range pt.Ranges {
		at[i] = tr.Start
	}

	var buf bytes.Buffer
	return func() (string, bool) {
		for i := rangesCount - 1; i >= 0; i-- {
			if at[i] > pt.Ranges[i].End {
				if i == 0 {
					return "", false
				}
				at[i] = pt.Ranges[i].Start
				at[i-1] += pt.Ranges[i-1].Step
			}
		}

		buf.Reset()
		buf.WriteString(pt.Prefix)
		for i, tr := range pt.Ranges {
			buf.WriteString(fmt.Sprintf("%0*d%s", tr.DigitCount, at[i], tr.Gap))
		}

		at[rangesCount-1] += pt.Ranges[rangesCount-1].Step
		return buf.String(), true
	}
}

func ParseBashTemplate(template string) (pt ParsedTemplate, err error) {
	// "prefix-{00001..00010..2}-gap-{001..100..2}-suffix"

	left := strings.Index(template, "{")
	if left == -1 {
		err = ErrInvalidBashFormat
		return
	}
	right := strings.LastIndex(template, "}")
	if right == -1 {
		err = ErrInvalidBashFormat
		return
	}
	if right < left {
		err = ErrInvalidBashFormat
		return
	}
	pt.Prefix = template[:left]

	for {
		tr := TemplateRange{}

		left := strings.Index(template, "{")
		if left == -1 {
			break
		}

		right := strings.Index(template, "}")
		if right == -1 {
			err = ErrInvalidBashFormat
			return
		}
		if right < left {
			err = ErrInvalidBashFormat
			return
		}
		inside := template[left+1 : right]

		numbers := strings.Split(inside, "..")
		if len(numbers) < 2 || len(numbers) > 3 {
			err = ErrInvalidBashFormat
			return
		} else if len(numbers) == 2 { // {0001..0999} case
			if tr.Start, err = strconv.Atoi(numbers[0]); err != nil {
				return
			}
			if tr.End, err = strconv.Atoi(numbers[1]); err != nil {
				return
			}
			tr.Step = 1
			tr.DigitCount = Min(len(numbers[0]), len(numbers[1]))
		} else if len(numbers) == 3 { // {0001..0999..2} case
			if tr.Start, err = strconv.Atoi(numbers[0]); err != nil {
				return
			}
			if tr.End, err = strconv.Atoi(numbers[1]); err != nil {
				return
			}
			if tr.Step, err = strconv.Atoi(numbers[2]); err != nil {
				return
			}
			tr.DigitCount = Min(len(numbers[0]), len(numbers[1]))
		}
		if err = validateBoundaries(tr.Start, tr.End, tr.Step); err != nil {
			return
		}

		// apply gap (either to next range or end of the template)
		template = template[right+1:]
		right = strings.Index(template, "{")
		if right >= 0 {
			tr.Gap = template[:right]
		} else {
			tr.Gap = template
		}

		pt.Ranges = append(pt.Ranges, tr)
	}
	return
}

func ParseAtTemplate(template string) (pt ParsedTemplate, err error) {
	// "prefix-@00001-gap-@100-suffix"
	left := strings.Index(template, "@")
	if left == -1 {
		err = ErrInvalidAtFormat
		return
	}
	pt.Prefix = template[:left]

	for {
		tr := TemplateRange{}

		left := strings.Index(template, "@")
		if left == -1 {
			break
		}

		number := ""
		for left++; len(template) > left && unicode.IsDigit(rune(template[left])); left++ {
			number += string(template[left])
		}

		tr.Start = 0
		if tr.End, err = strconv.Atoi(number); err != nil {
			return
		}
		tr.Step = 1
		tr.DigitCount = len(number)

		if err = validateBoundaries(tr.Start, tr.End, tr.Step); err != nil {
			return
		}

		// apply gap (either to next range or end of the template)
		template = template[left:]
		right := strings.Index(template, "@")
		if right >= 0 {
			tr.Gap = template[:right]
		} else {
			tr.Gap = template
		}

		pt.Ranges = append(pt.Ranges, tr)
	}
	return
}

func validateBoundaries(start, end, step int) error {
	if start > end {
		return ErrStartAfterEnd
	}
	if start < 0 {
		return ErrNegativeStart
	}
	if step <= 0 {
		return ErrNonPositiveStep
	}
	return nil
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

/////////////////////
// time formatting //
/////////////////////

func FormatUnixNano(unixnano int64, format string) string {
	t := time.Unix(0, unixnano)
	switch format {
	case "", RFC822:
		return t.Format(time.RFC822)
	default:
		return t.Format(format)
	}
}

func FormatTimestamp(tm time.Time) string { return tm.Format(timeStampFormat) }

func Duration2S(d time.Duration) string { return strconv.FormatInt(int64(d), 10) }
func S2Duration(s string) (time.Duration, error) {
	d, err := strconv.ParseInt(s, 0, 64)
	return time.Duration(d), err
}

func UnixNano2S(unixnano int64) string   { return strconv.FormatInt(unixnano, 10) }
func S2UnixNano(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
func IsTimeZero(t time.Time) bool        { return t.IsZero() || t.UTC().Unix() == 0 } // https://github.com/golang/go/issues/33597
