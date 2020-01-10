// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash"
	"io"
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pierrec/lz4/v3"
	"github.com/teris-io/shortid"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB

	DefaultBufSize = KiB * 32
	PageSize       = KiB * 4

	// Constant seeds for UUID generator
	uuidWorker  = 1
	u64idWorker = 2
	uuidSeed    = 17
	// Alphabet for generating UUIDs - similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"

	// misc
	SizeofI64 = int(unsafe.Sizeof(uint64(0)))
	SizeofI32 = int(unsafe.Sizeof(uint32(0)))
	SizeofI16 = int(unsafe.Sizeof(uint16(0)))

	configHomeEnvVar = "XDG_CONFIG_HOME" // https://wiki.archlinux.org/index.php/XDG_Base_Directory
	configDirMode    = 0755 | os.ModeDir
)
const (
	assertMsg = "assertion failed"
)
const (
	DoesNotExist = "does not exist"
	NoMountpaths = "no mountpaths"

	GcsUA     = "gcloud-golang-storage/20151204" // NOTE: taken from cloud.google.com/go/storage/storage.go (userAgent)
	GcsURL    = "http://storage.googleapis.com"
	GcsURLAlt = "http://www.googleapis.com"
)
const (
	LetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)
const (
	QuantityPercent = "percent"
	QuantityBytes   = "bytes"
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
	ReadOpenCloser interface {
		io.ReadCloser
		Open() (io.ReadCloser, error)
	}

	// ReadSizer is the interface that adds Size method to the basic reader.
	ReadSizer interface {
		io.Reader
		Size() int64
	}

	FileHandle struct {
		*os.File
		fqn string
	}

	// FileSectionHandle is a slice of already opened file with optional padding
	// that implements ReadOpenCloser interface
	FileSectionHandle struct {
		s         *io.SectionReader
		padding   int64 // padding size
		padOffset int64 // offset iniside padding when reading a file
	}

	// ByteHandle is a byte buffer(made from []byte) that implements
	// ReadOpenCloser interface
	ByteHandle struct {
		*bytes.Reader
	}

	// SizedReader is simple struct which implements ReadSizer interface.
	SizedReader struct {
		io.Reader
		size int64
	}

	nopOpener struct {
		io.ReadCloser
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
	sid64     *shortid.Shortid
	bucketReg *regexp.Regexp
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
var (
	_ ReadOpenCloser = &FileHandle{}
	_ ReadSizer      = &SizedReader{}
	_ ReadOpenCloser = &FileSectionHandle{}
	_ ReadOpenCloser = &nopOpener{}
	_ ReadOpenCloser = &ByteHandle{}
)

func init() {
	// General
	rand.Seed(time.Now().UnixNano())

	sid := shortid.MustNew(uuidWorker /* worker */, uuidABC, uuidSeed /* seed */)
	sid64 = shortid.MustNew(u64idWorker, uuidABC, uuidSeed)
	// NOTE: `shortid` library uses 01/2016 as starting timestamp, maybe we
	// should fork it and change it to the newer date?
	shortid.SetDefault(sid)
	rtie.Store(1013)

	bucketReg = regexp.MustCompile(`^[.a-zA-Z0-9_-]*$`)

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

func ShortID(id int64) uint32 { return uint32(id & int64(0xfffff)) }

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

func (ss StringSet) Add(key string) {
	ss[key] = struct{}{}
}

func (ss StringSet) Contains(key string) bool {
	_, ok := ss[key]
	return ok
}

//
// PairF32
//

func NewPairF32(p *atomic.PairF32) PairF32 {
	min, max := p.Load()
	return PairF32{
		Min: min,
		Max: max,
	}
}

func (fpair PairF32) String() string {
	if fpair.Min == 0 && fpair.Max == 0 {
		return "()"
	}
	return fmt.Sprintf("(%.2f, %.2f)", fpair.Min, fpair.Max)
}

//
// misc utils
//

func GenUUID() (uuid string, err error) {
	for {
		uuid, err = shortid.Generate()
		if err != nil {
			return
		}
		if uuid[0] != '-' && uuid[0] != '_' {
			return
		}
	}
}

func GenUUID64() (int64, error) {
	s, err := sid64.Generate()
	if err != nil {
		return 0, err
	}

	return int64(xxhash.ChecksumString64S(s, MLCG32)), nil
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
		panic(assertMsg)
	}
}

// NOTE: preferable usage is to have the 'if' in the calling code:
//       if (!cond) { AssertMsg(false, msg) }
// - otherwise the message (e.g. Sprintf) may get evaluated every time
func AssertMsg(cond bool, msg string) {
	if !cond {
		panic(assertMsg + ": " + msg)
	}
}
func AssertNoErr(err error) {
	if err != nil {
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

func CopyStruct(dst interface{}, src interface{}) {
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

/////////////////////
// files, IO, hash //
/////////////////////

func NewByteHandle(bt []byte) *ByteHandle {
	return &ByteHandle{
		bytes.NewReader(bt),
	}
}

func (b *ByteHandle) Close() error {
	return nil
}
func (b *ByteHandle) Open() (io.ReadCloser, error) {
	b.Seek(0, io.SeekStart)
	return b, nil
}

func NopOpener(r io.ReadCloser) ReadOpenCloser {
	return &nopOpener{r}
}

func (n *nopOpener) Open() (io.ReadCloser, error) {
	return n, nil
}

func NewFileHandle(fqn string) (*FileHandle, error) {
	file, err := os.Open(fqn)
	if err != nil {
		return nil, err
	}

	return &FileHandle{file, fqn}, nil
}

func (f *FileHandle) Open() (io.ReadCloser, error) {
	return os.Open(f.fqn)
}

func NewSizedReader(r io.Reader, size int64) *SizedReader {
	return &SizedReader{r, size}
}

func (f *SizedReader) Size() int64 {
	return f.size
}

func NewFileSectionHandle(r io.ReaderAt, offset, size, padding int64) (*FileSectionHandle, error) {
	sec := io.NewSectionReader(r, offset, size)
	return &FileSectionHandle{sec, padding, 0}, nil
}

func (f *FileSectionHandle) Open() (io.ReadCloser, error) {
	_, err := f.s.Seek(0, io.SeekStart)
	f.padOffset = 0
	return f, err
}

// Reads a file slice. When the slice finishes but the buffer is not filled yet,
// act as if it reads a few more bytes from somewhere
func (f *FileSectionHandle) Read(buf []byte) (n int, err error) {
	var fromPad int64

	// if it is still reading a file from disk - just continue reading
	if f.padOffset == 0 {
		n, err = f.s.Read(buf)
		// if it reads fewer bytes than expected and it does not fail,
		// try to "read" from padding
		if f.padding == 0 || n == len(buf) || (err != nil && err != io.EOF) {
			return n, err
		}
		fromPad = MinI64(int64(len(buf)-n), f.padding)
	} else {
		// slice is already read, keep reading padding bytes
		fromPad = MinI64(int64(len(buf)), f.padding-f.padOffset)
	}

	// either buffer is full or end of padding is reached. Nothing to read
	if fromPad == 0 {
		return n, io.EOF
	}

	// the number of remained bytes in padding is enough to complete read request
	for idx := n; idx < n+int(fromPad); idx++ {
		buf[idx] = 0
	}
	n += int(fromPad)
	f.padOffset += fromPad

	if f.padOffset < f.padding {
		return n, nil
	}
	return n, io.EOF
}

func (f *FileSectionHandle) Close() error { return nil }

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

func ValidateOmitBase(fqn, omitBase string) (err error) {
	const a = "does not represent an absolute path"
	if omitBase != "" && !filepath.IsAbs(omitBase) {
		return fmt.Errorf("pathname prefix '%s' %s", omitBase, a)
	}
	if !filepath.IsAbs(fqn) {
		return fmt.Errorf("pathname '%s' %s", fqn, a)
	}
	if omitBase != "" && !strings.HasPrefix(fqn, omitBase) {
		return fmt.Errorf("pathname '%s' does not begin with '%s'", fqn, omitBase)
	}
	return
}

func ValidateBucketName(bucket string) (err error) {
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

// CreateDir creates directory if does not exists. Does not return error when
// directory already exists.
func CreateDir(dir string) error {
	return os.MkdirAll(dir, configDirMode)
}

// CreateFile creates file and ensures that the directories for the file will be
// created if they do not yet exist.
func CreateFile(fname string) (*os.File, error) {
	if err := CreateDir(filepath.Dir(fname)); err != nil {
		return nil, err
	}
	return os.Create(fname)
}

// Rename renames file ensuring that the directory of dst exists. Creates
// destination directory when it does not exist.
// NOTE: Rename should not be used to move objects across different disks, see: fs.MvFile.
func Rename(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		// Retry with created directory - slow path.
		if err := CreateDir(filepath.Dir(dst)); err != nil {
			return err
		}
		return os.Rename(src, dst)
	}
	return nil
}

// RemoveFile removes object from path and ignores if the path no longer exists.
func RemoveFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// computes xxhash if requested
func CopyFile(src, dst string, buf []byte, needCksum bool) (written int64, cksum *Cksum, err error) {
	var (
		hasher *xxhash.XXHash64

		srcFile *os.File
		dstFile *os.File
		writer  io.Writer
	)
	if srcFile, err = os.Open(src); err != nil {
		return
	}
	if dstFile, err = CreateFile(dst); err != nil {
		glog.Errorf("Failed to create %s: %v", dst, err)
		srcFile.Close()
		return
	}

	writer = dstFile
	if needCksum {
		hasher = xxhash.New64()
		writer = io.MultiWriter(dstFile, hasher)
	}

	if written, err = io.CopyBuffer(writer, srcFile, buf); err != nil {
		glog.Errorf("Failed to copy %s -> %s: %v", src, dst, err)
	}
	dstFile.Close()
	srcFile.Close()

	if needCksum {
		cksum = &Cksum{ChecksumXXHash, HashToStr(hasher)}
	}
	return
}

// Saves the reader directly to a local file
// `size` is an optional argument, if it is set only first `size` bytes
// are saved to the file
func SaveReader(fqn string, reader io.Reader, buf []byte, needCksum bool, size ...int64) (cksum *Cksum, err error) {
	file, err := CreateFile(fqn)
	if err != nil {
		return nil, err
	}

	var (
		hasher *xxhash.XXHash64
		writer io.Writer = file
	)
	if needCksum {
		hasher = xxhash.New64()
		writer = io.MultiWriter(file, hasher)
	}
	if len(size) != 0 {
		sz := size[0]
		_, err = io.CopyBuffer(writer, io.LimitReader(reader, sz), buf)
	} else {
		_, err = io.CopyBuffer(writer, reader, buf)
	}

	file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to save to %q: %v", fqn, err)
	}

	if needCksum {
		cksum = NewCksum(ChecksumXXHash, HashToStr(hasher))
	}

	return cksum, nil
}

// Saves the reader to a temporary file `tmpfqn`, and if everything is OK
// it moves the temporary file to a given `fqn`
// `size` is an optional argument, if it is set only first `size` bytes
// are saved to the file
func SaveReaderSafe(tmpfqn, fqn string, reader io.Reader, buf []byte, needCksum bool, size ...int64) (cksum *Cksum, err error) {
	if fqn == "" {
		return nil, nil
	}

	if cksum, err = SaveReader(tmpfqn, reader, buf, needCksum, size...); err != nil {
		return nil, err
	}

	if err := Rename(tmpfqn, fqn); err != nil {
		return nil, err
	}
	return cksum, nil
}

// Read only the first line of a file.
// Do not use for big files: it reads all the content and then extracts the first
// line. Use for files that may contains a few lines with trailing EOL
func ReadOneLine(filename string) (string, error) {
	var line string
	err := ReadLines(filename, func(l string) error {
		line = l
		return io.EOF
	})
	return line, err
}

// Read only the first line of a file and return it as uint64
// Do not use for big files: it reads all the content and then extracts the first
// line. Use for files that may contains a few lines with trailing EOL
func ReadOneUint64(filename string) (uint64, error) {
	line, err := ReadOneLine(filename)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(line, 10, 64)
	return val, err
}

// Read only the first line of a file and return it as int64
// Do not use for big files: it reads all the content and then extracts the first
// line. Use for files that may contains a few lines with trailing EOL
func ReadOneInt64(filename string) (int64, error) {
	line, err := ReadOneLine(filename)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseInt(line, 10, 64)
	return val, err
}

// Read a file line by line and call a callback for each line until the file
// ends or a callback returns io.EOF
func ReadLines(filename string, cb func(string) error) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	lineReader := bufio.NewReader(bytes.NewBuffer(b))
	for {
		line, _, err := lineReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}

		if err := cb(string(line)); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}

// WriteWithHash reads data from an io.Reader, writes data to an io.Writer and computes
// xxHash on the data.
func WriteWithHash(w io.Writer, r io.Reader, buf []byte) (int64, string, error) {
	h := xxhash.New64()
	mw := io.MultiWriter(h, w)
	total, err := io.CopyBuffer(mw, r, buf)
	return total, HashToStr(h), err
}

func ReceiveAndChecksum(w io.Writer, r io.Reader, buf []byte, hashes ...hash.Hash) (written int64, err error) {
	var writer io.Writer
	if len(hashes) == 0 {
		writer = w
	} else {
		writers := make([]io.Writer, len(hashes)+1)
		for i, h := range hashes {
			writers[i] = h
		}
		writers[len(hashes)] = w
		writer = io.MultiWriter(writers...)
	}
	written, err = io.CopyBuffer(writer, r, buf)
	return
}

func ComputeXXHash(reader io.Reader, buf []byte) (cksumValue string, err error) {
	var (
		xx hash.Hash = xxhash.New64()
	)

	_, err = io.CopyBuffer(xx.(io.Writer), reader, buf)
	if err != nil {
		return "", fmt.Errorf("failed to copy buffer, err: %v", err)
	}
	return HashToStr(xx), nil
}

func ParseI64Range(str string, base, bits int, low, high int64) (int64, error) {
	Assert(low <= high)
	v, err := strconv.ParseInt(str, base, bits)
	if err != nil {
		return low, err
	}
	return CheckI64Range(v, low, high)
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

////////////////////////////
// local save and restore //
////////////////////////////

func LocalSave(path string, v interface{}, compress bool) error {
	var (
		zw        *lz4.Writer
		encoder   *jsoniter.Encoder
		tmp       = path + ".tmp"
		file, err = CreateFile(tmp)
	)
	if err != nil {
		return err
	}
	if compress {
		zw = lz4.NewWriter(file)
		encoder = jsoniter.NewEncoder(zw)
	} else {
		encoder = jsoniter.NewEncoder(file)
	}
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(v); err != nil {
		file.Close()
		os.Remove(file.Name())
		return err
	}
	if compress {
		_ = zw.Close()
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func LocalLoad(path string, v interface{}, decompress bool) error {
	var (
		decoder   *jsoniter.Decoder
		zr        *lz4.Reader
		file, err = os.Open(path)
	)
	if err != nil {
		return err
	}
	if decompress {
		zr = lz4.NewReader(file)
		decoder = jsoniter.NewDecoder(zr)
	} else {
		decoder = jsoniter.NewDecoder(file)
	}
	err = decoder.Decode(v)
	_ = file.Close()
	return err
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

// LoadAppConfig reads a config object from the config file.
func LoadAppConfig(appName, configFileName string, v interface{}) (err error) {
	// Check if config file exists.
	configDir := AppConfigPath(appName)
	configFilePath := filepath.Join(configDir, configFileName)
	if _, err = os.Stat(configFilePath); err != nil {
		return err
	}

	// Load config from file.
	err = LocalLoad(configFilePath, v, false /*compression*/)
	if err != nil {
		return fmt.Errorf("failed to load config file %q: %v", configFilePath, err)
	}
	return
}

// SaveAppConfig writes the config object to the config file.
func SaveAppConfig(appName, configFileName string, v interface{}) (err error) {
	// Check if config dir exists; if not, create one with default config.
	configDir := AppConfigPath(appName)
	configFilePath := filepath.Join(configDir, configFileName)
	return LocalSave(configFilePath, v, false /*compression*/)
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

/////////////////////
// time formatting //
/////////////////////

func FormatTime(t time.Time, format string) string {
	switch format {
	case "", RFC822:
		return t.Format(time.RFC822)
	default:
		return t.Format(format)
	}
}

func S2TimeUnix(timeStr string) (tunix int64, err error) {
	tunix, err = strconv.ParseInt(timeStr, 10, 64)
	return
}

// persistent mark indicating rebalancing in progress
func PersistentMarker(kind string) (pm string) {
	const (
		globRebMarker  = ".global_rebalancing"
		localRebMarker = ".resilvering"
	)

	switch kind {
	case ActLocalReb:
		pm = filepath.Join(GCO.Get().Confdir, localRebMarker)
	case ActGlobalReb:
		pm = filepath.Join(GCO.Get().Confdir, globRebMarker)
	default:
		Assert(false)
	}
	return
}
