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
	"net/url"
	"os"
	"path/filepath"
	"reflect"
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
	"github.com/teris-io/shortid"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB

	DefaultBufSize = 32 * KiB

	// Constant seeds for UUID generator
	uuidWorker = 1
	uuidSeed   = 17
	// Alphabet for generating UUIDs - similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "5nZJDft6LuzsjGNpPwY7r_Qa3-9vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk"

	// misc
	SizeofI64 = int(unsafe.Sizeof(uint64(0)))
	SizeofI32 = int(unsafe.Sizeof(uint32(0)))
	SizeofI16 = int(unsafe.Sizeof(uint16(0)))
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

const (
	DoesNotExist = "does not exist"
	NotSupported = "not supported yet"
	NoMountpaths = "no mountpaths"

	GcsURL    = "http://storage.googleapis.com"
	GcsURLAlt = "http://www.googleapis.com"
)

type (
	StringSet      map[string]struct{}
	SimpleKVs      map[string]string
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
		FSUsed     uint64  `json:"fs_used"`
		FSCapacity uint64  `json:"fs_capacity"`
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
		Proxy  map[string]jsoniter.RawMessage `json:"proxy"`
		Target map[string]jsoniter.RawMessage `json:"target"`
	}
)

var rtie atomic.Int32

func init() {
	sid := shortid.MustNew(uuidWorker /* worker */, uuidABC, uuidSeed /* seed */)
	// NOTE: `shortid` library uses 01/2016 as starting timestamp, maybe we
	// should fork it and change it to the newer date?
	shortid.SetDefault(sid)
	rtie.Store(1013)
}

func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
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
// common utils
//

func GenUUID() (string, error) {
	return shortid.Generate()
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
	glog.Errorf(f, a...)
	glog.Flush()
	ExitInfof(f, a...)
}

func ParseBool(s string) (value bool, err error) {
	if s == "" {
		return
	}
	value, err = strconv.ParseBool(s)
	return
}

func ErrorToSentence(err error) string {
	if err == nil {
		return ""
	}

	capitalized := CapitalizeString(err.Error())

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

const assertMsg = "assertion failed"

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

//
// files, IO, hash
//

var (
	_ ReadOpenCloser = &FileHandle{}
	_ ReadSizer      = &SizedReader{}
	_ ReadOpenCloser = &FileSectionHandle{}
)

type (
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

	// SizedReader is simple struct which implements ReadSizer interface.
	SizedReader struct {
		io.Reader
		size int64
	}
)

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
// NOTE: padded byte values are random
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
		fromPad = int64(len(buf) - n)
	} else {
		// slice is already read, keep reading padding bytes
		fromPad = MinI64(int64(len(buf)), f.padding-f.padOffset)
	}

	// either buffer is full or end of padding is reached. Nothing to read
	if fromPad == 0 {
		return n, io.EOF
	}

	// the number of remained bytes in padding is enough to complete read request
	if fromPad <= (f.padding - f.padOffset) {
		n += int(fromPad)
		f.padOffset += fromPad
		return n, nil
	}

	// the number of bytes remained in padding is less than required. "Read" as
	// many as possible and return io.EOF
	n += int(f.padding - f.padOffset)
	f.padOffset = f.padding
	return n, io.EOF
}

func (f *FileSectionHandle) Close() error { return nil }

// CreateDir creates directory if does not exists. Does not return error when
// directory already exists.
func CreateDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// CreateFile creates file and ensures that the directories for the file will be
// created if they do not yet exist.
func CreateFile(fname string) (*os.File, error) {
	if err := CreateDir(filepath.Dir(fname)); err != nil {
		return nil, err
	}
	return os.Create(fname)
}

// MvFile renames file ensuring that the directory of dst exists. Creates
// destination directory when it does not exist.
func MvFile(src, dst string) error {
	if err := CreateDir(filepath.Dir(dst)); err != nil {
		return err
	}
	return os.Rename(src, dst)
}

func CopyFile(src, dst string, buf []byte) (err error) {
	var (
		reader *os.File
		writer *os.File
	)
	if reader, err = os.Open(src); err != nil {
		glog.Errorf("Failed to open %s: %v", src, err)
		return
	}
	if writer, err = CreateFile(dst); err != nil {
		glog.Errorf("Failed to create %s: %v", dst, err)
		reader.Close()
		return
	}
	if _, err = io.CopyBuffer(writer, reader, buf); err != nil {
		glog.Errorf("Failed to copy %s -> %s: %v", src, dst, err)
	}
	writer.Close()
	reader.Close()
	return
}

func PathWalkErr(err error) string {
	if os.IsNotExist(err) {
		return ""
	}
	return fmt.Sprintf("filepath-walk invoked with err: %v", err)
}

// Saves the reader directly to a local file
// `size` is an optional argument, if it is set only first `size` bytes
// are saved to the file
func SaveReader(fqn string, reader io.Reader, buf []byte, needCksum bool, size ...int64) (cksum string, err error) {
	file, err := CreateFile(fqn)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("failed to save to %q: %v", fqn, err)
	}

	if needCksum {
		cksum = HashToStr(hasher)
	}

	return cksum, nil
}

// Saves the reader to a temporary file `tmpfqn`, and if everything is OK
// it moves the temporary file to a given `fqn`
// `size` is an optional argument, if it is set only first `size` bytes
// are saved to the file
func SaveReaderSafe(tmpfqn, fqn string, reader io.Reader, buf []byte, needCksum bool, size ...int64) (cksum string, err error) {
	if fqn == "" {
		return "", nil
	}

	if cksum, err = SaveReader(tmpfqn, reader, buf, needCksum, size...); err != nil {
		return "", err
	}

	if err := MvFile(tmpfqn, fqn); err != nil {
		return "", err
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

func ComputeXXHash(reader io.Reader, buf []byte) (csum string, errstr string) {
	var err error
	var xx hash.Hash = xxhash.New64()
	_, err = io.CopyBuffer(xx.(io.Writer), reader, buf)
	if err != nil {
		return "", fmt.Sprintf("Failed to copy buffer, err: %v", err)
	}
	return HashToStr(xx), ""
}

func ParseIntRanged(str string, base, bits int, low, high int64) (int64, error) {
	Assert(low <= high)
	v, err := strconv.ParseInt(str, base, bits)
	if err != nil {
		return low, err
	}

	if v < low || v > high {
		if low == high {
			return low, fmt.Errorf("only %d is supported", low)
		}
		return low, fmt.Errorf("it must be between %d and %d", low, high)
	}

	return v, nil
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

func Ratio(high, low, curr int64) float32 {
	Assert(high > low && high <= 100 && low > 0)
	if curr <= low {
		return 0
	}
	if curr >= high {
		return 1
	}
	return float32(curr-low) / float32(high-low)
}

func RatioPct(high, low, curr int64) int64 {
	Assert(high > low && high <= 100 && low > 0)
	if curr <= low {
		return 0
	}
	if curr >= high {
		return 100
	}
	return (curr - low) * 100 / (high - low)
}

//
// TEMPLATES/PARSING
//

type (
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
	ErrInvalidBashFormat = errors.New("input 'bash' format is invalid, should be 'prefix{0001..0010..1}suffix'")
	ErrInvalidAtFormat   = errors.New("input 'at' format is invalid, should be 'prefix@00100suffix'")

	ErrStartAfterEnd   = errors.New("'start' cannot be greater than 'end'")
	ErrNegativeStart   = errors.New("'start' is negative")
	ErrNonPositiveStep = errors.New("'step' is non positive number")
)

func (pt *ParsedTemplate) Count() int {
	count := 1
	for _, tr := range pt.Ranges {
		count *= (tr.End-tr.Start)/tr.Step + 1
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

type (
	ParsedQuantity struct {
		Type  string
		Value uint64
	}
)

const (
	QuantityPercent = "percent"
	QuantityBytes   = "bytes"
)

var (
	ErrInvalidQuantityUsage       = errors.New("invalid quantity, format should be '81%' or '1GB'")
	errInvalidQuantityNonNegative = errors.New("quantity should not be negative")
	ErrInvalidQuantityPercent     = errors.New("percent must be in the range (0, 100)")
	ErrInvalidQuantityBytes       = errors.New("value (bytes) must be non-negative")
)

func ParseQuantity(quantity string) (ParsedQuantity, error) {
	quantity = strings.Replace(quantity, " ", "", -1)
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

//
// time formatting
//

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
func S2Time(timeStr string) (t time.Time, err error) {
	var tunix int64
	if tunix, err = strconv.ParseInt(timeStr, 10, 64); err != nil {
		return
	}
	t = time.Unix(0, tunix)
	return
}
