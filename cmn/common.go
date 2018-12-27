// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
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

const DoesNotExist = "does not exist"

type (
	StringSet map[string]struct{}
	SimpleKVs map[string]string
	PairF32   struct {
		Min float32
		Max float32
	}
	PairU32 struct {
		Min uint32
		Max uint32
	}
)

//
// PairU32 & PairF32
//
func (src *PairU32) CopyTo(dst *PairU32) {
	atomic.StoreUint32(&dst.Min, atomic.LoadUint32(&src.Min))
	atomic.StoreUint32(&dst.Max, atomic.LoadUint32(&src.Max))
}

func (upair *PairU32) Init(f float32) {
	u := math.Float32bits(f)
	atomic.StoreUint32(&upair.Max, u)
	atomic.StoreUint32(&upair.Min, u)
}

func (upair *PairU32) U2F() (fpair PairF32) {
	min := atomic.LoadUint32(&upair.Min)
	fpair.Min = math.Float32frombits(min)
	max := atomic.LoadUint32(&upair.Max)
	fpair.Max = math.Float32frombits(max)
	return
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

var (
	_ ReadOpenCloser = &FileHandle{}
	_ ReadSizer      = &SizedReader{}
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

const DefaultBufSize = 32 * KiB

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

// ReadWriteWithHash reads data from an io.Reader, writes data to an io.Writer and computes
// xxHash on the data.
func ReadWriteWithHash(r io.Reader, w io.Writer, buf []byte) (int64, string, error) {
	var (
		total int64
	)
	if buf == nil {
		buf = make([]byte, DefaultBufSize)
	}

	h := xxhash.New64()
	mw := io.MultiWriter(h, w)
	for {
		n, err := r.Read(buf)
		total += int64(n)
		if err != nil && err != io.EOF {
			return 0, "", err
		}

		if n == 0 {
			break
		}

		mw.Write(buf[:n])
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(h.Sum64()))
	return total, hex.EncodeToString(b), nil
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
	written, err = io.CopyBuffer(writer, rrbody, buf)
	return
}

func ComputeXXHash(reader io.Reader, buf []byte) (csum string, errstr string) {
	var err error
	var xx hash.Hash64 = xxhash.New64()
	_, err = io.CopyBuffer(xx.(io.Writer), reader, buf)
	if err != nil {
		return "", fmt.Sprintf("Failed to copy buffer, err: %v", err)
	}
	hashIn64 := xx.Sum64()
	hashInBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashInBytes, hashIn64)
	csum = hex.EncodeToString(hashInBytes)
	return csum, ""
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
	Assert(high > low && high <= 100 && low < 100 && low > 0)
	if curr <= low {
		return 0
	}
	if curr >= high {
		return 1
	}
	return float32(curr-low) / float32(high-low)
}
