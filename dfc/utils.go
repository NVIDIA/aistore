// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

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
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
)

const (
	maxAttrSize = 1024
)

// Local unicast IP info
type localIPv4Info struct {
	ipv4 string
	mtu  int
}

type StringSet map[string]struct{}

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
	glog.Flush()
	glog.Fatalln(message)
}

// MinU64 returns min value of a and b for uint64 types
func MinU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
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

// getLocalIPv4List returns a list of local unicast IPv4 with MTU
func getLocalIPv4List(allowLoopback bool) (addrlist []*localIPv4Info, err error) {
	addrlist = make([]*localIPv4Info, 0)
	addrs, e := net.InterfaceAddrs()
	if e != nil {
		err = fmt.Errorf("Failed to get host unicast IPs, err: %v", e)
		return
	}
	iflist, e := net.Interfaces()
	if e != nil {
		err = fmt.Errorf("Failed to get interface list: %v", e)
		return
	}

	for _, addr := range addrs {
		curr := &localIPv4Info{}
		if ipnet, ok := addr.(*net.IPNet); ok && (!ipnet.IP.IsLoopback() || allowLoopback) {
			if ipnet.IP.To4() != nil {
				curr.ipv4 = ipnet.IP.String()
			}
		}
		if curr.ipv4 == "" {
			continue
		}

		for _, intf := range iflist {
			ifAddrs, e := intf.Addrs()
			// skip invalid interfaces
			if e != nil {
				continue
			}
			for _, ifAddr := range ifAddrs {
				if ipnet, ok := ifAddr.(*net.IPNet); ok && ipnet.IP.To4() != nil && ipnet.IP.String() == curr.ipv4 {
					curr.mtu = intf.MTU
					addrlist = append(addrlist, curr)
					break
				}
			}
			if curr.mtu != 0 {
				break
			}
		}
	}

	if len(addrlist) == 0 {
		return addrlist, fmt.Errorf("The host does not have any IPv4 addresses")
	}

	return addrlist, nil
}

// selectConfiguredIPv4 returns the first IPv4 from a preconfigured IPv4 list that
// matches any local unicast IPv4
func selectConfiguredIPv4(addrlist []*localIPv4Info, configuredList []string) (ipv4addr string, err error) {
	glog.Infof("Selecting one of the configured IPv4 addresses: %s...\n", configuredList)
	localList := ""

	for _, localaddr := range addrlist {
		localList += " " + localaddr.ipv4
		for _, ipv4 := range configuredList {
			if localaddr.ipv4 == strings.TrimSpace(ipv4) {
				glog.Warningf("Selected IPv4 %s from the configuration file\n", ipv4)
				return ipv4, nil
			}
		}
	}

	glog.Errorf("Configured IPv4 does not match any local one.\nLocal IPv4 list:%s; Configured ip: %s\n", localList, configuredList)
	return "", fmt.Errorf("Configured IPv4 does not match any local one")
}

// detectLocalIPv4 takes a list of local IPv4s and returns the best fit for a deamon to listen on it
func detectLocalIPv4(addrlist []*localIPv4Info) (ip net.IP, err error) {
	if len(addrlist) == 0 {
		return nil, fmt.Errorf("No addresses to choose from")
	} else if len(addrlist) == 1 {
		msg := fmt.Sprintf("Found only one IPv4: %s, MTU %d", addrlist[0].ipv4, addrlist[0].mtu)
		glog.Info(msg)
		if addrlist[0].mtu <= 1500 {
			glog.Warningf("IPv4 %s MTU size is small: %d\n", addrlist[0].ipv4, addrlist[0].mtu)
		}
		ip = net.ParseIP(addrlist[0].ipv4)
		if ip == nil {
			return nil, fmt.Errorf("Failed to parse IP address: %s", addrlist[0].ipv4)
		}
		return ip, nil
	}

	glog.Warningf("Warning: %d IPv4s available", len(addrlist))
	for _, intf := range addrlist {
		glog.Warningf("    %#v\n", *intf)
	}
	// FIXME: temp hack - make sure to keep working on laptops with dockers
	ip = net.ParseIP(addrlist[0].ipv4)
	if ip == nil {
		return nil, fmt.Errorf("Failed to parse IP address: %s", addrlist[0].ipv4)
	}
	return ip, nil
}

// getipv4addr returns an IPv4 for proxy/target to listen on it.
// 1. If there is an IPv4 in config - it tries to use it
// 2. If config does not contain IPv4 - it chooses one of local IPv4s
func getipv4addr(addrList []*localIPv4Info, configuredIPv4s string) (ip net.IP, err error) {
	if configuredIPv4s == "" {
		return detectLocalIPv4(addrList)
	}

	configuredList := strings.Split(configuredIPv4s, ",")
	selectedIPv4, err := selectConfiguredIPv4(addrList, configuredList)
	if err != nil {
		return nil, err
	}

	ip = net.ParseIP(selectedIPv4)
	if ip == nil {
		return nil, fmt.Errorf("Failed to parse ip %s", selectedIPv4)
	}
	return ip, nil
}

func CreateDir(dirname string) error {
	stat, err := os.Stat(dirname)
	if err == nil && stat.IsDir() {
		return nil
	}
	err = os.MkdirAll(dirname, 0755)
	return err
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
	if err != nil {
		return written, err
	}
	return
}

func CreateFile(fname string) (file *os.File, err error) {
	dirname := filepath.Dir(fname)
	if err = CreateDir(dirname); err != nil {
		return
	}
	file, err = os.Create(fname)
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

//===========================================================================
//
// typed checksum value
//
//===========================================================================
type cksumvalue interface {
	get() (string, string)
}

type cksumvalxxhash struct {
	tag string
	val string
}

type cksumvalmd5 struct {
	tag string
	val string
}

func newcksumvalue(kind string, val string) cksumvalue {
	if kind == "" {
		return nil
	}
	if val == "" {
		glog.Infof("Warning: checksum %s: empty value", kind)
		return nil
	}
	if kind == ChecksumXXHash {
		return &cksumvalxxhash{kind, val}
	}
	assert(kind == ChecksumMD5)
	return &cksumvalmd5{kind, val}
}

func (v *cksumvalxxhash) get() (string, string) { return v.tag, v.val }

func (v *cksumvalmd5) get() (string, string) { return v.tag, v.val }

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

// FIXME: usage
// mentioned in the https://github.com/golang/go/issues/11745#issuecomment-123555313 thread
// there must be a better way to handle this..
func isSyscallWriteError(err error) bool {
	switch e := err.(type) {
	case *url.Error:
		return isSyscallWriteError(e.Err)
	case *net.OpError:
		return e.Op == "write" && isSyscallWriteError(e.Err)
	case *os.SyscallError:
		return e.Syscall == "write"
	default:
		return false
	}
}

// Checks if the error is generated by any IO operation and if the error
// is severe enough to run the FSHC for mountpath testing
//
// for mountpath definition, see fs/mountfs.go
func isIOError(err error) bool {
	if err == nil {
		return false
	}
	if err == io.ErrShortWrite {
		return true
	}

	isIO := func(e error) bool {
		return e == syscall.EIO || // I/O error
			e == syscall.ENOTDIR || // mountpath is missing
			e == syscall.EBUSY || // device or resource is busy
			e == syscall.ENXIO || // No such device
			e == syscall.EBADF || // Bad file number
			e == syscall.ENODEV || // No such device
			e == syscall.EUCLEAN || // (mkdir)structure needs cleaning = broken filesystem
			e == syscall.EROFS || // readonly filesystem
			e == syscall.EDQUOT || // quota exceeded
			e == syscall.ESTALE || // stale file handle
			e == syscall.ENOSPC // no space left
	}

	switch e := err.(type) {
	case *os.PathError:
		return isIO(e.Err)
	case *os.SyscallError:
		return isIO(e.Err)
	default:
		return false
	}
}

func parsebool(s string) (value bool, err error) {
	if s == "" {
		return
	}
	value, err = strconv.ParseBool(s)
	return
}

func fqn2mpathInfo(fqn string) *fs.MountpathInfo {
	var (
		max    int
		result *fs.MountpathInfo
	)

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for _, mpathInfo := range availablePaths {
		rel, err := filepath.Rel(mpathInfo.Path, fqn)
		if err != nil || strings.HasPrefix(rel, "..") {
			continue
		}
		if len(mpathInfo.Path) > max {
			max = len(mpathInfo.Path)
			result = mpathInfo
		}
	}
	return result
}

func fqn2fs(fqn string) (fs string) {
	mpathInfo := fqn2mpathInfo(fqn)
	if mpathInfo == nil {
		return
	}

	fs = mpathInfo.FileSystem
	return
}

func fqn2mountPath(fqn string) (mpath string) {
	mpathInfo := fqn2mpathInfo(fqn)
	if mpathInfo == nil {
		return
	}

	mpath = mpathInfo.Path
	return
}

func splitFQN(fqn string) (mpath, bucket, objName string, isLocal bool, err error) {
	var bucketType string
	path := fqn

	availablePaths, _ := ctx.mountpaths.Mountpaths()
	found := false
OUTER:
	for mpath = range availablePaths {
		for _, bucketType = range []string{"local", "cloud"} {
			prefix := filepath.Join(mpath, bucketType) + string(filepath.Separator)
			if strings.HasPrefix(path, prefix) {
				path = strings.TrimPrefix(path, prefix)
				found = true
				break OUTER
			}
		}
	}

	if !found {
		err = fmt.Errorf("fqn: %s does not belong to any mountpath", fqn)
		return
	}

	items := strings.SplitN(path, "/", 2)
	// It must contain at least: bucket/objName
	if len(items) < 2 {
		err = fmt.Errorf("fqn: %s is not valid", fqn)
		return
	}
	bucket, objName = items[0], items[1]
	if len(objName) == 0 {
		err = fmt.Errorf("fqn: %s has empty object name", fqn)
		return
	}
	if len(bucket) == 0 {
		err = fmt.Errorf("fqn: %s has empty bucket name", fqn)
		return
	}

	isLocal = bucketType == "local"
	return
}

func maxUtilDisks(disksMetricsMap map[string]simplekvs, disks StringSet) (maxutil float64) {
	maxutil = -1
	util := func(disk string) (u float64) {
		if ioMetrics, ok := disksMetricsMap[disk]; ok {
			if utilStr, ok := ioMetrics["%util"]; ok {
				var err error
				if u, err = strconv.ParseFloat(utilStr, 32); err == nil {
					return
				}
			}
		}
		return
	}
	if len(disks) > 0 {
		for disk := range disks {
			if u := util(disk); u > maxutil {
				maxutil = u
			}
		}
		return
	}
	for disk := range disksMetricsMap {
		if u := util(disk); u > maxutil {
			maxutil = u
		}
	}
	return
}

func parsePort(p string) (int, error) {
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, err
	}

	if port <= 0 || port >= (1<<16) {
		return 0, fmt.Errorf("port number (%d) should be between 1 and 65535", port)
	}

	return port, nil
}

func strToBytes(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	s = strings.ToUpper(s)
	factors := map[string]int64{"T": TiB, "G": GiB, "M": MiB, "K": KiB, "B": 1}
	for k, v := range factors {
		if idx := strings.Index(s, k); idx != -1 {
			i, err := strconv.ParseInt(strings.TrimSpace(s[:idx]), 10, 64)
			return v * i, err
		}
	}
	i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return i, err
}

func bytesToStr(b int64, digits int) string {
	factors := map[string]int64{"TiB": TiB, "GiB": GiB, "MiB": MiB, "KiB": KiB, "B": 1}

	for s, f := range factors {
		if b >= f {
			return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(f), s)
		}
	}

	return fmt.Sprintf("%dB", b)
}
