// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/golang/glog"
)

const (
	maxAttrSize = 1024
	// use extra algorithms to choose a local IPv4 if there are more than 1 available
	guessTheBestIPv4 = false
)

// Local unicast IP info
type localIPv4Info struct {
	ipv4  string
	mtu   int
	speed int
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

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func divCeil(a, b int64) int64 {
	d, r := a/b, a%b
	if r > 0 {
		return d + 1
	}
	return d
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
func getLocalIPv4List() (addrlist []*localIPv4Info, err error) {
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
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
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
					curr.speed = netifSpeed(intf.Name)
					addrlist = append(addrlist, curr)
					break
				}
			}
			if curr.mtu != 0 {
				break
			}
		}
	}

	return addrlist, nil
}

// selectConfiguredIPv4 returns the first IPv4 from a preconfigured IPv4 list that
// matches any local unicast IPv4
func selectConfiguredIPv4(addrlist []*localIPv4Info, configuredIPv4s string) (ipv4addr string, errstr string) {
	glog.Infof("Selecting one of the configured IPv4 addresses: %s...\n", configuredIPv4s)
	localList := ""
	configuredList := strings.Split(configuredIPv4s, ",")
	for _, localaddr := range addrlist {
		localList += " " + localaddr.ipv4
		for _, ipv4 := range configuredList {
			if localaddr.ipv4 == strings.TrimSpace(ipv4) {
				glog.Warningf("Selected IPv4 %s from the configuration file\n", ipv4)
				return ipv4, ""
			}
		}
	}

	glog.Errorf("Configured IPv4 does not match any local one.\nLocal IPv4 list:%s\n", localList)
	return "", "Configured IPv4 does not match any local one"
}

func guessIPv4(addrlist []*localIPv4Info) (ipv4addr string, errstr string) {
	glog.Warning("Looking for the fastest network interface")
	// sort addresses in descendent order by interface speed
	ifLess := func(i, j int) bool {
		return addrlist[i].speed >= addrlist[j].speed
	}
	sort.Slice(addrlist, ifLess)

	// Take the first IPv4 if it is faster than the others
	if addrlist[0].speed != addrlist[1].speed {
		glog.Warningf("Interface %s is the fastest - %dMbit\n",
			addrlist[0].ipv4, addrlist[0].speed)
		if addrlist[0].mtu <= 1500 {
			glog.Warningf("IPv4 %s selected but MTU is low: %s\n", addrlist[0].mtu)
		}
		ipv4addr = addrlist[0].ipv4
		return
	}

	errstr = fmt.Sprintf("Failed to select one IPv4 of %d available\n", len(addrlist))
	return
}

// detectLocalIPv4 takes a list of local IPv4s and returns the best fit for a deamon to listen on it
func detectLocalIPv4(addrlist []*localIPv4Info) (ipv4addr string, errstr string) {
	if len(addrlist) == 1 {
		msg := fmt.Sprintf("Found only one IPv4: %s, MTU %d", addrlist[0].ipv4, addrlist[0].mtu)
		if addrlist[0].speed != 0 {
			msg += fmt.Sprintf(", bandwidth %d", addrlist[0].speed)
		}
		glog.Info(msg)
		if addrlist[0].mtu <= 1500 {
			glog.Warningf("IPv4 %s MTU size is small: %d\n", addrlist[0].ipv4, addrlist[0].mtu)
		}
		ipv4addr = addrlist[0].ipv4
		return
	}

	glog.Warningf("Warning: %d IPv4s available", len(addrlist))
	for _, intf := range addrlist {
		glog.Warningf("    %#v\n", *intf)
	}
	// FIXME: temp hack - make sure to keep working on laptops with dockers
	ipv4addr = addrlist[0].ipv4
	return
	/*
		if guessTheBestIPv4 {
			return guessIPv4(addrlist)
		}
		return "", "Failed to select network interface: more than one IPv4 available"
	*/
}

// getipv4addr returns an IPv4 for proxy/target to listen on it.
// 1. If there is an IPv4 in config - it tries to use it
// 2. If config does not contain IPv4 - it chooses one of local IPv4s
func getipv4addr() (ipv4addr string, errstr string) {
	addrlist, err := getLocalIPv4List()
	if err != nil {
		errstr = err.Error()
		return
	}
	if len(addrlist) == 0 {
		errstr = "The host does not have any IPv4 addresses"
		return
	}

	if ctx.config.Net.IPv4 != "" {
		return selectConfiguredIPv4(addrlist, ctx.config.Net.IPv4)
	}

	return detectLocalIPv4(addrlist)
}

func CreateDir(dirname string) (err error) {
	if _, err := os.Stat(dirname); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dirname, 0755); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return
}

func ReceiveAndChecksum(filewriter io.Writer, rrbody io.Reader,
	buf []byte, hashes ...hash.Hash) (written int64, errstr string) {
	var (
		writer io.Writer
		err    error
	)
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
		return written, err.Error()
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

func ComputeMD5(reader io.Reader, buf []byte, md5 hash.Hash) (csum string, errstr string) {
	var err error
	if buf == nil {
		_, err = io.Copy(md5.(io.Writer), reader)
	} else {
		_, err = io.CopyBuffer(md5.(io.Writer), reader, buf)
	}
	if err != nil {
		return "", fmt.Sprintf("Failed to copy buffer, err: %v", err)
	}
	hashInBytes := md5.Sum(nil)[:16]
	csum = hex.EncodeToString(hashInBytes)
	return csum, ""
}

func ComputeXXHash(reader io.Reader, buf []byte, xx hash.Hash64) (csum string, errstr string) {
	var err error
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
// dummy io.Writer & ReadToNull() helper
//
//===========================================================================
func ReadToNull(r io.Reader) (int64, error) {
	return io.Copy(ioutil.Discard, r)
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
// local (config) save and restore
//
//===========================================================================
func LocalSave(pathname string, v interface{}) error {
	tmp := pathname + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
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
	err = json.NewDecoder(file).Decode(v)
	_ = file.Close()
	return
}

func osRemove(prefix, fqn string) error {
	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("%s: removed %q", prefix, fqn)
	return nil
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

func parsebool(s string) (value bool, err error) {
	if s == "" {
		return
	}
	value, err = strconv.ParseBool(s)
	return
}
