// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

const (
	accessNetPublic = netAccess(1 << iota)
	accessNetIntraControl
	accessNetIntraData

	accessNetPublicControl = accessNetPublic | accessNetIntraControl
	accessNetPublicData    = accessNetPublic | accessNetIntraData
	accessControlData      = accessNetIntraControl | accessNetIntraData
	accessNetAll           = accessNetPublic | accessNetIntraData | accessNetIntraControl
)

// Network access of handlers (Public, IntraControl, & IntraData)
type (
	netAccess int

	// HTTP Range (aka Read Range)
	htrange struct {
		Start, Length int64
	}

	// Local unicast IP info
	localIPv4Info struct {
		ipv4 string
		mtu  int
	}
)

func (na netAccess) isSet(flag netAccess) bool { return na&flag == flag }

//
// IPV4
//

// returns a list of local unicast (IPv4, MTU)
func getLocalIPv4s(config *cmn.Config) (addrlist []*localIPv4Info, err error) {
	addrlist = make([]*localIPv4Info, 0, 4)

	addrs, e := net.InterfaceAddrs()
	if e != nil {
		err = fmt.Errorf("failed to get host unicast IPs: %w", e)
		return
	}
	iflist, e := net.Interfaces()
	if e != nil {
		err = fmt.Errorf("failed to get network interfaces: %w", e)
		return
	}

	for _, addr := range addrs {
		curr := &localIPv4Info{}
		if ipnet, ok := addr.(*net.IPNet); ok {
			if ipnet.IP.IsLoopback() {
				// K8s: always exclude 127.0.0.1 loopback
				if k8s.IsK8s() {
					continue
				}
				// non K8s and fspaths:
				if !config.TestingEnv() {
					if excludeLoopbackIP() {
						if ipnet.IP.To4() != nil {
							nlog.Warningln("(non-K8s, fspaths) deployment: excluding loopback IP:", ipnet.IP)
						}
						continue
					}
				}
			}
			if ipnet.IP.To4() == nil {
				continue
			}
			curr.ipv4 = ipnet.IP.String()
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
		return addrlist, errors.New("the host does not have any IPv4 addresses")
	}
	return addrlist, nil
}

// HACK, to accommodate non-K8s docker deployments and non-containerized
func excludeLoopbackIP() bool {
	if _, present := os.LookupEnv("AIS_LOCAL_PLAYGROUND"); present {
		return false
	}
	return true
}

// given configured list of hostnames, return the first one matching local unicast IPv4
func _selectHost(locIPs []*localIPv4Info, hostnames []string) (string, error) {
	sb := &strings.Builder{}
	sb.WriteByte('[')
	for i, lip := range locIPs {
		sb.WriteString(lip.ipv4)
		sb.WriteString("(MTU=")
		sb.WriteString(strconv.Itoa(lip.mtu))
		sb.WriteByte(')')
		if i < len(locIPs)-1 {
			sb.WriteByte(' ')
		}
	}
	sb.WriteByte(']')
	nlog.Infoln("local IPv4:", sb.String())
	nlog.Infoln("configured:", hostnames)

	for i, host := range hostnames {
		host = strings.TrimSpace(host)
		var ipv4 string
		if net.ParseIP(host) != nil { // parses as IP
			ipv4 = host
		} else {
			ip, err := cmn.Host2IP(host)
			if err != nil {
				nlog.Errorln("failed to resolve hostname(?)", host, "err:", err, "[idx:", i, len(hostnames))
				continue
			}
			ipv4 = ip.String()
			nlog.Infoln("resolved hostname", host, "to IP addr", ipv4)
		}
		for _, addr := range locIPs {
			if addr.ipv4 == ipv4 {
				nlog.Infoln("selected: hostname", host, "IP", ipv4)
				return host, nil
			}
		}
	}

	err := fmt.Errorf("failed to select hostname from: (%s, %v)", sb.String(), hostnames)
	nlog.Errorln(err)
	return "", err
}

// _localIP takes a list of local IPv4s and returns the best fit for a daemon to listen on it
func _localIP(addrList []*localIPv4Info) (ip net.IP, err error) {
	if len(addrList) == 0 {
		return nil, errors.New("no addresses to choose from")
	}
	if len(addrList) == 1 {
		nlog.Infof("Found only one IPv4: %s, MTU %d", addrList[0].ipv4, addrList[0].mtu)
		if addrList[0].mtu <= 1500 {
			nlog.Warningf("IPv4 %s MTU size is small: %d\n", addrList[0].ipv4, addrList[0].mtu)
		}
		if ip = net.ParseIP(addrList[0].ipv4); ip == nil {
			return nil, fmt.Errorf("failed to parse IP address: %s", addrList[0].ipv4)
		}
		return ip, nil
	}
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		nlog.Infof("%d IPv4s:", len(addrList))
		for _, addr := range addrList {
			nlog.Infof("    %#v\n", *addr)
		}
	}
	if ip = net.ParseIP(addrList[0].ipv4); ip == nil {
		return nil, fmt.Errorf("failed to parse IP address: %s", addrList[0].ipv4)
	}
	return ip, nil
}

func multihome(configuredIPv4s string) (pub string, extra []string) {
	if i := strings.IndexByte(configuredIPv4s, cmn.HostnameListSepa[0]); i <= 0 {
		cos.ExitAssertLog(i < 0, "invalid format:", configuredIPv4s)
		return configuredIPv4s, nil
	}

	// trim + validation
	lst := strings.Split(configuredIPv4s, cmn.HostnameListSepa)
	pub, extra = strings.TrimSpace(lst[0]), lst[1:]
	for i := range extra {
		extra[i] = strings.TrimSpace(extra[i])
		cos.ExitAssertLog(extra[i] != "", "invalid format (empty value):", configuredIPv4s)
		cos.ExitAssertLog(extra[i] != pub, "duplicated addr or hostname:", configuredIPv4s)
		for j := range i {
			cos.ExitAssertLog(extra[i] != extra[j], "duplicated addr or hostname:", configuredIPv4s)
		}
	}
	nlog.Infof("multihome: %s and %v", pub, extra)
	return pub, extra
}

// choose one of the local IPv4s if local config doesn't contain (explicitly) specified
func initNetInfo(ni *meta.NetInfo, addrList []*localIPv4Info, proto, configuredIPv4s, port string) (err error) {
	var (
		ip   net.IP
		host string
	)
	if configuredIPv4s == "" {
		if ip, err = _localIP(addrList); err == nil {
			ni.Init(proto, ip.String(), port)
		}
		return
	}

	lst := strings.Split(configuredIPv4s, cmn.HostnameListSepa)
	if host, err = _selectHost(addrList, lst); err == nil {
		ni.Init(proto, host, port)
	}
	return
}

/////////////
// htrange //
/////////////

// (compare w/ cmn.MakeRangeHdr)
func (r htrange) contentRange(size int64) string {
	return fmt.Sprintf("%s%d-%d/%d", cos.HdrContentRangeValPrefix, r.Start, r.Start+r.Length-1, size)
}

// ParseMultiRange parses a Range Header string as per RFC 7233.
// ErrNoOverlap is returned if none of the ranges overlap with the [0, size) content.
func parseMultiRange(s string, size int64) (ranges []htrange, err error) {
	var noOverlap bool
	if !strings.HasPrefix(s, cos.HdrRangeValPrefix) {
		return nil, fmt.Errorf("read range %q is invalid (prefix)", s)
	}
	allRanges := strings.Split(s[len(cos.HdrRangeValPrefix):], ",")
	for _, ra := range allRanges {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, fmt.Errorf("read range %q is invalid (-)", s)
		}
		var (
			r     htrange
			start = strings.TrimSpace(ra[:i])
			end   = strings.TrimSpace(ra[i+1:])
		)
		if start == "" {
			// If no start is specified, end specifies the range start relative
			// to the end of the file, and we are dealing with <suffix-length>
			// which has to be a non-negative integer as per RFC 7233 Section 2.1 "Byte-Ranges".
			if end == "" || end[0] == '-' {
				return nil, fmt.Errorf("read range %q is invalid as per RFC 7233 Section 2.1", ra)
			}
			i, err := strconv.ParseInt(end, 10, 64)
			if i < 0 || err != nil {
				return nil, fmt.Errorf("read range %q is invalid (see RFC 7233 Section 2.1)", ra)
			}
			if i > size {
				i = size
			}
			r.Start = size - i
			r.Length = size - r.Start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, fmt.Errorf("read range %q is invalid (start)", ra)
			}
			if i >= size {
				// If the range begins after the size of the content it does not overlap.
				noOverlap = true
				continue
			}
			r.Start = i
			if end == "" {
				// If no end is specified, range extends to the end of the file.
				r.Length = size - r.Start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.Start > i {
					return nil, fmt.Errorf("read range %q is invalid (end)", ra)
				}
				if i >= size {
					i = size - 1
				}
				r.Length = i - r.Start + 1
			}
		}
		ranges = append(ranges, r)
	}

	if noOverlap && len(ranges) == 0 {
		return nil, cmn.NewErrRangeNotSatisfiable(nil, allRanges, size)
	}
	return ranges, nil
}

//
// misc. helpers
//

func deploymentType() string {
	switch {
	case k8s.IsK8s():
		return apc.DeploymentK8s
	case cmn.GCO.Get().TestingEnv():
		return apc.DeploymentDev
	default:
		return runtime.GOOS
	}
}

// for AIS metadata filenames (constants), see `cmn/fname` package
func cleanupConfigDir(name string, keepInitialConfig bool) {
	if !keepInitialConfig {
		// remove plain-text (initial) config
		cos.RemoveFile(daemon.cli.globalConfigPath)
		cos.RemoveFile(daemon.cli.localConfigPath)
	}
	config := cmn.GCO.Get()
	filepath.Walk(config.ConfigDir, func(path string, finfo os.FileInfo, _ error) error {
		if strings.HasPrefix(finfo.Name(), ".ais.") {
			if err := cos.RemoveFile(path); err != nil {
				nlog.Errorf("%s: failed to cleanup %q, err: %v", name, path, err)
			}
		}
		return nil
	})
}

//
// common APPEND(file(s)) pre-parser
//

const appendHandleSepa = "|"

func preParse(packedHdl string) (items []string, err error) {
	items = strings.SplitN(packedHdl, appendHandleSepa, 4)
	if len(items) != 4 {
		err = fmt.Errorf("invalid APPEND handle: %q", packedHdl)
	}
	return
}
