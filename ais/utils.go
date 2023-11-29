// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
	errRangeNoOverlap struct {
		ranges []string // RFC 7233
		size   int64    // [0, size)
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
			// production or K8s: skip loopbacks
			if ipnet.IP.IsLoopback() && (!config.TestingEnv() || k8s.IsK8s()) {
				continue
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

// given configured list of hostnames, return the first one matching local unicast IPv4
func _selectHost(locIPs []*localIPv4Info, hostnames []string) (string, error) {
	msg := fmt.Sprintf("select hostname given: local IPv4 %v, configured %v", locIPs, hostnames)
	nlog.Infoln(msg)

	for i, host := range hostnames {
		var ipv4 string
		if net.ParseIP(host) != nil {
			// parses as IP
			ipv4 = strings.TrimSpace(host)
		} else {
			ip, err := _resolve(host)
			if err != nil {
				nlog.Errorln("failed to resolve hostname(?)", host, "err:", err, "[idx:", i, len(hostnames))
				continue
			}
			ipv4 = ip.String()
			nlog.Infoln("resolved hostname", host, "to IP addr", ipv4)
		}
		for _, addr := range locIPs {
			if addr.ipv4 == ipv4 {
				nlog.Infoln("Selected hostname", host, "IP", ipv4)
				return host, nil
			}
		}
	}

	err := errors.New("failed to " + msg)
	nlog.Errorln(err)
	return "", err
}

// _localIP takes a list of local IPv4s and returns the best fit for a daemon to listen on it
func _localIP(config *cmn.Config, addrList []*localIPv4Info) (ip net.IP, err error) {
	if len(addrList) == 0 {
		return nil, fmt.Errorf("no addresses to choose from")
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
	if config.FastV(4, cos.SmoduleAIS) {
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

// getNetInfo returns hostname to listen on.
// If config doesn't contain hostnames it chooses one of the local IPv4s
func getNetInfo(config *cmn.Config, addrList []*localIPv4Info, proto, configuredIPv4s, port string) (netInfo meta.NetInfo, err error) {
	var (
		ip   net.IP
		host string
	)
	if configuredIPv4s == "" {
		if ip, err = _localIP(config, addrList); err == nil {
			netInfo = *meta.NewNetInfo(proto, ip.String(), port)
		}
		return
	}

	lst := strings.Split(configuredIPv4s, cmn.HostnameListSepa)
	if host, err = _selectHost(addrList, lst); err == nil {
		netInfo = *meta.NewNetInfo(proto, host, port)
	}
	return
}

func _resolve(hostName string) (net.IP, error) {
	ips, err := net.LookupIP(hostName)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("failed to find non-empty IPv4 in list %v (hostName=%q)", ips, hostName)
}

func parseOrResolve(hostname string) (err error) {
	if net.ParseIP(hostname) != nil {
		// is a parse-able IP addr
		return
	}
	_, err = _resolve(hostname)
	return
}

/////////////
// htrange //
/////////////

func (r htrange) contentRange(size int64) string {
	return fmt.Sprintf("%s%d-%d/%d", cos.HdrContentRangeValPrefix, r.Start, r.Start+r.Length-1, size)
}

// ErrNoOverlap is returned by serveContent's parseRange if first-byte-pos of
// all of the byte-range-spec values is greater than the content size.
func (e *errRangeNoOverlap) Error() string {
	msg := fmt.Sprintf("overlap with the content [0, %d)", e.size)
	if len(e.ranges) == 1 {
		return fmt.Sprintf("range %q does not %s", e.ranges[0], msg)
	}
	return fmt.Sprintf("none of the ranges %v %s", e.ranges, msg)
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
		return nil, &errRangeNoOverlap{allRanges, size}
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
	filepath.Walk(config.ConfigDir, func(path string, finfo os.FileInfo, err error) error {
		if strings.HasPrefix(finfo.Name(), ".ais.") {
			if err := cos.RemoveFile(path); err != nil {
				nlog.Errorf("%s: failed to cleanup %q, err: %v", name, path, err)
			}
		}
		return nil
	})
}
