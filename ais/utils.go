// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
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

const fmtErrParseIP = "failed to parse local unicast IP address: %s"

// Network access of handlers (Public, IntraControl, & IntraData)
type (
	netAccess int

	// HTTP Range (aka Read Range)
	htrange struct {
		Start, Length int64
	}

	// local unicast (IP, MTU)
	localIPInfo struct {
		ip  string
		mtu int
	}
)

func (na netAccess) isSet(flag netAccess) bool { return na&flag == flag }

func (addr *localIPInfo) String() string {
	return fmt.Sprintf("IP: %s (MTU %d)", addr.ip, addr.mtu)
}

func (addr *localIPInfo) warn() {
	if addr.mtu <= 1500 {
		nlog.Warningln("Warning: small MTU")
	}
}

//
// local IP discovery + selection
//

// returns a list of local unicast IPs (and their MTU)
func getLocalIPs(config *cmn.Config, useIPv6 bool) ([]*localIPInfo, error) {
	if useIPv6 {
		return _getLocalIPv6s(config)
	}
	return _getLocalIPv4s(config)
}

//
// IPv6
//

// returns a list of local unicast (IPv6, MTU)
//
// Notes:
// - exclude link-local (fe80::/10) - non-rountable, requiring zone ID
// - loopback handling matches IPv4 path:
//   - exclude in K8s
//   - in non-K8s: exclude unless local playground
func _getLocalIPv6s(config *cmn.Config) ([]*localIPInfo, error) {
	iflist, ei := net.Interfaces()
	if ei != nil {
		return nil, fmt.Errorf("failed to get network interfaces: %w", ei)
	}

	addrlist := make([]*localIPInfo, 0, 4)
	for _, intf := range iflist {
		ifAddrs, e := intf.Addrs()
		// skip invalid interfaces
		if e != nil {
			continue
		}

		for _, ifAddr := range ifAddrs {
			ipnet, ok := ifAddr.(*net.IPNet)
			if !ok || ipnet.IP == nil {
				continue
			}
			ip := ipnet.IP
			if ip.To4() != nil {
				continue
			}
			if ip = ip.To16(); ip == nil {
				continue
			}

			if ip.IsLoopback() {
				// K8s: always exclude loopback
				if k8s.IsK8s() {
					continue
				}
				// non K8s and fspaths:
				if !config.TestingEnv() {
					if excludeLoopbackIP() {
						nlog.Warningln("(non-K8s, fspaths) deployment: excluding loopback IP:", ip)
						continue
					}
				}
			}

			// avoid zone-id requirements in advertised/dialed addresses
			if ip.IsLinkLocalUnicast() {
				continue
			}
			if ip.IsUnspecified() || ip.IsMulticast() {
				continue
			}

			addrlist = append(addrlist, &localIPInfo{ip: ip.String(), mtu: intf.MTU})
		}
	}
	if len(addrlist) == 0 {
		return nil, errors.New("the host does not have any IPv6 addresses")
	}
	return addrlist, nil
}

//
// IPv4
//

// returns a list of local unicast (IPv4, MTU)
func _getLocalIPv4s(config *cmn.Config) ([]*localIPInfo, error) {
	addrs, ea := net.InterfaceAddrs()
	if ea != nil {
		return nil, fmt.Errorf("failed to get host unicast IPs: %w", ea)
	}
	iflist, ei := net.Interfaces()
	if ei != nil {
		return nil, fmt.Errorf("failed to get network interfaces: %w", ei)
	}

	addrlist := make([]*localIPInfo, 0, 4)
	for _, addr := range addrs {
		curr := &localIPInfo{}
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
			curr.ip = ipnet.IP.String()
		}

		for _, intf := range iflist {
			ifAddrs, e := intf.Addrs()
			// skip invalid interfaces
			if e != nil {
				continue
			}
			for _, ifAddr := range ifAddrs {
				if ipnet, ok := ifAddr.(*net.IPNet); ok && ipnet.IP.To4() != nil && ipnet.IP.String() == curr.ip {
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
		return nil, errors.New("the host does not have any IPv4 addresses")
	}
	return addrlist, nil
}

// HACK to accommodate non-K8s (docker and non-containerized) deployments
// TODO -- FIXME: review IPv6-wise, and maybe remove
func excludeLoopbackIP() bool {
	if _, present := os.LookupEnv("AIS_LOCAL_PLAYGROUND"); present {
		return false
	}
	return true
}

func _stripBrackets(host string) string {
	host = strings.TrimSpace(host)
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		return host[1 : len(host)-1]
	}
	return host
}

func _isIPv6(ip net.IP) bool {
	return ip != nil && ip.To16() != nil && ip.To4() == nil
}

func _isIPv4(ip net.IP) bool {
	return ip != nil && ip.To4() != nil
}

// given configured list of hostnames, return the first one matching local unicast IP
// (IPv4 or IPv6, as prescribed)
func _selectHost(locIPs []*localIPInfo, hostnames []string, useIPv6 bool) (string, error) {
	var (
		sb cos.SB
		n  = len(locIPs)
		l  = 2 + 31*n
	)
	sb.Init(l)
	sb.WriteUint8('[')
	for i, lip := range locIPs {
		sb.WriteString(lip.ip)
		sb.WriteString("(MTU=")
		sb.WriteString(strconv.Itoa(lip.mtu))
		sb.WriteUint8(')')
		if i < n-1 {
			sb.WriteUint8(' ')
		}
	}
	sb.WriteUint8(']')

	sips := sb.String()
	if useIPv6 {
		nlog.Infoln("local IPv6:", sips)
	} else {
		nlog.Infoln("local IPv4:", sips)
	}
	nlog.Infoln("configured:", hostnames)

	for i, host := range hostnames {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}

		// allow bracketed literals: [2001:db8::1]
		hostNoBr := _stripBrackets(host)

		var ipstr string
		if pip := net.ParseIP(hostNoBr); pip != nil { // parses as IP literal
			if useIPv6 != _isIPv6(pip) {
				continue
			}
			ipstr = pip.String()
		} else {
			ip, err := cmn.Host2IP(hostNoBr, true /*local*/)
			if err != nil {
				nlog.Errorln("failed to resolve hostname(?)", hostNoBr, "err:", err, "[idx:", i, len(hostnames))
				continue
			}
			if useIPv6 != _isIPv6(ip) {
				continue
			}
			ipstr = ip.String()
			nlog.Infoln("resolved hostname", hostNoBr, "to IP addr", ipstr)
		}

		for _, addr := range locIPs {
			if addr.ip == ipstr {
				nlog.Infoln("selected: hostname", host, "IP", ipstr)
				return host, nil
			}
		}
	}

	err := fmt.Errorf("failed to select hostname from: (%s, %v)", sips, hostnames)
	nlog.Errorln(err)
	return "", err
}

// given a list of local IP addresses return the best fit to listen on
func _localIP(addrList []*localIPInfo, useIPv6 bool) (ip net.IP, _ error) {
	l := len(addrList)
	if l == 0 {
		return nil, errors.New("no unicast addresses to choose from")
	}

	if l == 1 {
		if ip = net.ParseIP(addrList[0].ip); ip == nil {
			return nil, fmt.Errorf(fmtErrParseIP, addrList[0].ip)
		}
		nlog.Infoln("Found a single", addrList[0].String())
		addrList[0].warn()
		return ip, nil
	}

	// NOTE:
	// - try using env-based CIDR disambiguation for local IP selection
	// - env.AisPubCIDR = "AIS_PUBLIC_IP_CIDR" takes precedence
	// - applies to both IPv4 and IPv6
	var (
		selected     = -1
		parsed       net.IP
		network, err = _parseCIDR(env.AisLocalRedirectCIDR, env.AisPubCIDR)
	)
	if err != nil {
		return nil, err
	}
	if network == nil {
		goto warn // ------>
	}
	for j := range l {
		if ip = net.ParseIP(addrList[j].ip); ip == nil {
			return nil, fmt.Errorf(fmtErrParseIP, addrList[0].ip)
		}
		// enforce family
		if useIPv6 && !_isIPv6(ip) {
			continue
		}
		if !useIPv6 && !_isIPv4(ip) {
			continue
		}

		if network.Contains(ip) {
			if selected >= 0 {
				return nil, fmt.Errorf("CIDR network %s contains multiple local unicast IPs: %s and %s",
					network, addrList[selected].ip, addrList[j].ip)
			}
			selected, parsed = j, ip
		}
	}
	if selected < 0 {
		nlog.Warningln("CIDR network", network.String(), "does not contain any local unicast IPs")
		goto warn
	}
	nlog.Infoln("CIDR network", network.String(), "contains a single local unicast IP:", addrList[selected].ip)
	addrList[selected].warn()
	return parsed, nil

warn:
	// TODO:
	// to reduce ambiguity
	// parse `config.Proxy.PrimaryURL` for network that must further _contain_ IP to select
	// from multiple `addrList` entries

	tag := "the first"
	selected = 0
	if ip = net.ParseIP(addrList[0].ip); ip == nil {
		return nil, fmt.Errorf(fmtErrParseIP, addrList[0].ip)
	}
	// local playground and multiple choice with no IPs configured: insist on selecting loopback
	if !ip.IsLoopback() && cmn.Rom.TestingEnv() && l > 1 {
		for j := 1; j < l; j++ {
			if ip1 := net.ParseIP(addrList[j].ip); ip1 != nil && ip1.IsLoopback() {
				selected, ip = j, ip1
				tag = "loopback"
				break
			}
		}
	}
	nlog.Warningln("given multiple choice, selecting", tag, addrList[selected].String())
	addrList[selected].warn()

	return ip, nil
}

func _parseCIDR(name, name2 string) (*net.IPNet, error) {
	cidr := os.Getenv(name)
	if name2 != "" {
		if mask := os.Getenv(name2); mask != "" {
			cidr, name = mask, name2
		}
	}
	if cidr == "" {
		return nil, nil
	}
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid '%s=%s': %w", name, cidr, err)
	}
	return network, nil
}

func multihome(configuredAddrs string) (pub string, extra []string) {
	if i := strings.IndexByte(configuredAddrs, cmn.HostnameListSepa[0]); i <= 0 {
		cos.ExitAssertLog(i < 0, "invalid format:", configuredAddrs)
		return configuredAddrs, nil
	}

	// trim + validation
	lst := strings.Split(configuredAddrs, cmn.HostnameListSepa)
	pub, extra = strings.TrimSpace(lst[0]), lst[1:]
	for i := range extra {
		extra[i] = strings.TrimSpace(extra[i])
		cos.ExitAssertLog(extra[i] != "", "invalid format (empty value):", configuredAddrs)
		cos.ExitAssertLog(extra[i] != pub, "duplicated addr or hostname:", configuredAddrs)
		for j := range i {
			cos.ExitAssertLog(extra[i] != extra[j], "duplicated addr or hostname:", configuredAddrs)
		}
	}
	nlog.Infof("multihome: %s and %v", pub, extra)
	return pub, extra
}

// choose one of the local IPs if local config doesn't contain (explicitly) specified
func initNetInfo(ni *meta.NetInfo, addrList []*localIPInfo, proto, configuredAddrs, port string, useIPv6 bool) (err error) {
	var (
		ip   net.IP
		host string
	)
	if configuredAddrs == "" {
		if ip, err = _localIP(addrList, useIPv6); err == nil {
			ni.Init(proto, ip.String(), port)
		}
		return
	}

	lst := strings.Split(configuredAddrs, cmn.HostnameListSepa)
	if host, err = _selectHost(addrList, lst, useIPv6); err == nil {
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
		start, end, ok := strings.Cut(ra, "-")
		if !ok {
			return nil, fmt.Errorf("read range %q is invalid (-)", s)
		}
		start = strings.TrimSpace(start)
		end = strings.TrimSpace(end)

		var (
			r htrange
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
	filepath.WalkDir(config.ConfigDir, func(path string, finfo os.DirEntry, _ error) error {
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

//
// conditionally empty vlabs
//

func bvlabs(bck *meta.Bck) map[string]string {
	if cmn.Rom.Features().IsSet(feat.EnableDetailedPromMetrics) {
		return map[string]string{stats.VlabBucket: bck.Cname("")}
	}
	return stats.EmptyBckVlabs
}

func xvlabs(bck *meta.Bck) map[string]string {
	if cmn.Rom.Features().IsSet(feat.EnableDetailedPromMetrics) {
		return map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: ""}
	}
	return stats.EmptyBckXlabs
}

//
// intra-cluster times and durations (base36)
//

func unixNano2S(unixnano int64) string   { return strconv.FormatInt(unixnano, 36) }
func s2UnixNano(s string) (int64, error) { return strconv.ParseInt(s, 36, 64) }
