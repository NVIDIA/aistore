// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

	// Local unicast IP info
	localIPv4Info struct {
		ipv4 string
		mtu  int
	}
)

func (na netAccess) isSet(flag netAccess) bool { return na&flag == flag }

func (addr *localIPv4Info) String() string {
	return fmt.Sprintf("IP: %s (MTU %d)", addr.ipv4, addr.mtu)
}

func (addr *localIPv4Info) warn() {
	if addr.mtu <= 1500 {
		nlog.Warningln("Warning: small MTU")
	}
}

//
// IPV4
//

// returns a list of local unicast (IPv4, MTU)
func getLocalIPv4s(config *cmn.Config) ([]*localIPv4Info, error) {
	addrs, ea := net.InterfaceAddrs()
	if ea != nil {
		return nil, fmt.Errorf("failed to get host unicast IPs: %w", ea)
	}
	iflist, ei := net.Interfaces()
	if ei != nil {
		return nil, fmt.Errorf("failed to get network interfaces: %w", ei)
	}

	addrlist := make([]*localIPv4Info, 0, 4)
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
		return nil, errors.New("the host does not have any IPv4 addresses")
	}
	return addrlist, nil
}

// HACK to accommodate non-K8s (docker and non-containerized) deployments
func excludeLoopbackIP() bool {
	if _, present := os.LookupEnv("AIS_LOCAL_PLAYGROUND"); present {
		return false
	}
	return true
}

// given configured list of hostnames, return the first one matching local unicast IPv4
func _selectHost(locIPs []*localIPv4Info, hostnames []string) (string, error) {
	var (
		sb strings.Builder
		n  = len(locIPs)
		l  = 2 + 31*n
	)
	sb.Grow(l)
	sb.WriteByte('[')
	for i, lip := range locIPs {
		sb.WriteString(lip.ipv4)
		sb.WriteString("(MTU=")
		sb.WriteString(strconv.Itoa(lip.mtu))
		sb.WriteByte(')')
		if i < n-1 {
			sb.WriteByte(' ')
		}
	}
	sb.WriteByte(']')

	sips := sb.String()
	nlog.Infoln("local IPv4:", sips)
	nlog.Infoln("configured:", hostnames)

	for i, host := range hostnames {
		host = strings.TrimSpace(host)
		var ipv4 string
		if net.ParseIP(host) != nil { // parses as IP
			ipv4 = host
		} else {
			ip, err := cmn.Host2IP(host, true /*local*/)
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

	err := fmt.Errorf("failed to select hostname from: (%s, %v)", sips, hostnames)
	nlog.Errorln(err)
	return "", err
}

// given a list of local IPv4s return the best fit to listen on
func _localIP(addrList []*localIPv4Info) (ip net.IP, _ error) {
	l := len(addrList)
	if l == 0 {
		return nil, errors.New("no unicast addresses to choose from")
	}

	if l == 1 {
		if ip = net.ParseIP(addrList[0].ipv4); ip == nil {
			return nil, fmt.Errorf(fmtErrParseIP, addrList[0].ipv4)
		}
		nlog.Infoln("Found a single", addrList[0].String())
		addrList[0].warn()
		return ip, nil
	}

	// NOTE:
	// - try using environment to eliminate ambiguity
	// - env.AisPubIPv4CIDR ("AIS_PUBLIC_IP_CIDR") takes precedence
	var (
		selected     = -1
		parsed       net.IP
		network, err = _parseCIDR(env.AisLocalRedirectCIDR, env.AisPubIPv4CIDR)
	)
	if err != nil {
		return nil, err
	}
	if network == nil {
		goto warn // ------>
	}
	for j := range l {
		if ip = net.ParseIP(addrList[j].ipv4); ip == nil {
			return nil, fmt.Errorf(fmtErrParseIP, addrList[0].ipv4)
		}
		if network.Contains(ip) {
			if selected >= 0 {
				return nil, fmt.Errorf("CIDR network %s contains multiple local unicast IPs: %s and %s",
					network, addrList[selected].ipv4, addrList[j].ipv4)
			}
			selected, parsed = j, ip
		}
	}
	if selected < 0 {
		nlog.Warningln("CIDR network", network.String(), "does not contain any local unicast IPs")
		goto warn
	}
	nlog.Infoln("CIDR network", network.String(), "contains a single local unicast IP:", addrList[selected].ipv4)
	addrList[selected].warn()
	return parsed, nil

warn:
	// TODO:
	// to reduce ambiguity
	// parse `config.Proxy.PrimaryURL` for network that must further _contain_ IP to select
	// from multiple `addrList` entries

	tag := "the first"
	selected = 0
	if ip = net.ParseIP(addrList[0].ipv4); ip == nil {
		return nil, fmt.Errorf(fmtErrParseIP, addrList[0].ipv4)
	}
	// local playground and multiple choice with no IPs configured: insist on selecting loopback
	if !ip.IsLoopback() && cmn.Rom.TestingEnv() && l > 1 {
		for j := 1; j < l; j++ {
			if ip1 := net.ParseIP(addrList[j].ipv4); ip1 != nil && ip1.IsLoopback() {
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
// override intra-cluster (target-to-target) SendFile timeout
//

func sendFileTimeout(config *cmn.Config, size int64, archived bool) time.Duration {
	debug.Assert(config != nil)
	tout := config.Timeout.SendFile.D()
	if size > cos.GiB || size < 0 || archived { // (heuristic)
		return tout
	}
	return min(tout, time.Minute)
}
