// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction/registry"
)

//
// request validation helpers - TODO: optionally, check node IDs vs Smap
//
func isIntraCall(hdr http.Header) bool { return hdr != nil && hdr.Get(cmn.HeaderCallerID) != "" }
func isIntraPut(hdr http.Header) bool  { return hdr != nil && hdr.Get(cmn.HeaderPutterID) != "" }

func isRedirect(q url.Values) (delta string) {
	if len(q) == 0 || q.Get(cmn.URLParamProxyID) == "" {
		return
	}
	return q.Get(cmn.URLParamUnixTime)
}

func redirectLatency(started time.Time, ptime string) (redelta int64) {
	pts, err := cmn.S2UnixNano(ptime)
	if err != nil {
		glog.Errorf("unexpected: failed to convert %s to int, err: %v", ptime, err)
		return
	}
	redelta = started.UnixNano() - pts
	if redelta < 0 && -redelta < int64(clusterClockDrift) {
		redelta = 0
	}
	return
}

//
// IPV4
//

// Local unicast IP info
type localIPv4Info struct {
	ipv4 string
	mtu  int
}

// getLocalIPv4List returns a list of local unicast IPv4 with MTU
func getLocalIPv4List() (addrlist []*localIPv4Info, err error) {
	addrlist = make([]*localIPv4Info, 0, 4)
	addrs, e := net.InterfaceAddrs()
	if e != nil {
		err = fmt.Errorf("failed to get host unicast IPs, err: %v", e)
		return
	}
	iflist, e := net.Interfaces()
	if e != nil {
		err = fmt.Errorf("failed to get interface list: %v", e)
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
		return addrlist, fmt.Errorf("the host does not have any IPv4 addresses")
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
	return "", fmt.Errorf("configured IPv4 does not match any local one")
}

// detectLocalIPv4 takes a list of local IPv4s and returns the best fit for a daemon to listen on it
func detectLocalIPv4(addrList []*localIPv4Info) (ip net.IP, err error) {
	if len(addrList) == 0 {
		return nil, fmt.Errorf("no addresses to choose from")
	} else if len(addrList) == 1 {
		glog.Infof("Found only one IPv4: %s, MTU %d", addrList[0].ipv4, addrList[0].mtu)
		if addrList[0].mtu <= 1500 {
			glog.Warningf("IPv4 %s MTU size is small: %d\n", addrList[0].ipv4, addrList[0].mtu)
		}
		if ip = net.ParseIP(addrList[0].ipv4); ip == nil {
			return nil, fmt.Errorf("failed to parse IP address: %s", addrList[0].ipv4)
		}
		return ip, nil
	}

	glog.Warningf("%d IPv4s available", len(addrList))
	for _, addr := range addrList {
		glog.Warningf("    %#v\n", *addr)
	}
	if ip = net.ParseIP(addrList[0].ipv4); ip == nil {
		return nil, fmt.Errorf("failed to parse IP address: %s", addrList[0].ipv4)
	}
	return ip, nil
}

// getIPv4 returns an IPv4 for proxy/target to listen on it.
// 1. If there is an IPv4 in config - it tries to use it
// 2. If config does not contain IPv4 - it chooses one of local IPv4s
func getIPv4(addrList []*localIPv4Info, configuredIPv4s string) (ip net.IP, err error) {
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
		return nil, fmt.Errorf("failed to parse ip %s", selectedIPv4)
	}
	return ip, nil
}

/////////////
// HELPERS //
/////////////

func reMirror(bprops, nprops *cmn.BucketProps) bool {
	if !bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return true
	}
	if bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return bprops.Mirror.Copies != nprops.Mirror.Copies
	}
	return false
}

func reEC(bprops, nprops *cmn.BucketProps, bck *cluster.Bck) bool {
	// TODO: xaction to remove all data generated by EC encoder.
	// For now, do nothing if EC is disabled.
	if !nprops.EC.Enabled {
		if bprops.EC.Enabled {
			// kill running ec-encode xact if it is active
			registry.Registry.DoAbort(cmn.ActECEncode, bck)
		}
		return false
	}
	if !bprops.EC.Enabled {
		return true
	}
	return bprops.EC.DataSlices != nprops.EC.DataSlices ||
		bprops.EC.ParitySlices != nprops.EC.ParitySlices
}

func withRetry(cond func() bool) (ok bool) {
	if ok = cond(); !ok {
		time.Sleep(time.Second)
		ok = cond()
	}
	return
}

func isETLRequest(query url.Values) bool {
	return query.Get(cmn.URLParamUUID) != ""
}
