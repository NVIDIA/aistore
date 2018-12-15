// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
)

//===========================================================================
//
// IPV4
//
//===========================================================================

// Local unicast IP info
type localIPv4Info struct {
	ipv4 string
	mtu  int
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

func copyFile(fromFQN, toFQN string) (fqnErr string, err error) {
	fileIn, err := os.Open(fromFQN)
	if err != nil {
		glog.Errorf("Failed to open source %s: %v", fromFQN, err)
		return fromFQN, err
	}
	defer fileIn.Close()

	fileOut, err := os.Create(toFQN)
	if err != nil {
		glog.Errorf("Failed to open destination %s: %v", toFQN, err)
		return toFQN, err
	}
	defer fileOut.Close()

	buf, slab := gmem2.AllocFromSlab2(cmn.MiB)
	defer slab.Free(buf)

	if _, err = io.CopyBuffer(fileOut, fileIn, buf); err != nil {
		glog.Errorf("Failed to copy %s -> %s: %v", fromFQN, toFQN, err)
		return toFQN, err
	}

	return "", nil
}

// query-able xactions
func validateXactionQueryable(kind string) (errstr string) {
	if kind == cmn.XactionRebalance || kind == cmn.XactionPrefetch {
		return
	}
	return fmt.Sprintf("Invalid xaction '%s', expecting one of [%s, %s]", kind, cmn.XactionRebalance, cmn.XactionPrefetch)
}

// versioningConfigured returns true if versioning for a given bucket is enabled
// NOTE:
//    AWS bucket versioning can be disabled on the cloud. In this case we do not
//    save/read/update version using xattrs. And the function returns that the
//    versioning is unsupported even if versioning is 'all' or 'cloud'.
func versioningConfigured(islocal bool) bool {
	versioning := cmn.GCO.Get().Ver.Versioning
	if islocal {
		return versioning == cmn.VersionAll || versioning == cmn.VersionLocal
	}
	return versioning == cmn.VersionAll || versioning == cmn.VersionCloud
}
