// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/OneOfOne/xxhash"
)

const (
	daemonIDEnv  = "AIS_DAEMON_ID"
	proxyIDFname = ".ais.proxy_id"
)

func initDaemonID(daemonType string, config *cmn.Config, publicAddr *net.TCPAddr) (daemonID string) {
	switch daemonType {
	case cmn.Target:
		daemonID = initTargetDaemonID(config, publicAddr)
	case cmn.Proxy:
		daemonID = initProxyDaemonID(config, publicAddr)
	default:
		cmn.Assertf(false, daemonType)
	}

	cmn.Assert(daemonID != "")
	return daemonID
}

/////////////
/// Proxy ///
/////////////

func initProxyDaemonID(config *cmn.Config, publicAddr *net.TCPAddr) (daemonID string) {
	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		glog.Infof("proxy[%q] daemonID from env", daemonID)
		goto persist
	}

	if daemonID = readProxyDaemonID(config); daemonID != "" {
		glog.Infof("proxy[%q] from %q", daemonID, proxyIDFname)
		return
	}

	daemonID = generateDaemonID(cmn.Proxy, config, publicAddr)
	glog.Infof("proxy[%q] daemonID randomly generated", daemonID)

persist:
	if err := writeProxyDID(config, daemonID); err != nil {
		glog.Fatal(err)
	}

	return daemonID
}

func writeProxyDID(config *cmn.Config, id string) error {
	return ioutil.WriteFile(filepath.Join(config.Confdir, proxyIDFname), []byte(id), 0o644)
}

func readProxyDaemonID(config *cmn.Config) string {
	if b, err := ioutil.ReadFile(filepath.Join(config.Confdir, proxyIDFname)); err == nil {
		return string(b)
	} else if !os.IsNotExist(err) {
		glog.Error(err)
	}
	return ""
}

//////////////
/// Target ///
//////////////

func initTargetDaemonID(config *cmn.Config, publicAddr *net.TCPAddr) (daemonID string) {
	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		glog.Infof("target[%q] daemonID from env", daemonID)
		goto persist
	}

	if err, vmd := fs.ReadVMD(); vmd != nil {
		glog.Infof("target[%q] daemonID from VMD", vmd.DaemonID)
		return vmd.DaemonID
	} else if err != nil {
		cmn.ExitLogf("%v", err)
	}

	daemonID = generateDaemonID(cmn.Target, config, publicAddr)
	glog.Infof("target[%q] daemonID randomly generated", daemonID)

persist:
	if err := fs.CreateVMD(daemonID).Persist(); err != nil {
		glog.Fatal(err)
	}

	return daemonID
}

///////////////
/// common ///
//////////////

func generateDaemonID(daemonType string, config *cmn.Config, publicAddr *net.TCPAddr) string {
	if !config.TestingEnv() {
		return cmn.GenDaemonID()
	}

	cs := xxhash.ChecksumString32S(publicAddr.String(), cmn.MLCG32)
	daemonID := strconv.Itoa(int(cs & 0xfffff))
	switch daemonType {
	case cmn.Target:
		return daemonID + "t" + config.Net.L4.PortStr
	case cmn.Proxy:
		return daemonID + "p" + config.Net.L4.PortStr
	}
	cmn.Assertf(false, daemonType)
	return ""
}
