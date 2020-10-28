// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

const (
	daemonIDEnv  = "AIS_DAEMON_ID"
	proxyIDFname = ".ais.proxy_id"
)

func initDaemonID(daemonType string, config *cmn.Config) (daemonID string) {
	switch daemonType {
	case cmn.Target:
		daemonID = initTargetDaemonID(config)
	case cmn.Proxy:
		daemonID = initProxyDaemonID(config)
	default:
		cmn.Assertf(false, daemonType)
	}

	cmn.Assert(daemonID != "")
	return daemonID
}

/////////////
/// Proxy ///
/////////////

func initProxyDaemonID(config *cmn.Config) (daemonID string) {
	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		glog.Infof("proxy[%q] daemonID from env", daemonID)
		goto persist
	}

	if daemonID = readProxyDaemonID(config); daemonID != "" {
		glog.Infof("proxy[%q] from %q", daemonID, proxyIDFname)
		return
	}

	daemonID = generateDaemonID(cmn.Proxy, config)
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

func initTargetDaemonID(config *cmn.Config) (daemonID string) {
	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		glog.Infof("target[%q] daemonID from env", daemonID)
		goto persist
	}

	if vmd := fs.ReadVMD(); vmd != nil {
		glog.Infof("target[%q] daemonID from VMD", vmd.DaemonID)
		return vmd.DaemonID
	}

	daemonID = generateDaemonID(cmn.Target, config)
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

func generateDaemonID(daemonType string, config *cmn.Config) string {
	if !config.TestingEnv() {
		return cmn.GenDaemonID()
	}

	switch daemonType {
	case cmn.Target:
		return cmn.RandStringStrong(4) + "t" + config.Net.L4.PortStr
	case cmn.Proxy:
		return cmn.RandStringStrong(4) + "p" + config.Net.L4.PortStr
	}
	cmn.Assertf(false, daemonType)
	return ""
}
