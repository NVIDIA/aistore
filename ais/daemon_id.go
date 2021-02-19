// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
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
	if daemon.cli.daemonID != "" {
		return daemon.cli.daemonID
	}

	if daemonID = os.Getenv(daemonIDEnv); daemonID != "" {
		glog.Infof("%s[%q] daemonID from env", daemonType, daemonID)
		return
	}

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
	if daemonID = readProxyDaemonID(config); daemonID != "" {
		glog.Infof("proxy[%q] from %q", daemonID, proxyIDFname)
		return
	}

	daemonID = generateDaemonID(cmn.Proxy, config)
	glog.Infof("proxy[%q] daemonID randomly generated", daemonID)
	return daemonID
}

func writeProxyDID(config *cmn.Config, id string) error {
	return ioutil.WriteFile(filepath.Join(config.Confdir, proxyIDFname), []byte(id), cmn.PermRWR)
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
	var err error
	if daemonID, err = fs.LoadDaemonID(config.FSpaths.Paths); err != nil {
		cmn.ExitLogf("%v", err)
	}

	if daemonID != "" {
		return
	}

	daemonID = generateDaemonID(cmn.Target, config)
	glog.Infof("target[%q] daemonID randomly generated", daemonID)
	return daemonID
}

///////////////
/// common ///
//////////////

func generateDaemonID(daemonType string, config *cmn.Config) string {
	if !config.TestingEnv() {
		return cmn.GenDaemonID()
	}

	daemonID := cmn.RandStringStrong(4)
	switch daemonType {
	case cmn.Target:
		return fmt.Sprintf("%st%d", daemonID, config.Net.L4.Port)
	case cmn.Proxy:
		return fmt.Sprintf("%sp%d", daemonID, config.Net.L4.Port)
	}
	cmn.Assertf(false, daemonType)
	return ""
}
