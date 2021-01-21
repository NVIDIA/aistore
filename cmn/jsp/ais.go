// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

////////////////
// ais config //
////////////////

func LoadConfig(confPath, daeRole string, config *cmn.Config) (err error) {
	cmn.GCO.SetConfigPath(confPath)
	_, err = Load(confPath, &config, Plain())
	if err != nil {
		return fmt.Errorf("failed to load config %q, err: %v", confPath, err)
	}
	config.SetRole(daeRole)

	if err = cmn.CreateDir(config.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", config.Log.Dir, err)
	}
	glog.SetLogDir(config.Log.Dir)
	if err = config.Validate(); err != nil {
		return
	}

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > cmn.GiB {
		glog.Errorf("log.max_size %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = cmn.MiB
	}

	differentIPs := config.Net.Hostname != config.Net.HostnameIntraControl
	differentPorts := config.Net.L4.Port != config.Net.L4.PortIntraControl
	config.Net.UseIntraControl = config.Net.HostnameIntraControl != "" &&
		config.Net.L4.PortIntraControl != 0 && (differentIPs || differentPorts)

	differentIPs = config.Net.Hostname != config.Net.HostnameIntraData
	differentPorts = config.Net.L4.Port != config.Net.L4.PortIntraData
	config.Net.UseIntraData = config.Net.HostnameIntraData != "" &&
		config.Net.L4.PortIntraData != 0 && (differentIPs || differentPorts)

	if err = cmn.SetLogLevel(config, config.Log.Level); err != nil {
		return fmt.Errorf("failed to set log level = %s, err: %s", config.Log.Level, err)
	}
	glog.Infof("log.dir: %q; l4.proto: %s; port: %d; verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("config_file: %q periodic.stats_time: %v", confPath, config.Periodic.StatsTime)
	return
}

func SetConfig(toUpdate *cmn.ConfigToUpdate, transient bool) error {
	config := cmn.GCO.BeginUpdate()
	err := SetConfigInMem(toUpdate, config)
	if transient || err != nil {
		cmn.GCO.DiscardUpdate()
		return err
	}
	if !transient {
		Save(cmn.GCO.GetConfigPath(), config, Plain())
	}
	cmn.GCO.CommitUpdate(config)
	return nil
}

func SetConfigInMem(toUpdate *cmn.ConfigToUpdate, config *cmn.Config) (err error) {
	config.Apply(*toUpdate)
	if toUpdate.Vmodule != nil {
		if err := cmn.SetGLogVModule(*toUpdate.Vmodule); err != nil {
			return fmt.Errorf("failed to set vmodule = %s, err: %v", *toUpdate.Vmodule, err)
		}
	}
	if toUpdate.LogLevel != nil {
		if err := cmn.SetLogLevel(config, *toUpdate.LogLevel); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", *toUpdate.LogLevel, err)
		}
	}
	err = config.Validate()
	return
}
