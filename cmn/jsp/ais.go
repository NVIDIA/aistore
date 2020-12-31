// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

////////////////
// ais config //
////////////////

func MustLoadConfig(confPath string) {
	if err := LoadConfig(confPath); err != nil {
		cmn.ExitLogf("%v", err)
	}
}

func LoadConfig(confPath string) (err error) {
	cmn.GCO.SetConfigPath(confPath)

	config := cmn.GCO.BeginUpdate()
	defer cmn.GCO.CommitUpdate(config)

	_, err = Load(confPath, &config, Options{})
	if err != nil {
		return fmt.Errorf("failed to load config %q, err: %v", confPath, err)
	}
	if err = cmn.CreateDir(config.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", config.Log.Dir, err)
	}
	glog.SetLogDir(config.Log.Dir)
	if err := config.Validate(); err != nil {
		return err
	}

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > cmn.GiB {
		glog.Errorf("log.max_size %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = cmn.MiB
	}

	differentIPs := config.Net.IPv4 != config.Net.IPv4IntraControl
	differentPorts := config.Net.L4.Port != config.Net.L4.PortIntraControl
	config.Net.UseIntraControl = config.Net.IPv4IntraControl != "" &&
		config.Net.L4.PortIntraControl != 0 && (differentIPs || differentPorts)

	differentIPs = config.Net.IPv4 != config.Net.IPv4IntraData
	differentPorts = config.Net.L4.Port != config.Net.L4.PortIntraData
	config.Net.UseIntraData = config.Net.IPv4IntraData != "" &&
		config.Net.L4.PortIntraData != 0 && (differentIPs || differentPorts)

	if err = cmn.SetLogLevel(config, config.Log.Level); err != nil {
		return fmt.Errorf("failed to set log level = %s, err: %s", config.Log.Level, err)
	}
	glog.Infof("log.dir: %q; l4.proto: %s; port: %d; verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("config_file: %q periodic.stats_time: %v", confPath, config.Periodic.StatsTime)
	return
}

func SetConfigMany(nvmap cmn.SimpleKVs) (err error) {
	if len(nvmap) == 0 {
		return errors.New("setConfig: empty nvmap")
	}
	var (
		conf      = cmn.GCO.BeginUpdate()
		transient = false
	)
	for name, value := range nvmap {
		if name == cmn.ActTransient {
			if transient, err = cmn.ParseBool(value); err != nil {
				err = fmt.Errorf("invalid value set for %s, err: %v", name, err)
				cmn.GCO.DiscardUpdate()
				return
			}
		} else {
			err := update(conf, name, value)
			if err != nil {
				cmn.GCO.DiscardUpdate()
				return err
			}
		}

		glog.Infof("%s: %s=%s", cmn.ActSetConfig, name, value)
	}

	// Validate config after everything is set
	if err := conf.Validate(); err != nil {
		cmn.GCO.DiscardUpdate()
		return err
	}

	cmn.GCO.CommitUpdate(conf)
	if !transient {
		_ = SaveConfig(cmn.ActSetConfig)
	}
	return
}

func SaveConfig(action string) (err error) {
	conf := cmn.GCO.Get()
	if err = Save(cmn.GCO.GetConfigPath(), conf, Options{}); err != nil {
		glog.Errorf("%s: failed to write, err: %v", action, err)
	} else {
		glog.Infof("Stored config (action=%s)", action)
	}
	return
}

func update(conf *cmn.Config, key, value string) (err error) {
	switch key {
	//
	// 1. TOP LEVEL CONFIG
	//
	case "vmodule":
		if err := cmn.SetGLogVModule(value); err != nil {
			return fmt.Errorf("failed to set vmodule = %s, err: %v", value, err)
		}
	case "log_level", "log.level":
		if err := cmn.SetLogLevel(conf, value); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", value, err)
		}
	default:
		return cmn.UpdateFieldValue(conf, key, value)
	}
	return nil
}
