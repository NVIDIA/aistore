// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"fmt"
	"os"
	"path"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

////////////////
// ais config //
////////////////

func LoadConfig(confPath, localConfPath, daeRole string, config *cmn.Config) (err error) {
	debug.Assert(confPath != "" && localConfPath != "")
	cmn.GCO.SetGlobalConfigPath(confPath)
	cmn.GCO.SetLocalConfigPath(localConfPath)
	_, err = Load(confPath, &config, PlainLocal())
	if err != nil {
		return fmt.Errorf("failed to load config %q, err: %v", confPath, err)
	}
	config.SetRole(daeRole)

	localConf := &cmn.LocalConfig{}
	_, err = Load(localConfPath, localConf, Plain())
	if err != nil {
		return fmt.Errorf("failed to load local config %q, err: %v", localConfPath, err)
	}
	if err = config.SetLocalConf(localConf); err != nil {
		return
	}

	overrideConfig, err := LoadOverrideConfig(localConf.ConfigDir)
	if err != nil {
		return err
	}

	if overrideConfig != nil {
		err = config.Apply(*overrideConfig)
	} else {
		err = config.Validate()
	}
	if err != nil {
		return
	}

	if err = cmn.CreateDir(config.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", config.Log.Dir, err)
	}
	glog.SetLogDir(config.Log.Dir)

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > cmn.GiB {
		glog.Warningf("log.max_size %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = cmn.MiB
	}

	if err = cmn.SetLogLevel(config, config.Log.Level); err != nil {
		return fmt.Errorf("failed to set log level %q, err: %s", config.Log.Level, err)
	}
	glog.Infof("log.dir: %q; l4.proto: %s; port: %d; verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("config_file: %q periodic.stats_time: %v", confPath, config.Periodic.StatsTime)

	cmn.GCO.PutLocal(localConf)
	return
}

func LoadOverrideConfig(configDir string) (config *cmn.ConfigToUpdate, err error) {
	config = &cmn.ConfigToUpdate{}
	_, err = Load(path.Join(configDir, cmn.OverrideConfigFname), config, Plain())
	if os.IsNotExist(err) {
		err = nil
	}
	return
}

func SaveOverrideConfig(configDir string, config *cmn.ConfigToUpdate) error {
	return Save(path.Join(configDir, cmn.OverrideConfigFname), config, Plain())
}

func SaveConfig(config *cmn.Config) error {
	return Save(cmn.GCO.GetGlobalConfigPath(), config, PlainLocal())
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
