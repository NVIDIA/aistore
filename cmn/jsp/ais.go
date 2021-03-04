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

	// NOTE: the following two "loads" are loading plain-text config
	//       and are executed only once when the node starts up;
	//       once started, the node can then reload the last
	//       updated version of the (global|local) config
	//       from the configured location
	_, err = Load(confPath, &config.ClusterConfig, cmn.Plain())
	if err != nil {
		return fmt.Errorf("failed to load global config %q, err: %v", confPath, err)
	}
	config.SetRole(daeRole)
	_, err = Load(localConfPath, &config.LocalConfig, cmn.Plain())
	if err != nil {
		return fmt.Errorf("failed to load local config %q, err: %v", localConfPath, err)
	}

	overrideConfig, err := LoadOverrideConfig(config.ConfigDir)
	if err != nil {
		return err
	}

	if overrideConfig != nil {
		err = config.Apply(*overrideConfig, cmn.Daemon)
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
		config.Log.Dir, config.Net.L4.Proto, config.HostNet.Port, config.Log.Level)
	glog.Infof("config_file: %q periodic.stats_time: %v", confPath, config.Periodic.StatsTime)
	return
}

func LoadOverrideConfig(configDir string) (config *cmn.ConfigToUpdate, err error) {
	config = &cmn.ConfigToUpdate{}
	_, err = LoadMeta(path.Join(configDir, cmn.OverrideConfigFname), config)
	if os.IsNotExist(err) {
		err = nil
	}
	return
}

func SaveOverrideConfig(configDir string, config *cmn.ConfigToUpdate) error {
	return SaveMeta(path.Join(configDir, cmn.OverrideConfigFname), config)
}
