// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

////////////////
// ais config //
////////////////

// selected config overrides via command line
type ConfigCLI struct {
	ConfFile  string        // config filename
	LogLevel  string        // takes precedence over config.Log.Level
	StatsTime time.Duration // overrides config.Periodic.StatsTime
	ProxyURL  string        // primary proxy URL to override config.Proxy.PrimaryURL
}

func LoadConfig(clivars *ConfigCLI) (*cmn.Config, bool) {
	config, changed, err := _loadConfig(clivars)
	if err != nil {
		cmn.ExitLogf(err.Error())
	}
	return config, changed
}

func _loadConfig(clivars *ConfigCLI) (config *cmn.Config, changed bool, err error) {
	cmn.GCO.SetConfigFile(clivars.ConfFile)

	config = cmn.GCO.BeginUpdate()
	defer cmn.GCO.CommitUpdate(config)

	err = Load(clivars.ConfFile, &config, Options{})

	// NOTE: glog.Errorf + os.Exit is used instead of glog.Fatalf to not crash
	// with dozens of backtraces on screen - this is user error not some
	// internal error.

	if err != nil {
		return nil, false, fmt.Errorf("failed to load config %q, err: %s", clivars.ConfFile, err)
	}
	if err = flag.Lookup("log_dir").Value.Set(config.Log.Dir); err != nil {
		return nil, false, fmt.Errorf("failed to flag-set log directory %q, err: %s", config.Log.Dir, err)
	}
	if err = cmn.CreateDir(config.Log.Dir); err != nil {
		return nil, false, fmt.Errorf("failed to create log dir %q, err: %s", config.Log.Dir, err)
	}
	if err := config.Validate(); err != nil {
		return nil, false, err
	}

	// glog rotate
	glog.MaxSize = config.Log.MaxSize
	if glog.MaxSize > cmn.GiB {
		glog.Errorf("log.max_size %d exceeded 1GB, setting the default 1MB", glog.MaxSize)
		glog.MaxSize = cmn.MiB
	}

	config.Net.HTTP.Proto = "http" // not validating: read-only, and can take only two values
	if config.Net.HTTP.UseHTTPS {
		config.Net.HTTP.Proto = "https"
	}

	differentIPs := config.Net.IPv4 != config.Net.IPv4IntraControl
	differentPorts := config.Net.L4.Port != config.Net.L4.PortIntraControl
	config.Net.UseIntraControl = false
	if config.Net.IPv4IntraControl != "" && config.Net.L4.PortIntraControl != 0 && (differentIPs || differentPorts) {
		config.Net.UseIntraControl = true
	}

	differentIPs = config.Net.IPv4 != config.Net.IPv4IntraData
	differentPorts = config.Net.L4.Port != config.Net.L4.PortIntraData
	config.Net.UseIntraData = false
	if config.Net.IPv4IntraData != "" && config.Net.L4.PortIntraData != 0 && (differentIPs || differentPorts) {
		config.Net.UseIntraData = true
	}

	// CLI override
	if clivars.StatsTime != 0 {
		config.Periodic.StatsTime = clivars.StatsTime
		changed = true
	}
	if clivars.ProxyURL != "" {
		config.Proxy.PrimaryURL = clivars.ProxyURL
		changed = true
	}
	if clivars.LogLevel != "" {
		if err = cmn.SetLogLevel(config, clivars.LogLevel); err != nil {
			return nil, false, fmt.Errorf("failed to set log level = %s, err: %s", clivars.LogLevel, err)
		}
		config.Log.Level = clivars.LogLevel
		changed = true
	} else if err = cmn.SetLogLevel(config, config.Log.Level); err != nil {
		return nil, false, fmt.Errorf("failed to set log level = %s, err: %s", config.Log.Level, err)
	}
	glog.Infof("log.dir: %q; l4.proto: %s; port: %d; verbosity: %s",
		config.Log.Dir, config.Net.L4.Proto, config.Net.L4.Port, config.Log.Level)
	glog.Infof("config_file: %q periodic.stats_time: %v", clivars.ConfFile, config.Periodic.StatsTime)
	return
}

func SetConfigMany(nvmap cmn.SimpleKVs) (err error) {
	if len(nvmap) == 0 {
		return errors.New("setConfig: empty nvmap")
	}
	var (
		conf    = cmn.GCO.BeginUpdate()
		persist bool
	)
	for name, value := range nvmap {
		if name == cmn.ActPersist {
			if persist, err = cmn.ParseBool(value); err != nil {
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

	if persist {
		conf := cmn.GCO.Get()
		if err := Save(cmn.GCO.GetConfigFile(), conf, Options{}); err != nil {
			glog.Errorf("%s: failed to write, err: %v", cmn.ActSetConfig, err)
		} else {
			glog.Infof("%s: stored", cmn.ActSetConfig)
		}
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
