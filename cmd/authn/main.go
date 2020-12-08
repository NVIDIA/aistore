// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"flag"
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/dbdriver"
)

var (
	version, build string
	configPath     string
	conf           = &config{}
)

// Set up glog with options from configuration file
func updateLogOptions() error {
	if err := cmn.CreateDir(conf.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", conf.Log.Dir, err)
	}
	glog.SetLogDir(conf.Log.Dir)

	if conf.Log.Level != "" {
		v := flag.Lookup("v").Value
		if v == nil {
			return fmt.Errorf("nil -v Value")
		}
		if err := v.Set(conf.Log.Level); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", conf.Log.Level, err)
		}
	}
	return nil
}

func main() {
	fmt.Printf("version: %s | build_time: %s\n", version, build)

	var err error

	flag.Parse()
	confFlag := flag.Lookup("config")
	if confFlag != nil {
		configPath = confFlag.Value.String()
	}

	if configPath == "" {
		cmn.ExitLogf("Missing configuration file")
	}

	if glog.V(4) {
		glog.Infof("Reading configuration from %s", configPath)
	}
	if _, err = jsp.Load(configPath, conf, jsp.Plain()); err != nil {
		cmn.ExitLogf("Failed to load configuration from %q: %v", configPath, err)
	}
	conf.path = configPath
	conf.applySecrets()
	if err = conf.validate(); err != nil {
		cmn.ExitLogf("Invalid configuration: %v", err)
	}

	if err = updateLogOptions(); err != nil {
		cmn.ExitLogf("Failed to set up logger: %v", err)
	}

	dbPath := filepath.Join(conf.ConfDir, authDB)
	driver, err := dbdriver.NewBuntDB(dbPath)
	if err != nil {
		cmn.ExitLogf("Failed to init local database: %v", err)
	}
	mgr, err := newUserManager(driver)
	if err != nil {
		cmn.ExitLogf("Failed to init user manager: %v", err)
	}

	srv := newAuthServ(mgr)
	if err := srv.run(); err != nil {
		cmn.ExitLogf("Server failed: %v", err)
	}
}
