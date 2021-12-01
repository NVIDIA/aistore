// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/authn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/dbdriver"
)

const (
	authDB          = "authn.db"
	secretKeyEnvVar = "SECRETKEY"
)

var (
	version, build string
	configPath     string
)

func init() {
	flag.StringVar(&configPath, "config", "",
		"config filename: local file that stores the global cluster configuration")
}

// Set up glog with options from configuration file
func updateLogOptions() error {
	if err := cos.CreateDir(authn.Conf.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", authn.Conf.Log.Dir, err)
	}
	glog.SetLogDir(authn.Conf.Log.Dir)

	if authn.Conf.Log.Level != "" {
		v := flag.Lookup("v").Value
		if v == nil {
			return fmt.Errorf("nil -v Value")
		}
		if err := v.Set(authn.Conf.Log.Level); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", authn.Conf.Log.Level, err)
		}
	}
	return nil
}

func installSignalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		os.Exit(0)
	}()
}

func main() {
	fmt.Printf("version: %s | build_time: %s\n", version, build)

	installSignalHandler()

	var err error

	flag.Parse()
	confFlag := flag.Lookup("config")
	if confFlag != nil {
		configPath = confFlag.Value.String()
	}

	if configPath == "" {
		cos.ExitLogf("Missing configuration file")
	}

	if glog.V(4) {
		glog.Infof("Reading configuration from %s", configPath)
	}
	if _, err = jsp.LoadMeta(configPath, authn.Conf); err != nil {
		cos.ExitLogf("Failed to load configuration from %q: %v", configPath, err)
	}
	authn.Conf.Path = configPath
	if val := os.Getenv(secretKeyEnvVar); val != "" {
		authn.Conf.Server.Secret = val
	}

	if err = updateLogOptions(); err != nil {
		cos.ExitLogf("Failed to set up logger: %v", err)
	}

	dbPath := filepath.Join(authn.Conf.ConfDir, authDB)
	driver, err := dbdriver.NewBuntDB(dbPath)
	if err != nil {
		cos.ExitLogf("Failed to init local database: %v", err)
	}
	mgr, err := authn.NewUserManager(driver)
	if err != nil {
		cos.ExitLogf("Failed to init user manager: %v", err)
	}

	srv := authn.NewServer(mgr)
	if err := srv.Run(); err != nil {
		cos.ExitLogf("Server failed: %v", err)
	}
}
