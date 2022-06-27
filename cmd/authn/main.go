// Package authn provides AuthN server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/dbdriver"
)

const (
	authDB          = "authn.db"
	secretKeyEnvVar = "SECRETKEY"
)

var (
	version    = "1.0"
	build      string
	configPath string
)

func init() {
	flag.StringVar(&configPath, "config", "",
		"config filename: local file that stores the global cluster configuration")
}

// Set up glog with options from configuration file
func updateLogOptions() error {
	if err := cos.CreateDir(Conf.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", Conf.Log.Dir, err)
	}
	glog.SetLogDir(Conf.Log.Dir)

	if Conf.Log.Level != "" {
		v := flag.Lookup("v").Value
		if v == nil {
			return fmt.Errorf("nil -v Value")
		}
		if err := v.Set(Conf.Log.Level); err != nil {
			return fmt.Errorf("failed to set log level = %s, err: %v", Conf.Log.Level, err)
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
	fmt.Printf("version: %s | build: %s\n", version, build)

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
	if _, err = jsp.LoadMeta(configPath, Conf); err != nil {
		cos.ExitLogf("Failed to load configuration from %q: %v", configPath, err)
	}
	Conf.Path = configPath
	if val := os.Getenv(secretKeyEnvVar); val != "" {
		Conf.Server.Secret = val
	}

	if err = updateLogOptions(); err != nil {
		cos.ExitLogf("Failed to set up logger: %v", err)
	}

	dbPath := filepath.Join(Conf.ConfDir, authDB)
	driver, err := dbdriver.NewBuntDB(dbPath)
	if err != nil {
		cos.ExitLogf("Failed to init local database: %v", err)
	}
	mgr, err := NewUserManager(driver)
	if err != nil {
		cos.ExitLogf("Failed to init user manager: %v", err)
	}

	srv := NewServer(mgr)
	if err := srv.Run(); err != nil {
		cos.ExitLogf("Server failed: %v", err)
	}
}
