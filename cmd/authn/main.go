// Package authn is authentication server for AIStore.
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
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/kvdb"
)

const secretKeyPodEnv = "SECRETKEY" // via https://kubernetes.io/docs/concepts/configuration/secret

var (
	build     string
	buildtime string

	configPath string
)

func init() {
	flag.StringVar(&configPath, "config", "", svcName+" configuration")
}

func glogFlush() {
	for {
		time.Sleep(30 * time.Second)
		glog.Flush()
	}
}

func main() {
	var configDir string
	if len(os.Args) == 2 && os.Args[1] == "version" {
		printVer()
		os.Exit(0)
	}
	if len(os.Args) == 1 || (len(os.Args) == 2 && strings.Contains(os.Args[1], "help")) {
		printVer()
		flag.PrintDefaults()
		os.Exit(0)
	}
	installSignalHandler()
	flag.Parse()

	confDirFlag := flag.Lookup("config")
	if confDirFlag != nil {
		configDir = confDirFlag.Value.String()
	}
	if configDir == "" {
		configDir = os.Getenv(env.AuthN.ConfDir)
	}
	if configDir == "" {
		cos.ExitLogf("Missing %s configuration file (to specify, use '-%s' option or '%s' environment)",
			svcName, confDirFlag.Name, env.AuthN.ConfDir)
	}
	configPath = filepath.Join(configDir, fname.AuthNConfig)
	if _, err := jsp.LoadMeta(configPath, Conf); err != nil {
		cos.ExitLogf("Failed to load configuration from %q: %v", configPath, err)
	}
	if val := os.Getenv(secretKeyPodEnv); val != "" {
		Conf.Server.Secret = val
	}
	if err := updateLogOptions(); err != nil {
		cos.ExitLogf("Failed to set up logger: %v", err)
	}
	if Conf.Verbose() {
		glog.Infof("Loaded configuration from %s", configPath)
	}

	dbPath := filepath.Join(configDir, fname.AuthNDB)
	driver, err := kvdb.NewBuntDB(dbPath)
	if err != nil {
		cos.ExitLogf("Failed to init local database: %v", err)
	}
	mgr, err := newMgr(driver)
	if err != nil {
		cos.ExitLogf("Failed to init manager: %v", err)
	}

	glog.Infof("Version %s (build %s)\n", cmn.VersionAuthN+"."+build, buildtime)

	go glogFlush()

	srv := newServer(mgr)
	err = srv.Run()
	cos.Close(mgr.db)
	if err != nil {
		cos.ExitLogf("Server failed: %v", err)
	}
}

// Set up glog with options from configuration file
func updateLogOptions() error {
	if err := cos.CreateDir(Conf.Log.Dir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", Conf.Log.Dir, err)
	}
	glog.SetLogDirRole(Conf.Log.Dir, "auth")
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

func printVer() {
	fmt.Printf("version %s (build %s)\n", cmn.VersionAuthN+"."+build, buildtime)
}
