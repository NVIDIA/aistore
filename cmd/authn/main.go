// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

var (
	build     string
	buildtime string
)

func logFlush() {
	for {
		time.Sleep(time.Minute) // TODO: must be configurable
		nlog.Flush(nlog.ActNone)
	}
}

func main() {
	cfgPath := flag.String("config", "", config.ServiceName+" configuration")
	showVer := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVer || len(os.Args) == 2 && os.Args[1] == "version" {
		printVer()
		os.Exit(0)
	}
	if len(os.Args) == 1 || (len(os.Args) == 2 && strings.Contains(os.Args[1], "help")) {
		printVer()
		flag.PrintDefaults()
		os.Exit(0)
	}
	installSignalHandler()
	cm := config.NewConfManager()
	configDir, configFile := resolveConfigPath(*cfgPath)
	cm.Init(configFile)
	updateLogOptions(cm)
	driver := createDB(configDir)
	mgr, code, err := newMgr(cm, driver)
	if err != nil {
		cos.ExitLogf("Failed to init manager: %v(%d)", err, code)
	}

	nlog.Infof("Version %s (build %s)\n", cmn.VersionAuthN+"."+build, buildtime)

	go logFlush()

	srv := newServer(mgr)
	err = srv.Run()

	nlog.Flush(nlog.ActExit)
	cos.Close(mgr.db)
	if err != nil {
		cos.ExitLogf("Server failed: %v", err)
	}
}

func createDB(configDir string) *kvdb.BuntDriver {
	dbPath := filepath.Join(configDir, fname.AuthNDB)
	driver, err := kvdb.NewBuntDB(dbPath)
	if err != nil {
		cos.ExitLogf("Failed to init local database: %v", err)
	}
	return driver
}

func resolveConfigPath(flagConf string) (configDir, configFile string) {
	switch {
	case flagConf != "":
		fi, err := os.Stat(flagConf)
		if err != nil {
			cos.ExitLogf("Invalid %s configuration path %q: %v", config.ServiceName, flagConf, err)
		}
		if fi.IsDir() {
			configDir = flagConf
			configFile = filepath.Join(configDir, fname.AuthNConfig)
		} else {
			configFile = flagConf
			configDir = filepath.Dir(flagConf)
		}
	case os.Getenv(env.AisAuthConfDir) != "":
		configDir = os.Getenv(env.AisAuthConfDir)
		configFile = filepath.Join(configDir, fname.AuthNConfig)
	default:
		cos.ExitLogf("Missing %s configuration file (use '-config' or '%s')",
			config.ServiceName, env.AisAuthConfDir)
	}
	return
}

func updateLogOptions(cm *config.ConfManager) {
	logDir := cm.GetLogDir()
	if err := cos.CreateDir(logDir); err != nil {
		err = fmt.Errorf("failed to create log dir %q, err: %v", logDir, err)
		cos.ExitLogf("Failed to set up logger: %v", err)
	}
	nlog.SetPre(logDir, "auth")
}

func installSignalHandler() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Exit(0)
	}()
}

func printVer() {
	fmt.Printf("version %s (build %s)\n", cmn.VersionAuthN+"."+build, buildtime)
}
