package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/golang/glog"
)

var (
	configPath string
	conf       = &config{}
)

// Set up glog with options from configuration file
func updateLogOptions() error {
	err := flag.Lookup("log_dir").Value.Set(conf.Log.Dir)
	if err != nil {
		return fmt.Errorf("Failed to flag-set glog dir %q, err: %v", conf.Log.Dir, err)
	}
	if err = dfc.CreateDir(conf.Log.Dir); err != nil {
		return fmt.Errorf("Failed to create log dir %q, err: %v", conf.Log.Dir, err)
	}

	if conf.Log.Level != "" {
		v := flag.Lookup("v").Value
		if v == nil {
			return fmt.Errorf("nil -v Value")
		}
		if err = v.Set(conf.Log.Level); err != nil {
			return fmt.Errorf("Failed to set log level = %s, err: %v", conf.Log.Level, err)
		}
	}
	return nil
}

func main() {
	var (
		err error
	)

	flag.Parse()
	confFlag := flag.Lookup("config")
	if confFlag != nil {
		configPath = confFlag.Value.String()
	}

	if configPath == "" {
		glog.Fatalf("Missing configuration file")
	}

	if glog.V(4) {
		log.Printf("Reading configuration from %s\n", configPath)
	}
	if err = dfc.LocalLoad(configPath, conf); err != nil {
		glog.Fatalf("Failed to load configuration: %v\n", err)
	}
	if err = conf.validate(); err != nil {
		glog.Fatalf("Invalid configuration: %v\n", err)
	}

	if err = updateLogOptions(); err != nil {
		glog.Fatalf("Failed to set up logger: %v\n", err)
	}

	dbPath := filepath.Join(conf.ConfDir, dbFile)
	srv := newAuthServ(newUserManager(dbPath))
	if err := srv.run(); err != nil {
		glog.Fatalf(err.Error())
	}
}
