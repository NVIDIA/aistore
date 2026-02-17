// Package kvdb contains authN server code for interacting with a persistent key-value store
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb

import (
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"
)

type dbType string

const (
	EmptyDB dbType = ""
	BuntDB  dbType = "BuntDB"
)

func CreateDriver(cm *config.ConfManager) kvdb.Driver {
	dbPath := cm.GetDBPath()
	switch dbType(cm.GetDBType()) {
	case EmptyDB, BuntDB:
		return createBuntDB(dbPath)
	default:
		cos.ExitLogf(
			"Invalid database type provided in config: %q. Currently supported database types are: %q.",
			cm.GetDBType(), BuntDB,
		)
		return nil
	}
}

func createBuntDB(path string) kvdb.Driver {
	driver, err := kvdb.NewBuntDB(path)
	if err != nil {
		cos.ExitLogf("Failed to init local database: %v", err)
	}
	return driver
}
