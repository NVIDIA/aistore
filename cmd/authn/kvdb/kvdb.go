// Package kvdb contains authN server code for interacting with a persistent key-value store
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"

	"github.com/lestrrat-go/jwx/v2/jwk"
)

type dbType string

const (
	EmptyDB dbType = ""
	BuntDB  dbType = "BuntDB"
)

const KeyMetadataVersion = 1

// KeyData is the single unit of storage for auth key state (JWKS and future fields).
// It is read/written atomically.
// MetaVersion is the schema version of this struct.
type KeyData struct {
	MetaVersion int     `json:"meta_version"`
	JWKS        jwk.Set `json:"-"`
}

func (m *KeyData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MetaVersion int     `json:"meta_version"`
		JWKS        jwk.Set `json:"jwks"`
	}{m.MetaVersion, m.JWKS})
}

func (m *KeyData) UnmarshalJSON(data []byte) error {
	var raw struct {
		MetaVersion int             `json:"meta_version"`
		JWKS        json.RawMessage `json:"jwks"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw.JWKS) == 0 {
		return errors.New("key metadata missing jwks")
	}
	set, err := jwk.Parse(raw.JWKS)
	if err != nil {
		return fmt.Errorf("parse JWKS: %w", err)
	}
	m.MetaVersion = raw.MetaVersion
	m.JWKS = set
	return nil
}

// AuthStorageDriver loads and persists key metadata (JWKS and related fields) as one unit.
type AuthStorageDriver interface {
	LoadKeyData() (*KeyData, error)
	PersistKeyData(m *KeyData) error
}

const (
	keyDataCollection = "key"
	keyDataKey        = "metadata"
)

// Driver wraps a kvdb.Driver and adds authn-specific capabilities.
// It implements both cmn/kvdb.Driver (via embedding) and AuthStorageDriver.
type Driver struct {
	kvdb.Driver
}

var _ kvdb.Driver = (*Driver)(nil)
var _ AuthStorageDriver = (*Driver)(nil)

func (d *Driver) LoadKeyData() (*KeyData, error) {
	raw, _, err := d.Driver.GetString(keyDataCollection, keyDataKey)
	if err != nil {
		return nil, fmt.Errorf("load key metadata from DB: %w", err)
	}
	var m KeyData
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("parse key metadata: %w", err)
	}
	return &m, nil
}

func (d *Driver) PersistKeyData(m *KeyData) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err := d.Driver.SetString(keyDataCollection, keyDataKey, string(raw)); err != nil {
		return err
	}
	return nil
}

func CreateDriver(cm *config.ConfManager) *Driver {
	dbPath := cm.GetDBPath()
	switch dbType(cm.GetDBType()) {
	case EmptyDB, BuntDB:
		return &Driver{Driver: createBuntDB(dbPath)}
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
