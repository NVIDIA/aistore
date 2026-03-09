// Package mock contains mocks for authN kvdb interfaces
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"sync"

	"github.com/NVIDIA/aistore/cmd/authn/kvdb"
)

// KeyDataStorage is a mock in-memory AuthStorageDriver for tests.
type KeyDataStorage struct {
	mu         sync.Mutex
	rawMeta    json.RawMessage
	persistErr error
}

var _ kvdb.AuthStorageDriver = (*KeyDataStorage)(nil)

func NewKeyDataStorage(rawMeta json.RawMessage, persistErr error) *KeyDataStorage {
	return &KeyDataStorage{rawMeta: rawMeta, persistErr: persistErr}
}

func (m *KeyDataStorage) LoadKeyData() (*kvdb.KeyData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rawMeta) == 0 {
		return nil, fmt.Errorf("no key data stored: %w", fs.ErrNotExist)
	}
	var md kvdb.KeyData
	if err := json.Unmarshal(m.rawMeta, &md); err != nil {
		return nil, err
	}
	jwks, _ := md.JWKS.Clone()
	return &kvdb.KeyData{
		MetaVersion: md.MetaVersion,
		JWKS:        jwks,
	}, nil
}

func (m *KeyDataStorage) PersistKeyData(meta *kvdb.KeyData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.persistErr != nil {
		return m.persistErr
	}
	jwks, _ := meta.JWKS.Clone()
	md := &kvdb.KeyData{
		MetaVersion: meta.MetaVersion,
		JWKS:        jwks,
	}
	rawMeta, err := json.Marshal(md)
	if err != nil {
		return err
	}
	m.rawMeta = rawMeta
	return nil
}
