// Package dbdriver provides a local database server for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"sort"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dbdriver"
	jsoniter "github.com/json-iterator/go"
)

type DBDriver struct {
	mtx    sync.RWMutex
	values map[string]string
}

// interface guard
var _ dbdriver.Driver = (*DBDriver)(nil)

func NewDBDriver() dbdriver.Driver { return &DBDriver{values: make(map[string]string)} }
func (*DBDriver) Close() error     { return nil }

func (*DBDriver) makePath(collection, key string) string {
	return collection + dbdriver.CollectionSepa + key
}

func (bd *DBDriver) Set(collection, key string, object interface{}) error {
	b := cos.MustMarshal(object)
	return bd.SetString(collection, key, string(b))
}

func (bd *DBDriver) Get(collection, key string, object interface{}) error {
	s, err := bd.GetString(collection, key)
	if err != nil {
		return err
	}
	return jsoniter.Unmarshal([]byte(s), object)
}

func (bd *DBDriver) SetString(collection, key, data string) error {
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	name := bd.makePath(collection, key)
	bd.values[name] = data
	return nil
}

func (bd *DBDriver) GetString(collection, key string) (string, error) {
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	name := bd.makePath(collection, key)
	value, ok := bd.values[name]
	if !ok {
		return "", dbdriver.NewErrNotFound(collection, key)
	}
	return value, nil
}

func (bd *DBDriver) Delete(collection, key string) error {
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	name := bd.makePath(collection, key)
	_, ok := bd.values[name]
	if !ok {
		return dbdriver.NewErrNotFound(collection, key)
	}
	delete(bd.values, name)
	return nil
}

func (bd *DBDriver) List(collection, pattern string) ([]string, error) {
	var (
		keys   = make([]string, 0)
		filter string
	)
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	filter = bd.makePath(collection, pattern)
	for k := range bd.values {
		if strings.HasPrefix(k, filter) {
			_, key := dbdriver.ParsePath(k)
			if key != "" {
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (bd *DBDriver) DeleteCollection(collection string) error {
	keys, err := bd.List(collection, "")
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	if err != nil || len(keys) == 0 {
		return err
	}
	for _, k := range keys {
		delete(bd.values, k)
	}
	return nil
}

func (bd *DBDriver) GetAll(collection, pattern string) (map[string]string, error) {
	var (
		values = make(map[string]string)
		filter string
	)
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	filter = bd.makePath(collection, pattern)
	for k, v := range bd.values {
		if strings.HasPrefix(k, filter) {
			_, key := dbdriver.ParsePath(k)
			if key != "" {
				values[key] = v
			}
		}
	}
	return values, nil
}
