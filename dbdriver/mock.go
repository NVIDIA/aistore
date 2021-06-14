// Package dbdriver provides a local database server for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package dbdriver

import (
	"sort"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

type DBMock struct {
	mtx    sync.RWMutex
	values map[string]string
}

// interface guard
var _ Driver = (*DBMock)(nil)

func NewDBMock() Driver                                { return &DBMock{values: make(map[string]string)} }
func (*DBMock) makePath(collection, key string) string { return collection + collectionSepa + key }
func (*DBMock) Close() error                           { return nil }

func (bd *DBMock) Set(collection, key string, object interface{}) error {
	b := cos.MustMarshal(object)
	return bd.SetString(collection, key, string(b))
}

func (bd *DBMock) Get(collection, key string, object interface{}) error {
	s, err := bd.GetString(collection, key)
	if err != nil {
		return err
	}
	return jsoniter.Unmarshal([]byte(s), object)
}

func (bd *DBMock) SetString(collection, key, data string) error {
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	name := bd.makePath(collection, key)
	bd.values[name] = data
	return nil
}

func (bd *DBMock) GetString(collection, key string) (string, error) {
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	name := bd.makePath(collection, key)
	value, ok := bd.values[name]
	if !ok {
		return "", NewErrNotFound(collection, key)
	}
	return value, nil
}

func (bd *DBMock) Delete(collection, key string) error {
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	name := bd.makePath(collection, key)
	_, ok := bd.values[name]
	if !ok {
		return NewErrNotFound(collection, key)
	}
	delete(bd.values, name)
	return nil
}

func (bd *DBMock) List(collection, pattern string) ([]string, error) {
	var (
		keys   = make([]string, 0)
		filter string
	)
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	filter = bd.makePath(collection, pattern)
	for k := range bd.values {
		if strings.HasPrefix(k, filter) {
			_, key := parsePath(k)
			if key != "" {
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (bd *DBMock) DeleteCollection(collection string) error {
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

func (bd *DBMock) GetAll(collection, pattern string) (map[string]string, error) {
	var (
		values = make(map[string]string)
		filter string
	)
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	filter = bd.makePath(collection, pattern)
	for k, v := range bd.values {
		if strings.HasPrefix(k, filter) {
			_, key := parsePath(k)
			if key != "" {
				values[key] = v
			}
		}
	}
	return values, nil
}
