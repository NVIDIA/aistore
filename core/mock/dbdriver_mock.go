// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	jsoniter "github.com/json-iterator/go"
)

type DBDriver struct {
	mtx    sync.RWMutex
	values map[string]string
}

// interface guard
var _ kvdb.Driver = (*DBDriver)(nil)

func NewDBDriver() kvdb.Driver { return &DBDriver{values: make(map[string]string)} }
func (*DBDriver) Close() error { return nil }

func (*DBDriver) makePath(collection, key string) string {
	return collection + kvdb.CollectionSepa + key
}

func (bd *DBDriver) Set(collection, key string, object any) (int, error) {
	b := cos.MustMarshal(object)
	return bd.SetString(collection, key, string(b))
}

func (bd *DBDriver) Get(collection, key string, object any) (int, error) {
	s, code, err := bd.GetString(collection, key)
	if err != nil {
		return code, err
	}
	if err = jsoniter.Unmarshal([]byte(s), object); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func (bd *DBDriver) SetString(collection, key, data string) (int, error) {
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	name := bd.makePath(collection, key)
	bd.values[name] = data
	return http.StatusOK, nil
}

func (bd *DBDriver) GetString(collection, key string) (string, int, error) {
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	name := bd.makePath(collection, key)
	value, ok := bd.values[name]
	if !ok {
		return "", http.StatusNotFound, cos.NewErrNotFound(nil, collection+" \""+key+"\"")
	}
	return value, http.StatusOK, nil
}

func (bd *DBDriver) Delete(collection, key string) (int, error) {
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	name := bd.makePath(collection, key)
	_, ok := bd.values[name]
	if !ok {
		return http.StatusNotFound, cos.NewErrNotFound(nil, collection+" \""+key+"\"")
	}
	delete(bd.values, name)
	return http.StatusOK, nil
}

func (bd *DBDriver) List(collection, pattern string) ([]string, int, error) {
	var (
		keys   = make([]string, 0)
		filter string
	)
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	filter = bd.makePath(collection, pattern)
	for k := range bd.values {
		if strings.HasPrefix(k, filter) {
			_, key := kvdb.ParsePath(k)
			if key != "" {
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys, http.StatusOK, nil
}

func (bd *DBDriver) DeleteCollection(collection string) (int, error) {
	keys, code, err := bd.List(collection, "")
	bd.mtx.Lock()
	defer bd.mtx.Unlock()
	if err != nil || len(keys) == 0 {
		return code, err
	}
	for _, k := range keys {
		delete(bd.values, k)
	}
	return http.StatusOK, nil
}

func (bd *DBDriver) GetAll(collection, pattern string) (map[string]string, int, error) {
	var (
		values = make(map[string]string)
		filter string
	)
	bd.mtx.RLock()
	defer bd.mtx.RUnlock()
	filter = bd.makePath(collection, pattern)
	for k, v := range bd.values {
		if strings.HasPrefix(k, filter) {
			_, key := kvdb.ParsePath(k)
			if key != "" {
				values[key] = v
			}
		}
	}
	return values, http.StatusOK, nil
}
