// Package kvdb contains authN server code for interacting with a persistent key-value store
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb

import (
	"errors"
	"io/fs"
	"net/http"
	"sort"
	"testing"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/alicebob/miniredis/v2"
)

func newTestRedisDriver(t *testing.T) *redisDriver {
	srv := miniredis.RunT(t)
	driver, err := newRedisDriver(authn.KVServiceConf{Addr: srv.Addr()})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = driver.Close() })
	return driver
}

func TestRedisDriverSetGetDelete(t *testing.T) {
	driver := newTestRedisDriver(t)

	code, err := driver.SetString("users", "alice", "reader")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)

	got, code, err := driver.GetString("users", "alice")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)
	tassert.Errorf(t, got == "reader", "expected stored value, got %q", got)

	code, err = driver.Delete("users", "alice")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)

	got, code, err = driver.GetString("users", "alice")
	tassert.Errorf(t, errors.Is(err, fs.ErrNotExist), "expected not-exist error, got %v", err)
	tassert.Errorf(t, code == http.StatusNotFound, "expected status %d, got %d", http.StatusNotFound, code)
	tassert.Errorf(t, got == "", "expected empty value after delete, got %q", got)
}

func TestRedisDriverListAndGetAll(t *testing.T) {
	driver := newTestRedisDriver(t)

	for key, val := range map[string]string{
		"admin": "su",
		"alice": "reader",
		"bob":   "writer",
	} {
		_, err := driver.SetString("users", key, val)
		tassert.CheckFatal(t, err)
	}
	_, err := driver.SetString("roles", "admin", "role")
	tassert.CheckFatal(t, err)

	keys, code, err := driver.List("users", "a")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)
	sort.Strings(keys)
	tassert.Errorf(t, len(keys) == 2 && keys[0] == "admin" && keys[1] == "alice",
		"expected admin/alice keys, got %#v", keys)

	values, code, err := driver.GetAll("users", "")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)
	tassert.Errorf(t, len(values) == 3, "expected 3 users, got %#v", values)
	tassert.Errorf(t, values["alice"] == "reader", "expected alice value, got %q", values["alice"])
	tassert.Errorf(t, values["bob"] == "writer", "expected bob value, got %q", values["bob"])
}

func TestRedisDriverDeleteCollection(t *testing.T) {
	driver := newTestRedisDriver(t)

	_, err := driver.SetString("users", "alice", "reader")
	tassert.CheckFatal(t, err)
	_, err = driver.SetString("users", "bob", "writer")
	tassert.CheckFatal(t, err)
	_, err = driver.SetString("roles", "admin", "role")
	tassert.CheckFatal(t, err)

	code, err := driver.DeleteCollection("users")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)

	users, code, err := driver.List("users", "")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)
	tassert.Errorf(t, len(users) == 0, "expected users collection deleted, got %#v", users)

	role, code, err := driver.GetString("roles", "admin")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, code == http.StatusOK, "expected status %d, got %d", http.StatusOK, code)
	tassert.Errorf(t, role == "role", "expected other collection preserved, got %q", role)
}
