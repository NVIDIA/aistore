// Package kvdb contains authN server code for interacting with a persistent key-value store
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"

	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
)

// redisDriver implements cmn/kvdb.Driver backed by Redis.
//
// Key layout mirrors BuntDB: every record is stored as a flat Redis string key
// with the form  "<collection>##<key>",  where "##" is kvdb.CollectionSepa.
// This allows zero-friction migration (dump BuntDB → RESTORE into Redis) and
// keeps ParsePath / MakePath semantics identical across backends.
type redisDriver struct {
	client *redis.Client
}

// interface guard
var _ kvdb.Driver = (*redisDriver)(nil)

func newRedisDriver(conf *authn.KVServiceConf) (*redisDriver, error) {
	opts := &redis.Options{
		Addr:     conf.Addr,
		Password: conf.Password,
		DB:       conf.DBIndex,
	}
	if conf.TLSEnabled {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	client := redis.NewClient(opts)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis: cannot connect to %q: %w", conf.Addr, err)
	}
	return &redisDriver{client: client}, nil
}

func (r *redisDriver) Close() error {
	return r.client.Close()
}

func (r *redisDriver) Set(collection, key string, object any) (int, error) {
	b := cos.MustMarshal(object)
	return r.SetString(collection, key, string(b))
}

func (r *redisDriver) Get(collection, key string, object any) (int, error) {
	s, code, err := r.GetString(collection, key)
	if err != nil {
		return code, err
	}
	if err := jsoniter.UnmarshalFromString(s, object); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func (r *redisDriver) SetString(collection, key, data string) (int, error) {
	name := kvdb.MakePath(collection, key)
	if err := r.client.Set(context.Background(), name, data, 0).Err(); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func (r *redisDriver) GetString(collection, key string) (string, int, error) {
	name := kvdb.MakePath(collection, key)
	val, err := r.client.Get(context.Background(), name).Result()
	if err != nil {
		if err == redis.Nil {
			what := collection + " \"" + key + "\""
			return "", http.StatusNotFound, cos.NewErrNotFound(nil, what)
		}
		return "", http.StatusInternalServerError, err
	}
	return val, http.StatusOK, nil
}

func (r *redisDriver) Delete(collection, key string) (int, error) {
	name := kvdb.MakePath(collection, key)
	n, err := r.client.Del(context.Background(), name).Result()
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if n == 0 {
		what := collection + " \"" + key + "\""
		return http.StatusNotFound, cos.NewErrNotFound(nil, what)
	}
	return http.StatusOK, nil
}

func (r *redisDriver) DeleteCollection(collection string) (int, error) {
	pattern := kvdb.MakePath(collection, "*")
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(context.Background(), cursor, pattern, 256).Result()
		if err != nil {
			return http.StatusInternalServerError, err
		}
		if len(keys) > 0 {
			if err := r.client.Del(context.Background(), keys...).Err(); err != nil {
				return http.StatusInternalServerError, err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return http.StatusOK, nil
}

// List returns the key portion (without collection prefix) of all entries whose
// full path matches the given glob pattern within the collection.
// If pattern contains no wildcards it is treated as a prefix ("prefix*").
func (r *redisDriver) List(collection, pattern string) ([]string, int, error) {
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		pattern += "*"
	}
	keyPattern := kvdb.MakePath(collection, pattern)

	var (
		cursor uint64
		keys   []string
	)
	for {
		batch, next, err := r.client.Scan(context.Background(), cursor, keyPattern, 256).Result()
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		for _, fullKey := range batch {
			_, k := kvdb.ParsePath(fullKey)
			if k != "" {
				keys = append(keys, k)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, http.StatusOK, nil
}

// GetAll returns a map of key→value for all entries matching the pattern.
// The SCAN+GET approach is safe: a key deleted between SCAN and GET is silently
// skipped (it was being removed concurrently, which is a valid transient state).
func (r *redisDriver) GetAll(collection, pattern string) (map[string]string, int, error) {
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		pattern += "*"
	}
	keyPattern := kvdb.MakePath(collection, pattern)

	values := make(map[string]string)
	var cursor uint64
	for {
		batch, next, err := r.client.Scan(context.Background(), cursor, keyPattern, 256).Result()
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		for _, fullKey := range batch {
			val, err := r.client.Get(context.Background(), fullKey).Result()
			if err != nil {
				if err == redis.Nil {
					continue // key deleted between SCAN and GET
				}
				return nil, http.StatusInternalServerError, err
			}
			_, k := kvdb.ParsePath(fullKey)
			if k != "" {
				values[k] = val
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return values, http.StatusOK, nil
}
