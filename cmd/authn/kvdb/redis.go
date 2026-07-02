// Package kvdb contains authN server code for interacting with a persistent key-value store
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"

	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
)

const redisScanCount = 256

// redisDriver implements cmn/kvdb.Driver with the same collection/key layout as BuntDB.
type redisDriver struct {
	client  *redis.Client
	timeout time.Duration
}

var _ kvdb.Driver = (*redisDriver)(nil)

func newRedisDriver(conf authn.KVServiceConf) (*redisDriver, error) {
	opts := &redis.Options{
		Addr:                  cmn.HostPort(conf.Host, strconv.Itoa(conf.Port)),
		Password:              conf.Password(),
		DB:                    conf.DBIndex,
		ContextTimeoutEnabled: true,
	}
	if conf.TLSEnabled {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	client := redis.NewClient(opts)
	driver := &redisDriver{client: client, timeout: conf.Timeout.D()}
	ctx, cancel := driver.ctx()
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return driver, nil
}

func (r *redisDriver) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), r.timeout)
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
	if err := jsoniter.Unmarshal([]byte(s), object); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func (r *redisDriver) SetString(collection, key, data string) (int, error) {
	name := kvdb.MakePath(collection, key)
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Set(ctx, name, data, 0).Err(); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func (r *redisDriver) GetString(collection, key string) (string, int, error) {
	name := kvdb.MakePath(collection, key)
	ctx, cancel := r.ctx()
	defer cancel()
	val, err := r.client.Get(ctx, name).Result()
	if err != nil {
		return redisToCommonErr(err, collection, key)
	}
	return val, http.StatusOK, nil
}

func (r *redisDriver) Delete(collection, key string) (int, error) {
	name := kvdb.MakePath(collection, key)
	ctx, cancel := r.ctx()
	defer cancel()
	n, err := r.client.Del(ctx, name).Result()
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if n == 0 {
		_, code, err := redisToCommonErr(redis.Nil, collection, key)
		return code, err
	}
	return http.StatusOK, nil
}

func (r *redisDriver) DeleteCollection(collection string) (int, error) {
	pattern := kvdb.MakePath(collection, "*")
	ctx, cancel := r.ctx()
	defer cancel()
	return r.scan(ctx, pattern, func(keys []string) error {
		if len(keys) == 0 {
			return nil
		}
		return r.client.Del(ctx, keys...).Err()
	})
}

func (r *redisDriver) List(collection, pattern string) ([]string, int, error) {
	pattern = normalizePattern(pattern)
	fullPattern := kvdb.MakePath(collection, pattern)
	keys := make([]string, 0)
	ctx, cancel := r.ctx()
	defer cancel()
	code, err := r.scan(ctx, fullPattern, func(batch []string) error {
		for _, path := range batch {
			_, key := kvdb.ParsePath(path)
			if key != "" {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, code, err
}

func (r *redisDriver) GetAll(collection, pattern string) (map[string]string, int, error) {
	pattern = normalizePattern(pattern)
	fullPattern := kvdb.MakePath(collection, pattern)
	values := make(map[string]string)
	ctx, cancel := r.ctx()
	defer cancel()
	code, err := r.scan(ctx, fullPattern, func(batch []string) error {
		for _, path := range batch {
			val, err := r.client.Get(ctx, path).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				return err
			}
			_, key := kvdb.ParsePath(path)
			if key != "" {
				values[key] = val
			}
		}
		return nil
	})
	return values, code, err
}

func (r *redisDriver) scan(ctx context.Context, pattern string, fn func([]string) error) (int, error) {
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(ctx, cursor, pattern, redisScanCount).Result()
		if err != nil {
			return http.StatusInternalServerError, err
		}
		if err := fn(keys); err != nil {
			return http.StatusInternalServerError, err
		}
		cursor = next
		if cursor == 0 {
			return http.StatusOK, nil
		}
	}
}

func normalizePattern(pattern string) string {
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return pattern + "*"
	}
	return pattern
}

func redisToCommonErr(err error, collection, key string) (string, int, error) {
	if errors.Is(err, redis.Nil) {
		what := collection
		if key != "" {
			what += " \"" + key + "\""
		}
		return "", http.StatusNotFound, cos.NewErrNotFound(nil, what)
	}
	return "", http.StatusInternalServerError, err
}
