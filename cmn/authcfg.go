// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 *
 */
package cmn

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type (
	AuthNConfig struct {
		mtx     sync.RWMutex     `list:"omit"` // for cmn.IterFields
		Path    string           `json:"path"`
		ConfDir string           `json:"confdir"`
		Log     AuthNLogConf     `json:"log"`
		Net     AuthNetConf      `json:"net"`
		Server  AuthNServerConf  `json:"auth"`
		Timeout AuthNTimeoutConf `json:"timeout"`
	}
	AuthNLogConf struct {
		Dir   string `json:"dir"`
		Level string `json:"level"`
	}
	AuthNetConf struct {
		HTTP AuthNHTTPConf `json:"http"`
	}
	AuthNHTTPConf struct {
		Port        int    `json:"port"`
		UseHTTPS    bool   `json:"use_https"`
		Certificate string `json:"server_crt"`
		Key         string `json:"server_key"`
	}
	AuthNServerConf struct {
		Secret       string       `json:"secret"`
		ExpirePeriod DurationJSON `json:"expiration_time"`
	}
	AuthNTimeoutConf struct {
		Default DurationJSON `json:"default_timeout"`
	}

	AuthNConfigToUpdate struct {
		Server *AuthNServerConfToUpdate `json:"auth"`
	}
	AuthNServerConfToUpdate struct {
		Secret       *string `json:"secret"`
		ExpirePeriod *string `json:"expiration_time"`
	}
)

func (c *AuthNConfig) Lock()    { c.mtx.Lock() }
func (c *AuthNConfig) RLock()   { c.mtx.RLock() }
func (c *AuthNConfig) Unlock()  { c.mtx.Unlock() }
func (c *AuthNConfig) RUnlock() { c.mtx.RUnlock() }

func (c *AuthNConfig) Secret() string {
	c.RLock()
	secret := c.Server.Secret
	c.RUnlock()
	return secret
}

func (c *AuthNConfig) ApplyUpdate(cu *AuthNConfigToUpdate) error {
	c.Lock()
	defer c.Unlock()
	if cu.Server == nil {
		return errors.New("configuration is empty")
	}
	if cu.Server.Secret != nil {
		if *cu.Server.Secret == "" {
			return errors.New("empty secret")
		}
		c.Server.Secret = *cu.Server.Secret
	}
	if cu.Server.ExpirePeriod != nil {
		dur, err := time.ParseDuration(*cu.Server.ExpirePeriod)
		if err != nil {
			return fmt.Errorf("invalid expire time format %s, err: %v", *cu.Server.ExpirePeriod, err)
		}
		c.Server.ExpirePeriod = DurationJSON(dur)
	}
	return nil
}
