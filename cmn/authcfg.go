// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
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

	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	AuthNConfig struct {
		sync.RWMutex `list:"omit"`    // for cmn.IterFields
		Path         string           `json:"path"`
		ConfDir      string           `json:"confdir"`
		Log          AuthNLogConf     `json:"log"`
		Net          AuthNetConf      `json:"net"`
		Server       AuthNServerConf  `json:"auth"`
		Timeout      AuthNTimeoutConf `json:"timeout"`
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
		Secret       string           `json:"secret"`
		ExpirePeriod cos.DurationJSON `json:"expiration_time"`
	}
	AuthNTimeoutConf struct {
		Default cos.DurationJSON `json:"default_timeout"`
	}
	AuthNConfigToUpdate struct {
		Server *AuthNServerConfToUpdate `json:"auth"`
	}
	AuthNServerConfToUpdate struct {
		Secret       *string `json:"secret"`
		ExpirePeriod *string `json:"expiration_time"`
	}
)

func (c *AuthNConfig) Secret() (secret string) {
	c.RLock()
	secret = c.Server.Secret
	c.RUnlock()
	return
}

func (c *AuthNConfig) ApplyUpdate(cu *AuthNConfigToUpdate) error {
	c.Lock()
	defer c.Unlock()
	if cu.Server == nil {
		return errors.New("configuration is empty")
	}
	if cu.Server.Secret != nil {
		if *cu.Server.Secret == "" {
			return errors.New("secret not defined")
		}
		c.Server.Secret = *cu.Server.Secret
	}
	if cu.Server.ExpirePeriod != nil {
		dur, err := time.ParseDuration(*cu.Server.ExpirePeriod)
		if err != nil {
			return fmt.Errorf("invalid time format %s, err: %v", *cu.Server.ExpirePeriod, err)
		}
		c.Server.ExpirePeriod = cos.DurationJSON(dur)
	}
	return nil
}
