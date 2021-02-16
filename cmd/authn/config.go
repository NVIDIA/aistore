// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	secretKeyEnvVar = "SECRETKEY"
)

type (
	config struct {
		mtx     sync.RWMutex
		path    string
		ConfDir string        `json:"confdir"`
		Log     logConfig     `json:"log"`
		Net     netConfig     `json:"net"`
		Auth    authConfig    `json:"auth"`
		Timeout timeoutConfig `json:"timeout"`
	}
	logConfig struct {
		Dir   string `json:"dir"`
		Level string `json:"level"`
	}
	netConfig struct {
		HTTP httpConfig `json:"http"`
	}
	httpConfig struct {
		Port        int    `json:"port"`
		UseHTTPS    bool   `json:"use_https"`
		Certificate string `json:"server_crt"`
		Key         string `json:"server_key"`
	}
	authConfig struct {
		Secret          string        `json:"secret"`
		ExpirePeriodStr string        `json:"expiration_time"`
		ExpirePeriod    time.Duration `json:"-"`
	}
	timeoutConfig struct {
		DefaultStr string        `json:"default_timeout"`
		Default    time.Duration `json:"-"`
	}

	configToUpdate struct {
		Auth *authConfigToUpdate `json:"auth"`
	}
	authConfigToUpdate struct {
		Secret          *string `json:"secret"`
		ExpirePeriodStr *string `json:"expiration_time"`
	}
)

// Replaces secret key used to en(de)code tokens with a value from environment
// variable.  Useful for deploying AuthN in k8s with k8s feature 'secrets'.
func (c *config) applySecrets() {
	if val := os.Getenv(secretKeyEnvVar); val != "" {
		c.Auth.Secret = val
	}
}

func (c *config) validate() (err error) {
	if c.Auth.ExpirePeriod, err = time.ParseDuration(c.Auth.ExpirePeriodStr); err != nil {
		return fmt.Errorf("invalid expire time format %s, err: %v", c.Auth.ExpirePeriodStr, err)
	}

	return nil
}

func (c *config) secret() string {
	c.mtx.RLock()
	secret := c.Auth.Secret
	c.mtx.RUnlock()
	return secret
}

func (c *config) applyUpdate(cu *configToUpdate) error {
	if cu.Auth == nil {
		return errors.New("configuration is empty")
	}
	if cu.Auth.Secret != nil {
		if *cu.Auth.Secret == "" {
			return errors.New("empty secret")
		}
		c.Auth.Secret = *cu.Auth.Secret
	}
	if cu.Auth.ExpirePeriodStr != nil {
		dur, err := time.ParseDuration(*cu.Auth.ExpirePeriodStr)
		if err != nil {
			return fmt.Errorf("invalid expire time format %s, err: %v", *cu.Auth.ExpirePeriodStr, err)
		}
		c.Auth.ExpirePeriodStr = *cu.Auth.ExpirePeriodStr
		c.Auth.ExpirePeriod = dur
	}
	return nil
}
