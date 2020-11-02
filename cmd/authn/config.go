// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"fmt"
	"os"
	"time"
)

const (
	secretKeyEnvVar = "SECRETKEY"
)

type (
	config struct {
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
)

// Replaces super user name and password, and secret key used to en(de)code
// tokens with values from environment variables is they are set.
// Useful for deploying AuthN in k8s with k8s feature 'secrets' to avoid
// having those setings in configuration file.
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
