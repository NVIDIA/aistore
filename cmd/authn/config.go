// Authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"fmt"
	"time"
)

type config struct {
	ConfDir string        `json:"confdir"`
	Proxy   proxyConfig   `json:"proxy"`
	Log     logConfig     `json:"log"`
	Net     netConfig     `json:"net"`
	Auth    authConfig    `json:"auth"`
	Timeout timeoutConfig `json:"timeout"`
}
type proxyConfig struct {
	URL string `json:"url"`
}
type logConfig struct {
	Dir   string `json:"dir"`
	Level string `json:"level"`
}
type netConfig struct {
	HTTP httpConfig `json:"http"`
}
type httpConfig struct {
	Port        int    `json:"port"`
	UseHTTPS    bool   `json:"use_https"`
	Certificate string `json:"server_cert"`
	Key         string `json:"server_key"`
}
type authConfig struct {
	Secret          string        `json:"secret"`
	Username        string        `json:"username"`
	Password        string        `json:"password"`
	ExpirePeriodStr string        `json:"expiration_time"`
	ExpirePeriod    time.Duration `json:"-"`
}
type timeoutConfig struct {
	DefaultStr string        `json:"default_timeout"`
	Default    time.Duration `json:"-"` // omitempty
}

func (c *config) validate() (err error) {
	if c.Auth.ExpirePeriod, err = time.ParseDuration(c.Auth.ExpirePeriodStr); err != nil {
		return fmt.Errorf("invalid expire time format %s, err: %v", c.Auth.ExpirePeriodStr, err)
	}

	return nil
}
