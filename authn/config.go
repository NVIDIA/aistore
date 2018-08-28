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
	Proxy   proxyconfig   `json:"proxy"`
	Log     logconfig     `json:"log"`
	Net     netconfig     `json:"net"`
	Auth    authconfig    `json:"auth"`
	Timeout timeoutconfig `json:"timeout"`
}
type proxyconfig struct {
	URL string `json:"url"`
}
type logconfig struct {
	Dir   string `json:"logdir"`
	Level string `json:"loglevel"`
}
type netconfig struct {
	HTTP httpconfig `json:"http"`
}
type httpconfig struct {
	Port        int    `json:"port"`
	UseHTTPS    bool   `json:"use_https"`
	Certificate string `json:"server_cert"`
	Key         string `json:"server_key"`
}
type authconfig struct {
	Secret          string        `json:"secret"`
	Username        string        `json:"username"`
	Password        string        `json:"password"`
	ExpirePeriodStr string        `json:"expiration_time"`
	ExpirePeriod    time.Duration `json:"-"`
}
type timeoutconfig struct {
	DefaultStr string        `json:"default_timeout"`
	Default    time.Duration `json:"-"` // omitempty
}

func (c *config) validate() (err error) {
	if c.Auth.ExpirePeriod, err = time.ParseDuration(c.Auth.ExpirePeriodStr); err != nil {
		return fmt.Errorf("Bad expire time format %s, err: %v", c.Auth.ExpirePeriodStr, err)
	}

	return nil
}
