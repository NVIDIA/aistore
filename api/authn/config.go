// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

type (
	Config struct {
		Log     LogConf     `json:"log"`
		Net     NetConf     `json:"net"`
		Server  ServerConf  `json:"auth"`
		Timeout TimeoutConf `json:"timeout"`
		// private
		mu sync.RWMutex
	}
	LogConf struct {
		Dir   string `json:"dir"`
		Level string `json:"level"`
	}
	NetConf struct {
		HTTP HTTPConf `json:"http"`
	}
	HTTPConf struct {
		Certificate string `json:"server_crt"`
		Key         string `json:"server_key"`
		Port        int    `json:"port"`
		UseHTTPS    bool   `json:"use_https"`
	}
	ServerConf struct {
		Secret string       `json:"secret"`
		Expire cos.Duration `json:"expiration_time"`
		// private
		psecret *string
		pexpire *cos.Duration
	}
	TimeoutConf struct {
		Default cos.Duration `json:"default_timeout"`
	}
	ConfigToUpdate struct {
		Server *ServerConfToSet `json:"auth"`
	}
	ServerConfToSet struct {
		Secret *string `json:"secret,omitempty"`
		Expire *string `json:"expiration_time,omitempty"`
	}
	// TokenList is a list of tokens pushed by authn
	TokenList struct {
		Tokens  []string `json:"tokens"`
		Version int64    `json:"version,string"`
	}
)

var (
	_ jsp.Opts = (*Config)(nil)

	authcfgJspOpts = jsp.Plain() // TODO: use CCSign(MetaverAuthNConfig)
	authtokJspOpts = jsp.Plain() // ditto MetaverTokens
)

func (*Config) JspOpts() jsp.Options { return authcfgJspOpts }

func (c *Config) Lock()   { c.mu.Lock() }
func (c *Config) Unlock() { c.mu.Unlock() }

func (c *Config) Init() {
	c.Server.psecret = &c.Server.Secret
	c.Server.pexpire = &c.Server.Expire
}

func (c *Config) Verbose() bool {
	level, err := strconv.Atoi(c.Log.Level)
	debug.AssertNoErr(err)
	return level > 3
}

func (c *Config) Secret() string        { return *c.Server.psecret }
func (c *Config) Expire() time.Duration { return time.Duration(*c.Server.pexpire) }

func (c *Config) SetSecret(val *string) {
	c.Server.Secret = *val
	c.Server.psecret = val
}

func (c *Config) ApplyUpdate(cu *ConfigToUpdate) error {
	if cu.Server == nil {
		return errors.New("configuration is empty")
	}
	if cu.Server.Secret != nil {
		if *cu.Server.Secret == "" {
			return errors.New("secret not defined")
		}
		c.SetSecret(cu.Server.Secret)
	}
	if cu.Server.Expire != nil {
		dur, err := time.ParseDuration(*cu.Server.Expire)
		if err != nil {
			return fmt.Errorf("invalid time format %s: %v", *cu.Server.Expire, err)
		}
		v := cos.Duration(dur)
		c.Server.Expire = v
		c.Server.pexpire = &v
	}
	return nil
}
