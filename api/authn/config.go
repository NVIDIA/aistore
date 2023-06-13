// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
		sync.RWMutex `list:"omit"` // for cmn.IterFields
		Log          LogConf       `json:"log"`
		Net          NetConf       `json:"net"`
		Server       ServerConf    `json:"auth"`
		Timeout      TimeoutConf   `json:"timeout"`
	}
	LogConf struct {
		Dir   string `json:"dir"`
		Level string `json:"level"`
	}
	NetConf struct {
		HTTP HTTPConf `json:"http"`
	}
	HTTPConf struct {
		Port        int    `json:"port"`
		UseHTTPS    bool   `json:"use_https"`
		Certificate string `json:"server_crt"`
		Key         string `json:"server_key"`
	}
	ServerConf struct {
		Secret       string       `json:"secret"`
		ExpirePeriod cos.Duration `json:"expiration_time"`
	}
	TimeoutConf struct {
		Default cos.Duration `json:"default_timeout"`
	}
	ConfigToUpdate struct {
		Server *ServerConfToUpdate `json:"auth"`
	}
	ServerConfToUpdate struct {
		Secret       *string `json:"secret"`
		ExpirePeriod *string `json:"expiration_time"`
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

func (c *Config) Secret() (secret string) {
	c.RLock()
	secret = c.Server.Secret
	c.RUnlock()
	return
}

func (c *Config) Verbose() bool {
	level, err := strconv.Atoi(c.Log.Level)
	debug.AssertNoErr(err)
	return level > 3
}

func (c *Config) ApplyUpdate(cu *ConfigToUpdate) error {
	if cu.Server == nil {
		return errors.New("configuration is empty")
	}
	c.Lock()
	defer c.Unlock()
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
		c.Server.ExpirePeriod = cos.Duration(dur)
	}
	return nil
}
