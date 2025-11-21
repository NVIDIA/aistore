// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

type (
	Config struct {
		Server  ServerConf  `json:"auth"`
		Log     LogConf     `json:"log"`
		Net     NetConf     `json:"net"`
		Timeout TimeoutConf `json:"timeout"`
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
		psecret *string       `json:"-"`
		pexpire *cos.Duration `json:"-"`
		Secret  string        `json:"secret"`
		Expire  cos.Duration  `json:"expiration_time"`
		PubKey  *string       `json:"public_key"`
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

func (c *Config) Init() {
	c.Server.psecret = &c.Server.Secret
	c.Server.pexpire = &c.Server.Expire
}

func (c *Config) Verbose() bool {
	if c.Log.Level == "" {
		return false
	}
	level, err := strconv.Atoi(c.Log.Level)
	debug.AssertNoErr(err)
	return level > 3
}

func (c *Config) Secret() cmn.Censored  { return cmn.Censored(*c.Server.psecret) }
func (c *Config) Expire() time.Duration { return time.Duration(*c.Server.pexpire) }
