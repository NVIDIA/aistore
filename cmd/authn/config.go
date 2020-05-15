// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/jsp"
)

const (
	suNameEnvVar    = "AUTHN_SU_NAME"
	suPassEnvVar    = "AUTHN_SU_PASS"
	secretKeyEnvVar = "SECRETKEY"
)

type (
	config struct {
		path    string
		ConfDir string        `json:"confdir"`
		Cluster clusterConfig `json:"cluster"`
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
		Certificate string `json:"server_cert"`
		Key         string `json:"server_key"`
	}
	authConfig struct {
		Secret          string        `json:"secret"`
		Username        string        `json:"username"`
		Password        string        `json:"password"`
		ExpirePeriodStr string        `json:"expiration_time"`
		ExpirePeriod    time.Duration `json:"-"`
	}
	timeoutConfig struct {
		DefaultStr string        `json:"default_timeout"`
		Default    time.Duration `json:"-"`
	}
	clusterConfig struct {
		mtx  sync.RWMutex
		Conf map[string][]string `json:"conf,omitempty"` // clusterID/Alias <=> list of proxy IPs to connect
	}
)

// Replaces super user name and password, and secret key used to en(de)code
// tokens with values from environment variables is they are set.
// Useful for deploying AuthN in k8s with k8s feature 'secrets' to avoid
// having those setings in configuration file.
func (c *config) applySecrets() {
	if val := os.Getenv(suNameEnvVar); val != "" {
		c.Auth.Username = val
	}
	if val := os.Getenv(suPassEnvVar); val != "" {
		c.Auth.Password = val
	}
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

func (c *config) updateClusters(cluConf *clusterConfig) error {
	if len(cluConf.Conf) == 0 {
		return nil
	}
	for cluster, lst := range cluConf.Conf {
		if len(lst) == 0 {
			return fmt.Errorf("cluster %q URL list is empty", cluster)
		}
	}
	for cluster, lst := range cluConf.Conf {
		c.Cluster.Conf[cluster] = lst
	}
	return nil
}

func (c *config) save() error {
	return jsp.Save(conf.path, c, jsp.Plain())
}

func (c *config) clusterExists(id string) bool {
	_, ok := c.Cluster.Conf[id]
	return ok
}

func (c *config) deleteCluster(id string) {
	delete(c.Cluster.Conf, id)
}
