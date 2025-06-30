// Package config provides types and functions to configure AIS CLI.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// default pathname: $HOME/.config/ais/cli

const (
	urlFmt           = "%s://%s:%d"
	defaultAISIP     = "127.0.0.1"
	defaultAISPort   = 8080
	defaultAuthNPort = 52001
	defaultDockerIP  = "172.50.0.2"
)

const tipReset = "To reset config to system defaults, run 'ais config reset'. Or, edit the JSON file (above) directly."

type (
	ClusterConfig struct {
		URL               string `json:"url"`
		DefaultAISHost    string `json:"default_ais_host"`
		DefaultDockerHost string `json:"default_docker_host"`
		// TLS
		Certificate   string `json:"client_crt"`     // X.509 certificate
		CertKey       string `json:"client_crt_key"` // X.509 key
		ClientCA      string `json:"client_ca_tls"`  // #6410
		SkipVerifyCrt bool   `json:"skip_verify_crt"`
	}
	TimeoutConfig struct {
		TCPTimeoutStr  string        `json:"tcp_timeout"`
		TCPTimeout     time.Duration `json:"-"`
		HTTPTimeoutStr string        `json:"http_timeout"`
		HTTPTimeout    time.Duration `json:"-"`
	}
	AuthConfig struct {
		URL string `json:"url"`
	}
	AliasConfig cos.StrKVs // (see DefaultAliasConfig below)

	// all of the above
	Config struct {
		Cluster         ClusterConfig `json:"cluster"`
		Timeout         TimeoutConfig `json:"timeout"`
		Auth            AuthConfig    `json:"auth"`
		Aliases         AliasConfig   `json:"aliases"`
		DefaultProvider string        `json:"default_provider,omitempty"` // NOTE: not supported yet (see app.go)
		NoColor         bool          `json:"no_color"`
		Verbose         bool          `json:"verbose"` // more warnings, errors with backtraces and details
		NoMore          bool          `json:"no_more"`
	}
)

var (
	ConfigDir     string
	defaultConfig Config

	DefaultAliasConfig = AliasConfig{
		// object
		"get":      "object get",
		"put":      "object put",
		"rmo":      "object rm",
		"prefetch": "object prefetch", // same as "job start prefetch"
		// bucket
		"ls":     "bucket ls",
		"create": "bucket create",
		"cp":     "bucket cp",
		"rmb":    "bucket rm",
		"evict":  "bucket evict",
		// job
		"start":         "job start",
		"stop":          "job stop",
		"wait":          "job wait",
		apc.ActDsort:    "job start " + apc.ActDsort,
		apc.ActDownload: "job start " + apc.ActDownload,
		apc.ActBlobDl:   "job start " + apc.ActBlobDl,
		// storage
		"space-cleanup": "storage cleanup",
		"scrub":         "storage validate",
	}
)

func init() {
	// $HOME/.config/ais/cli
	ConfigDir = cos.HomeConfigDir(fname.HomeCLI)
	proto := "http"
	if value := os.Getenv(env.AisUseHTTPS); cos.IsParseBool(value) {
		proto = "https"
	}
	aisURL := fmt.Sprintf(urlFmt, proto, defaultAISIP, defaultAISPort)
	defaultConfig = Config{
		Cluster: ClusterConfig{
			URL:               aisURL,
			DefaultAISHost:    aisURL,
			DefaultDockerHost: fmt.Sprintf(urlFmt, proto, defaultDockerIP, defaultAISPort),
			SkipVerifyCrt:     cos.IsParseBool(os.Getenv(env.AisSkipVerifyCrt)),
		},
		Timeout: TimeoutConfig{
			TCPTimeoutStr:  "60s",
			TCPTimeout:     60 * time.Second,
			HTTPTimeoutStr: "0s",
			HTTPTimeout:    0,
		},
		Auth: AuthConfig{
			URL: fmt.Sprintf(urlFmt, proto, defaultAISIP, defaultAuthNPort),
		},
		Aliases:         DefaultAliasConfig,
		DefaultProvider: apc.AIS,
		NoColor:         false,
		NoMore:          false,
	}
}

/////////////////
// AliasConfig //
/////////////////

// compare w/ showAliasHandler(*cli.Context)
func (a AliasConfig) String() string { return a.Str("\t ") }

func (a AliasConfig) Str(indent string) (s string) {
	b := cos.StrKVs(a)
	keys := b.Keys()
	sort.Slice(keys, func(i, j int) bool { return b[keys[i]] < b[keys[j]] })

	var (
		n    int
		next bool
	)
	for _, k := range keys {
		if next {
			s += "; "
			if len(s)-n > 60 {
				s += "\n" + indent
				n = len(s)
			}
		}
		s += k + " => '" + a[k] + "'"
		next = true
	}
	return
}

////////////
// Config //
////////////

func (c *Config) validate() (err error) {
	if c.Timeout.TCPTimeout, err = time.ParseDuration(c.Timeout.TCPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.tcp_timeout format %q: %v", c.Timeout.TCPTimeoutStr, err)
	}
	if c.Timeout.HTTPTimeout, err = time.ParseDuration(c.Timeout.HTTPTimeoutStr); err != nil {
		return fmt.Errorf("invalid timeout.http_timeout format %q: %v", c.Timeout.HTTPTimeoutStr, err)
	}
	if c.DefaultProvider != "" && !apc.IsProvider(c.DefaultProvider) {
		return fmt.Errorf("invalid default_provider value %q, expected one of [%s]", c.DefaultProvider, apc.Providers)
	}

	if c.Aliases == nil {
		c.Aliases = DefaultAliasConfig
	}
	return nil
}

func (c *Config) WarnTLS(server string) {
	err := cos.Stat(c.Cluster.Certificate)
	if err == nil {
		err = cos.Stat(c.Cluster.CertKey)
	}
	if err == nil {
		return
	}
	path := filepath.Join(ConfigDir, fname.CliConfig)

	fmt.Fprintln(os.Stderr, "Warning: CLI may have a problem communicating with "+server+" - CLI config at")
	fmt.Fprintf(os.Stderr, "%s contains invalid public/private key pair (%q,%q), err: %v\n\n",
		path, c.Cluster.Certificate, c.Cluster.CertKey, err)
}

func Load(args []string, reset string) (*Config, error) {
	var (
		cfg          = &Config{}
		resetAndExit bool
	)
	if err := jsp.LoadAppConfig(ConfigDir, fname.CliConfig, cfg); err != nil {
		if !os.IsNotExist(err) {
			if !cos.StringInSlice(reset, args) {
				path := filepath.Join(ConfigDir, fname.CliConfig)
				return nil, fmt.Errorf("failed to load CLI config %q: %v\n\n%s", path, err, tipReset)
			}
			resetAndExit = true // NOTE: just go ahead and reset
		}

		// revert to default config
		if err = Save(&defaultConfig); err != nil {
			return nil, err // (unlikely)
		}
		if resetAndExit {
			fmt.Println("Done.")
			os.Exit(0)
		}
		cfg = &defaultConfig
		return cfg, err
	}

	if err := cfg.validate(); err != nil {
		path := filepath.Join(ConfigDir, fname.CliConfig)
		if cos.StringInSlice(reset, args) {
			fmt.Fprintf(os.Stderr, "CLI config at %s: %v\n", path, err)
			fmt.Fprint(os.Stderr, "Resetting config to system defaults...\t")
			time.Sleep(time.Second)
			if err = Save(&defaultConfig); err != nil {
				return nil, err // (unlikely)
			}
			fmt.Println("Done.")
			os.Exit(0)
		}

		return nil, fmt.Errorf("CLI config at %s: %v\n\n%s", path, err, tipReset)
	}
	return cfg, nil
}

func Reset() error {
	return Save(&defaultConfig)
}

func Save(cfg *Config) error {
	err := jsp.SaveAppConfig(ConfigDir, fname.CliConfig, cfg)
	if err != nil {
		return fmt.Errorf("failed to save config file: %v", err)
	}
	return nil
}

func Path() string {
	return filepath.Join(ConfigDir, fname.CliConfig)
}
