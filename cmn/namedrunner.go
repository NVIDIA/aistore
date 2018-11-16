/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package cmn provides common low-level types and utilities for all dfcpub projects
package cmn

type (
	Runner interface {
		Setname(string)
		Getname() string
		Run() error
		Stop(error)
	}
	Configured interface {
		Getconf() *Config
		Setconf(*Config)
	}
	Named struct {
		name string
	}
	NamedConfigured struct {
		Named
		config *Config
	}
)

func (r *Named) Setname(n string) { r.name = n }
func (r *Named) Getname() string  { return r.name }

// TODO: CoW versioning/cloning WTP
func (r *NamedConfigured) Getconf() *Config  { return r.config }
func (r *NamedConfigured) Setconf(c *Config) { r.config = c }
