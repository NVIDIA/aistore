// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	Runner interface {
		Setname(string)
		Getname() string
		Run() error
		Stop(error)
	}
	Named struct {
		name string
	}
)

func (r *Named) Setname(n string) { r.name = n }
func (r *Named) Getname() string  { return r.name }
