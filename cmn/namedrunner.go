// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	Runner interface {
		SetRunName(string)
		GetRunName() string
		Run() error
		Stop(error)
	}
	Named struct {
		name string
	}
	NamedID struct {
		Named
		id string
	}
)

func (r *Named) SetRunName(n string) { r.name = n }
func (r *Named) GetRunName() string  { return r.name }
func (r *NamedID) SetID(id string)   { Assert(r.id == ""); r.id = id } // at construction
func (r *NamedID) ID() string        { return r.id }
