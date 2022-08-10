// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	Py38   = "python3.8"
	Py310  = "python3.10"
	Py310s = "python3.10.streaming"
)

type (
	runtime interface {
		Type() string
		PodSpec() string
		CodeEnvName() string
		DepsEnvName() string
	}
	runbase struct {
	}
	py38 struct {
		runbase
	}
	py310 struct {
		runbase
	}
	py310s struct {
		runbase
	}
)

var Runtimes map[string]runtime

func init() {
	Runtimes = make(map[string]runtime, 3)

	for _, r := range []runtime{py38{}, py310{}, py310s{}} {
		if _, ok := Runtimes[r.Type()]; ok {
			debug.AssertMsg(false, "duplicate type "+r.Type())
		} else {
			Runtimes[r.Type()] = r
		}
	}
}

func (runbase) CodeEnvName() string { return "AISTORE_CODE" }
func (runbase) DepsEnvName() string { return "AISTORE_DEPS" }

func (py38) Type() string    { return Py38 }
func (py38) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<VERSION>", "3.8") }

func (py310) Type() string    { return Py310 }
func (py310) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<VERSION>", "3.10") }

func (py310s) Type() string    { return Py310s }
func (py310s) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<VERSION>", "3.10") }
