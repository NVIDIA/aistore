// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import (
	_ "embed"
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
		Tag() string // container images "aistore/runtime_python:<TAG>"
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

var (
	//go:embed podspec.yaml
	pyPodSpec string

	all map[string]runtime
)

func Get(runtime string) (r runtime, ok bool) {
	r, ok = all[runtime]
	return
}

func init() {
	all = make(map[string]runtime, 3)
	for _, r := range []runtime{py38{}, py310{}, py310s{}} {
		if _, ok := all[r.Tag()]; ok {
			debug.AssertMsg(false, "duplicate type "+r.Tag())
		} else {
			all[r.Tag()] = r
		}
	}
}

func (runbase) CodeEnvName() string { return "AISTORE_CODE" }
func (runbase) DepsEnvName() string { return "AISTORE_DEPS" }

func (py38) Tag() string     { return Py38 }
func (py38) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.8") }

func (py310) Tag() string     { return Py310 }
func (py310) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.10") }

func (py310s) Tag() string     { return Py310s }
func (py310s) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.10") }
