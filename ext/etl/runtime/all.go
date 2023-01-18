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
	Py38  = "python3.8v2"
	Py310 = "python3.10v2"
	Py311 = "python3.11v2"
)

type (
	runtime interface {
		Name() string
		PodSpec() string
		CodeEnvName() string
		DepsEnvName() string
	}
	runbase struct{}
	py38    struct{ runbase }
	py310   struct{ runbase }
	py311   struct{ runbase }
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

func GetNames() (names []string) {
	names = make([]string, 0, len(all))
	for n := range all {
		names = append(names, n)
	}
	return
}

func init() {
	all = make(map[string]runtime, 3)
	for _, r := range []runtime{py38{}, py310{}, py311{}} {
		if _, ok := all[r.Name()]; ok {
			debug.Assert(false, "duplicate type "+r.Name())
		} else {
			all[r.Name()] = r
		}
	}
}

func (runbase) CodeEnvName() string { return "AISTORE_CODE" }
func (runbase) DepsEnvName() string { return "AISTORE_DEPS" }

// container images: "aistorage/runtime_python:<TAG>"
func (py38) Name() string    { return Py38 }
func (py38) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.8v2") }

func (py310) Name() string    { return Py310 }
func (py310) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.10v2") }

func (py311) Name() string    { return Py311 }
func (py311) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.11v2") }
