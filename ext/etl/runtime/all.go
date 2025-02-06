// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import (
	_ "embed"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	Py39  = "python3.9v2"
	Py310 = "python3.10v2"
	Py311 = "python3.11v2"
	Py312 = "python3.12v2"
	Py313 = "python3.13v2"
)

type (
	runtime interface {
		Name() string
		PodSpec() string
		CodeEnvName() string
		DepsEnvName() string
	}
	runbase struct{}
	py39    struct{ runbase }
	py310   struct{ runbase }
	py311   struct{ runbase }
	py312   struct{ runbase }
	py313   struct{ runbase }
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
	all = make(map[string]runtime, 5)
	for _, r := range []runtime{py39{}, py310{}, py311{}, py312{}, py313{}} {
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
func (py39) Name() string    { return Py39 }
func (py39) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.9v2") }

func (py310) Name() string    { return Py310 }
func (py310) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.10v2") }

func (py311) Name() string    { return Py311 }
func (py311) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.11v2") }

func (py312) Name() string    { return Py312 }
func (py312) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.12v2") }

func (py313) Name() string    { return Py313 }
func (py313) PodSpec() string { return strings.ReplaceAll(pyPodSpec, "<TAG>", "3.13v2") }
