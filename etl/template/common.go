// Package template provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package template

const (
	RuntimePy2 = "python2"
	RuntimePy3 = "python3"
)

var Runtimes map[string]Runtime

type (
	Runtime interface {
		Type() string
		PodSpec() string
		CodeFileName() string
		DepsFileName() string
	}
)

func init() {
	Runtimes = make(map[string]Runtime, 2)

	for _, runtime := range []Runtime{RuntimePython2{}, RuntimePython3{}} {
		Runtimes[runtime.Type()] = runtime
	}
}
