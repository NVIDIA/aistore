// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

// supported runtimes
const (
	Python2 = "python2"
	Python3 = "python3"
)

var Runtimes map[string]runtime

type (
	runtime interface {
		Type() string
		PodSpec() string
		CodeEnvName() string
		DepsEnvName() string
	}
)

func init() {
	Runtimes = make(map[string]runtime, 2)

	for _, r := range []runtime{py2{}, py3{}} {
		Runtimes[r.Type()] = r
	}
}
