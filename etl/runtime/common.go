// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

// supported runtimes
const (
	Python2   = "python2"
	Python3   = "python3"
	Python36  = "python3.6"
	Python38  = "python3.8"
	Python310 = "python3.10"
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
	Runtimes = make(map[string]runtime, 5)

	for _, r := range []runtime{py2{}, py3{}, py36{}, py38{}, py310{}} {
		Runtimes[r.Type()] = r
	}
}
