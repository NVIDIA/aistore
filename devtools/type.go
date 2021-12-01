// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package devtools

import "net/http"

type (
	LogF func(format string, a ...interface{})

	Ctx struct {
		Client *http.Client
		Log    LogF
	}
)
