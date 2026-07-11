// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

type global struct {
	incFinished func()
}

var g global

func Init(incFinished func()) {
	g.incFinished = incFinished
}
