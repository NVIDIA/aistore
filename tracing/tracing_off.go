//go:build !oteltracing

// Package tracing offers support for distributed tracing utilizing OpenTelemetry (OTEL).
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package tracing

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
)

func IsEnabled() bool { return false }

func Init(*cmn.TracingConf, *meta.Snode, any, string) {}

func Shutdown() {}

func NewTraceableHandler(handler http.Handler, _ string) http.Handler { return handler }

func NewTraceableClient(client *http.Client) *http.Client { return client }
