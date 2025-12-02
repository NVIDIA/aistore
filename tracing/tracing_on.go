//go:build oteltracing

// Package tracing offers support for distributed tracing utilizing OpenTelemetry (OTEL).
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tracing

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Init() enables OpenTelemetry tracing for an AIS node.
// Semantics and constraints:
// - Build-time feature:
//     This function is only compiled when AIS is built with the `oteltracing`
//     build tag (see tracing_off.go for the no-op stub used otherwise).
//     Callers invoke Init() unconditionally.
// - Config handling:
//     The config.Tracing field is a *cmn.TracingConf pointer. A nil conf
//     or a conf with Enabled == false is treated as "tracing disabled":
//       - we log an informational message and return without error.
//     This makes it safe to omit the `tracing` section from ais.conf entirely.
// - Exporter endpoint:
//     When tracing is enabled, ExporterEndpoint MUST be non-empty. If it is
//     empty we log an error, trip a debug.Assert in debug builds, and return
//     without installing a tracer provider. The node continues to run with
//     tracing effectively disabled.
// - Error handling (is intentionally tolerant):
//     Any failure to construct the exporter (newExporter) or resource will
//     trip debug assertions but will not crash non-debug builds. In those
//     cases, tp remains nil and no global tracer provider is installed.

var (
	tp *trace.TracerProvider
)

func Init(conf *cmn.TracingConf, snode *meta.Snode, exp any /* trace.SpanExporter */, version string) {
	if conf == nil || !conf.Enabled {
		nlog.Infof("distributed tracing not enabled (%+v)", conf)
		return
	}
	if conf.ExporterEndpoint == "" {
		nlog.Errorln("exporter endpoint can't be empty")
		debug.Assert(false)
		return
	}
	var (
		exporter trace.SpanExporter
		err      error
		ok       bool
	)
	if exp == nil {
		// default trace.SpanExporter
		exporter, err = newExporter(conf)
		if err != nil {
			nlog.Errorln("failed to start exporter:", err)
			debug.AssertNoErr(err)
			return
		}
	} else {
		// unit test
		exporter, ok = exp.(trace.SpanExporter)
		debug.Assertf(ok, "invalid exporter type %T", exp)
	}
	tp = trace.NewTracerProvider(
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(conf.SamplerProbability))),
		trace.WithBatcher(exporter),
		trace.WithResource(newResource(conf, snode, version)),
	)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	otel.SetTracerProvider(tp)
}

func loadAccessToken(tokenFilePath string) string {
	debug.Assert(tokenFilePath != "", "token filepath cannot be empty")
	data, err := os.ReadFile(tokenFilePath)
	debug.AssertNoErr(err)
	return strings.TrimSpace(string(data))
}

func newExporter(conf *cmn.TracingConf) (trace.SpanExporter, error) {
	var headers map[string]string
	if conf.ExporterAuth.IsEnabled() {
		token := loadAccessToken(conf.ExporterAuth.TokenFile)
		headers = map[string]string{conf.ExporterAuth.TokenHeader: token}
	}

	options := []otlptracegrpc.Option{
		otlptracegrpc.WithHeaders(headers),
		otlptracegrpc.WithEndpoint(conf.ExporterEndpoint),
	}
	if conf.SkipVerify {
		options = append(options, otlptracegrpc.WithInsecure())
	}

	return otlptracegrpc.New(context.Background(), options...)
}

// newResource returns a resource describing this application.
func newResource(conf *cmn.TracingConf, snode *meta.Snode, version string) *resource.Resource {
	servicePrefix := strings.TrimSuffix(conf.ServiceNamePrefix, "-")
	if servicePrefix == "" {
		servicePrefix = "aistore" // TODO -- constant
	}
	serviceName := servicePrefix + "-" + snode.DaeType
	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(serviceName),
		attribute.String("version", version),
		attribute.String("daemonID", snode.DaeID),
		attribute.String("pod", os.Getenv(env.AisK8sPod)),
	}
	for k, v := range conf.ExtraAttributes {
		attrs = append(attrs, attribute.String(k, v))
	}
	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			attrs...,
		),
	)
	return r
}

func IsEnabled() bool {
	return tp != nil
}

// used in tests only
func ForceFlush() { tp.ForceFlush(context.Background()) }

func Shutdown() {
	if !IsEnabled() {
		return
	}
	if err := tp.Shutdown(context.Background()); err != nil {
		cos.ExitLog(err)
	}
	tp = nil
}

func NewTraceableHandler(handler http.Handler, operation string) http.Handler {
	if IsEnabled() {
		return otelhttp.NewHandler(handler, operation)
	}
	return handler
}

func NewTraceableClient(client *http.Client) *http.Client {
	if IsEnabled() {
		client.Transport = otelhttp.NewTransport(client.Transport)
	}
	return client
}
