//go:build oteltracing

// Package tracing offers support for distributed tracing utilizing OpenTelemetry (OTEL).
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package tracing

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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

var tp *trace.TracerProvider

func loadAccessToken(tokenFilePath string) string {
	cos.AssertMsg(tokenFilePath != "", "token filepath cannot be empty")
	data, err := os.ReadFile(tokenFilePath)
	cos.AssertNoErr(err)
	return strings.TrimSpace(string(data))
}

func newExporter(conf *cmn.TracingConf) (trace.SpanExporter, error) {
	headers := map[string]string{}
	if conf.ExporterAuth.IsEnabled() {
		token := loadAccessToken(conf.ExporterAuth.TokenFile)
		headers[conf.ExporterAuth.TokenHeader] = token
	}

	options := []otlptracegrpc.Option{
		otlptracegrpc.WithHeaders(headers),
		otlptracegrpc.WithEndpoint(conf.ExporterEndpoint),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{Enabled: true}),
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
		attribute.String("pod", os.Getenv("MY_POD")), // TODO: get from consts
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

func Init(conf *cmn.TracingConf, snode *meta.Snode, version string) {
	if conf == nil || !conf.Enabled {
		cos.ExitLogf("distributed tracing not enabled (%+v)", conf)
		return
	}

	cos.AssertMsg(conf.ExporterEndpoint != "", "exporter endpoint can't be empty")
	exp, err := newExporter(conf)
	cos.AssertNoErr(err)

	tp = trace.NewTracerProvider(
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(conf.SamplerProbablity))),
		trace.WithBatcher(exp),
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

func Shutdown() {
	if tp != nil {
		return
	}
	if err := tp.Shutdown(context.Background()); err != nil {
		cos.ExitLog(err)
	}
}

func NewTraceableHandler(handler http.Handler, operation string) http.Handler {
	return otelhttp.NewHandler(handler, operation)
}

func NewTraceableClient(client *http.Client) *http.Client {
	if IsEnabled() {
		client.Transport = otelhttp.NewTransport(client.Transport)
	}
	return client
}
