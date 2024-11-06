//go:build oteltracing

// Package tracing offers support for distributed tracing utilizing OpenTelemetry (OTEL).
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var _ = Describe("Tracing", func() {
	const aisVersion = "v3.33"

	var (
		exporter *tracetest.InMemoryExporter

		origExporter = newExporter
		dummySnode   = &meta.Snode{DaeID: "test", DaeType: "proxy"}

		newTestHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("-"))
		})

		expectResourceAttrs = func(attrs []attribute.KeyValue) {
			expectedAttributes := map[string]string{
				"service.name": "aistore-" + dummySnode.DaeType,
				"version":      aisVersion,
				"daemonID":     dummySnode.DaeID,
			}
			Expect(len(attrs)).NotTo(BeEquivalentTo(0))
			matched := 0
			for _, attribute := range attrs {
				value, ok := expectedAttributes[string(attribute.Key)]
				if !ok {
					continue
				}
				Expect(attribute.Value.AsString()).To(BeEquivalentTo(value))
				matched++
			}
			Expect(matched).To(BeEquivalentTo(len(expectedAttributes)))
		}
	)

	BeforeEach(func() {
		exporter = tracetest.NewInMemoryExporter()
		newExporter = func(conf *cmn.TracingConf) (trace.SpanExporter, error) {
			return exporter, nil
		}
	})

	AfterEach(func() {
		newExporter = origExporter
	})

	Describe("Server", func() {
		AfterEach(func() {
			Shutdown()
			tp = nil
		})
		It("should export server trace when tracing enabled", func() {
			Init(&cmn.TracingConf{
				ExporterEndpoint:   "dummy",
				Enabled:            true,
				SamplerProbability: 1.0,
			}, dummySnode, aisVersion)
			Expect(IsEnabled()).To(BeTrue())

			server := httptest.NewServer(NewTraceableHandler(newTestHandler, "testendpoint"))
			defer server.Close()

			_, err := http.Get(server.URL)
			Expect(err).NotTo(HaveOccurred())

			tp.ForceFlush(context.Background())

			Expect(len(exporter.GetSpans())).To(BeEquivalentTo(1))
			span := exporter.GetSpans()[0]
			expectResourceAttrs(span.Resource.Attributes())
		})

		It("should do nothing when tracing disabled", func() {
			Init(&cmn.TracingConf{
				Enabled: false,
			}, dummySnode, aisVersion)
			Expect(IsEnabled()).To(BeFalse())

			server := httptest.NewServer(NewTraceableHandler(newTestHandler, "testendpoint"))
			defer server.Close()

			_, err := http.Get(server.URL)
			Expect(err).NotTo(HaveOccurred())

			Expect(len(exporter.GetSpans())).To(BeEquivalentTo(0))
		})
	})

	Describe("Client", func() {
		AfterEach(func() {
			Shutdown()
			tp = nil
		})
		It("should export client trace when tracing enabled", func() {
			Init(&cmn.TracingConf{
				ExporterEndpoint:   "dummy",
				Enabled:            true,
				SamplerProbability: 1.0,
			}, dummySnode, aisVersion)
			Expect(IsEnabled()).To(BeTrue())

			server := httptest.NewServer(newTestHandler)
			defer server.Close()

			client := NewTraceableClient(http.DefaultClient)
			_, isOtelType := client.Transport.(*otelhttp.Transport)
			Expect(isOtelType).To(BeTrue())

			resp, err := client.Get(server.URL)
			Expect(err).NotTo(HaveOccurred())

			_, err = io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())

			tp.ForceFlush(context.Background())

			Expect(len(exporter.GetSpans())).To(BeEquivalentTo(1))
			span := exporter.GetSpans()[0]
			expectResourceAttrs(span.Resource.Attributes())
		})

		It("should do nothing when tracing disabled", func() {
			Init(&cmn.TracingConf{
				Enabled: false,
			}, dummySnode, aisVersion)
			Expect(IsEnabled()).To(BeFalse())

			server := httptest.NewServer(newTestHandler)
			defer server.Close()

			client := NewTraceableClient(http.DefaultClient)

			resp, err := client.Get(server.URL)
			Expect(err).NotTo(HaveOccurred())

			_, err = io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())

			Expect(len(exporter.GetSpans())).To(BeEquivalentTo(0))
		})
	})
})
