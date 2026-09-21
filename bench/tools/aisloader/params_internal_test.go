// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// (*params).validate: verifies representative valid settings and incompatible combinations.
func TestParamsValidate(t *testing.T) {
	newValidParams := func() *params {
		return &params{
			workloadParams: workloadParams{
				putPct: 50,
			},
			sizeCksumParams: sizeCksumParams{
				minSize:   cos.KiB,
				maxSize:   cos.MiB,
				cksumType: cos.ChecksumOneXxh,
			},
			loaderParams: loaderParams{
				loaderID: "0",
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*params)
		wantErr string
	}{
		{name: "valid"},
		{
			name: "size range",
			mutate: func(p *params) {
				p.minSize, p.maxSize = cos.MiB, cos.KiB
			},
			wantErr: "min and max size",
		},
		{
			name:    "PUT percentage",
			mutate:  func(p *params) { p.putPct = 101 },
			wantErr: "PUT percent 101",
		},
		{
			name:    "update percentage",
			mutate:  func(p *params) { p.updateExistingPct = -1 },
			wantErr: "invalid -1 percentage",
		},
		{
			name:    "multipart chunks",
			mutate:  func(p *params) { p.multipartChunks = -1 },
			wantErr: "multipart-chunks -1",
		},
		{
			name:    "multipart percentage",
			mutate:  func(p *params) { p.multipartPct = 101 },
			wantErr: "pctmultipart 101",
		},
		{
			name: "MPD stream with PUT",
			mutate: func(p *params) {
				p.mpdStreamPct = 10
			},
			wantErr: "cannot combine pctmpdstream 10 with pctput 50",
		},
		{
			name: "GetBatch with checksum verification",
			mutate: func(p *params) {
				p.getBatchSize = 16
				p.verifyHash = true
			},
			wantErr: "'--get-batchsize' cannot be used with verifyhash",
		},
		{
			name:    "GetBatch size",
			mutate:  func(p *params) { p.getBatchSize = 1001 },
			wantErr: "value 1001 (must be 1-1000)",
		},
		{
			name:    "negative stats interval",
			mutate:  func(p *params) { p.statsShowInterval = -1 },
			wantErr: "stats show interval -1",
		},
		{
			name:    "empty loader ID",
			mutate:  func(p *params) { p.loaderID = "" },
			wantErr: "loaderID can't be empty",
		},
		{
			name:    "too many virtual directories",
			mutate:  func(p *params) { p.numVirtDirs = 100_000 },
			wantErr: "'--num-subdirs' (100000)",
		},
		{
			name: "direct S3 with random proxy",
			mutate: func(p *params) {
				s3Endpoint = "http://localhost:9000"
				p.randomProxy = true
			},
			wantErr: "'-s3endpoint' and '-randomproxy' are mutually exclusive",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetValidationGlobals(t)

			p := newValidParams()
			if test.mutate != nil {
				test.mutate(p)
			}

			err := p.validate()
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validate() error = %v, expected substring %q", err, test.wantErr)
			}
		})
	}
}

func resetValidationGlobals(t *testing.T) {
	t.Helper()

	oldS3Endpoint, oldIP, oldPort := s3Endpoint, ip, port
	oldRandomNames := useRandomObjName
	oldSuffixLen, oldSuffixID := suffixIDMaskLen, suffixID
	oldETLSpec := etlInitSpec

	s3Endpoint = ""
	ip = defaultClusterIP
	port = "8080"
	useRandomObjName = false
	suffixIDMaskLen, suffixID = 0, 0
	etlInitSpec = nil

	t.Cleanup(func() {
		s3Endpoint, ip, port = oldS3Endpoint, oldIP, oldPort
		useRandomObjName = oldRandomNames
		suffixIDMaskLen, suffixID = oldSuffixLen, oldSuffixID
		etlInitSpec = oldETLSpec
	})
}
