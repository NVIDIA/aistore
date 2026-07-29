// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"net/http"
	"testing"
	"time"
)

func TestDownloaderVerifiesTLSCertificates(t *testing.T) {
	_, client := newDloadClients(time.Second)
	transport := client.Transport.(*http.Transport)
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("downloader TLS client disables certificate verification")
	}
}
