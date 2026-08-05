// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/core/meta"
)

func TestCreateRemoteBaseParamsTLSVerification(t *testing.T) {
	for _, name := range []string{env.AisClientCert, env.AisClientCertKey, env.AisClientCA, env.AisSkipVerifyCrt} {
		t.Setenv(name, "")
	}

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token := r.Header.Get(apc.HdrAuthorization); token != "" {
			http.Error(w, "health request included authorization", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	prev := gcfg
	prevToken := loggedUserToken
	t.Cleanup(func() {
		gcfg = prev
		loggedUserToken = prevToken
	})
	gcfg = &config.Config{Timeout: config.TimeoutConfig{HTTPTimeout: time.Second}}
	loggedUserToken = "secret-token"

	bp := createRemoteBaseParams(&meta.RemAis{URL: server.URL})
	if _, _, err := remoteHealthUptime(bp); err == nil {
		t.Fatal("expected the untrusted server certificate to be rejected")
	}

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caPath, cert, 0o600); err != nil {
		t.Fatal(err)
	}
	gcfg.Cluster.ClientCA = caPath
	bp = createRemoteBaseParams(&meta.RemAis{URL: server.URL})
	if _, _, err := remoteHealthUptime(bp); err != nil {
		t.Fatal(err)
	}

	gcfg.Cluster.ClientCA = ""
	gcfg.Cluster.SkipVerifyCrt = true
	bp = createRemoteBaseParams(&meta.RemAis{URL: server.URL})
	if _, _, err := remoteHealthUptime(bp); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRemoteBaseParamsMTLS(t *testing.T) {
	for _, name := range []string{env.AisClientCert, env.AisClientCertKey, env.AisClientCA, env.AisSkipVerifyCrt} {
		t.Setenv(name, "")
	}

	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test client CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test client"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	clientCertPath := filepath.Join(dir, "client.pem")
	clientKeyPath := filepath.Join(dir, "client-key.pem")
	if err := os.WriteFile(clientCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clientKeyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(caCert)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}
	server.StartTLS()
	defer server.Close()

	serverCAPath := filepath.Join(dir, "server-ca.pem")
	serverCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(serverCAPath, serverCert, 0o600); err != nil {
		t.Fatal(err)
	}

	prev := gcfg
	t.Cleanup(func() { gcfg = prev })
	gcfg = &config.Config{
		Timeout: config.TimeoutConfig{HTTPTimeout: time.Second},
		Cluster: config.ClusterConfig{ClientCA: serverCAPath},
	}
	bp := createRemoteBaseParams(&meta.RemAis{URL: server.URL})
	if _, _, err := remoteHealthUptime(bp); err == nil {
		t.Fatal("expected mTLS request without a client certificate to fail")
	}

	gcfg.Cluster.Certificate = clientCertPath
	gcfg.Cluster.CertKey = clientKeyPath
	bp = createRemoteBaseParams(&meta.RemAis{URL: server.URL})
	if _, _, err := remoteHealthUptime(bp); err != nil {
		t.Fatal(err)
	}
}
