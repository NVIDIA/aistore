// Package tls provides support for TLS.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package tls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"
)

type certLoader struct {
	cert              atomic.Pointer[tls.Certificate]
	certFile, keyFile string
	retries           int
}
type GetCertCB func(_ *tls.ClientHelloInfo) (*tls.Certificate, error)
type GetClientCertCB func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error)

var (
	loader *certLoader
)

const loadInterval = 1 * time.Hour
const loadRetries = 6

func Init(certFile, keyFile string) (err error) {
	debug.Assertf(loader != nil, "TLS loader shouldn't be initialized more than once")

	// Allow creation of nil loader, that makes some check easier later.
	if certFile != "" && keyFile != "" {
		loader, err = startLoader(certFile, keyFile)
	}
	if err != nil {
		nlog.Warningln("Fail to load TLS certificates at start up")
		loader = nil
	}
	return err
}

func GetCert() GetCertCB {
	if loader == nil {
		return nil
	}
	return func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) { return loader.getCert(), nil }
}

func GetClientCert() GetClientCertCB {
	if loader == nil {
		return nil
	}
	return func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) { return loader.getCert(), nil }
}

func IsLoaderSet() bool {
	return loader != nil
}

// startLoader will monitor files of certPath and keyPath and reload certificates
// if any of the two was updated.
func startLoader(certPath, keyPath string) (c *certLoader, err error) {
	c = &certLoader{
		certFile: certPath,
		keyFile:  keyPath,
		retries:  0,
	}
	// Immediately try to load existing certs.
	if err := c.load(); err != nil {
		return nil, err
	}

	hk.Reg("tlsloader", c.housekeep, loadInterval)
	return
}

func (c *certLoader) housekeep() time.Duration {
	if err := c.load(); err != nil {
		c.retries++
		if c.retries > loadRetries {
			nlog.Errorf("unable to load TLS certificate: %v", err)
			debug.AssertNoErr(err)
		}
	} else {
		c.retries = 0
	}
	return loadInterval
}

func (c *certLoader) getCert() *tls.Certificate {
	return c.cert.Load()
}

func (c *certLoader) load() error {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		nlog.Errorln("failed to load X509 key pair:", err)
		return err
	}

	// Compare fingerprints of previous and current certificates, and log if updated
	if prevCert := c.cert.Load(); prevCert != nil {
		var (
			prevFringerprint string
			curFingerprint   string
		)
		if prevFringerprint, err = fingerprint(prevCert); err != nil {
			nlog.Errorln(err)
		}
		if curFingerprint, err = fingerprint(&cert); err != nil {
			nlog.Errorln(err)
		}
		if prevFringerprint != curFingerprint {
			nlog.Infof("Certificate has changed. New fingerprint: %s", curFingerprint)
		}
	}
	c.cert.Store(&cert)
	return nil
}

// Function to compute the fingerprint of a TLS certificate
func fingerprint(tlsCert *tls.Certificate) (string, error) {
	if tlsCert.Leaf == nil {
		// Parse the leaf certificate if not already parsed
		var err error
		tlsCert.Leaf, err = x509.ParseCertificate(tlsCert.Certificate[0])
		if err != nil {
			return "", errors.New("error on parsing TLS certificate")
		}
	}

	hash := sha256.Sum256(tlsCert.Leaf.Raw)
	return hex.EncodeToString(hash[:]), nil
}
