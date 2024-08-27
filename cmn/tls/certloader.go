// Package tls provides support for TLS.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"
	"github.com/OneOfOne/xxhash"
)

const name = "certificate-loader"

const (
	hktime = time.Hour // TODO -- FIXME: revise and remove
)

type (
	_cert struct {
		// TODO -- FIXME: add mtime, ctime, size
		tls.Certificate
		notBefore time.Time
		notAfter  time.Time
		digest    uint64
	}
	certLoader struct {
		curr     atomic.Pointer[_cert]
		certFile string
		keyFile  string
	}

	GetCertCB       func(_ *tls.ClientHelloInfo) (*tls.Certificate, error)
	GetClientCertCB func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error)
)

var (
	loader *certLoader
)

// (htrun only)
func Init(certFile, keyFile string) (err error) {
	if certFile == "" && keyFile == "" {
		return nil
	}

	debug.Assert(loader == nil)
	loader = &certLoader{certFile: certFile, keyFile: keyFile}
	if err = loader.load(true); err != nil {
		nlog.Errorln("FATAL:", err)
		loader = nil
		return err
	}
	hk.Reg(name, loader.hk, hktime) // TODO -- FIXME: revise - use notAfter
	return nil
}

func (c *certLoader) _get() *tls.Certificate { return &c.curr.Load().Certificate }

func (c *certLoader) _hello(*tls.ClientHelloInfo) (*tls.Certificate, error) { return c._get(), nil }

func GetCert() (GetCertCB, error) {
	if loader == nil {
		return nil, errors.New(name + " is <nil>")
	}
	return loader._hello, nil
}

func (c *certLoader) _info(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return c._get(), nil
}

func GetClientCert() (GetClientCertCB, error) {
	if loader == nil {
		return nil, errors.New(name + " is <nil>")
	}
	return loader._info, nil
}

func (c *certLoader) hk() time.Duration {
	if err := c.load(false); err != nil {
		nlog.Errorln(err)
	}
	return hktime
}

// TODO -- FIXME: use mtime, ctime, etc.
func (c *certLoader) load(first bool) (err error) {
	var (
		parsed _cert
	)
	parsed.Certificate, err = tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return fmt.Errorf("%s: failed to load X.509, err: %w", name, err)
	}
	if err = parsed.init(); err != nil {
		return err
	}

	if !first {
		curr := c.curr.Load()
		if curr.digest == parsed.digest {
			debug.Assert(curr.notAfter == parsed.notAfter, curr.notAfter, " vs ", parsed.notAfter)
			return nil // nothing to do
		}
	}

	c.curr.Store(&parsed)

	// log
	var sb strings.Builder
	sb.WriteString(c.certFile)
	parsed._str(&sb)
	nlog.Infoln(sb.String())

	return nil
}

///////////
// _cert //
///////////

func (parsed *_cert) _str(sb *strings.Builder) {
	sb.WriteByte('[')
	sb.WriteString(cos.FormatTime(parsed.notBefore, ""))
	sb.WriteByte(',')
	sb.WriteString(cos.FormatTime(parsed.notAfter, ""))
	sb.WriteByte(',')
	sb.WriteString(strconv.Itoa(int(parsed.digest >> 48)))
	sb.WriteString("...")
	sb.WriteByte(']')
}

func (parsed *_cert) init() (err error) {
	if parsed.Certificate.Leaf == nil {
		parsed.Certificate.Leaf, err = x509.ParseCertificate(parsed.Certificate.Certificate[0])
		if err != nil {
			return fmt.Errorf("%s: failed to parse X.509, err: %w", name, err)
		}
	}
	{
		parsed.digest = xxhash.Checksum64S(parsed.Certificate.Leaf.Raw, cos.MLCG32)
		parsed.notBefore = parsed.Certificate.Leaf.NotBefore
		parsed.notAfter = parsed.Certificate.Leaf.NotAfter
	}
	return nil
}
