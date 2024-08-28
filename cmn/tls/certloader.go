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
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"
)

const name = "certificate-loader"

type (
	xcert struct {
		tls.Certificate
		parent    *certLoader
		modTime   time.Time
		notBefore time.Time
		notAfter  time.Time
		size      int64
	}
	certLoader struct {
		xcert    atomic.Pointer[xcert]
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
	if err = loader.load(true /* starting up*/); err != nil {
		nlog.Errorln("FATAL:", err)
		loader = nil
		return err
	}
	hk.Reg(name, loader.hk, loader.hktime())
	return nil
}

func (cl *certLoader) hktime() (d time.Duration) {
	const warn = "X.509 will soon expire - remains:"
	rem := time.Until(cl.xcert.Load().notAfter)
	switch {
	case rem > 24*time.Hour:
		d = 6 * time.Hour
	case rem > 6*time.Hour:
		d = time.Hour
	case rem > time.Hour:
		d = 10 * time.Minute
	case rem > 10*time.Minute:
		nlog.Warningln(cl.certFile, warn, rem)
		d = time.Minute
	case rem > 0:
		nlog.Errorln(cl.certFile, warn, rem)
		d = min(10*time.Second, rem)
	default: // expired
		d = time.Hour
	}
	return d
}

func (cl *certLoader) _get() *tls.Certificate { return &cl.xcert.Load().Certificate }

func (cl *certLoader) _hello(*tls.ClientHelloInfo) (*tls.Certificate, error) { return cl._get(), nil }

func GetCert() (GetCertCB, error) {
	if loader == nil {
		return nil, errors.New(name + " is <nil>")
	}
	return loader._hello, nil
}

func (cl *certLoader) _info(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return cl._get(), nil
}

func GetClientCert() (GetClientCertCB, error) {
	if loader == nil {
		return nil, errors.New(name + " is <nil>")
	}
	return loader._info, nil
}

func (cl *certLoader) hk() time.Duration {
	if err := cl.load(false /* starting up*/); err != nil {
		nlog.Errorln(err)
	}
	return cl.hktime()
}

func (cl *certLoader) load(startingUp bool) (err error) {
	var (
		finfo os.FileInfo
		xcert = xcert{parent: cl}
	)
	// 1. fstat
	finfo, err = os.Stat(cl.certFile)
	if err != nil {
		return fmt.Errorf("%s: failed to fstat X.509 %q, err: %w", name, cl.certFile, err)
	}

	// 2. updated?
	if !startingUp {
		xcert := cl.xcert.Load()
		debug.Assert(xcert != nil, "expecting to load at startup")
		if finfo.ModTime() == xcert.modTime && finfo.Size() == xcert.size {
			return nil
		}
	}

	// 3. read and parse
	xcert.Certificate, err = tls.LoadX509KeyPair(cl.certFile, cl.keyFile)
	if err != nil {
		return fmt.Errorf("%s: failed to load X.509, err: %w", name, err)
	}
	if err = xcert.ini(finfo); err != nil {
		return err
	}

	// 4. keep and log
	cl.xcert.Store(&xcert)
	nlog.Infoln(xcert.String())

	return nil
}

///////////
// xcert //
///////////

func (x *xcert) String() string {
	var sb strings.Builder
	sb.WriteString(x.parent.certFile)

	sb.WriteByte('[')
	sb.WriteString(cos.FormatTime(x.notBefore, ""))
	sb.WriteByte(',')
	sb.WriteString(cos.FormatTime(x.notAfter, ""))
	sb.WriteByte(']')

	return sb.String()
}

// NOTE: second time parsing certificate (first time in tls.LoadX509KeyPair above)
// to find out valid time bounds
func (x *xcert) ini(finfo os.FileInfo) (err error) {
	if x.Certificate.Leaf == nil {
		x.Certificate.Leaf, err = x509.ParseCertificate(x.Certificate.Certificate[0])
		if err != nil {
			return fmt.Errorf("%s: failed to parse X.509 %q, err: %w", name, x.parent.certFile, err)
		}
	}
	{
		x.modTime = finfo.ModTime()
		x.size = finfo.Size()
		x.notBefore = x.Certificate.Leaf.NotBefore
		x.notAfter = x.Certificate.Leaf.NotAfter
	}
	now := time.Now()
	if now.Before(x.notBefore) || now.After(x.notAfter) {
		nlog.Errorln(x.parent.certFile, "X.509 is invalid - outside its certified time range [", x.notBefore, x.notAfter, "]")
	}
	return nil
}
