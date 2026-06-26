// Package certloader loads and reloads X.509 certs.
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package certloader

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"
)

//
// related sources: api/x509.go, ais/x509.go, and cmd/cli/cli/x509.go
//

// Loader names. DfltCL is also the hk registration key - unchanged, do not rename.
const (
	DfltCL = "tls-cert-loader"     // intra-cluster server & client cert; default for the pub listeners
	PubCL  = "tls-cert-loader-pub" // user-facing (net.http.pub) cert, when separately configured
)

const (
	dfltTimeInvalid = time.Hour
	warnSoonExpire  = 3 * 24 * time.Hour
)

const fmtErrExpired = "%s: %s expired (valid until %v)"

// node-level flags for certs, shared by all loaders
const certFlags = cos.CertificateExpired | cos.CertificateInvalid | cos.CertWillSoonExpire

type (
	xcert struct {
		tls.Certificate
		parent    *CertLoader
		modTime   time.Time
		notBefore time.Time
		notAfter  time.Time
		size      int64
	}
	CertLoader struct {
		mgr      *Manager
		xcert    atomic.Pointer[xcert]
		certFile string
		keyFile  string
		name     string
		prefix   string // Props key prefix; "" for the default loader
		flags    atomic.Int64
	}

	// Manager owns the (fixed) set of loaders and the publishing of node flags
	Manager struct {
		tstats  cos.StatsUpdater
		loaders []*CertLoader
		mu      sync.Mutex
	}

	// tls.Config.GetCertificate
	GetCertCB func(_ *tls.ClientHelloInfo) (*tls.Certificate, error)

	// tls.Config.GetClientCertificate
	GetClientCertCB func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error)

	errExpired struct {
		msg string
	}
)

// static construction - see Init
var (
	Default *CertLoader
	Pub     *CertLoader
	Mgr     *Manager
)

var ErrNoCerts = errors.New("no TLS certificate loaders configured")

// when config USE_HTTPS: Init the package (and its default cert loader);
// the pub loader, when separately configured, is initialized _after_ this call (see Pub.Init).
func Init(tstats cos.StatsUpdater, certFile, keyFile string) error {
	debug.Assert(tstats != nil)
	debug.Assert(Mgr == nil, "certloader.Init called twice")

	Default = &CertLoader{name: DfltCL}
	Pub = &CertLoader{name: PubCL, prefix: "pub."}
	Mgr = newManager(Default, Pub)
	Mgr.tstats = tstats

	return Default.Init(certFile, keyFile)
}

/////////////
// Manager //
/////////////

func newManager(ls ...*CertLoader) *Manager {
	m := &Manager{loaders: ls}
	for i, cl := range ls {
		debug.Assert(cl.name != "", "unnamed cert loader #", i)
		for _, prev := range ls[:i] {
			debug.Assert(prev.name != cl.name, "duplicate cert loader name: ", cl.name)
		}
		cl.mgr = m
	}
	return m
}

// (Re)load every configured loader (errors joined, if any)
func (m *Manager) LoadAll() error {
	if m == nil {
		return ErrNoCerts
	}
	var (
		errs []error
		n    int
	)
	for _, cl := range m.loaders {
		if !cl.configured() {
			continue
		}
		n++
		if err := cl.load(); err != nil {
			errs = append(errs, err)
		}
	}
	if n == 0 {
		return ErrNoCerts
	}
	return errors.Join(errs...)
}

// Props: aggregated `what=certificate` view.
// The default loader's keys are unprefixed (back-compat with cmd/cli/cli/x509.go);
// any additional loader contributes its keys under cl.prefix.
// Nil receiver: `what=certificate` on a node that runs plain HTTP.
func (m *Manager) Props() (out cos.StrKVs) {
	if m == nil {
		return nil
	}
	for _, cl := range m.loaders {
		if !cl.configured() {
			continue
		}
		props := cl.Props()
		if len(props) == 0 {
			continue
		}
		if out == nil {
			out = make(cos.StrKVs, len(props))
		}
		for k, v := range props {
			out[cl.prefix+k] = v
		}
	}
	return out
}

// union of all loader cert flags -> node-level state.flags, so a shared cert bit
// is cleared only when no loader needs it
func (m *Manager) publish() {
	debug.Assert(m.tstats != nil, "certloader.Init not called")
	m.mu.Lock()
	union := cos.NodeStateFlags(0)
	for _, cl := range m.loaders {
		union |= cos.NodeStateFlags(cl.flags.Load())
	}
	m.tstats.SetClrFlag(cos.NodeAlerts, union&certFlags, certFlags&^union)
	m.mu.Unlock()
}

////////////////
// CertLoader //
////////////////

func (cl *CertLoader) Init(certFile, keyFile string) (err error) {
	debug.Assert(cl.mgr != nil && cl.mgr.tstats != nil, cl.name, ": certloader.Init not called")
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("%s: missing certificate or key (%q, %q)", cl.name, certFile, keyFile)
	}
	cl.certFile, cl.keyFile = certFile, keyFile
	if err = cl.load(); err != nil {
		nlog.Errorln("FATAL:", err)
		return err
	}

	hk.Reg(cl.name, cl.hk, cl.hktime())
	return nil
}

func (cl *CertLoader) configured() bool { return cl.certFile != "" }

// load triggers certificate file read from disk
func (cl *CertLoader) load() (err error) {
	if err = cl.do(false /*compare*/); err == nil {
		return nil
	}
	if isExpired(err) {
		cl.setFlags(cos.CertificateExpired)
	} else {
		cl.setFlags(cos.CertificateInvalid)
	}
	return err
}

// record this loader's own cert flags, defer stats publishing to manager
func (cl *CertLoader) setFlags(flags cos.NodeStateFlags) {
	cl.flags.Store(int64(flags & certFlags))
	cl.mgr.publish()
}

func (cl *CertLoader) Props() (out cos.StrKVs) {
	if cl == nil {
		return nil
	}
	flags := cos.NodeStateFlags(cl.flags.Load())
	if flags.IsAnySet(cos.CertificateInvalid | cos.CertificateExpired) {
		out = make(cos.StrKVs, 1)
		flags &= cos.CertificateInvalid | cos.CertificateExpired
		out["error"] = flags.Str()
		return out
	}
	xcert := cl.xcert.Load()
	if xcert == nil {
		return nil
	}

	out = make(cos.StrKVs, 6)
	leaf := xcert.Certificate.Leaf
	{
		out["version"] = strconv.Itoa(leaf.Version)
		out["issued-by (CN)"] = leaf.Issuer.CommonName
		out["signature-algorithm"] = leaf.SignatureAlgorithm.String()
		out["public-key-algorithm"] = leaf.PublicKeyAlgorithm.String()
		if leaf.SerialNumber != nil {
			out["serial-number"] = leaf.SerialNumber.String()
		}
		out["valid"] = "from " + fmtTime(leaf.NotBefore)
		out["valid"] += " to " + fmtTime(leaf.NotAfter)

		if flags.IsSet(cos.CertWillSoonExpire) {
			out["warning"] = cos.CertWillSoonExpire.Str()
		}
	}

	return out
}

func (cl *CertLoader) GetCert() (GetCertCB, error) {
	if err := cl.ready(); err != nil {
		return nil, err
	}
	return cl._hello, nil
}

func (cl *CertLoader) GetClientCert() (GetClientCertCB, error) {
	if err := cl.ready(); err != nil {
		return nil, err
	}
	return cl._info, nil
}

//
// private
//

func (cl *CertLoader) hk(int64) time.Duration {
	if err := cl.do(true /*compare*/); err != nil {
		nlog.Errorln(err)
	}
	return cl.hktime()
}

func (cl *CertLoader) hktime() (d time.Duration) {
	flags := cos.NodeStateFlags(cl.flags.Load())
	if flags.IsAnySet(cos.CertificateExpired | cos.CertificateInvalid) {
		return dfltTimeInvalid
	}
	xcert := cl.xcert.Load()
	if xcert == nil {
		return dfltTimeInvalid
	}

	// (still) valid
	const warn = "X.509 will soon expire - remains:"
	rem := time.Until(xcert.notAfter)
	switch {
	case rem > hk.DayInterval:
		d = 6 * time.Hour
		if rem < warnSoonExpire {
			cl.setFlags(cos.CertWillSoonExpire)
		}
	case rem > 6*time.Hour:
		d = time.Hour
	case rem > time.Hour:
		d = 10 * time.Minute
	case rem > 10*time.Minute:
		nlog.Warningln(cl.certFile, warn, rem)
		d = time.Minute
	case rem > 0:
		nlog.Errorln(cl.certFile, warn, rem)
		d = time.Minute
	default: // expired
		cl.setFlags(cos.CertificateExpired)
		d = dfltTimeInvalid
	}
	return d
}

// nil-safe: a first-ever load of an already-expired cert sets the flag with
// no xcert stored
func (cl *CertLoader) errorf() error {
	flags := cos.NodeStateFlags(cl.flags.Load())
	switch {
	case flags.IsSet(cos.CertificateInvalid):
		return fmt.Errorf("%s: (%s, %s) is invalid", cl.name, cl.certFile, cl.keyFile)
	case flags.IsSet(cos.CertificateExpired):
		var notAfter time.Time
		if xcert := cl.xcert.Load(); xcert != nil {
			notAfter = xcert.notAfter
		}
		msg := fmt.Sprintf(fmtErrExpired, cl.name, cl.certFile, notAfter)
		return &errExpired{msg}
	default:
		return nil
	}
}

// initialized and (currently) usable
// (nil receiver covers both GetCert and GetClientCert)
func (cl *CertLoader) ready() error {
	if cl == nil {
		return errors.New("cert loader not initialized")
	}
	if err := cl.errorf(); err != nil {
		return err
	}
	if cl.xcert.Load() == nil {
		return fmt.Errorf("%s: not initialized", cl.name)
	}
	return nil
}

func (cl *CertLoader) _get() *tls.Certificate { return &cl.xcert.Load().Certificate }

func (cl *CertLoader) _hello(*tls.ClientHelloInfo) (*tls.Certificate, error) { return cl._get(), nil }

func (cl *CertLoader) _info(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return cl._get(), nil
}

func (cl *CertLoader) do(compare bool) (err error) {
	var (
		finfo os.FileInfo
		xcert = xcert{parent: cl}
	)
	// 1. fstat
	finfo, err = os.Stat(cl.certFile)
	if err != nil {
		return fmt.Errorf("%s: failed to fstat %q, err: %w", cl.name, cl.certFile, err)
	}

	// 2. updated? (nothing loaded yet compares as "updated")
	if compare {
		if prev := cl.xcert.Load(); prev != nil {
			if mtime := finfo.ModTime(); mtime.Equal(prev.modTime) && finfo.Size() == prev.size {
				return nil
			}
		}
	}

	// 3. read and parse
	xcert.Certificate, err = tls.LoadX509KeyPair(cl.certFile, cl.keyFile)
	if err != nil {
		return fmt.Errorf("%s: failed to load (%s, %s), err: %w", cl.name, cl.certFile, cl.keyFile, err)
	}
	rem, err := xcert.ini(finfo)
	if err != nil {
		return err
	}

	// 4. ok
	cl.xcert.Store(&xcert)
	var soon cos.NodeStateFlags
	if rem < warnSoonExpire {
		soon = cos.CertWillSoonExpire
	}
	cl.setFlags(soon)

	nlog.Infoln(xcert.String())
	return nil
}

///////////
// xcert //
///////////

func (x *xcert) String() string {
	var (
		sb        cos.SB
		notBefore = x.notBefore.String()
		notAfter  = x.notAfter.String()
		l         = len(x.parent.certFile) + 1 + len(notBefore) + 1 + len(notAfter) + 1
	)
	sb.Init(l)
	sb.WriteString(x.parent.certFile)
	sb.WriteUint8('[')
	sb.WriteString(notBefore)
	sb.WriteUint8(',')
	sb.WriteString(notAfter)
	sb.WriteUint8(']')

	return sb.String()
}

// NOTE: second time parsing certificate (first time in tls.LoadX509KeyPair above)
// to find out valid time bounds
func (x *xcert) ini(finfo os.FileInfo) (rem time.Duration, err error) {
	if x.Certificate.Leaf == nil {
		x.Certificate.Leaf, err = x509.ParseCertificate(x.Certificate.Certificate[0])
		if err != nil {
			return 0, fmt.Errorf("%s: failed to parse %q, err: %w", x.parent.name, x.parent.certFile, err)
		}
	}
	{
		x.modTime = finfo.ModTime()
		x.size = finfo.Size()
		x.notBefore = x.Certificate.Leaf.NotBefore
		x.notAfter = x.Certificate.Leaf.NotAfter
	}
	now := time.Now()
	switch {
	case now.After(x.notAfter):
		msg := fmt.Sprintf(fmtErrExpired, x.parent.name, x.parent.certFile, x.notAfter)
		err = &errExpired{msg}
	case now.Before(x.notBefore):
		err = fmt.Errorf("%s: %s not valid yet: (%v, %v)", x.parent.name, x.parent.certFile, x.notBefore, x.notAfter)
	default:
		rem = x.notAfter.Sub(now)
	}
	return rem, err
}

//
// other
//

func (e *errExpired) Error() string { return e.msg }

func isExpired(err error) bool {
	_, ok := err.(*errExpired)
	return ok
}

// YATF
func fmtTime(tm time.Time) string {
	s := tm.String()
	i := strings.Index(s, " +")
	if i > 0 {
		return s[0:i]
	}
	return s
}
