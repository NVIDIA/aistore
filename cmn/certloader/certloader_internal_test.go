// Package certloader loads and reloads X.509 certs.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package certloader

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestMain(m *testing.M) {
	hk.Init(false)
	os.Exit(m.Run())
}

// minimal cos.StatsUpdater that tracks node-alert flags
type mockStats struct{ alerts int64 }

var _ cos.StatsUpdater = (*mockStats)(nil)

func (*mockStats) Inc(string)                         {}
func (*mockStats) Add(string, int64)                  {}
func (*mockStats) Observe(string, float64)            {}
func (*mockStats) SetFlag(string, cos.NodeStateFlags) {}
func (*mockStats) ClrFlag(string, cos.NodeStateFlags) {}
func (*mockStats) AddWith(...cos.NamedVal64)          {}
func (*mockStats) IncWith(string, map[string]string)  {}
func (m *mockStats) Get(string) int64                 { return m.alerts }
func (m *mockStats) SetClrFlag(_ string, set, clr cos.NodeStateFlags) {
	m.alerts = int64((cos.NodeStateFlags(m.alerts) | set) &^ clr)
}

// NOTE: the package-level Init constructs Default/Pub/Mgr and must not run twice
// (see TestPackageInit); everything else builds its own manager here
func newTestMgr(cls ...*CertLoader) (*Manager, *mockStats) {
	ms := &mockStats{}
	m := newManager(cls...)
	m.tstats = ms
	return m, ms
}

func valid() (notBefore, notAfter time.Time) {
	return time.Now().Add(-time.Hour), time.Now().Add(365 * 24 * time.Hour)
}

// (re)generate a self-signed cert/key at the given paths
func genCert(t *testing.T, certFile, keyFile string, notBefore, notAfter time.Time) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tassert.CheckFatal(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	tassert.CheckFatal(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	tassert.CheckFatal(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	tassert.CheckFatal(t, os.WriteFile(certFile, certPEM, 0o600))
	tassert.CheckFatal(t, os.WriteFile(keyFile, keyPEM, 0o600))
}

// self-signed cert/key written to a fresh temp dir
func writeCert(t *testing.T, notBefore, notAfter time.Time) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	genCert(t, certFile, keyFile, notBefore, notAfter)
	return certFile, keyFile
}

// ditto, valid()
func writeValid(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	notBefore, notAfter := valid()
	return writeCert(t, notBefore, notAfter)
}

type certKind int

const (
	noCert  certKind = iota // not configured
	badCert                 // configured; files do not exist
	okCert                  // configured; valid self-signed
)

type loaderSpec struct {
	name string
	prop string // Props key expected non-empty ("" - don't check)
	kind certKind
}

func (spec *loaderSpec) newLoader(t *testing.T) *CertLoader {
	t.Helper()
	switch spec.kind {
	case noCert:
		return &CertLoader{name: spec.name}
	case badCert:
		dir := t.TempDir()
		return &CertLoader{name: spec.name, certFile: filepath.Join(dir, "no.pem"), keyFile: filepath.Join(dir, "no.key")}
	default:
		cert, key := writeValid(t)
		return &CertLoader{name: spec.name, certFile: cert, keyFile: key}
	}
}

func TestCertLoaderValid(t *testing.T) {
	cert, key := writeValid(t)
	cl := &CertLoader{name: "cl-valid"}
	m, ms := newTestMgr(cl)

	tassert.CheckFatal(t, cl.Init(cert, key))

	p := cl.Props()
	tassert.Fatalf(t, p["error"] == "" && p["valid"] != "", "unexpected props: %v", p)

	_, err := cl.GetCert()
	tassert.CheckFatal(t, err)
	_, err = cl.GetClientCert()
	tassert.CheckFatal(t, err)

	tassert.CheckFatal(t, m.LoadAll())
	tassert.Fatalf(t, cos.NodeStateFlags(ms.alerts)&certFlags == 0, "unexpected alerts: %x", ms.alerts)
}

// Init is all-or-nothing: a partial (cert, key) leaves the loader unconfigured
func TestCertLoaderInitIncomplete(t *testing.T) {
	cert, _ := writeValid(t)
	cl := &CertLoader{name: "cl-incomplete"}
	newTestMgr(cl)

	tassert.Fatalf(t, cl.Init("", "") != nil, "expected error: no cert, no key")
	tassert.Fatalf(t, cl.Init(cert, "") != nil, "expected error: no key")
	tassert.Fatalf(t, !cl.configured(), "expected loader to remain unconfigured")
	tassert.Fatalf(t, cl.Props() == nil, "expected nil props, got %v", cl.Props())
}

// expired at the very first load: the flag is set with no xcert stored -
// Props/GetCert must report the condition, not nil-deref
func TestCertLoaderExpiredFirstLoad(t *testing.T) {
	cert, key := writeCert(t, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	cl := &CertLoader{name: "cl-expired"}
	_, ms := newTestMgr(cl)

	err := cl.Init(cert, key)
	tassert.Fatalf(t, err != nil && isExpired(err), "expected expired error, got %v", err)
	tassert.Fatalf(t, cl.xcert.Load() == nil, "expected nothing stored")

	p := cl.Props()
	tassert.Fatalf(t, p["error"] != "", "expected error props, got %v", p)

	_, err = cl.GetCert()
	tassert.Fatalf(t, err != nil && isExpired(err), "expected expired error, got %v", err)
	_, err = cl.GetClientCert()
	tassert.Fatalf(t, err != nil, "expected expired error")

	tassert.Fatalf(t, cos.NodeStateFlags(ms.alerts)&cos.CertificateExpired != 0, "expected expired alert")
}

// hk path: do(compare) reloads on mtime/size change only
func TestCertLoaderCompare(t *testing.T) {
	notBefore, notAfter := valid()
	cert, key := writeCert(t, notBefore, notAfter)
	cl := &CertLoader{name: "cl-compare"}
	newTestMgr(cl)
	tassert.CheckFatal(t, cl.Init(cert, key))

	first := cl.xcert.Load()
	tassert.Fatalf(t, first != nil, "expected loaded cert")

	tassert.CheckFatal(t, cl.do(true /*compare*/))
	tassert.Fatalf(t, cl.xcert.Load() == first, "unchanged file: expected no reload")

	genCert(t, cert, key, notBefore, notAfter)
	// deterministic mtime bump (a regenerated cert can be byte-identical in size)
	future := time.Now().Add(time.Second)
	tassert.CheckFatal(t, os.Chtimes(cert, future, future))

	tassert.CheckFatal(t, cl.do(true /*compare*/))
	tassert.Fatalf(t, cl.xcert.Load() != first, "modified file: expected reload")
}

func TestLoadAll(t *testing.T) {
	tests := []struct {
		name       string
		loaders    []loaderSpec
		errHas     []string
		wantAlerts cos.NodeStateFlags
		wantErr    bool
	}{
		{
			// e.g., the x509 reload API on a node that runs plain HTTP
			name:    "none configured",
			loaders: []loaderSpec{{name: "cl-a"}, {name: "cl-b"}},
			wantErr: true,
		},
		{
			// Optional static loader case: those not initialized are skipped
			name:    "one configured, one not",
			loaders: []loaderSpec{{name: "cl-none"}, {name: "cl-good", kind: okCert, prop: "valid"}},
		},
		{
			// must not early-return: the healthy loader after the bad one still loads
			name: "invalid first",
			loaders: []loaderSpec{
				{name: "cl-bad", kind: badCert, prop: "error"},
				{name: "cl-good", kind: okCert, prop: "valid"},
			},
			wantErr:    true,
			wantAlerts: cos.CertificateInvalid,
		},
		{
			name: "all invalid - errors aggregated",
			loaders: []loaderSpec{
				{name: "cl-a", kind: badCert, prop: "error"},
				{name: "cl-b", kind: badCert, prop: "error"},
			},
			errHas:     []string{"cl-a", "cl-b"},
			wantErr:    true,
			wantAlerts: cos.CertificateInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cls := make([]*CertLoader, len(tc.loaders))
			for i := range tc.loaders {
				cls[i] = tc.loaders[i].newLoader(t)
			}
			m, ms := newTestMgr(cls...)

			err := m.LoadAll()
			tassert.Fatalf(t, (err != nil) == tc.wantErr, "LoadAll: got %v, want error: %t", err, tc.wantErr)
			for _, s := range tc.errHas {
				tassert.Fatalf(t, strings.Contains(err.Error(), s), "want %q in %q", s, err)
			}
			alerts := cos.NodeStateFlags(ms.alerts) & certFlags
			tassert.Fatalf(t, alerts == tc.wantAlerts, "alerts: got %x, want %x", int64(alerts), int64(tc.wantAlerts))

			for i := range tc.loaders {
				spec := &tc.loaders[i]
				if spec.prop == "" {
					continue
				}
				props := cls[i].Props()
				tassert.Fatalf(t, props[spec.prop] != "", "%s: expected %q, got %v", spec.name, spec.prop, props)
			}
		})
	}
}

// aggregated `what=certificate`: default loader unprefixed, additional loaders
// under their own prefix; unconfigured loaders contribute nothing
func TestManagerProps(t *testing.T) {
	certA, keyA := writeValid(t)
	certB, keyB := writeValid(t)

	dflt := &CertLoader{name: "cl-dflt"}
	pub := &CertLoader{name: "cl-pub", prefix: "pub."}
	m, _ := newTestMgr(dflt, pub)

	tassert.Fatalf(t, m.Props() == nil, "nothing configured: expected nil props")

	tassert.CheckFatal(t, dflt.Init(certA, keyA))
	props := m.Props()
	tassert.Fatalf(t, props["valid"] != "", "expected default props, got %v", props)
	tassert.Fatalf(t, props["pub.valid"] == "", "pub not configured: expected no pub props, got %v", props)

	tassert.CheckFatal(t, pub.Init(certB, keyB))
	props = m.Props()
	tassert.Fatalf(t, props["valid"] != "" && props["pub.valid"] != "", "expected both, got %v", props)
}

// Default/Pub/Mgr are nil until the package-level Init - which a plain-HTTP node
// never calls, while the x509 reload API and `what=certificate` remain reachable
func TestNilReceivers(t *testing.T) {
	var (
		m  *Manager
		cl *CertLoader
	)
	tassert.Fatalf(t, m.LoadAll() != nil, "expected error")
	tassert.Fatalf(t, m.Props() == nil, "expected nil props")
	tassert.Fatalf(t, cl.Props() == nil, "expected nil props")

	_, err := cl.GetCert()
	tassert.Fatalf(t, err != nil, "expected error")
	_, err = cl.GetClientCert()
	tassert.Fatalf(t, err != nil, "expected error")
}

// two independent certificates in one manager: reloads and the node-alert union
// (a shared cert bit is cleared only when no loader needs it)
func TestMultiInstance(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	longAfter := time.Now().Add(365 * 24 * time.Hour)

	certA, keyA := writeCert(t, past, longAfter)
	certB, keyB := writeCert(t, past, longAfter)

	clA := &CertLoader{name: "cl-a"}
	clB := &CertLoader{name: "cl-b"}
	_, ms := newTestMgr(clA, clB)

	tassert.CheckFatal(t, clA.Init(certA, keyA))
	tassert.CheckFatal(t, clB.Init(certB, keyB))
	// both valid -> no node cert alerts
	tassert.Fatalf(t, cos.NodeStateFlags(ms.alerts)&certFlags == 0, "unexpected alerts: %x", ms.alerts)

	// reload B as expired -> node reflects expired; A stays valid (independent)
	genCert(t, certB, keyB, past, past)
	tassert.Fatalf(t, clB.load() != nil, "expected B expired error")
	tassert.Fatalf(t, cos.NodeStateFlags(ms.alerts)&cos.CertificateExpired != 0, "expected expired alert from B")
	p := clA.Props()
	tassert.Fatalf(t, p["error"] == "" && p["valid"] != "", "A should remain valid: %v", p)

	// fix B and reload -> shared bit cleared only now that neither loader needs it
	genCert(t, certB, keyB, past, longAfter)
	tassert.CheckFatal(t, clB.load())
	tassert.Fatalf(t, cos.NodeStateFlags(ms.alerts)&certFlags == 0, "alerts should be cleared: %x", ms.alerts)
}

// NOTE: the one and only test permitted to call the package-level Init -
// it constructs Default/Pub/Mgr for the lifetime of this test binary, and a
// second call trips debug.Assert("certloader.Init called twice")
func TestPackageInit(t *testing.T) {
	cert, key := writeValid(t)

	tassert.CheckFatal(t, Init(&mockStats{}, cert, key))

	tassert.Fatalf(t, Default != nil && Pub != nil && Mgr != nil, "expected loaders constructed")
	tassert.Fatalf(t, Default.name == DfltCL && Pub.name == PubCL, "unexpected names: %q, %q", Default.name, Pub.name)
	tassert.Fatalf(t, Default.mgr == Mgr && Pub.mgr == Mgr, "expected both wired to Mgr")
	tassert.Fatalf(t, !Pub.configured(), "expected pub loader to stay inert")

	props := Mgr.Props()
	tassert.Fatalf(t, props["valid"] != "", "expected default loader props, got %v", props)
	tassert.Fatalf(t, props["pub.valid"] == "", "expected no pub props, got %v", props)

	tassert.CheckFatal(t, Mgr.LoadAll())
}
