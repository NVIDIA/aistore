//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"

	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

type testOCIConfigProvider struct {
	key    *rsa.PrivateKey
	region string
}

func newTestOCIConfigProvider(t *testing.T, region string) ocicmn.ConfigurationProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test RSA key: %v", err)
	}
	return testOCIConfigProvider{key: key, region: region}
}

func (p testOCIConfigProvider) Region() (string, error)       { return p.region, nil }
func (testOCIConfigProvider) KeyFingerprint() (string, error) { return "aa:bb:cc", nil }
func (testOCIConfigProvider) UserOCID() (string, error) {
	return "ocid1.user.oc1..exampleuniqueID", nil
}
func (testOCIConfigProvider) TenancyOCID() (string, error) {
	return "ocid1.tenancy.oc1..exampleuniqueID", nil
}
func (p testOCIConfigProvider) PrivateRSAKey() (*rsa.PrivateKey, error) { return p.key, nil }
func (testOCIConfigProvider) KeyID() (string, error) {
	return "ocid1.tenancy.oc1..example/ocid1.user.oc1..example/aa:bb:cc", nil
}
func (testOCIConfigProvider) AuthType() (ocicmn.AuthConfig, error) {
	return ocicmn.AuthConfig{AuthType: ocicmn.UserPrincipal}, nil
}

func TestOCIRegionForBck(t *testing.T) {
	bp := &ocibp{defaultRegion: "us-ashburn-1"}

	if got := bp.regionForBck(nil); got != "us-ashburn-1" {
		t.Fatalf("expected default region, got %q", got)
	}

	bck := &cmn.Bck{
		Name:     "bucket",
		Provider: apc.OCI,
		Props: &cmn.Bprops{
			Extra: cmn.ExtraProps{
				OCI: cmn.ExtraPropsOCI{Region: "us-phoenix-1"},
			},
		},
	}
	if got := bp.regionForBck(bck); got != "us-phoenix-1" {
		t.Fatalf("expected per-bucket region, got %q", got)
	}
}

func TestOCIFetchCliConfigInstancePrincipalExplicitRegion(t *testing.T) {
	orig := newOCIInstancePrincipalProvider
	defer func() { newOCIInstancePrincipalProvider = orig }()

	var gotRegion string
	newOCIInstancePrincipalProvider = func(region string) (ocicmn.ConfigurationProvider, error) {
		gotRegion = region
		return newTestOCIConfigProvider(t, "us-phoenix-1"), nil
	}

	t.Setenv(env.OCIInstancePrincipalAuth, "true")
	t.Setenv(env.OCIRegion, "us-phoenix-1")
	t.Setenv(env.OCICompartmentOCID, "ocid1.compartment.oc1..example")

	bp := &ocibp{}
	if err := bp.fetchCliConfig(); err != nil {
		t.Fatalf("fetchCliConfig failed: %v", err)
	}
	if gotRegion != "us-phoenix-1" {
		t.Fatalf("expected explicit region to be passed through, got %q", gotRegion)
	}
	if bp.defaultRegion != "us-phoenix-1" {
		t.Fatalf("expected default region to be set, got %q", bp.defaultRegion)
	}
	if bp.compartmentOCID != "ocid1.compartment.oc1..example" {
		t.Fatalf("expected compartment OCID to be preserved, got %q", bp.compartmentOCID)
	}
}

func TestOCIFetchCliConfigInstancePrincipalInferredRegion(t *testing.T) {
	orig := newOCIInstancePrincipalProvider
	defer func() { newOCIInstancePrincipalProvider = orig }()

	newOCIInstancePrincipalProvider = func(region string) (ocicmn.ConfigurationProvider, error) {
		if region != "" {
			t.Fatalf("expected empty region override, got %q", region)
		}
		return newTestOCIConfigProvider(t, "us-ashburn-1"), nil
	}

	t.Setenv(env.OCIInstancePrincipalAuth, "true")

	bp := &ocibp{}
	if err := bp.fetchCliConfig(); err != nil {
		t.Fatalf("fetchCliConfig failed: %v", err)
	}
	if bp.defaultRegion != "us-ashburn-1" {
		t.Fatalf("expected provider region to be used, got %q", bp.defaultRegion)
	}
}

func TestOCIUseInstancePrincipalAuthRejectsInvalidBool(t *testing.T) {
	t.Setenv(env.OCIInstancePrincipalAuth, "definitely")

	bp := &ocibp{}
	_, err := bp.useInstancePrincipalAuth()
	if err == nil {
		t.Fatal("expected invalid bool parse to fail")
	}
	if !strings.Contains(err.Error(), env.OCIInstancePrincipalAuth) {
		t.Fatalf("expected error to mention env var, got %v", err)
	}
}

func TestOCIFetchCliConfigInstancePrincipalRequiresRegion(t *testing.T) {
	orig := newOCIInstancePrincipalProvider
	defer func() { newOCIInstancePrincipalProvider = orig }()

	newOCIInstancePrincipalProvider = func(_ string) (ocicmn.ConfigurationProvider, error) {
		return newTestOCIConfigProvider(t, ""), nil
	}

	t.Setenv(env.OCIInstancePrincipalAuth, "true")

	bp := &ocibp{}
	err := bp.fetchCliConfig()
	if err == nil {
		t.Fatal("expected missing region to fail")
	}
	if !strings.Contains(err.Error(), env.OCIRegion) {
		t.Fatalf("expected error to mention %s, got %v", env.OCIRegion, err)
	}
}

func TestOCIClientCreatesRegionClientOnce(t *testing.T) {
	bp := &ocibp{
		defaultRegion:         "us-ashburn-1",
		configurationProvider: newTestOCIConfigProvider(t, "us-ashburn-1"),
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	clients := make(chan *ocios.ObjectStorageClient, 8)
	errs := make(chan error, 8)

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			client, _, err := bp.ociClient(nil)
			if err == nil {
				clients <- client
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(clients)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ociClient failed: %v", err)
		}
	}
	var first *ocios.ObjectStorageClient
	for client := range clients {
		if first == nil {
			first = client
			continue
		}
		if first != client {
			t.Fatal("expected all goroutines to receive the same cached client instance")
		}
	}
}
