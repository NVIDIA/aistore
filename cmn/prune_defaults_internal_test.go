// Package cmn provides common types and utilities for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"

	jsoniter "github.com/json-iterator/go"
)

// The authoritative set of ClusterConfig sections participating in sparse
// persistence. Adding a pointerized section does not implicitly join this set:
// it must implement defaultOmittable and be listed here.
//
// v5.0: ec, mirror, checksum, disk, periodic, and downloader joined the set.
// Their pre-v5.0 validators hard-error on a zero section (ec.data_slices < 1,
// mirror.copies < 2, empty checksum.type, zero watermarks, out-of-range periodic
// and downloader durations), so a config persisted by v5.0 cannot be read by a
// pre-v5.0 node - downgrade below v5.0 is unsupported.
// Contrast tcb/tco/arch/lso, which pre-v5.0 hydrates via XactConf.Validate();
// same for rate_limit and write_policy, whose zero sections have always validated.
var expectedOmittable = []string{
	"TCB", "TCO", "Arch", "Lso", "Chunks", "EC", "Mirror",
	"Cksum", "Disk", "Periodic", "Downloader", "RateLimit", "WritePolicy",
}

func omittableNames(c *ClusterConfig) []string {
	var names []string
	c.rangeDefaultOmittable(func(sf *reflect.StructField, _ reflect.Value) {
		names = append(names, sf.Name)
	})
	return names
}

// Guards against:
//   - an expected section losing its defaultOmittable implementation;
//   - the omittable set changing without the corresponding compatibility review
//     and release disclaimer.
func TestOmittableCoverage(t *testing.T) {
	got := omittableNames(&ClusterConfig{})
	expected := slices.Clone(expectedOmittable)
	slices.Sort(got)
	slices.Sort(expected)

	if !slices.Equal(got, expected) {
		t.Fatalf("default-omittable set changed: got %v, expected %v\n"+
			"if intentional, update expectedOmittable and check the rolling-upgrade "+
			"window: an older node must hydrate the now-absent section from zero",
			got, expected)
	}
}

// Admission criterion: PruneOmittables shallow-copies a section before
// validating it and compares the result with reflect.DeepEqual. Reference-typed
// fields could let validation mutate the live section, while nil-versus-empty
// values could prevent otherwise-default sections from being stripped.
func TestOmittableValueTypesOnly(t *testing.T) {
	c := &ClusterConfig{}
	c.allocOmittables()

	c.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		var walk func(reflect.Type, string)
		walk = func(typ reflect.Type, path string) {
			switch typ.Kind() {
			case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface,
				reflect.Func, reflect.Chan, reflect.UnsafePointer:
				t.Errorf("%s: %s is %v; default-omittable sections must be value-typed",
					sf.Name, path, typ.Kind())
			case reflect.Struct:
				for i := range typ.NumField() {
					f := typ.Field(i)
					walk(f.Type, path+"."+f.Name)
				}
			case reflect.Array:
				walk(typ.Elem(), path+"[]")
			}
		}
		walk(field.Type().Elem(), sf.Name)
	})
}

// An all-default config must persist with every omittable section absent.
func TestPruneAllDefault(t *testing.T) {
	c := &ClusterConfig{}
	c.allocOmittables()
	validateOmittable(t, c)

	c.PruneOmittables()

	c.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		if !field.IsNil() {
			t.Errorf("%s: all-default section not stripped", sf.Name)
		}
	})
}

// Mirrors _runPre: the value reaching _encode is not necessarily validated, so
// stripping must also work for zero-valued, unhydrated sections.
func TestPruneUnvalidated(t *testing.T) {
	c := &ClusterConfig{}
	c.allocOmittables() // zero-valued by construction: no validation here

	c.PruneOmittables()

	c.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		if !field.IsNil() {
			t.Errorf("%s: zero-valued section not stripped", sf.Name)
		}
	})
}

// A section with any non-default field survives intact, including its
// zero-valued siblings. Those are hydrated on load, not while pruning.
func TestPruneRetainsNonDefault(t *testing.T) {
	c := &ClusterConfig{}
	c.allocOmittables()
	c.Lso.WalkBuffer = lsoWalkBufDflt * 2

	c.PruneOmittables()

	if c.Lso == nil {
		t.Fatal("Lso: non-default section stripped")
	}
	if c.Lso.WalkBuffer != lsoWalkBufDflt*2 {
		t.Errorf("Lso.WalkBuffer: got %d, expected %d",
			c.Lso.WalkBuffer, lsoWalkBufDflt*2)
	}

	// PruneOmittables validates a scratch copy; it must not hydrate in place.
	if c.Lso.SbundleMult != 0 {
		t.Errorf("Lso.SbundleMult: got %d, expected 0 (in-place hydration)",
			c.Lso.SbundleMult)
	}
	if c.Lso.IdleTime != 0 {
		t.Errorf("Lso.IdleTime: got %v, expected 0 (in-place hydration)",
			c.Lso.IdleTime)
	}

	// Unrelated all-default sections must strip independently.
	if c.TCB != nil || c.TCO != nil || c.Arch != nil {
		t.Error("unrelated all-default sections were not stripped")
	}
}

// JSON round trip: prune -> encode -> decode -> materialize. This also
// exercises repeated validation and verifies that pruning a shallow config
// copy does not mutate the source.
func TestPruneRoundTrip(t *testing.T) {
	orig := &ClusterConfig{}
	orig.allocOmittables()
	validateOmittable(t, orig)

	// Deliberately shallow: PruneOmittables must not mutate shared sections.
	sparse := *orig
	sparse.PruneOmittables()

	b, err := jsoniter.Marshal(&sparse)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	loaded := &ClusterConfig{}
	if err := jsoniter.Unmarshal(b, loaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	loaded.allocOmittables()
	validateOmittable(t, loaded)

	loaded.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		want := reflect.ValueOf(orig).Elem().FieldByName(sf.Name)
		if !reflect.DeepEqual(field.Elem().Interface(), want.Elem().Interface()) {
			t.Errorf("%s: round-trip mismatch\n got: %+v\nwant: %+v",
				sf.Name, field.Elem().Interface(), want.Elem().Interface())
		}
	})

	// The persisted representation must actually be sparse.
	for _, name := range expectedOmittable {
		field := reflect.ValueOf(&sparse).Elem().FieldByName(name)
		if !field.IsNil() {
			t.Errorf("%s: present in the persisted form", name)
		}
	}

	var raw map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	typ := reflect.TypeOf(sparse)
	for _, name := range expectedOmittable {
		sf, ok := typ.FieldByName(name)
		if !ok {
			t.Fatalf("ClusterConfig.%s not found", name)
		}
		tag, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
		if tag == "" || tag == "-" {
			t.Fatalf("ClusterConfig.%s: invalid JSON tag %q", name, sf.Tag.Get("json"))
		}
		if _, ok := raw[tag]; ok {
			t.Errorf("%s: %q present in the encoded form (missing `omitempty`?)", name, tag)
		}
	}
	// The source must remain fully materialized.
	orig.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		if field.IsNil() {
			t.Errorf("%s: PruneOmittables mutated the source config", sf.Name)
		}
	})
}

// An override may be merged into a freshly decoded sparse cluster config.
// The section may be entirely absent or explicitly present but empty.
// Hydrating first keeps a single-knob override from silently redefining the
// rest of its section.
func TestOverrideOntoSparseSections(t *testing.T) {
	tests := []struct {
		name string
		c    *ClusterConfig
	}{
		{
			name: "absent sections",
			c:    &ClusterConfig{},
		},
		{
			name: "present empty sections",
			c: &ClusterConfig{
				Disk: &DiskConf{},
				EC:   &ECConf{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.c
			if err := c.hydrateOmittables(); err != nil {
				t.Fatalf("hydrate: %v", err)
			}

			// Startup and metasync-receive paths merge node overrides as
			// apc.Daemon with IgnoreScope.
			toUpdate := &ConfigToSet{
				Disk: &DiskConfToSet{DiskUtilHighWM: apc.Ptr[int64](85)},
				EC:   &ECConfToSet{DataSlices: apc.Ptr(4)},
			}
			if err := CopyProps(toUpdate, c, apc.Daemon, CopyPropsOpts{IgnoreScope: true}); err != nil {
				t.Fatalf("copy daemon props: %v", err)
			}
			validateOmittable(t, c)

			// Node-scoped override: merge onto hydrated defaults.
			if c.Disk.DiskUtilHighWM != 85 {
				t.Errorf("Disk.DiskUtilHighWM: got %d, expected 85",
					c.Disk.DiskUtilHighWM)
			}
			if c.Disk.DiskUtilLowWM != diskUtilLowWMDflt ||
				c.Disk.DiskUtilMaxWM != diskUtilMaxWMDflt {
				t.Errorf("Disk watermarks: got (%d, %d, %d), expected (%d, 85, %d)",
					c.Disk.DiskUtilLowWM, c.Disk.DiskUtilHighWM,
					c.Disk.DiskUtilMaxWM, diskUtilLowWMDflt,
					diskUtilMaxWMDflt)
			}

			// Cluster-scoped EC override from a node must be ignored.
			if c.EC.DataSlices != ecDataSlicesDflt {
				t.Errorf("EC.DataSlices: got %d, expected %d after ignored node override",
					c.EC.DataSlices, ecDataSlicesDflt)
			}
			if c.EC.ObjSizeLimit != ecObjSizeLimitDflt {
				t.Errorf("EC.ObjSizeLimit: got %d, expected %d",
					c.EC.ObjSizeLimit, ecObjSizeLimitDflt)
			}

			// The same update at cluster scope must change DataSlices without
			// reinterpreting zero ObjSizeLimit as "EC every object."
			toUpdate = &ConfigToSet{
				EC: &ECConfToSet{DataSlices: apc.Ptr(4)},
			}
			if err := CopyProps(toUpdate, c, apc.Cluster, CopyPropsOpts{}); err != nil {
				t.Fatalf("copy cluster props: %v", err)
			}
			validateOmittable(t, c)

			if c.EC.DataSlices != 4 {
				t.Errorf("EC.DataSlices: got %d, expected 4", c.EC.DataSlices)
			}
			if c.EC.ObjSizeLimit != ecObjSizeLimitDflt {
				t.Errorf("EC.ObjSizeLimit: got %d, expected inherited default %d",
					c.EC.ObjSizeLimit, ecObjSizeLimitDflt)
			}
		})
	}
}

// hydrateOmittables validates every omittable section, and Config.Validate
// then does it again - so the section validators must be idempotent.
// Note that ECConf.Validate keys off the section as a whole (`*c == ECConf{}`),
// which makes this more than a formality.
func TestOmittableValidateIdempotent(t *testing.T) {
	c := &ClusterConfig{}
	if err := c.hydrateOmittables(); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	c.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		first := reflect.New(field.Type().Elem())
		first.Elem().Set(field.Elem())

		o := field.Interface().(defaultOmittable)
		if err := o.Validate(); err != nil {
			t.Fatalf("%s: re-validate: %v", sf.Name, err)
		}
		if !reflect.DeepEqual(first.Elem().Interface(), field.Elem().Interface()) {
			t.Errorf("%s: Validate() is not idempotent\n first: %+v\nsecond: %+v",
				sf.Name, first.Elem().Interface(), field.Elem().Interface())
		}
	})
}

// Defaults materialized in the inherited configuration remain inherited values
// across a partial override. Explicitly resetting a derived field to zero asks
// the final validation pass to derive it again.
func TestOverridePreservesInheritedDerivedDefaults(t *testing.T) {
	c := &ClusterConfig{}
	if err := c.hydrateOmittables(); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	inheritedSmooth := c.Disk.IostatTimeSmooth
	if inheritedSmooth != cos.Duration(8*time.Second) {
		t.Fatalf("default IostatTimeSmooth: got %v, expected 8s", inheritedSmooth)
	}

	// Override only the source field: the already inherited smooth interval
	// remains unchanged.
	c.Disk.IostatTimeLong = cos.Duration(4 * time.Second)
	validateOmittable(t, c)

	if c.Disk.IostatTimeSmooth != inheritedSmooth {
		t.Errorf("IostatTimeSmooth: got %v, expected inherited value %v",
			c.Disk.IostatTimeSmooth, inheritedSmooth)
	}

	// An explicit zero restores derived-default behavior.
	c.Disk.IostatTimeSmooth = 0
	validateOmittable(t, c)

	const expected = 16 * time.Second
	if c.Disk.IostatTimeSmooth.D() != expected {
		t.Errorf("re-derived IostatTimeSmooth: got %v, expected %v",
			c.Disk.IostatTimeSmooth, expected)
	}
}

// The metasync path may prune the same representation repeatedly.
func TestPruneIdempotent(t *testing.T) {
	c := &ClusterConfig{}
	c.allocOmittables()
	c.Lso.WalkBuffer = lsoWalkBufDflt * 2

	c.PruneOmittables()
	first := *c.Lso

	c.PruneOmittables()
	if c.Lso == nil {
		t.Fatal("Lso: stripped on the second pass")
	}
	if !reflect.DeepEqual(first, *c.Lso) {
		t.Errorf("Lso: not idempotent\nfirst: %+v\nsecond: %+v",
			first, *c.Lso)
	}
}

func validateOmittable(t *testing.T, c *ClusterConfig) {
	t.Helper()

	c.rangeDefaultOmittable(func(sf *reflect.StructField, field reflect.Value) {
		if err := field.Interface().(defaultOmittable).Validate(); err != nil {
			t.Fatalf("%s: %v", sf.Name, err)
		}
	})
}
