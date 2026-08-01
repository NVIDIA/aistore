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

	jsoniter "github.com/json-iterator/go"
)

// The authoritative set of ClusterConfig sections participating in sparse
// persistence. Adding a pointerized section does not implicitly join this set:
// it must implement defaultOmittable and be listed here.
//
// v5.0: ec and mirror joined the set. Their pre-v5.0 validators hard-error on a
// zero section (ec.data_slices < 1, mirror.copies < 2), so a config persisted by
// v5.0 cannot be read by a pre-v5.0 node - downgrade below v5.0 is unsupported.
// Contrast tcb/tco/arch/lso, which pre-v5.0 hydrates via XactConf.Validate().
var expectedOmittable = []string{"TCB", "TCO", "Arch", "Lso", "Chunks", "EC", "Mirror"}

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
	c.ensureDefaults()

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
	c.ensureDefaults()
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
	c.ensureDefaults() // allocate, but deliberately skip validation

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
	c.ensureDefaults()
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
	orig.ensureDefaults()
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
	loaded.ensureDefaults()
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

// The metasync path may prune the same representation repeatedly.
func TestPruneIdempotent(t *testing.T) {
	c := &ClusterConfig{}
	c.ensureDefaults()
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
