// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const IterFieldNameSepa = "."

const (
	tagOmitempty = "omitempty" // the field must be omitted when empty (only for read-only walk)
	tagOmit      = "omit"      // the field must be omitted
	tagReadonly  = "readonly"  // the field can be only read
	tagInline    = "inline"    // the fields of a struct are embedded into parent field keys
)

type (
	// Represents a single named field
	IterField interface {
		Value() any                          // returns the value
		String() string                      // string representation of the value
		SetValue(v any, force ...bool) error // `force` ignores `tagReadonly` (to be used with caution!)
	}

	field struct {
		name    string
		v       reflect.Value
		listTag string
		opts    IterOpts
		dirty   bool // indicates `SetValue` done
	}

	IterOpts struct {
		// Skip fields based on allowed tag
		Allowed string
		// Visits all the fields, not only the leaves.
		VisitAll bool
		// Read-only walk is true by default (compare with `UpdateFieldValue`)
		// Note that `tagOmitempty` is limited to read-only - has no effect when `OnlyRead == false`.
		OnlyRead bool
	}

	updateFunc func(uniqueTag string, field IterField) (error, bool)
)

type (
	// controls how CopyProps/_copyProps enforce config scope and transient rules
	CopyPropsOpts struct {
		Transient   bool // treat changes as transient: forbid writes to allow:"cluster"
		IgnoreScope bool // when true, skip allow:"cluster" violations instead of failing (config override-only)
	}
)

// interface guard
var _ IterField = (*field)(nil)

// IterFields walks the struct and calls `updf` callback at every leaf field that it
// encounters. The (nested) names are created by joining the json tag with dot.
// Iteration supports reading another, custom tag `list` with values:
//   - `tagOmitempty` - omit empty fields (only for read run)
//   - `tagOmit` - omit field
//   - `tagReadonly` - field cannot be updated (returns error on `SetValue`)
//
// Examples of usages for tags can be found in `BucketProps` or `Config` structs.
//
// Passing additional options with `IterOpts` can for example call callback
// also at the non-leaf structures.
func IterFields(v any, updf updateFunc, opts ...IterOpts) error {
	o := IterOpts{OnlyRead: true} // by default it's read run
	if len(opts) > 0 {
		o = opts[0]
	}
	_, _, err := iterFields("", v, updf, o)
	return err
}

// UpdateFieldValue updates the field in the struct with given value.
// Returns error if the field was not found or could not be updated.
func UpdateFieldValue(s any, name string, value any) error {
	found := false
	err := IterFields(s, func(uniqueTag string, field IterField) (error, bool) {
		if uniqueTag == name {
			found = true
			return field.SetValue(value), true
		}
		return nil, false
	}, IterOpts{OnlyRead: false})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("unknown property %q", name)
	}
	return nil
}

func iterFields(prefix string, v any, updf updateFunc, opts IterOpts) (dirty, stop bool, err error) {
	srcVal := reflect.ValueOf(v)
	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}
	for i := range srcVal.NumField() {
		var (
			srcTyField  = srcVal.Type().Field(i)
			srcValField = srcVal.Field(i)
			isInline    bool
		)

		// Check if we need to skip given field.
		listTag := srcTyField.Tag.Get("list")
		if listTag == tagOmit {
			continue
		}

		jsonTag, jsonTagPresent := srcTyField.Tag.Lookup("json")
		tags := strings.Split(jsonTag, ",")
		fieldName := tags[0]
		if fieldName == "-" {
			continue
		}
		if len(tags) > 1 {
			isInline = tags[1] == tagInline
		}

		// Determines if the pointer to struct was allocated.
		// In case it was  but no field in the struct was
		// updated we must later set it to `nil`.
		var allocatedStruct bool

		// If the field is a pointer to a struct we must dereference it.
		if srcValField.Kind() == reflect.Ptr && srcValField.Type().Elem().Kind() == reflect.Struct {
			if srcValField.IsNil() {
				if opts.OnlyRead {
					// read-only:
					// - allocate a temporary struct just for iteration
					// - do not assign it back to the parent (`allocatedStruct` remains false)
					tmp := reflect.New(srcValField.Type().Elem())
					srcValField = tmp.Elem()
				} else {
					// write mode: the field must be settable
					debug.Assertf(srcValField.CanSet(), "write-mode: %q (type=%s) is not settable",
						prefix+fieldName, srcValField.Type())
					allocatedStruct = true
					srcValField.Set(reflect.New(srcValField.Type().Elem()))
					srcValField = srcValField.Elem()
				}
			} else {
				srcValField = srcValField.Elem()
			}
		}

		// Read-only walk skips empty (zero) fields.
		if opts.OnlyRead && listTag == tagOmitempty && srcValField.IsZero() {
			continue
		}

		if opts.Allowed != "" {
			allowTag := srcTyField.Tag.Get("allow")
			if allowTag != "" && allowTag != opts.Allowed {
				continue
			}
		}

		// If it's `any` we must get concrete type.
		if srcValField.Kind() == reflect.Interface && !srcValField.IsZero() {
			srcValField = srcValField.Elem()
		}

		var dirtyField bool
		switch {
		case srcValField.Kind() == reflect.Slice:
			if !jsonTagPresent {
				continue
			}
			name := prefix + fieldName
			field := &field{name: name, v: srcValField, listTag: listTag, opts: opts}
			err, stop = updf(name, field)
			dirtyField = field.dirty
		case srcValField.Kind() != reflect.Struct:
			// We require that not-omitted fields have JSON tag.
			debug.Assert(jsonTagPresent, prefix+"["+fieldName+"]")

			// Set value for the field
			name := prefix + fieldName
			field := &field{name: name, v: srcValField, listTag: listTag, opts: opts}
			err, stop = updf(name, field)
			dirtyField = field.dirty
		default:
			// Recurse into struct
			// Always take address if possible (assuming that we will set value)
			if srcValField.CanAddr() {
				srcValField = srcValField.Addr()
			}
			p := prefix
			if fieldName != "" {
				// If struct has JSON tag, we want to include it.
				p += fieldName
			}
			if opts.VisitAll {
				field := &field{name: p, v: srcValField, listTag: listTag, opts: opts}
				err, stop = updf(p, field)
				dirtyField = field.dirty
			}
			if !strings.HasSuffix(p, IterFieldNameSepa) && !isInline {
				p += IterFieldNameSepa
			}
			if err == nil && !stop {
				dirtyField, stop, err = iterFields(p, srcValField.Interface(), updf, opts)
				if allocatedStruct && !dirtyField {
					// if we initialized new struct with no fields set inside
					// we must restore the value back to `nil`
					srcValField = srcVal.Field(i)
					srcValField.Set(reflect.Zero(srcValField.Type()))
				}
			}
		}

		dirty = dirty || dirtyField
		if stop {
			return dirty, stop, err
		}
		if err != nil {
			return dirty, true, err
		}
	}

	return dirty, stop, err
}

// CopyProps update dst with the values from src
func CopyProps(src, dst any, asType string, copts ...CopyPropsOpts) error {
	var (
		srcVal = reflect.ValueOf(src)
		dstVal = reflect.ValueOf(dst).Elem()
		opts   CopyPropsOpts
	)
	if len(copts) > 0 {
		opts = copts[0]
	}
	debug.Assertf(slices.Contains([]string{apc.Daemon, apc.Cluster}, asType), "unexpected config level: %s", asType)
	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}
	return _copyProps(srcVal, dstVal, asType, opts)
}

// copyProps helper: unwrap up to one pointer and one interface in any order
// (as we don't have arbitrary nested config structs)
func peel2(v reflect.Value) reflect.Value {
	for range 2 {
		switch v.Kind() {
		case reflect.Interface:
			if v.IsNil() {
				return v
			}
			v = v.Elem()
		case reflect.Ptr:
			if v.IsNil() {
				return v
			}
			v = v.Elem()
		default:
			return v
		}
	}
	return v
}

func _copyProps(srcVal, dstVal reflect.Value, asType string, opts CopyPropsOpts) error {
	// normalize pointers on entry
	for srcVal.Kind() == reflect.Ptr && !srcVal.IsNil() {
		srcVal = srcVal.Elem()
	}
	for dstVal.Kind() == reflect.Ptr {
		if dstVal.IsNil() && dstVal.CanSet() && dstVal.Type().Elem().Kind() == reflect.Struct {
			dstVal.Set(reflect.New(dstVal.Type().Elem()))
		}
		dstVal = dstVal.Elem()
	}

	// leaf mode if src is not a struct
	if srcVal.Kind() != reflect.Struct {
		// peel pointer/interface on src
		srcVal = peel2(srcVal)

		// make dst addressable inner value
		if dstVal.Kind() == reflect.Ptr {
			if dstVal.IsNil() && dstVal.CanSet() {
				dstVal.Set(reflect.New(dstVal.Type().Elem()))
			}
			dstVal = dstVal.Elem()
		}

		// expect to be able to set the leaf
		debug.Assertf(!dstVal.IsValid() || dstVal.CanSet(), "copyProps: destination leaf not settable: %v", dstVal)

		if !dstVal.IsValid() || !dstVal.CanSet() {
			return nil
		}
		if srcVal.IsValid() && srcVal.Type().AssignableTo(dstVal.Type()) {
			dstVal.Set(srcVal)
			return nil
		}
		if srcVal.IsValid() && srcVal.Type().ConvertibleTo(dstVal.Type()) {
			dstVal.Set(srcVal.Convert(dstVal.Type()))
			return nil
		}
		return nil
	}

	// from here, expect dst to be struct too
	if dstVal.Kind() != reflect.Struct {
		err := fmt.Errorf("copyProps(%s): dst kind %s (type %s) is not struct for src %s",
			asType, dstVal.Kind(), dstVal.Type(), srcVal.Type())
		debug.AssertNoErr(err)
		return err
	}

	for i := range srcVal.NumField() {
		sf := srcVal.Type().Field(i)
		// respect copy:"skip" and unexported fields
		if copyTag, ok := sf.Tag.Lookup("copy"); ok && copyTag == "skip" {
			continue
		}
		if sf.PkgPath != "" { // unexported
			continue
		}

		fieldName := sf.Name
		srcField := srcVal.Field(i)

		// find destination by NAME
		dstField := dstVal.FieldByName(fieldName)

		// embedded (anonymous) src struct not present by name on dst: recurse into parent
		if !dstField.IsValid() && sf.Anonymous {
			kind := sf.Type.Kind()
			if kind == reflect.Ptr {
				kind = sf.Type.Elem().Kind()
			}
			if kind == reflect.Struct {
				if cos.Nilable(srcField.Kind()) && srcField.IsNil() {
					continue
				}
				if err := _copyProps(srcField, dstVal, asType, opts); err != nil {
					return err
				}
				continue
			}
		}

		// destination may legitimately not have this field (e.g., local-config FSPConf)
		if !dstField.IsValid() {
			continue
		}
		// skip nil leaves for nilable kinds (*ToSet style)
		if cos.Nilable(srcField.Kind()) && srcField.IsNil() {
			continue
		}

		// scope enforcement from dst tag
		if dtf, ok := dstVal.Type().FieldByName(fieldName); ok {
			allowedScope := dtf.Tag.Get("allow")

			// 3 special cases
			switch {
			case opts.IgnoreScope && allowedScope == apc.Cluster && asType == apc.Daemon:
				name := strings.ToLower(fieldName)
				nlog.Warningln("ignoring node override for cluster-scoped config:", name)
				continue
			case opts.Transient && allowedScope == apc.Cluster:
				name := strings.ToLower(fieldName)
				return fmt.Errorf("%s (cluster-scoped) configuration cannot be changed transiently", name)
			case allowedScope != "" && allowedScope != asType:
				name := strings.ToLower(fieldName)
				if allowedScope == apc.Cluster && asType == apc.Daemon {
					return fmt.Errorf("%s configuration can only be globally updated", name)
				}
				return fmt.Errorf("cannot update %s configuration: expecting %q scope, got %q", name, allowedScope, asType)
			}
		}

		//
		// struct recurse vs leaf assign ----------------------
		//
		s := peel2(srcField)
		if cos.Nilable(s.Kind()) && s.IsNil() {
			continue
		}

		d := dstField
		if d.Kind() == reflect.Ptr {
			if d.IsNil() && d.CanSet() && d.Type().Elem().Kind() == reflect.Struct {
				d.Set(reflect.New(d.Type().Elem()))
			}
			if d.Kind() == reflect.Ptr && d.Elem().IsValid() {
				d = d.Elem()
			}
		}

		// recurse only when _both_ are structs
		if s.Kind() == reflect.Struct && d.Kind() == reflect.Struct {
			if err := _copyProps(srcField, dstField, asType, opts); err != nil { // pass originals to allow alloc
				return err
			}
			continue
		}

		//
		// not recursing - assigning
		//
		leafDst := dstField
		if leafDst.Kind() == reflect.Ptr {
			// if nil, allocate only when we can set; skip otherwise
			if leafDst.IsNil() {
				if !leafDst.CanSet() {
					// not settable and nil pointer: nothing we can do
					continue
				}
				leafDst.Set(reflect.New(leafDst.Type().Elem()))
			}
			leafDst = leafDst.Elem()
		}
		leafSrc := peel2(srcField)

		// require valid/settable dst and valid src
		if !leafDst.IsValid() || !leafDst.CanSet() || !leafSrc.IsValid() {
			continue
		}

		dstT := leafDst.Type()
		srcT := leafSrc.Type()
		if srcT.AssignableTo(dstT) {
			leafDst.Set(leafSrc)
			continue
		}
		if srcT.ConvertibleTo(dstT) {
			leafDst.Set(leafSrc.Convert(dstT))
			continue
		}

		// incompatible: assert and ignore
		debug.Assertf(false, "copyProps: incompatible types for %q: src=%v dst=%v (asType=%s)", fieldName, srcT, dstT, asType)
	}

	return nil
}

func mergeProps(src, dst any) {
	srcVal := getElem(src)
	dstVal := getElem(dst)
	for i := range srcVal.NumField() {
		var (
			srcField = srcVal.Field(i)
			dstField = dstVal.FieldByName(srcVal.Type().Field(i).Name)
		)

		if srcField.IsNil() {
			continue
		}

		// Special case to handle maps
		if srcField.Kind() == reflect.Map && dstField.Kind() == reflect.Map {
			if !srcField.IsNil() {
				// Addr().Interface() allows us to modify the original dst, Interface() only makes a copy
				mergeMaps(srcField.Interface(), dstField.Addr().Interface())
			}
			continue
		}

		if dstField.IsNil() ||
			(srcField.Elem().Kind() != reflect.Struct && srcField.Elem().Kind() != reflect.Invalid) {
			dstField.Set(srcField)
			continue
		}

		// Recurse into struct
		mergeProps(srcField.Interface(), dstField.Interface())
	}
}

func mergeMaps(src, dst any) {
	srcMap := getElem(src)
	dstMap := getElem(dst)
	if srcMap.Kind() != reflect.Map || dstMap.Kind() != reflect.Map {
		return
	}
	for _, key := range srcMap.MapKeys() {
		srcVal := srcMap.MapIndex(key)
		dstMap.SetMapIndex(key, srcVal)
	}
}

func getElem(a any) reflect.Value {
	val := reflect.ValueOf(a)
	if val.Kind() == reflect.Ptr {
		return val.Elem()
	}
	return val
}

///////////
// field //
///////////

func (f *field) Value() any { return f.v.Interface() }

func (f *field) String() (s string) {
	if f.v.Kind() == reflect.String {
		// NOTE: this will panic if the value's type is derived from string (e.g. WritePolicy)
		s = f.Value().(string)
	} else {
		s = fmt.Sprintf("%v", f.Value())
	}
	return
}

func (f *field) SetValue(src any, force ...bool) error {
	debug.Assert(!f.opts.OnlyRead)
	dst := f.v
	if f.listTag == tagReadonly && (len(force) == 0 || !force[0]) {
		return fmt.Errorf("property %q is readonly", f.name)
	}
	if !dst.CanSet() {
		return fmt.Errorf("failed to set value: %v", dst)
	}

	srcVal := reflect.ValueOf(src)
reflectDst:
	if srcVal.Kind() == reflect.String {
		dstType := dst.Type().Name()
		// added types: cos.Duration and cos.SizeIEC
		if dstType == "Duration" || dstType == "SizeIEC" {
			var (
				err error
				d   time.Duration
				n   int64
				s   = srcVal.String()
			)
			if dstType == "Duration" {
				d, err = time.ParseDuration(s)
				n = int64(d)
			} else {
				n, err = cos.ParseSize(s, cos.UnitsIEC)
			}
			if err == nil {
				dst.SetInt(n)
				f.dirty = true
			}
			return err
		}
	}
	switch srcVal.Kind() {
	case reflect.String:
		switch dst.Kind() {
		case reflect.String:
			dst.SetString(srcVal.String())
		case reflect.Bool:
			n, err := cos.ParseBool(srcVal.String())
			if err != nil {
				return err
			}
			dst.SetBool(n)
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			n, err := strconv.ParseInt(srcVal.String(), 10, 64)
			if err != nil {
				return err
			}
			dst.SetInt(n)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			n, err := strconv.ParseUint(srcVal.String(), 10, 64)
			if err != nil {
				return err
			}
			dst.SetUint(n)
		case reflect.Float32, reflect.Float64:
			n, err := strconv.ParseFloat(srcVal.String(), dst.Type().Bits())
			if err != nil {
				return err
			}
			dst.SetFloat(n)
		case reflect.Ptr:
			dst.Set(reflect.New(dst.Type().Elem())) // set pointer to default value
			dst = dst.Elem()                        // dereference pointer
			goto reflectDst
		case reflect.Slice:
			// A slice value looks like: "[value1 value2]"
			s := strings.TrimPrefix(srcVal.String(), "[")
			s = strings.TrimSuffix(s, "]")

			// "none" is a system-wide convention
			if s == apc.ResetToken {
				empty := reflect.MakeSlice(dst.Type(), 0, 0)
				dst.Set(empty)
				break
			}

			if s != "" {
				vals := strings.Split(s, " ")
				tp := reflect.TypeFor[string]()
				lst := reflect.MakeSlice(reflect.SliceOf(tp), 0, 10)
				for _, v := range vals {
					if v == "" {
						continue
					}
					lst = reflect.Append(lst, reflect.ValueOf(v))
				}
				dst.Set(lst)
			}
		case reflect.Map:
			// do nothing (e.g. ObjAttrs.CustomMD)
		default:
			debug.Assertf(false, "field.name: %s, field.type: %s", f.listTag, dst.Kind())
		}
	default:
		if !srcVal.IsValid() {
			if src != nil {
				debug.FailTypeCast(srcVal)
				return nil
			}
			srcVal = reflect.Zero(dst.Type())
		}
		if dst.Kind() == reflect.Ptr {
			if dst.IsNil() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			dst = dst.Elem()
		}
		dst.Set(srcVal)
	}

	f.dirty = true
	return nil
}
