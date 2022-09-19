// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

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
	for i := 0; i < srcVal.NumField(); i++ {
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
				allocatedStruct = true
				srcValField.Set(reflect.New(srcValField.Type().Elem()))
			}
			srcValField = srcValField.Elem()
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
		if srcValField.Kind() == reflect.Slice {
			if !jsonTagPresent {
				continue
			}
			name := prefix + fieldName
			field := &field{name: name, v: srcValField, listTag: listTag, opts: opts}
			err, stop = updf(name, field)
			dirtyField = field.dirty
		} else if srcValField.Kind() != reflect.Struct {
			// We require that not-omitted fields have JSON tag.
			debug.AssertMsg(jsonTagPresent, prefix+"["+fieldName+"]")

			// Set value for the field
			name := prefix + fieldName
			field := &field{name: name, v: srcValField, listTag: listTag, opts: opts}
			err, stop = updf(name, field)
			dirtyField = field.dirty
		} else {
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

			if !strings.HasSuffix(p, ".") && !isInline {
				p += "."
			}

			if err == nil && !stop {
				dirtyField, stop, err = iterFields(p, srcValField.Interface(), updf, opts)
				if allocatedStruct && !dirtyField {
					// If we initialized new struct but no field inside
					// it was set we must set the value of the field to
					// `nil` (as it was before) otherwise we manipulated
					// the field for no reason.
					srcValField = srcVal.Field(i)
					srcValField.Set(reflect.Zero(srcValField.Type()))
				}
			}
		}

		dirty = dirty || dirtyField
		if stop {
			return
		}

		if err != nil {
			return dirty, true, err
		}
	}
	return
}

func copyProps(src, dst any, asType string) (err error) {
	var (
		srcVal = reflect.ValueOf(src)
		dstVal = reflect.ValueOf(dst).Elem()
	)
	debug.Assertf(cos.StringInSlice(asType, []string{apc.Daemon, apc.Cluster}), "unexpected config level: %s", asType)

	for i := 0; i < srcVal.NumField(); i++ {
		copyTag, ok := srcVal.Type().Field(i).Tag.Lookup("copy")
		if ok && copyTag == "skip" {
			continue
		}

		var (
			srcValField = srcVal.Field(i)
			fieldName   = srcVal.Type().Field(i).Name
			dstValField = dstVal.FieldByName(fieldName)
		)

		if srcValField.IsNil() {
			continue
		}

		t, ok := dstVal.Type().FieldByName(fieldName)
		debug.AssertMsg(ok, fieldName)
		// NOTE: the tag is used exclusively to enforce local vs global scope of the config var
		allowed := t.Tag.Get("allow")
		if allowed != "" && allowed != asType {
			return fmt.Errorf("cannot set property %s with config level %q as %q",
				fieldName, allowed, asType)
		}

		if dstValField.Kind() != reflect.Struct && dstValField.Kind() != reflect.Invalid {
			// Set value for the field
			if srcValField.Kind() != reflect.Ptr {
				dstValField.Set(srcValField)
			} else {
				dstValField.Set(srcValField.Elem())
			}
		} else {
			// Recurse into struct
			err = copyProps(srcValField.Elem().Interface(), dstValField.Addr().Interface(), asType)
			if err != nil {
				return
			}
		}
	}
	return
}

func mergeProps(src, dst any) {
	var (
		srcVal = reflect.ValueOf(src).Elem()
		dstVal = reflect.ValueOf(dst).Elem()
	)

	for i := 0; i < srcVal.NumField(); i++ {
		var (
			srcValField = srcVal.Field(i)
			dstValField = dstVal.FieldByName(srcVal.Type().Field(i).Name)
		)

		if srcValField.IsNil() {
			continue
		}

		if dstValField.IsNil() ||
			(srcValField.Elem().Kind() != reflect.Struct && srcValField.Elem().Kind() != reflect.Invalid) {
			dstValField.Set(srcValField)
			continue
		}

		// Recurse into struct
		mergeProps(srcValField.Interface(), dstValField.Interface())
	}
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
	switch srcVal.Kind() {
	case reflect.String:
		s := srcVal.String()
	reflectDst:
		switch dst.Kind() {
		case reflect.String:
			dst.SetString(s)
		case reflect.Bool:
			n, err := cos.ParseBool(s)
			if err != nil {
				return err
			}
			dst.SetBool(n)
		case reflect.Int64:
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				if dst.Type().Name() == "Duration" /*cos.Duration*/ {
					var d time.Duration
					d, err = time.ParseDuration(s)
					n = int64(d)
				} else if dst.Type().Name() == "Size" /*cos.Size*/ {
					n, err = cos.S2B(s)
				}
			}
			if err != nil {
				return err
			}
			dst.SetInt(n)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			dst.SetInt(n)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return err
			}
			dst.SetUint(n)
		case reflect.Float32, reflect.Float64:
			n, err := strconv.ParseFloat(s, dst.Type().Bits())
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
			if s != "" {
				vals := strings.Split(s, " ")
				tp := reflect.TypeOf(vals[0])
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
