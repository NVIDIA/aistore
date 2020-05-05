// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	tagOmitempty = "omitempty" // the field must be omitted when empty (only for read-only walk)
	tagOmit      = "omit"      // the field must be omitted
	tagReadonly  = "readonly"  // the field can be only read
)

type (
	// Represents single field in struct that was found during walking.
	IterField interface {
		// Value returns value of given field.
		Value() interface{}
		// SetValue sets a given value. `force` ignores `tagReadonly` and sets
		// a given value anyway - should be used with caution.
		SetValue(v interface{}, force ...bool) error
	}

	field struct {
		name    string
		v       reflect.Value
		listTag string
		dirty   bool // Determines if the value for the field was set by `SetValue`.
		opts    IterOpts
	}

	IterOpts struct {
		// Visits all the fields, not only the leaves.
		VisitAll bool
		// Determines whether this is only a read-only or write walk.
		// Read-only walk takes into consideration `tagOmitempty`.
		OnlyRead bool
	}
)

func (f *field) Value() interface{} {
	return f.v.Interface()
}

func (f *field) SetValue(src interface{}, force ...bool) error {
	Assert(!f.opts.OnlyRead)

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
			n, err := ParseBool(s)
			if err != nil {
				return err
			}
			dst.SetBool(n)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
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
		default:
			AssertMsg(false, fmt.Sprintf("field.name: %s, field.type: %s", f.listTag, dst.Kind()))
		}
	default:
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

func iterFields(prefix string, v interface{}, f func(uniqueTag string, field IterField) (error, bool), opts IterOpts) (dirty bool, err error, stop bool) {
	srcVal := reflect.ValueOf(v)
	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}

	for i := 0; i < srcVal.NumField(); i++ {
		var (
			srcTyField  = srcVal.Type().Field(i)
			srcValField = srcVal.Field(i)
		)

		// Check if we need to skip given field.
		listTag := srcTyField.Tag.Get("list")
		if listTag == tagOmit {
			continue
		}

		jsonTag, jsonTagPresent := srcTyField.Tag.Lookup("json")
		fieldName := strings.Split(jsonTag, ",")[0]
		if fieldName == "-" {
			continue
		}

		var (
			// Determines if the pointer to struct was allocated.
			// In case it was  but no field in the struct was
			// updated we must later set it to `nil`.
			allocatedStruct bool
		)

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

		var dirtyField bool
		if srcValField.Kind() != reflect.Struct {
			// We require that not-omitted fields have JSON tag.
			Assert(jsonTagPresent)

			// Set value for the field
			name := prefix + fieldName
			field := &field{name: name, v: srcValField, listTag: listTag, opts: opts}
			err, stop = f(name, field)
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
				err, stop = f(p, field)
				dirtyField = field.dirty
			}

			if !strings.HasSuffix(p, ".") {
				p += "."
			}

			if err == nil && !stop {
				dirtyField, err, stop = iterFields(p, srcValField.Interface(), f, opts)
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
			return dirty, err, true
		}
	}
	return
}

// IterFields walks the struct and calls `f` callback at every leaf field that it
// encounters. The (nested) names are created by joining the json tag with dot.
// Iteration supports reading another, custom tag `list` with values:
//   * `tagOmitempty` - omit empty fields (only for read run)
//   * `tagOmit` - omit field
//   * `tagReadonly` - field cannot be updated (returns error on `SetValue`)
// Examples of usages for tags can be found in `BucketProps` or `Config` structs.
//
// Passing additional options with `IterOpts` can for example call callback
// also at the non-leaf structures.
func IterFields(v interface{}, f func(uniqueTag string, field IterField) (error, bool), opts ...IterOpts) error {
	o := IterOpts{OnlyRead: true} // by default it's read run
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err, _ := iterFields("", v, f, o)
	return err
}

// UpdateFieldValue updates the field in the struct with given value.
// Returns error if the field was not found or could not be updated.
func UpdateFieldValue(s interface{}, name string, value interface{}) error {
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

func copyProps(src, dst interface{}) {
	var (
		srcVal = reflect.ValueOf(src)
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

		if dstValField.Kind() != reflect.Struct {
			// Set value for the field
			dstValField.Set(srcValField.Elem())
		} else {
			// Recurse into struct
			copyProps(srcValField.Elem().Interface(), dstValField.Addr().Interface())
		}
	}
}
