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

// Represents single field in struct that was found during walking.
type IterField interface {
	// Value returns value of given field.
	Value() interface{}
	// SetValue sets given value. `force` ignores `list:"readonly"` tag and sets
	// the value anyway - should be used with caution.
	SetValue(v interface{}, force ...bool) error
}

type field struct {
	v       reflect.Value
	listTag string
	dirty   bool // Determines if the value for the field was set by `SetValue`.
}

func (f *field) Value() interface{} {
	return f.v.Interface()
}

func (f *field) SetValue(src interface{}, force ...bool) error {
	dst := f.v
	if f.listTag == "readonly" && (len(force) == 0 || !force[0]) {
		return fmt.Errorf("cannot set value which is readonly: %v", dst)
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

func iterFields(prefix string, v interface{}, f func(uniqueTag string, field IterField) (error, bool)) (dirty bool, err error, stop bool) {
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
		if listTag == "omit" {
			continue
		}

		jsonTag, ok := srcTyField.Tag.Lookup("json")
		// We require that not-omitted fields have json tag.
		Assert(ok)
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

		var dirtyField bool
		if srcValField.Kind() != reflect.Struct {
			// Set value for the field
			field := &field{v: srcValField, listTag: listTag}
			err, stop = f(prefix+fieldName, field)
			dirtyField = field.dirty
		} else {
			// Recurse into struct

			// Always take address if possible (assuming that we will set value)
			if srcValField.CanAddr() {
				srcValField = srcValField.Addr()
			}

			dirtyField, err, stop = iterFields(prefix+fieldName+".", srcValField.Interface(), f)
			if allocatedStruct && !dirtyField {
				// If we initialized new struct but no field inside
				// it was set we must set the value of the field to
				// `nil` (as it was before) otherwise we manipulated
				// the field for no reason.
				srcValField = srcVal.Field(i)
				srcValField.Set(reflect.Zero(srcValField.Type()))
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

// IterFields walks the struct and calls `f` callback at every field that it
// encounters. The (nested) names are created by joining the json tag with dot.
// Iteration supports reading another, custom tag `list`. Thanks to this tag
// it is possible to tell that we need to `omit` (`list:"omit"`) a given field
// or tell that it is only `readonly` (`list:"readonly"`) (examples of these can
// be seen in `BucketProps` or `Config` structs).
func IterFields(v interface{}, f func(uniqueTag string, field IterField) (error, bool)) error {
	_, err, _ := iterFields("", v, f)
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
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("failed to find %q field in struct %T", name, s)
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
