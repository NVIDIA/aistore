// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"text/tabwriter"
	"text/template"

	jsoniter "github.com/json-iterator/go"
)

// auxiliary
type Opts struct {
	AltMap  template.FuncMap
	Units   string
	UseJSON bool
}

func Jopts(usejs bool) Opts { return Opts{UseJSON: usejs} }

// main func
func Print(object any, templ string, aux ...Opts) error {
	var opts Opts
	if len(aux) > 0 {
		opts = aux[0]
	}
	if opts.UseJSON {
		if o, ok := object.(forMarshaler); ok {
			object = o.forMarshal()
		}
		out, err := jsoniter.MarshalIndent(object, "", "    ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(Writer, string(out))
		return err
	}

	fmap := funcMap
	if opts.AltMap != nil {
		fmap = make(template.FuncMap, len(funcMap))
		for k, v := range funcMap {
			if altv, ok := opts.AltMap[k]; ok {
				fmap[k] = altv
			} else {
				fmap[k] = v
			}
		}
	}

	parsedTempl, err := template.New("DisplayTemplate").Funcs(fmap).Parse(templ)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(Writer, 0, 8, 1, '\t', 0)
	if err := parsedTempl.Execute(w, object); err != nil {
		return err
	}
	return w.Flush()
}
