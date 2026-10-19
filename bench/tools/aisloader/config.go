// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type configValue struct {
	name, value string
	line        int
}

func loadConfig(f *flag.FlagSet, configPath string) error {
	explicit := make(map[string]bool)
	f.Visit(func(fl *flag.Flag) { explicit[fl.Name] = true })
	if !explicit["config"] {
		return nil
	}

	if configPath == "" {
		return errors.New("-config requires a nonempty path")
	}
	file, err := os.Open(configPath)
	if err != nil {
		return err
	}
	defer file.Close()

	values, err := readConfig(file, f)
	if err != nil {
		return fmt.Errorf("config %q: %w", configPath, err)
	}

	for _, v := range values {
		if explicit[v.name] {
			continue
		}
		if err := f.Set(v.name, v.value); err != nil {
			return fmt.Errorf("config %q: line %d: %s=%q: %w", configPath, v.line, v.name, v.value, err)
		}
	}
	return nil
}

func readConfig(r io.Reader, f *flag.FlagSet) ([]configValue, error) {
	d := yaml.NewDecoder(r)
	var doc, extra yaml.Node
	if err := d.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("read YAML: %w", err)
	}
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("read YAML: %w", err)
		}
		return nil, errors.New("config must contain exactly one YAML document")
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("config must be a flat YAML mapping")
	}

	root := doc.Content[0]
	seen := make(map[string]bool, len(root.Content)/2)
	values := make([]configValue, 0, len(root.Content)/2)
	for i := 0; i < len(root.Content); i += 2 {
		key, node := root.Content[i], root.Content[i+1]
		name := key.Value
		if seen[name] {
			return nil, fmt.Errorf("line %d: duplicate config key %q", key.Line, name)
		}
		seen[name] = true
		if isCLIOnly(name) {
			return nil, fmt.Errorf("line %d: %q is CLI-only", key.Line, name)
		}
		if f.Lookup(name) == nil {
			return nil, fmt.Errorf("line %d: unknown config key %q", key.Line, name)
		}
		if node.Kind == yaml.AliasNode {
			node = node.Alias
		}
		if node.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("line %d: %s: config values must be scalars", key.Line, name)
		}
		// null leaves the flag unset; everything else is parsed by the flag itself
		if node.Tag == "!!null" {
			continue
		}
		values = append(values, configValue{name: name, value: node.Value, line: key.Line})
	}
	return values, nil
}

func isCLIOnly(name string) bool {
	switch name {
	case "config", "usage", "version", "dry-run", "getloaderid":
		return true
	default:
		return false
	}
}
