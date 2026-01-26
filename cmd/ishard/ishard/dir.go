// Package ishard provides utility for shard the initial dataset
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ishard

import (
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmd/ishard/ishard/config"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
)

// Represents the hierarchical structure of virtual directories within a bucket
type dirNode struct {
	children map[string]*dirNode
	records  *shard.Records
}

func newDirNode() *dirNode {
	return &dirNode{
		children: make(map[string]*dirNode),
		records:  shard.NewRecords(16),
	}
}

func (n *dirNode) insert(keyPath, fullPath string, size int64) {
	parts := strings.Split(keyPath, "/")
	current := n

	for i, part := range parts {
		if _, exists := current.children[part]; !exists {
			if i == len(parts)-1 {
				ext := filepath.Ext(fullPath)
				base := strings.TrimSuffix(keyPath, ext)
				current.records.Insert(&shard.Record{
					Key:  base,
					Name: base,
					Objects: []*shard.RecordObj{{
						ContentPath:  fullPath,
						StoreType:    shard.SGLStoreType,
						Offset:       0,
						MetadataSize: 0,
						Size:         size,
						Extension:    ext,
					}},
				})
			} else {
				current.children[part] = newDirNode()
			}
		}
		current = current.children[part]
	}
}

// apply performs a preorder traversal through the tree starting from the node `n`,
// applying the given reaction `act` to the Records of each node. The traversal stops if an error occurs.
func (n *dirNode) apply(act *config.MissingExtManager, recursive bool) error {
	if n == nil {
		return nil
	}

	newRecs, err := act.React(n.records)
	if err != nil {
		return err
	}
	if newRecs != nil {
		n.records.Drain()
		n.records = newRecs
	}

	if !recursive {
		return nil
	}

	for _, child := range n.children {
		if err := child.apply(act, recursive); err != nil {
			return err
		}
	}

	return nil
}
