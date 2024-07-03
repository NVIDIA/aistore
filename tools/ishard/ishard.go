// Package ishard provides utility for shard the initial dataset
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ishard

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/tools/ishard/config"
)

var (
	cfg        *config.Config
	baseParams api.BaseParams
)

var (
	shardIter  cos.ParsedTemplate
	shardCount int64
)

// Represents the hierarchical structure of virtual directories within a bucket
type Node struct {
	children map[string]*Node
	records  *shard.Records
}

func NewNode() *Node {
	return &Node{
		children: make(map[string]*Node),
		records:  shard.NewRecords(16),
	}
}

func (n *Node) Insert(path string, size int64) {
	parts := strings.Split(path, "/")
	current := n

	for i, part := range parts {
		if _, exists := current.children[part]; !exists {
			if i == len(parts)-1 {
				ext := filepath.Ext(path)
				base := strings.TrimSuffix(path, ext)
				current.records.Insert(&shard.Record{
					Key:  base,
					Name: base,
					Objects: []*shard.RecordObj{{
						ContentPath:  path,
						StoreType:    shard.SGLStoreType,
						Offset:       0,
						MetadataSize: 0,
						Size:         size,
						Extension:    ext,
					}},
				})
			} else {
				current.children[part] = NewNode()
			}
		}
		current = current.children[part]
	}
}

func (n *Node) Print(prefix string) {
	for name, child := range n.children {
		fmt.Printf("%s%s/", prefix, name)
		names := []string{}
		child.records.Lock()
		for _, r := range child.records.All() {
			names = append(names, r.Name)
		}
		child.records.Unlock()
		fmt.Println(names)

		child.Print(prefix + "  ")
	}
}

// Archive objects from this node into shards according to subdirectories structure
func (n *Node) Archive() error {
	paths := []string{}
	if _, err := archiveNode(n, "", &paths); err != nil {
		return err
	}

	if cfg.Collapse && len(paths) != 0 {
		if err := generateShard(paths); err != nil {
			return err
		}
	}

	return nil
}

func archiveNode(node *Node, path string, parentPaths *[]string) (int64, error) {
	totalSize := int64(0)
	paths := []string{}

	for name, child := range node.children {
		fullPath := path + "/" + name
		if path == "" {
			fullPath = name
		}
		subtreeSize, err := archiveNode(child, fullPath, &paths)
		if err != nil {
			return 0, err
		}
		totalSize += subtreeSize
	}

	node.records.Lock()
	for _, record := range node.records.All() {
		totalSize += record.TotalSize()
		for _, obj := range record.Objects {
			paths = append(paths, record.MakeUniqueName(obj))
		}
		if totalSize > cfg.MaxShardSize {
			if err := generateShard(paths); err != nil {
				return 0, err
			}
			totalSize = 0
			paths = []string{}
		}
	}
	node.records.Unlock()

	if len(paths) == 0 {
		return 0, nil
	}

	// Allow to flatten remaining objects into parent directory
	if cfg.Collapse {
		*parentPaths = append(*parentPaths, paths...)
		paths = nil
		return totalSize, nil
	}

	// Otherwise, archive all remaining objects regardless the current total size
	if err := generateShard(paths); err != nil {
		return 0, err
	}

	return totalSize, nil
}

func generateShard(paths []string) error {
	name, hasNext := shardIter.Next()
	if !hasNext {
		return fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", shardCount)
	}

	msg := cmn.ArchiveBckMsg{
		ToBck: cfg.DstBck,
		ArchiveMsg: apc.ArchiveMsg{
			ArchName: name + cfg.Ext,
			ListRange: apc.ListRange{
				ObjNames: paths,
			},
		},
	}

	if _, err := api.ArchiveMultiObj(baseParams, cfg.SrcBck, &msg); err != nil {
		return fmt.Errorf("failed to archive shard %s: %w", name, err)
	}

	return nil
}

// Init sets the configuration for ishard. If a config is provided, it uses that;
// otherwise, it loads from CLI or uses the default config.
func Init(cfgArg *config.Config) error {
	var err error

	// Use provided config if given
	if cfgArg != nil {
		cfg = cfgArg
	} else {
		cfg, err = config.Load()
		if err != nil {
			nlog.Errorf("Error initializing config: %v. Using default config.", err)
			defaultCfg := config.DefaultConfig
			cfg = &defaultCfg
		}
	}

	baseParams = api.BaseParams{URL: cfg.URL}
	baseParams.Client = cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true})

	if shardIter, err = cos.NewParsedTemplate(strings.TrimSpace(cfg.ShardTemplate)); err != nil {
		return err
	}
	shardIter.InitIter()
	shardCount = shardIter.Count()

	return err
}

func Start() error {
	msg := &apc.LsoMsg{}
	objList, err := api.ListObjects(baseParams, cfg.SrcBck, msg, api.ListArgs{})
	if err != nil {
		return err
	}

	root := NewNode()
	for _, en := range objList.Entries {
		root.Insert(en.Name, en.Size)
	}

	return root.Archive()
}
