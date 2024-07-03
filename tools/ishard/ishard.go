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
	"github.com/NVIDIA/aistore/xact"
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
func (n *Node) archive(path string) (parentRecords *shard.Records, _ int64, _ error) {
	var (
		totalSize int64
		recs      = shard.NewRecords(16)
		errCh     = make(chan error, 1)
	)

	for name, child := range n.children {
		fullPath := path + "/" + name
		if path == "" {
			fullPath = name
		}
		childRecords, subtreeSize, err := child.archive(fullPath)
		if err != nil {
			return nil, 0, err
		}
		if childRecords != nil && childRecords.Len() != 0 {
			recs.Insert(childRecords.All()...)
		}
		totalSize += subtreeSize
	}

	wg := cos.NewLimitedWaitGroup(cmn.MaxParallelism(), 0)
	n.records.Lock()
	for _, record := range n.records.All() {
		totalSize += record.TotalSize()
		recs.Insert(record)

		if totalSize < cfg.MaxShardSize {
			continue
		}

		name, hasNext := shardIter.Next()
		if !hasNext {
			return nil, 0, fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", shardCount)
		}
		wg.Add(1)
		go func(recs *shard.Records, name string) {
			generateShard(recs, name, errCh)
			wg.Done()
		}(recs, name)

		totalSize = 0
		recs = shard.NewRecords(16)
	}
	n.records.Unlock()

	// if cfg.Collapse is not set, or no parent to collapse to (root level), archive all remaining objects regardless the current total size
	if !cfg.Collapse || path == "" {
		name, hasNext := shardIter.Next()
		if !hasNext {
			return nil, 0, fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", shardCount)
		}

		wg.Add(1)
		go func(recs *shard.Records, name string) {
			generateShard(recs, name, errCh)
			wg.Done()
		}(recs, name)

		defer recs.Drain()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		return nil, 0, err
	}

	return recs, totalSize, nil
}

func generateShard(recs *shard.Records, name string, errCh chan error) {
	defer recs.Drain()

	if recs.Len() == 0 {
		return
	}

	paths := []string{}
	recs.Lock()
	for _, record := range recs.All() {
		for _, obj := range record.Objects {
			paths = append(paths, record.MakeUniqueName(obj))
		}
	}
	recs.Unlock()

	msg := cmn.ArchiveBckMsg{
		ToBck: cfg.DstBck,
		ArchiveMsg: apc.ArchiveMsg{
			ArchName: name + cfg.Ext,
			ListRange: apc.ListRange{
				ObjNames: paths,
			},
		},
	}

	_, err := api.ArchiveMultiObj(baseParams, cfg.SrcBck, &msg)
	if err != nil {
		errCh <- fmt.Errorf("failed to archive shard %s: %w", name, err)
	}
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

	if _, _, err := root.archive(""); err != nil {
		return err
	}

	if err := api.WaitForXactionIdle(baseParams, &xact.ArgsMsg{Kind: apc.ActArchive, Bck: cfg.SrcBck}); err != nil {
		return err
	}

	return err
}
