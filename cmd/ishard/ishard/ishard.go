// Package ishard provides utility for shard the initial dataset
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ishard

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/ishard/ishard/config"
	"github.com/NVIDIA/aistore/cmd/ishard/ishard/factory"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
)

/////////////
// dirNode //
/////////////

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

//////////////
// ISharder //
//////////////

// ISharder executes an initial sharding job with given configuration
type ISharder struct {
	cfg            *config.Config
	baseParams     api.BaseParams
	sampleKeyRegex *regexp.Regexp

	shardFactory *factory.ShardFactory
}

// archive traverses through nodes and collects records on the way. Once it reaches
// the desired amount, it runs a goroutine to archive those collected records
func (is *ISharder) archive(n *dirNode, path string) (parentRecords *shard.Records, _ int64, _ error) {
	var (
		totalSize int64
		recs      = shard.NewRecords(16)
		errCh     = make(chan error, 1)
		wg        = cos.NewLimitedWaitGroup(cmn.MaxParallelism(), 0)
	)

	for name, child := range n.children {
		select {
		case err := <-errCh:
			return nil, 0, err
		default:
		}

		fullPath := path + "/" + name
		if path == "" {
			fullPath = name
		}
		childRecords, subtreeSize, err := is.archive(child, fullPath)
		if err != nil {
			return nil, 0, err
		}
		if childRecords != nil && childRecords.Len() != 0 {
			recs.Insert(childRecords.All()...)
		}
		totalSize += subtreeSize

		if totalSize < is.cfg.MaxShardSize {
			continue
		}

		wg.Add(1)
		go func(recs *shard.Records, size int64) {
			is.shardFactory.Create(recs, size, errCh)
			wg.Done()
		}(recs, totalSize)

		totalSize = 0
		recs = shard.NewRecords(16)
	}

	n.records.Lock()
	for _, record := range n.records.All() {
		select {
		case err := <-errCh:
			return nil, 0, err
		default:
		}

		totalSize += record.TotalSize()
		recs.Insert(record)

		if totalSize < is.cfg.MaxShardSize {
			continue
		}

		wg.Add(1)
		go func(recs *shard.Records, size int64) {
			is.shardFactory.Create(recs, size, errCh)
			wg.Done()
		}(recs, totalSize)

		totalSize = 0
		recs = shard.NewRecords(16)
	}
	n.records.Unlock()

	// if cfg.Collapse is not set, or no parent to collapse to (root level), archive all remaining objects regardless the current total size
	if !is.cfg.Collapse || path == "" {
		wg.Add(1)
		go func(recs *shard.Records, size int64) {
			is.shardFactory.Create(recs, size, errCh)
			wg.Done()
		}(recs, totalSize)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		return nil, 0, err
	case <-done:
		return recs, totalSize, nil
	}
}

// NewISharder instantiates an ISharder with the configuration if provided;
// otherwise, it loads from CLI or uses the default config.
func NewISharder(cfgArg *config.Config) (is *ISharder, err error) {
	is = &ISharder{}

	// Use provided config if given
	if cfgArg != nil {
		is.cfg = cfgArg
	} else {
		is.cfg, err = config.Load()
		if err != nil {
			defaultCfg := config.DefaultConfig
			is.cfg = &defaultCfg
		}
	}

	is.baseParams = api.BaseParams{URL: is.cfg.URL}
	is.baseParams.Client = cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true})

	is.sampleKeyRegex = regexp.MustCompile(is.cfg.SampleKeyPattern.Regex)

	return is, err
}

func (is *ISharder) Start() error {
	var (
		root         = newDirNode()
		objCounter   int
		objTotalSize int64
		err          error
	)

	msg := &apc.LsoMsg{Prefix: is.cfg.SrcPrefix, Flags: apc.LsNameSize}
	// Parse object list
	for {
		objListPage, err := api.ListObjectsPage(is.baseParams, is.cfg.SrcBck, msg, api.ListArgs{})
		if err != nil {
			return err
		}

		for _, en := range objListPage.Entries {
			sampleKey := is.sampleKeyRegex.ReplaceAllString(en.Name, is.cfg.SampleKeyPattern.CaptureGroup)
			root.insert(sampleKey, en.Name, en.Size)
			objCounter++
			objTotalSize += en.Size
		}

		if is.cfg.Progress {
			fmt.Printf("\rSource Objects: %s", cos.ToSizeIEC(objTotalSize, 2))
		}

		if objListPage.ContinuationToken == "" {
			break
		}
		msg.ContinuationToken = objListPage.ContinuationToken
	}

	if is.shardFactory, err = factory.NewShardFactory(is.baseParams, is.cfg.SrcBck, is.cfg.DstBck, is.cfg.Ext, is.cfg.ShardTemplate, is.cfg.DryRunFlag); err != nil {
		return err
	}

	// Check missing extensions
	if is.cfg.MExtMgr != nil {
		if err := root.apply(is.cfg.MExtMgr, true); err != nil {
			return err
		}
		debug.Assert(
			objTotalSize == is.cfg.MExtMgr.EffectiveObjSize || is.cfg.MExtMgr.Name == "exclude",
			"except for the 'exclude' action, the total object size calculated by the missing extension manager should equal the original size",
		)
	}

	if is.cfg.Progress {
		is.shardFactory.NewBar(is.cfg.MExtMgr.EffectiveObjSize)
	}

	if _, _, err := is.archive(root, ""); err != nil {
		return err
	}

	if is.cfg.DryRunFlag.IsSet {
		return nil
	}

	is.shardFactory.Wait()

	if is.cfg.SortFlag.IsSet || is.cfg.EKMFlag.IsSet {
		dsortUUID, err := is.sort(is.shardFactory.OutShardNames)
		if err != nil {
			return err
		}

		if err := is.waitSort(dsortUUID, nil); err != nil {
			return err
		}
	}
	return nil
}
