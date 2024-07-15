// Package ishard provides utility for shard the initial dataset
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ishard

import (
	"errors"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/tabwriter"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/ishard/ishard/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/xact"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
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

//lint:ignore U1000 Ignore unused function temporarily for debugging
func (n *dirNode) print(prefix string) {
	for name, child := range n.children {
		fmt.Printf("%s%s/", prefix, name)
		names := []string{}
		child.records.Lock()
		for _, r := range child.records.All() {
			names = append(names, r.Name)
		}
		child.records.Unlock()
		fmt.Println(names)

		child.print(prefix + "  ")
	}
}

//////////////
// ISharder //
//////////////

// ISharder executes an initial sharding job with given configuration
type ISharder struct {
	cfg            *config.Config
	shardIter      cos.ParsedTemplate
	baseParams     api.BaseParams
	progressBar    *mpb.Bar
	missingExtAct  config.MissingExtFunc
	sampleKeyRegex *regexp.Regexp
	CLItemplate    *template.Template
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

		name, hasNext := is.shardIter.Next()
		if !hasNext {
			return nil, 0, fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", is.shardIter.Count())
		}

		sh := &shard.Shard{
			Size:    totalSize,
			Records: recs,
			Name:    name,
		}
		totalSize = 0
		recs = shard.NewRecords(16)

		if is.cfg.DryRunFlag.IsSet {
			is.printShard(sh)
			continue
		}

		wg.Add(1)
		go func(sh *shard.Shard) {
			is.generateShard(sh, errCh)
			wg.Done()
		}(sh)
	}

	n.records.Lock()
	for _, record := range n.records.All() {
		totalSize += record.TotalSize()
		recs.Insert(record)

		if totalSize < is.cfg.MaxShardSize {
			continue
		}

		name, hasNext := is.shardIter.Next()
		if !hasNext {
			return nil, 0, fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", is.shardIter.Count())
		}

		sh := &shard.Shard{
			Size:    totalSize,
			Records: recs,
			Name:    name,
		}
		totalSize = 0
		recs = shard.NewRecords(16)

		if is.cfg.DryRunFlag.IsSet {
			is.printShard(sh)
			continue
		}

		wg.Add(1)
		go func(sh *shard.Shard) {
			is.generateShard(sh, errCh)
			wg.Done()
		}(sh)
	}
	n.records.Unlock()

	// if cfg.Collapse is not set, or no parent to collapse to (root level), archive all remaining objects regardless the current total size
	if !is.cfg.Collapse || path == "" {
		name, hasNext := is.shardIter.Next()
		if !hasNext {
			return nil, 0, fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", is.shardIter.Count())
		}

		sh := &shard.Shard{
			Size:    totalSize,
			Records: recs,
			Name:    name,
		}

		if is.cfg.DryRunFlag.IsSet {
			is.printShard(sh)
		} else {
			wg.Add(1)
			go func(sh *shard.Shard) {
				is.generateShard(sh, errCh)
				wg.Done()
			}(sh)
		}
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

func (is *ISharder) generateShard(sh *shard.Shard, errCh chan error) {
	defer sh.Records.Drain()
	if sh.Records.Len() == 0 {
		return
	}

	paths := []string{}
	sh.Records.Lock()
	for _, record := range sh.Records.All() {
		for _, obj := range record.Objects {
			paths = append(paths, obj.ContentPath)
		}
	}
	sh.Records.Unlock()

	msg := cmn.ArchiveBckMsg{
		ToBck: is.cfg.DstBck,
		ArchiveMsg: apc.ArchiveMsg{
			ArchName: sh.Name + is.cfg.Ext,
			ListRange: apc.ListRange{
				ObjNames: paths,
			},
		},
	}

	_, err := api.ArchiveMultiObj(is.baseParams, is.cfg.SrcBck, &msg)
	if err != nil {
		errCh <- fmt.Errorf("failed to archive shard %s: %w", sh.Name, err)
	}

	if is.progressBar != nil {
		is.progressBar.IncrBy(len(paths))
	}
}

func (is *ISharder) findMissingExt(node *dirNode, recursive bool) error {
	if node == nil {
		return nil
	}

	node.records.Lock()
	for _, record := range node.records.All() {
		extSet := make(map[string]struct{}, len(is.cfg.SampleExtensions))
		for _, ext := range is.cfg.SampleExtensions {
			extSet[ext] = struct{}{}
		}

		for _, obj := range record.Objects {
			delete(extSet, obj.Extension)
		}

		if len(extSet) > 0 {
			for ext := range extSet {
				if err := is.missingExtAct(ext); err != nil {
					return err
				}
			}
		}
	}
	node.records.Unlock()

	if recursive {
		for _, child := range node.children {
			if err := is.findMissingExt(child, recursive); err != nil {
				return err
			}
		}
	}

	return nil
}

func (is *ISharder) printShard(sh *shard.Shard) {
	defer sh.Records.Drain()
	if sh.Records.Len() == 0 {
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	if err := is.CLItemplate.Execute(w, sh); err != nil {
		fmt.Println("error executing template: %w", err)
	}
	w.Flush()
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

	if is.shardIter, err = cos.NewParsedTemplate(strings.TrimSpace(is.cfg.ShardTemplate)); err != nil {
		return nil, err
	}
	is.shardIter.InitIter()

	is.progressBar = nil

	if len(is.cfg.SampleExtensions) > 0 && is.cfg.MissingExtAction == "" {
		return nil, errors.New("MissingExtAction must be specified if SampleExtensions are provided")
	}
	is.missingExtAct = config.MissingExtActMap[is.cfg.MissingExtAction]
	is.sampleKeyRegex = regexp.MustCompile(is.cfg.SampleKeyPattern.Regex)

	if is.cfg.DryRunFlag.IsSet {
		var sb strings.Builder
		sb.WriteString("{{$shard := .}}{{appendExt $shard.Name}}\t{{formatSize $shard.Size}}\n")
		sb.WriteString("{{range $rec := .Records.All}}")
		if is.cfg.DryRunFlag.Mode == "show_keys" {
			sb.WriteString("  {{sampleKey $rec.Name}}\t\n")
		}
		sb.WriteString("{{range $obj := .Objects}}    {{contentPath $shard.Name $obj.ContentPath}}\t{{formatSize $obj.Size}}\n")
		sb.WriteString("{{end}}{{end}}")

		is.CLItemplate = template.Must(template.New("shard").Funcs(template.FuncMap{
			"formatSize":  func(size int64) string { return cos.ToSizeIEC(size, 2) },
			"sampleKey":   func(sampleKey string) string { return "[" + sampleKey + "]" },
			"contentPath": func(shardName, objContentPath string) string { return filepath.Join(shardName, objContentPath) },
			"appendExt":   func(shardName string) string { return shardName + is.cfg.Ext },
		}).Parse(sb.String()))
	}

	return is, err
}

func (is *ISharder) Start() error {
	msg := &apc.LsoMsg{Prefix: is.cfg.SrcPrefix, Flags: apc.LsNameSize}
	objList, err := api.ListObjects(is.baseParams, is.cfg.SrcBck, msg, api.ListArgs{})
	if err != nil {
		return err
	}

	// Configure progress bar
	if is.cfg.Progress {
		var (
			text     = "Objects Processed: "
			progress = mpb.New(mpb.WithWidth(64))
			total    = int64(len(objList.Entries))
			options  = []mpb.BarOption{
				mpb.PrependDecorators(
					decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
					decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
				),
				mpb.AppendDecorators(
					decor.NewPercentage("%d", decor.WCSyncSpaceR),
					decor.Elapsed(decor.ET_STYLE_GO, decor.WCSyncWidth),
				),
			}
		)
		is.progressBar = progress.AddBar(total, options...)
	}

	// Parse object list
	root := newDirNode()
	for _, en := range objList.Entries {
		sampleKey := is.sampleKeyRegex.ReplaceAllString(en.Name, is.cfg.SampleKeyPattern.CaptureGroup)
		root.insert(sampleKey, en.Name, en.Size)
	}

	// Check missing extensions
	if len(is.cfg.SampleExtensions) > 0 {
		if err := is.findMissingExt(root, true); err != nil {
			return err
		}
	}

	if _, _, err := is.archive(root, ""); err != nil {
		return err
	}

	if is.cfg.DryRunFlag.IsSet {
		return nil
	}

	// Wait until all archive xactions reach to idle stage
	return api.WaitForXactionIdle(is.baseParams, &xact.ArgsMsg{Kind: apc.ActArchive, Bck: is.cfg.SrcBck})
}
