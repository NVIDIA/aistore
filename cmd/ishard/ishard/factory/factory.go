// Package factory provides functions to create shards and track their creation progress
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package factory

import (
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/ishard/ishard/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/vbauerster/mpb/v4"
)

// shardFactory creates shards and keeps track of the progress of their creation
type ShardFactory struct {
	baseParams api.BaseParams
	fromBck    cmn.Bck
	toBck      cmn.Bck
	ext        string

	// Create
	shardIterMu sync.Mutex
	shardIter   cos.ParsedTemplate

	// Poll
	pollCh       chan *shard.Shard
	pollWg       sync.WaitGroup
	pollProgress *mpb.Bar

	// Dry run
	dryRunCfg   config.DryRunFlag
	dryRunCLIMu sync.Mutex
	CLItemplate *template.Template
}

func NewShardFactory(baseParams api.BaseParams, fromBck, toBck cmn.Bck, ext, shardTmpl string, dryRun config.DryRunFlag) (sf *ShardFactory, err error) {
	sf = &ShardFactory{
		baseParams: baseParams,
		fromBck:    fromBck,
		toBck:      toBck,
		ext:        ext,

		// block when number of creating shards reaches to archive xacts's workCh size. otherwise xact commit may timeout. see xact/xs/archive.go
		pollCh:    make(chan *shard.Shard, 512),
		dryRunCfg: dryRun,
	}

	if sf.shardIter, err = cos.NewParsedTemplate(strings.TrimSpace(shardTmpl)); err != nil {
		return nil, err
	}
	sf.shardIter.InitIter()

	if dryRun.IsSet {
		var sb strings.Builder
		sb.WriteString("{{$shard := .}}{{appendExt $shard.Name}}\t{{formatSize $shard.Size}}\n")
		sb.WriteString("{{range $rec := .Records.All}}")
		if dryRun.Mode == "show_keys" {
			sb.WriteString("  {{sampleKey $rec.Name}}\t\n")
		}
		sb.WriteString("{{range $obj := .Objects}}    {{contentPath $shard.Name $obj.ContentPath}}\t{{formatSize $obj.Size}}\n")
		sb.WriteString("{{end}}{{end}}")

		sf.CLItemplate = template.Must(template.New("shard").Funcs(template.FuncMap{
			"formatSize":  func(size int64) string { return cos.ToSizeIEC(size, 2) },
			"sampleKey":   func(sampleKey string) string { return "[" + sampleKey + "]" },
			"contentPath": func(shardName, objContentPath string) string { return filepath.Join(shardName, objContentPath) },
			"appendExt":   func(shardName string) string { return shardName + ext },
		}).Parse(sb.String()))
	}

	sf.pollWg.Add(1)
	go sf.poll()
	return
}

func (sf *ShardFactory) Create(recs *shard.Records, size int64, errCh chan error) {
	if recs.Len() == 0 {
		return
	}

	defer recs.Drain()

	sf.shardIterMu.Lock()
	name, hasNext := sf.shardIter.Next()
	sf.shardIterMu.Unlock()
	if !hasNext {
		errCh <- fmt.Errorf("number of shards to be created exceeds expected number of shards (%d)", sf.shardIter.Count())
		return
	}

	sh := &shard.Shard{
		Size:    size,
		Records: recs,
		Name:    name,
	}

	paths := []string{}
	sh.Records.Lock()
	for _, record := range sh.Records.All() {
		for _, obj := range record.Objects {
			paths = append(paths, obj.ContentPath)
		}
	}
	sh.Records.Unlock()

	if sf.dryRunCfg.IsSet {
		sf.dryRunCLIMu.Lock()
		w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
		if err := sf.CLItemplate.Execute(w, sh); err != nil {
			fmt.Println("error executing template: %w", err)
		}
		w.Flush()
		sf.dryRunCLIMu.Unlock()
		return
	}

	msg := cmn.ArchiveBckMsg{
		ToBck: sf.toBck,
		ArchiveMsg: apc.ArchiveMsg{
			ArchName: sh.Name + sf.ext,
			ListRange: apc.ListRange{
				ObjNames: paths,
			},
		},
	}

	_, err := api.ArchiveMultiObj(sf.baseParams, sf.fromBck, &msg)
	if err != nil {
		errCh <- fmt.Errorf("failed to archive shard %s: %w", sh.Name, err)
		return
	}

	sf.pollCh <- sh
}

// start to poll completion of created shard from AIStore
func (sf *ShardFactory) poll() {
	pool := make(map[string]struct{}, 512)
	for sh := range sf.pollCh {
		backoff := time.Second
		for {
			if _, exists := pool[sh.Name+sf.ext]; exists {
				if sf.pollProgress != nil {
					sf.pollProgress.IncrInt64(sh.Size)
				}
				break
			}

			time.Sleep(backoff)
			shardList, err := api.ListObjects(sf.baseParams, sf.toBck, &apc.LsoMsg{Prefix: sf.shardIter.Prefix, Flags: apc.LsNameSize}, api.ListArgs{})
			if err != nil {
				nlog.Errorln(err)
				continue
			}
			for _, entry := range shardList.Entries {
				pool[entry.Name] = struct{}{}
			}
			backoff = time.Duration(min(time.Second*10, backoff*2))
		}
	}
	sf.pollWg.Done()
}

func (sf *ShardFactory) Wait() {
	close(sf.pollCh)
	sf.pollWg.Wait()
}
