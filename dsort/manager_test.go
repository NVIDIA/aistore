// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tinylib/msgp/msgp"
)

var _ = Describe("Init", func() {
	BeforeEach(func() {
		ctx.smapOwner = newTestSmap("target")
		ctx.node = ctx.smapOwner.Get().Tmap["target"]
		fs.Init()
	})

	It("should init with tar extension", func() {
		m := &Manager{ctx: dsortContext{t: cluster.NewTargetMock(nil)}}
		m.lock()
		defer m.unlock()
		sr := &ParsedRequestSpec{Extension: cmn.ExtTar, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cmn.ParsedQuantity{Type: cmn.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeFalse())
	})

	It("should init with tgz extension", func() {
		m := &Manager{ctx: dsortContext{t: cluster.NewTargetMock(nil)}}
		m.lock()
		defer m.unlock()
		sr := &ParsedRequestSpec{Extension: cmn.ExtTarTgz, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cmn.ParsedQuantity{Type: cmn.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})

	It("should init with tar.gz extension", func() {
		m := &Manager{ctx: dsortContext{t: cluster.NewTargetMock(nil)}}
		m.lock()
		defer m.unlock()
		sr := &ParsedRequestSpec{Extension: cmn.ExtTgz, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cmn.ParsedQuantity{Type: cmn.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})

	It("should init with zip extension", func() {
		m := &Manager{ctx: dsortContext{t: cluster.NewTargetMock(nil)}}
		m.lock()
		defer m.unlock()
		sr := &ParsedRequestSpec{Extension: cmn.ExtZip, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cmn.ParsedQuantity{Type: cmn.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})
})

func BenchmarkRecordsMarshal(b *testing.B) {
	benches := []struct {
		recordCnt    int
		recordObjCnt int
	}{
		{recordCnt: 100, recordObjCnt: 1},
		{recordCnt: 100, recordObjCnt: 3},
		{recordCnt: 100, recordObjCnt: 5},

		{recordCnt: 1_000, recordObjCnt: 1},
		{recordCnt: 1_000, recordObjCnt: 3},
		{recordCnt: 1_000, recordObjCnt: 5},

		{recordCnt: 10_000, recordObjCnt: 1},
		{recordCnt: 10_000, recordObjCnt: 3},
		{recordCnt: 10_000, recordObjCnt: 5},

		{recordCnt: 100_000, recordObjCnt: 1},
		{recordCnt: 100_000, recordObjCnt: 3},
		{recordCnt: 100_000, recordObjCnt: 5},
	}

	for _, bench := range benches {
		name := fmt.Sprintf("r:%d_o:%d", bench.recordCnt, bench.recordObjCnt)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			records := generateRecords(bench.recordCnt, bench.recordObjCnt)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				w := ioutil.Discard
				err := records.EncodeMsg(msgp.NewWriterSize(w, serializationBufSize))
				cmn.AssertNoErr(err)
			}
		})
	}
}

func BenchmarkRecordsUnmarshal(b *testing.B) {
	benches := []struct {
		recordCnt    int
		recordObjCnt int
	}{
		{recordCnt: 100, recordObjCnt: 1},
		{recordCnt: 100, recordObjCnt: 3},
		{recordCnt: 100, recordObjCnt: 5},

		{recordCnt: 1_000, recordObjCnt: 1},
		{recordCnt: 1_000, recordObjCnt: 3},
		{recordCnt: 1_000, recordObjCnt: 5},

		{recordCnt: 10_000, recordObjCnt: 1},
		{recordCnt: 10_000, recordObjCnt: 3},
		{recordCnt: 10_000, recordObjCnt: 5},

		{recordCnt: 100_000, recordObjCnt: 1},
		{recordCnt: 100_000, recordObjCnt: 3},
		{recordCnt: 100_000, recordObjCnt: 5},
	}

	for _, bench := range benches {
		name := fmt.Sprintf("r:%d_o:%d", bench.recordCnt, bench.recordObjCnt)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			records := generateRecords(bench.recordCnt, bench.recordObjCnt)
			buf := bytes.NewBuffer(nil)
			w := msgp.NewWriter(buf)
			err := records.EncodeMsg(w)
			cmn.AssertNoErr(err)
			cmn.AssertNoErr(w.Flush())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				buf := bytes.NewReader(buf.Bytes())
				newRecords := extract.NewRecords(bench.recordCnt)
				b.StartTimer()

				err := newRecords.DecodeMsg(msgp.NewReaderSize(buf, serializationBufSize))
				cmn.AssertNoErr(err)
			}
		})
	}
}

func BenchmarkCreationPhaseMetadataMarshal(b *testing.B) {
	benches := []struct {
		shardCnt     int
		recordCnt    int
		recordObjCnt int
	}{
		{shardCnt: 10, recordCnt: 100, recordObjCnt: 1},
		{shardCnt: 10, recordCnt: 100, recordObjCnt: 3},
		{shardCnt: 1_0000, recordCnt: 100, recordObjCnt: 1},
		{shardCnt: 1_0000, recordCnt: 100, recordObjCnt: 3},

		{shardCnt: 10, recordCnt: 10_000, recordObjCnt: 1},
		{shardCnt: 10, recordCnt: 10_000, recordObjCnt: 3},
		{shardCnt: 1_000, recordCnt: 1_000, recordObjCnt: 1},
		{shardCnt: 1_000, recordCnt: 1_000, recordObjCnt: 3},
	}

	for _, bench := range benches {
		name := fmt.Sprintf("s:%d_r:%d_o:%d", bench.shardCnt, bench.recordCnt, bench.recordObjCnt)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			md := CreationPhaseMetadata{
				Shards: generateShards(bench.shardCnt, bench.recordCnt, bench.recordObjCnt),
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := ioutil.Discard
				err := md.EncodeMsg(msgp.NewWriterSize(w, serializationBufSize))
				cmn.AssertNoErr(err)
			}
		})
	}
}

func BenchmarkCreationPhaseMetadataUnmarshal(b *testing.B) {
	benches := []struct {
		shardCnt     int
		recordCnt    int
		recordObjCnt int
	}{
		{shardCnt: 10, recordCnt: 100, recordObjCnt: 1},
		{shardCnt: 10, recordCnt: 100, recordObjCnt: 3},
		{shardCnt: 1_0000, recordCnt: 100, recordObjCnt: 1},
		{shardCnt: 1_0000, recordCnt: 100, recordObjCnt: 3},

		{shardCnt: 10, recordCnt: 10_000, recordObjCnt: 1},
		{shardCnt: 10, recordCnt: 10_000, recordObjCnt: 3},
		{shardCnt: 1_000, recordCnt: 1_000, recordObjCnt: 1},
		{shardCnt: 1_000, recordCnt: 1_000, recordObjCnt: 3},
	}

	for _, bench := range benches {
		name := fmt.Sprintf("s:%d_r:%d_o:%d", bench.shardCnt, bench.recordCnt, bench.recordObjCnt)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			md := CreationPhaseMetadata{
				Shards: generateShards(bench.shardCnt, bench.recordCnt, bench.recordObjCnt),
			}
			buf := bytes.NewBuffer(nil)
			w := msgp.NewWriter(buf)
			cmn.AssertNoErr(md.EncodeMsg(w))
			cmn.AssertNoErr(w.Flush())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				buf := bytes.NewReader(buf.Bytes())
				newMD := &CreationPhaseMetadata{}
				b.StartTimer()

				err := newMD.DecodeMsg(msgp.NewReaderSize(buf, serializationBufSize))
				cmn.AssertNoErr(err)
			}
		})
	}
}

func generateShards(shardCnt, recordCnt, recordObjCnt int) []*extract.Shard {
	shards := make([]*extract.Shard, 0, shardCnt)
	for i := 0; i < shardCnt; i++ {
		s := &extract.Shard{
			Name:    fmt.Sprintf("shard-%d", i),
			Size:    rand.Int63(),
			Records: generateRecords(recordCnt, recordObjCnt),
		}
		shards = append(shards, s)
	}
	return shards
}

func generateRecords(recordCnt, recordObjCnt int) *extract.Records {
	records := extract.NewRecords(recordCnt)
	for i := 0; i < recordCnt; i++ {
		r := &extract.Record{
			Key:      cmn.RandString(20),
			Name:     cmn.RandString(30),
			DaemonID: cmn.RandString(10),
		}
		for j := 0; j < recordObjCnt; j++ {
			r.Objects = append(r.Objects, &extract.RecordObj{
				ContentPath:    cmn.RandString(50),
				ObjectFileType: "abc",
				StoreType:      "ab",
				Offset:         rand.Int63(),
				MetadataSize:   512,
				Size:           rand.Int63(),
				Extension:      "." + cmn.RandString(4),
			})
		}
		records.Insert(r)
	}
	return records
}
