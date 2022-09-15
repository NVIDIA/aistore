// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/aisfs/ais"
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type (
	bucketMock struct {
		objs map[string]struct{}
	}
)

// interface guard
var _ ais.Bucket = (*bucketMock)(nil)

func newBucketMock() *bucketMock {
	return &bucketMock{
		objs: make(map[string]struct{}, 1),
	}
}

func (bm *bucketMock) addObj(obj string)    { bm.objs[obj] = struct{}{} }
func (bm *bucketMock) removeObj(obj string) { delete(bm.objs, obj) }

func (*bucketMock) Name() string              { return "empty" }
func (bm *bucketMock) Bck() cmn.Bck           { return cmn.Bck{Name: bm.Name(), Provider: apc.AIS} }
func (*bucketMock) APIParams() api.BaseParams { return api.BaseParams{} }
func (bm *bucketMock) HeadObject(objName string) (obj *ais.Object, exists bool, err error) {
	_, ok := bm.objs[objName]
	return nil, ok, nil
}

func (bm *bucketMock) ListObjects(prefix, _ string, pageSize uint) (objs []*ais.Object, nextToken string, err error) {
	for obj := range bm.objs {
		if !strings.HasPrefix(obj, prefix) {
			continue
		}

		objs = append(objs, ais.NewObject(obj, bm, 1024))
		if len(objs) == int(pageSize) {
			break
		}
	}

	return objs, "", nil
}

func (bm *bucketMock) DeleteObject(objName string) (err error) {
	delete(bm.objs, objName)
	return nil
}

var _ = Describe("Namespace", func() {
	var (
		bck *bucketMock
		ns  *namespace
	)

	var (
		fpath   = "a/b/c"
		dpath   = "a/b/c/"
		subDirs = []string{"a/", "a/b/"}
	)

	Describe("with space in cache", func() {
		BeforeEach(func() {
			var err error
			bck = newBucketMock()
			cfg := &ServerConfig{}
			ns, err = newNamespace(bck, nil, cfg)
			Expect(err).NotTo(HaveOccurred())
		})

		Describe("add", func() {
			It("should add file to namespace", func() {
				ns.add(entryFileTy, dtAttrs{
					id:   invalidInodeID,
					path: fpath,
					obj:  ais.NewObject(fpath, bck, 1024),
				})
				res, exists := ns.lookup(fpath)
				Expect(exists).To(BeTrue())
				Expect(res.IsDir()).To(BeFalse())
				Expect(res.Object).NotTo(BeNil())
				Expect(res.Object.Size).To(BeEquivalentTo(1024))

				for _, dirPath := range subDirs {
					_, exists = ns.lookup(dirPath)
					Expect(exists).To(BeTrue())
				}
			})

			It("should add directory to namespace", func() {
				ns.add(entryDirTy, dtAttrs{
					id:   invalidInodeID,
					path: dpath,
				})
				res, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())
				Expect(res.IsDir()).To(BeTrue())
				Expect(res.Object).To(BeNil())

				for _, dirPath := range subDirs {
					_, exists = ns.lookup(dirPath)
					Expect(exists).To(BeTrue())
				}
			})
		})

		Describe("remove", func() {
			It("should remove file from namespace", func() {
				ns.add(entryFileTy, dtAttrs{
					id:   invalidInodeID,
					path: fpath,
					obj:  ais.NewObject(fpath, bck, 1024),
				})
				_, exists := ns.lookup(fpath)
				Expect(exists).To(BeTrue())

				ns.remove(fpath)
				_, exists = ns.lookup(fpath)
				Expect(exists).To(BeFalse())

				for _, dirPath := range subDirs {
					_, exists = ns.lookup(dirPath)
					Expect(exists).To(BeTrue())
				}
			})

			It("should remove directory from namespace", func() {
				ns.add(entryDirTy, dtAttrs{
					id:   invalidInodeID,
					path: dpath,
				})
				_, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())

				ns.remove(dpath)
				_, exists = ns.lookup(dpath)
				Expect(exists).To(BeFalse())

				for _, dirPath := range subDirs {
					_, exists = ns.lookup(dirPath)
					Expect(exists).To(BeTrue())
				}
			})

			It("should remove nonempty directory from namespace", func() {
				filesPaths := []string{
					dpath + "d",
					dpath + "e/f",
				}

				ns.add(entryDirTy, dtAttrs{
					id:   invalidInodeID,
					path: dpath,
				})
				_, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())

				for _, filePath := range filesPaths {
					ns.add(entryFileTy, dtAttrs{
						id:   invalidInodeID,
						path: filePath,
					})
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeTrue())
				}

				ns.remove(dpath)
				_, exists = ns.lookup(dpath)
				Expect(exists).To(BeFalse())

				for _, filePath := range filesPaths {
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeFalse())
				}
			})
		})

		Describe("listEntries", func() {
			It("should list no entries", func() {
				var entries []nsEntry
				ns.listEntries("", func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(0))
			})

			It("should list entries for given directory", func() {
				var (
					entries []nsEntry

					filesPaths = []string{
						"a",
						dpath + "c",
						dpath + "d",
						dpath + "e/f",
					}
				)

				ns.add(entryDirTy, dtAttrs{
					id:   invalidInodeID,
					path: dpath,
				})
				_, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())

				for _, filePath := range filesPaths {
					ns.add(entryFileTy, dtAttrs{
						id:   invalidInodeID,
						path: filePath,
					})
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeTrue())
				}

				ns.listEntries(dpath, func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(3))

				var files, dirs []string
				for _, entry := range entries {
					Expect(entry.Name()).To(HavePrefix(dpath))
					switch entry.Ty() {
					case entryFileTy:
						files = append(files, entry.Name())
					case entryDirTy:
						dirs = append(dirs, entry.Name())
					}
				}

				Expect(files).To(HaveLen(2))
				Expect(dirs).To(HaveLen(1))
			})

			It("should only list directories after files are removed", func() {
				var (
					entries []nsEntry

					filesPaths = []string{
						"a",
						dpath + "c",
						dpath + "d",
						dpath + "e/f",
					}
				)

				ns.add(entryDirTy, dtAttrs{
					id:   invalidInodeID,
					path: dpath,
				})
				_, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())

				for _, filePath := range filesPaths {
					ns.add(entryFileTy, dtAttrs{
						id:   invalidInodeID,
						path: filePath,
					})
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeTrue())
				}

				ns.listEntries(dpath, func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(3))

				for _, filePath := range filesPaths {
					ns.remove(filePath)
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeFalse())
				}

				entries = nil
				ns.listEntries(dpath, func(v nsEntry) {
					entries = append(entries, v)
				})

				var files, dirs []string
				for _, entry := range entries {
					Expect(entry.Name()).To(HavePrefix(dpath))
					switch entry.Ty() {
					case entryFileTy:
						files = append(files, entry.Name())
					case entryDirTy:
						dirs = append(dirs, entry.Name())
					}
				}

				Expect(files).To(HaveLen(0))
				Expect(dirs).To(HaveLen(1))
			})
		})
	})

	Describe("without space in the cache", func() {
		BeforeEach(func() {
			var err error
			bck = newBucketMock()
			cfg := &ServerConfig{}
			ns, err = newNamespace(bck, nil, cfg)
			Expect(err).NotTo(HaveOccurred())

			ns.cacheHasAllObjects.Store(false)
		})

		Describe("listEntries", func() {
			It("should list no entries", func() {
				var entries []nsEntry
				ns.listEntries("", func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(0))
			})

			It("should list entries for given directory", func() {
				var (
					entries []nsEntry

					filesPaths = []string{
						"a",
						dpath + "c",
						dpath + "d",
						dpath + "e/f",
					}
				)

				for _, filePath := range filesPaths {
					bck.addObj(filePath)
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeTrue())
				}

				_, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())

				ns.listEntries(dpath, func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(3))

				var files, dirs []string
				for _, entry := range entries {
					Expect(entry.Name()).To(HavePrefix(dpath))
					switch entry.Ty() {
					case entryFileTy:
						files = append(files, entry.Name())
					case entryDirTy:
						dirs = append(dirs, entry.Name())
					}
				}

				Expect(files).To(HaveLen(2))
				Expect(dirs).To(HaveLen(1))
			})

			It("should only list directories after files are removed", func() {
				var (
					entries []nsEntry

					filesPaths = []string{
						"a",
						dpath + "c",
						dpath + "d",
						dpath + "e/f",
						dpath + "g/h/i",
					}

					expectedEntries = []string{
						dpath + "c",
						dpath + "d",
						dpath + "e/",
						dpath + "g/",
					}
				)

				for _, filePath := range filesPaths {
					bck.addObj(filePath)
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeTrue())
				}

				_, exists := ns.lookup(dpath)
				Expect(exists).To(BeTrue())

				ns.listEntries(dpath, func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(len(expectedEntries)))

				for _, entry := range entries {
					Expect(expectedEntries).To(ContainElement(entry.Name()))
				}

				for _, filePath := range filesPaths {
					bck.removeObj(filePath)
					_, exists := ns.lookup(filePath)
					Expect(exists).To(BeFalse())
				}

				entries = nil
				ns.listEntries(dpath, func(v nsEntry) {
					entries = append(entries, v)
				})
				Expect(entries).To(HaveLen(0))
			})
		})
	})
})
