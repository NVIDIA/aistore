// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("CommunicatorTest", func() {
	var (
		tmpDir            string
		comm              Communicator
		tMock             cluster.Target
		transformerServer *httptest.Server
		targetServer      *httptest.Server
		proxyServer       *httptest.Server

		dataSize      = int64(cos.MiB * 50)
		transformData = make([]byte, dataSize)

		bck        = cmn.Bck{Name: "commBck", Provider: apc.AIS, Ns: cmn.NsGlobal}
		objName    = "commObj"
		clusterBck = cluster.NewBck(
			bck.Name, bck.Provider, bck.Ns,
			&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}},
		)
		bmdMock = mock.NewBaseBownerMock(clusterBck)
	)

	BeforeEach(func() {
		// Generate random data.
		_, err := rand.Read(transformData)
		Expect(err).NotTo(HaveOccurred())

		// Initialize the filesystem and directories.
		tmpDir, err = os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		mpath := filepath.Join(tmpDir, "mpath")
		err = cos.CreateDir(mpath)
		Expect(err).NotTo(HaveOccurred())
		fs.TestNew(nil)
		fs.TestDisableValidation()
		_, err = fs.Add(mpath, "daeID")
		Expect(err).NotTo(HaveOccurred())

		tMock = mock.NewTarget(bmdMock)
		// cluster.InitLomLocker(tMock)

		// Create an object.
		lom := &cluster.LOM{ObjName: objName}
		err = lom.InitBck(clusterBck.Bucket())
		Expect(err).NotTo(HaveOccurred())
		err = createRandomFile(lom.FQN, dataSize)
		Expect(err).NotTo(HaveOccurred())
		lom.SetAtimeUnix(time.Now().UnixNano())
		lom.SetSize(dataSize)
		err = lom.Persist()
		Expect(err).NotTo(HaveOccurred())

		// Initialize the HTTP servers.
		transformerServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write(transformData)
			Expect(err).NotTo(HaveOccurred())
		}))
		targetServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := comm.OnlineTransform(w, r, clusterBck, objName)
			Expect(err).NotTo(HaveOccurred())
		}))
		proxyServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, targetServer.URL, http.StatusMovedPermanently)
		}))
	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)
		proxyServer.Close()
		transformerServer.Close()
		targetServer.Close()
	})

	tests := []string{
		Hpush,
		Hpull,
		Hrev,
	}

	for _, commType := range tests {
		It("should perform transformation "+commType, func() {
			pod := &corev1.Pod{}
			pod.SetName("somename")

			xctn := mock.NewXact(apc.ActETLInline)
			comm = makeCommunicator(commArgs{
				bootstrapper: &etlBootstrapper{
					t: tMock,
					msg: InitSpecMsg{
						InitMsgBase: InitMsgBase{
							CommTypeX: commType,
						},
					},
					pod:  pod,
					uri:  transformerServer.URL,
					xctn: xctn,
				},
			})
			resp, err := http.Get(proxyServer.URL)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(b)).To(Equal(len(transformData)))
			Expect(b).To(Equal(transformData))
		})
	}
})

// Creates a file with random content.
func createRandomFile(fileName string, size int64) error {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	f, err := cos.CreateFile(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := f.Write(b)
	if err != nil {
		return err
	}
	if int64(n) != size {
		return fmt.Errorf("could not write %d bytes, wrote %d bytes", size, n)
	}
	return nil
}
