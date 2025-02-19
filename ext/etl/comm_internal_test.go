// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	cryptorand "crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func TestETLTransform(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}

var _ = Describe("CommunicatorTest", func() {
	var (
		tmpDir            string
		comm              Communicator
		transformerServer *httptest.Server
		targetServer      *httptest.Server
		proxyServer       *httptest.Server

		dataSize      = int64(50 * cos.MiB)
		transformData = make([]byte, dataSize)

		bck              = cmn.Bck{Name: "commBck", Provider: apc.AIS, Ns: cmn.NsGlobal}
		objName          = "commObj"
		etlTransformArgs = "{\"from_time\":2.43,\"to_time\":3.43}"
		clusterBck       = meta.NewBck(
			bck.Name, bck.Provider, bck.Ns,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}},
		)
		bmdMock = mock.NewBaseBownerMock(clusterBck)
	)

	BeforeEach(func() {
		// Generate random data.
		_, err := cryptorand.Read(transformData)
		Expect(err).NotTo(HaveOccurred())

		// Initialize the filesystem and directories.
		tmpDir, err = os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())

		mpath := filepath.Join(tmpDir, "mpath")
		err = cos.CreateDir(mpath)
		Expect(err).NotTo(HaveOccurred())
		fs.TestNew(nil)
		_, err = fs.Add(mpath, "daeID")
		Expect(err).NotTo(HaveOccurred())

		_ = mock.NewTarget(bmdMock)
		// cluster.InitLomLocker(tMock)

		// Create an object.
		lom := &core.LOM{ObjName: objName}
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
			receivedEtlTransformArgs := r.URL.Query().Get(apc.QparamETLTransformArgs)
			Expect(receivedEtlTransformArgs).To(Equal(etlTransformArgs))

			_, err := w.Write(transformData)
			Expect(err).NotTo(HaveOccurred())
		}))
		targetServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedEtlTransformArgs := r.URL.Query().Get(apc.QparamETLTransformArgs)
			err := comm.InlineTransform(w, r, lom, receivedEtlTransformArgs)
			Expect(err).NotTo(HaveOccurred())
		}))
		proxyServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirectURL := targetServer.URL + r.URL.Path + "?"
			if r.URL.RawQuery != "" {
				redirectURL += r.URL.RawQuery + "&"
			}
			http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
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
			boot := &etlBootstrapper{
				msg: InitSpecMsg{
					InitMsgBase: InitMsgBase{
						CommTypeX: commType,
					},
				},
				pod:  pod,
				uri:  transformerServer.URL,
				xctn: xctn,
			}
			comm = newCommunicator(nil, boot, nil)

			q := url.Values{}
			q.Add(apc.QparamETLTransformArgs, etlTransformArgs)
			resp, err := http.Get(proxyServer.URL + "?" + q.Encode())
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			b, err := cos.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(b)).To(Equal(len(transformData)))
			Expect(b).To(Equal(transformData))
		})
	}
})

// Creates a file with random content.
func createRandomFile(fileName string, size int64) error {
	b := make([]byte, size)
	if _, err := cryptorand.Read(b); err != nil {
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
