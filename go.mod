module github.com/NVIDIA/aistore

go 1.17

// NOTE: Remember to update `deploy/test/ci` image if the dependencies are updated.

// Direct dependecies.
require (
	cloud.google.com/go/storage v1.15.0
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/NVIDIA/go-tfdata v0.3.1
	github.com/OneOfOne/xxhash v1.2.8
	github.com/aws/aws-sdk-go v1.38.32
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/fatih/color v1.10.0
	github.com/form3tech-oss/jwt-go v3.2.2+incompatible
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f
	github.com/jacobsa/fuse v0.0.0-20210811193110-7782064498ca
	github.com/json-iterator/go v1.1.11
	github.com/karrick/godirwalk v1.16.1
	github.com/klauspost/reedsolomon v1.9.12
	github.com/lufia/iostat v1.1.0
	github.com/onsi/ginkgo v1.16.2
	github.com/onsi/gomega v1.11.0
	github.com/pierrec/lz4/v3 v3.3.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.10.0
	github.com/seiflotfy/cuckoofilter v0.0.0-20201222105146-bc6005554a0c
	github.com/teris-io/shortid v0.0.0-20201117134242-e59966efd125
	github.com/tidwall/buntdb v1.2.3
	github.com/tinylib/msgp v1.1.5
	github.com/urfave/cli v1.22.5
	github.com/valyala/fasthttp v1.24.0
	github.com/vbauerster/mpb/v4 v4.12.2
	golang.org/x/crypto v0.0.0-20210503195802-e9a32991a82e
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210903071746-97244b99971b
	golang.org/x/term v0.0.0-20210503060354-a79de5458b56
	google.golang.org/api v0.46.0
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.21.0
	k8s.io/apimachinery v0.21.0
	k8s.io/client-go v0.21.0
	k8s.io/metrics v0.21.0
)

// Indirect dependecies.
require (
	cloud.google.com/go v0.81.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/andybalholm/brotli v1.0.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20200812162917-85c65e2d0165 // indirect
	github.com/frankban/quicktest v1.5.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/go-logr/logr v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/klauspost/compress v1.12.2 // indirect
	github.com/klauspost/cpuid/v2 v2.0.6 // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.18.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tidwall/btree v0.5.0 // indirect
	github.com/tidwall/gjson v1.7.5 // indirect
	github.com/tidwall/grect v0.1.1 // indirect
	github.com/tidwall/match v1.0.3 // indirect
	github.com/tidwall/pretty v1.1.0 // indirect
	github.com/tidwall/rtred v0.1.2 // indirect
	github.com/tidwall/tinyqueue v0.1.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/net v0.0.0-20210504132125-bbd867fde50d // indirect
	golang.org/x/oauth2 v0.0.0-20210427180440-81ed05c6b58c // indirect
	golang.org/x/text v0.3.6 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	golang.org/x/tools v0.1.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210504143626-3b2ad6ccc450 // indirect
	google.golang.org/grpc v1.37.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	k8s.io/klog/v2 v2.8.0 // indirect
	k8s.io/utils v0.0.0-20210305010621-2afb4311ab10 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.1.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)
