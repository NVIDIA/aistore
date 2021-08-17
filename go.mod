module github.com/NVIDIA/aistore

go 1.16

// NOTE: Remember to update `deploy/test/ci` image if the dependencies are updated.

require (
	cloud.google.com/go/storage v1.15.0
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/NVIDIA/go-tfdata v0.3.1
	github.com/OneOfOne/xxhash v1.2.8
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/andybalholm/brotli v1.0.2 // indirect
	github.com/aws/aws-sdk-go v1.38.32
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/fatih/color v1.10.0
	github.com/form3tech-oss/jwt-go v3.2.2+incompatible
	github.com/frankban/quicktest v1.5.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f
	github.com/jacobsa/fuse v0.0.0-20210811193110-7782064498ca
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/json-iterator/go v1.1.11
	github.com/karrick/godirwalk v1.16.1
	github.com/klauspost/compress v1.12.2 // indirect
	github.com/klauspost/cpuid/v2 v2.0.6 // indirect
	github.com/klauspost/reedsolomon v1.9.12
	github.com/lufia/iostat v1.1.0
	github.com/onsi/ginkgo v1.16.2
	github.com/onsi/gomega v1.11.0
	github.com/pierrec/lz4/v3 v3.3.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.10.0
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/seiflotfy/cuckoofilter v0.0.0-20201222105146-bc6005554a0c
	github.com/teris-io/shortid v0.0.0-20201117134242-e59966efd125
	github.com/tidwall/btree v0.5.0 // indirect
	github.com/tidwall/buntdb v1.2.3
	github.com/tidwall/gjson v1.7.5 // indirect
	github.com/tinylib/msgp v1.1.5
	github.com/urfave/cli v1.22.5
	github.com/valyala/fasthttp v1.24.0
	github.com/vbauerster/mpb/v4 v4.12.2
	golang.org/x/crypto v0.0.0-20210503195802-e9a32991a82e
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/net v0.0.0-20210504132125-bbd867fde50d // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210503173754-0981d6026fa6
	golang.org/x/term v0.0.0-20210503060354-a79de5458b56
	google.golang.org/api v0.46.0
	google.golang.org/genproto v0.0.0-20210504143626-3b2ad6ccc450 // indirect
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	k8s.io/api v0.21.0
	k8s.io/apimachinery v0.21.0
	k8s.io/client-go v0.21.0
	k8s.io/metrics v0.21.0
	k8s.io/utils v0.0.0-20210305010621-2afb4311ab10 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.1.1 // indirect
)
