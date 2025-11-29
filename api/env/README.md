## System environment variables

The package contains system environment variables - globally defined names that include `AIS_ENDPOINT`, `AIS_AUTHN_TOKEN_FILE`, and more.

As such, the `env` package is, effectively, part of the API: the names defined here are used throughout, both in the Go code and in the scripts. In particular, deployment scripts.

> It is important to preserve consistency and reference the same names (without copy-paste duplication, when possible)

|   Source   | Environment Variables Group |
|------------|----------|
| `ais.go`   | `aisnode` executable (see also: [aisnode command line](https://github.com/NVIDIA/aistore/blob/main/docs/command_line.md)) |
| `authn.go` | [AuthN](https://github.com/NVIDIA/aistore/tree/main/cmd/authn) environment|
| `aws.go`   | Amazon S3 backend (`S3_ENDPOINT`, etc.) |
| `oci.go`   | Oracle OCI backend |
| `test.go`  | Integration tests |

## See also

* List of _all_ [environment variables](https://github.com/NVIDIA/aistore/blob/main/docs/environment-vars.md)
* List of [system filenames ("filename constants")](https://github.com/NVIDIA/aistore/blob/main/cmn/fname/fname.go)
