## System environment variables

The package contains system environment variables - globally defined names that include `AIS_ENDPOINT`, `AIS_AUTHN_TOKEN_FILE`, and more.

As such, the `env` package is, effectively, part of the API: the names defined here are used throughout, both in the Go code and in the scripts. In particular, deployment scripts.

> It is important to preserve consistency and reference the same names (without copy-paste duplication, when possible)

* `ais.go`:   AIS environment
* `authn.go`: AuthN environment
* `debug.go`: DEBUG environment (build and command-line)

For the list of private system filenames (aka "filename constants"), see also: cmn/fname/fname.go
