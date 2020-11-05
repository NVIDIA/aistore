Devtools package purpose is solely to provide common functions for golang-based development tools around AIStore, which
include: tests, benchmarks, AIS Loader, soaktests.

It should **not** be used inside packages which are used during cluster runtime!