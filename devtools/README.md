Devtools package purpose is solely to provide common functions for golang-based development tools around AIStore, which
include: tests, benchmarks, AIS Loader, soaktests.


This package should not be merged to/unified with the `cmn` package, or any other general-purpose package. 
General-purpose packages like `cnm` are used in core parts of AIStore, so using `devtools` in general-purpose
packages is strongly discouraged.

### When to not use `devtools`

Functions from this package should not be used outside of packages mentioned above (`bench`, `soak`, etc). The functions
 are not optimized for performance.

If a function's use is solely for testing (for example, the function uses `testing` package), it should be placed
 in `tutils` package.

### When to use `devtools`

When writing a function which is supposed to be used outside of core AIS parts (tests, soaktests, aisloader),
and shared between them.
 