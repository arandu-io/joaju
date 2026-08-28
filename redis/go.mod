module github.com/arandu-io/joaju/redis

go 1.26

// The bus, and its own module because of what is under it.
//
// The root module of this repository has one require line and a CI step that
// fails when it grows a second (ADR 0052): subpackage ws is this project's own
// RFC 6455 so that the transport costs nothing in the dependency graph, and a
// Redis client imported up there would spend that on every project that runs a
// single instance and needs no bus.
//
// Go has no optional dependency, so the only place a driver can live is behind
// a module boundary (ADR 0048). This is the same arrangement as
// github.com/arandu-io/hesape/redis, which is the connection this module
// borrows rather than opening a second one, and as github.com/arandu-io/hesape/filesystem/s3.
//
// hesape/redis carries a tag of its own, and so does this module: a Go submodule
// is versioned by the directory plus the version, and the proxy serves it under
// the full module path. Which version that is lives on the require line below
// and nowhere else: this sentence used to name one, and it went on naming
// redis/v0.5.0 through every bump after it. A version written in prose is a
// version nothing checks.
//
// # The parent is a require and not a replace
//
// It was `replace github.com/arandu-io/joaju => ..` until the require below, and
// that was the only replace in the collection. A replace pointing at .. builds
// this module against the working tree, so it never builds against the version
// anybody outside resolves: a symbol dropped from the parent and still used here
// compiles on the machine that dropped it and fails for the first person who
// imports this.
//
// The cost of the require is the release order, and the cost is the point. A
// change here that needs a symbol the parent has not published yet waits for the
// parent to be tagged. Putting the replace back is exactly how that stops being
// true, and it hides more than itself: the version check that compares this file
// against what is published skips any module carrying one, so a replace added
// today would also silence the alarm about the versions above going stale.
//
// Nothing in this module may have a replace, and nothing in the root module may
// either. The CI step called "no replace directive" is what says so, and it
// takes no argument: zero is cheaper to enforce than any rule with an allowance
// in it.
require (
	github.com/arandu-io/hesape/redis v0.7.0
	github.com/arandu-io/joaju v0.6.0
)

require (
	github.com/arandu-io/hesape v0.17.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
