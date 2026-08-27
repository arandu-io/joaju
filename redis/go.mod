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
// hesape/redis carries a tag of its own, redis/v0.5.0, which is how a Go
// submodule is versioned: the tag is the directory plus the version, and the
// proxy serves it under the full module path.
require (
	github.com/arandu-io/hesape/redis v0.6.1
	github.com/arandu-io/joaju v0.1.0
)

require (
	github.com/arandu-io/hesape v0.15.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The parent is this directory's own, so the two move together: a change to the
// Bus interface and the implementation of it are one commit, and the version
// above is what anyone outside the repository resolves.
replace github.com/arandu-io/joaju => ..
