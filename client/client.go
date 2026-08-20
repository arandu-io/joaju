// Package client is the browser half of the Pusher protocol: the JavaScript a
// page loads to talk to a [github.com/arandu-io/joaju.Server], and the bytes and
// handler that put it on the wire.
//
// The protocol only exists if something on the other end speaks it, and nothing
// in a browser speaks this one. The usual browser half of it arrives through
// npm, which this project does not have anywhere. So joaju.js is written here
// and served by the binary, the same way HTMX and Alpine already arrive. There
// is no package.json, no lockfile, no bundler and no build step: the file that
// is embedded is the file a browser runs.
//
// # Why this is a handler and not another route on the Server
//
// A joaju deployment answers nine routes and this is not the tenth. The reason
// is the CSP rather than the route table: the pages this
// client runs on are served under script-src 'self', so the script has to come
// from the ORIGIN THE PAGE IS ON.
// A joaju server is frequently not that origin -- cmd/joaju is a process of its
// own, at wss://socket.example.com, and a <script src> pointing there is refused
// by the browser before a socket is ever attempted.
//
// A route on the Server would therefore work in the deployment where joaju is
// mounted inside the application and be forbidden in the deployment where it is
// not, which is the same URL meaning two things -- with the broken one
// failing only in the deployment that has a separate socket server, i.e. in
// production. So the application mounts it, on its own origin:
//
//	mux.HandleFunc(client.Path, client.Handler)
//
// An Arandu application does not even do that: it already has one asset table
// with one content-addressed URL scheme, and this is a file for it:
//
//	view.RegisterAsset(client.Name, client.ContentType, client.Script())
//
// Both reach the same bytes. [Handler] exists for everything that is not an
// Arandu application -- a standalone Go server, a test -- because a package that
// could only be used from the framework would make the framework a dependency of
// speaking the protocol.
package client

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"
)

// script is the client, embedded rather than read from disk: a file the binary
// looks for at runtime is a deployment step, and the promise is one binary.
//
//go:embed joaju.js
var script []byte

const (
	// Name is the file name the script is served under, and the name to register
	// it with.
	Name = "joaju.js"

	// ContentType is what the script is served as. The charset is explicit
	// because a script served without one is decoded by rules that differ
	// between browsers, and this file has no non-ASCII bytes to be wrong about
	// only until somebody adds one.
	ContentType = "application/javascript; charset=utf-8"

	// Path is where [Handler] expects to be mounted: the prefix, with the
	// trailing slash that makes it a subtree pattern for http.ServeMux.
	//
	// It is under the same _arandu prefix the framework's assets use, so an
	// application has one reserved namespace and not two, and it is a
	// DIFFERENT leaf under it -- the framework serves _arandu/assets/ from its
	// own table, and two handlers on one prefix would be a request answered by
	// whichever was registered second.
	Path = "/_arandu/joaju/"

	// immutable is the caching a content-addressed URL earns: the bytes cannot
	// change without the hash changing, so there is nothing to revalidate.
	immutable = "public, max-age=31536000, immutable"

	// revalidate is what a URL that does NOT carry the current hash gets. It is
	// served, because a stale reference should degrade into a slow page rather
	// than a broken one, and it is served uncached, because it is a URL whose
	// contents genuinely can change under it.
	revalidate = "no-cache"
)

// Script is the bytes of the client.
//
// It answers a copy, which costs one allocation at the one call site an
// application has. The alternative is exporting the embedded slice, and a caller
// that wrote into it would leave the served bytes disagreeing with [Hash] -- at
// a URL that every browser was told to cache for a year.
func Script() []byte { return slices.Clone(script) }

// Hash is the path segment the script is served under: the first twelve hex
// characters of the SHA-256 of its bytes.
//
// It is the scheme hesape/view.AssetHash uses, computed here rather than
// imported, and that is deliberate. Importing it would put the framework's view
// runtime -- a template engine, a component registry, a render pipeline -- into
// the dependency graph of a WebSocket server, to reuse three lines. The same
// three lines are already duplicated once on purpose, in `aru`, for the same
// reason and with the same note: this is a contract across a repository
// boundary, and what keeps the two in step is that both have a name and a test.
//
// The hash is what makes the URL safe to cache forever: changing the script
// changes the URL, so no browser serves a stale one and nobody has to remember
// to bump a version.
func Hash() string {
	sum := sha256.Sum256(script)

	return hex.EncodeToString(sum[:])[:12]
}

// URL is the path the script is served at: /_arandu/joaju/<hash>/joaju.js
//
// It is what a <script src> names. Absolute, because a relative reference
// inherits the hash of whatever document it sits in, and [Handler] serves a URL
// carrying the wrong hash without caching -- so a relative one would be
// re-downloaded on every page view, silently.
func URL() string { return Path + Hash() + "/" + Name }

// Handler serves the client.
//
// It reads no file and knows no directory: the response body is the embedded
// slice, and the request path is only ever compared against two strings. That is
// why there is no path traversal to defend against here rather than a defence
// against it -- there is nothing under a path for a request to walk to.
//
// A path carrying the current hash is immutable and cached for a year; any other
// path that still names the script is served uncached, so a page holding a URL
// from the previous build is slow rather than broken. Anything else is 404.
//
// It is a func of this shape, and not an http.Handler value, so that it reads
// the same as hesape/view.Handler at the call site that mounts it -- there is one
// way an Arandu repository serves embedded bytes:
//
//	mux.HandleFunc(client.Path, client.Handler)
func Handler(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, Path)
	hash, name, ok := strings.Cut(rest, "/")
	if !ok || name != Name {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", ContentType)
	if hash == Hash() {
		w.Header().Set("Cache-Control", immutable)
	} else {
		w.Header().Set("Cache-Control", revalidate)
	}
	// The content type is exact and the body is a script, so a browser has no
	// reason to sniff -- and every reason not to be allowed to, since sniffing is
	// how a response gets treated as a type its author did not choose.
	w.Header().Set("X-Content-Type-Options", "nosniff")

	_, _ = w.Write(script)
}
