package client_test

import (
	"bytes"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arandu-io/joaju/client"
)

// serve is one request through the handler, called directly rather than through
// a mux.
//
// Directly on purpose: http.ServeMux cleans a path before it dispatches, so a
// request routed through one never carries "..". The traversal test below is
// about what the handler does with a path nobody cleaned, which is the situation
// any other router may hand it.
func serve(t *testing.T, path string) *http.Response {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "http://example.test/placeholder", nil)
	request.URL.Path = path
	client.Handler(recorder, request)

	return recorder.Result()
}

func TestTheHandlerServesTheScript(t *testing.T) {
	response := serve(t, client.URL())
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", client.URL(), response.StatusCode)
	}
	if got := response.Header.Get("Content-Type"); got != client.ContentType {
		t.Errorf("Content-Type is %q, want %q", got, client.ContentType)
	}
	if got := response.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options is %q: a script a browser may sniff is a script it may treat as something else", got)
	}

	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(response.Body); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body.Bytes(), client.Script()) {
		t.Error("the bytes served are not the bytes embedded")
	}
	if body.Len() == 0 {
		t.Fatal("the script is empty, so every test in this file is proving nothing")
	}
}

// The cache header has to be honest, because the immutable one is a promise for
// a year: a URL served with it and then changed underneath is a page that cannot
// be fixed by deploying.
func TestTheCacheHeaderIsOnlyImmutableForTheContentAddressedURL(t *testing.T) {
	current := serve(t, client.URL())
	defer func() { _ = current.Body.Close() }()

	if got := current.Header.Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Errorf("the URL carrying the current hash is served %q, want it cached forever", got)
	}

	stale := serve(t, client.Path+"000000000000/"+client.Name)
	defer func() { _ = stale.Body.Close() }()

	if stale.StatusCode != http.StatusOK {
		t.Errorf("a URL from a previous build answered %d: a stale reference should be slow, not broken", stale.StatusCode)
	}
	if got := stale.Header.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("a URL carrying the wrong hash is served %q, want no-cache", got)
	}
}

// The handler serves one file and reaches no directory, so there is nothing for
// a path to walk to. This is the test that says so rather than the comment.
func TestTheHandlerServesNothingButTheScript(t *testing.T) {
	forbidden := []string{
		client.Path + "hash/../../../../etc/passwd",
		client.Path + "hash/client.go",
		client.Path + "hash/joaju.js/../client.go",
		client.Path + "../client.go",
		client.Path + "hash",
		client.Path,
		"/etc/passwd",
		"/",
	}

	for _, path := range forbidden {
		response := serve(t, path)
		body := new(bytes.Buffer)
		if _, err := body.ReadFrom(response.Body); err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()

		if response.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, response.StatusCode)
		}
		if bytes.Contains(body.Bytes(), []byte("package client")) {
			t.Errorf("GET %s answered with this package's source", path)
		}
	}
}

// Script answers a copy, and this is why: the served bytes and the hash in the
// URL must not be able to disagree, and a caller holding the embedded slice
// could make them.
func TestScriptIsHandedOutAsACopy(t *testing.T) {
	before := client.Hash()

	scribbled := client.Script()
	for i := range scribbled {
		scribbled[i] = 'x'
	}

	if client.Hash() != before {
		t.Fatal("writing into the slice Script() answered changed the hash of what is served")
	}
	if bytes.Equal(client.Script(), scribbled) {
		t.Fatal("Script() answered the embedded slice itself")
	}
}

// The URL carries the hash of the bytes, so a change to the script is a change to
// the URL and no browser is left holding a version that was cached forever.
func TestTheURLCarriesTheHashOfWhatIsServed(t *testing.T) {
	url := client.URL()

	if !strings.HasPrefix(url, client.Path) {
		t.Errorf("URL() = %q, which is not under Path %q", url, client.Path)
	}
	if !strings.HasSuffix(url, "/"+client.Name) {
		t.Errorf("URL() = %q, which does not end in the file name", url)
	}
	if !strings.Contains(url, client.Hash()) {
		t.Errorf("URL() = %q, which does not carry the hash %q", url, client.Hash())
	}
	if len(client.Hash()) != 12 {
		t.Errorf("the hash is %d characters, want 12 -- the scheme is hesape/view.AssetHash's", len(client.Hash()))
	}
}

// RULE 13, checked rather than promised, and this package is where it is most
// likely to be broken: it is the one that ships JavaScript.
//
// A project runs with `git clone && aru dev`. The moment a package.json appears
// next to a .js file, somebody has a reason to run npm install, and the reason
// will look good.
func TestNoNodeAnywhereInThisRepository(t *testing.T) {
	forbidden := []string{
		"package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml",
		"bun.lockb", "bun.lock", "node_modules", "vite.config.js", "vite.config.ts",
		"rollup.config.js", "webpack.config.js", "tsconfig.json",
	}

	// ".." is the whole repository, because the promise is about the repository
	// and not about this package.
	err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if strings.Contains(path, "/.git/") {
			return nil
		}
		for _, name := range forbidden {
			if entry.Name() == name {
				t.Errorf("%s exists: the promise is that a project runs without Node", path)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The CSP the pages this runs on are served under is script-src 'self', and it
// has no unsafe-eval and no unsafe-inline. A script that needed either would be a
// script that could only be shipped by loosening the policy -- which is paying in
// security for a convenience, and is exactly what embedding the file was supposed
// to avoid.
//
// The check is a substring search, which is not a parser and does not pretend to
// be: it catches the honest version, which is the one that actually happens.
func TestTheScriptNeedsNoUnsafeContentSecurityPolicy(t *testing.T) {
	source := string(client.Script())

	for _, banned := range []string{
		"eval(",
		"new Function",
		"document.write",
		".innerHTML",
		".outerHTML",
		"insertAdjacentHTML",
		"setTimeout('",
		"setTimeout(\"",
		"import(",
		"//unpkg.com",
		"//cdn.",
		"http://",
		"https://",
	} {
		if strings.Contains(source, banned) {
			t.Errorf("the script contains %q, which needs a CSP this project does not serve, or a host it does not talk to", banned)
		}
	}
}

// The script is what a browser runs, so the syntax has to be a browser's. The
// parse is free and it is the one failure that would otherwise reach a page.
func TestTheScriptIsSyntacticallyValid(t *testing.T) {
	runtime := findJSRuntime(t)

	// Loading it is the parse, and the file assigns a global and starts nothing,
	// so loading it is also all it does.
	check := []byte("if (typeof globalThis.Joaju !== 'function') { throw new Error('the script defined no Joaju'); }\n")

	if out, err := runtime.run(t, nil, client.Script(), check); err != nil {
		t.Fatalf("the script does not load: %v\n%s", err, out)
	}
}
