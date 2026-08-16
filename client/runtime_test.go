package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arandu-io/joaju/client"
)

// A JavaScript runtime is a TEST dependency and never a product one.
//
// The client is served by the binary and runs in a browser, so nothing about
// shipping it involves Node. But a client nobody ever ran is a client nobody
// knows works: the file is the half of the protocol this repository cannot
// exercise from Go, and a handler test proves only that the bytes were served.
//
// It is the same line the Autobahn suite is already on in ws/internal/autobahn --
// that one runs in a container with Python in it, and there is no Python in the
// product either.
//
// Everything that needs one skips when there is none, so `go test ./...` on a
// machine with no runtime installed passes and says which tests did not run.

// jsRuntime is a JavaScript runtime that can run one file, or a skip.
type jsRuntime struct {
	name string
	path string
	// argv is what goes before the script name. Node takes it bare; the others
	// have a subcommand, and deno additionally has to be told that a script may
	// open a socket and read an environment variable.
	argv []string
}

// findJSRuntime is the three runtimes this project checks for, in the order it
// prefers them.
//
// Node first because it is the one a developer is most likely to have and the
// one CI would install. All three are only ever asked to parse and run one
// self-contained file: the scripts below use no module system and no package, so
// nothing here depends on how a given runtime resolves an import.
func findJSRuntime(t *testing.T) jsRuntime {
	t.Helper()

	for _, candidate := range []jsRuntime{
		{name: "node"},
		{name: "bun", argv: []string{"run"}},
		{name: "deno", argv: []string{"run", "--allow-net", "--allow-env"}},
	} {
		path, err := exec.LookPath(candidate.name)
		if err != nil {
			continue
		}
		candidate.path = path

		return candidate
	}

	t.Skip("no JavaScript runtime is installed (node, deno or bun): the client was not run, only served")

	return jsRuntime{}
}

// run executes one script and answers what it wrote to standard output.
//
// The parts are concatenated into a single file rather than required from one
// another, and that is what keeps all three runtimes interchangeable: there is
// no module system involved, so nothing depends on whether a given runtime reads
// a .js file as CommonJS or as an ES module -- a question that has a different
// answer in each of them and that a package.json would be the usual way to
// settle. There is no package.json in this repository and there is not going to
// be one.
func (r jsRuntime) run(t *testing.T, environment []string, parts ...[]byte) (string, error) {
	t.Helper()

	directory := t.TempDir()
	script := filepath.Join(directory, "run.js")
	if err := os.WriteFile(script, bytes.Join(parts, []byte("\n;\n")), 0o600); err != nil {
		t.Fatal(err)
	}

	// A scenario that hangs is a failure and not a suite that never ends. The
	// scripts carry a guard of their own at twenty seconds, so reaching this one
	// means the runtime itself is stuck.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, r.path, append(append([]string{}, r.argv...), script)...)
	command.Dir = directory
	command.Env = append(os.Environ(), environment...)

	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	if err := command.Run(); err != nil {
		return stdout.String() + "\n" + stderr.String(), err
	}

	return stdout.String(), nil
}

// scenario reads one of the scripts in testdata.
func scenario(t *testing.T, name string) []byte {
	t.Helper()

	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}

	return body
}

// runScenario runs the client, the shared harness and one scenario against a
// live server, and decodes the JSON object the scenario printed.
//
// The bytes it runs are [client.Script], not the file on disk: what is embedded
// is what a browser receives, and a test of the file beside it would pass over an
// embed directive that had stopped matching.
func runScenario(t *testing.T, name string, environment []string) map[string]any {
	t.Helper()

	runtime := findJSRuntime(t)
	out, err := runtime.run(t, environment,
		client.Script(),
		scenario(t, "harness.js"),
		scenario(t, name),
	)
	if err != nil {
		t.Fatalf("%s did not finish: %v\n%s", name, err, out)
	}

	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("%s printed something that is not its result: %v\n%s", name, err, out)
	}

	return result
}
