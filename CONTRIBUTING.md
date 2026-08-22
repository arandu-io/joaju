# Contributing

## Sign your commits

Every commit needs a `Signed-off-by` line:

```
git commit -s -m "what changed and why"
```

That line is the [Developer Certificate of Origin](https://developercertificate.org/):
you are stating that you wrote the change, or that you have the right to submit
it under this project's license. It is not a copyright assignment — you keep
your copyright, and this project can never be relicensed behind your back.

We use DCO rather than a CLA on purpose. A CLA would let the project relicense
later, and the price is that every contributor has to sign a legal document
before their first patch.

Code borrowed from another project cannot be signed off, and the `ws` subpackage
is where that matters most: it is this project's own RFC 6455, written against
the specification. A pull request that pastes a websocket library into it is one
we cannot take, whatever the license says.

## Before you open a pull request

```
gofmt -l $(find . -name '*.go' -not -path '*/testdata/*' -not -name '*.kyse.go')   # no output
go vet -tags 'integration e2e' ./...
go test -race -tags 'integration e2e' ./...
bash tests/test-layout-guard.sh
```

The tags matter: without them the suites that need something running are not
compiled at all, and a suite nothing built is a suite nothing checked. They pull
in a RESP server for `redis/tests/Integration` and a JavaScript runtime for
`tests/E2E`; both skip and say so when there is none.

CI runs these, and a handful of checks besides; `.github/workflows/ci.yml` is
the list, and it is what decides. The one worth knowing before you write the
change guards the graph: nothing enters it but this project's own modules and
`golang.org/x`, the standard library's annex. That is not an accident of the
current feature set — the `ws` subpackage exists so the transport costs nothing
in the dependency graph. A pull request that adds a dependency needs to argue
for it first, in an issue.

## Where a test goes

One question comes before all the others, and it is technical: does the test
need an identifier the package does not export?

**Yes** — it stays beside the code, and the file name says so: `<name>_internal_test.go`.
A `package main` has no external form, so its tests are always these.

**No** — it goes in the mirrored tree, under the category that describes what it
does:

| directory | what runs there |
|---|---|
| `tests/Unit/` | one thing, fast, deterministic, nothing running |
| `tests/Feature/` | a whole feature, across layers |
| `tests/Integration/` | real components — a server, a cache, an adapter |
| `tests/E2E/` | the way a real client would use it |
| `tests/Fuzz/` | with the corpus beside the target |
| `tests/Specification/` | conformance against a specification that is not ours |

`Integration` and `E2E` sit behind the `integration` and `e2e` build tags, so a
fresh clone runs `go test ./...` and gets an answer without a server or a
JavaScript runtime having to exist first.

Two things the go tool imposes, and neither is style. The `package` clause is
always lower case, however the directory is spelled. And a file whose name does
not end in `_test.go` is production code: `go test` runs nothing inside a
`BrokerTest.go`, with no error and no warning.

`bash tests/test-layout-guard.sh` checks all of it, and the decision behind it is
ADR-0075.

## What the commit message says

What changed and why. The why is the part that is not in the diff, and it is the
part someone will need in two years.

No AI attribution of any kind: no `Co-Authored-By` for an assistant, no
"generated with" footer. Commits are authored by the people who submit them.

## Architecture decisions

The decisions this project has already made live in `arandu-io/docs`, and every
one that closed a door has an ADR. If your change contradicts one, say so in the
pull request and argue for the change of decision — that is a normal thing to
do, and it is better than a patch that quietly works around it.
