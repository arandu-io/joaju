---
name: joaju-transport
description: Change anything under joaju's ws subpackage — this project's own RFC 6455. Use when the request touches frames, opcodes, masking, fragmentation, the opening handshake, Sec-WebSocket-Accept, close codes, the closing handshake, UTF-8 validation of text frames, read and write deadlines, buffer sizes, or permessage-deflate. Use when the request mentions "websocket protocol", "RFC 6455", "frame", "close code 1002", "1006", "handshake fails", "Autobahn", "conformance", "fuzz", "corpus", "the ws package", or "just import gorilla/websocket" — importing a websocket library, or any third-party module, into the root module is what CI exists to refuse. Covers the dependency guard, the borrowed-code rule, the fuzz corpus, and how the conformance report is regenerated, because the one committed is marked stale.
license: MIT
---

# Changing the transport

`ws/` is this project's own implementation of RFC 6455. It is not a fork, it
borrows no code, and it is why the dependency graph has one entry in it. Six
production files:

```sh
ls ws/*.go | grep -v _test    # client.go conn.go frame.go handshake.go server.go utf8.go
```

## Before you write anything

**The library you are about to import is the thing this package exists instead
of.** The root `go.mod` has one `require` line:

```sh
grep -c '^require' go.mod   # 1
go list -m all              # this module, hesape, and golang.org/x/crypto under it
```

CI fails on a second, and the check is worth running before a commit rather than
reading about after one:

```sh
export GOWORK=off
go list -deps -f '{{if .Module}}{{.Module.Path}}{{end}}' ./... \
  | sort -u | grep -vE '^github\.com/arandu-io/|^golang\.org/x/'
```

Empty output is the pass. Anything printed is a module in the graph that should
not be there.

The trade being protected is small and measurable. What the rest of the
repository reaches for out of `ws` is twelve names of the package and seven
methods of `Conn`:

```sh
grep -rhoE '\bws\.[A-Za-z_][A-Za-z0-9_]*' server.go tests/Feature/*.go \
  tests/Specification/echo/main.go | sort -u | wc -l      # 12
```

`server.go` alone uses nine of those twelve, plus `Close`, `ReadMessage`,
`SetPongHandler`, `SetReadDeadline`, `SetReadLimit`, `SetWriteDeadline` and
`WriteMessage`. A library for that surface brings a client stack, a proxy
dialer, a buffer pool and permessage-deflate along with it — and a stream of
security advisories to track forever.

**Borrowed code cannot be signed off.** A contributor states they wrote the
change or have the right to submit it, and `ws/` is where that matters most. CI
runs the cheap version of the check — copied code arrives carrying its licence
or its attribution file:

```sh
find ws \( -name 'LICENSE*' -o -name 'THIRD_PARTY*' -o -name 'NOTICE*' \)
```

It will not catch a careful paste. It catches the honest one, which is the one
that happens.

## The procedure

**1. Read the section of the RFC first.** The specification is the authority
here, not another implementation. The files say which section they answer:
opcodes and lengths are section 5.2, close codes are 7.4.1, the accept key is
1.3, the text-frame UTF-8 requirement is 5.6.

**2. Write the change and its test beside it.** Everything in `ws/` is
`*_internal_test.go`, because the package has no external form worth testing
against — the fields being exercised are unexported. Name the test as a sentence
about behaviour: `TestReadFrameRefusesWhatSectionFiveForbids`,
`TestFormatCloseTruncatesAtARuneBoundary`,
`TestUTF8ValidatorLatchesOnceInvalid`.

**3. Run the gates.**

```sh
export GOWORK=off
gofmt -l $(find . -name '*.go' -not -path '*/testdata/*' -not -name '*.kyse.go')
go build ./...
go vet ./...
go test -race ./...
```

The race detector matters more here than anywhere else in this repository: one
connection is one goroutine, a channel is shared by every goroutine subscribed
to it, and a broadcast walks the subscriber set while sockets open and close
underneath it.

**4. Run the fuzz targets that cover what you touched**, at least briefly. There
are seven in `ws/`:

```sh
grep -rho 'func Fuzz[A-Za-z0-9_]*' ws/*_test.go | sort -u    # 7
go test ./ws -run='^$' -fuzz='^FuzzReadFrame$' -fuzztime=30s
```

The seed corpus and every crasher under `ws/testdata/fuzz/` already run as
ordinary subtests in `go test`, so a regression is caught on every push for
free. What a long run buys is the paths a minute does not reach. A crasher
belongs in `testdata/fuzz`, committed by a person who has read it.

**5. Regenerate the conformance report.** See below — this step is not optional
when `ws/` changed, and the report on disk currently describes code that is no
longer here.

## The conformance report is stale, and says so

`tests/Specification/REPORT.txt` opens with the word `STALE`. It was produced
against the transport that this one replaced, so every number in it is about
code that is not in the tree. A figure that describes code no longer here is
worse than no figure — do not quote it, and do not copy a pass rate out of it
into a README, a commit message or a comment.

```sh
./tests/Specification/run.sh
```

That regenerates it. What it does: builds the echo server, starts it on
`:9001`, drives the official Autobahn fuzzing client at it from a container,
collects the reports and exits non-zero if any case came back as anything but
`OK` or `NON-STRICT`.

- It needs Docker. Nothing else in the repository does; `go build ./...` and
  `go test ./...` neither run it nor need it.
- Autobahn 18, every case except `12.*` and `13.*`. Those are permessage-deflate
  — the transport implements the extension and the echo server does not
  negotiate it, so the two groups would measure nothing.
- The case count is checked rather than trusted: `expected_cases=301` at
  `tests/Specification/run.sh:144`. The suite exits zero when it gives up on a
  connection halfway through, and every case it did reach can be green, so a
  truncated run is otherwise indistinguishable from a clean one.
- `AUTOBAHN_TARGET` is the one part that is not the same everywhere.
  `host.docker.internal` is right on Docker Desktop, colima and Rancher Desktop;
  a plain Linux daemon needs the host's bridge address.
- The image is amd64. On an arm64 machine it runs under emulation, and group 9
  pushes 16 MiB per case, so expect minutes rather than seconds.

CI runs it, but only when `ws/**` or `tests/Specification/**` changed — the
suite is too slow to make every pull request wait for it.

## What the transport does not have, and why each is absent

- **No `CheckOrigin` field on `Upgrader`.** The same-origin check always runs
  and there is no configuration that widens it. Cross-origin sockets are not
  available; when they are, one function changes rather than a list in a config
  file. Refusing an origin beyond that is the `ConnectPolicy`'s, which can only
  narrow.
- **No `Subprotocols`.** The Pusher protocol carries its version in the query
  string, so `Sec-WebSocket-Protocol` has nothing to negotiate. A field that is
  always empty is a field that gets wired wrong the first time somebody needs
  it.
- **No compression.** permessage-deflate is a second framing path and a
  CRIME-shaped hazard where a payload mixes attacker-controlled text with a
  session identifier.
- **No client stack beyond `Dialer`.** `DefaultDialer` and `Dialer.Dial` exist
  for tests and for one instance talking to another. No proxy support, no
  redirect following, no cookie jar. A library carries those because it does not
  know who is calling; this one does.

## The two invariants a change here breaks most easily

**Concurrency.** One goroutine reads and any number write. `Conn.ReadMessage` is
not safe from two goroutines — it carries the state of a fragmented message and
of the UTF-8 check across calls — and every write path takes a mutex, because
two frames interleaved on one socket is a protocol violation the peer cannot
recover from. The asymmetry is what the read loop needs: answering a ping is a
write, and it happens on the reading goroutine while another may be
broadcasting.

**Nothing goes out after the close frame.** Section 5.5.1 is explicit, and
`ErrCloseSent` is returned rather than the write going out anyway. That is what
keeps a shutdown from racing a broadcast into a frame the peer must reject.

## When the surface changes

Adding an exported symbol to `ws` is fine; the counts above are measurements,
not budgets. What is not fine is leaving a written count behind that no longer
matches. Three places state this number today — the comment above the `require`
line in `go.mod`, the "# The transport" section of `doc.go`, and the README —
and they do not agree. `go.mod` says nineteen and decomposes it as twelve of the
package plus seven methods of `Conn`; the README says nineteen; `doc.go` says
ten. The measurement above says twelve plus seven, so `doc.go` is the one that
is wrong. Re-run the `grep` before trusting any of them, and fix all three
together when the surface moves. A number that describes code nobody measured is
worse than no number.
