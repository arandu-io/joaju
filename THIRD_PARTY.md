# Third-party code

## gorilla/websocket

`websocket/` is a fork of [gorilla/websocket](https://github.com/gorilla/websocket),
BSD 2-Clause. The copyright notice is intact at the top of every file, and
`websocket/AUTHORS` is the upstream author list, unmodified.

A fork rather than a dependency, argued in
[ADR 0052](https://github.com/arandu-io/docs/blob/main/adr/0052-joaju-e-o-servidor-de-websocket.md).
What it buys is being able to change the core of connection handling; what it
costs is that every upstream security fix is ours to track. That work is real,
and it is why this is a repository of its own rather than something the
framework core carries (ADR 0004).

### What changed from upstream

**`proxy.go` no longer dials SOCKS5.** Upstream falls through to
`golang.org/x/net/proxy` for any scheme that is not HTTP or HTTPS, and that one
branch was the only third-party import this repository would have had. It now
refuses the scheme with a named error instead of ignoring it.

That is the first thing the fork was used for, and it is the shape of what it is
for: a websocket server dialling out through SOCKS5 is not something this
project does, and inheriting a dependency for it is not a decision anybody took.

### Formatting

`doc.go` and `server.go` were run through `gofmt`. Upstream still carries the
pre-Go-1.19 doc comment style -- a bare `Overview` heading and indented code
blocks, where the current tool writes `# Overview` and tab-indented blocks -- and
one trailing blank line. RULE 5 runs `gofmt -l` over everything, so the choice
was to format or to carve an exception into the project's own filter.

Formatting won, and the cost is named: a merge from upstream will conflict on
those two files. The conflict is whitespace and heading markers, which is the
cheapest kind to resolve, and the alternative was a rule with a hole in it.

### What did not come across

The upstream `_test.go` files, `examples/` and the CI configuration. Those tests
exercise upstream's own API; what decides whether a websocket implementation is
correct is the [Autobahn](https://github.com/crossbario/autobahn-testsuite)
conformance suite, which is also what `laravel/reverb` measures itself against.

### The licence

```
Copyright (c) 2023 The Gorilla Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
```
