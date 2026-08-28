#!/usr/bin/env bash
#
# build-tags.sh answers about the build tags of the module in the current
# directory, so that no suite hides behind one.
#
# The failure it exists for was measured and not guessed. A directory whose
# every .go file sits behind a build tag is invisible to the default gate: with
# go1.26.6, on a module with a normal package beside it, both `go build ./...`
# and `go vet ./...` exit 0 and print nothing about it. A directory holding only
# _test.go files is not the same case -- vet does refuse that one -- which is
# why this looked covered. redis/tests/Integration is the first kind, and it
# stopped compiling with three gates in a row calling the repository clean.
#
# The obvious repair is to pass the tags, and the obvious way to pass them is to
# write them down. That is the same bug one step later: a tag added tomorrow is
# a suite nobody vets, and nothing announces it either. So they are read off the
# //go:build lines instead of listed here.
#
# Two modes:
#
#	list	print this module's custom tags, space separated, for -tags
#	check	fail unless every one of them reaches a file
#
# Run it from the directory holding the go.mod under test. The module boundary
# is the point: `./...` stops at a nested go.mod, so the root module and redis/
# have to be asked separately.

set -euo pipefail

# kyse is excluded by name, and it is the only exclusion by name.
#
# A .kyse.go file is a view template. It is Go-shaped exactly as far as the
# //go:build line at the top and no further -- the body holds @extends(...),
# which is not Go. Turning its tag on hands those files to the compiler, which
# answers "illegal character U+0040 '@'" and fails a gate that was checking
# something else entirely.
#
# There is no .kyse.go in this repository and there should not be one. The
# exclusion is here so that the day one arrives it does not take this gate down
# with it -- which is the day nobody would connect the two.
readonly EXCLUDED_TAG=kyse

# files_of_this_module is every .go file this module owns.
#
# It stops at a nested go.mod, which is where `./...` stops too, and the two have
# to agree: read from the repository root without this, redis/tests/Integration's
# tag is discovered and then reported unreachable, because the root module cannot
# compile a file belonging to another module. That is a true sentence about the
# wrong module.
files_of_this_module() {
	local nested exclude

	nested=$(find . -mindepth 2 -name go.mod -exec dirname {} \;)
	if [ -z "$nested" ]; then
		find . -name '*.go' -not -path './.git/*'

		return
	fi

	# One anchored pattern per nested module: "./redis" becomes "^\./redis/", so
	# a directory of that name somewhere else in the tree is not swept up with it.
	exclude=$(printf '%s\n' "$nested" | sed -e 's|[].[^$*\\]|\\&|g' -e 's|^|^|' -e 's|$|/|')

	find . -name '*.go' -not -path './.git/*' | grep -vE -f <(printf '%s\n' "$exclude") || true
}

# tags_in_tree is every identifier appearing in a //go:build line of this module.
#
# The constraint grammar has &&, ||, ! and parentheses. None of them is a tag,
# so pulling the identifiers out is enough and parsing the expression is not
# needed: a tag named anywhere in a constraint is a tag some file is behind, and
# the whole question here is which files the default build never sees.
tags_in_tree() {
	local files

	# Held in a variable and checked, rather than piped straight into xargs: with
	# nothing on its input, xargs still runs sed once, and a sed with no file
	# argument reads stdin -- which is a gate that hangs instead of failing.
	files=$(files_of_this_module)
	[ -n "$files" ] || return 0

	printf '%s\n' "$files" |
		tr '\n' '\0' |
		xargs -0 sed -n 's|^//go:build ||p' |
		grep -oE '[A-Za-z_][A-Za-z0-9_.]*' |
		sort -u
}

# predeclared is what Go itself defines, which is not this project's to discover.
#
# The GOOS and GOARCH names come from the toolchain rather than a list kept here,
# because a list of them is a list that is wrong the next time one is added. The
# rest are the constraints the toolchain sets on its own. A file behind `unix` or
# `go1.26` is a portability constraint and not a suite hiding.
predeclared() {
	go tool dist list | tr '/' '\n'
	printf '%s\n' ignore cgo unix race msan asan boringcrypto purego gc gccgo
}

# custom_tags is what is left: the tags this project invented.
custom_tags() {
	local known
	known=$(predeclared | sort -u)

	tags_in_tree |
		grep -vE '^go1(\.[0-9]+)*$' |
		grep -vxF "$EXCLUDED_TAG" |
		grep -vxF -f <(printf '%s\n' "$known") ||
		true
}

# inventory is what the module compiles under one set of tags: the packages, and
# the files in each.
#
# The files and not only the packages, because a tag does not have to own a whole
# directory. One that adds a single file to a package that already exists leaves
# the package list identical, and comparing only those would call it unreachable.
inventory() {
	go list -tags "$1" -f '{{.ImportPath}} {{.GoFiles}} {{.TestGoFiles}} {{.XTestGoFiles}}' ./... | sort
}

list() {
	custom_tags | tr '\n' ' ' | sed 's/ *$//'
}

check() {
	local tags base found failed=0

	tags=$(custom_tags)
	if [ -z "$tags" ]; then
		echo "no custom build tags in this module"

		return 0
	fi

	base=$(inventory '')

	while read -r tag; do
		[ -n "$tag" ] || continue

		if [ "$(inventory "$tag")" = "$base" ]; then
			echo "the build tag '$tag' reaches no file in this module"
			echo "  it is named in a //go:build line, so something is behind it and"
			echo "  nothing is: a renamed tag, a deleted directory, or a typo in the"
			echo "  one file that used it. Either fix the constraint or drop it."
			failed=1
		else
			found=$(comm -13 <(printf '%s\n' "$base") <(inventory "$tag") | cut -d' ' -f1 | sort -u | tr '\n' ' ')
			echo "the build tag '$tag' reaches: ${found% }"
		fi
	done <<<"$tags"

	return "$failed"
}

case "${1:-}" in
list) list ;;
check) check ;;
*)
	echo "usage: $0 list|check" >&2
	exit 2
	;;
esac
