#!/usr/bin/env bash
#
# The four checks of ADR-0075, over what git tracks, in the order of importance.
#
# It runs from anywhere and answers for the whole repository, both modules
# included. Exit 0 is a pass; anything else names what is wrong.
#
#   bash tests/test-layout-guard.sh
#
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 2

fail=0

# report prints a verdict and remembers it.
report() {
    printf '[FAILED] %s\n' "$1"
    fail=1
}

# built asks whether the go tool compiles a path at all.
#
# It does not build a directory whose name starts with _ or . , and it does not
# build testdata/. Nothing under those is a package, so no rule about where a
# test lives can apply to it -- ws/_rewritten/ is parked there on purpose, whole,
# and the nightly fuzz discovery skips the same paths for the same reason.
built() {
    case "/$1" in
        */_*/* | */.*/* | */testdata/*) return 1 ;;
    esac
    return 0
}

# moduleOf answers the directory of the go.mod that owns a path, or "." for the
# root module.
moduleOf() {
    local dir
    dir=$(dirname "$1")
    while [ "$dir" != "." ] && [ "$dir" != "/" ]; do
        if [ -f "$dir/go.mod" ]; then
            printf '%s\n' "$dir"
            return
        fi
        dir=$(dirname "$dir")
    done
    printf '.\n'
}

# The three listings every check reads, taken once.
#
# Each one is required to be non-empty, and that is not defensiveness. Every
# check below is of the form "no file in this listing is wrong", and that
# sentence is TRUE of a listing with nothing in it: pointed at an empty tree, or
# run where git tracks nothing, the guard passes having verified nothing. It is
# the same defect as reading no status from go list, one layer up -- a
# measurement that did not measure is not an approval.
goFiles=$(git ls-files '*.go')
testFiles=$(git ls-files '*_test.go')
modFiles=$(git ls-files | grep -E '(^|/)go\.mod$' || true)

[ -n "$goFiles" ] || report "git tracks no .go file here, so checks 1 and 3 verified nothing"
[ -n "$testFiles" ] || report "git tracks no _test.go file here, so check 2 verified nothing"
[ -n "$modFiles" ] || report "git tracks no go.mod here, so check 4 verified nothing"

# 1. A file named *Test.go is production code. `go test` runs nothing inside it,
#    and it says so with no error and no warning: the suite goes quiet and the
#    integration stays green. It is the first check because it is the only one
#    whose failure is invisible everywhere else.
#
#    The pattern takes no character class before Test, and that is deliberate.
#    [A-Za-z]Test\.go$ catches ButtonTest.go and misses view_Test.go, because the
#    character before Test is _ and _ is not in the class -- and view_Test.go is
#    the same silence for the same reason. Ending in Test.go is the whole of the
#    question; a lower-case _test.go does not match it.
offenders=$(printf '%s\n' "$goFiles" | grep -E 'Test\.go$' || true)
if [ -n "$offenders" ]; then
    printf '%s\n' "$offenders"
    report "*Test.go is not run by go test"
fi

# 2. A test outside tests/ has to say so in its name.
#
#    "Outside tests/" is relative to the go.mod that owns the file and not to
#    this directory. redis/ is a module of its own, so redis/tests/Unit is inside
#    its own tree; anchoring the pattern on the repository root would let every
#    test in every submodule through. Anchoring it on nothing -- matching tests/
#    at any depth -- opens the other hole, and app/Services/tests/ walks in.
#
#    One name is exempt, and it is a fact about the go tool rather than a
#    preference: go doc and pkg.go.dev render Example functions only from the
#    directory of the package they document, so moving example_test.go deletes
#    published documentation instead of relocating a test.
offenders=""
while IFS= read -r file; do
    # An empty listing still feeds one empty line through printf, and without
    # this the check reports a violation with no file name on it.
    [ -n "$file" ] || continue
    built "$file" || continue
    [ "${file##*/}" = "example_test.go" ] && continue

    module=$(moduleOf "$file")
    relative=$file
    [ "$module" != "." ] && relative=${file#"$module"/}

    case "$relative" in
        tests/*) continue ;;
    esac
    case "$file" in
        *_internal_test.go) continue ;;
    esac

    offenders+="$file"$'\n'
done < <(printf '%s\n' "$testFiles")
if [ -n "$offenders" ]; then
    printf '%s' "$offenders"
    report "test outside tests/ without the _internal suffix"
fi

# 3. A capitalized package clause is not idiomatic Go. The directories are
#    capitalized on purpose -- macOS is case-insensitive and tests/Unit is the
#    only spelling there can be -- and the identifiers are not.
offenders=$(printf '%s\n' "$goFiles" | xargs grep -ln '^package [A-Z]' 2>/dev/null || true)
if [ -n "$offenders" ]; then
    printf '%s\n' "$offenders"
    report "capitalized package clause"
fi

# 4. tests/Helpers is scaffolding. A production binary that imports it ships it,
#    which is a defect rather than organization.
#
#    The question goes to the production packages of each module, one module at a
#    time. `go list -deps ./...` cannot answer it: ./... matches tests/Helpers
#    itself, so the package reports its own existence as a violation and the
#    check fails on a tree that is correct.
#
#    And a go list that did not run is a failure to measure, not a pass. Sending
#    its diagnostics to /dev/null and reading no status is how a module that does
#    not build becomes a green check.
while IFS= read -r modfile; do
    [ -n "$modfile" ] || continue
    dir=$(dirname "$modfile")

    if ! packages=$(cd "$dir" && GOWORK=off go list ./... 2>&1); then
        printf '%s\n' "$packages"
        report "go list failed in $dir/: the reach of tests/Helpers was not measured"
        continue
    fi

    production=$(printf '%s\n' "$packages" | grep -vE '/tests(/|$)' || true)
    [ -z "$production" ] && continue

    # The word splitting is the point: an import path never carries a space, and
    # go list wants one argument per package.
    # shellcheck disable=SC2086
    if ! deps=$(cd "$dir" && GOWORK=off go list -deps $production 2>&1); then
        printf '%s\n' "$deps"
        report "go list -deps failed in $dir/: the reach of tests/Helpers was not measured"
        continue
    fi

    offenders=$(printf '%s\n' "$deps" | grep -E '/tests/Helpers(/|$)' || true)
    if [ -n "$offenders" ]; then
        printf '%s\n' "$offenders"
        report "tests/Helpers reached by a production package of $dir/"
    fi
done < <(printf '%s\n' "$modFiles")

exit $fail
