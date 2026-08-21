// Package tests is the base the suites of this module build on.
//
// What belongs here is what more than one suite needs. A helper one suite uses
// belongs beside that suite, and most of them do: the fixtures that stand a
// server up are per suite, because what a Unit test wants standing is not what a
// Feature test wants standing.
//
// These three are here because the alternative is two copies of the same
// twenty lines, which is the second way of doing something that already had
// one -- and the way a channel name is minted is exactly the thing this project
// wants to have one of.
//
// The suites:
//
//	tests/Unit/           one thing, with nothing running
//	tests/Feature/        a whole feature, over a real socket
//	tests/E2E/            the JavaScript client, behind the e2e build tag
//	tests/Specification/  the Autobahn harness, driven by run.sh
package tests

import (
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/protocols/pusher"
)

// Tenant is the customer every channel in these suites belongs to.
//
// It is a string no channel name in them contains, so a test can assert that
// the tenant did not reach the wire by looking for it.
const Tenant = "acme"

// ChannelName is a channel of [Tenant], which is the only way there is to build
// one: the name goes through a Grant like everything else, because
// [joaju.NewChannelName] takes the tenant off one.
func ChannelName(t *testing.T, requested string) joaju.ChannelName {
	t.Helper()

	g := auth.SystemGrant(broadcasting.ChannelJoin, Tenant)
	name, err := joaju.NewChannelName(g, requested)
	if err != nil {
		t.Fatalf("NewChannelName(%q) = %v", requested, err)
	}

	return name
}

// Encode is a frame as the bytes a client reads, so that a test can compare what
// a socket was written against what the protocol would have written.
func Encode(t *testing.T, f pusher.Frame) string {
	t.Helper()

	b, err := pusher.Encode(f)
	if err != nil {
		t.Fatalf("Encode(%v) = %v", f, err)
	}

	return string(b)
}
