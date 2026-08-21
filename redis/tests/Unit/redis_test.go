package unit

import (
	"context"
	"testing"

	"github.com/arandu-io/hesape/redis/connections"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/redis"
)

// The four refusals of this package, and they are here rather than in the
// integration suite because none of them reaches a server: a refusal that needed
// one would be a refusal nobody checks on a laptop.
//
// What a refusal buys is the difference between an error at the call and a panic
// hours later in a subscription goroutine the relay started.

// unreachable is a bus over a connection to nothing.
//
// connections.Connect does not dial, so the address below is never asked for.
func unreachable(t *testing.T) joaju.Bus {
	t.Helper()

	b, err := redis.NewBus(connections.Connect(connections.Config{Address: "127.0.0.1:1"}))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	return b
}

// TestNewBusRefusesANilConnection: an instance with no Redis hands
// joaju.NewRelay a nil Bus, which is a deployment it supports and reports. A
// bus over no connection is the same intention expressed where it becomes a
// panic in a subscription goroutine instead.
func TestNewBusRefusesANilConnection(t *testing.T) {
	if _, err := redis.NewBus(nil); err == nil {
		t.Fatal("NewBus(nil) succeeded, and the first publish would have dereferenced it")
	}
}

// TestSubscribeRefusesAnEmptyChannelList: the driver accepts one, subscribes to
// nothing and blocks, which from the outside is indistinguishable from a
// channel nobody is talking on.
func TestSubscribeRefusesAnEmptyChannelList(t *testing.T) {
	if err := unreachable(t).Subscribe(context.Background(), nil, func(_, _ string) {}); err == nil {
		t.Fatal("Subscribe with no channels succeeded, and would have blocked forever")
	}
}

// TestSubscribeRefusesANilCallback: it panics on the first message, in a
// goroutine started by the relay, and the first message may be hours away.
func TestSubscribeRefusesANilCallback(t *testing.T) {
	topic := joaju.TopicPrefix + "acme:orders"
	if err := unreachable(t).Subscribe(context.Background(), []string{topic}, nil); err == nil {
		t.Fatal("Subscribe with no callback succeeded")
	}
}

// TestPublishRefusesAnEmptyChannel: Redis would take it, deliver it to nobody
// and answer zero, which is a message lost by a call that reported success.
func TestPublishRefusesAnEmptyChannel(t *testing.T) {
	if _, err := unreachable(t).Publish(context.Background(), "", "{}"); err == nil {
		t.Fatal("Publish with no channel succeeded")
	}
}
