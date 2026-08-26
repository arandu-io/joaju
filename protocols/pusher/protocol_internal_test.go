package pusher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
)

// pausingSubscriptionBroker stops one socket immediately before the channel
// records its subscription. It leaves every other Broker and Channel operation
// on the production in-memory implementations.
type pausingSubscriptionBroker struct {
	joaju.Broker

	socket      joaju.SocketID
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newPausingSubscriptionBroker(base joaju.Broker, socket joaju.SocketID) *pausingSubscriptionBroker {
	return &pausingSubscriptionBroker{
		Broker:  base,
		socket:  socket,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *pausingSubscriptionBroker) FindOrCreate(ctx context.Context, g auth.Grant, name joaju.ChannelName) (joaju.Channel, error) {
	held, err := b.Broker.FindOrCreate(ctx, g, name)
	if err != nil {
		return nil, err
	}

	return &pausingSubscriptionChannel{Channel: held, broker: b}, nil
}

func (b *pausingSubscriptionBroker) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

type pausingSubscriptionChannel struct {
	joaju.Channel
	broker *pausingSubscriptionBroker
}

func (c *pausingSubscriptionChannel) Subscribe(ctx context.Context, g auth.Grant, conn *joaju.Connection, member joaju.Member) error {
	if conn.ID() == c.broker.socket {
		c.broker.enteredOnce.Do(func() { close(c.broker.entered) })
		select {
		case <-c.broker.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return c.Channel.Subscribe(ctx, g, conn, member)
}

func TestConcurrentSubscriptionSurvivesTheLastSubscriberLeaving(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryBroker()
	first, _ := channelTestConnection(t, "acme", "first")
	next, _ := channelTestConnection(t, "acme", "next")
	broker := newPausingSubscriptionBroker(base, next.ID())
	t.Cleanup(broker.unblock)
	protocol := NewPusher(broker, channelTestJoinPolicy{}, PusherConfig{})

	subscribe := []byte(`{"event":"pusher:subscribe","data":{"channel":"orders.17"}}`)
	if err := protocol.Message(ctx, first, subscribe); err != nil {
		t.Fatalf("subscribing the first socket: %v", err)
	}

	subscribed := make(chan error, 1)
	go func() {
		subscribed <- protocol.Message(ctx, next, subscribe)
	}()

	select {
	case <-broker.entered:
	case <-time.After(time.Second):
		t.Fatal("the concurrent subscription did not reach the channel")
	}

	unsubscribe := []byte(`{"event":"pusher:unsubscribe","data":{"channel":"orders.17"}}`)
	if err := protocol.Message(ctx, first, unsubscribe); err != nil {
		t.Fatalf("unsubscribing the previous last socket: %v", err)
	}

	broker.unblock()
	if err := <-subscribed; err != nil {
		t.Fatalf("subscribing the replacement socket: %v", err)
	}

	name := channelTestName(t, "acme", "orders.17")
	held, err := base.Find(ctx, channelTestJoinGrant(t, "acme", "reader"), name)
	if errors.Is(err, joaju.ErrNoChannel) {
		t.Fatal("the replacement socket was subscribed to a channel the broker had already removed")
	}
	if err != nil {
		t.Fatalf("finding the channel after the concurrent subscriptions: %v", err)
	}
	if !held.Subscribed(next) {
		t.Fatal("the broker's channel does not hold the replacement socket")
	}
}
