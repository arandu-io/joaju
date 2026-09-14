package joaju

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

type broadcasterTestAuthorization struct{}

func (broadcasterTestAuthorization) Auth(context.Context, string) (auth.Grant, any, error) {
	return auth.Grant{}, "auth-result", nil
}

func (broadcasterTestAuthorization) ValidAuthenticationResponse(context.Context, auth.Grant, broadcasting.Channel, any) (any, error) {
	return "valid-result", nil
}

func (broadcasterTestAuthorization) Broadcast(context.Context, auth.Grant, []broadcasting.Channel, string, map[string]any) error {
	return nil
}

type broadcasterTestCarrier struct {
	*fleetTestBroker

	mu      sync.Mutex
	carried []Event
}

func (b *broadcasterTestCarrier) Carry(_ context.Context, event Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.carried = append(b.carried, event)
}

func (b *broadcasterTestCarrier) events() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Event(nil), b.carried...)
}

func TestHesapeBroadcasterDeliversAnActionGrantThroughJoaju(t *testing.T) {
	tenant := "acme"
	channel := &relayTestChannel{name: relayTestName(t, tenant, "orders")}
	carrier := &broadcasterTestCarrier{fleetTestBroker: newFleetTestBroker(channel)}
	broadcaster, err := NewHesapeBroadcaster(broadcasterTestAuthorization{}, carrier)
	if err != nil {
		t.Fatalf("NewHesapeBroadcaster() = %v", err)
	}
	payload := map[string]any{"invoice": float64(42), "socket": "123.456"}

	err = broadcaster.Broadcast(
		context.Background(),
		auth.SystemGrant("invoice.sent", tenant),
		[]broadcasting.Channel{broadcasting.NewChannel("orders")},
		"invoice.sent",
		payload,
	)
	if err != nil {
		t.Fatalf("Broadcast() = %v", err)
	}

	want := Event{
		Name:    "invoice.sent",
		Channel: channel.name,
		Data:    []byte(`{"invoice":42}`),
		Socket:  "123.456",
	}
	if got := channel.delivered(); !reflect.DeepEqual(got, []Event{want}) {
		t.Fatalf("local delivery = %#v, want %#v", got, []Event{want})
	}
	if got := carrier.events(); !reflect.DeepEqual(got, []Event{want}) {
		t.Fatalf("fleet delivery = %#v, want %#v", got, []Event{want})
	}
	if got := payload["socket"]; got != "123.456" {
		t.Fatalf("Broadcast mutated its caller's payload: socket = %#v", got)
	}
}

func TestHesapeBroadcasterRefusesAnUnscopedTenant(t *testing.T) {
	for _, tenant := range []string{"", "not:valid"} {
		t.Run(tenant, func(t *testing.T) {
			broadcaster, err := NewHesapeBroadcaster(
				broadcasterTestAuthorization{},
				newFleetTestBroker(),
			)
			if err != nil {
				t.Fatalf("NewHesapeBroadcaster() = %v", err)
			}

			err = broadcaster.Broadcast(
				context.Background(),
				auth.SystemGrant("invoice.sent", tenant),
				[]broadcasting.Channel{broadcasting.NewChannel("orders")},
				"invoice.sent",
				nil,
			)
			if err == nil {
				t.Fatal("Broadcast() accepted an event without a valid tenant")
			}
		})
	}
}
