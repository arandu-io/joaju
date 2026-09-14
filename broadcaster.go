package joaju

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// HesapeBroadcaster adapts Hesape broadcasting to the channels Joaju already
// owns. Authentication remains with the configured Hesape broadcaster; only
// its delivery is replaced, so publishing cannot create a second socket system.
type HesapeBroadcaster struct {
	authorization broadcasting.Broadcaster
	broker        Broker
}

// NewHesapeBroadcaster builds a Hesape driver whose events are delivered by
// broker. authorization supplies the existing channel-authentication response.
func NewHesapeBroadcaster(authorization broadcasting.Broadcaster, broker Broker) (*HesapeBroadcaster, error) {
	if authorization == nil {
		return nil, errors.New("joaju: a Hesape broadcaster needs the application's broadcasting authenticator")
	}
	if broker == nil {
		return nil, errors.New("joaju: a Hesape broadcaster needs the Broker that holds the server's channels")
	}
	return &HesapeBroadcaster{authorization: authorization, broker: broker}, nil
}

// Auth delegates the application's existing channel authorization.
func (b *HesapeBroadcaster) Auth(ctx context.Context, channel string) (auth.Grant, any, error) {
	return b.authorization.Auth(ctx, channel)
}

// ValidAuthenticationResponse delegates the application's existing response.
func (b *HesapeBroadcaster) ValidAuthenticationResponse(ctx context.Context, g auth.Grant, channel broadcasting.Channel, result any) (any, error) {
	return b.authorization.ValidAuthenticationResponse(ctx, g, channel, result)
}

// Broadcast delivers one Hesape event through Joaju's local Broker and Relay.
func (b *HesapeBroadcaster) Broadcast(ctx context.Context, g auth.Grant, channels []broadcasting.Channel, event string, payload map[string]any) error {
	if len(channels) == 0 {
		return nil
	}
	if event == "" {
		return errors.New("joaju: a broadcast needs an event name")
	}
	if strings.HasPrefix(event, ProtocolPrefix) || strings.HasPrefix(event, InternalPrefix) {
		return fmt.Errorf("joaju: %q is a reserved event name, and only the protocol may send one", event)
	}

	data := make(map[string]any, len(payload))
	for key, value := range payload {
		data[key] = value
	}
	var socket SocketID
	if value, ok := data["socket"].(string); ok {
		socket = SocketID(value)
	}
	delete(data, "socket")
	encoded, err := json.Marshal(data)
	if err != nil {
		return broadcasting.WrapBroadcastError(err, "broadcasting: encoding the payload of %s: %v", event, err)
	}

	for _, channel := range channels {
		name, err := NewChannelName(g, channel.Name)
		if err != nil {
			return err
		}
		message := Event{Name: event, Channel: name, Data: encoded, Socket: socket}

		held, err := b.broker.Find(ctx, g, name)
		switch {
		case errors.Is(err, ErrNoChannel):
		case err != nil:
			return err
		default:
			if err := held.Broadcast(ctx, message); err != nil {
				return err
			}
		}
		if carrier, ok := b.broker.(Carrier); ok {
			carrier.Carry(ctx, message)
		}
	}
	return nil
}

var _ broadcasting.Broadcaster = (*HesapeBroadcaster)(nil)
