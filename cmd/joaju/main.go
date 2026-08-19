// Command joaju runs the WebSocket server as a process.
//
// The library is mountable: [joaju.Server] is an http.Handler, and an Arandu
// application that wants sockets puts it behind its own middleware and its own
// policies. This command is the other deployment -- the socket server as a
// process of its own, which is what a container image runs.
//
// # What it is configured with
//
// The environment, every variable prefixed JOAJU_. There is no flag and no
// configuration file, because two ways to say the same thing is two ways to be
// wrong about what a running process is doing. See [config].
//
// # What it decides, and what it refuses to decide
//
// A [joaju.ServerConfig] needs a [joaju.ConnectPolicy] and a
// [joaju.SubscriptionPolicy], and a policy is an application's own rule about
// its own people. A generic process has no people. What it has is the app
// secret, which in the Pusher protocol is the credential of the application
// itself, so that is what it authenticates: the HTTP API is signed and
// [frontDoor] verifies it, a browser is the anonymous reader hesape calls
// auth.Guest, and the tenant of both is the process's configuration and never
// the request's.
//
// The consequence is stated where it is decided, in [subscriptionPolicy]: a
// browser reaches a public channel because the tenant is the whole question
// there, and reaches a private or presence one by offering the signature the
// Pusher protocol has its application issue -- which arrives as
// [joaju.Subscription.Auth] and is recomputed here. The signature decides
// nothing on its own; the policy does, and the tenant still comes off the
// Grant.
//
// # What terminates TLS
//
// Not this. It serves HTTP and a reverse proxy in front of it holds the
// certificate.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arandu-io/joaju"
)

func main() {
	if err := run(); err != nil {
		// Before the logger exists there is nowhere else for this to go, and
		// after it exists a second copy would be one line reported twice. The
		// exit status is what a supervisor reads either way.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run builds everything, serves until a signal, and stops.
func run() error {
	cfg, err := loadConfig(os.LookupEnv)
	if err != nil {
		return err
	}

	// JSON, because the output of a server is read by whatever collects it and
	// not by the person who started it. The level is the only thing configured.
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	server, err := newServer(cfg, log)
	if err != nil {
		return err
	}

	maxBody := cfg.MaxBodySize
	if maxBody == 0 {
		maxBody = joaju.DefaultMaxBodySize
	}

	httpServer := &http.Server{
		Handler: frontDoor{
			appKey:  cfg.AppKey,
			secret:  []byte(cfg.AppSecret),
			tenant:  cfg.Tenant,
			maxBody: maxBody,
			now:     time.Now,
			log:     log,
			next:    server,
		},
		// The one timeout set here. ReadTimeout and WriteTimeout would be a
		// second answer to a question [joaju.ServerConfig] already answers with
		// PongTimeout and WriteTimeout, and they would answer it for a
		// connection net/http no longer owns.
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		// net/http's own errors -- a bad TLS handshake, a panic in a handler --
		// go where every other line goes rather than to the standard logger.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	// The listener is opened before anything is announced, so the line below is
	// true when it is written: a port already in use is an error and not a
	// server that claims to be listening and is not.
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("joaju: %s cannot be listened on: %w", cfg.Addr, err)
	}

	// SIGINT and SIGTERM, which are the two a container runtime and a terminal
	// send. SIGTSTP is deliberately not caught: a Go process suspended by the
	// terminal is resumed by the terminal, and catching it would turn a suspend
	// into a shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serving := make(chan error, 1)
	go func() { serving <- httpServer.Serve(listener) }()

	log.Info("joaju: listening",
		slog.String("address", listener.Addr().String()),
		slog.String("app_id", cfg.AppID))

	select {
	case err := <-serving:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("joaju: the listener stopped: %w", err)
	case <-ctx.Done():
	}
	// The handler is put back before the shutdown runs, so a second signal from
	// somebody who has waited long enough kills the process instead of being
	// swallowed by a shutdown that is already under way.
	stop()

	return shutdown(server, httpServer, log, cfg.ShutdownTimeout)
}

// newServer assembles the server out of the configuration.
//
// The two policies are values with no state beyond the tenant, so they are built
// here rather than injected: there is one application in this process and one
// rule for it, and a seam to swap them for others would be a second way to
// decide who may connect.
func newServer(cfg config, log *slog.Logger) (*joaju.Server, error) {
	broker := joaju.NewMemoryBroker()
	subscribe := subscriptionPolicy{
		tenant: cfg.Tenant,
		appKey: cfg.AppKey,
		secret: []byte(cfg.AppSecret),
	}

	return joaju.NewServer(joaju.ServerConfig{
		AppID:  cfg.AppID,
		AppKey: cfg.AppKey,
		Broker: broker,
		Connect: connectPolicy{
			tenant:  cfg.Tenant,
			origins: cfg.AllowedOrigins,
		},
		Subscribe: subscribe,
		Protocol:  frameLayer(broker, subscribe, cfg),
		Log:       log,

		// Relay is nil, so the four metrics routes answer for this process --
		// which is the true answer for a deployment of one and the wrong answer
		// for any other. A second instance needs a bus, the bus is Redis, and
		// Redis lives in a submodule this binary does not import yet. Until it
		// does, MetricsTimeout would be a timeout on a question nobody is asked.
		//
		// Observer is nil too, which [joaju.NewServer] reads as NopObserver:
		// there is nothing in this process for the five events to be reported
		// to, and a metrics exporter is a decision of its own.

		MaxMessageSize:       cfg.MaxMessageSize,
		MaxBodySize:          cfg.MaxBodySize,
		OutboundQueue:        cfg.OutboundQueue,
		MaxConnections:       cfg.MaxConnections,
		MaxMessagesPerSecond: cfg.MaxMessagesPerSecond,
		WriteTimeout:         cfg.WriteTimeout,
		PingInterval:         cfg.PingInterval,
		PongTimeout:          cfg.PongTimeout,
	})
}

// frameLayer is the [joaju.Protocol] this process hands its sockets to.
//
// The Pusher protocol is the library's, not this binary's: the server owns the
// socket and owns no part of the frame layer, and a second implementation of it
// living in a command would be the second answer to what a frame looks like that
// [joaju.Protocol] exists to prevent. This function is one line for that reason,
// and it is the only line here that names a constructor rather than a type -- so
// it is the only line that has to change if the frame layer is spelled
// differently when it lands.
func frameLayer(broker joaju.Broker, subscribe joaju.SubscriptionPolicy, cfg config) joaju.Protocol {
	return joaju.NewPusher(broker, subscribe, joaju.PusherConfig{
		// The client is told to ping at half the read deadline. It is an
		// instruction a browser obeys and PongTimeout is a deadline the server
		// enforces, so the first has to sit comfortably below the second: a
		// client pinging at the deadline pings a socket already hung up on, and
		// the round trip it needs has not even been spent yet.
		//
		// A zero PongTimeout is ServerConfig filling its own default, so the
		// number is known and the same one is used here. Leaving the field out
		// instead would send the client to its own fallback, which the Pusher
		// clients put at 120 seconds -- four times this server's deadline, so
		// every idle socket would be hung up on before the client thought to
		// ping.
		ActivityTimeout:          activityTimeout(cfg.PongTimeout),
		ClientEvents:             cfg.ClientEvents,
		MaxChannelsPerConnection: cfg.MaxChannelsPerConnection,
	})
}

// activityTimeout is how long a client may stay silent before it should ping.
//
// Half the read deadline, so that a client obeying the instruction is heard well
// before the deadline it would otherwise reach with the round trip still
// unspent.
func activityTimeout(pong time.Duration) time.Duration {
	if pong <= 0 {
		pong = joaju.DefaultPongTimeout
	}

	return pong / 2
}

// shutdown stops the process in the order that leaves nothing half-served.
//
// The listener goes first, so nothing new arrives at a process that is leaving
// and the requests already in flight are answered. The sockets go second,
// because http.Server.Shutdown neither closes nor waits for them: they were
// hijacked out of net/http at the upgrade, and [joaju.Server.Close] is what
// terminates them -- across every tenant, on purpose.
//
// Whether the close frame reaches each client is best effort, which is the
// library's own decision and the right one: a socket that will not take the
// frame is a socket there is nothing left to say to.
func shutdown(server *joaju.Server, httpServer *http.Server, log *slog.Logger, within time.Duration) error {
	// Not the signalled context: it is already cancelled, and a shutdown given a
	// cancelled deadline is a shutdown that skips everything it was asked to do.
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()

	log.Info("joaju: stopping", slog.Duration("within", within))

	err := httpServer.Shutdown(ctx)
	server.Close(ctx)

	if err != nil {
		return fmt.Errorf("joaju: the shutdown did not finish within %s: %w", within, err)
	}

	return nil
}
