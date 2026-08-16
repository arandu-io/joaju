package main

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
)

// The prefix every variable this process reads carries.
//
// One prefix and one source: there is no configuration file, no flag and no
// remote configuration service, because a process that can be configured three
// ways is a process whose running configuration nobody can state. The
// environment is the one a container runtime, a systemd unit and a shell all
// already have.
const envPrefix = "JOAJU_"

// The defaults that are this binary's own, rather than the library's.
//
// Everything a [joaju.ServerConfig] already has a Default for is left at zero
// and filled in by [joaju.NewServer] -- that is the struct's stated convention,
// and repeating a number here would be a second place to read it from. These
// three have no counterpart there because they belong to the process and not to
// the server.
const (
	// defaultAddr is the address the server listens on when none was given.
	// The port is the one a client's configuration expects by default.
	defaultAddr = ":8080"
	// defaultShutdownTimeout is how long a signalled process gives itself to
	// stop listening, answer the requests already in flight and close the
	// sockets it holds. See [shutdown] for what happens in that window.
	defaultShutdownTimeout = 10 * time.Second
	// defaultReadHeaderTimeout bounds how long a client may take to send its
	// request headers.
	//
	// It is the one http.Server timeout this process sets. ReadTimeout and
	// WriteTimeout would be wrong here: a WebSocket is hijacked out of net/http
	// and its deadlines are the ones [joaju.ServerConfig] carries -- PongTimeout
	// on the read side, WriteTimeout on the write side -- so a second pair set
	// on the http.Server would be a second answer to the same question, and the
	// stricter of the two would end sockets that are behaving.
	defaultReadHeaderTimeout = 10 * time.Second
)

// config is what this process reads out of its environment, already parsed.
//
// The socket limits are held as they arrived, zero included, because zero is
// the value [joaju.ServerConfig] documents as "the Default of the same name".
// Filling one in here would move the default out of the library that states it
// and into a binary that only passes it on.
type config struct {
	// Addr is the TCP address to listen on, in net.Listen form.
	//
	// It is HTTP, and there is no TLS field: a WebSocket server terminating its
	// own TLS would need a certificate, a reload path and a cipher policy, all
	// three of which the reverse proxy in front of it already has.
	Addr string

	// AppID and AppKey are the {appId} of the API routes and the {appKey} of the
	// socket route. AppSecret is the shared secret the API routes are signed
	// with; see [frontDoor].
	AppID     string
	AppKey    string
	AppSecret string

	// Tenant is whose data this process serves, and it is configuration rather
	// than anything that arrives with a request.
	//
	// It is the whole of the tenant rule for a standalone binary: every Grant
	// issued here carries this tenant, every channel name is built from such a
	// Grant, and nothing on the wire can change it. One process is one
	// application, and one application is one customer's namespace.
	Tenant string

	// AllowedOrigins is the origins a browser may open a socket from, verbatim
	// and complete -- "https://app.example.com", scheme and port included.
	//
	// Empty means the only origin check is the transport's own same-origin
	// default, which already refuses an Origin naming another host. The list
	// narrows that; it cannot widen it. See [connectPolicy.Can].
	AllowedOrigins []string

	// ClientEvents is whether a frame one browser publishes is relayed to the
	// others on the channel. Off unless a deployment says otherwise, which is
	// the protocol's default too. See [joaju.ClientEvents].
	ClientEvents joaju.ClientEvents

	// The socket's limits, passed through untouched. Zero is the Default of the
	// same name in [joaju.ServerConfig], except MaxConnections, where a negative
	// number means no limit.
	MaxMessageSize       int64
	MaxBodySize          int64
	OutboundQueue        int
	MaxConnections       int
	MaxMessagesPerSecond int
	WriteTimeout         time.Duration
	PingInterval         time.Duration
	PongTimeout          time.Duration

	// ShutdownTimeout is the deadline the signal handler works to. Zero means
	// [defaultShutdownTimeout].
	ShutdownTimeout time.Duration

	// LogLevel is the lowest level that is written. Zero is slog.LevelInfo.
	LogLevel slog.Level
}

// environment is where [loadConfig] reads from: os.LookupEnv in the process,
// and a map in a test.
//
// It is a function and not the os package directly so that reading the
// configuration is testable without setting variables on the running process --
// which a test that runs in parallel with another cannot do safely.
type environment func(name string) (string, bool)

// loadConfig reads the whole configuration, or says which variable was wrong
// and why.
//
// Every value is read and every error is returned at the first one, rather than
// collected: a process that will not start has one thing to fix, and reporting
// the first is what a reader acts on. The four required variables are the
// application's identity, and there is no default that could stand in for any of
// them -- a server that invents its own app key answers no client's
// configuration, and one that invents a secret accepts nobody's signature.
func loadConfig(env environment) (config, error) {
	cfg := config{
		Addr:      env.text("ADDR"),
		AppID:     env.text("APP_ID"),
		AppKey:    env.text("APP_KEY"),
		AppSecret: env.text("APP_SECRET"),
		Tenant:    env.text("TENANT"),
	}
	if cfg.Addr == "" {
		cfg.Addr = defaultAddr
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{"APP_ID", cfg.AppID},
		{"APP_KEY", cfg.AppKey},
		{"APP_SECRET", cfg.AppSecret},
		{"TENANT", cfg.Tenant},
	} {
		if required.value == "" {
			return config{}, fmt.Errorf("joaju: %s%s is not set, and this server cannot invent one", envPrefix, required.name)
		}
	}
	// The same rule the rest of the ecosystem applies, applied where the value
	// enters: a tenant is concatenated into a channel name, a Redis topic and a
	// log field, so one carrying a separator reaches another tenant's namespace.
	// auth.ValidTenant is the definition, and a second one written here is how
	// the looser of the two gets found.
	if !auth.ValidTenant(cfg.Tenant) {
		return config{}, fmt.Errorf("joaju: %sTENANT is %q, which cannot be a tenant: it is limited to letters, digits, - and _, up to 64 characters", envPrefix, cfg.Tenant)
	}

	cfg.AllowedOrigins = env.list("ALLOWED_ORIGINS")

	var err error
	if cfg.ClientEvents, err = env.clientEvents("CLIENT_EVENTS"); err != nil {
		return config{}, err
	}
	if cfg.MaxMessageSize, err = env.size("MAX_MESSAGE_SIZE"); err != nil {
		return config{}, err
	}
	if cfg.MaxBodySize, err = env.size("MAX_BODY_SIZE"); err != nil {
		return config{}, err
	}
	if cfg.OutboundQueue, err = env.count("OUTBOUND_QUEUE"); err != nil {
		return config{}, err
	}
	if cfg.MaxConnections, err = env.count("MAX_CONNECTIONS"); err != nil {
		return config{}, err
	}
	if cfg.MaxMessagesPerSecond, err = env.count("MAX_MESSAGES_PER_SECOND"); err != nil {
		return config{}, err
	}
	if cfg.WriteTimeout, err = env.duration("WRITE_TIMEOUT"); err != nil {
		return config{}, err
	}
	if cfg.PingInterval, err = env.duration("PING_INTERVAL"); err != nil {
		return config{}, err
	}
	if cfg.PongTimeout, err = env.duration("PONG_TIMEOUT"); err != nil {
		return config{}, err
	}
	if cfg.ShutdownTimeout, err = env.duration("SHUTDOWN_TIMEOUT"); err != nil {
		return config{}, err
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if cfg.LogLevel, err = env.level("LOG_LEVEL"); err != nil {
		return config{}, err
	}

	return cfg, nil
}

// text is one variable, trimmed, or the empty string.
func (env environment) text(name string) string {
	value, _ := env(envPrefix + name)

	return strings.TrimSpace(value)
}

// list is one variable read as a comma-separated list, with the empty entries
// dropped -- so a trailing comma is not an origin nobody can match.
func (env environment) list(name string) []string {
	raw := env.text(name)
	if raw == "" {
		return nil
	}

	entries := strings.Split(raw, ",")
	kept := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry = strings.TrimSpace(entry); entry != "" {
			kept = append(kept, entry)
		}
	}
	if len(kept) == 0 {
		return nil
	}

	return kept
}

// count is one variable read as a whole number. An unset variable is zero,
// which every field that uses it documents the meaning of.
func (env environment) count(name string) (int, error) {
	raw := env.text(name)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("joaju: %s%s is %q, which is not a whole number", envPrefix, name, raw)
	}

	return value, nil
}

// size is count for a field measured in bytes, and refuses a negative one: a
// limit below zero is not a smaller limit, it is a limit nothing can satisfy.
func (env environment) size(name string) (int64, error) {
	raw := env.text(name)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("joaju: %s%s is %q, which is not a number of bytes", envPrefix, name, raw)
	}
	if value < 0 {
		return 0, fmt.Errorf("joaju: %s%s is %d, and a size cannot be negative", envPrefix, name, value)
	}

	return value, nil
}

// duration is one variable read in Go's own duration notation -- "30s", "2m",
// "1h30m" -- because a bare number of seconds is a unit the reader has to know
// by heart, and this one is written down.
func (env environment) duration(name string) (time.Duration, error) {
	raw := env.text(name)
	if raw == "" {
		return 0, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("joaju: %s%s is %q, which is not a duration: write it as 30s, 2m or 1h30m", envPrefix, name, raw)
	}
	if value < 0 {
		return 0, fmt.Errorf("joaju: %s%s is %s, and a duration cannot be negative", envPrefix, name, value)
	}

	return value, nil
}

// clientEvents reads the one switch that changes what this server relays.
//
// It is spelled out rather than accepted as any non-empty string, because the
// value that turns it on is the value somebody types once and nobody reads
// again: "false" quietly enabling client events would be the worst possible
// direction for that mistake to fail in.
func (env environment) clientEvents(name string) (joaju.ClientEvents, error) {
	raw := env.text(name)
	if raw == "" {
		return joaju.ClientEventsOff, nil
	}

	on, err := strconv.ParseBool(raw)
	if err != nil {
		return joaju.ClientEventsOff, fmt.Errorf("joaju: %s%s is %q, which is not true or false", envPrefix, name, raw)
	}
	if on {
		return joaju.ClientEventsOn, nil
	}

	return joaju.ClientEventsOff, nil
}

// level reads the lowest level the log writes: debug, info, warn or error.
//
// slog parses it, so the offsets it understands -- "warn+1" -- are understood
// here too, and the vocabulary is the one the log lines themselves are labelled
// with. It is the only thing about the logger a deployment chooses.
func (env environment) level(name string) (slog.Level, error) {
	raw := env.text(name)
	if raw == "" {
		return slog.LevelInfo, nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return slog.LevelInfo, fmt.Errorf("joaju: %s%s is %q, which is not a level: write debug, info, warn or error", envPrefix, name, raw)
	}

	return level, nil
}
