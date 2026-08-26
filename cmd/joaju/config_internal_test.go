package main

import (
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arandu-io/joaju/protocols/pusher"
)

// lookup turns a map into an [environment], so a test never sets a variable on
// the process it runs in -- which is state one test shares with every other one
// running beside it.
func lookup(vars map[string]string) environment {
	return func(name string) (string, bool) {
		value, set := vars[name]

		return value, set
	}
}

// identity is the four variables without which no configuration is valid, so a
// test about anything else does not restate them.
func identity() map[string]string {
	return map[string]string{
		"JOAJU_APP_ID":     "3",
		"JOAJU_APP_KEY":    "278d425bdf160c739803",
		"JOAJU_APP_SECRET": "7ad3773142a6692b25b8",
		"JOAJU_TENANT":     "acme",
	}
}

func TestLoadConfigRequiresTheApplicationIdentity(t *testing.T) {
	t.Parallel()

	for _, missing := range []string{"JOAJU_APP_ID", "JOAJU_APP_KEY", "JOAJU_APP_SECRET", "JOAJU_TENANT"} {
		t.Run(missing, func(t *testing.T) {
			t.Parallel()

			vars := identity()
			delete(vars, missing)

			_, err := loadConfig(lookup(vars))
			if err == nil {
				t.Fatalf("a configuration without %s was accepted", missing)
			}
			if !strings.Contains(err.Error(), missing) {
				t.Fatalf("error = %v, and it does not name %s -- the one thing the reader has to fix", err, missing)
			}
		})
	}
}

func TestLoadConfigRefusesATenantThatCannotBeANamespace(t *testing.T) {
	t.Parallel()

	// A tenant is concatenated into a channel name and a relay topic, so one
	// carrying the separator reaches another tenant's namespace. The definition
	// is auth.ValidTenant's, and this only checks that it is consulted.
	vars := identity()
	vars["JOAJU_TENANT"] = "acme:evil"

	if _, err := loadConfig(lookup(vars)); err == nil {
		t.Fatal("a tenant carrying the tenant separator was accepted")
	}
}

func TestLoadConfigLeavesTheLibrarysDefaultsToTheLibrary(t *testing.T) {
	t.Parallel()

	cfg, err := loadConfig(lookup(identity()))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	// Zero, every one of them: joaju.ServerConfig documents zero as "the Default
	// of the same name", so a number filled in here would be a second place to
	// read the default from -- and the one the reader does not look at.
	if cfg.MaxMessageSize != 0 || cfg.MaxBodySize != 0 || cfg.OutboundQueue != 0 ||
		cfg.MaxConnections != 0 || cfg.MaxMessagesPerSecond != 0 ||
		cfg.MaxChannelsPerConnection != 0 ||
		cfg.WriteTimeout != 0 || cfg.PingInterval != 0 || cfg.PongTimeout != 0 {
		t.Fatalf("a limit was filled in here instead of by joaju.NewServer: %+v", cfg)
	}

	if cfg.Addr != defaultAddr {
		t.Errorf("Addr = %q, want %q", cfg.Addr, defaultAddr)
	}
	if cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, defaultShutdownTimeout)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %s, want %s", cfg.LogLevel, slog.LevelInfo)
	}
	if cfg.ClientEvents != pusher.ClientEventsOff {
		t.Error("client events are on without being asked for, and off is the default they should have")
	}
	if cfg.AllowedOrigins != nil {
		t.Errorf("AllowedOrigins = %q, want none", cfg.AllowedOrigins)
	}
}

func TestLoadConfigReadsEveryVariable(t *testing.T) {
	t.Parallel()

	vars := identity()
	for name, value := range map[string]string{
		"JOAJU_ADDR":                        "127.0.0.1:9000",
		"JOAJU_ALLOWED_ORIGINS":             " https://app.example.com , https://admin.example.com ,",
		"JOAJU_CLIENT_EVENTS":               "true",
		"JOAJU_MAX_MESSAGE_SIZE":            "20480",
		"JOAJU_MAX_BODY_SIZE":               "2097152",
		"JOAJU_OUTBOUND_QUEUE":              "128",
		"JOAJU_MAX_CONNECTIONS":             "-1",
		"JOAJU_MAX_MESSAGES_PER_SECOND":     "30",
		"JOAJU_MAX_CHANNELS_PER_CONNECTION": "25",
		"JOAJU_WRITE_TIMEOUT":               "5s",
		"JOAJU_PING_INTERVAL":               "20s",
		"JOAJU_PONG_TIMEOUT":                "1m",
		"JOAJU_SHUTDOWN_TIMEOUT":            "30s",
		"JOAJU_LOG_LEVEL":                   "debug",
	} {
		vars[name] = value
	}

	cfg, err := loadConfig(lookup(vars))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.Addr != "127.0.0.1:9000" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	// Trimmed, and the trailing comma is not an origin nobody can match.
	if want := []string{"https://app.example.com", "https://admin.example.com"}; !slices.Equal(cfg.AllowedOrigins, want) {
		t.Errorf("AllowedOrigins = %q, want %q", cfg.AllowedOrigins, want)
	}
	if cfg.ClientEvents != pusher.ClientEventsOn {
		t.Error("ClientEvents is off after being asked for")
	}
	if cfg.MaxMessageSize != 20480 || cfg.MaxBodySize != 2097152 {
		t.Errorf("sizes = %d and %d", cfg.MaxMessageSize, cfg.MaxBodySize)
	}
	if cfg.OutboundQueue != 128 || cfg.MaxMessagesPerSecond != 30 || cfg.MaxChannelsPerConnection != 25 {
		t.Errorf("counts = %d, %d and %d", cfg.OutboundQueue, cfg.MaxMessagesPerSecond, cfg.MaxChannelsPerConnection)
	}
	// Negative is the way to say no limit at all, and it takes writing -1 rather
	// than leaving a variable out.
	if cfg.MaxConnections != -1 {
		t.Errorf("MaxConnections = %d, want -1", cfg.MaxConnections)
	}
	if cfg.WriteTimeout != 5*time.Second || cfg.PingInterval != 20*time.Second || cfg.PongTimeout != time.Minute {
		t.Errorf("timeouts = %s, %s and %s", cfg.WriteTimeout, cfg.PingInterval, cfg.PongTimeout)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %s", cfg.LogLevel)
	}
}

func TestLoadConfigRefusesAnOriginABrowserCannotSend(t *testing.T) {
	t.Parallel()

	for _, origin := range []string{
		"app.example.com",
		"ftp://app.example.com",
		"https://user@app.example.com",
		"https://app.example.com/path",
		"https://app.example.com?mode=socket",
		"https://app.example.com#socket",
	} {
		t.Run(origin, func(t *testing.T) {
			t.Parallel()

			vars := identity()
			vars["JOAJU_ALLOWED_ORIGINS"] = origin

			_, err := loadConfig(lookup(vars))
			if err == nil {
				t.Fatalf("JOAJU_ALLOWED_ORIGINS=%q was accepted", origin)
			}
			if !strings.Contains(err.Error(), "JOAJU_ALLOWED_ORIGINS") || !strings.Contains(err.Error(), origin) {
				t.Fatalf("error = %v, and it does not identify the invalid origin", err)
			}
		})
	}
}

func TestLoadConfigRefusesAnOriginListWithNoOrigin(t *testing.T) {
	t.Parallel()

	vars := identity()
	vars["JOAJU_ALLOWED_ORIGINS"] = ", ,"

	_, err := loadConfig(lookup(vars))
	if err == nil {
		t.Fatal("JOAJU_ALLOWED_ORIGINS containing no origin was accepted as no allowlist")
	}
	if !strings.Contains(err.Error(), "JOAJU_ALLOWED_ORIGINS") {
		t.Fatalf("error = %v, and it does not name JOAJU_ALLOWED_ORIGINS", err)
	}
}

func TestLoadConfigRefusesAValueItCannotRead(t *testing.T) {
	t.Parallel()

	// A process that starts with a limit it silently ignored is a process
	// running a configuration nobody wrote. Every one of these is a refusal to
	// start, and every message names the variable.
	for name, value := range map[string]string{
		"JOAJU_MAX_MESSAGE_SIZE":            "10kb",
		"JOAJU_MAX_BODY_SIZE":               "-1",
		"JOAJU_OUTBOUND_QUEUE":              "many",
		"JOAJU_MAX_MESSAGES_PER_SECOND":     "30/s",
		"JOAJU_MAX_CHANNELS_PER_CONNECTION": "a hundred",
		"JOAJU_WRITE_TIMEOUT":               "5",
		"JOAJU_PING_INTERVAL":               "-20s",
		"JOAJU_SHUTDOWN_TIMEOUT":            "-1s",
		"JOAJU_CLIENT_EVENTS":               "yes please",
		"JOAJU_LOG_LEVEL":                   "chatty",
	} {
		t.Run(name+"="+value, func(t *testing.T) {
			t.Parallel()

			vars := identity()
			vars[name] = value

			_, err := loadConfig(lookup(vars))
			if err == nil {
				t.Fatalf("%s=%q was accepted", name, value)
			}
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("error = %v, and it does not name %s", err, name)
			}
		})
	}
}

func TestLoadConfigRefusesNegativeOperationalCounts(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"JOAJU_OUTBOUND_QUEUE", "JOAJU_MAX_MESSAGES_PER_SECOND"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			vars := identity()
			vars[name] = "-1"

			_, err := loadConfig(lookup(vars))
			if err == nil {
				t.Fatalf("%s=-1 was accepted", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("error = %v, and it does not name %s", err, name)
			}
		})
	}
}
