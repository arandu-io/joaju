// Command echo is the server the Autobahn TestSuite fuzzing client attacks.
//
// It accepts a websocket handshake on any path and writes every message it
// reads straight back, with the same opcode and the same bytes. That is the
// whole application: every case the suite measures is a property of the
// protocol layer underneath, so the more this program does, the less the score
// says about [ws].
//
// It is not part of the server. Nothing imports it, it is under internal, and
// it exists so that `go build ./...` keeps compiling the thing the conformance
// run depends on -- a harness that stops building is a harness nobody notices
// is broken until the day it is needed.
//
// Run it with run.sh in the parent directory, which starts it, points the
// suite at it and tears it down. Started by hand:
//
//	go run ./ws/internal/autobahn/echo -addr :9001
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arandu-io/joaju/ws"
)

// readLimit is the ceiling on one echoed message, and it is deliberately far
// above anything the suite sends.
//
// Group 9 goes up to 16 MiB in a single frame, and group 9.3 up to 16 MiB
// spread over fragments. A limit at or below that turns those cases into a
// close with 1009 where the suite is waiting for its bytes back, which reads as
// a conformance failure and is really a harness misconfiguration -- the most
// expensive kind, because the score looks like a bug in [ws].
//
// Thirty-two mebibytes leaves the limit wired up and exercised while leaving
// the largest case twice the room it needs.
const readLimit = 32 << 20

// shutdownGrace is how long the process waits for connections to end after a
// signal. It is short: the suite drives this, and a run that was interrupted is
// a run that is being discarded anyway.
const shutdownGrace = 2 * time.Second

func main() {
	addr := flag.String("addr", ":9001", "address to listen on")
	flag.Parse()

	log.SetFlags(0)
	log.SetPrefix("autobahn-echo: ")

	server := &http.Server{
		Addr:    *addr,
		Handler: http.HandlerFunc(echo),

		// No ReadTimeout and no WriteTimeout, and this is the second setting
		// that decides cases rather than tuning them.
		//
		// http.Server timeouts cover the whole connection, not one request, and
		// a hijacked websocket inherits them. Several cases send a frame in
		// pieces, seconds apart, to check that a reader waits rather than
		// guessing; case 9 pushes 16 MiB, which takes as long as it takes. A
		// deadline here would fail those with a transport error that has
		// nothing to do with the protocol.
		//
		// ReadHeaderTimeout stays, because it bounds only the handshake, which
		// arrives in one piece or is not a handshake.
		ReadHeaderTimeout: 10 * time.Second,
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", *addr, err)
	}

	// The resolved address, not the flag: run.sh waits for this line before it
	// starts the container, and ":9001" is not something it can dial.
	log.Printf("listening on %s", listener.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve failed: %v", err)
	}
	<-done
}

// upgrader is shared by every request. It holds no per-connection state.
//
// HandshakeTimeout bounds writing the 101 and is cleared afterwards by
// [ws.Upgrader.Upgrade], so it costs the long, slow cases nothing.
var upgrader = ws.Upgrader{HandshakeTimeout: 10 * time.Second}

// echo upgrades the request and mirrors every message until the connection
// ends.
func echo(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already answered the client. This line is for whoever
		// reads the log after a run where every case failed at the handshake --
		// an Origin the check refused, say, which is invisible from the suite's
		// side, where it is just a 403.
		log.Printf("upgrade refused for %s: %v", r.RemoteAddr, err)

		return
	}
	defer func() { _ = conn.Close() }()

	conn.SetReadLimit(readLimit)

	// No read deadline. Cases 5.19 and 5.20 hold a fragmented message open for
	// a second between fragments to check that control frames still get
	// through; a deadline is exactly what they are built to catch.
	//
	// Nothing here leaks: the process lives for one run and run.sh kills it.
	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			// Every ending arrives here, and almost all of them are the point
			// of the case that produced them: the peer closed, or [ws] failed
			// the connection with the close code the suite is checking for.
			// ReadMessage has already sent that frame and shut the socket.
			return
		}

		if err := conn.WriteMessage(messageType, payload); err != nil {
			log.Printf("echo write failed for %s: %v", conn.RemoteAddr(), err)

			return
		}
	}
}
