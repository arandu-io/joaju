package pusher

import (
	"net/http"

	"github.com/arandu-io/joaju"
)

// This file is the HTTP half of the Pusher protocol: the routes a client calls
// instead of opening a socket.
//
// They are here and not in [joaju] for the reason the frames are: a wire format
// is what a client speaks over a socket AND what it calls over HTTP, and
// /apps/{appId}/events is as much this protocol's as pusher:subscribe is. The
// server owns the socket, hands a [joaju.API] to [joaju.Protocol.Routes], and
// mounts what comes back.
//
// # What is NOT here
//
// The socket route. An upgrade is the transport's: the server compares the app
// key, mints the socket id, runs the [joaju.ConnectPolicy] and hands what comes
// out to a [joaju.Protocol] as a [joaju.Connection]. Nothing in this package is
// ever given an http.ResponseWriter it may hijack.

// routes is the Pusher HTTP API, built out of everything the server lets a
// route reach.
//
// It embeds the [joaju.API] rather than holding it in a field so that a handler
// reads Broker, Connect and Subscribe by their own names: the whole of this
// type's state is that value, and a second name for it would be a second thing
// to keep in step.
type routes struct {
	joaju.API
}

// Routes are the Pusher HTTP API, on the patterns the protocol names them.
func (p *pusher) Routes(api joaju.API) http.Handler {
	return routes{api}.mux()
}

// mux registers every route on a ServeMux of its own.
//
// It is the protocol's mux and not the server's, so the patterns here can be
// read as the protocol's list: what a client may call, in one place, next to
// the frames it would otherwise have sent.
func (r routes) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /up", up)

	return mux
}

// up is GET /up: the health check.
//
// It is the one route with no Grant, and it can be, because it reads nothing:
// it says this process is answering, which whoever can reach the port already
// knows. It is a function and not a method for the same reason -- there is
// nothing on the [joaju.API] it could want.
func up(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
