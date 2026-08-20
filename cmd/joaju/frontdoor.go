package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/arandu-io/hesape/auth"
)

// apiPrefix is the path the eight HTTP API routes share, and the one thing
// [frontDoor] has to know about the route table.
//
// The other two routes -- GET /app/{appKey} and GET /up -- carry no signature in
// the Pusher protocol, so the prefix is not a copy of the route table but the
// line between the credentialed half and the open half of it.
const apiPrefix = "/apps/"

// The query parameters of the Pusher REST authentication scheme.
const (
	// paramAuthKey is the app key, which names who is calling. It is not a
	// secret -- the browser has it too -- and it is checked so that a request
	// signed for another application is refused before the HMAC is computed.
	paramAuthKey = "auth_key"
	// paramAuthTimestamp is when the caller signed, in seconds since the epoch.
	paramAuthTimestamp = "auth_timestamp"
	// paramAuthSignature is the signature itself, hex encoded.
	paramAuthSignature = "auth_signature"
	// paramBodyMD5 is the digest of the request body, and it is what puts the
	// body inside a signature computed over the query string.
	paramBodyMD5 = "body_md5"
)

// signatureWindow is how far from now a signature may be stamped.
//
// Recomputing the HMAC and comparing is enough to prove the caller held the
// secret and says nothing about when, so a publish captured off the wire
// replays for as long as the secret lives. The window is the protocol's own
// answer --
// auth_timestamp exists for exactly this -- and ten minutes is wide enough that
// a container with a drifting clock still starts and narrow enough that a
// captured request is stale by the time anyone finds it.
const signatureWindow = 10 * time.Minute

// The identity a verified API caller acts as.
const (
	// applicationSubjectPrefix goes in front of the app key to make the subject
	// id, so a log line says which application acted rather than "app".
	applicationSubjectPrefix = "app:"
	// roleApplication marks the subject that proved the app secret. The two
	// policies read it, and nothing else mints it.
	roleApplication = "application"
)

// frontDoor is authentication for a process that has none of its own.
//
// [joaju.Server] reads the subject off the request context and never
// authenticates anybody: mounted inside an Arandu application it sits behind
// hesape/auth's Authenticate middleware, which is the only thing in the
// ecosystem that calls auth.WithSubject. A standalone process has no session to
// read, so it needs a front door, and the Pusher protocol already specifies
// which one:
//
//	/apps/...          server to server, signed with the app secret
//	/app/{appKey}      a browser opening a socket, no credential at all
//	/up                the health check, which discloses nothing
//
// This is not a second way to prove who is calling, which is what the routes
// themselves rule out: mounted inside an application they verify no credential
// of their own, because the request has already been through that
// application's front door. It is the first and only way in this deployment --
// no host application stands in front of this process, so nothing else in it
// authenticates anybody.
//
// What it does NOT do is take a tenant off the request. The tenant is the
// process's, from [config.Tenant], and it is the same one for the signed caller
// and the anonymous browser. A header naming a tenant would be the tenant rule
// undone by a middleware.
type frontDoor struct {
	// appKey is the key a signature has to name, and secret is what it has to be
	// computed with.
	appKey string
	secret []byte
	// tenant is what every subject minted here carries.
	tenant string
	// maxBody bounds what is read before the body is digested. It is the
	// server's own MaxBodySize, resolved by the caller, so that the limit a
	// request meets here is the limit it would have met inside.
	maxBody int64
	// now is time.Now, and a fixed clock in a test.
	now func() time.Time
	log *slog.Logger
	// next is the server.
	next http.Handler
}

// ServeHTTP decides who the request is from and hands it on.
//
// A request it cannot place is a request for a route that does not exist, and it
// goes through as the anonymous reader so that the answer is the server's 404
// rather than an authentication error about a path nobody serves.
func (d frontDoor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, apiPrefix) {
		// The socket route and the health check. auth.Guest is hesape's declared
		// anonymous reader, and its tenant is required and is the
		// application's, from configuration -- which is this process's whole
		// position: nobody signs in here, and the tenant is still not the
		// client's to choose.
		d.next.ServeHTTP(w, r.WithContext(auth.WithSubject(r.Context(), auth.Guest(d.tenant))))
		return
	}

	// The body is read here because body_md5 is inside the signed string, and it
	// is put back because the route behind this reads it too. The limit is the
	// server's, so a body too large is refused with the same number in both
	// places.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, d.maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			d.refuse(w, r, http.StatusRequestEntityTooLarge, "The body is larger than this server reads.", err)
			return
		}
		d.refuse(w, r, http.StatusBadRequest, "The body could not be read.", err)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	if err := d.verify(r, body); err != nil {
		// The sentence names the parameter that was wrong and goes to the log;
		// the caller is told that the signature was invalid and nothing else,
		// for the reason [joaju.Server] gives about its own refusals: a refusal
		// that explains itself is a refusal that helps whoever is guessing.
		d.refuse(w, r, http.StatusUnauthorized, "Authentication signature invalid.", err)
		return
	}

	d.next.ServeHTTP(w, r.WithContext(auth.WithSubject(r.Context(), d.application())))
}

// application is the subject a verified caller acts as.
//
// It is the application itself and not a person: what the signature proves is
// possession of the app secret, and the app secret belongs to the server that
// holds it. A subject invented here with a user id read out of the request would
// be an identity the request asserted about itself.
func (d frontDoor) application() auth.Subject {
	return auth.Subject{
		ID:     applicationSubjectPrefix + d.appKey,
		Tenant: d.tenant,
		Roles:  []string{roleApplication},
	}
}

// verify checks the Pusher signature on an API request.
//
// The order is the cheapest refusal first: the key names the wrong application,
// the stamp is outside the window, and only then the HMAC -- which is the one
// step that costs anything and the one that must not leak how far a guess got.
func (d frontDoor) verify(r *http.Request, body []byte) error {
	query := r.URL.Query()

	if key := query.Get(paramAuthKey); key != d.appKey {
		return fmt.Errorf("%s is %q and this server answers for another application", paramAuthKey, key)
	}

	stamp := query.Get(paramAuthTimestamp)
	seconds, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		return fmt.Errorf("%s is %q, which is not a number of seconds since the epoch", paramAuthTimestamp, stamp)
	}
	if skew := d.now().Sub(time.Unix(seconds, 0)).Abs(); skew > signatureWindow {
		return fmt.Errorf("%s is %s away from now, and this server accepts %s", paramAuthTimestamp, skew.Round(time.Second), signatureWindow)
	}

	want := signature(d.secret, signingString(r.Method, r.URL.EscapedPath(), query, body))
	// hmac.Equal and not ==, because the comparison of a secret-derived value
	// against one a caller chose is exactly the comparison whose duration says
	// how many leading characters were right.
	if !hmac.Equal([]byte(want), []byte(query.Get(paramAuthSignature))) {
		return errors.New("the signature does not match what this server computed")
	}

	return nil
}

// refuse answers and records the reason, in the shape [joaju.Server] answers in
// -- a JSON object with one message field -- so that a caller does not have to
// tell the front door and the server apart to read a refusal.
func (d frontDoor) refuse(w http.ResponseWriter, r *http.Request, status int, message string, err error) {
	d.log.InfoContext(r.Context(), "joaju: a request was refused at the front door",
		slog.String("route", r.Method+" "+r.URL.Path),
		slog.Int("status", status),
		slog.Any("error", err))

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
}

// signingString builds the string a Pusher REST signature is computed over:
// the method, the path and the query, each on its own line.
//
// The query is every parameter except the signature itself, sorted by name and
// written "name=value" joined by "&" -- unescaped, because what is signed is
// what the caller wrote and not what it put on the wire. body_md5 is dropped
// from what arrived and recomputed here when there is a body, so a caller cannot
// sign one digest and send another payload.
//
// appId, appKey and channelName are not in the query at all: they are path
// values and never appear in a URL query, so there is nothing to drop.
//
// The digest is MD5 because the protocol says MD5. It is not being trusted as a
// hash function on its own -- it is inside an HMAC-SHA256 over the whole string
// -- and changing it would mean no Pusher client could sign a request this
// server accepts.
func signingString(method, path string, query url.Values, body []byte) string {
	params := make(map[string]string, len(query)+1)
	for name, values := range query {
		if name == paramAuthSignature || name == paramBodyMD5 {
			continue
		}
		if len(values) > 0 {
			params[name] = values[0]
		}
	}
	if len(body) > 0 {
		params[paramBodyMD5] = hex.EncodeToString(digest(body))
	}

	pairs := make([]string, 0, len(params))
	for _, name := range slices.Sorted(maps.Keys(params)) {
		pairs = append(pairs, name+"="+params[name])
	}

	return method + "\n" + path + "\n" + strings.Join(pairs, "&")
}

// signature is the hex HMAC-SHA256 of the signing string under the app secret,
// which is what the caller puts in auth_signature.
func signature(secret []byte, signing string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signing))

	return hex.EncodeToString(mac.Sum(nil))
}

// digest is the body_md5 of a request body.
//
// It is a function of its own so that the one call to a broken hash function in
// this repository is in one place, with the sentence above it explaining why the
// protocol leaves no choice.
func digest(body []byte) []byte {
	sum := md5.Sum(body)

	return sum[:]
}
