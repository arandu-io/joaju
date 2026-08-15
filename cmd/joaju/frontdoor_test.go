package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
)

// The application this front door answers for. The key and the secret are the
// pair from Pusher's own worked example of the REST signature, so that
// [TestSigningStringIsThePusherCanonicalForm] and the tests around it agree on
// who is calling.
const (
	testAppKey    = "278d425bdf160c739803"
	testAppSecret = "7ad3773142a6692b25b8"
	testTenant    = "acme"
)

// testClock is the moment the signatures in these tests are stamped with, so
// that a test does not fail one day because the stamp aged past
// [signatureWindow].
var testClock = time.Unix(1353684343, 0)

// TestSigningStringIsThePusherCanonicalForm checks this implementation against
// the worked example in Pusher's REST authentication documentation.
//
// It is the one test here that proves anything about interoperability, and the
// canonical form is what it has to prove: HMAC-SHA256 over a string is not
// ambiguous, and every implementation that has ever disagreed about a Pusher
// signature disagreed about which string to sign -- what goes in it, in what
// order, and escaped or not. A client SDK that builds this string has to be
// accepted by this server.
//
// The body and its digest are the documented ones, which is a second check
// worth having: it says the body_md5 computed here is the body_md5 a client
// computes.
func TestSigningStringIsThePusherCanonicalForm(t *testing.T) {
	t.Parallel()

	body := `{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}`
	query := url.Values{
		paramAuthKey:       {testAppKey},
		paramAuthTimestamp: {"1353684343"},
		"auth_version":     {"1.0"},
		// The digest as the caller sent it, which is dropped and recomputed. It
		// is the documented one, so recomputing has to produce it again.
		paramBodyMD5: {"ec365a775a4cd0599faeb73354201b6f"},
	}

	got := signingString(http.MethodPost, "/apps/3/events", query, []byte(body))
	want := "POST\n/apps/3/events\n" +
		"auth_key=278d425bdf160c739803" +
		"&auth_timestamp=1353684343" +
		"&auth_version=1.0" +
		"&body_md5=ec365a775a4cd0599faeb73354201b6f"
	if got != want {
		t.Fatalf("signing string =\n%q\nwant\n%q", got, want)
	}

	// The hex below was computed independently of this code, with
	// `openssl dgst -sha256 -hmac`, over the string above. It is here so that a
	// change to signature is caught by something other than signature.
	mac := signature([]byte(testAppSecret), got)
	if mac != "97eff1d32a774db3ee5e1a4abb79a33b06527b8e498a4756ac716fdd36ba0f59" {
		t.Fatalf("signature = %q", mac)
	}
}

func TestSigningStringLeavesOutTheSignatureAndRecomputesTheDigest(t *testing.T) {
	t.Parallel()

	// body_md5 as it arrived is dropped and computed again from the body, so a
	// caller cannot sign one digest and send another payload. The signature
	// itself is dropped because it is the thing being computed.
	query := url.Values{
		paramAuthSignature: {"whatever the caller sent"},
		paramBodyMD5:       {"a digest of something else"},
		paramAuthKey:       {testAppKey},
		paramAuthTimestamp: {"1353684343"},
	}

	signing := signingString(http.MethodPost, "/apps/3/events", query, []byte("{}"))

	if strings.Contains(signing, "whatever the caller sent") {
		t.Error("the signature the caller sent is inside the string it is compared against")
	}
	if strings.Contains(signing, "a digest of something else") {
		t.Error("the digest the caller sent was signed instead of the body that arrived")
	}
	// md5 of "{}", which is what a body of "{}" has to produce.
	if want := paramBodyMD5 + "=99914b932bd37a50b983c5e7c90ae93b"; !strings.Contains(signing, want) {
		t.Errorf("signing string = %q, and it does not carry %q", signing, want)
	}
}

func TestSigningStringSortsTheQueryAndKeepsTheMethodAndPath(t *testing.T) {
	t.Parallel()

	query := url.Values{
		"zulu":             {"last"},
		paramAuthKey:       {testAppKey},
		paramAuthTimestamp: {"1353684343"},
	}

	signing := signingString(http.MethodGet, "/apps/3/channels", query, nil)
	lines := strings.Split(signing, "\n")
	if len(lines) != 3 {
		t.Fatalf("signing string = %q, want three lines", signing)
	}
	if lines[0] != http.MethodGet || lines[1] != "/apps/3/channels" {
		t.Fatalf("the method and path lines are %q and %q", lines[0], lines[1])
	}
	// Sorted by name, and with no body there is no body_md5 at all.
	want := paramAuthKey + "=" + testAppKey + "&" + paramAuthTimestamp + "=1353684343&zulu=last"
	if lines[2] != want {
		t.Fatalf("query line = %q, want %q", lines[2], want)
	}
}

// door builds a front door whose next handler records the subject it was given,
// which is the only thing a front door produces.
func door(t *testing.T) (frontDoor, *recorded) {
	t.Helper()

	seen := &recorded{}

	return frontDoor{
		appKey:  testAppKey,
		secret:  []byte(testAppSecret),
		tenant:  testTenant,
		maxBody: 1 << 20,
		now:     func() time.Time { return testClock },
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		next:    seen,
	}, seen
}

// recorded is the server's place in these tests.
type recorded struct {
	reached bool
	subject auth.Subject
	found   bool
	body    string
}

func (r *recorded) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.reached = true
	r.subject, r.found = auth.SubjectFrom(req.Context())
	body, _ := io.ReadAll(req.Body)
	r.body = string(body)
	w.WriteHeader(http.StatusOK)
}

// sign builds a request the front door should accept.
func sign(t *testing.T, method, path, body string) *http.Request {
	t.Helper()

	query := url.Values{
		paramAuthKey:       {testAppKey},
		paramAuthTimestamp: {strconv.FormatInt(testClock.Unix(), 10)},
		"auth_version":     {"1.0"},
	}
	query.Set(paramAuthSignature, signature([]byte(testAppSecret), signingString(method, path, query, []byte(body))))

	return httptest.NewRequest(method, path+"?"+query.Encode(), strings.NewReader(body))
}

func TestFrontDoorAdmitsTheApplicationAndLeavesTheBodyReadable(t *testing.T) {
	t.Parallel()

	door, seen := door(t)
	body := `{"name":"order.placed","channel":"private-orders.17","data":"{}"}`

	response := httptest.NewRecorder()
	door.ServeHTTP(response, sign(t, http.MethodPost, "/apps/3/events", body))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if !seen.found {
		t.Fatal("the request reached the server with no subject on it, and every API route answers 401 to that")
	}
	if !seen.subject.HasRole(roleApplication) {
		t.Errorf("subject = %+v, and it does not carry %q", seen.subject, roleApplication)
	}
	if seen.subject.Tenant != testTenant {
		t.Errorf("tenant = %q, want %q -- it is the process's and never the request's", seen.subject.Tenant, testTenant)
	}
	if seen.subject.ID != applicationSubjectPrefix+testAppKey {
		t.Errorf("subject id = %q", seen.subject.ID)
	}
	// The front door reads the body to digest it, so it has to put it back.
	if seen.body != body {
		t.Errorf("the server read %q, and the client sent %q", seen.body, body)
	}
}

func TestFrontDoorRefusesWhatItCannotVerify(t *testing.T) {
	t.Parallel()

	stale := strconv.FormatInt(testClock.Add(-2*signatureWindow).Unix(), 10)

	for name, corrupt := range map[string]func(url.Values){
		"no signature at all": func(q url.Values) { q.Del(paramAuthSignature) },
		"another application": func(q url.Values) {
			q.Set(paramAuthKey, "another key entirely")
		},
		"a signature for another body": func(q url.Values) {
			q.Set(paramAuthSignature, signature([]byte(testAppSecret),
				signingString(http.MethodPost, "/apps/3/events", q, []byte(`{"other":"body"}`))))
		},
		"a signature under another secret": func(q url.Values) {
			q.Set(paramAuthSignature, signature([]byte("not the secret"),
				signingString(http.MethodPost, "/apps/3/events", q, []byte("{}"))))
		},
		"no timestamp":      func(q url.Values) { q.Del(paramAuthTimestamp) },
		"a stale timestamp": func(q url.Values) { q.Set(paramAuthTimestamp, stale) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			door, seen := door(t)

			signed := sign(t, http.MethodPost, "/apps/3/events", "{}")
			query := signed.URL.Query()
			corrupt(query)
			signed.URL.RawQuery = query.Encode()

			response := httptest.NewRecorder()
			door.ServeHTTP(response, signed)

			if seen.reached {
				t.Fatal("the request reached the server")
			}
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
			}
			// The refusal says it was refused and nothing about which part was
			// wrong, in the shape joaju.Server answers refusals in.
			var answer map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
				t.Fatalf("the refusal is not the JSON the server answers with: %v", err)
			}
			if answer["message"] == "" {
				t.Errorf("the refusal carries no message: %s", response.Body)
			}
		})
	}
}

func TestFrontDoorLetsTheSocketRouteThroughAsTheAnonymousReader(t *testing.T) {
	t.Parallel()

	// The Pusher protocol has no credential on the socket route: a browser
	// arrives with the app key in the path and nothing else, and what it may
	// hear is settled per channel afterwards.
	door, seen := door(t)

	response := httptest.NewRecorder()
	door.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/app/"+testAppKey, nil))

	if !seen.reached {
		t.Fatal("the socket route was refused at the front door")
	}
	if !seen.found {
		t.Fatal("the socket route reached the server with no subject, and it answers 401 to that")
	}
	if !seen.subject.IsGuest() {
		t.Errorf("subject = %+v, want the declared anonymous reader", seen.subject)
	}
	if seen.subject.Tenant != testTenant {
		t.Errorf("tenant = %q, want %q", seen.subject.Tenant, testTenant)
	}
	if seen.subject.HasRole(roleApplication) {
		t.Error("a browser that signed nothing is carrying the application's role")
	}
}

func TestFrontDoorRefusesABodyLargerThanTheServerReads(t *testing.T) {
	t.Parallel()

	door, seen := door(t)
	door.maxBody = 16

	response := httptest.NewRecorder()
	door.ServeHTTP(response, sign(t, http.MethodPost, "/apps/3/events", strings.Repeat("x", 64)))

	if seen.reached {
		t.Fatal("a body over the limit reached the server")
	}
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}
