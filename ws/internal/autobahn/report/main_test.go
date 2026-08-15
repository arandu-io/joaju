package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestPassRule pins the rule the run's exit code comes from. A harness that
// reports green on a failing case is worse than no harness, and this is the one
// function standing between the two.
func TestPassRule(t *testing.T) {
	for _, tc := range []struct {
		name     string
		behavior string
		close    string
		want     bool
	}{
		{"clean", "OK", "OK", true},
		{"non-strict is a pass", "NON-STRICT", "OK", true},
		{"close not scored", "OK", "INFORMATIONAL", true},
		{"lower case from the suite", "ok", "informational", true},
		{"failed", "FAILED", "OK", false},
		{"wrong close code", "OK", "WRONG CODE", false},
		{"unclean close", "OK", "UNCLEAN", false},
		{"unimplemented is not a pass", "UNIMPLEMENTED", "OK", false},
		{"missing fields", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := passing(caseResult{Behavior: tc.behavior, BehaviorClose: tc.close})
			if got != tc.want {
				t.Errorf("passing(%q, %q) = %v, want %v", tc.behavior, tc.close, got, tc.want)
			}
		})
	}
}

// TestRunCountsAndFails walks a reports directory the shape the suite writes
// one, and checks that the verdict, the counts and the failure detail all come
// out of it.
func TestRunCountsAndFails(t *testing.T) {
	dir := t.TempDir()
	writeIndex(t, dir, map[string]caseResultInput{
		"1.1.1":  {Behavior: "OK", BehaviorClose: "OK", CloseCode: "1000"},
		"1.1.2":  {Behavior: "NON-STRICT", BehaviorClose: "INFORMATIONAL", CloseCode: "1000"},
		"7.9.1":  {Behavior: "FAILED", BehaviorClose: "WRONG CODE", CloseCode: "1000", ReportFile: "case_7_9_1.json"},
		"10.1.1": {Behavior: "OK", BehaviorClose: "OK", CloseCode: "null"},
	})
	writeDetail(t, dir, "case_7_9_1.json", caseDetail{
		Description: "Send close with <b>invalid</b> close code 0.",
		Expectation: "Clean close with protocol error<br>code 1002.",
		Result:      "Actual close code 1000.",
	})

	text, failures, _, err := run(dir)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if failures != 1 {
		t.Fatalf("failures = %d, want 1", failures)
	}
	for _, want := range []string{
		"cases run: 4",
		"failures: 1",
		"7.9.1  behavior=FAILED close=WRONG CODE remoteCloseCode=1000",
		"Send close with invalid close code 0.",
		"Clean close with protocol error code 1002.",
		"FAIL: 4 cases, 1 failures",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("report is missing %q\n---\n%s", want, text)
		}
	}
	// The scoreboard counts every behaviour the suite reported, not just the
	// failing ones: two OK, one NON-STRICT, one FAILED.
	for _, want := range []string{"OK             2", "NON-STRICT     1", "FAILED         1"} {
		if !strings.Contains(text, want) {
			t.Errorf("report is missing the count line %q\n---\n%s", want, text)
		}
	}
	// A count of three non-strict cases without their numbers sends a reader
	// back to the suite's own index to find out which three.
	if !strings.Contains(text, "non-strict (1.1.2)") {
		t.Errorf("the non-strict case was not named\n---\n%s", text)
	}
}

// TestFailureWithoutCloseCode covers the row the suite writes when the peer
// sent no close frame at all, which is a finding and not a missing field.
func TestFailureWithoutCloseCode(t *testing.T) {
	dir := t.TempDir()
	writeIndex(t, dir, map[string]caseResultInput{
		"6.4.1": {Behavior: "FAILED", BehaviorClose: "UNCLEAN", CloseCode: "null"},
	})

	text, failures, _, err := run(dir)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if failures != 1 {
		t.Fatalf("failures = %d, want 1", failures)
	}
	if !strings.Contains(text, "remoteCloseCode=none") {
		t.Errorf("a case with no close frame did not say so\n---\n%s", text)
	}
	if !strings.Contains(text, "(no case detail:") {
		t.Errorf("a case with no report file did not say so\n---\n%s", text)
	}
}

// TestRunPasses checks that a clean run says so and reports no failures, which
// is the case that decides whether the script exits zero.
//
// It includes a case the suite does not score, because that is what a clean run
// of this suite actually looks like: 7.1.6, 7.13.1 and 7.13.2 come back
// INFORMATIONAL every time.
func TestRunPasses(t *testing.T) {
	dir := t.TempDir()
	writeIndex(t, dir, map[string]caseResultInput{
		"1.1.1":  {Behavior: "OK", BehaviorClose: "OK", CloseCode: "1000"},
		"2.1.1":  {Behavior: "OK", BehaviorClose: "INFORMATIONAL", CloseCode: "null"},
		"7.13.1": {Behavior: "INFORMATIONAL", BehaviorClose: "INFORMATIONAL", CloseCode: "1002"},
	})

	text, failures, _, err := run(dir)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if failures != 0 {
		t.Fatalf("failures = %d, want 0", failures)
	}
	if !strings.Contains(text, "PASS: 3 cases, 0 failures") {
		t.Errorf("report does not say it passed\n---\n%s", text)
	}
	if strings.Contains(text, "failing cases") {
		t.Errorf("a clean run listed failing cases\n---\n%s", text)
	}
	// An unscored case is a pass, but it may never become invisible.
	if !strings.Contains(text, "not scored by the suite (7.13.1)") {
		t.Errorf("the unscored case was not named\n---\n%s", text)
	}
}

// TestRunMissingIndex is the difference between a run that failed and a run
// that never happened. Exit code 2 rather than 1 depends on the error.
func TestRunMissingIndex(t *testing.T) {
	if _, _, _, err := run(t.TempDir()); err == nil {
		t.Fatal("run on an empty directory returned no error")
	}
}

// TestCaseNumberOrder pins the ordering, because a failure list sorted as text
// puts 1.1.10 before 1.1.2 and cannot be read against the suite's index.
func TestCaseNumberOrder(t *testing.T) {
	ids := []string{"10.1.1", "1.1.2", "9.1.10", "2.1.1", "1.1.10", "9.1.2", "1.1.1"}
	want := []string{"1.1.1", "1.1.2", "1.1.10", "2.1.1", "9.1.2", "9.1.10", "10.1.1"}

	sort.Sort(byCaseNumber(ids))
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order = %v, want %v", ids, want)
		}
	}
}

// TestFlatten covers the markup the suite puts in prose written for its HTML
// report.
func TestFlatten(t *testing.T) {
	got := flatten("Send a <b>text</b> message<br>with\n a\tsplit  reason.")
	if want := "Send a text message with a split reason."; got != want {
		t.Errorf("flatten = %q, want %q", got, want)
	}
}

// TestCloseCode covers the null the suite writes when no close frame arrived.
func TestCloseCode(t *testing.T) {
	for raw, want := range map[string]string{"1002": "1002", "null": "none", "": "none"} {
		if got := closeCode(json.RawMessage(raw)); got != want {
			t.Errorf("closeCode(%q) = %q, want %q", raw, got, want)
		}
	}
}

// caseResultInput mirrors the index rows the suite writes, with the close code
// as text so that a test can write null.
type caseResultInput struct {
	Behavior      string `json:"behavior"`
	BehaviorClose string `json:"behaviorClose"`
	CloseCode     string `json:"-"`
	ReportFile    string `json:"reportfile,omitempty"`
}

func writeIndex(t *testing.T, dir string, cases map[string]caseResultInput) {
	t.Helper()

	var b strings.Builder
	b.WriteString(`{"joaju/ws":{`)
	first := true
	for id, c := range cases {
		if !first {
			b.WriteString(",")
		}
		first = false
		code := c.CloseCode
		if code == "" {
			code = "null"
		}
		b.WriteString(`"` + id + `":{"behavior":"` + c.Behavior +
			`","behaviorClose":"` + c.BehaviorClose +
			`","remoteCloseCode":` + code +
			`,"reportfile":"` + c.ReportFile + `"}`)
	}
	b.WriteString("}}")

	if err := os.WriteFile(filepath.Join(dir, "index.json"), []byte(b.String()), 0o600); err != nil {
		t.Fatalf("writing the index: %v", err)
	}
}

func writeDetail(t *testing.T, dir, name string, detail caseDetail) {
	t.Helper()

	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshalling the case detail: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
		t.Fatalf("writing the case detail: %v", err)
	}
}
