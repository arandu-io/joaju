// Command report turns an Autobahn TestSuite run into a verdict.
//
// The suite writes one JSON file per case and an index.json that names them.
// Read by hand that is a few hundred files; read by a browser it is a page that
// nothing in a pipeline can fail on. This reads the index, prints a scoreboard
// and exits non-zero when any case is not OK or NON-STRICT, which is what makes
// the run a check rather than a report.
//
// Usage:
//
//	go run ./ws/internal/autobahn/report -reports <dir> [-out REPORT.txt]
//	    [-context <preamble>] [-min-cases <n>]
//
// -min-cases is the guard against a truncated run. The suite exits zero when it
// gives up on a connection halfway through, having written a green result for
// every case it did reach, and that is indistinguishable from a pass without
// knowing how many cases there should have been.
//
// It is the only place the pass rule lives, and the rule is:
//
//   - behavior must be OK or NON-STRICT. NON-STRICT is a case the suite would
//     have preferred to see handled another way and does not consider wrong;
//     the reference implementations it ships against sit there too.
//   - behaviorClose must be OK or INFORMATIONAL. INFORMATIONAL is the suite
//     saying this case does not score the closing handshake, which is most of
//     the echo groups; treating it as a failure would fail a clean run.
//   - a behavior of INFORMATIONAL is not scored at all. The suite marks a
//     handful of cases that way -- 7.1.6, 7.13.1 and 7.13.2 -- because their
//     expectation text says the outcome "depends on implementation defined
//     close behavior": there is no right answer to compare against, so there is
//     nothing to fail. They are counted and listed on their own so that the
//     count cannot quietly grow.
//
// Anything else is a failure and is printed with what the case asked for.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func main() {
	reports := flag.String("reports", "reports", "directory holding the suite's index.json")
	out := flag.String("out", "", "file to write the report to, in addition to stdout")
	context := flag.String("context", "", "lines describing the run, printed above the scoreboard")
	minCases := flag.Int("min-cases", 0, "fail if the suite ran fewer cases than this")
	flag.Parse()

	text, failed, ran, err := run(*reports)
	if err != nil {
		fmt.Fprintf(os.Stderr, "autobahn-report: %v\n", err)
		os.Exit(2)
	}
	text = *context + text

	// A run that stopped early is not a pass, and without this it looks like
	// one: the suite exits zero when it gives up on a connection, and every
	// case it did reach can be green. The first run of this harness stopped at
	// 7.3.6 having reported 221 of 301 cases, all of them OK.
	if *minCases > 0 && ran < *minCases {
		text += fmt.Sprintf("\nINCOMPLETE: the suite reported %d cases and %d were expected.\n"+
			"It stopped early, so the cases it never reached are unmeasured rather\n"+
			"than passing, and this run is not a result.\n", ran, *minCases)
		failed++
	}

	fmt.Print(text)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(text), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "autobahn-report: %v\n", err)
			os.Exit(2)
		}
	}

	if failed > 0 {
		os.Exit(1)
	}
}

// caseResult is one row of the suite's index.json.
//
// The suite writes more fields than these; what is decoded is what the verdict
// and the failure lines need. RemoteCloseCode is json.RawMessage because the
// suite writes a number when it got a close code and null when it did not, and
// null is itself a finding worth printing.
type caseResult struct {
	Behavior        string          `json:"behavior"`
	BehaviorClose   string          `json:"behaviorClose"`
	RemoteCloseCode json.RawMessage `json:"remoteCloseCode"`
	ReportFile      string          `json:"reportfile"`
}

// caseDetail is the part of a per-case report file that says what was being
// asked for. It is only read for cases that failed, because it is the answer to
// the only question a failure raises.
//
// The suite's own "case" field is its internal index, a number, and is not
// decoded: the identifier a reader wants is the dotted one, and that is the key
// in the index. Declaring it as a string here made every failure print a
// decoding error instead of its expectation.
type caseDetail struct {
	Description string `json:"description"`
	Expectation string `json:"expectation"`
	Result      string `json:"result"`
}

// passingBehavior, unscored and passingClose are the pass rule. See the package
// comment for what each of the three words means to the suite.
var (
	passingBehavior = map[string]bool{"OK": true, "NON-STRICT": true}
	unscored        = map[string]bool{"INFORMATIONAL": true}
	passingClose    = map[string]bool{"OK": true, "INFORMATIONAL": true}
)

// run reads the index and builds the report text, returning how many cases
// failed and how many the suite reported at all.
func run(dir string) (text string, failures, total int, err error) {
	raw, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return "", 0, 0, fmt.Errorf("cannot read the suite index: %w", err)
	}

	// The index is keyed by agent name, and there is one agent here. Decoding
	// into a map rather than a struct is what keeps the agent renameable in
	// fuzzingclient.json without touching this.
	var index map[string]map[string]caseResult
	if err := json.Unmarshal(raw, &index); err != nil {
		return "", 0, 0, fmt.Errorf("cannot parse the suite index: %w", err)
	}

	var b strings.Builder

	agents := make([]string, 0, len(index))
	for agent := range index {
		agents = append(agents, agent)
	}
	sort.Strings(agents)

	for _, agent := range agents {
		cases := index[agent]
		ids := make([]string, 0, len(cases))
		for id := range cases {
			ids = append(ids, id)
		}
		sort.Sort(byCaseNumber(ids))

		counts := map[string]int{}
		var failed, notScored, nonStrict []string
		for _, id := range ids {
			behavior := strings.ToUpper(cases[id].Behavior)
			counts[behavior]++
			switch {
			case unscored[behavior]:
				notScored = append(notScored, id)
			case !passing(cases[id]):
				failed = append(failed, id)
			case behavior == "NON-STRICT":
				nonStrict = append(nonStrict, id)
			}
		}

		total += len(ids)
		failures += len(failed)

		fmt.Fprintf(&b, "agent: %s\n", agent)
		fmt.Fprintf(&b, "cases run: %d\n", len(ids))
		for _, behavior := range sortedKeys(counts) {
			fmt.Fprintf(&b, "  %-14s %d\n", behavior, counts[behavior])
		}
		fmt.Fprintf(&b, "failures: %d\n", len(failed))

		if len(nonStrict) > 0 {
			fmt.Fprintf(&b, "\nnon-strict (%s)\n", strings.Join(nonStrict, ", "))
			b.WriteString("  a pass. The suite got the outcome it allows rather than the one it\n")
			b.WriteString("  prefers -- on the 6.4 cases that is failing an invalid text payload\n")
			b.WriteString("  at the end of the frame carrying it instead of at the bad octet.\n")
		}

		if len(notScored) > 0 {
			fmt.Fprintf(&b, "\nnot scored by the suite (%s)\n", strings.Join(notScored, ", "))
			b.WriteString("  these cases say their outcome depends on implementation defined\n")
			b.WriteString("  close behaviour, so the suite compares them against nothing.\n")
		}

		if len(failed) > 0 {
			b.WriteString("\nfailing cases\n")
			for _, id := range failed {
				writeFailure(&b, dir, id, cases[id])
			}
		}
		b.WriteString("\n")
	}

	verdict := "PASS"
	if failures > 0 {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "%s: %d cases, %d failures\n", verdict, total, failures)

	return b.String(), failures, total, nil
}

// passing applies the pass rule to one case.
func passing(c caseResult) bool {
	return passingBehavior[strings.ToUpper(c.Behavior)] && passingClose[strings.ToUpper(c.BehaviorClose)]
}

// writeFailure prints one failing case with what the suite wanted from it.
func writeFailure(b *strings.Builder, dir, id string, c caseResult) {
	fmt.Fprintf(b, "  %s  behavior=%s close=%s remoteCloseCode=%s\n",
		id, or(c.Behavior, "?"), or(c.BehaviorClose, "?"), closeCode(c.RemoteCloseCode))

	detail, err := readDetail(dir, c.ReportFile)
	if err != nil {
		// A missing per-case file is not worth failing the report over: the
		// verdict came from the index and is already correct without it.
		fmt.Fprintf(b, "      (no case detail: %v)\n", err)

		return
	}
	for _, line := range []struct{ label, text string }{
		{"description", detail.Description},
		{"expectation", detail.Expectation},
		{"result", detail.Result},
	} {
		if text := flatten(line.text); text != "" {
			fmt.Fprintf(b, "      %-11s %s\n", line.label+":", text)
		}
	}
}

// readDetail loads the per-case report the index points at.
func readDetail(dir, file string) (caseDetail, error) {
	if file == "" {
		return caseDetail{}, fmt.Errorf("the index names no report file")
	}

	// The name comes from the suite's own index, but it is still a path built
	// from a file, so it is confined to the reports directory.
	f, err := os.Open(filepath.Join(dir, filepath.Base(file)))
	if err != nil {
		return caseDetail{}, err
	}
	defer func() { _ = f.Close() }()

	raw, err := io.ReadAll(f)
	if err != nil {
		return caseDetail{}, err
	}

	var detail caseDetail
	if err := json.Unmarshal(raw, &detail); err != nil {
		return caseDetail{}, err
	}

	return detail, nil
}

// flatten turns the suite's HTML-flavoured prose into one line of text.
//
// The description and expectation fields carry <br>, <b> and newlines because
// they are written for the HTML report. Left alone they break the alignment of
// every failure block.
func flatten(s string) string {
	replacer := strings.NewReplacer(
		"<br>", " ", "<br/>", " ", "<br />", " ",
		"<b>", "", "</b>", "", "<i>", "", "</i>", "",
		"<pre>", " ", "</pre>", " ", "&nbsp;", " ",
		"\n", " ", "\r", " ", "\t", " ",
	)

	return strings.Join(strings.Fields(replacer.Replace(s)), " ")
}

// closeCode renders the remoteCloseCode field, which is null when the peer sent
// no close frame.
func closeCode(raw json.RawMessage) string {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return "none"
	}

	return text
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}

	return s
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

// byCaseNumber orders case identifiers the way the suite numbers them, so that
// 9.1.2 comes before 9.1.10 and group 10 comes after group 9.
//
// Sorting them as strings puts 1.1.10 before 1.1.2 and 10.1.1 before 2.1.1,
// which makes a failure list impossible to read against the suite's own index.
type byCaseNumber []string

func (s byCaseNumber) Len() int      { return len(s) }
func (s byCaseNumber) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s byCaseNumber) Less(i, j int) bool {
	a, b := strings.Split(s[i], "."), strings.Split(s[j], ".")
	for k := 0; k < len(a) && k < len(b); k++ {
		x, errX := strconv.Atoi(a[k])
		y, errY := strconv.Atoi(b[k])
		if errX != nil || errY != nil {
			// A segment that is not a number: fall back to comparing the whole
			// identifier as text rather than guessing.
			if a[k] != b[k] {
				return a[k] < b[k]
			}

			continue
		}
		if x != y {
			return x < y
		}
	}

	return len(a) < len(b)
}
