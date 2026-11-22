package main

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// diff compares two sessions read from the same collector: a is the baseline,
// b the candidate. Everything is derived from the two event lists, so it needs
// no server support.
func (c *cli) diff(ctx context.Context, args []string) error {
	fs := c.flags("diff")
	asJSON := fs.Bool("json", false, "print the differences as one JSON object")
	failOnDiff := fs.Bool("fail-on-diff", false, "exit 3 when the sessions differ")
	ignoreCounts := fs.Bool("ignore-counts", false, "treat a mutation or event present in both sessions as equal whatever its count")
	pos, err := c.parseArgs(fs, args, 2, "<id|name a> <id|name b>")
	if err != nil {
		return err
	}
	var refs [2]sessionRef
	var evs [2][]core.Event
	for i, ref := range pos {
		s, err := c.resolve(ctx, ref)
		if err != nil {
			return err
		}
		info := s.Info()
		if evs[i], err = s.Events(ctx); err != nil {
			return err
		}
		// What the collector knows of lost events (dropped, quarantined) is in its summary, not in the
		// session. A summary that cannot be read does not stop the comparison.
		sum, _ := s.Summary(ctx)
		refs[i] = sessionRef{ID: info.ID, Name: info.Name, Recording: recordingOf(info, sum)}
	}
	rep := diffEvents(evs[0], evs[1], *ignoreCounts)
	rep.A, rep.B = refs[0], refs[1]
	if *asJSON {
		printJSON(c.stdout, rep)
	} else {
		printDiff(c.stdout, rep)
	}
	if *failOnDiff && rep.Differs {
		return checkError{msg: "check failed: sessions differ"}
	}
	return nil
}

// Event names carry the values of one particular run (ids in URL paths, SQL
// literals, Redis keys, job arguments). normalizeName replaces every such
// value with the same placeholder, so the same operation gets the same name
// in every run whatever kind of value it carried. It works on text alone and
// does not parse SQL.
//
// A number that names an outcome is not such a value: "HTTP 500" and
// "HTTP 503" are different failures, although the number rule alone would
// make both "HTTP ?". reOutcome lists those codes; they are compared exactly.
const (
	placeholder    = "?"
	maxNormalized  = 200 // runes kept of a normalised name
	sureHexID      = 16  // a hex string this long is an id even without a digit
	divergeContext = 2   // events shown on each side of a divergence
)

var (
	// A quoted literal opens only after a non-word character, so the
	// apostrophe in "can't" does not start one. A doubled quote inside a
	// literal is an escaped quote.
	reQuoted = regexp.MustCompile(`(^|[^\pL\pN_'])'(?:[^']|'')*'`)
	reUUID   = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	// Session, transaction and span ids: ts_<hex>, tx_<hex>, sp_<hex>.
	reTsessionID = regexp.MustCompile(`\b(?:ts|tx|sp)_[0-9a-fA-F]{8,}\b`)
	// Hex strings from 12 characters on are candidate ids.
	reHex     = regexp.MustCompile(`\b[0-9a-fA-F]{12,}\b`)
	reBindVar = regexp.MustCompile(`\$\d+\b`)
	// A number that is not part of an identifier: 42 and 9.99, but not the
	// digits of orders_2024, t2 or v1.
	reNumber = regexp.MustCompile(`\b\d+(?:\.\d+)?\b`)
	reInList = regexp.MustCompile(`(?i)\bIN\s*\(\s*\?(?:\s*,\s*\?)*\s*\)`)
	// Codes from a small fixed set, kept as they are: an HTTP status after
	// "HTTP", "status" or "StatusCode" ("HTTP 503", "HTTP/1.1 503", "status
	// code: 503"), a SQLSTATE, a database error number ("Error 1062 (23000)",
	// "error code 40001", "mysql error 1213"), a process exit status, and a
	// gRPC code that has no name ("Code(17)", "grpc status 14"; named ones are
	// words already). "status" must be followed by a space or a colon, so the
	// SQL "status = 200" is still a literal. The rule is a guess from the
	// words around a number: "job status: 123 queued" is kept exact too, and
	// "status=503" without a space is not.
	reOutcome = regexp.MustCompile(`(?i)\b(?:HTTP(?:/\d(?:\.\d)?)?(?: status(?: code)?:?)? [1-5]\d\d\b` +
		`|status(?: code)?:? [1-5]\d\d\b` +
		`|StatusCode:? ?[1-5]\d\d\b` +
		`|SQLSTATE[:=]? ?\(?[0-9A-Z]{5}\b` +
		`|Error \d{4} \([0-9A-Z]{5}\)` +
		`|(?:mysql error|error code|exit status|exit code|grpc status) \d+\b` +
		`|(?-i:Code)\(\d+\))`)
)

// Private-use runes stand in for the reOutcome matches while the other rules
// run; no rule touches them.
const (
	outcomeFirst = '\uE000'
	outcomeLast  = '\uF8FF'
)

func isOutcomeMark(r rune) bool { return r >= outcomeFirst && r <= outcomeLast }

func normalizeName(s string) string {
	// Text that already holds a private-use rune is normalised without this
	// step rather than risk restoring the wrong thing.
	var outcomes []string
	if !strings.ContainsFunc(s, isOutcomeMark) {
		var b strings.Builder
		at := 0
		for _, m := range reOutcome.FindAllStringIndex(s, -1) {
			// "http 127.0.0.1": the number goes on, so it is not a code.
			partOfMore := m[1]+1 < len(s) && s[m[1]] == '.' && s[m[1]+1] >= '0' && s[m[1]+1] <= '9'
			if partOfMore || len(outcomes) > outcomeLast-outcomeFirst {
				continue
			}
			b.WriteString(s[at:m[0]])
			b.WriteRune(rune(outcomeFirst + len(outcomes)))
			outcomes = append(outcomes, s[m[0]:m[1]])
			at = m[1]
		}
		b.WriteString(s[at:])
		s = b.String()
	}
	s = reQuoted.ReplaceAllString(s, "${1}"+placeholder)
	s = reUUID.ReplaceAllString(s, placeholder)
	s = reTsessionID.ReplaceAllString(s, placeholder)
	s = reHex.ReplaceAllStringFunc(s, func(m string) string {
		// A shorter one made of letters only is more likely a word.
		if len(m) >= sureHexID || strings.ContainsAny(m, "0123456789") {
			return placeholder
		}
		return m
	})
	s = reBindVar.ReplaceAllString(s, placeholder)
	s = reNumber.ReplaceAllString(s, placeholder)
	if len(outcomes) > 0 {
		// One inside a quoted literal went with the literal.
		s = strings.Map(func(r rune) rune {
			if isOutcomeMark(r) {
				return -1
			}
			return r
		}, restoreOutcomes(s, outcomes))
	}
	s = strings.Join(strings.Fields(s), " ")
	s = reInList.ReplaceAllString(s, "IN (?)")
	if r := []rune(s); len(r) > maxNormalized {
		s = string(r[:maxNormalized-1]) + "…"
	}
	return s
}

// restoreOutcomes puts each kept code back in place of its mark.
func restoreOutcomes(s string, outcomes []string) string {
	var b strings.Builder
	for _, r := range s {
		if i := int(r - outcomeFirst); isOutcomeMark(r) && i < len(outcomes) {
			b.WriteString(outcomes[i])
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

type sessionRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Recording is "incomplete" for a session known to have lost events and
	// absent otherwise. What differs from such a side may be what it has not
	// got, not what the run did.
	Recording string `json:"recording,omitempty"`
}

// recordingOf is the Recording of a session reference.
func recordingOf(info client.Info, sum client.Summary) string {
	if sum.Incomplete || info.Incomplete || len(info.IncompleteReasons) > 0 || info.SealError != "" {
		return "incomplete"
	}
	return ""
}

// diffReport is what differs between two sessions; it is also the --json
// output. Every list is sorted and non-nil.
type diffReport struct {
	A          sessionRef     `json:"a"`
	B          sessionRef     `json:"b"`
	Differs    bool           `json:"differs"`
	Failures   failureDiff    `json:"failures"`
	Mutations  []mutationDiff `json:"mutations"`
	Events     []eventDiff    `json:"events"`
	Divergence []divergence   `json:"divergence"`
}

type failureDiff struct {
	OnlyA []failureGroup `json:"only_a"`
	OnlyB []failureGroup `json:"only_b"`
}

// failureKey identifies a failure across runs; Name and Err are normalised.
type failureKey struct {
	Service string    `json:"service"`
	Kind    core.Kind `json:"kind"`
	Name    string    `json:"name"`
	Err     string    `json:"err"`
}

// failureGroup is a failure one session has more often than the other. Count
// is the surplus; Seqs are all its occurrences in that session.
type failureGroup struct {
	failureKey
	Count int      `json:"count"`
	Seqs  []uint64 `json:"seqs"`
}

type mutationKey struct {
	Store  string `json:"store"`
	Entity string `json:"entity"`
	Op     string `json:"op"`
}

// mutationDiff counts the mutations that took effect (A, B) and those whose
// transaction rolled back.
type mutationDiff struct {
	mutationKey
	A           int `json:"a"`
	B           int `json:"b"`
	RolledBackA int `json:"rolled_back_a"`
	RolledBackB int `json:"rolled_back_b"`
}

// eventKey identifies an event across runs; Name is normalised.
type eventKey struct {
	Service string    `json:"service"`
	Kind    core.Kind `json:"kind"`
	Name    string    `json:"name"`
}

type eventDiff struct {
	eventKey
	A int `json:"a"`
	B int `json:"b"`
}

// divergence is the first position at which one service's event sequence
// differs between the sessions. Position counts from 1 within that service;
// A and B hold the events around it, with their recorded names.
type divergence struct {
	Service  string     `json:"service"`
	Position int        `json:"position"`
	LenA     int        `json:"len_a"`
	LenB     int        `json:"len_b"`
	A        []divEvent `json:"a"`
	B        []divEvent `json:"b"`
	order    uint64     // where it happens in the baseline
}

type divEvent struct {
	Seq      uint64    `json:"seq"`
	Kind     core.Kind `json:"kind"`
	Name     string    `json:"name"`
	Diverges bool      `json:"diverges,omitempty"`
	key      eventKey
}

// counts holds how often a key occurs in session a ([0]) and b ([1]).
type counts[K comparable] map[K]*[2]int

func (c counts[K]) add(k K, side int) {
	if c[k] == nil {
		c[k] = new([2]int)
	}
	c[k][side]++
}

func (c counts[K]) get(k K) [2]int {
	if n := c[k]; n != nil {
		return *n
	}
	return [2]int{}
}

// differ compares two counts; with ignoreCounts only presence matters.
func differ(n [2]int, ignoreCounts bool) bool {
	if ignoreCounts {
		return (n[0] > 0) != (n[1] > 0)
	}
	return n[0] != n[1]
}

func diffEvents(a, b []core.Event, ignoreCounts bool) diffReport {
	rep := diffReport{
		Failures:   failureDiff{OnlyA: []failureGroup{}, OnlyB: []failureGroup{}},
		Mutations:  []mutationDiff{},
		Events:     []eventDiff{},
		Divergence: []divergence{},
	}
	failures, events := counts[failureKey]{}, counts[eventKey]{}
	effective, rolledBack := counts[mutationKey]{}, counts[mutationKey]{}
	var failSeqs [2]map[failureKey][]uint64
	var steps [2]map[string][]divEvent
	for side, evs := range [2][]core.Event{a, b} {
		failSeqs[side], steps[side] = map[failureKey][]uint64{}, map[string][]divEvent{}
		for i := range evs {
			e := &evs[i]
			k := eventKey{Service: e.Service, Kind: e.Kind, Name: normalizeName(e.Name)}
			events.add(k, side)
			steps[side][e.Service] = append(steps[side][e.Service], divEvent{Seq: e.Seq, Kind: e.Kind, Name: e.Name, key: k})
			if e.Err != "" {
				fk := failureKey{Service: e.Service, Kind: e.Kind, Name: k.Name, Err: normalizeName(e.Err)}
				failures.add(fk, side)
				failSeqs[side][fk] = append(failSeqs[side][fk], e.Seq)
			}
			for j := range e.Mutations {
				m := &e.Mutations[j]
				mk := mutationKey{Store: m.Store, Entity: m.Entity, Op: strings.ToLower(m.Op)}
				if m.Effective != nil && !*m.Effective {
					rolledBack.add(mk, side)
				} else {
					effective.add(mk, side)
				}
			}
		}
	}

	for k, n := range failures {
		switch {
		case n[0] > n[1]:
			rep.Failures.OnlyA = append(rep.Failures.OnlyA, failureGroup{k, n[0] - n[1], failSeqs[0][k]})
		case n[1] > n[0]:
			rep.Failures.OnlyB = append(rep.Failures.OnlyB, failureGroup{k, n[1] - n[0], failSeqs[1][k]})
		}
	}
	byFailure := func(x, y failureGroup) int {
		return cmp.Or(cmp.Compare(x.Service, y.Service), cmp.Compare(x.Kind, y.Kind), cmp.Compare(x.Name, y.Name), cmp.Compare(x.Err, y.Err))
	}
	slices.SortFunc(rep.Failures.OnlyA, byFailure)
	slices.SortFunc(rep.Failures.OnlyB, byFailure)

	mutKeys := map[mutationKey]bool{}
	for k := range effective {
		mutKeys[k] = true
	}
	for k := range rolledBack {
		mutKeys[k] = true
	}
	for k := range mutKeys {
		eff, rb := effective.get(k), rolledBack.get(k)
		if differ(eff, ignoreCounts) || differ(rb, ignoreCounts) {
			rep.Mutations = append(rep.Mutations, mutationDiff{k, eff[0], eff[1], rb[0], rb[1]})
		}
	}
	slices.SortFunc(rep.Mutations, func(x, y mutationDiff) int {
		return cmp.Or(cmp.Compare(x.Store, y.Store), cmp.Compare(x.Entity, y.Entity), cmp.Compare(x.Op, y.Op))
	})

	for k, n := range events {
		if differ(*n, ignoreCounts) {
			rep.Events = append(rep.Events, eventDiff{k, n[0], n[1]})
		}
	}
	slices.SortFunc(rep.Events, func(x, y eventDiff) int {
		return cmp.Or(cmp.Compare(x.Service, y.Service), cmp.Compare(x.Kind, y.Kind), cmp.Compare(x.Name, y.Name))
	})

	services := map[string]bool{}
	for side := range steps {
		for svc := range steps[side] {
			services[svc] = true
		}
	}
	for svc := range services {
		if d, ok := diverge(svc, steps[0][svc], steps[1][svc], ignoreCounts); ok {
			rep.Divergence = append(rep.Divergence, d)
		}
	}
	slices.SortFunc(rep.Divergence, func(x, y divergence) int {
		return cmp.Or(cmp.Compare(x.order, y.order), cmp.Compare(x.Service, y.Service))
	})

	rep.Differs = len(rep.Failures.OnlyA)+len(rep.Failures.OnlyB)+len(rep.Mutations)+len(rep.Events)+len(rep.Divergence) > 0
	return rep
}

// diverge finds the first position at which one service's sequences differ.
// With ignoreCounts a run of consecutive identical events counts as one, so
// an immediate retry does not shift everything after it.
func diverge(service string, a, b []divEvent, ignoreCounts bool) (divergence, bool) {
	if ignoreCounts {
		same := func(x, y divEvent) bool { return x.key == y.key }
		a, b = slices.CompactFunc(slices.Clone(a), same), slices.CompactFunc(slices.Clone(b), same)
	}
	i := 0
	for i < len(a) && i < len(b) && a[i].key == b[i].key {
		i++
	}
	if i == len(a) && i == len(b) {
		return divergence{}, false
	}
	d := divergence{Service: service, Position: i + 1, LenA: len(a), LenB: len(b), A: around(a, i), B: around(b, i)}
	switch {
	case i < len(a):
		d.order = a[i].Seq * 2
	case len(a) > 0:
		d.order = a[len(a)-1].Seq*2 + 1 // just after the baseline's last event
	default:
		d.order = math.MaxUint64 // the baseline never ran this service
	}
	return d, true
}

// around returns the events within divergeContext of position i, marking the
// one at i. The result is non-nil.
func around(s []divEvent, i int) []divEvent {
	out := []divEvent{}
	for j := max(0, i-divergeContext); j < min(len(s), i+divergeContext+1); j++ {
		e := s[j]
		e.Diverges = j == i
		out = append(out, e)
	}
	return out
}

func printDiff(w io.Writer, rep diffReport) {
	out := bufio.NewWriter(w)
	defer out.Flush()
	fmt.Fprintf(out, "a  %s  %s\nb  %s  %s\n", rep.A.ID, rep.A.Name, rep.B.ID, rep.B.Name)
	for _, side := range []struct{ label, recording string }{{"a", rep.A.Recording}, {"b", rep.B.Recording}} {
		if side.recording == "incomplete" {
			fmt.Fprintf(out, "note: %s is incomplete: what it lacks may be events that were lost, not what the run did\n", side.label)
		}
	}
	if !rep.Differs {
		fmt.Fprintln(out, "no differences")
		return
	}
	for _, side := range []struct {
		label string
		list  []failureGroup
	}{{"a", rep.Failures.OnlyA}, {"b", rep.Failures.OnlyB}} {
		if len(side.list) == 0 {
			continue
		}
		fmt.Fprintf(out, "\nfailures only in %s:\n", side.label)
		for _, f := range side.list {
			fmt.Fprintf(out, "  %dx %s %s %s: %s  (%s)\n", f.Count, f.Service, f.Kind, clip(f.Name, 70), clip(f.Err, 120), joinSeqs(f.Seqs))
		}
	}
	if len(rep.Mutations) > 0 {
		fmt.Fprintln(out, "\nmutations that took effect (a -> b):")
		for _, m := range rep.Mutations {
			line := fmt.Sprintf("  %s/%s %s  %d -> %d", m.Store, m.Entity, m.Op, m.A, m.B)
			if m.RolledBackA != m.RolledBackB {
				line += fmt.Sprintf("  (rolled back %d -> %d)", m.RolledBackA, m.RolledBackB)
			}
			fmt.Fprintln(out, line)
		}
	}
	if len(rep.Events) > 0 {
		fmt.Fprintln(out, "\nevents (a -> b):")
		for _, e := range rep.Events {
			fmt.Fprintf(out, "  %s %s %s  %d -> %d\n", e.Service, e.Kind, clip(e.Name, 90), e.A, e.B)
		}
	}
	for i, d := range rep.Divergence {
		label := "also diverges"
		if i == 0 {
			label = "first divergence"
		}
		fmt.Fprintf(out, "\n%s: service %s, position %d (a has %d events, b has %d)\n", label, d.Service, d.Position, d.LenA, d.LenB)
		printDivSide(out, "a", d.A)
		printDivSide(out, "b", d.B)
	}
}

func printDivSide(w io.Writer, label string, evs []divEvent) {
	marked := false
	for _, e := range evs {
		mark := " "
		if e.Diverges {
			mark, marked = ">", true
		}
		fmt.Fprintf(w, "  %s%s #%d %s %s\n", label, mark, e.Seq, e.Kind, clip(e.Name, 90))
	}
	if !marked {
		fmt.Fprintf(w, "  %s> (no more events)\n", label)
	}
}

// joinSeqs renders "#4, #9", shortened after a few entries.
func joinSeqs(seqs []uint64) string {
	const show = 5
	parts := slicex.Map(seqs[:min(len(seqs), show)], func(s uint64) string { return fmt.Sprintf("#%d", s) })
	if len(seqs) > show {
		parts = append(parts, fmt.Sprintf("+%d more", len(seqs)-show))
	}
	return strings.Join(parts, ", ")
}
