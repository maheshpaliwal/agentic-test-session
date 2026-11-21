package collector

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"net/url"
	"time"
)

const (
	// maxListIDs is the most session ids one GET /v1/sessions?ids= may name.
	maxListIDs = 1000
	// listLookups is how many of a request's ids are looked up in the store
	// at the same time.
	listLookups = 8
	// listLookupTimeout bounds the store calls for one id.
	listLookupTimeout = 5 * time.Second
	// headerUnanswered is set on every answer to an ids request: the number
	// of requested ids left out because the store failed for them or the
	// request ended first. An id missing from the body means "no events held"
	// only when this is 0.
	headerUnanswered = "X-Tsession-Unanswered"
)

// sessionCount is one element of the answer to GET /v1/sessions?ids=: the
// part of store.SessionInfo that costs a point lookup per session. first_ns
// and last_ns are left out because no store answers them without reading the
// session's events (or, on DynamoDB, without a method Store does not have).
type sessionCount struct {
	ID         string `json:"id"`
	EventCount int    `json:"event_count"`
	Sealed     bool   `json:"sealed"`
}

func (c *Collector) handleSessions(w http.ResponseWriter, r *http.Request) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		// r.URL.Query() would drop what it cannot parse, and a damaged ids
		// parameter would then be answered with every session.
		writeErr(w, http.StatusBadRequest, fmt.Errorf("query: %w", err))
		return
	}
	if raw, ok := q["ids"]; ok {
		ids, err := parseListIDs(raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		out, unanswered := c.sessionCounts(r.Context(), ids)
		w.Header().Set(headerUnanswered, strconv.Itoa(unanswered))
		writeJSON(w, http.StatusOK, out)
		return
	}
	l, err := c.st.ListSessions(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if l == nil {
		l = []store.SessionInfo{}
	}
	writeJSON(w, http.StatusOK, l)
}

// parseListIDs reads the ids parameter: comma-separated session ids, in any
// number of ids= values. Empty elements are skipped and a repeated id counts
// once; the order of first appearance is kept.
func parseListIDs(vals []string) ([]string, error) {
	var ids []string
	seen := map[string]bool{}
	for _, v := range vals {
		for id := range strings.SplitSeq(v, ",") {
			if id == "" || seen[id] {
				continue
			}
			if !validID.MatchString(id) {
				return nil, fmt.Errorf("ids: %w", errBadID)
			}
			if len(ids) == maxListIDs {
				return nil, fmt.Errorf("ids: more than %d session ids", maxListIDs)
			}
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// sessionCounts answers for each of ids that has stored events, in the order
// given, with one Head (and, for a session that has events, one GetSeal) per
// id: it never lists the sessions and never reads events. Lookups run
// listLookups at a time and stop when ctx ends. unanswered counts the ids
// that could not be looked up; they are missing from the result.
func (c *Collector) sessionCounts(ctx context.Context, ids []string) (out []sessionCount, unanswered int) {
	type answer struct {
		sc     sessionCount
		held   bool
		failed bool
	}
	res := make([]answer, len(ids))
	var (
		next     atomic.Int64
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	lookup := func(id string) (sc sessionCount, held bool, err error) {
		// These run on goroutines of their own: a panic in the store would
		// end the process instead of the one request, as it does elsewhere.
		defer func() {
			if r := recover(); r != nil {
				sc, held, err = sessionCount{}, false, fmt.Errorf("store panicked: %v", r)
			}
		}()
		// A bound of their own: a store that hangs must not keep the workers
		// of every poll until its client gives up.
		ctx, cancel := context.WithTimeout(ctx, listLookupTimeout)
		defer cancel()
		head, err := c.st.Head(ctx, id)
		if err != nil || head == 0 {
			return sessionCount{}, false, err
		}
		si, err := c.st.GetSeal(ctx, id)
		if err != nil {
			return sessionCount{}, false, err
		}
		// Seq runs gapless from 1 (store.Store), so the head is the count.
		return sessionCount{ID: id, EventCount: int(head), Sealed: si != nil}, true, nil
	}
	for range min(listLookups, len(ids)) {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(ids) {
					return
				}
				if ctx.Err() != nil {
					res[i].failed = true
					continue
				}
				sc, held, err := lookup(ids[i])
				if err != nil {
					res[i].failed = true
					errOnce.Do(func() { firstErr = fmt.Errorf("session %s: %w", ids[i], err) })
					continue
				}
				res[i].sc, res[i].held = sc, held
			}
		})
	}
	wg.Wait()
	out = make([]sessionCount, 0, len(ids))
	for i := range res {
		switch {
		case res[i].failed:
			unanswered++
		case res[i].held:
			out = append(out, res[i].sc)
		}
	}
	if firstErr != nil && ctx.Err() == nil {
		c.log.Warn("session list by id: store lookups failed", "unanswered", unanswered, "asked", len(ids), "err", firstErr)
	}
	return out, unanswered
}
