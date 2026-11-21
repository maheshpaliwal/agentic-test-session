package controlplane

import (
	"encoding/base64"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MaxPageLimit is the largest PageQuery.Limit.
const MaxPageLimit = 1000

// PageQuery selects sessions for Manager.ListPage and PageLister.
type PageQuery struct {
	// State and Name keep only the sessions in that state and with exactly
	// that name; empty means any.
	State State
	Name  string
	// Limit is the page size, 1..MaxPageLimit: the result is then the newest
	// matching sessions, newest first. 0 asks for every matching session,
	// oldest first (the order of Store.List).
	Limit int
	// Before is Page.Next of the previous page: only sessions older than the
	// last one of that page are returned. It needs a Limit.
	Before string
}

// Page is the answer to a PageQuery.
type Page struct {
	Sessions []Session
	// Next is the cursor of the following page (PageQuery.Before); empty when
	// there are no older sessions. Only set for a query with a Limit.
	Next string
	// Total is the number of sessions matching State and Name, whatever Limit
	// and Before are, or -1 when the store cannot tell without reading every
	// candidate (FileStore: a query by Name with a Limit).
	Total int
	// Unreadable is the number of sessions that belong in Sessions and could
	// not be read by the store (see CheckedStore); they are missing from it.
	Unreadable int
}

// PageLister is an optional Store capability: listing one page of sessions
// without producing all of them. A Manager lists and slices for a store
// that does not have it.
//
// Sessions are ordered by creation time, then id, and a cursor names a place
// in that order, not a count of sessions. Paging with Next therefore returns
// no session twice, whatever is put, evicted or deleted meanwhile, as long as
// a session keeps its CreatedAt (a Manager never changes it). A session that
// is in the store and matches the query during the whole run is returned
// exactly once. One that is created, deleted, or starts or stops matching
// during the run may or may not be returned. A page can hold fewer sessions
// than Limit while Next is set (sessions that could not be read, or that
// changed while the page was read).
type PageLister interface {
	// ListPage returns an error wrapping ErrInvalid for a query that
	// validate rejects.
	ListPage(PageQuery) (Page, error)
}

// pageKey is the place of a session in the listing order.
type pageKey struct {
	created int64 // createdNanos of CreatedAt
	id      string
}

func keyOf(s Session) pageKey { return pageKey{createdNanos(s.CreatedAt), s.ID} }

func (k pageKey) older(o pageKey) bool {
	return k.created < o.created || k.created == o.created && k.id < o.id
}

var (
	minNanoTime = time.Unix(0, math.MinInt64)
	maxNanoTime = time.Unix(0, math.MaxInt64)
)

// createdNanos is t in nanoseconds since 1970, which is what the listing
// order and the archive index keep of CreatedAt. Times outside the years
// 1678 to 2262 (the zero time among them) count as the nearest end.
func createdNanos(t time.Time) int64 {
	switch {
	case t.Before(minNanoTime):
		return math.MinInt64
	case t.After(maxNanoTime):
		return math.MaxInt64
	}
	return t.UnixNano()
}

// encodeCursor is the Page.Next that continues after the session at k.
func encodeCursor(k pageKey) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(k.created, 10) + "." + k.id))
}

func decodeCursor(c string) (pageKey, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return pageKey{}, err
	}
	num, id, ok := strings.Cut(string(b), ".")
	if !ok {
		return pageKey{}, fmt.Errorf("no separator")
	}
	created, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return pageKey{}, err
	}
	return pageKey{created, id}, nil
}

// cursor validates q and decodes q.Before. has is false when there is none.
func (q PageQuery) cursor() (k pageKey, has bool, err error) {
	if q.Limit < 0 || q.Limit > MaxPageLimit {
		return k, false, fmt.Errorf("%w: limit must be 1..%d", ErrInvalid, MaxPageLimit)
	}
	if q.Before == "" {
		return k, false, nil
	}
	if q.Limit == 0 {
		return k, false, fmt.Errorf("%w: before needs a limit", ErrInvalid)
	}
	if k, err = decodeCursor(q.Before); err != nil {
		return k, false, fmt.Errorf("%w: before is not a cursor returned by this API", ErrInvalid)
	}
	return k, true, nil
}

func (q PageQuery) matches(s Session) bool {
	return (q.State == "" || s.State == q.State) && (q.Name == "" || s.Name == q.Name)
}

// pageCand is a session that may belong on a page: its place in the order
// and, for an archived one in a FileStore, its index entry.
type pageCand struct {
	pageKey
	ref archRef
	hot bool
}

// newest keeps the n newest of the candidates added to it, without holding
// the others: a min-heap whose root is the oldest one kept.
type newest struct {
	n int
	h []pageCand
}

func (t *newest) add(c pageCand) {
	h := t.h
	if len(h) < t.n {
		h = append(h, c)
		for i := len(h) - 1; i > 0; {
			p := (i - 1) / 2
			if !h[i].older(h[p].pageKey) {
				break
			}
			h[i], h[p] = h[p], h[i]
			i = p
		}
		t.h = h
		return
	}
	if len(h) == 0 || !h[0].older(c.pageKey) {
		return
	}
	h[0] = c
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(h) && h[l].older(h[m].pageKey) {
			m = l
		}
		if r < len(h) && h[r].older(h[m].pageKey) {
			m = r
		}
		if m == i {
			break
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
}

// sorted returns the kept candidates, newest first. The heap is used up.
func (t *newest) sorted() []pageCand {
	sortNewestFirst(t.h)
	return t.h
}

func sortNewestFirst(c []pageCand) {
	slices.SortFunc(c, func(a, b pageCand) int {
		switch {
		case b.older(a.pageKey):
			return -1
		case a.older(b.pageKey):
			return 1
		}
		return 0
	})
}

// pageOf answers q from all, the sessions matching q.State and q.Name in the
// order of Store.List.
func pageOf(all []Session, q PageQuery, cur pageKey, hasCur bool) Page {
	if q.Limit <= 0 {
		return Page{Sessions: all, Total: len(all)}
	}
	p := Page{Total: len(all), Sessions: make([]Session, 0, min(len(all), q.Limit))}
	for i := len(all) - 1; i >= 0; i-- {
		if hasCur && !keyOf(all[i]).older(cur) {
			continue
		}
		if len(p.Sessions) == q.Limit {
			p.Next = encodeCursor(keyOf(p.Sessions[q.Limit-1]))
			break
		}
		p.Sessions = append(p.Sessions, all[i])
	}
	return p
}

// ListPage implements PageLister.
func (s *MemStore) ListPage(q PageQuery) (Page, error) {
	cur, hasCur, err := q.cursor()
	if err != nil {
		return Page{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if q.Limit == 0 {
		out := make([]Session, 0, len(s.m))
		for _, x := range s.m {
			if q.matches(x) {
				out = append(out, x)
			}
		}
		sortSessions(out)
		return Page{Sessions: out, Total: len(out)}, nil
	}
	top, total := newest{n: q.Limit + 1}, 0
	for _, x := range s.m {
		if !q.matches(x) {
			continue
		}
		total++
		if k := keyOf(x); !hasCur || k.older(cur) {
			top.add(pageCand{pageKey: k, hot: true})
		}
	}
	sel := top.sorted()
	p := Page{Total: total, Sessions: make([]Session, 0, min(len(sel), q.Limit))}
	if len(sel) > q.Limit {
		sel = sel[:q.Limit]
		p.Next = encodeCursor(sel[q.Limit-1].pageKey)
	}
	for _, c := range sel {
		p.Sessions = append(p.Sessions, s.m[c.id])
	}
	return p, nil
}

// ListPage returns the sessions q selects: one page of them, newest first,
// when q.Limit is set, and all of them, oldest first, otherwise. The error
// wraps ErrInvalid for a Limit out of range or a Before that is not a cursor.
// See PageLister for what paging guarantees while sessions change.
func (m *Manager) ListPage(q PageQuery) (Page, error) {
	if pl, ok := m.cfg.Store.(PageLister); ok {
		return pl.ListPage(q)
	}
	cur, hasCur, err := q.cursor()
	if err != nil {
		return Page{}, err
	}
	// A store that cannot say which sessions a list is missing: count what it
	// failed to read meanwhile (another reader's failures may be among them).
	cs, checked := m.cfg.Store.(CheckedStore)
	failedBefore := int64(0)
	if checked {
		failedBefore = cs.ReadErrors()
	}
	all := m.List(q.State)
	if q.Name != "" {
		all = slices.DeleteFunc(slices.Clone(all), func(s Session) bool { return s.Name != q.Name })
	}
	p := pageOf(all, q, cur, hasCur)
	if checked {
		p.Unreadable = int(cs.ReadErrors() - failedBefore)
	}
	return p, nil
}
