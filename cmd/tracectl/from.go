package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/dashboard"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// --from FILE: the read commands work on a file written by `tracectl export`
// and no service is asked. The file is opened the way `dashboard -open` opens
// it (dashboard.NewOffline), and the control-plane and collector handlers the
// dashboard would serve are called in this process, through the HTTP client
// of the commands: so a command prints for the file what it prints against
// `dashboard -open FILE`, which is what it prints for the live session.

// The addresses the commands "ask" with --from. Nothing listens there and
// nothing is resolved: handlerTransport answers by host name.
const (
	fileCPHost  = "controlplane.from-file.invalid"
	fileColHost = "collector.from-file.invalid"
)

// fromCommands are the commands that work with --from: those that only read.
var fromCommands = []string{"digest", "summary", "report", "events", "trace", "show", "entities", "history", "diff", "status", "list"}

// fromFlag collects the files of a repeated --from.
type fromFlag []string

func (f *fromFlag) String() string { return strings.Join(*f, ",") }

func (f *fromFlag) Set(v string) error {
	if v == "" {
		return errors.New("a file name is required")
	}
	*f = append(*f, v)
	return nil
}

// handlerTransport answers a request with the handler registered for its
// host, in this process: no socket, no listener.
type handlerTransport map[string]http.Handler

func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	h, ok := t[r.URL.Host]
	if !ok {
		return nil, fmt.Errorf("--from: no service at %s: a file is read, nothing else is asked", r.URL.Host)
	}
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	// As a server hands a request to its handler.
	req := r.Clone(r.Context())
	if req.Body == nil {
		req.Body = http.NoBody
	}
	req.Host, req.RequestURI = r.URL.Host, r.URL.RequestURI()
	w := &memResponse{header: http.Header{}}
	h.ServeHTTP(w, req)
	w.WriteHeader(http.StatusOK) // a handler that wrote nothing
	return &http.Response{
		Status: fmt.Sprintf("%d %s", w.code, http.StatusText(w.code)), StatusCode: w.code,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: w.sent, Body: io.NopCloser(&w.body), ContentLength: int64(w.body.Len()), Request: r,
	}, nil
}

// memResponse is the http.ResponseWriter of handlerTransport: it keeps what
// the handler wrote.
type memResponse struct {
	header http.Header // the handler's to change until the status is written
	sent   http.Header // as they were when it was
	code   int
	body   bytes.Buffer
}

func (w *memResponse) Header() http.Header { return w.header }

func (w *memResponse) WriteHeader(code int) {
	if w.code == 0 {
		w.code, w.sent = code, w.header.Clone()
	}
}

func (w *memResponse) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.body.Write(b)
}

// openFrom opens the files of --from and points the commands at them.
func (c *cli) openFrom(paths []string) error {
	// Standard input that is a terminal would be waited on without a word.
	if slices.Contains(paths, "-") {
		// (The null device is a character device too, and is no terminal: it reads as an empty file.)
		null, _ := os.Stat(os.DevNull)
		if st, err := os.Stdin.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 && (null == nil || !os.SameFile(st, null)) {
			return errors.New("--from - reads an export from standard input, which is a terminal: pipe one into it (gunzip -c run.ndjson.gz | tracectl --from - digest), or name the file")
		}
	}
	d, err := dashboard.NewOfflineStrict(paths)
	if err != nil {
		// A digest names its file as it was given when the digest was written: a relative name is one of
		// another directory when the command is run elsewhere.
		var pe *fs.PathError
		if errors.As(err, &pe) && errors.Is(err, fs.ErrNotExist) && !filepath.IsAbs(pe.Path) {
			if wd, werr := os.Getwd(); werr == nil {
				return fmt.Errorf("%w (looked for in %s; a command copied from a digest or a report names the file as it was called where that was written)", err, wd)
			}
		}
		return err
	}
	c.files = d.Files()
	for _, f := range c.files {
		if f.Unproven {
			// It is read, and judged as a session of which the control plane knows nothing is: no gate
			// passes on a file that does not say how its session ended, or that it is whole.
			fmt.Fprintf(c.stderr, "tracectl: note: %s does not say how its session ended or how many events it holds (its first line is lost, or it was "+
				"exported by an older tracectl or without the control plane): --fail-on-incomplete and --fail-on-unconfirmed fail on it; export the session again\n",
				shellWord(f.Path, false))
		}
		if f.Truncated != nil {
			// dashboard -open shows such a file, marked incomplete, for a person
			// to look at. A command whose output and exit status are acted on
			// does not judge a part of a recording.
			return fmt.Errorf("%w: the file is cut short (an interrupted export or download); export the session again", f.Truncated)
		}
	}
	c.opts.BaseURL, c.opts.CollectorURL = "http://"+fileCPHost, "http://"+fileColHost
	c.opts.HTTPClient = &http.Client{Transport: handlerTransport{fileCPHost: d.ControlPlane(), fileColHost: d.Collector()}}
	return nil
}

// fromSessions adds the sessions a command was not given, with --from: the
// session of the one file, or for diff the sessions of the two files
// (baseline, then candidate). pos are the command's positional arguments and
// n is how many it takes. A command that names its sessions is left as it is.
func (c *cli) fromSessions(cmd string, pos []string, n int) ([]string, error) {
	want := 1
	if cmd == "diff" {
		want = 2
	}
	if len(c.files) == 0 || len(pos) != n-want {
		return pos, nil
	}
	ids := slicex.Map(c.files, func(f dashboard.OpenedFile) string { return f.SessionID })
	switch {
	case len(ids) == want:
		return append(ids, pos...), nil
	case want == 2:
		return nil, fmt.Errorf("diff compares two sessions and --from was given %d: give two files (--from BASELINE --from CANDIDATE), or name two of the sessions (%s)",
			len(ids), strings.Join(ids, ", "))
	}
	return nil, fmt.Errorf("--from was given %d files: name the session to read (%s)", len(ids), strings.Join(ids, ", "))
}

// resolve finds the session a command names. With --from it is one of the
// files' sessions, named by its id or by its name (the file's name without
// the extension).
func (c *cli) resolve(ctx context.Context, ref string) (*client.Session, error) {
	if len(c.files) == 0 {
		return resolve(ctx, c.opts, ref)
	}
	// An id is an id, whatever a file or a session is called. Otherwise the reference is a session's name
	// or a file's name without the extension, and has to mean one session.
	id := ""
	if slices.ContainsFunc(c.files, func(f dashboard.OpenedFile) bool { return f.SessionID == ref }) {
		id = ref
	} else {
		var ids []string
		for _, f := range c.files {
			base := strings.TrimSuffix(filepath.Base(f.Path), filepath.Ext(f.Path))
			if (f.Name == ref || base == ref) && !slices.Contains(ids, f.SessionID) {
				ids = append(ids, f.SessionID)
			}
		}
		switch len(ids) {
		case 0:
			return nil, fmt.Errorf("no session with the id or the name %q in the files of --from: %s", ref, c.fromList())
		case 1:
			id = ids[0]
		default:
			return nil, fmt.Errorf("%q names %d sessions in the files of --from (%s): name the session by its id", ref, len(ids), strings.Join(ids, ", "))
		}
	}
	return client.Attach(ctx, c.opts, id)
}

// fromList is "FILE holds ts_x, FILE holds ts_y".
func (c *cli) fromList() string {
	return strings.Join(slicex.Map(c.files, func(f dashboard.OpenedFile) string { return f.Path + " holds " + f.SessionID }), ", ")
}

// fromFile is the file of --from that holds session id, as it was given on
// the command line; empty without --from.
func (c *cli) fromFile(id string) string {
	i := slices.IndexFunc(c.files, func(f dashboard.OpenedFile) bool { return f.SessionID == id })
	if i < 0 {
		return ""
	}
	return c.files[i].Path
}

// fromWord is fromFile as a command names it (see shellWord).
func (c *cli) fromWord(id string) string {
	if f := c.fromFile(id); f != "" {
		return shellWord(f, false)
	}
	return ""
}

// The longest file name a digest repeats in its commands, in bytes: a digest
// has a size limit and names the file up to fifteen times. A digest that is
// short of room (the session's own name and reasons are shortened too)
// repeats a shorter one only.
const (
	fromArgMax      = 200
	fromArgTightMax = 40
)

// shellWord is the file name path as one word of a shell command: as it is,
// or in single quotes where a shell would read it otherwise. It is the word
// FILE for a name that is too long (tight: the digest is short of room) or
// that single quotes cannot hold on one line of Markdown: the reader then
// puts the name in.
func shellWord(path string, tight bool) string {
	plain := func(r rune) bool {
		return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_./-+=:,@%", r)
	}
	quotable := func(r rune) bool { return r >= ' ' && r < 0x7f && r != '\'' && r != '`' }
	limit := fromArgMax
	if tight {
		limit = fromArgTightMax
	}
	switch {
	case len(path) > limit || strings.HasPrefix(path, "-") || strings.HasPrefix(path, "/dev/") || strings.ContainsFunc(path, func(r rune) bool { return !quotable(r) }):
		return "FILE"
	case strings.ContainsFunc(path, func(r rune) bool { return !plain(r) }):
		return "'" + path + "'"
	case strings.HasPrefix(path, "=") || strings.HasPrefix(path, "~"):
		return "'" + path + "'" // a shell expands these at the start of a word
	}
	return path
}
