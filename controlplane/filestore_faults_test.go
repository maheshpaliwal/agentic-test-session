package controlplane

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var errInjected = errors.New("injected fault")

// archFaults makes the calls a FileStore uses to change its archive file
// fail. A failing write still puts the first keep(len) bytes into the file,
// as a full disk or an I/O error does.
type archFaults struct {
	// failWrite is asked about every write, with their count so far (from 1).
	failWrite    func(call int, b []byte) (keep int, fail bool)
	failTruncate bool
	failSync     bool
	failSize     bool
	writes       int
}

func injectFaults(fs *FileStore) *archFaults {
	a := &archFaults{}
	real := defaultArchOps()
	fs.ops = archOps{
		writeAt: func(fh *os.File, b []byte, off int64) (int, error) {
			a.writes++
			if a.failWrite != nil {
				if keep, fail := a.failWrite(a.writes, b); fail {
					n, _ := real.writeAt(fh, b[:keep], off)
					return n, errInjected
				}
			}
			return real.writeAt(fh, b, off)
		},
		truncate: func(fh *os.File, size int64) error {
			if a.failTruncate {
				return errInjected
			}
			return real.truncate(fh, size)
		},
		sync: func(fh *os.File) error {
			if a.failSync {
				return errInjected
			}
			return real.sync(fh)
		},
		size: func(fh *os.File) (int64, error) {
			if a.failSize {
				return 0, errInjected
			}
			return real.size(fh)
		},
	}
	return a
}

// clear lets every call through again.
func (a *archFaults) clear() { *a = archFaults{} }

func archiveBytes(t *testing.T, fs *FileStore) []byte {
	t.Helper()
	b, err := os.ReadFile(fs.archivePath())
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return b
}

// wantArchEnd checks what must hold after every archive write, failed or
// not: the next line is not written below the end of the file.
func wantArchEnd(t *testing.T, what string, fs *FileStore) {
	t.Helper()
	if size := int64(len(archiveBytes(t, fs))); fs.archSize < size {
		t.Fatalf("%s: the next line would be written at %d, inside a file of %d bytes", what, fs.archSize, size)
	}
}

// An archive append fails, at any point of the write, and so may every step
// of cleaning up after it. Whatever is left in the file, the store must go on
// as if the append had not been tried: the sessions stay, later lines are
// not written into the leftovers, a session archived later is read with its
// last record and a session deleted later stays deleted, also after a restart
// without a journal.
func TestFileStoreFailedArchiveAppend(t *testing.T) {
	type outcome int
	const (
		truncated outcome = iota // the file is as long as before
		blanked                  // the written bytes are spaces
		left                     // the written bytes are in the file, ended by a newline
		leftTorn                 // the same, and the newline could not be written
		leftHole                 // the size of the file is not known: the newline is after all the bytes of the append
	)
	firstWrite := func(keep func(n int) int) func(int, []byte) (int, bool) {
		return func(call int, b []byte) (int, bool) { return keep(len(b)), call == 1 }
	}
	// From the second write on (the first one is the append itself).
	laterWrites := func(calls ...int) func(int, []byte) (int, bool) {
		return func(call int, b []byte) (int, bool) {
			for _, c := range calls {
				if call == c {
					return 0, true
				}
			}
			return 0, false
		}
	}
	both := func(fns ...func(int, []byte) (int, bool)) func(int, []byte) (int, bool) {
		return func(call int, b []byte) (int, bool) {
			for _, fn := range fns {
				if keep, fail := fn(call, b); fail {
					return keep, true
				}
			}
			return 0, false
		}
	}
	cuts := map[string]func(n int) int{
		"nothing written":     func(int) int { return 0 },
		"cut in line 1":       func(int) int { return 10 },
		"cut in line 2":       func(n int) int { return n / 2 },
		"cut before the end":  func(n int) int { return n - 1 },
		"everything written":  func(n int) int { return n },
		"cut after line 1":    nil, // set below, it depends on the line
		"cut in the last one": func(n int) int { return n - 20 },
	}
	type tc struct {
		name string
		set  func(a *archFaults, keep func(int) int)
		want outcome
		// rearchive makes the failing append one that is fsynced (a session
		// that is archived already), and the fault a failing fsync.
		rearchive bool
	}
	cases := []tc{
		{name: "write fails, truncate works", want: truncated,
			set: func(a *archFaults, keep func(int) int) { a.failWrite = firstWrite(keep) }},
		{name: "write and truncate fail, blanking works", want: blanked,
			set: func(a *archFaults, keep func(int) int) { a.failWrite, a.failTruncate = firstWrite(keep), true }},
		{name: "write and truncate fail, blanking is not fsynced", want: blanked,
			set: func(a *archFaults, keep func(int) int) {
				a.failWrite, a.failTruncate, a.failSync = firstWrite(keep), true, true
			}},
		{name: "write, truncate and blanking fail", want: left,
			set: func(a *archFaults, keep func(int) int) {
				a.failWrite, a.failTruncate = both(firstWrite(keep), laterWrites(2)), true
			}},
		{name: "write, truncate and blanking fail, size unknown", want: leftHole,
			set: func(a *archFaults, keep func(int) int) {
				a.failWrite, a.failTruncate, a.failSize = both(firstWrite(keep), laterWrites(2)), true, true
			}},
		{name: "write, truncate, blanking and the newline fail", want: leftTorn,
			set: func(a *archFaults, keep func(int) int) {
				a.failWrite, a.failTruncate = both(firstWrite(keep), laterWrites(2, 3)), true
			}},
		{name: "fsync fails, truncate works", want: truncated, rearchive: true,
			set: func(a *archFaults, _ func(int) int) { a.failSync = true }},
		{name: "fsync and truncate fail", want: blanked, rearchive: true,
			set: func(a *archFaults, _ func(int) int) { a.failSync, a.failTruncate = true, true }},
		{name: "fsync, truncate and blanking fail", want: left, rearchive: true,
			set: func(a *archFaults, _ func(int) int) {
				a.failSync, a.failTruncate, a.failWrite = true, true, laterWrites(2)
			}},
	}
	for _, c := range cases {
		for cutName, keep := range cuts {
			if c.rearchive && cutName != "everything written" {
				continue // the write itself works in these
			}
			t.Run(c.name+"/"+cutName, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "s.json")
				fs := uncached(openStore(t, path))
				k0, k1 := archiveSession(0, "kept 0", StateSealed), archiveSession(1, "kept 1", StateAborted)
				// a is long: a later, shorter line would overwrite only its
				// start if it were written at the old end of the file.
				a := archiveSession(2, "a "+strings.Repeat("long ", 60), StateSealed)
				b, d := archiveSession(3, "b", StateSealed), archiveSession(4, "d", StateAborted)
				mustPut(t, fs, k0, k1, a, b, d)
				fs.Evict(k0.ID, k1.ID)
				if c.rearchive {
					fs.Evict(a.ID, b.ID, d.ID)
					a.Name, b.Name, d.Name = a.Name+"2", "b2", "d2"
					mustPut(t, fs, a, b, d)
				}
				if err := fs.Compact(); err != nil {
					t.Fatal(err)
				}
				sizeBefore := fs.archSize
				if keep == nil {
					keep = func(int) int { return len(jsonLine(t, a)) + 1 }
				}
				total := len(jsonLine(t, a)) + len(jsonLine(t, b)) + len(jsonLine(t, d)) + 3
				written := keep(total)

				faults := injectFaults(fs)
				c.set(faults, keep)
				fs.Evict(a.ID, b.ID, d.ID)
				faults.clear()

				for _, s := range []Session{a, b, d} {
					if _, hot := fs.MemStore.Get(s.ID); !hot {
						t.Fatalf("%s left the hot set although its archive line was not written", s.ID)
					}
				}
				wantArchEnd(t, "after the failed append", fs)
				raw := archiveBytes(t, fs)
				tail := raw[min(sizeBefore, int64(len(raw))):]
				want := c.want
				if written == 0 && want != leftHole {
					want = truncated // nothing to blank or to end
				}
				switch want {
				case truncated:
					if len(tail) != 0 || fs.archSize != sizeBefore {
						t.Fatalf("file has %d bytes after the old end, next line at %d, want 0 and %d", len(tail), fs.archSize, sizeBefore)
					}
				case blanked:
					if len(tail) != written || len(bytes.TrimSpace(tail)) != 0 {
						t.Fatalf("after the old end: %q, want %d spaces", tail, written)
					}
				case left:
					if len(tail) != written+1 || tail[len(tail)-1] != '\n' || fs.archTorn {
						t.Fatalf("after the old end: %d bytes ending in %q (torn=%v), want the %d written and a newline", len(tail), tail[len(tail)-1:], fs.archTorn, written)
					}
				case leftHole:
					if written == 0 { // no blanking was tried: the injected fault hit the newline
						if len(tail) != 0 || !fs.archTorn || fs.archSize != sizeBefore+int64(total) {
							t.Fatalf("after the old end: %d bytes, next line at %d (torn=%v)", len(tail), fs.archSize, fs.archTorn)
						}
						break
					}
					if len(tail) != total+1 || tail[len(tail)-1] != '\n' || fs.archSize != sizeBefore+int64(total)+1 {
						t.Fatalf("after the old end: %d bytes, next line at %d, want a newline after the %d bytes of the append", len(tail), fs.archSize, total)
					}
				case leftTorn:
					if len(tail) != written || !fs.archTorn {
						t.Fatalf("after the old end: %d bytes (torn=%v), want the %d written and the torn mark", len(tail), fs.archTorn, written)
					}
				}
				wantStore(t, "after the failed append", fs, []Session{k0, k1, a, b, d})

				// A shorter line than the failed one, the old record of b
				// behind it if it is still there; and d is deleted.
				b.Name = "b, archived later"
				mustPut(t, fs, b)
				fs.Evict(b.ID)
				wantArchEnd(t, "after the next append", fs)
				if _, hot := fs.MemStore.Get(b.ID); hot {
					t.Fatal("the append after the failed one did not work")
				}
				if err := fs.Delete(d.ID); err != nil {
					t.Fatal(err)
				}
				if raw := archiveBytes(t, fs); bytes.Contains(raw, []byte(d.ID)) {
					t.Fatalf("a line of the deleted session is left in the archive:\n%s", raw)
				}
				wantStore(t, "after the next append", fs, []Session{k0, k1, a, b}, d.ID)
				if err := fs.Compact(); err != nil { // no journal: the files alone decide
					t.Fatal(err)
				}
				re := uncached(openStore(t, path))
				wantStore(t, "reopened", re, []Session{k0, k1, a, b}, d.ID)
				if _, hot := re.MemStore.Get(b.ID); hot {
					t.Fatal("b is hot after the restart: its archive line was not found")
				}
				for _, s := range []Session{k0, k1, a, b} {
					if n := bytes.Count(archiveBytes(t, re), []byte(fmt.Sprintf("%q", s.ID))); n > 1 {
						t.Fatalf("%d lines of %s in the archive after a restart", n, s.ID)
					}
				}
			})
		}
	}
}
