package controlplane

import (
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// MaxNameRunes bounds a session name after sanitising.
const MaxNameRunes = 120

// SanitizeName trims, collapses whitespace, strips control and bidi-override
// characters and invalid UTF-8, and truncates to MaxNameRunes (ending in "…").
// It never fails; the result may be empty.
func SanitizeName(s string) string {
	s = strings.ToValidUTF8(s, "")
	var b strings.Builder
	space := false
	n := 0
	over := false
	for _, r := range s {
		switch {
		case unicode.IsSpace(r), r == '​' && false:
			space = true
			continue
		case unicode.IsControl(r), r >= '‪' && r <= '‮', r >= '⁦' && r <= '⁩':
			continue
		}
		if space && n > 0 {
			b.WriteByte(' ')
			n++
		}
		space = false
		b.WriteRune(r)
		n++
		if n > MaxNameRunes+1 {
			over = true
			break
		}
	}
	out := b.String()
	if over || utf8.RuneCountInString(out) > MaxNameRunes {
		rs := []rune(out)
		out = strings.TrimRight(string(rs[:MaxNameRunes-1]), " ") + "…"
	}
	return out
}

var placeholderRE = regexp.MustCompile(`\{([^{}]*)\}`)

// bindingDisplayName is the label used for a binding in auto names:
// "x-user-id" for headers, "jwt:sub", "cookie:sid", "query:uid" otherwise.
func bindingDisplayName(b Binding) string {
	switch b.Source {
	case BindHeader:
		return b.Name
	case BindJWTClaim:
		return "jwt:" + b.Name
	}
	return b.Source + ":" + b.Name
}

func bindingMatches(b Binding, ref string) bool {
	if b.Source == BindHeader {
		return strings.EqualFold(b.Name, ref) || strings.EqualFold(bindingDisplayName(b), ref)
	}
	return b.Name == ref || bindingDisplayName(b) == ref
}

// ExpandNameTemplate expands {bind}, {bind.<name>}, {initiated_by}, {time}
// (HH:MM:SS UTC) and {date} (YYYY-MM-DD). Unknown placeholders are left
// as-is; a missing binding expands to "?". Expansion is single-pass.
func ExpandNameTemplate(name string, bs []Binding, initiatedBy string, now time.Time) string {
	if !strings.Contains(name, "{") {
		return name
	}
	now = now.UTC()
	return placeholderRE.ReplaceAllStringFunc(name, func(m string) string {
		key := m[1 : len(m)-1]
		switch {
		case key == "bind":
			if len(bs) == 0 {
				return "?"
			}
			return bs[0].Value
		case strings.HasPrefix(key, "bind."):
			ref := key[len("bind."):]
			for _, b := range bs {
				if bindingMatches(b, ref) {
					return b.Value
				}
			}
			return "?"
		case key == "initiated_by":
			return initiatedBy
		case key == "time":
			return now.Format("15:04:05")
		case key == "date":
			return now.Format("2006-01-02")
		}
		return m
	})
}

// AutoName generates a readable name: "<name>=<value>" of the first binding
// (joined with " + " for up to 2, then " +N"), or "<initiated_by> HH:MM:SS".
func AutoName(bs []Binding, initiatedBy string, now time.Time) string {
	if len(bs) == 0 {
		return strings.TrimSpace(initiatedBy + " " + now.UTC().Format("15:04:05"))
	}
	parts := make([]string, 0, 2)
	for i, b := range bs {
		if i == 2 {
			break
		}
		parts = append(parts, bindingDisplayName(b)+"="+b.Value)
	}
	out := strings.Join(parts, " + ")
	if extra := len(bs) - len(parts); extra > 0 {
		out += " +" + strconv.Itoa(extra)
	}
	return out
}

// resolveName returns the final sanitised name and whether it was generated.
func resolveName(name string, bs []Binding, initiatedBy string, now time.Time) (string, bool) {
	if n := SanitizeName(ExpandNameTemplate(name, bs, SanitizeName(initiatedBy), now)); n != "" {
		return n, false
	}
	return SanitizeName(AutoName(bs, SanitizeName(initiatedBy), now)), true
}

// Rename relabels a session in any state still known to the store (including
// SEALED/archived). The name is template-expanded and sanitised; an empty
// result is ErrInvalid. It clears name_auto.
func (m *Manager) Rename(id, name string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.cfg.Store.Get(id)
	if !ok {
		return Session{}, ErrNotFound
	}
	n := SanitizeName(ExpandNameTemplate(name, s.Bindings, SanitizeName(s.InitiatedBy), time.Now()))
	if n == "" {
		return s, ErrInvalid
	}
	s.Name, s.NameAuto = n, false
	if err := m.putLocked(s); err != nil {
		return s, err
	}
	return s, nil
}
