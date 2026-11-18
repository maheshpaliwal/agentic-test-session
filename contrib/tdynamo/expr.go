package tdynamo

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// redItem converts an item to plain values with sensitive attribute names
// masked at every depth.
func redItem(item map[string]types.AttributeValue) map[string]any {
	if item == nil {
		return nil
	}
	return core.RedactAny(plainItem(item)).(map[string]any)
}

var exprKeywords = map[string]bool{
	"and": true, "or": true, "not": true, "between": true, "in": true,
	"set": true, "remove": true, "add": true, "delete": true,
}

// exprTok is one lexical token of an expression: an attribute path, a
// "#name" / ":value" placeholder, a function name, or punctuation.
type exprTok struct {
	text string
	kind byte // 'a' attribute, '#' name placeholder, ':' value placeholder, 'f' function/keyword, 'p' punctuation
}

func lexExpr(e string) []exprTok {
	var out []exprTok
	for i := 0; i < len(e); {
		c := e[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++
		case c == '#' || c == ':' || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i + 1
			for j < len(e) && (e[j] == '_' || e[j] == '.' || e[j] >= 'a' && e[j] <= 'z' || e[j] >= 'A' && e[j] <= 'Z' || e[j] >= '0' && e[j] <= '9' ||
				(e[j] == '#' && c != ':') || (e[j] == '[' || e[j] == ']') && c != ':') {
				j++
			}
			w := e[i:j]
			k := byte('a')
			switch {
			case c == ':':
				k = ':'
			case exprKeywords[strings.ToLower(w)]:
				k = 'f'
			case j < len(e) && e[j] == '(':
				k = 'f'
			}
			if c == '#' {
				k = '#'
			}
			out = append(out, exprTok{w, k})
			i = j
		case c == '<' || c == '>' || c == '=' || c == '!':
			j := i + 1
			for j < len(e) && (e[j] == '<' || e[j] == '>' || e[j] == '=') {
				j++
			}
			out = append(out, exprTok{e[i:j], 'p'})
			i = j
		default:
			out = append(out, exprTok{string(c), 'p'})
			i++
		}
	}
	return out
}

// lastSegment returns the final attribute name of a (resolved) path.
func lastSegment(p string) string {
	if i := strings.LastIndex(p, "."); i >= 0 {
		p = p[i+1:]
	}
	if i := strings.Index(p, "["); i >= 0 {
		p = p[:i]
	}
	return p
}

// resolveName maps a path possibly containing "#n" segments to real names.
func resolveName(p string, names map[string]string) string {
	if !strings.Contains(p, "#") {
		return p
	}
	parts := strings.Split(p, ".")
	for i, seg := range parts {
		base, idx := seg, ""
		if j := strings.Index(seg, "["); j >= 0 {
			base, idx = seg[:j], seg[j:]
		}
		if r, ok := names[base]; ok && strings.HasPrefix(base, "#") {
			base = r
		}
		parts[i] = base + idx
	}
	return strings.Join(parts, ".")
}

// resolved is an expression with names substituted and values attached.
type resolved struct {
	Expr   string         // names resolved, value placeholders kept
	Inline string         // names and (redacted) values resolved
	Values map[string]any // placeholder -> redacted value
}

// sensitiveValues returns the ":v" placeholders of exprs (the expressions of
// one request, which share their placeholders) whose value must be masked: a
// placeholder is sensitive when its own name is, or when it shares a term with
// a sensitive attribute. A term is one comparison, assignment, function call
// or IN / BETWEEN list, on either side of the operator; terms end at AND, OR,
// NOT, a clause keyword (SET, REMOVE, ADD, DELETE) or a comma outside
// parentheses.
func sensitiveValues(names map[string]string, exprs ...string) map[string]bool {
	sens := map[string]bool{}
	for _, e := range exprs {
		var term []string // placeholders of the current term
		hot := false      // the current term names a sensitive attribute
		depth, between := 0, false
		flush := func() {
			if hot {
				for _, p := range term {
					sens[p] = true
				}
			}
			term, hot = term[:0], false
		}
		for _, t := range lexExpr(e) {
			switch t.kind {
			case 'a', '#':
				if core.IsSensitiveKey(lastSegment(resolveName(t.text, names))) {
					hot = true
				}
			case ':':
				term = append(term, t.text)
				if core.IsSensitiveKey(strings.TrimPrefix(t.text, ":")) {
					sens[t.text] = true
				}
			case 'f':
				switch strings.ToLower(t.text) {
				case "between":
					between = true
				case "and":
					if between { // the AND of "a BETWEEN :lo AND :hi"
						between = false
					} else {
						flush()
					}
				case "or", "not", "set", "remove", "add", "delete":
					flush()
				}
			case 'p':
				switch t.text {
				case "(":
					depth++
				case ")":
					depth--
				case ",":
					if depth <= 0 {
						flush()
					}
				}
			}
		}
		flush()
	}
	return sens
}

// resolveExprs resolves the "#n" names and ":v" values of the expressions of
// one request (nil or empty expressions resolve to the zero value). Values
// bound to a sensitive attribute anywhere in the request are masked in every
// expression, see sensitiveValues.
func resolveExprs(names map[string]string, vals map[string]types.AttributeValue, exprs ...*string) []resolved {
	texts := make([]string, len(exprs))
	for i, e := range exprs {
		texts[i] = str(e)
	}
	sens := sensitiveValues(names, texts...)
	out := make([]resolved, len(exprs))
	for i, e := range texts {
		out[i] = resolveExpr(e, names, vals, sens)
	}
	return out
}

// resolveExpr renders one expression; sens lists the placeholders to mask.
func resolveExpr(expr string, names map[string]string, vals map[string]types.AttributeValue, sens map[string]bool) resolved {
	r := resolved{Values: map[string]any{}}
	if expr == "" {
		return r
	}
	var expB, inB strings.Builder
	var prev exprTok
	for _, t := range lexExpr(expr) {
		if prev.text != "" && t.text != ")" && t.text != "," && prev.text != "(" &&
			!(t.text == "(" && prev.kind == 'f' && !exprKeywords[strings.ToLower(prev.text)]) {
			expB.WriteByte(' ')
			inB.WriteByte(' ')
		}
		prev = t
		switch t.kind {
		case 'a', '#':
			name := resolveName(t.text, names)
			expB.WriteString(name)
			inB.WriteString(name)
		case ':':
			expB.WriteString(t.text)
			var v any = t.text
			av, bound := vals[t.text]
			if bound {
				v = plainAV(av)
			}
			if sens[t.text] {
				v = core.RedactedValue
			} else {
				v = core.RedactAny(v)
			}
			if bound {
				r.Values[t.text] = v
			}
			b, _ := json.Marshal(v)
			if len(b) > 256 {
				b = append(b[:256:256], "..."...)
			}
			inB.Write(b)
		default:
			expB.WriteString(t.text)
			inB.WriteString(t.text)
		}
	}
	r.Expr = strings.TrimSpace(expB.String())
	r.Inline = strings.TrimSpace(inB.String())
	return r
}

func nonEmpty(m map[string]any) any {
	if len(m) == 0 {
		return nil
	}
	return m
}
