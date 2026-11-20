package collector

import (
	"os"
	"strings"
	"testing"
)

// metricDoc is one metric as registered or as documented.
type metricDoc struct{ kind, help string }

// registeredMetrics reads the name, type and help text of every metric in an
// exposition, from its "# HELP" and "# TYPE" lines.
func registeredMetrics(exposition string) map[string]metricDoc {
	out := map[string]metricDoc{}
	for line := range strings.Lines(exposition) {
		f := strings.SplitN(strings.TrimSpace(line), " ", 4)
		if len(f) != 4 || f[0] != "#" {
			continue
		}
		m := out[f[2]]
		switch f[1] {
		case "HELP":
			m.help = f[3]
		case "TYPE":
			m.kind = f[3]
		}
		out[f[2]] = m
	}
	return out
}

// documentedMetrics reads the table under "### <service>" in the "## Metrics"
// section of docs/CONFIG.md: | `name` | type | labels | meaning |.
func documentedMetrics(t *testing.T, service string) map[string]metricDoc {
	t.Helper()
	b, err := os.ReadFile("../docs/CONFIG.md")
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(b), "\n## Metrics\n")
	if !ok {
		t.Fatal(`docs/CONFIG.md has no "## Metrics" section`)
	}
	_, table, ok := strings.Cut(section, "\n### "+service+"\n")
	if !ok {
		t.Fatalf(`the Metrics section of docs/CONFIG.md has no "### %s" table`, service)
	}
	table, _, _ = strings.Cut(table, "\n#") // up to the next heading
	out := map[string]metricDoc{}
	for line := range strings.Lines(table) {
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		name := strings.TrimSpace(cells[0])
		if len(cells) != 4 || !strings.HasPrefix(name, "`") {
			continue // prose, the header or the separator row
		}
		name = strings.Trim(name, "`")
		if _, dup := out[name]; dup {
			t.Errorf("docs/CONFIG.md lists %s twice under %s", name, service)
		}
		out[name] = metricDoc{kind: strings.TrimSpace(cells[1]), help: strings.TrimSpace(cells[3])}
	}
	return out
}

// The metrics table in docs/CONFIG.md lists exactly the metrics the collector
// registers, with their type and help text. Label names are not part of the
// exposition until a sample exists, so that column is not checked.
func TestMetricsAreDocumented(t *testing.T) {
	c, _ := newC(t, 0, "")
	var b strings.Builder
	if _, err := c.tel.reg.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	registered, documented := registeredMetrics(b.String()), documentedMetrics(t, "Collector")
	if len(registered) == 0 {
		t.Fatal("no metric is registered")
	}
	for name, r := range registered {
		d, ok := documented[name]
		switch {
		case !ok:
			t.Errorf("%s is registered but missing from the Collector table in docs/CONFIG.md", name)
		case d != r:
			t.Errorf("%s: docs/CONFIG.md says %q %q, the code registers %q %q", name, d.kind, d.help, r.kind, r.help)
		}
	}
	for name := range documented {
		if _, ok := registered[name]; !ok {
			t.Errorf("%s is in the Collector table in docs/CONFIG.md but is not registered", name)
		}
	}
}
