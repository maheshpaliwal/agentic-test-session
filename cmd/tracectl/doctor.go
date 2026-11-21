package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/ident"
	"github.com/maheshpaliwal/agentic-test-session/internal/scrub"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// doctor checks the pieces a recording needs: both services reachable and
// the instrumented services connected to the control plane.
func (c *cli) doctor(ctx context.Context, args []string) error {
	fs := c.flags("doctor")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	need := fs.Int("require-services", 0, "exit 3 unless at least this many services are connected (a CI preflight)")
	needDelivery := fs.Bool("require-delivery", false, "exit 3 when a connected service reports events it cannot deliver, or its reports do not arrive "+
		"(a check DURING a run: it sees only services that report, and only the sessions running now)")
	if _, err := parseArgs(fs, args, 0, ""); err != nil {
		return err
	}
	cp := strings.TrimRight(cmp.Or(c.opts.BaseURL, os.Getenv("TSESSION_CONTROLPLANE_URL"), client.DefaultURL), "/")
	col := strings.TrimRight(cmp.Or(c.opts.CollectorURL, os.Getenv("TSESSION_COLLECTOR_URL"), client.DefaultCollectorURL), "/")
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	hc := cmp.Or(c.opts.HTTPClient, http.DefaultClient)
	rep := doctorReport{ControlPlane: probe(ctx, hc, cp), Collector: probe(ctx, hc, col), Subscribers: []client.Subscriber{}}
	if rep.ControlPlane.OK {
		if active, err := client.List(ctx, c.opts, "ACTIVE"); err == nil {
			rep.ActiveSessions = len(active)
		} else {
			rep.Notes = append(rep.Notes, "listing active sessions failed: "+err.Error())
		}
		if on, err := client.KillSwitch(ctx, c.opts); err == nil {
			rep.KillSwitch = on
		} else {
			rep.Notes = append(rep.Notes, "reading the kill switch failed: "+err.Error())
		}
		subs, err := client.Subscribers(ctx, c.opts)
		switch {
		case err == nil:
			rep.Subscribers = append(rep.Subscribers, subs...)
			rep.noteDelivery()
		case client.StatusCode(err) == http.StatusNotFound:
			rep.Notes = append(rep.Notes, "this control plane is too old to list connected services")
		default:
			rep.Notes = append(rep.Notes, "listing connected services failed: "+err.Error())
		}
		if err == nil && len(subs) == 0 {
			rep.Notes = append(rep.Notes, "no service is connected, so nothing can be recorded: check that the service runs with "+
				"TSESSION_ENABLED=true and that its TSESSION_CONTROLPLANE_URL points at "+client.DisplayURL(cp))
		}
	}
	if *asJSON {
		printJSON(c.stdout, rep)
	} else {
		rep.print(c.stdout)
	}
	if !rep.ControlPlane.OK || !rep.Collector.OK {
		return fmt.Errorf("doctor: a service is unreachable")
	}
	if n := len(rep.Subscribers); n < *need {
		return checkError{msg: fmt.Sprintf("check failed: %d service(s) connected, --require-services wants %d", n, *need)}
	}
	if bad := rep.undeliverable(); *needDelivery && len(bad) > 0 {
		return checkError{msg: "check failed: --require-delivery: " + strings.Join(bad, "; ")}
	}
	return nil
}

// who names a connected service for a message.
func who(s client.Subscriber) string {
	return cmp.Or(s.Service, "an unnamed service") + " (" + cmp.Or(s.Instance, s.DeliveryInstance, "?") + ")"
}

// undeliverable names the connected services that cannot deliver as far as
// the control plane knows: those whose reports do not arrive, and those that
// reported events held or lost with a failed request to the collector. It is
// what --require-delivery fails on.
func (r doctorReport) undeliverable() []string {
	var out []string
	for _, s := range r.Subscribers {
		switch {
		case s.DeliveryUnheard:
			out = append(out, who(s)+" sends no delivery reports")
		case s.Pending.Stuck():
			out = append(out, who(s)+" cannot deliver: "+s.Pending.LastError)
		}
	}
	return out
}

// noteDelivery makes the delivery errors safe to print (they are the
// services' own words, and go into CI logs) and adds the remedy for every
// service that cannot deliver.
func (r *doctorReport) noteDelivery() {
	var named []string
	for i, s := range r.Subscribers {
		if s.Pending == nil {
			continue
		}
		p := *s.Pending
		p.LastError = clip(scrub.Text(ident.CleanN(p.LastError, 1024)), 200)
		r.Subscribers[i].Pending = &p
		if name := cmp.Or(s.Service, "the unnamed service"); p.Stuck() && !slices.Contains(named, name) {
			named = append(named, name)
			r.Notes = append(r.Notes, name+" cannot deliver its events to the collector: check TSESSION_COLLECTOR_URL of "+name+
				" (it must be the collector's address as seen from that service)")
		}
	}
}

type probeResult struct {
	URL   string `json:"url"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type doctorReport struct {
	ControlPlane   probeResult `json:"control_plane"`
	Collector      probeResult `json:"collector"`
	ActiveSessions int         `json:"active_sessions"`
	// KillSwitch is true while the global kill switch is on: nothing is
	// recorded for any session.
	KillSwitch  bool                `json:"kill_switch"`
	Subscribers []client.Subscriber `json:"subscribers"`
	Notes       []string            `json:"notes,omitempty"`
}

// probe asks a service's /healthz.
func probe(ctx context.Context, hc *http.Client, base string) probeResult {
	res := probeResult{URL: client.DisplayURL(base)} // printed, and often into a CI log
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", nil)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	resp, err := hc.Do(req)
	if err != nil {
		res.Error = client.ScrubURLError(err).Error()
		return res
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		res.Error = "GET /healthz: " + resp.Status
		return res
	}
	res.OK = true
	return res
}

func (r doctorReport) print(w io.Writer) {
	line := func(name string, p probeResult) {
		status := "ok"
		if !p.OK {
			status = "UNREACHABLE  " + p.Error
		}
		fmt.Fprintf(w, "%-14s %s  %s\n", name, p.URL, status)
	}
	line("control plane", r.ControlPlane)
	line("collector", r.Collector)
	if !r.ControlPlane.OK {
		return
	}
	if r.KillSwitch {
		fmt.Fprintln(w, "KILL SWITCH ON  nothing is recorded for any session; to resume recording run: tracectl kill off")
	}
	fmt.Fprintf(w, "active sessions  %d\n", r.ActiveSessions)
	fmt.Fprintf(w, "connected services  %d\n", len(r.Subscribers))
	unnamed := 0
	for _, s := range r.Subscribers {
		if s.Service == "" {
			unnamed++
			continue
		}
		fmt.Fprintf(w, "  %-24s %-28s connected %s", clip(s.Service, 24), clip(s.Instance, 28),
			time.Since(s.ConnectedAt).Round(time.Second))
		// It said that it sends delivery reports, and the control plane has
		// heard nothing of it for too long: something refuses them. The control
		// plane decides that, by the rule a session's verdict goes by, so the
		// two never disagree.
		switch {
		case !s.DeliveryUnheard:
			// It reports, and what it reports is events of running sessions it
			// holds or lost while its requests to the collector fail.
			if p := s.Pending; p.Stuck() {
				fmt.Fprintf(w, "  CANNOT DELIVER: %d event(s) undelivered", p.Undelivered)
				if p.Dropped > 0 {
					fmt.Fprintf(w, ", %d dropped", p.Dropped)
				}
				fmt.Fprintf(w, " in %d session(s); last error: %s", p.Sessions, p.LastError)
			}
		case s.LastDeliveryReport.IsZero():
			fmt.Fprint(w, "  NO DELIVERY REPORTS: none has arrived from it; its log says why")
		default:
			fmt.Fprintf(w, "  NO DELIVERY REPORTS: none has arrived from it for %s; its log says why", time.Since(s.LastDeliveryReport).Round(time.Second))
		}
		fmt.Fprintln(w)
	}
	if unnamed > 0 {
		fmt.Fprintf(w, "  %d unnamed (an older SDK, or another client following the active set)\n", unnamed)
	}
	if len(r.Subscribers) > 0 {
		fmt.Fprintln(w, "  connected means the service follows sessions; it does not show that it can reach the collector")
	}
	for _, n := range r.Notes {
		fmt.Fprintln(w, "note: "+n)
	}
}
