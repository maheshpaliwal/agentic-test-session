package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// doctor checks the pieces a recording needs: both services reachable and
// the instrumented services connected to the control plane.
func (c *cli) doctor(ctx context.Context, args []string) error {
	fs := c.flags("doctor")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	need := fs.Int("require-services", 0, "exit 3 unless at least this many services are connected (a CI preflight)")
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
	return nil
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
		// It said that it sends delivery reports, and none has arrived since
		// it connected (it sends one at once): something refuses them.
		if s.DeliveryInstance != "" && s.LastDeliveryReport.IsZero() && time.Since(s.ConnectedAt) > 2*time.Second {
			fmt.Fprint(w, "  NO DELIVERY REPORTS: none has arrived from it; its log says why")
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
