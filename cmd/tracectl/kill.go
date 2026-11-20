package main

import (
	"context"
	"fmt"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// kill sets or reads the control plane's global kill switch: while it is on,
// no service records anything, for any session.
func (c *cli) kill(ctx context.Context, args []string) error {
	pos, err := parseArgs(c.flags("kill"), args, 1, "on|off|status")
	if err != nil {
		return err
	}
	switch pos[0] {
	case "on", "off":
		if err := client.SetKill(ctx, c.opts, pos[0] == "on"); err != nil {
			return err
		}
	case "status":
	default:
		return fmt.Errorf("usage: tracectl kill on|off|status")
	}
	// Read the state back instead of echoing the request: this is what the
	// connected services are being told.
	on, err := client.KillSwitch(ctx, c.opts)
	if err != nil {
		return err
	}
	fmt.Fprintln(c.stdout, killLine(on))
	return nil
}

// killLine describes the state of the kill switch in one line.
func killLine(on bool) string {
	if on {
		return "kill switch: ON   recording is stopped for every session (tracectl kill off resumes it)"
	}
	return "kill switch: off  sessions record normally"
}
