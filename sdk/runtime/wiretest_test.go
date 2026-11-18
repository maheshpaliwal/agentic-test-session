package runtime

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// decodeFrames is a minimal stdlib reader for the ingest framing, enough for
// exporter tests (scalar fields, link_ids, attrs, flags). The full decoder and
// field-for-field conformance live in proto/tsessionproto (sdkwire_test.go).
func decodeFrames(body []byte) ([]core.Event, error) {
	var out []core.Event
	for len(body) > 0 {
		n, k := binary.Uvarint(body)
		if k <= 0 || uint64(len(body)-k) < n {
			return out, fmt.Errorf("bad frame length")
		}
		ev, err := decodeEvent(body[k : k+int(n)])
		if err != nil {
			return out, err
		}
		out = append(out, ev)
		body = body[k+int(n):]
	}
	return out, nil
}

func readFrames(r io.Reader) []core.Event {
	b, _ := io.ReadAll(r)
	evs, _ := decodeFrames(b)
	return evs
}

func decodeEvent(b []byte) (core.Event, error) {
	var e core.Event
	for len(b) > 0 {
		key, k := binary.Uvarint(b)
		if k <= 0 {
			return e, fmt.Errorf("bad tag")
		}
		b = b[k:]
		field, wire := int(key>>3), int(key&7)
		var v uint64
		var raw []byte
		switch wire {
		case wVarint:
			if v, k = binary.Uvarint(b); k <= 0 {
				return e, fmt.Errorf("bad varint")
			}
			b = b[k:]
		case wBytes:
			n, k := binary.Uvarint(b)
			if k <= 0 || uint64(len(b)-k) < n {
				return e, fmt.Errorf("bad length")
			}
			raw, b = b[k:k+int(n)], b[k+int(n):]
		default:
			return e, fmt.Errorf("unexpected wire type %d", wire)
		}
		s := string(raw)
		switch field {
		case 1:
			e.SessionID = s
		case 2:
			e.TraceID = s
		case 3:
			e.SpanID = s
		case 4:
			e.ParentID = s
		case 5:
			e.LinkIDs = append(e.LinkIDs, s)
		case 6:
			e.Service = s
		case 7:
			e.Instance = s
		case 8:
			e.Kind = core.Kind(s)
		case 9:
			e.Name = s
		case 10:
			e.HLC = v
		case 11:
			e.EndHLC = v
		case 12:
			e.LocalSeq = v
		case 13:
			e.Seq = v
		case 14:
			e.StartNs = int64(v)
		case 15:
			e.EndNs = int64(v)
		case 16:
			e.Req = decodePayload(raw)
		case 17:
			e.Resp = decodePayload(raw)
		case 20:
			e.TxID = s
		case 21:
			e.Attempt = int(v)
		case 22:
			e.Err = s
		case 23:
			if e.Attrs == nil {
				e.Attrs = map[string]string{}
			}
			var key, val string
			for m := raw; len(m) > 0; {
				t, k := binary.Uvarint(m)
				n, k2 := binary.Uvarint(m[k:])
				str := string(m[k+k2 : k+k2+int(n)])
				if t>>3 == 1 {
					key = str
				} else {
					val = str
				}
				m = m[k+k2+int(n):]
			}
			e.Attrs[key] = val
		case 24:
			e.Flags = append(e.Flags, s)
		}
	}
	return e, nil
}

func decodePayload(b []byte) core.PayloadRef {
	var p core.PayloadRef
	for len(b) > 0 {
		key, k := binary.Uvarint(b)
		b = b[k:]
		switch key {
		case 1<<3 | wBytes, 2<<3 | wBytes:
			n, k := binary.Uvarint(b)
			raw := b[k : k+int(n)]
			b = b[k+int(n):]
			if key>>3 == 1 {
				p.Inline = append([]byte(nil), raw...)
			} else {
				p.BlobHash = string(raw)
			}
		default: // varint fields 3..5
			v, k := binary.Uvarint(b)
			b = b[k:]
			switch key >> 3 {
			case 3:
				p.Size = int(v)
			case 4:
				p.Truncated = v != 0
			case 5:
				p.Redacted = v != 0
			}
		}
	}
	return p
}
