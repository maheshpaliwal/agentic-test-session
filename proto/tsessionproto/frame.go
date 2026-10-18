package tsessionproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"

	"github.com/maheshpaliwal/agentic-test-session/core"
	pb "github.com/maheshpaliwal/agentic-test-session/proto/tsessionpb"
)

// ContentType is the media type of a framed Event stream.
const ContentType = "application/x-protobuf"

// Decoder errors. ErrOversize and ErrInvalid concern one message and leave the
// stream usable (call Next again); ErrCorrupt means the framing is lost and
// reading must stop. Other errors come from the underlying reader.
var (
	ErrOversize = errors.New("tsessionproto: message exceeds size limit")
	ErrInvalid  = errors.New("tsessionproto: undecodable message")
	ErrCorrupt  = errors.New("tsessionproto: corrupt framing")
)

type appendWriter struct{ b []byte }

func (w *appendWriter) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }

// AppendFramed appends ev as one varint-length-delimited Event message.
func AppendFramed(dst []byte, ev core.Event) ([]byte, error) {
	w := appendWriter{b: dst}
	if _, err := (protodelim.MarshalOptions{}).MarshalTo(&w, FromCore(ev)); err != nil {
		return dst, err
	}
	return w.b, nil
}

// Encode frames a batch as a stream of Event messages.
func Encode(batch []core.Event) ([]byte, error) {
	var out []byte
	for i := range batch {
		var err error
		if out, err = AppendFramed(out, batch[i]); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
	}
	return out, nil
}

// Decoder reads framed Event messages one at a time.
type Decoder struct {
	br  *bufio.Reader
	max int
	buf bytes.Buffer
}

// NewDecoder returns a Decoder over r. Messages longer than maxMsg bytes
// (encoded size) are skipped and reported as ErrOversize.
func NewDecoder(r io.Reader, maxMsg int) *Decoder {
	return &Decoder{br: bufio.NewReaderSize(r, 64<<10), max: maxMsg}
}

// Next returns the next event, io.EOF at a clean end of stream, ErrOversize /
// ErrInvalid for a bad message (framing intact, keep reading), ErrCorrupt when
// the stream cannot be resynchronised, or a read error from the source.
func (d *Decoder) Next() (core.Event, error) {
	size, err := d.readLen()
	if err != nil {
		return core.Event{}, err
	}
	if size > uint64(d.max) {
		if _, err := io.CopyN(io.Discard, d.br, int64(min(size, 1<<62))); err != nil {
			return core.Event{}, eofToCorrupt(err)
		}
		return core.Event{}, fmt.Errorf("%w: %d > %d bytes", ErrOversize, size, d.max)
	}
	d.buf.Reset()
	if n, err := d.buf.ReadFrom(io.LimitReader(d.br, int64(size))); err != nil {
		return core.Event{}, err
	} else if uint64(n) != size {
		return core.Event{}, fmt.Errorf("%w: truncated message", ErrCorrupt)
	}
	var m pb.Event
	if err := proto.Unmarshal(d.buf.Bytes(), &m); err != nil {
		return core.Event{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	ev, err := ToCore(&m)
	if err != nil {
		return core.Event{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return ev, nil
}

// readLen reads the varint length prefix.
func (d *Decoder) readLen() (uint64, error) {
	var x uint64
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := d.br.ReadByte()
		if err != nil {
			if err == io.EOF && i > 0 {
				return 0, fmt.Errorf("%w: truncated length prefix", ErrCorrupt)
			}
			return 0, err // io.EOF at a message boundary is a clean end
		}
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				break
			}
			return x | uint64(b)<<(7*i), nil
		}
		x |= uint64(b&0x7f) << (7 * i)
	}
	return 0, fmt.Errorf("%w: bad length prefix", ErrCorrupt)
}

// eofToCorrupt maps an EOF while skipping a message body to ErrCorrupt.
func eofToCorrupt(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return fmt.Errorf("%w: truncated message", ErrCorrupt)
	}
	return err
}
