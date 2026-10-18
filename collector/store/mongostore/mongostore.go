package mongostore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// MaxEventBytes is the largest event JSON accepted. MongoDB's document limit
// is 16MiB; this leaves room for the indexed scalar fields.
const MaxEventBytes = 15 << 20

// ErrEventTooLarge is returned (wrapped; use errors.Is) for an event whose
// JSON does not fit in one BSON document. It is not a seq conflict, so the
// collector treats the event as poison.
var ErrEventTooLarge = errors.New("mongostore: event exceeds BSON document limit")

// Config configures Open.
type Config struct {
	URI              string        // mongodb:// or mongodb+srv:// URI; never logged
	Database         string        // default "tsession"
	EventsCollection string        // default "events"
	SealsCollection  string        // default "seals"
	ConnectTimeout   time.Duration // server selection + initial ping; default 10s
	OperationTimeout time.Duration // applied to calls whose ctx has no deadline; default 30s
	MaxPoolSize      uint64        // 0 = driver default
}

func (c *Config) defaults() {
	if c.Database == "" {
		c.Database = "tsession"
	}
	if c.EventsCollection == "" {
		c.EventsCollection = "events"
	}
	if c.SealsCollection == "" {
		c.SealsCollection = "seals"
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.OperationTimeout <= 0 {
		c.OperationTimeout = 30 * time.Second
	}
}

// Store is the MongoDB store.Store.
type Store struct {
	client *mongo.Client
	events *mongo.Collection
	seals  *mongo.Collection
	opTO   time.Duration
}

var _ store.Store = (*Store)(nil)

// eventDoc is the stored form of one event. Optional fields are omitted so
// the partial tx index only holds deciding events.
type eventDoc struct {
	SessionID   string `bson:"session_id"`
	Seq         int64  `bson:"seq"`
	Instance    string `bson:"instance"`
	LocalSeq    int64  `bson:"local_seq"`
	SpanID      string `bson:"span_id"`
	Kind        string `bson:"kind"`
	Service     string `bson:"service"`
	TxID        string `bson:"tx_id,omitempty"`
	TxDecides   bool   `bson:"tx_decides,omitempty"`
	TxEffective bool   `bson:"tx_effective,omitempty"`
	StartNs     int64  `bson:"start_ns"`
	EndNs       int64  `bson:"end_ns"`
	Body        []byte `bson:"body"`
}

type sealDoc struct {
	ID          string   `bson:"_id"`
	Incomplete  bool     `bson:"incomplete"`
	Orphaned    []string `bson:"orphaned"`
	BundleURI   string   `bson:"bundle_uri"`
	SealedAt    string   `bson:"sealed_at"`
	Quarantined int      `bson:"quarantined"`
}

// Open connects, verifies the server is reachable and creates the indexes
// (idempotent; safe when several collectors start together).
func Open(ctx context.Context, cfg Config) (*Store, error) {
	cfg.defaults()
	if cfg.URI == "" {
		return nil, errors.New("mongostore: empty URI")
	}
	redact := func(err error) error {
		return errors.New(strings.ReplaceAll(err.Error(), cfg.URI, "[redacted-uri]"))
	}
	co := options.Client().ApplyURI(cfg.URI).
		SetServerSelectionTimeout(cfg.ConnectTimeout).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetAppName("tsession-collector")
	if cfg.MaxPoolSize > 0 {
		co.SetMaxPoolSize(cfg.MaxPoolSize)
	}
	client, err := mongo.Connect(co)
	if err != nil {
		return nil, fmt.Errorf("mongostore: connect: %w", redact(err))
	}
	cctx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := client.Ping(cctx, nil); err != nil {
		client.Disconnect(context.Background()) //nolint:errcheck
		return nil, fmt.Errorf("mongostore: ping: %w", redact(err))
	}
	db := client.Database(cfg.Database)
	s := &Store{client: client, events: db.Collection(cfg.EventsCollection), seals: db.Collection(cfg.SealsCollection), opTO: cfg.OperationTimeout}
	ictx, icancel := context.WithTimeout(ctx, cfg.ConnectTimeout+cfg.OperationTimeout)
	defer icancel()
	if err := s.ensureIndexes(ictx); err != nil {
		client.Disconnect(context.Background()) //nolint:errcheck
		return nil, fmt.Errorf("mongostore: indexes: %w", redact(err))
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	_, err := s.events.Indexes().CreateMany(ctx, []mongo.IndexModel{
		// The concurrency guard and the Events/Head access path.
		{Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "seq", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("session_seq")},
		{Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "instance", Value: 1}, {Key: "local_seq", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("session_instance_local")},
		// Covers ListSessions' $group without fetching documents.
		{Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "start_ns", Value: 1}, {Key: "end_ns", Value: 1}},
			Options: options.Index().SetName("session_span")},
		// TxOutcomes: only deciding events are indexed.
		{Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "tx_id", Value: 1}},
			Options: options.Index().SetName("session_tx").
				SetPartialFilterExpression(bson.D{{Key: "tx_decides", Value: true}})},
	})
	return err
}

// ctx applies the default operation timeout when the caller set no deadline.
func (s *Store) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.opTO)
}

func noNUL(s string) string {
	if strings.IndexByte(s, 0) < 0 {
		return s
	}
	return strings.ReplaceAll(s, "\x00", "")
}

func (s *Store) Head(ctx context.Context, sid string) (uint64, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	return s.head(ctx, sid)
}

func (s *Store) head(ctx context.Context, sid string) (uint64, error) {
	var d struct {
		Seq int64 `bson:"seq"`
	}
	err := s.events.FindOne(ctx, bson.D{{Key: "session_id", Value: sid}},
		options.FindOne().SetSort(bson.D{{Key: "seq", Value: -1}}).SetProjection(bson.D{{Key: "seq", Value: 1}, {Key: "_id", Value: 0}})).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(d.Seq), nil
}

func (s *Store) AppendEvents(ctx context.Context, sid string, evs []core.Event) error {
	if len(evs) == 0 {
		return nil
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	// Marshal everything first so an oversized or unmarshalable event fails
	// before any write (no partial batch for poison events).
	docs := make([]any, len(evs))
	for i := range evs {
		e := &evs[i]
		if e.Seq > math.MaxInt64 || e.LocalSeq > math.MaxInt64 {
			return store.ErrSeqConflict
		}
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		body = store.StripNUL(body)
		if len(body) > MaxEventBytes {
			return fmt.Errorf("%w: seq %d is %d bytes (max %d)", ErrEventTooLarge, e.Seq, len(body), MaxEventBytes)
		}
		d := eventDoc{SessionID: sid, Seq: int64(e.Seq), Instance: noNUL(e.Instance), LocalSeq: int64(e.LocalSeq),
			SpanID: noNUL(e.SpanID), Kind: noNUL(string(e.Kind)), Service: noNUL(e.Service),
			TxID: noNUL(e.TxID), StartNs: e.StartNs, EndNs: e.EndNs, Body: body}
		if d.TxID != "" {
			if eff, decides := store.TxResult(e); decides {
				d.TxDecides, d.TxEffective = true, eff
			}
		}
		docs[i] = d
	}

	head, err := s.head(ctx, sid)
	if err != nil {
		return err
	}
	for i := range evs {
		if evs[i].Seq != head+1+uint64(i) {
			return store.ErrSeqConflict
		}
	}
	_, err = s.events.InsertMany(ctx, docs, options.InsertMany().SetOrdered(true))
	if err == nil {
		return nil
	}
	if mongo.IsDuplicateKeyError(err) {
		// Concurrent writer took the Seq (or a duplicate instance/local_seq).
		// Events before the failing one stay stored: see the package doc.
		return fmt.Errorf("%w: %v", store.ErrSeqConflict, err)
	}
	return err
}

func (s *Store) Events(ctx context.Context, sid string, from uint64, limit int) ([]core.Event, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	if from > math.MaxInt64 {
		return nil, nil
	}
	fo := options.Find().SetSort(bson.D{{Key: "seq", Value: 1}}).SetProjection(bson.D{{Key: "body", Value: 1}, {Key: "_id", Value: 0}})
	if limit > 0 {
		fo.SetLimit(int64(limit))
	}
	cur, err := s.events.Find(ctx, bson.D{{Key: "session_id", Value: sid}, {Key: "seq", Value: bson.D{{Key: "$gte", Value: int64(from)}}}}, fo)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx) //nolint:errcheck
	var out []core.Event
	for cur.Next(ctx) {
		var d struct {
			Body []byte `bson:"body"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		var e core.Event
		if err := json.Unmarshal(d.Body, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, cur.Err()
}

func (s *Store) TxOutcomes(ctx context.Context, sid string) (map[string]bool, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	cur, err := s.events.Find(ctx, bson.D{{Key: "session_id", Value: sid}, {Key: "tx_decides", Value: true}},
		options.Find().SetSort(bson.D{{Key: "seq", Value: 1}}).
			SetProjection(bson.D{{Key: "tx_id", Value: 1}, {Key: "tx_effective", Value: 1}, {Key: "_id", Value: 0}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx) //nolint:errcheck
	out := map[string]bool{}
	for cur.Next(ctx) {
		var d struct {
			TxID      string `bson:"tx_id"`
			Effective bool   `bson:"tx_effective"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		store.MergeTxOutcome(out, d.TxID, d.Effective)
	}
	return out, cur.Err()
}

func (s *Store) ListSessions(ctx context.Context) ([]store.SessionInfo, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	cur, err := s.events.Aggregate(ctx, mongo.Pipeline{{{Key: "$group", Value: bson.D{
		{Key: "_id", Value: "$session_id"},
		{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
		{Key: "first", Value: bson.D{{Key: "$min", Value: "$start_ns"}}},
		{Key: "last", Value: bson.D{{Key: "$max", Value: "$end_ns"}}},
	}}}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx) //nolint:errcheck
	var out []store.SessionInfo
	for cur.Next(ctx) {
		var d struct {
			ID    string `bson:"_id"`
			N     int    `bson:"n"`
			First int64  `bson:"first"`
			Last  int64  `bson:"last"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, store.SessionInfo{ID: d.ID, EventCount: d.N, FirstNs: d.First, LastNs: d.Last})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	// Every document in seals is a sealed session.
	sc, err := s.seals.Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer sc.Close(ctx) //nolint:errcheck
	sealed := map[string]bool{}
	for sc.Next(ctx) {
		var d struct {
			ID string `bson:"_id"`
		}
		if err := sc.Decode(&d); err != nil {
			return nil, err
		}
		sealed[d.ID] = true
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Sealed = sealed[out[i].ID]
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) GetSeal(ctx context.Context, sid string) (*store.SealInfo, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	var d sealDoc
	err := s.seals.FindOne(ctx, bson.D{{Key: "_id", Value: sid}}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	at, _ := time.Parse(time.RFC3339Nano, d.SealedAt)
	return &store.SealInfo{Incomplete: d.Incomplete, Orphaned: d.Orphaned, BundleURI: d.BundleURI, SealedAt: at, Quarantined: d.Quarantined}, nil
}

func (s *Store) PutSeal(ctx context.Context, sid string, si store.SealInfo) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	orph := si.Orphaned
	if orph == nil {
		orph = []string{}
	}
	_, err := s.seals.UpdateOne(ctx, bson.D{{Key: "_id", Value: sid}}, bson.D{{Key: "$set", Value: bson.D{
		{Key: "incomplete", Value: si.Incomplete},
		{Key: "orphaned", Value: orph},
		{Key: "bundle_uri", Value: si.BundleURI},
		{Key: "sealed_at", Value: si.SealedAt.Format(time.RFC3339Nano)},
		{Key: "quarantined", Value: si.Quarantined},
	}}}, options.UpdateOne().SetUpsert(true))
	return err
}

func (s *Store) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.client.Disconnect(ctx)
}
