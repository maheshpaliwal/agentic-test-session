// Package dynamostore is the Amazon DynamoDB store.Store.
//
// Layout: one table, partition key pk (S, the session id), sort key sk (S).
//
//	sk = "META"                  head, event_count, first_ns, last_ns
//	sk = "SEAL"                  seal info (present only once sealed)
//	sk = "E#<seq, 20 digits>"    first (or only) item of one event
//	sk = "E#<seq>#c<n>"          continuation item n (1-based) of that event
//
// Zero-padded seqs make lexicographic sk order equal numeric order, and
// "E#<seq>" sorts before its "#c<n>" continuations and before "E#<seq+1>".
//
// An event is stored as gzipped JSON in the binary attribute "body". DynamoDB
// items are capped at 400KB, so bodies larger than chunkBytes continue in
// continuation items written in the same transaction. The first item also
// carries queryable scalars: seq, kind, service, start_ns, end_ns and, for
// transaction-deciding events, tx_id and tx_eff (see store.TxResult).
//
// Limits: a single event may occupy at most MaxEventBytes of gzipped JSON
// (about 3.4MB, so roughly 4MB of ordinary JSON); a larger event makes
// AppendEvents fail before anything is written. The cap exists because one
// event must fit in one TransactWriteItems call (4MB, 100 actions).
//
// Concurrency: every transaction updates META conditioned on the head the
// writer observed, so of several writers racing for the same Seq exactly one
// wins and the rest get store.ErrSeqConflict. A batch that does not fit one
// transaction is split into several; each is atomic, but the batch as a whole
// is not: if a later chunk fails, earlier chunks stay committed (a retry of
// the same batch then hits store.ErrSeqConflict, like any replayed Seq).
//
// Unlike the Postgres store there is no unique (instance, local_seq) index:
// replaying a batch is rejected through its Seq alone.
package dynamostore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

const (
	// DefaultTable is the table used when Config.TableName is empty.
	DefaultTable = "tsession"
	// MaxEventBytes is the largest gzipped event body AppendEvents accepts.
	MaxEventBytes = 3_400_000

	defaultTimeout       = 15 * time.Second
	defaultCreateTimeout = 60 * time.Second

	chunkBytes   = 256 * 1024 // body bytes per item; keeps items well under 400KB
	maxTxActions = 100        // DynamoDB limit per TransactWriteItems
	txBudget     = 3_800_000  // headroom under the 4MB transaction size limit
	itemOverhead = 512        // per-item allowance for keys, names and scalars
	maxKeyBytes  = 2048       // partition key limit
	eventPrefix  = "E#"       // sort key prefix of event items
	eventEnd     = "E$"       // '$' sorts right after '#': upper bound of events
	metaSK       = "META"     // per-session counters
	sealSK       = "SEAL"     // per-session seal info
	seqDigits    = 20         // len("18446744073709551615")
	contSep      = "#c"       // continuation sk suffix separator
	timeFormat   = time.RFC3339Nano
)

// Attribute names.
const (
	aPK, aSK         = "pk", "sk"
	aBody            = "body"
	aChunks          = "chunks"
	aSeq             = "seq"
	aKind            = "kind"
	aService         = "service"
	aStartNs, aEndNs = "start_ns", "end_ns"
	aTxID, aTxEff    = "tx_id", "tx_eff"
	aHead            = "head"
	aCount           = "event_count"
	aFirstNs         = "first_ns"
	aLastNs          = "last_ns"
	aIncomplete      = "incomplete"
	aOrphaned        = "orphaned"
	aBundleURI       = "bundle_uri"
	aSealedAt        = "sealed_at"
	aQuarantined     = "quarantined"
)

// Config configures Open. Credentials come from the default AWS chain
// (environment, shared config, instance role, ...) and are never logged.
type Config struct {
	TableName string // default DefaultTable
	Region    string // empty: from the default chain (AWS_REGION, profile)
	// Endpoint overrides the service URL (DynamoDB Local, LocalStack).
	Endpoint string
	// CreateTable creates the table (on-demand billing) when it is missing
	// and waits until it is ACTIVE. Otherwise a missing table is an error.
	CreateTable bool
	// Timeout bounds every individual DynamoDB call (default 15s). A
	// shorter deadline on the caller's context still wins.
	Timeout time.Duration
	// CreateTimeout bounds waiting for a new table to become ACTIVE
	// (default 60s).
	CreateTimeout time.Duration
}

// api is the subset of *dynamodb.Client the store uses; tests fake it.
type api interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
}

var _ api = (*dynamodb.Client)(nil)

// Store is the DynamoDB store.Store.
type Store struct {
	db      api
	table   string
	timeout time.Duration
}

var _ store.Store = (*Store)(nil)

// Open connects, verifies (or with CreateTable creates) the table and
// returns the store.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	var opts []func(*config.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	ac, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("dynamostore: load aws config: %w", err)
	}
	db := dynamodb.NewFromConfig(ac, func(o *dynamodb.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return newStore(ctx, db, cfg)
}

func newStore(ctx context.Context, db api, cfg Config) (*Store, error) {
	if cfg.TableName == "" {
		cfg.TableName = DefaultTable
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.CreateTimeout <= 0 {
		cfg.CreateTimeout = defaultCreateTimeout
	}
	s := &Store{db: db, table: cfg.TableName, timeout: cfg.Timeout}
	if err := s.ensureTable(ctx, cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) cctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}

func (s *Store) ensureTable(ctx context.Context, cfg Config) error {
	cc, cancel := s.cctx(ctx)
	d, err := s.db.DescribeTable(cc, &dynamodb.DescribeTableInput{TableName: &s.table})
	cancel()
	var nf *types.ResourceNotFoundException
	switch {
	case errors.As(err, &nf):
		if !cfg.CreateTable {
			return fmt.Errorf("dynamostore: table %q not found (enable CreateTable to create it)", s.table)
		}
		cc, cancel := s.cctx(ctx)
		_, err := s.db.CreateTable(cc, &dynamodb.CreateTableInput{
			TableName:   &s.table,
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(aPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(aSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(aPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(aSK), KeyType: types.KeyTypeRange},
			},
		})
		cancel()
		var inUse *types.ResourceInUseException // created concurrently
		if err != nil && !errors.As(err, &inUse) {
			return fmt.Errorf("dynamostore: create table: %w", err)
		}
		return s.waitActive(ctx, cfg.CreateTimeout)
	case err != nil:
		return fmt.Errorf("dynamostore: describe table: %w", err)
	}
	if err := checkKeySchema(d.Table); err != nil {
		return fmt.Errorf("dynamostore: table %q: %w", s.table, err)
	}
	if d.Table.TableStatus != types.TableStatusActive {
		return s.waitActive(ctx, cfg.CreateTimeout)
	}
	return nil
}

func checkKeySchema(t *types.TableDescription) error {
	if t == nil || len(t.KeySchema) != 2 ||
		aws.ToString(t.KeySchema[0].AttributeName) != aPK || t.KeySchema[0].KeyType != types.KeyTypeHash ||
		aws.ToString(t.KeySchema[1].AttributeName) != aSK || t.KeySchema[1].KeyType != types.KeyTypeRange {
		return fmt.Errorf("unexpected key schema (want %s HASH, %s RANGE)", aPK, aSK)
	}
	return nil
}

func (s *Store) waitActive(ctx context.Context, max time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, max)
	defer cancel()
	for {
		cc, cc2 := s.cctx(ctx)
		d, err := s.db.DescribeTable(cc, &dynamodb.DescribeTableInput{TableName: &s.table})
		cc2()
		if err == nil && d.Table != nil && d.Table.TableStatus == types.TableStatusActive {
			return checkKeySchema(d.Table)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("dynamostore: waiting for table %q to become ACTIVE: %w", s.table, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Close is a no-op (the AWS client holds no resources that need closing).
func (s *Store) Close() error { return nil }

// --- keys and attribute helpers ---

func eventSK(seq uint64) string { return fmt.Sprintf("%s%0*d", eventPrefix, seqDigits, seq) }

func contSK(seq uint64, n int) string { return eventSK(seq) + contSep + strconv.Itoa(n) }

func strAV(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func numAV(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func u64AV(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

func getS(it map[string]types.AttributeValue, k string) string {
	if v, ok := it[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func getI(it map[string]types.AttributeValue, k string) int64 {
	if v, ok := it[k].(*types.AttributeValueMemberN); ok {
		n, _ := strconv.ParseInt(v.Value, 10, 64)
		return n
	}
	return 0
}

func getU(it map[string]types.AttributeValue, k string) uint64 {
	if v, ok := it[k].(*types.AttributeValueMemberN); ok {
		n, _ := strconv.ParseUint(v.Value, 10, 64)
		return n
	}
	return 0
}

func noNUL(s string) string { return strings.ReplaceAll(s, "\x00", "") }

func checkSession(sid string) error {
	if sid == "" || len(sid) > maxKeyBytes {
		return fmt.Errorf("dynamostore: invalid session id (want 1..%d bytes)", maxKeyBytes)
	}
	return nil
}

// --- event encoding ---

// encoded is one event as ready-to-write items (first item, then
// continuations) plus an estimate of its request size.
type encoded struct {
	items []map[string]types.AttributeValue
	size  int
}

func gz(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzip(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func encodeEvent(sid string, e *core.Event) (encoded, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return encoded{}, err
	}
	body, err := gz(raw)
	if err != nil {
		return encoded{}, err
	}
	if len(body) > MaxEventBytes {
		return encoded{}, fmt.Errorf("dynamostore: event seq %d too large: %d bytes gzipped (max %d)", e.Seq, len(body), MaxEventBytes)
	}
	var parts [][]byte
	for len(body) > chunkBytes {
		parts = append(parts, body[:chunkBytes])
		body = body[chunkBytes:]
	}
	parts = append(parts, body)

	first := map[string]types.AttributeValue{
		aPK:      strAV(sid),
		aSK:      strAV(eventSK(e.Seq)),
		aSeq:     u64AV(e.Seq),
		aKind:    strAV(noNUL(string(e.Kind))),
		aStartNs: numAV(e.StartNs),
		aEndNs:   numAV(e.EndNs),
		aBody:    &types.AttributeValueMemberB{Value: parts[0]},
	}
	if e.Service != "" {
		first[aService] = strAV(noNUL(e.Service))
	}
	if eff, decides := store.TxResult(e); decides && e.TxID != "" {
		first[aTxID] = strAV(noNUL(e.TxID))
		first[aTxEff] = &types.AttributeValueMemberBOOL{Value: eff}
	}
	if len(parts) > 1 {
		first[aChunks] = numAV(int64(len(parts) - 1))
	}
	enc := encoded{items: []map[string]types.AttributeValue{first}}
	enc.size = itemOverhead + len(sid) + len(parts[0]) + len(e.Service) + len(e.TxID)
	for i, p := range parts[1:] {
		enc.items = append(enc.items, map[string]types.AttributeValue{
			aPK:   strAV(sid),
			aSK:   strAV(contSK(e.Seq, i+1)),
			aBody: &types.AttributeValueMemberB{Value: p},
		})
		enc.size += itemOverhead + len(sid) + len(p)
	}
	return enc, nil
}

// --- AppendEvents ---

type meta struct {
	exists          bool
	head, count     uint64
	firstNs, lastNs int64
}

func (s *Store) getMeta(ctx context.Context, sid string) (meta, error) {
	cc, cancel := s.cctx(ctx)
	defer cancel()
	out, err := s.db.GetItem(cc, &dynamodb.GetItemInput{
		TableName:      &s.table,
		ConsistentRead: aws.Bool(true),
		Key:            map[string]types.AttributeValue{aPK: strAV(sid), aSK: strAV(metaSK)},
	})
	if err != nil {
		return meta{}, fmt.Errorf("dynamostore: get meta: %w", err)
	}
	if len(out.Item) == 0 {
		return meta{}, nil
	}
	return meta{exists: true, head: getU(out.Item, aHead), count: getU(out.Item, aCount),
		firstNs: getI(out.Item, aFirstNs), lastNs: getI(out.Item, aLastNs)}, nil
}

func (s *Store) AppendEvents(ctx context.Context, sid string, evs []core.Event) error {
	if len(evs) == 0 {
		return nil
	}
	if err := checkSession(sid); err != nil {
		return err
	}
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			return store.ErrSeqConflict
		}
	}
	// Encode everything first so an oversized event fails before any write.
	enc := make([]encoded, len(evs))
	for i := range evs {
		var err error
		if enc[i], err = encodeEvent(sid, &evs[i]); err != nil {
			return err
		}
	}
	m, err := s.getMeta(ctx, sid)
	if err != nil {
		return err
	}
	if evs[0].Seq != m.head+1 {
		return store.ErrSeqConflict
	}
	for lo := 0; lo < len(evs); {
		hi, actions, size := lo, 1, itemOverhead // 1 action for META
		for hi < len(evs) {
			n := len(enc[hi].items)
			if hi > lo && (actions+n > maxTxActions || size+enc[hi].size > txBudget) {
				break
			}
			actions += n
			size += enc[hi].size
			hi++
		}
		var err error
		if m, err = s.appendChunk(ctx, sid, m, evs[lo:hi], enc[lo:hi]); err != nil {
			return err
		}
		lo = hi
	}
	return nil
}

// appendChunk writes evs (continuing m.head) in one transaction and returns
// the updated meta.
func (s *Store) appendChunk(ctx context.Context, sid string, m meta, evs []core.Event, enc []encoded) (meta, error) {
	next := m
	next.exists = true
	next.head = evs[len(evs)-1].Seq
	next.count += uint64(len(evs))
	for i := range evs {
		if (!m.exists && i == 0) || evs[i].StartNs < next.firstNs {
			next.firstNs = evs[i].StartNs
		}
		if evs[i].EndNs > next.lastNs {
			next.lastNs = evs[i].EndNs
		}
	}
	upd := &types.Update{
		TableName:        &s.table,
		Key:              map[string]types.AttributeValue{aPK: strAV(sid), aSK: strAV(metaSK)},
		UpdateExpression: aws.String("SET #head = :h, #cnt = :c, #first = :f, #last = :l"),
		ExpressionAttributeNames: map[string]string{
			"#pk": aPK, "#head": aHead, "#cnt": aCount, "#first": aFirstNs, "#last": aLastNs,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":h": u64AV(next.head), ":c": u64AV(next.count), ":f": numAV(next.firstNs), ":l": numAV(next.lastNs),
		},
	}
	if m.exists {
		upd.ConditionExpression = aws.String("#head = :exp")
		upd.ExpressionAttributeValues[":exp"] = u64AV(m.head)
	} else {
		upd.ConditionExpression = aws.String("attribute_not_exists(#pk)")
	}
	acts := []types.TransactWriteItem{{Update: upd}}
	for _, e := range enc {
		for _, it := range e.items {
			acts = append(acts, types.TransactWriteItem{Put: &types.Put{
				TableName:                &s.table,
				Item:                     it,
				ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
				ExpressionAttributeNames: map[string]string{"#pk": aPK},
			}})
		}
	}
	cc, cancel := s.cctx(ctx)
	defer cancel()
	if _, err := s.db.TransactWriteItems(cc, &dynamodb.TransactWriteItemsInput{TransactItems: acts}); err != nil {
		if isConflict(err) {
			return m, store.ErrSeqConflict
		}
		return m, fmt.Errorf("dynamostore: append: %w", err)
	}
	return next, nil
}

// isConflict reports whether err means another writer won: a failed
// condition, or a concurrent transaction touching the same META item.
func isConflict(err error) bool {
	var tc *types.TransactionCanceledException
	if errors.As(err, &tc) {
		for _, r := range tc.CancellationReasons {
			switch aws.ToString(r.Code) {
			case "ConditionalCheckFailed", "TransactionConflict":
				return true
			}
		}
		return false
	}
	var tx *types.TransactionConflictException
	return errors.As(err, &tx)
}

// --- reads ---

func seqRange(sid string, from uint64) (cond string, names map[string]string, vals map[string]types.AttributeValue) {
	return "#pk = :pk AND #sk BETWEEN :lo AND :hi",
		map[string]string{"#pk": aPK, "#sk": aSK},
		map[string]types.AttributeValue{":pk": strAV(sid), ":lo": strAV(eventSK(from)), ":hi": strAV(eventEnd)}
}

// partial is an event whose continuation items are still being read.
type partial struct {
	seq   string // sk of the first item
	parts [][]byte
	want  int // total items expected
}

func (s *Store) Events(ctx context.Context, sid string, from uint64, limit int) ([]core.Event, error) {
	if err := checkSession(sid); err != nil {
		return nil, err
	}
	cond, names, vals := seqRange(sid, from)
	names["#body"], names["#chunks"] = aBody, aChunks
	in := &dynamodb.QueryInput{
		TableName:                 &s.table,
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    &cond,
		ProjectionExpression:      aws.String("#sk, #body, #chunks"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: vals,
	}
	var out []core.Event
	var cur *partial
	for {
		if limit > 0 {
			in.Limit = aws.Int32(int32(min(limit-len(out), 1000)))
		}
		cc, cancel := s.cctx(ctx)
		page, err := s.db.Query(cc, in)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dynamostore: query events: %w", err)
		}
		for _, it := range page.Items {
			sk := getS(it, aSK)
			body, _ := it[aBody].(*types.AttributeValueMemberB)
			if body == nil {
				return nil, fmt.Errorf("dynamostore: corrupt item %s/%s: no body", sid, sk)
			}
			if base, _, isCont := strings.Cut(sk, contSep); isCont {
				if cur == nil || cur.seq != base {
					return nil, fmt.Errorf("dynamostore: corrupt session %s: orphan continuation %s", sid, sk)
				}
				cur.parts = append(cur.parts, body.Value)
			} else {
				if cur != nil {
					return nil, fmt.Errorf("dynamostore: corrupt session %s: event %s truncated", sid, cur.seq)
				}
				cur = &partial{seq: sk, parts: [][]byte{body.Value}, want: 1 + int(getI(it, aChunks))}
			}
			if len(cur.parts) < cur.want {
				continue
			}
			e, err := decodeEvent(cur.parts)
			if err != nil {
				return nil, fmt.Errorf("dynamostore: decode %s/%s: %w", sid, cur.seq, err)
			}
			cur = nil
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		if len(page.LastEvaluatedKey) == 0 {
			break
		}
		in.ExclusiveStartKey = page.LastEvaluatedKey
	}
	if cur != nil {
		return nil, fmt.Errorf("dynamostore: corrupt session %s: event %s truncated", sid, cur.seq)
	}
	return out, nil
}

func decodeEvent(parts [][]byte) (core.Event, error) {
	var e core.Event
	body := parts[0]
	if len(parts) > 1 {
		body = bytes.Join(parts, nil)
	}
	raw, err := gunzip(body)
	if err != nil {
		return e, err
	}
	return e, json.Unmarshal(raw, &e)
}

func (s *Store) Head(ctx context.Context, sid string) (uint64, error) {
	if err := checkSession(sid); err != nil {
		return 0, err
	}
	cc, cancel := s.cctx(ctx)
	defer cancel()
	out, err := s.db.GetItem(cc, &dynamodb.GetItemInput{
		TableName:                &s.table,
		ConsistentRead:           aws.Bool(true),
		Key:                      map[string]types.AttributeValue{aPK: strAV(sid), aSK: strAV(metaSK)},
		ProjectionExpression:     aws.String("#head"),
		ExpressionAttributeNames: map[string]string{"#head": aHead},
	})
	if err != nil {
		return 0, fmt.Errorf("dynamostore: head: %w", err)
	}
	return getU(out.Item, aHead), nil
}

func (s *Store) TxOutcomes(ctx context.Context, sid string) (map[string]bool, error) {
	if err := checkSession(sid); err != nil {
		return nil, err
	}
	cond, names, vals := seqRange(sid, 0)
	names["#tx_id"], names["#tx_eff"] = aTxID, aTxEff
	in := &dynamodb.QueryInput{
		TableName:                 &s.table,
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    &cond,
		FilterExpression:          aws.String("attribute_exists(#tx_eff)"),
		ProjectionExpression:      aws.String("#tx_id, #tx_eff"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: vals,
	}
	out := map[string]bool{}
	for {
		cc, cancel := s.cctx(ctx)
		page, err := s.db.Query(cc, in)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dynamostore: query tx outcomes: %w", err)
		}
		for _, it := range page.Items {
			eff, ok := it[aTxEff].(*types.AttributeValueMemberBOOL)
			if id := getS(it, aTxID); ok && id != "" {
				store.MergeTxOutcome(out, id, eff.Value)
			}
		}
		if len(page.LastEvaluatedKey) == 0 {
			return out, nil
		}
		in.ExclusiveStartKey = page.LastEvaluatedKey
	}
}

// ListSessions scans the whole table filtered to META and SEAL items. A Scan
// reads every item (events included) and is billed for it, which is fine for
// a test/inspection tool with a handful of sessions but does not scale to a
// large production table; a GSI on a constant partition would, at the cost of
// a hot key and an extra write per session.
func (s *Store) ListSessions(ctx context.Context) ([]store.SessionInfo, error) {
	in := &dynamodb.ScanInput{
		TableName:                &s.table,
		ConsistentRead:           aws.Bool(true),
		FilterExpression:         aws.String("#sk IN (:m, :s)"),
		ProjectionExpression:     aws.String("#pk, #sk, #head, #cnt, #first, #last"),
		ExpressionAttributeNames: map[string]string{"#pk": aPK, "#sk": aSK, "#head": aHead, "#cnt": aCount, "#first": aFirstNs, "#last": aLastNs},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":m": strAV(metaSK), ":s": strAV(sealSK),
		},
	}
	infos := map[string]*store.SessionInfo{}
	sealed := map[string]bool{}
	for {
		cc, cancel := s.cctx(ctx)
		page, err := s.db.Scan(cc, in)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dynamostore: scan sessions: %w", err)
		}
		for _, it := range page.Items {
			id := getS(it, aPK)
			if getS(it, aSK) == sealSK {
				sealed[id] = true
				continue
			}
			infos[id] = &store.SessionInfo{ID: id, EventCount: int(getI(it, aCount)),
				FirstNs: getI(it, aFirstNs), LastNs: getI(it, aLastNs)}
		}
		if len(page.LastEvaluatedKey) == 0 {
			break
		}
		in.ExclusiveStartKey = page.LastEvaluatedKey
	}
	var out []store.SessionInfo
	for id, si := range infos {
		si.Sealed = sealed[id]
		out = append(out, *si)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) GetSeal(ctx context.Context, sid string) (*store.SealInfo, error) {
	if err := checkSession(sid); err != nil {
		return nil, err
	}
	cc, cancel := s.cctx(ctx)
	defer cancel()
	out, err := s.db.GetItem(cc, &dynamodb.GetItemInput{
		TableName:      &s.table,
		ConsistentRead: aws.Bool(true),
		Key:            map[string]types.AttributeValue{aPK: strAV(sid), aSK: strAV(sealSK)},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamostore: get seal: %w", err)
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	si := &store.SealInfo{BundleURI: getS(out.Item, aBundleURI), Quarantined: int(getI(out.Item, aQuarantined)), Orphaned: []string{}}
	if b, ok := out.Item[aIncomplete].(*types.AttributeValueMemberBOOL); ok {
		si.Incomplete = b.Value
	}
	if l, ok := out.Item[aOrphaned].(*types.AttributeValueMemberL); ok {
		for _, v := range l.Value {
			if sv, ok := v.(*types.AttributeValueMemberS); ok {
				si.Orphaned = append(si.Orphaned, sv.Value)
			}
		}
	}
	if t, err := time.Parse(timeFormat, getS(out.Item, aSealedAt)); err == nil {
		si.SealedAt = t
	}
	return si, nil
}

// PutSeal overwrites the session's SEAL item, so repeating it is harmless.
func (s *Store) PutSeal(ctx context.Context, sid string, si store.SealInfo) error {
	if err := checkSession(sid); err != nil {
		return err
	}
	orph := make([]types.AttributeValue, 0, len(si.Orphaned))
	for _, o := range si.Orphaned {
		orph = append(orph, strAV(noNUL(o)))
	}
	item := map[string]types.AttributeValue{
		aPK:          strAV(sid),
		aSK:          strAV(sealSK),
		aIncomplete:  &types.AttributeValueMemberBOOL{Value: si.Incomplete},
		aOrphaned:    &types.AttributeValueMemberL{Value: orph},
		aBundleURI:   strAV(noNUL(si.BundleURI)),
		aSealedAt:    strAV(si.SealedAt.UTC().Format(timeFormat)),
		aQuarantined: numAV(int64(si.Quarantined)),
	}
	cc, cancel := s.cctx(ctx)
	defer cancel()
	if _, err := s.db.PutItem(cc, &dynamodb.PutItemInput{TableName: &s.table, Item: item}); err != nil {
		return fmt.Errorf("dynamostore: put seal: %w", err)
	}
	return nil
}
