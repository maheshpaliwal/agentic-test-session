package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"tsession/core"
)

const pgSchema = `
CREATE TABLE IF NOT EXISTS sessions (
  id          text PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now(),
  sealed      boolean NOT NULL DEFAULT false,
  incomplete  boolean NOT NULL DEFAULT false,
  orphaned    jsonb NOT NULL DEFAULT '[]',
  bundle_uri  text NOT NULL DEFAULT '',
  sealed_at   timestamptz
);
CREATE TABLE IF NOT EXISTS session_events (
  session_id text   NOT NULL,
  seq        bigint NOT NULL,
  instance   text   NOT NULL,
  local_seq  bigint NOT NULL,
  span_id    text   NOT NULL,
  kind       text   NOT NULL,
  service    text   NOT NULL,
  tx_id      text   NOT NULL DEFAULT '',
  end_hlc    bigint NOT NULL,
  start_ns   bigint NOT NULL,
  end_ns     bigint NOT NULL,
  body       jsonb  NOT NULL,
  PRIMARY KEY (session_id, seq),
  UNIQUE (session_id, instance, local_seq)
);
CREATE INDEX IF NOT EXISTS session_events_tx ON session_events (session_id, tx_id) WHERE tx_id <> '';
CREATE TABLE IF NOT EXISTS entity_mutations (
  session_id text   NOT NULL,
  store      text   NOT NULL,
  entity     text   NOT NULL,
  pk         text   NOT NULL,
  seq        bigint NOT NULL,
  ord        int    NOT NULL,
  op         text   NOT NULL,
  body       jsonb  NOT NULL,
  PRIMARY KEY (session_id, store, entity, pk, seq, ord)
);
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS quarantined int NOT NULL DEFAULT 0;
`

// DefaultPGSchema is the schema trace tables live in unless configured otherwise.
const DefaultPGSchema = "tsession"

var schemaRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ValidateSchemaName strictly validates a Postgres schema identifier
// (lowercase ASCII letters, digits, underscore; max 63 bytes).
func ValidateSchemaName(s string) error {
	if !schemaRe.MatchString(s) {
		return fmt.Errorf("store: invalid pg schema %q (want [a-z_][a-z0-9_]{0,62})", s)
	}
	return nil
}

// PGConfig configures OpenPGConfig.
type PGConfig struct {
	DSN    string
	Schema string // default DefaultPGSchema
}

// PG is the Postgres Store.
type PG struct{ pool *pgxpool.Pool }

// OpenPG connects with the default schema and auto-migrates.
func OpenPG(ctx context.Context, dsn string) (*PG, error) {
	return OpenPGConfig(ctx, PGConfig{DSN: dsn})
}

// OpenPGConfig connects, creates the schema if needed, pins the pool's
// search_path to it (so tables never land in the app's default schema) and
// auto-migrates.
func OpenPGConfig(ctx context.Context, cfg PGConfig) (*PG, error) {
	schema := cfg.Schema
	if schema == "" {
		schema = DefaultPGSchema
	}
	if err := ValidateSchemaName(schema); err != nil {
		return nil, err
	}
	qs := pgx.Identifier{schema}.Sanitize()

	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	// Bootstrap: create the schema over a connection without the pinned path.
	bc, err := pgx.ConnectConfig(ctx, pc.ConnConfig.Copy())
	if err != nil {
		return nil, err
	}
	_, err = bc.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+qs)
	bc.Close(ctx) //nolint:errcheck
	if err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}

	if pc.ConnConfig.RuntimeParams == nil {
		pc.ConnConfig.RuntimeParams = map[string]string{}
	}
	pc.ConnConfig.RuntimeParams["search_path"] = qs
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &PG{pool: pool}, nil
}

// migrate runs the DDL under an advisory lock so concurrent collectors
// starting together do not race on CREATE TABLE IF NOT EXISTS.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('tsession.migrate'))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, pgSchema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

func noNUL(s string) string {
	if strings.IndexByte(s, 0) < 0 {
		return s
	}
	return strings.ReplaceAll(s, "\x00", "")
}

// entity_mutations is a write-only side index kept for ad-hoc SQL analysis
// and external tooling (it is also asserted by the integration suite). The
// collector's entities endpoints do NOT read it: they derive from the cached,
// tx-resolved event view, because Mutation.Effective depends on tx events
// that arrive independently of the mutation row.

// MutationPK renders a mutation's primary key canonically (sorted keys).
func MutationPK(pk map[string]any) string {
	b, _ := json.Marshal(pk)
	if len(pk) == 0 {
		return "{}"
	}
	return string(b)
}

func (p *PG) AppendEvents(ctx context.Context, sid string, evs []core.Event) error {
	if len(evs) == 0 {
		return nil
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var last *uint64
	if err := tx.QueryRow(ctx, `SELECT max(seq) FROM session_events WHERE session_id=$1`, sid).Scan(&last); err != nil {
		return err
	}
	want := uint64(1)
	if last != nil {
		want = *last + 1
	}
	for i := range evs {
		if evs[i].Seq != want+uint64(i) {
			return ErrSeqConflict
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sessions(id) VALUES($1) ON CONFLICT DO NOTHING`, sid); err != nil {
		return err
	}
	b := &pgx.Batch{}
	for i := range evs {
		e := &evs[i]
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		body = StripNUL(body) // jsonb rejects \u0000
		b.Queue(`INSERT INTO session_events(session_id,seq,instance,local_seq,span_id,kind,service,tx_id,end_hlc,start_ns,end_ns,body)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			sid, int64(e.Seq), noNUL(e.Instance), int64(e.LocalSeq), noNUL(e.SpanID), noNUL(string(e.Kind)), noNUL(e.Service), noNUL(e.TxID),
			int64(e.EndHLC), e.StartNs, e.EndNs, body)
		for j, m := range e.Mutations {
			mb, _ := json.Marshal(m)
			b.Queue(`INSERT INTO entity_mutations(session_id,store,entity,pk,seq,ord,op,body)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
				sid, noNUL(m.Store), noNUL(m.Entity), noNUL(MutationPK(m.PK)), int64(e.Seq), j, noNUL(m.Op), StripNUL(mb))
		}
	}
	br := tx.SendBatch(ctx, b)
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			if isUniqueViolation(err) {
				return ErrSeqConflict // concurrent writer / duplicate (instance, local_seq)
			}
			return err
		}
	}
	if err := br.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *PG) Events(ctx context.Context, sid string, from uint64, limit int) ([]core.Event, error) {
	q := `SELECT body FROM session_events WHERE session_id=$1 AND seq>=$2 ORDER BY seq`
	args := []any{sid, int64(from)}
	if limit > 0 {
		q += ` LIMIT $3`
		args = append(args, limit)
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Event
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var e core.Event
		if err := json.Unmarshal(body, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (p *PG) TxOutcomes(ctx context.Context, sid string) (map[string]bool, error) {
	rows, err := p.pool.Query(ctx, `SELECT tx_id, kind, COALESCE(body->>'err',''), COALESCE(body->'attrs'->>'commit_failed','')
		FROM session_events
		WHERE session_id=$1 AND tx_id<>'' AND kind IN ($2,$3) ORDER BY seq`,
		sid, string(core.KindDBTxCommit), string(core.KindDBTxRollbck))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id, kind, errS, cf string
		if err := rows.Scan(&id, &kind, &errS, &cf); err != nil {
			return nil, err
		}
		e := core.Event{Kind: core.Kind(kind), TxID: id, Err: errS}
		if cf != "" {
			e.Attrs = map[string]string{"commit_failed": cf}
		}
		txOutcome(&e, out)
	}
	return out, rows.Err()
}

func (p *PG) Head(ctx context.Context, sid string) (uint64, error) {
	var last *int64
	if err := p.pool.QueryRow(ctx, `SELECT max(seq) FROM session_events WHERE session_id=$1`, sid).Scan(&last); err != nil {
		return 0, err
	}
	if last == nil {
		return 0, nil
	}
	return uint64(*last), nil
}

func (p *PG) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	rows, err := p.pool.Query(ctx, `SELECT e.session_id, count(*), min(e.start_ns), max(e.end_ns), COALESCE(s.sealed,false)
		FROM session_events e LEFT JOIN sessions s ON s.id=e.session_id
		GROUP BY e.session_id, s.sealed ORDER BY e.session_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionInfo
	for rows.Next() {
		var si SessionInfo
		if err := rows.Scan(&si.ID, &si.EventCount, &si.FirstNs, &si.LastNs, &si.Sealed); err != nil {
			return nil, err
		}
		out = append(out, si)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, rows.Err()
}

func (p *PG) GetSeal(ctx context.Context, sid string) (*SealInfo, error) {
	var si SealInfo
	var orph []byte
	var sealed bool
	err := p.pool.QueryRow(ctx, `SELECT sealed, incomplete, orphaned, bundle_uri, COALESCE(sealed_at, now()), quarantined
		FROM sessions WHERE id=$1`, sid).Scan(&sealed, &si.Incomplete, &orph, &si.BundleURI, &si.SealedAt, &si.Quarantined)
	if err == pgx.ErrNoRows || (err == nil && !sealed) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(orph, &si.Orphaned)
	return &si, nil
}

func (p *PG) PutSeal(ctx context.Context, sid string, si SealInfo) error {
	orph, _ := json.Marshal(si.Orphaned)
	if si.Orphaned == nil {
		orph = []byte("[]")
	}
	_, err := p.pool.Exec(ctx, `INSERT INTO sessions(id,sealed,incomplete,orphaned,bundle_uri,sealed_at,quarantined)
		VALUES($1,true,$2,$3,$4,$5,$6)
		ON CONFLICT (id) DO UPDATE SET sealed=true, incomplete=$2, orphaned=$3, bundle_uri=$4, sealed_at=$5, quarantined=$6`,
		sid, si.Incomplete, orph, si.BundleURI, si.SealedAt, si.Quarantined)
	return err
}

func (p *PG) Close() error { p.pool.Close(); return nil }
