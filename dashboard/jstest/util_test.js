'use strict';
// Run: node dashboard/jstest/util_test.js (also invoked by `go test ./dashboard` when node is on PATH).
const assert = require('assert');
const U = require('../web/util.js');

assert.strictEqual(U.pkLabel('{"id":1}'), 'id=1');
assert.strictEqual(U.pkLabel('{"b":2,"a":"x"}'), 'a=x, b=2');
assert.strictEqual(U.pkLabel({ id: 7 }), 'id=7');
assert.strictEqual(U.pkLabel('42'), '42');
assert.strictEqual(U.pkLabel('{bad'), '{bad');
assert.strictEqual(U.pkRaw({ b: 1, a: 2 }), '{"a":2,"b":1}');
assert.strictEqual(U.pkRaw('{"id":1}'), '{"id":1}');

assert.ok(U.isRedactedValue('[REDACTED]'));
assert.ok(U.isRedactedValue('"[REDACTED]"'));
assert.ok(!U.isRedactedValue('card [REDACTED]'));
assert.ok(!U.isRedactedValue(5));

// bad timestamps must not throw
for (const bad of [NaN, Infinity, 1e30, -1e30, undefined, null, 'x']) assert.strictEqual(U.isoFromNs(bad), null);
assert.strictEqual(U.isoFromNs(1.7e18), new Date(1.7e12).toISOString());
assert.ok(U.unsafeNs(1.7e18) && !U.unsafeNs(1e15));
assert.strictEqual(U.fmtNs(NaN), '-');
assert.strictEqual(U.fmtNs(1500000), '1.5 ms');
assert.ok(Math.abs(U.relNs(1.7e18 + 3e6, 1.7e18) - 3e6) < 1e3);
assert.ok(isNaN(U.relNs(undefined, 1)));

// merge: newest copy replaces stale (effective null -> false)
const D = { events: [], bySeq: new Map(), bySpan: new Map(), maxSeq: 0, t0: 0 };
const ev = (seq, eff) => ({ seq: seq, span_id: 's' + seq, start_ns: 100 + seq, tx_id: 't', mutations: [{ effective: eff }] });
let r = U.mergeEvents(D, [ev(1, null), ev(2, null)]);
assert.deepStrictEqual(r, { added: 2, changed: 0 });
assert.ok(U.hasUnresolved(D.events));
r = U.mergeEvents(D, [ev(1, false), ev(2, true), ev(3, true)]);
assert.deepStrictEqual(r, { added: 1, changed: 2 });
assert.ok(!U.hasUnresolved(D.events));
assert.strictEqual(D.bySeq.get(1).mutations[0].effective, false);
assert.strictEqual(D.events.length, 3);
assert.strictEqual(D.maxSeq, 3);
r = U.mergeEvents(D, [ev(1, false)]);
assert.deepStrictEqual(r, { added: 0, changed: 0 });

assert.strictEqual(U.ndjson([{ a: 1 }, { b: 2 }]), '{"a":1}\n{"b":2}\n');
assert.strictEqual(U.ndjson([]), '');
assert.deepStrictEqual(U.sessionWarnings({}), []);
assert.strictEqual(U.sessionWarnings({ seal_error: 'x', incomplete: true, dropped_events: 3 }).length, 2);
assert.strictEqual(U.kindGroup('api.in'), 'api');
assert.strictEqual(U.kindGroup('db.tx.commit'), 'tx');
assert.strictEqual(U.kindGroup('db.write'), 'db');
assert.strictEqual(U.kindGroup('worker.retry'), 'retry');
assert.strictEqual(U.kindGroup('worker.dlq'), 'dlq');
assert.strictEqual(U.kindGroup('sdk.dropped'), 'sys');
assert.strictEqual(U.kindGroup(undefined), 'sys');
assert.strictEqual(U.fmtAgo(1000), 'just now');
assert.strictEqual(U.fmtAgo(125000), '2m ago');
assert.strictEqual(U.fmtAgo(-5), 'just now');
assert.strictEqual(U.fmtAgo(NaN), '-');
assert.strictEqual(U.opClass('UPDATE'), 'update');
assert.strictEqual(U.opClass('expire'), 'other');

// ---------- trigger grouping ----------
let sq = 0;
const E = (span, kind, o) => Object.assign({ seq: ++sq, span_id: span, kind: kind, name: kind, start_ns: 1e12 + sq * 1e6, end_ns: 1e12 + sq * 1e6 + 5e5, hlc: 1e12 + sq * 1e6 }, o || {});
const keysOf = (r) => r.groups.map((g) => g.key);

// api.in completes LAST (seq order) but is the trigger and sorts first by start
sq = 0;
const kid1 = E('c1', 'db.read', { parent_id: 'api1', start_ns: 2e12 + 10, hlc: 2e12 + 10 });
const kid2 = E('c2', 'db.write', { parent_id: 'api1', start_ns: 2e12 + 20, hlc: 2e12 + 20, tx_id: 't1', mutations: [{ store: 'mysql', entity: 'bad_stock_container', op: 'insert', effective: true }] });
const kid2b = E('c2b', 'db.write', { parent_id: 'api1', start_ns: 2e12 + 21, hlc: 2e12 + 21, tx_id: 't1', mutations: [{ store: 'mysql', entity: 'sequence_generator', op: 'update', effective: true }] });
const kid3 = E('c3', 'cache.read', { parent_id: 'c1', start_ns: 2e12 + 12, hlc: 2e12 + 12 });
const apiEv = E('api1', 'api.in', { name: 'POST /api/bad-stock/v1/drop-zone', start_ns: 2e12, hlc: 2e12, end_ns: 2e12 + 100 });
r = U.groupEvents([kid1, kid2, kid2b, kid3, apiEv]);
assert.strictEqual(r.groups.length, 1);
let g = r.groups[0];
assert.strictEqual(g.key, 'api1');
assert.strictEqual(g.kind, 'api');
assert.strictEqual(g.title, 'POST /api/bad-stock/v1/drop-zone');
assert.deepStrictEqual(g.rows.map((x) => x.e.span_id), ['api1', 'c1', 'c3', 'c2', 'c2b']); // by start, parent before child, not by seq
assert.deepStrictEqual(g.rows.map((x) => x.depth), [0, 1, 2, 1, 1]);
assert.ok(apiEv.seq > kid1.seq, 'trigger completed after its children');
assert.strictEqual(g.rows[0].e, apiEv);
assert.strictEqual(g.mutText, 'mysql: bad_stock_container +1, sequence_generator ~1');
assert.strictEqual(U.chipsText(g), '1 db.read \u00b7 2 db.write \u00b7 1 cache');
assert.strictEqual(g.dur, 100);
assert.strictEqual(g.status, 'ok');
assert.strictEqual(g.rows[2].tx, undefined);
assert.deepStrictEqual(g.rows.filter((x) => x.tx).map((x) => x.tx.pos), ['first', 'last']);
assert.strictEqual(r.keyOf.get(kid3.seq), 'api1');

// errors: any child error or api 5xx => group error
const kidErr = Object.assign({}, kid1, { err: 'boom' });
assert.strictEqual(U.groupEvents([kidErr, apiEv]).groups[0].status, 'error');
assert.strictEqual(U.groupEvents([kid1, Object.assign({}, apiEv, { attrs: { 'http.status': '503' } })]).groups[0].status, 'error');
assert.strictEqual(U.groupEvents([kid1, Object.assign({}, apiEv, { resp: { inline: { status: 500 } } })]).groups[0].status, 'error');
assert.strictEqual(U.groupEvents([kid1, Object.assign({}, apiEv, { attrs: { 'http.status': '201' } })]).groups[0].status, 'ok');

// input order must not matter
const perm = [apiEv, kid3, kid2b, kid1, kid2].sort(() => 0.5 - Math.random());
assert.deepStrictEqual(U.groupEvents(perm).groups[0].rows.map((x) => x.e.span_id), ['api1', 'c1', 'c3', 'c2', 'c2b']);

// rolled back / unresolved flags
const rbEv = E('rb', 'db.write', { parent_id: 'api1', tx_id: 'tx9', mutations: [{ store: 'pg', entity: 'x', op: 'update', effective: false }] });
const unEv = E('un', 'db.write', { parent_id: 'api1', tx_id: 'tx8', mutations: [{ store: 'pg', entity: 'y', op: 'delete' }] });
g = U.groupEvents([apiEv, rbEv, unEv]).groups[0];
assert.deepStrictEqual(g.flags, ['rolled_back', 'tx_unresolved']);
assert.strictEqual(g.mutText, 'pg: x ~1 (1 rolled back), y -1');

// worker is its OWN group, linked to the publisher (not nested under it); retries counted
sq = 0;
const a2 = E('a2', 'api.in', { name: 'POST /jobs', start_ns: 3e12, hlc: 3e12 });
const pub = E('p2', 'queue.publish', { name: 'orders.fulfil', parent_id: 'a2', start_ns: 3e12 + 5, hlc: 3e12 + 5 });
const w1 = E('w1', 'worker.start', { name: 'orders.fulfil', attempt: 1, link_ids: ['p2'], start_ns: 3e12 + 100, hlc: 3e12 + 100, err: 'lock' });
const w1db = E('w1d', 'db.read', { parent_id: 'w1', start_ns: 3e12 + 110, hlc: 3e12 + 110 });
const w1r = E('w1r', 'worker.retry', { parent_id: 'w1', start_ns: 3e12 + 150, hlc: 3e12 + 150 });
const w2 = E('w2', 'worker.start', { name: 'orders.fulfil', attempt: 2, link_ids: ['p2'], start_ns: 3e12 + 900, hlc: 3e12 + 900 });
r = U.groupEvents([w2, w1r, w1db, w1, pub, a2]);
assert.deepStrictEqual(keysOf(r), ['a2', 'w1', 'w2']);
assert.deepStrictEqual(r.byKey.get('a2').rows.map((x) => x.e.span_id), ['a2', 'p2']);
assert.strictEqual(r.byKey.get('w1').title, 'worker orders.fulfil \u00b7 attempt 1');
assert.strictEqual(r.byKey.get('w1').asyncLabel, 'async \u00b7 triggered by POST /jobs (queue.publish orders.fulfil)');
assert.strictEqual(r.byKey.get('w1').retries, 1);
assert.strictEqual(r.byKey.get('w1').status, 'error');
assert.strictEqual(r.byKey.get('w2').triggeredBy.groupKey, 'a2');
assert.deepStrictEqual(r.pubWorkers.get('p2'), ['w1', 'w2']);
// publisher missing from the session: worker still its own group, label says so
r = U.groupEvents([w1]);
assert.strictEqual(r.groups[0].asyncLabel, 'async \u00b7 publisher not in this session');
// queue.consume is an async trigger too
assert.strictEqual(U.groupEvents([E('q1', 'queue.consume', { name: 'jobs', link_ids: ['zzz'] })]).groups[0].kind, 'consume');

// nested api.in (parent present, e.g. service-to-service) stays inside the outer group
sq = 0;
const outer = E('o1', 'api.in', { start_ns: 4e12, hlc: 4e12 });
const out1 = E('o2', 'api.out', { parent_id: 'o1', start_ns: 4e12 + 1, hlc: 4e12 + 1 });
const inner = E('o3', 'api.in', { parent_id: 'o2', start_ns: 4e12 + 2, hlc: 4e12 + 2 });
const innerDb = E('o4', 'db.read', { parent_id: 'o3', start_ns: 4e12 + 3, hlc: 4e12 + 3 });
r = U.groupEvents([innerDb, inner, out1, outer]);
assert.deepStrictEqual(keysOf(r), ['o1']);
assert.deepStrictEqual(r.groups[0].rows.map((x) => x.depth), [0, 1, 2, 3]);

// orphans: a root that is not api.in/worker.start becomes an 'untriggered' group (per top-level span)
sq = 0;
const cron = E('cr', 'db.write', { name: 'DELETE FROM drafts', start_ns: 5e12, hlc: 5e12 });
const cronKid = E('crk', 'cache.write', { parent_id: 'cr', start_ns: 5e12 + 1, hlc: 5e12 + 1 });
const cron2 = E('cr2', 'db.read', { name: 'SELECT 1', start_ns: 6e12, hlc: 6e12 });
r = U.groupEvents([cronKid, cron2, cron]);
assert.deepStrictEqual(keysOf(r), ['cr', 'cr2']);
assert.strictEqual(r.groups[0].kind, 'untriggered');
assert.strictEqual(r.groups[0].title, 'untriggered \u00b7 db.write DELETE FROM drafts');
assert.strictEqual(r.groups[0].rows.length, 2);

// missing parent: grouped by the missing span id, so the group keeps its key once the parent arrives (live tail)
sq = 0;
const lc1 = E('l1', 'db.read', { parent_id: 'later', start_ns: 7e12 + 10, hlc: 7e12 + 10 });
const lc2 = E('l2', 'db.write', { parent_id: 'later', start_ns: 7e12 + 20, hlc: 7e12 + 20 });
let live = U.groupEvents([lc1, lc2], { live: true });
assert.strictEqual(live.groups.length, 1);
assert.strictEqual(live.groups[0].key, 'later');
assert.strictEqual(live.groups[0].trigger, null);
assert.ok(/^awaiting trigger/.test(live.groups[0].title));
assert.ok(/^untriggered/.test(U.groupEvents([lc1, lc2], { live: false }).groups[0].title));
const later = E('later', 'api.in', { name: 'GET /late', start_ns: 7e12, hlc: 7e12, end_ns: 7e12 + 50 });
live = U.groupEvents([lc1, lc2, later], { live: true });
assert.strictEqual(live.groups.length, 1);
assert.strictEqual(live.groups[0].key, 'later');
assert.strictEqual(live.groups[0].kind, 'api');
assert.strictEqual(live.groups[0].rows[0].e, later);
// api.in with a missing parent is itself a trigger
assert.strictEqual(U.groupEvents([E('x1', 'api.in', { parent_id: 'gone' })]).groups[0].kind, 'api');

// cycles and long chains terminate and keep every event exactly once
sq = 0;
const cy = [E('cy1', 'db.read', { parent_id: 'cy3' }), E('cy2', 'db.read', { parent_id: 'cy1' }), E('cy3', 'db.read', { parent_id: 'cy2' }), E('cy4', 'db.read', { parent_id: 'cy1' })];
r = U.groupEvents(cy);
assert.strictEqual(r.groups.length, 1);
assert.strictEqual(r.groups[0].rows.length, 4);
assert.ok(/cycle/.test(r.groups[0].key));
const self = E('self', 'db.read', { parent_id: 'self' });
assert.strictEqual(U.groupEvents([self]).groups[0].rows.length, 1);
const chain = [];
for (let i = 0; i < 300; i++) chain.push(E('ch' + i, 'db.read', { parent_id: i ? 'ch' + (i - 1) : undefined }));
r = U.groupEvents(chain);
assert.strictEqual(r.groups.reduce((a, x) => a + x.rows.length, 0), 300);
assert.ok(r.groups.every((x) => x.rows.every((y) => y.depth <= 6)));
assert.deepStrictEqual(U.groupEvents([]).groups, []);

// ordering of groups: by trigger START (hlc, then start_ns), not by completion seq
sq = 0;
const late = E('gl', 'api.in', { name: 'late', start_ns: 9e12, hlc: 9e12 });   // seq 1 (completed first) but starts last
const early = E('ge', 'api.in', { name: 'early', start_ns: 8e12, hlc: 8e12 }); // seq 2
assert.deepStrictEqual(keysOf(U.groupEvents([late, early])), ['ge', 'gl']);
assert.ok(late.seq < early.seq);
// hlc wins over start_ns when both present; start_ns breaks hlc ties; seq breaks the rest
assert.ok(U.cmpStart({ hlc: 1, start_ns: 9, seq: 1 }, { hlc: 2, start_ns: 1, seq: 2 }) < 0);
assert.ok(U.cmpStart({ hlc: 5, start_ns: 1, seq: 2 }, { hlc: 5, start_ns: 2, seq: 1 }) < 0);
assert.ok(U.cmpStart({ start_ns: 1, seq: 1 }, { start_ns: 1, seq: 2 }) < 0);

// live update merge: regrouping after new events keeps keys; new events attach to the existing group
const D2 = { events: [], bySeq: new Map(), bySpan: new Map(), maxSeq: 0, t0: 0 };
sq = 0;
const m1 = E('m1', 'api.in', { name: 'GET /a', start_ns: 1e13, hlc: 1e13 });
const m1k = E('m1k', 'db.read', { parent_id: 'm1', start_ns: 1e13 + 1, hlc: 1e13 + 1 });
U.mergeEvents(D2, [m1k]);
let before = U.groupEvents(D2.events, { live: true });
assert.strictEqual(before.groups[0].key, 'm1');
U.mergeEvents(D2, [m1]);
U.mergeEvents(D2, [E('m1k2', 'db.write', { parent_id: 'm1', start_ns: 1e13 + 2, hlc: 1e13 + 2 })]);
const after = U.groupEvents(D2.events, { live: true });
assert.deepStrictEqual(keysOf(after), ['m1']);
assert.strictEqual(after.groups[0].rows.length, 3);
assert.strictEqual(after.groups[0].kind, 'api');
// a replaced event (newest copy wins) changes the summary in place
U.mergeEvents(D2, [Object.assign({}, D2.bySeq.get(m1k.seq), { err: 'late failure' })]);
assert.strictEqual(U.groupEvents(D2.events).groups[0].status, 'error');
assert.strictEqual(U.fmtClock(NaN), '-');
assert.ok(/^\d\d:\d\d:\d\d\.\d{3}$/.test(U.fmtClock(1.7e18)));

// session naming helpers
assert.strictEqual(U.entryOf([]), null);
const en = U.entryOf([apiEv, kid1, w1, w2, E('zz', 'api.in', { name: 'GET /two', start_ns: 5e13, hlc: 5e13 })]);
assert.strictEqual(en.title, 'POST /api/bad-stock/v1/drop-zone');
assert.strictEqual(en.more, 3);
assert.strictEqual(U.entryText(en), 'entry: POST /api/bad-stock/v1/drop-zone (+3 more triggers)');
assert.strictEqual(U.entryText({ title: 'GET /a', more: 1 }), 'entry: GET /a (+1 more trigger)');
assert.strictEqual(U.entryText({ title: 'GET /a', more: 0 }), 'entry: GET /a');
assert.strictEqual(U.entryText(null), '');
assert.strictEqual(U.bindingText({ source: 'header', name: 'x-wms-uid', value: 'act_bind_9' }), 'x-wms-uid: act_bind_9');
const hay = U.sessionHay({ id: 'ts_1', name: 'N', bindings: [{ source: 'jwt_claim', name: 'sub', value: 'u_123' }] }, en);
assert.ok(hay.indexOf('u_123') >= 0 && hay.indexOf('drop-zone') >= 0 && hay.indexOf('jwt_claim') >= 0);

console.log('util ok');
