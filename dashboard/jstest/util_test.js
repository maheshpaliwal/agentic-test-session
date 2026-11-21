'use strict';
// Run: node dashboard/jstest/util_test.js (also invoked by `go test ./dashboard` when node is on PATH).
const assert = require('assert');
const U = require('../web/util.js');

assert.strictEqual(U.pkLabel('{"id":1}'), 'id=1');
assert.strictEqual(U.pkLabel('{"b":2,"a":"x"}'), 'a=x, b=2');
assert.strictEqual(U.pkLabel({ id: 7 }), 'id=7');
assert.strictEqual(U.pkLabel('42'), '42');
assert.strictEqual(U.pkLabel('{bad'), '{bad');
assert.strictEqual(U.pkLabel('{}'), '(key unknown)');
assert.strictEqual(U.pkLabel(undefined), '');

// An update with a before image and no after image has unknown new values; anything else is diffed as usual.
const img = { inline: { sku: 'widget', qty: 10 } };
assert.strictEqual(U.afterUnavailable({ op: 'update', before: img }), true);
assert.strictEqual(U.afterUnavailable({ op: 'update', before: img, diff: [{ field: 'qty', old: 10 }] }), true); // as sent by an older SDK
assert.strictEqual(U.afterUnavailable({ op: 'update', before: { blob_hash: 'abc' }, after: {} }), true);
assert.strictEqual(U.afterUnavailable({ op: 'update', before: img, after: img }), false);
assert.strictEqual(U.afterUnavailable({ op: 'update', after: img }), false);
assert.strictEqual(U.afterUnavailable({ op: 'update' }), false);
assert.strictEqual(U.afterUnavailable({ op: 'delete', before: img }), false);
assert.strictEqual(U.afterUnavailable(undefined), false);
assert.strictEqual(U.pkRaw({ b: 1, a: 2 }), '{"a":2,"b":1}');
// The diff table follows the diff the SDK recorded. A value that changed under redaction looks the same in both
// images: the recorded entry is what says it changed, and the row carries neither value.
{
  const red = { op: 'update', before: { inline: { id: 1, password: '[REDACTED]' } }, after: { inline: { id: 1, password: '[REDACTED]' } },
    diff: [{ field: 'password', old: '[REDACTED]', new: '[REDACTED]' }] };
  assert.deepStrictEqual(U.diffRows(red), [{ field: 'password', old: undefined, nw: undefined, kind: 'hid' },
    { field: 'id', old: '1', nw: '1', kind: 'same' }]);
  assert.ok(!JSON.stringify(U.diffRows(red)).includes(U.MARK), 'a hidden change shows neither value');
  // The same images without a recorded diff (an older SDK): nothing to tell, so the images are compared.
  assert.deepStrictEqual(U.diffRows({ op: 'update', before: red.before, after: red.after }).map((r) => r.kind), ['same', 'same']);
  assert.deepStrictEqual(U.diffRows({ op: 'update', before: red.before, after: red.after, diff: [] }).map((r) => r.kind), ['same', 'same']);
  // A redacted value on one side only is an added or a removed field, not a hidden change.
  assert.deepStrictEqual(U.diffRows({ op: 'insert', after: { inline: { token: '[REDACTED]' } }, diff: [{ field: 'token', new: '[REDACTED]' }] }),
    [{ field: 'token', old: undefined, nw: '"[REDACTED]"', kind: 'add' }]);
  // A Redis set: the images are two member lists, the diff names what was added and removed. The members are not
  // compared by position, so none is reported as unchanged or changed beside the recorded rows.
  const set = { op: 'update', before: { inline: ['a', 'b', 'c'] }, after: { inline: ['a', 'c', 'd'] },
    diff: [{ field: 'added', new: ['d'] }, { field: 'removed', old: ['b'] }] };
  assert.deepStrictEqual(U.diffRows(set), [{ field: 'added', old: undefined, nw: '["d"]', kind: 'add' },
    { field: 'removed', old: '["b"]', nw: undefined, kind: 'del' }]);
  // A list: the recorded indexes are the changes; the image rows they name are not repeated.
  const list = { op: 'update', before: { inline: ['x', 'y'] }, after: { inline: ['x', 'z'] }, diff: [{ field: '[1]', old: 'y', new: 'z' }] };
  assert.deepStrictEqual(U.diffRows(list).map((r) => r.field + ':' + r.kind), ['[1]:chg']);
  // A Redis string that changed under redaction: one hidden change, and no "unchanged" row for the value itself.
  assert.deepStrictEqual(U.diffRows({ op: 'update', before: { inline: '[REDACTED]' }, after: { inline: '[REDACTED]' },
    diff: [{ field: 'value', old: '[REDACTED]', new: '[REDACTED]' }] }), [{ field: 'value', old: undefined, nw: undefined, kind: 'hid' }]);
  // A nested field the diff names as a whole is not listed again leaf by leaf.
  const nested = { op: 'update', before: { inline: { id: 1, addr: { city: 'A', zip: '1' } } }, after: { inline: { id: 1, addr: { city: 'B', zip: '1' } } },
    diff: [{ field: 'addr', old: { city: 'A', zip: '1' }, new: { city: 'B', zip: '1' } }] };
  assert.deepStrictEqual(U.diffRows(nested).map((r) => r.field + ':' + r.kind), ['addr:chg', 'id:same']);
  // The diff omits a null value: the image tells "was null" from "was not there".
  const nul = { op: 'update', before: { inline: { note: null } }, after: { inline: { note: 'x' } }, diff: [{ field: 'note', new: 'x' }] };
  assert.deepStrictEqual(U.diffRows(nul), [{ field: 'note', old: 'null', nw: '"x"', kind: 'chg' }]);
  // A diff the SDK cut short is shown as recorded: the images do not add changes of their own.
  const cut = { op: 'update', before: { inline: { a: 1, b: 1 } }, after: { inline: { a: 2, b: 2 } }, diff: [{ field: 'a', old: 1, new: 2 }] };
  assert.deepStrictEqual(U.diffRows(cut).map((r) => r.field + ':' + r.kind), ['a:chg']);
  // No recorded diff: image against image, as before.
  assert.deepStrictEqual(U.diffRows({ op: 'update', before: { inline: { a: 1, b: 2 } }, after: { inline: { a: 1, c: 3 } } }).map((r) => r.field + ':' + r.kind),
    ['a:same', 'b:del', 'c:add']);
  assert.deepStrictEqual(U.diffRows({ op: 'delete' }), []);
  assert.deepStrictEqual(U.flatten({ a: { b: 1 }, c: [2], d: {} }, '', {}), { 'a.b': '1', 'c[0]': '2', d: '{}' });
}

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
assert.deepStrictEqual(U.sessionWarnings({ incomplete: true, incomplete_reasons: ['service a (instance i): 0 of 2 recorded events reached the collector'] }).slice(1),
  ['Events did not reach the collector: service a (instance i): 0 of 2 recorded events reached the collector']);
assert.deepStrictEqual(U.sessionWarnings({ incomplete: true, incomplete_reasons: ['the collector has 3 events from a/i, which did not report how many it recorded'] }).slice(1),
  ['Delivery is not accounted for: the collector has 3 events from a/i, which did not report how many it recorded']);
assert.deepStrictEqual(U.deliveryUnconfirmed({}), []);
assert.deepStrictEqual(U.deliveryUnconfirmed(null), []);
assert.deepStrictEqual(U.deliveryUnconfirmed({ delivery_unconfirmed: 'x' }), []);
assert.deepStrictEqual(U.deliveryUnconfirmed({ delivery_unconfirmed: ['service b (instance j): the collector has 9 events', '', 3] }),
  ['Delivery not confirmed: service b (instance j): the collector has 9 events']);
// The warning stands on its own: such a session is not incomplete.
assert.deepStrictEqual(U.sessionWarnings({ delivery_unconfirmed: ['x'] }), []);
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
assert.strictEqual(U.bindingText({ source: 'header', name: 'x-user-id', value: 'act_bind_9' }), 'x-user-id: act_bind_9');
const hay = U.sessionHay({ id: 'ts_1', name: 'N', bindings: [{ source: 'jwt_claim', name: 'sub', value: 'u_123' }] }, en);
assert.ok(hay.indexOf('u_123') >= 0 && hay.indexOf('drop-zone') >= 0 && hay.indexOf('jwt_claim') >= 0);

// ---------- copy as curl ----------
const httpEv = (kind, inline, extra) => Object.assign({ kind: kind, req: { inline: inline } }, extra || {});
const SKIP_NOTE = 'Left out headers that curl sets itself or that would attribute the replay to the recorded session: ';
const BASE_NOTE = 'The recorded URL has no scheme or host: set BASE_URL first, for example export BASE_URL=http://127.0.0.1:8080.';
const TYPE_NOTE = 'No Content-Type header was recorded: curl will send application/x-www-form-urlencoded.';

// plain GET (outbound, absolute URL, Go-style header arrays); computed and propagation headers are left out
const cGet = U.curlFor(httpEv('api.out', { method: 'GET', url: 'https://shipping.example.invalid/v1/labels?id=7',
  headers: { Accept: ['application/json'], 'Accept-Encoding': ['gzip'], Host: ['shipping.example.invalid'], Traceparent: ['00-aa-bb-01'], 'User-Agent': ['Go-http-client/1.1'], 'X-Tsession-Id': ['ts_1'], 'X-Tsession-Hlc': ['12'] } }));
assert.strictEqual(cGet.command, [
  "curl -X GET 'https://shipping.example.invalid/v1/labels?id=7' \\",
  "  -H 'Accept: application/json' \\",
  "  -H 'User-Agent: Go-http-client/1.1'"].join('\n'));
assert.deepStrictEqual(cGet.notes, [SKIP_NOTE + 'Accept-Encoding, Host, Traceparent, X-Tsession-Id, X-Tsession-Hlc.']);

// POST with a JSON body; repeated header values get one -H each
const cPost = U.curlFor(httpEv('api.out', { method: 'POST', url: 'https://shipping.example.invalid/v1/labels',
  headers: { 'Content-Type': ['application/json'], 'Content-Length': ['41'], 'X-Tag': ['a', 'b'] }, body: { order_id: 'ord_1001', service: 'ground' }, body_size: 41 }));
assert.strictEqual(cPost.command, [
  "curl -X POST 'https://shipping.example.invalid/v1/labels' \\",
  "  -H 'Content-Type: application/json' \\",
  "  -H 'X-Tag: a' \\",
  "  -H 'X-Tag: b' \\",
  "  --data-raw '{\"order_id\":\"ord_1001\",\"service\":\"ground\"}'"].join('\n'));
assert.deepStrictEqual(cPost.notes, [SKIP_NOTE + 'Content-Length.']);

// inbound: path-only URL behind a $BASE_URL placeholder; redacted header and body values stay in place (demo data shape)
const cIn = U.curlFor({ kind: 'api.in', flags: ['redacted'], req: { redacted: true, inline: { method: 'POST', path: '/orders?expand=items',
  headers: { authorization: '[REDACTED]', 'content-type': 'application/json', 'x-api-key': '[REDACTED]' },
  body: { customer_id: 'cus_synth_001', payment: { token: '[REDACTED]' } } } } });
assert.strictEqual(cIn.command, [
  "curl -X POST \"$BASE_URL\"'/orders?expand=items' \\",
  "  -H 'authorization: [REDACTED]' \\",
  "  -H 'content-type: application/json' \\",
  "  -H 'x-api-key: [REDACTED]' \\",
  "  --data-raw '{\"customer_id\":\"cus_synth_001\",\"payment\":{\"token\":\"[REDACTED]\"}}'"].join('\n'));
assert.deepStrictEqual(cIn.notes, [BASE_NOTE,
  'Not replayable as is: redacted headers still hold [REDACTED] (authorization, x-api-key). Put the real values back.',
  'Not replayable as is: the body holds redacted values ([REDACTED]).']);
// a redacted query value, and glob characters in the URL
const cQ = U.curlFor(httpEv('api.in', { method: 'HEAD', url: '/items?token=%5BREDACTED%5D&ids[]=1' }));
assert.strictEqual(cQ.command, "curl --head --globoff \"$BASE_URL\"'/items?token=%5BREDACTED%5D&ids[]=1'");
assert.deepStrictEqual(cQ.notes, [BASE_NOTE, 'Not replayable as is: the URL holds redacted values ([REDACTED]).']);

// hostile input: nothing recorded may leave its quotes
const hostile = httpEv('api.out', { method: 'PUT', url: 'https://h.example.invalid/a\'b?q=$(id)&r=`id`;x="y"',
  headers: {
    'X-Note': ['it\'s "q" `id` $(id); echo $HOME \\ !'],
    'X-Quote\'Name': ['v'],
    'X-Empty': [''],
    'X-Split': ['a\r\nInjected: 1'],
    'X-Tab': ['a\tb'],
    'X Bad': ['v'],
    '@/etc/passwd': ['v'],
    'X-Colon:': ['v'],
    'X-Num': [5],
  }, body: 'a\'b\n$(reboot) `id` "$HOME"' });
const cHost = U.curlFor(hostile);
assert.strictEqual(cHost.command, [
  "curl -X PUT 'https://h.example.invalid/a'\\''b?q=$(id)&r=`id`;x=\"y\"' \\",
  "  -H 'X-Note: it'\\''s \"q\" `id` $(id); echo $HOME \\ !' \\",
  "  -H 'X-Quote'\\''Name: v' \\",
  "  -H 'X-Empty;' \\",
  "  --data-raw 'a'\\''b\n$(reboot) `id` \"$HOME\"'"].join('\n'));
assert.deepStrictEqual(cHost.notes, [
  'Dropped headers whose name or value cannot be passed to a shell safely: "X-Split", "X-Tab", "X Bad", "@/etc/passwd", "X-Colon:", "X-Num".',
  TYPE_NOTE]);
// control characters other than a newline in the body: ANSI-C quoting with every special character escaped
const cAnsi = U.curlFor(httpEv('api.out', { method: 'POST', url: 'http://h.example.invalid/', headers: { 'Content-Type': 'text/plain' }, body: 'it\'s\r\n\u001b[201~ \\ ! \u0085\tend' }));
assert.strictEqual(cAnsi.command, [
  "curl -X POST 'http://h.example.invalid/' \\",
  "  -H 'Content-Type: text/plain' \\",
  "  --data-raw $'it\\x27s\\r\\n\\x1b[201~ \\\\ \\x21 \\xc2\\x85\\tend'"].join('\n'));
assert.deepStrictEqual(cAnsi.notes, ["The body has control characters, so it is quoted as $'...': run the command in bash or zsh."]);
const cNul = U.curlFor(httpEv('api.out', { method: 'POST', url: 'http://h.example.invalid/', body: 'a\u0000b' }));
assert.strictEqual(cNul.command, "curl -X POST 'http://h.example.invalid/'");
assert.deepStrictEqual(cNul.notes, ['The body contains a NUL byte, which no shell argument can carry: it was left out.']);
// what a real shell makes of the commands: curl is replaced by a function that prints its arguments
(function shellRoundTrip() {
  const cp = require('child_process');
  const argv = (cmd) => {
    const r = cp.spawnSync('bash', ['-c', 'curl() { printf \'%s\\0\' "$@"; }\n' + cmd], { env: { PATH: process.env.PATH, BASE_URL: 'http://127.0.0.1:1', HOME: '/nonexistent' }, encoding: 'utf8' });
    if (r.error || r.status !== 0) return null;
    return r.stdout.split('\u0000').slice(0, -1);
  };
  if (!argv("curl 'x'")) { console.log('bash not available: shell round trip skipped'); return; }
  assert.deepStrictEqual(argv(cHost.command), ['-X', 'PUT', hostile.req.inline.url,
    '-H', 'X-Note: it\'s "q" `id` $(id); echo $HOME \\ !', '-H', 'X-Quote\'Name: v', '-H', 'X-Empty;', '--data-raw', hostile.req.inline.body]);
  assert.deepStrictEqual(argv(cAnsi.command), ['-X', 'POST', 'http://h.example.invalid/', '-H', 'Content-Type: text/plain', '--data-raw', 'it\'s\r\n\u001b[201~ \\ ! \u0085\tend']);
  assert.deepStrictEqual(argv(cIn.command).slice(0, 3), ['-X', 'POST', 'http://127.0.0.1:1/orders?expand=items']);
})();
// A shell without $'...' (dash, POSIX sh) must still see exactly one curl invocation: a body with a control
// character and a quote once ended the quoted word there and ran the rest as a command.
(function posixShellRunsOneCommand() {
  const cp = require('child_process');
  const evil = U.curlFor(httpEv('api.out', { method: 'POST', url: 'http://h.example.invalid/', headers: { 'Content-Type': 'text/plain' }, body: '\u0001\' ; echo INJECTED #' }));
  assert.ok(evil.command.indexOf("\\'") < 0, 'a backslash-quote must never be emitted: ' + evil.command);
  for (const sh of ['/bin/dash', '/bin/sh']) {
    const r = cp.spawnSync(sh, ['-c', 'curl() { echo CURL; }\n' + evil.command], { env: { PATH: process.env.PATH, HOME: '/nonexistent' }, encoding: 'utf8' });
    if (r.error) { console.log(sh + ' not available: skipped'); continue; }
    assert.strictEqual(r.stdout, 'CURL\n', sh + ' ran something other than one curl: ' + JSON.stringify(r.stdout));
  }
})();

// truncated body: the recorded part stays, the note says how much is missing
const cTrunc = U.curlFor(httpEv('api.in', { method: 'POST', url: '/bulk', headers: { 'Content-Type': ['application/json'] }, body: '{"items":[{"sku":"A"', body_size: 5000 }, { flags: ['truncated'] }));
assert.strictEqual(cTrunc.command, [
  "curl -X POST \"$BASE_URL\"'/bulk' \\",
  "  -H 'Content-Type: application/json' \\",
  "  --data-raw '{\"items\":[{\"sku\":\"A\"'"].join('\n'));
assert.deepStrictEqual(cTrunc.notes, [BASE_NOTE, 'Not replayable as is: the body is truncated (20 of 5000 bytes recorded).']);
assert.deepStrictEqual(U.curlFor({ kind: 'api.in', req: { truncated: true, inline: { method: 'POST', url: '/bulk', headers: { 'Content-Type': ['text/plain'] }, body: 'abc' } } }).notes,
  [BASE_NOTE, 'Not replayable as is: the body is truncated.']);
// a body the recorder replaced by a placeholder
assert.deepStrictEqual(U.curlFor(httpEv('api.in', { method: 'POST', url: '/upload', headers: { 'Content-Type': ['multipart/form-data; boundary=x'] }, body: '<multipart/form-data, 2048 bytes>', body_size: 2048 })).notes,
  [BASE_NOTE, 'Not replayable as is: the body was not recorded, only the placeholder <multipart/form-data, 2048 bytes>.']);

// not an HTTP request event, or no usable request payload
assert.strictEqual(U.curlFor(null), null);
assert.strictEqual(U.curlFor({ kind: 'db.write', req: { inline: { method: 'GET', url: '/x' } } }), null);
assert.strictEqual(U.curlFor({ kind: 'queue.publish', req: { inline: { order_id: 'ord_1001' } } }), null);
assert.strictEqual(U.curlFor({ kind: 'api.in' }), null);
assert.strictEqual(U.curlFor(httpEv('api.out', { order_id: 'ord_1001', service: 'ground' })), null); // no method or URL recorded
assert.strictEqual(U.curlFor({ kind: 'api.in', req: { truncated: true, inline: '{"method":"POST","url":"/orders","hea' } }), null); // whole payload cut
assert.strictEqual(U.curlFor(httpEv('api.in', { method: 'GET -o /tmp/x', url: '/x' })), null);
assert.strictEqual(U.curlFor(httpEv('api.in', { method: 'GET', url: '-o /tmp/x' })), null);
assert.strictEqual(U.curlFor(httpEv('api.in', { method: 'GET', url: '/x\nrm -rf ~' })), null);

// connectedText: which services are connected, for the empty-session state
assert.strictEqual(U.connectedText(null), '');
assert.strictEqual(U.connectedText({ error: 'not found' }), '');
assert.ok(U.connectedText([]).startsWith('No instrumented service is connected'));
assert.strictEqual(U.connectedText([{ service: 'worker' }, { service: 'api', instance: 'a1' }, { service: 'api', instance: 'a2' }, {}]),
  'Connected to the control plane: api (2), worker, 1 unnamed.');
assert.strictEqual(U.connectedText(Array.from({ length: 10 }, (_, i) => ({ service: 's' + i }))),
  'Connected to the control plane: s0, s1, s2, s3, s4, s5, s6, s7, 2 more.');

// fmtNs: a value that rounds up to 1000 of a unit is shown in the next unit
assert.strictEqual(U.fmtNs(1e6), '1.0 ms');
assert.strictEqual(U.fmtNs(999600), '1.0 ms');
assert.strictEqual(U.fmtNs(999400), '999 µs');
assert.strictEqual(U.fmtNs(999.96e6), '1.00 s');
assert.strictEqual(U.fmtNs(999.6), '1 µs');
assert.strictEqual(U.fmtNs(500), '500 ns');

// ---------- the session list, a page at a time ----------
assert.strictEqual(U.pageSize(null), 200);
assert.strictEqual(U.pageSize('2'), 2);
assert.strictEqual(U.pageSize('1000'), 1000);
for (const bad of ['0', '1001', '-1', '2.5', 'x', '', '1e2']) assert.strictEqual(U.pageSize(bad), 200);
assert.strictEqual(U.listQuery({ limit: 200 }), '?limit=200');
assert.strictEqual(U.listQuery({ limit: 2, state: 'ACTIVE', before: 'a+b/c=' }), '?limit=2&state=ACTIVE&before=a%2Bb%2Fc%3D');
assert.strictEqual(U.headerCount('12'), 12);
assert.strictEqual(U.headerCount('0'), 0);
for (const bad of [null, undefined, '', '-1', '1.5', 'x', '12abc']) assert.strictEqual(U.headerCount(bad), null);

// event counts are asked for by id: one request per page, never more ids than the collector takes
assert.deepStrictEqual(U.idChunks([], 200), []);
assert.deepStrictEqual(U.idChunks(['a', 'b', 'a', 'c'], 200), [['a', 'b', 'c']]);
assert.deepStrictEqual(U.idChunks(['a', 'b', 'c'], 2), [['a', 'b'], ['c']]);
{
  const many = Array.from({ length: 2500 }, (_, i) => 'ts_' + i);
  for (const size of [1000, 5000, 0, undefined]) assert.deepStrictEqual(U.idChunks(many, size).map((c) => c.length), [1000, 1000, 500]);
  assert.strictEqual(U.idChunks(many.slice(0, 200), 200).length, 1);
  // a page of 200 ids of 30 characters is about 6 KB of query
  const page = Array.from({ length: 200 }, (_, i) => 'ts_' + String(i).padStart(27, '0'));
  const q = U.idsQuery(page);
  assert.ok(q.length > 6000 && q.length < 6400, String(q.length));
  assert.strictEqual(q.split(',').length, 200);
}
assert.strictEqual(U.idsQuery(['ts_a', 'ts_b.c-d']), '?ids=ts_a,ts_b.c-d');
assert.strictEqual(U.idsQuery(['a b', 'c,d']), '?ids=a%20b,c%2Cd');
{
  const had = new Map([['a', { event_count: 1 }], ['b', { event_count: 2 }], ['z', { event_count: 9 }]]);
  // answered for every id: the answer's numbers; an id that is missing has no events; ids not asked keep theirs
  let m = U.mergeCounts(had, ['a', 'b', 'c'], [{ id: 'a', event_count: 5, sealed: true }, { id: 'c', event_count: 0, service_count: 2 }], 0);
  assert.deepStrictEqual(Array.from(m.counts), [['a', { event_count: 5 }], ['z', { event_count: 9 }], ['c', { event_count: 0, service_count: 2 }]]);
  assert.deepStrictEqual(m.missing, []);
  assert.strictEqual(had.get('a').event_count, 1, 'the input is not modified');
  // a collector without the ids parameter: no header, every session it holds; the extra ones are ignored
  m = U.mergeCounts(had, ['a', 'b'], [{ id: 'x', event_count: 7 }, { id: 'a', event_count: 3 }, { id: 'y', event_count: 8 }], null);
  assert.deepStrictEqual(Array.from(m.counts), [['a', { event_count: 3 }], ['z', { event_count: 9 }]]);
  assert.deepStrictEqual(m.missing, []);
  // some ids could not be looked up: a missing id keeps what it had and is asked again
  m = U.mergeCounts(had, ['a', 'b', 'c'], [{ id: 'a', event_count: 4 }], 1);
  assert.deepStrictEqual(Array.from(m.counts), [['a', { event_count: 4 }], ['b', { event_count: 2 }], ['z', { event_count: 9 }]]);
  assert.deepStrictEqual(m.missing, ['b', 'c']);
  // the request failed: nothing changes, everything is asked again
  for (const bad of [null, undefined, 'bad gateway', { error: 'x' }]) {
    m = U.mergeCounts(had, ['a', 'c', 'a'], bad, null);
    assert.deepStrictEqual(Array.from(m.counts), Array.from(had));
    assert.deepStrictEqual(m.missing, ['a', 'c']);
  }
  // junk in the answer is skipped
  m = U.mergeCounts(new Map(), ['a'], [null, 3, { event_count: 1 }, { id: 'a', event_count: 2 }], 0);
  assert.deepStrictEqual(Array.from(m.counts), [['a', { event_count: 2 }]]);
}

// order: newest first; fractions of a second of any length; the id decides a tie
const row = (id, at, extra) => Object.assign({ id: id, created_at: at }, extra);
const ordered = [
  row('ts_c', '2026-07-08T09:00:01Z'),
  row('ts_b', '2026-07-08T09:00:00.5Z'),
  row('ts_a', '2026-07-08T09:00:00.25Z'),
  row('ts_z', '2026-07-08T09:00:00.123456789Z'),
  row('ts_y', '2026-07-08T09:00:00.123456788Z'),
  row('ts_x', '2026-07-08T09:00:00Z'),
  row('ts_w', '2026-07-08T09:00:00Z'),
  row('ts_v', '2026-07-08T10:59:59+02:00'), // 08:59:59Z
  row('ts_u', 'not a time'),
];
assert.deepStrictEqual(ordered.slice().reverse().sort(U.sessionOrder).map((s) => s.id), ordered.map((s) => s.id));
assert.deepStrictEqual([ordered[5], ordered[0], ordered[8], ordered[3], ordered[6], ordered[1], ordered[7], ordered[4], ordered[2]].sort(U.sessionOrder).map((s) => s.id), ordered.map((s) => s.id));
assert.strictEqual(U.sessionOrder(ordered[5], row('ts_x', '2026-07-08T09:00:00.000Z')), 0);

{
  const at = (i) => '2026-07-08T09:00:' + String(i).padStart(2, '0') + 'Z';
  const mk = (i, extra) => row('ts_' + String(i).padStart(2, '0'), at(i), extra);
  const ids = (rows) => rows.map((s) => s.id).join(' ');
  // first load
  let m = U.mergeFirstPage(null, [mk(8), mk(9), mk(7)], true);
  assert.strictEqual(ids(m.rows), 'ts_09 ts_08 ts_07');
  assert.strictEqual(m.older, 0);
  // "Load more" appends the next page; a row that comes again replaces its earlier copy
  let rows = U.mergeRows(m.rows, [mk(6), mk(5), mk(4), mk(7, { name: 'again' })]);
  assert.strictEqual(ids(rows), 'ts_09 ts_08 ts_07 ts_06 ts_05 ts_04');
  assert.strictEqual(rows[2].name, 'again');
  // refresh: a new session arrived, 08 was deleted, 09 changed; the rows loaded below the page stay
  m = U.mergeFirstPage(rows, [mk(10), mk(9, { state: 'SEALED' }), mk(7)], true);
  assert.strictEqual(ids(m.rows), 'ts_10 ts_09 ts_07 ts_06 ts_05 ts_04');
  assert.strictEqual(m.rows[1].state, 'SEALED');
  assert.strictEqual(m.older, 3);
  // refresh whose page reaches the newest loaded row: nothing can be missing in between
  m = U.mergeFirstPage(rows, [mk(11), mk(10), mk(9)], true);
  assert.strictEqual(ids(m.rows), 'ts_11 ts_10 ts_09 ts_08 ts_07 ts_06 ts_05 ts_04');
  assert.strictEqual(m.older, 5);
  // refresh whose page ends above the newest loaded row: sessions in between may be missing
  // (here ts_10), so the list starts over from the page instead of keeping a hole
  m = U.mergeFirstPage(rows, [mk(13), mk(12), mk(11)], true);
  assert.strictEqual(ids(m.rows), 'ts_13 ts_12 ts_11');
  assert.strictEqual(m.older, 0);
  // refresh that returns everything there is: exactly that
  m = U.mergeFirstPage(rows, [mk(9), mk(4)], false);
  assert.strictEqual(ids(m.rows), 'ts_09 ts_04');
  assert.strictEqual(m.older, 0);
  assert.deepStrictEqual(U.mergeFirstPage(rows, [], false), { rows: [], older: 0 });
  // an empty page with more behind it (sessions the server could not read) changes nothing
  assert.strictEqual(U.mergeFirstPage(rows, [], true).rows, rows);
  // sessions created in the same instant are split by id, like the server does
  const same = [row('ts_b', at(1)), row('ts_a', at(1))];
  m = U.mergeFirstPage([row('ts_c', at(1)), same[0], same[1], row('ts_0', at(1))], [row('ts_c', at(1)), same[0]], true);
  assert.strictEqual(ids(m.rows), 'ts_c ts_b ts_a ts_0');
  assert.strictEqual(m.older, 2);
}

assert.strictEqual(U.listSummary({ loaded: 1, shown: 1, total: 1, more: false, state: '' }), '1 session');
assert.strictEqual(U.listSummary({ loaded: 5, shown: 5, total: 5, more: false, state: '' }), '5 sessions');
assert.strictEqual(U.listSummary({ loaded: 5, shown: 2, total: 5, more: false, state: '' }), '2 of 5 sessions');
assert.strictEqual(U.listSummary({ loaded: 3, shown: 3, total: 3, more: false, state: 'SEALED' }), '3 SEALED sessions');
assert.strictEqual(U.listSummary({ loaded: 1, shown: 1, total: 1, more: false, state: 'ACTIVE' }), '1 ACTIVE session');
assert.strictEqual(U.listSummary({ loaded: 3, shown: 1, total: 3, more: false, state: 'SEALED' }), '1 of 3 SEALED sessions');
assert.strictEqual(U.listSummary({ loaded: 200, shown: 200, total: 12345, more: true, state: '' }),
  'Showing the newest 200 of 12345 sessions. Search looks only at the sessions that are loaded.');
assert.strictEqual(U.listSummary({ loaded: 400, shown: 400, total: null, more: true, state: 'SEALED' }),
  'Showing the newest 400 SEALED sessions. Search looks only at the sessions that are loaded.');
assert.strictEqual(U.listSummary({ loaded: 200, shown: 3, total: 12345, more: true, state: '' }),
  '3 match among the newest 200 of 12345 sessions. Search looks only at the sessions that are loaded: load more to search older ones.');
assert.ok(U.unreadableText(1).startsWith('1 archived session could not be read by the control plane and is missing'));
assert.ok(U.unreadableText(30).startsWith('30 archived sessions could not be read by the control plane and are missing'));

// listSummary: rows below the first page are not refreshed, and the line says so
assert.strictEqual(U.listSummary({ loaded: 5, shown: 5, total: 5, more: false, state: '' }), '5 sessions');
assert.strictEqual(U.listSummary({ loaded: 5, shown: 5, total: 5, more: false, state: '', below: 3 }),
  '5 sessions. Finished sessions below the newest page are not refreshed; reload the page to update them.');
assert.ok(U.listSummary({ loaded: 400, shown: 400, total: 900, more: true, state: '', below: 200 }).endsWith('reload the page to update them.'));
assert.ok(!/not refreshed/.test(U.listSummary({ loaded: 200, shown: 200, total: 900, more: true, state: '', below: 0 })));

// Entity history: which event flags mean an uncertain key or row image (mirrors core.UncertainFlag).
for (const f of ['pk_unknown', 'pk_not_unique', 'pk_lookup_failed', 'upsert_unknown', 'after_unavailable', 'after_from_args', 'after_failed',
  'image_skipped', 'image_skipped:deadline', 'image_timeout', 'image_failed', 'image_truncated', 'diff_truncated']) assert.strictEqual(U.uncertainFlag(f), true, f);
for (const f of ['redacted', 'tx_unresolved', 'orphaned', 'returning_appended', 'returning_partial', '', ':pk_unknown', 'PK_UNKNOWN', 'constructor', '__proto__', 'toString', undefined, null, 7])
  assert.strictEqual(U.uncertainFlag(f), false, String(f));
assert.strictEqual(U.flagBase('image_skipped:deadline'), 'image_skipped');
assert.strictEqual(U.flagBase('orphaned'), 'orphaned');
assert.strictEqual(U.uncertainCount([{ flags: ['pk_not_unique', 'after_unavailable'] }, { flags: ['redacted'] }, { flags: ['image_skipped:serializable'] }, {}]), 2);
// Entries of a collector that predates the field, and anything malformed, count as not uncertain.
assert.strictEqual(U.uncertainCount([{ seq: 1 }, { flags: null }, { flags: 'pk_unknown' }, null, undefined]), 0);
assert.strictEqual(U.uncertainCount(undefined), 0);

console.log('util ok');
