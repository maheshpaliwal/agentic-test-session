'use strict';
/* Pure helpers shared by app.js and the node tests (dashboard/jstest/util_test.js). No DOM access. */
(function (root) {
  const MARK = '[REDACTED]';
  const MAX_DATE_MS = 8.64e15;

  // True when the value (a string, or its JSON text) is nothing but the redaction marker.
  function isRedactedValue(v) {
    if (typeof v !== 'string') return false;
    const t = v.trim();
    return t === MARK || t === '"' + MARK + '"';
  }

  // ISO string for a ns epoch timestamp, or null when it cannot be represented.
  function isoFromNs(ns) {
    if (typeof ns !== 'number' || !isFinite(ns)) return null;
    const ms = ns / 1e6;
    if (Math.abs(ms) > MAX_DATE_MS) return null;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d.toISOString();
  }
  function unsafeNs(ns) { return typeof ns === 'number' && Math.abs(ns) > Number.MAX_SAFE_INTEGER; }

  function fmtNs(ns) {
    if (typeof ns !== 'number' || !isFinite(ns)) return '-';
    const a = Math.abs(ns);
    if (a >= 1e9) return (ns / 1e9).toFixed(2) + ' s';
    if (a >= 1e6) return (ns / 1e6).toFixed(1) + ' ms';
    if (a >= 1e3) return (ns / 1e3).toFixed(0) + ' µs';
    return Math.round(ns) + ' ns';
  }
  // Relative time computed from ms values (difference of two large ns doubles loses less than a ms).
  function relNs(ns, t0) {
    if (typeof ns !== 'number' || typeof t0 !== 'number' || !isFinite(ns) || !isFinite(t0)) return NaN;
    return (Math.floor(ns / 1e6) - Math.floor(t0 / 1e6)) * 1e6 + ((ns % 1e6) - (t0 % 1e6));
  }

  function parsePK(pk) {
    if (typeof pk === 'string') {
      const t = pk.trim();
      if (t[0] === '{') { try { const o = JSON.parse(t); if (o && typeof o === 'object') return o; } catch (e) { /* fall through */ } }
      return pk;
    }
    return pk;
  }
  // Compact display: {"id":1} -> id=1 ; multiple keys -> a=1, b=2.
  function pkLabel(pk) {
    if (pk === undefined || pk === null) return '';
    const o = parsePK(pk);
    if (o === null || typeof o !== 'object') return String(o);
    const k = Object.keys(o).sort();
    return k.length ? k.map((x) => x + '=' + (typeof o[x] === 'object' ? JSON.stringify(o[x]) : o[x])).join(', ') : '{}';
  }
  // Canonical form used in URL paths (the collector accepts the canonical JSON string).
  function pkRaw(pk) {
    if (pk === undefined || pk === null) return '';
    if (typeof pk === 'string') return pk;
    if (typeof pk === 'object') { const o = {}; Object.keys(pk).sort().forEach((k) => { o[k] = pk[k]; }); return JSON.stringify(o); }
    return String(pk);
  }

  const unresolvedMut = (e) => !!e && (e.mutations || []).some((m) => m.effective === null || m.effective === undefined);
  const hasUnresolved = (events) => events.some(unresolvedMut);
  const effSig = (e) => (e.mutations || []).map((m) => m.effective === true ? 'T' : m.effective === false ? 'F' : 'N').join('') + '|' + (e.flags || []).join(',');

  // Merge fetched events into D (bySeq/bySpan/events/t0); newest copy wins. Returns {added, changed}.
  function mergeEvents(D, evs) {
    let added = 0, changed = 0, resort = false;
    evs.forEach((e) => {
      const old = D.bySeq.get(e.seq);
      if (old) {
        if (effSig(old) !== effSig(e) || JSON.stringify(old.attrs || null) !== JSON.stringify(e.attrs || null)) changed++;
        const i = D.events.indexOf(old);
        if (i >= 0) D.events[i] = e; else { D.events.push(e); resort = true; }
      } else { D.events.push(e); added++; resort = true; }
      D.bySeq.set(e.seq, e); D.bySpan.set(e.span_id, e);
      if (e.seq > D.maxSeq) D.maxSeq = e.seq;
      if (!D.t0 || e.start_ns < D.t0) D.t0 = e.start_ns;
    });
    if (resort) D.events.sort((a, b) => a.seq - b.seq);
    return { added: added, changed: changed };
  }

  const ndjson = (events) => events.map((e) => JSON.stringify(e)).join('\n') + (events.length ? '\n' : '');

  // Which warning lines apply to a session.
  function sessionWarnings(s) {
    const w = [];
    if (!s) return w;
    if (s.seal_error) w.push('Seal failed: ' + s.seal_error);
    if (s.incomplete) w.push('This session is incomplete: some events or async work may be missing' + (s.dropped_events ? ' (' + s.dropped_events + ' events dropped)' : '') + '. Treat the history as a lower bound.');
    else if (s.dropped_events) w.push(s.dropped_events + ' events were dropped.');
    return w;
  }

  // Colour/icon group for an event kind: api db cache queue worker retry dlq tx sys.
  function kindGroup(k) {
    k = String(k || '');
    if (k === 'worker.retry') return 'retry';
    if (k === 'worker.dlq') return 'dlq';
    if (k.indexOf('db.tx.') === 0) return 'tx';
    const p = k.split('.')[0];
    return ['api', 'db', 'cache', 'queue', 'worker'].indexOf(p) >= 0 ? p : 'sys';
  }
  // "5s ago" style label for an age in ms.
  function fmtAgo(ms) {
    if (typeof ms !== 'number' || !isFinite(ms)) return '-';
    if (ms < 0) ms = 0;
    const s = Math.floor(ms / 1000);
    if (s < 5) return 'just now';
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    if (s < 86400) return Math.floor(s / 3600) + 'h ago';
    return Math.floor(s / 86400) + 'd ago';
  }
  // Normalised mutation op class: insert update delete upsert other.
  function opClass(op) {
    const o = String(op || '').toLowerCase();
    return ['insert', 'update', 'delete', 'upsert'].indexOf(o) >= 0 ? o : 'other';
  }


  // ---------- trigger grouping ----------
  /* Start order: hlc first (when both present), then start_ns, then seq. NOT completion order (seq). */
  function cmpStart(a, b) {
    if (a.hlc && b.hlc && a.hlc !== b.hlc) return a.hlc < b.hlc ? -1 : 1;
    const x = a.start_ns || 0, y = b.start_ns || 0;
    if (x !== y) return x < y ? -1 : 1;
    return (a.seq || 0) - (b.seq || 0);
  }
  const ASYNC_TRIGGER = { 'worker.start': 'worker', 'queue.consume': 'consume' };
  const MAX_DEPTH = 64, MAX_INDENT = 6;
  const OP_SYM = { insert: '+', update: '~', delete: '-', upsert: '⇅', other: '•' };
  const spanKey = (e) => e.span_id || ('seq:' + e.seq);

  function httpStatus(e) {
    const a = e.attrs && e.attrs['http.status'];
    if (a !== undefined && a !== null && a !== '') return Number(a);
    const r = e.resp && e.resp.inline;
    return r && typeof r === 'object' && typeof r.status === 'number' ? r.status : NaN;
  }
  function fmtClock(ns) {
    if (typeof ns !== 'number' || !isFinite(ns) || Math.abs(ns / 1e6) > MAX_DATE_MS) return '-';
    const d = new Date(ns / 1e6), p = (n, w) => String(n).padStart(w || 2, '0');
    return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds()) + '.' + p(d.getMilliseconds(), 3);
  }

  /* Group a session's events by TRIGGER: one API call (api.in without a parent in the session), one async job
     (worker.start / queue.consume), or an untriggered orphan root. Every other event joins the group of its nearest
     trigger ancestor along parent_id. Pure and total: tolerates missing parents, cycles, deep chains, any input order.
     opts.live: the session is still receiving events (missing parents may simply not have arrived yet).
     Returns {groups (sorted by trigger start), byKey, keyOf (seq -> group key), pubWorkers (publish span -> [group key])}. */
  function groupEvents(events, opts) {
    opts = opts || {};
    const bySpan = new Map();
    events.forEach((e) => { if (e.span_id) { const o = bySpan.get(e.span_id); if (!o || (e.seq || 0) >= (o.seq || 0)) bySpan.set(e.span_id, e); } });
    const memo = new Map();
    function resolve(e) {
      const path = [], onPath = new Map();
      let cur = e, res = null;
      for (let depth = 0; ; depth++) {
        const id = spanKey(cur);
        if (memo.has(id)) { res = memo.get(id); break; }
        if (onPath.has(id)) {
          const cyc = path.slice(onPath.get(id)).map(spanKey).sort();
          res = { key: 'cycle:' + cyc[0], kind: 'untriggered', root: null, reason: 'cycle' };
          break;
        }
        if (depth >= MAX_DEPTH) { res = { key: 'deep:' + id, kind: 'untriggered', root: null, reason: 'depth' }; break; }
        onPath.set(id, path.length); path.push(cur);
        if (ASYNC_TRIGGER[cur.kind]) { res = { key: id, kind: ASYNC_TRIGGER[cur.kind], root: cur }; break; }
        const p = cur.parent_id ? bySpan.get(cur.parent_id) : null;
        if (p && p !== cur) { cur = p; continue; }
        if (cur.kind === 'api.in') res = { key: id, kind: 'api', root: cur };
        else if (cur.parent_id && cur.parent_id !== cur.span_id) res = { key: cur.parent_id, kind: 'untriggered', root: null, reason: 'missing_parent', missing: cur.parent_id };
        else res = { key: id, kind: 'untriggered', root: cur, reason: 'root' };
        break;
      }
      path.forEach((x) => memo.set(spanKey(x), res));
      return res;
    }
    const gm = new Map(), keyOf = new Map();
    events.forEach((e) => {
      const r = resolve(e);
      let g = gm.get(r.key);
      if (!g) { g = { key: r.key, kind: r.kind, trigger: r.root, reason: r.reason || '', missing: r.missing || '', events: [] }; gm.set(r.key, g); }
      g.events.push(e);
      keyOf.set(e.seq, r.key);
    });

    const pubWorkers = new Map();
    const groups = Array.from(gm.values());
    groups.forEach((g) => {
      // children ordered depth-first by start (parent before child), trigger first
      const inG = new Set(g.events.map(spanKey));
      const kids = new Map(), rootsL = [];
      g.events.forEach((e) => {
        if (e === g.trigger) return;
        const pid = e.parent_id;
        if (pid && pid !== e.span_id && inG.has(pid) && bySpan.get(pid)) {
          if (!kids.has(pid)) kids.set(pid, []);
          kids.get(pid).push(e);
        } else rootsL.push(e);
      });
      const rows = [], seen = new Set();
      const walk = (e, d) => {
        const id = spanKey(e);
        if (seen.has(id)) return;
        seen.add(id);
        rows.push({ e: e, depth: Math.min(d, MAX_INDENT) });
        (kids.get(id) || []).slice().sort(cmpStart).forEach((c) => walk(c, d + 1));
      };
      if (g.trigger) walk(g.trigger, 0);
      rootsL.sort(cmpStart).forEach((e) => walk(e, g.trigger ? 1 : 0));
      g.events.slice().sort(cmpStart).forEach((e) => walk(e, 1)); // cycle members / leftovers
      g.rows = rows;
      // bracket of db transactions: rows between the first and last row of a tx_id
      const span_ = new Map();
      rows.forEach((r, i) => { const t = r.e.tx_id; if (t) { const o = span_.get(t) || { a: i, b: i }; o.b = i; span_.set(t, o); } });
      span_.forEach((o, t) => {
        const rb = rows.some((r) => r.e.tx_id === t && (r.e.kind === 'db.tx.rollback' || (r.e.mutations || []).some((m) => m.effective === false)));
        for (let i = o.a; i <= o.b; i++) if (!rows[i].tx) rows[i].tx = { id: t, pos: o.a === o.b ? 'only' : i === o.a ? 'first' : i === o.b ? 'last' : 'mid', rolledBack: rb };
      });

      // facts
      const evs = g.events, c = {};
      let minS = Infinity, maxE = 0, errN = 0, sort = null;
      const mut = new Map(), fl = new Set();
      evs.forEach((e) => {
        c[e.kind] = (c[e.kind] || 0) + 1;
        if (e.err) errN++;
        if (!sort || cmpStart(e, sort) < 0) sort = e;
        if (e.start_ns < minS) minS = e.start_ns;
        if (e.end_ns > maxE) maxE = e.end_ns;
        (e.mutations || []).forEach((m) => {
          const k = (m.store || '?') + '\u0000' + (m.entity || '?');
          const o = mut.get(k) || { store: m.store || '?', entity: m.entity || '?', ops: { insert: 0, update: 0, delete: 0, upsert: 0, other: 0 }, n: 0, rolledBack: 0 };
          o.ops[opClass(m.op)]++; o.n++;
          if (m.effective === false) { o.rolledBack++; fl.add('rolled_back'); }
          mut.set(k, o);
        });
        if (unresolvedMut(e) && e.tx_id) fl.add('tx_unresolved');
        (e.flags || []).forEach((x) => { if (x === 'tx_unresolved' || x === 'orphaned') fl.add(x); });
        if (e.attrs && e.attrs.retry_pending) fl.add('retry_pending');
      });
      const t = g.trigger, st = t ? httpStatus(t) : NaN;
      g.errCount = errN;
      g.status = errN > 0 || (t && t.kind === 'api.in' && st >= 500) ? 'error' : 'ok';
      g.httpStatus = isNaN(st) ? null : st;
      g.flags = Array.from(fl).sort();
      g.start = t ? t.start_ns : minS;
      g.end = t ? Math.max(t.end_ns || 0, t.start_ns || 0) : maxE;
      g.dur = Math.max(0, g.end - g.start);
      g.sortEvent = t || sort;
      g.counts = c;
      g.retries = c['worker.retry'] || 0;
      g.chips = [['db.read', 'db.read', c['db.read']], ['db.write', 'db.write', c['db.write']], ['cache', 'cache', (c['cache.read'] || 0) + (c['cache.write'] || 0)],
        ['api.out', 'api.out', c['api.out']], ['queue.publish', 'queue.publish', c['queue.publish']], ['retry', g.retries === 1 ? 'retry' : 'retries', g.retries], ['dlq', 'dlq', c['worker.dlq']],
        ['err', errN === 1 ? 'error' : 'errors', errN]].filter((x) => x[2] > 0).map((x) => ({ k: x[0], label: x[1], n: x[2] }));
      g.mutations = Array.from(mut.values()).sort((a, b) => (a.store + a.entity < b.store + b.entity ? -1 : 1));
      g.mutText = mutationText(g.mutations);
      g.firstSeq = Math.min.apply(null, evs.map((e) => e.seq));
      g.lastSeq = Math.max.apply(null, evs.map((e) => e.seq));
      // async link: worker trigger -> publisher span
      g.triggeredBy = null; g.linkMissing = false;
      if (t && ASYNC_TRIGGER[t.kind]) {
        const pub = (t.link_ids || []).map((l) => bySpan.get(l)).filter(Boolean)[0];
        if (pub) {
          g.triggeredBy = { span_id: pub.span_id, seq: pub.seq, name: pub.name || '', groupKey: (memo.get(spanKey(pub)) || {}).key };
          if (!pubWorkers.has(pub.span_id)) pubWorkers.set(pub.span_id, []);
          pubWorkers.get(pub.span_id).push(g.key);
        } else g.linkMissing = true;
      }
    });
    groups.sort((a, b) => cmpStart(a.sortEvent, b.sortEvent) || (a.key < b.key ? -1 : 1));
    const byKey = new Map(groups.map((g) => [g.key, g]));
    pubWorkers.forEach((ks) => ks.sort((a, b) => cmpStart(byKey.get(a).sortEvent, byKey.get(b).sortEvent)));
    groups.forEach((g) => {
      g.title = groupTitle(g, opts);
      if (g.triggeredBy) {
        const pg = byKey.get(g.triggeredBy.groupKey);
        g.triggeredBy.groupTitle = pg ? pg.title : '';
        g.asyncLabel = 'async · triggered by ' + (pg ? pg.title : 'unknown') + ' (queue.publish ' + g.triggeredBy.name + ')';
      } else if (g.linkMissing) g.asyncLabel = 'async · publisher not in this session';
      else g.asyncLabel = '';
    });
    return { groups: groups, byKey: byKey, keyOf: keyOf, pubWorkers: pubWorkers };
  }
  function groupTitle(g, opts) {
    const t = g.trigger;
    if (g.kind === 'api') return t.name || 'api.in';
    if (g.kind === 'worker') return 'worker ' + (t.name || '?') + ' · attempt ' + (t.attempt || 1);
    if (g.kind === 'consume') return 'consume ' + (t.name || '?') + (t.attempt ? ' · attempt ' + t.attempt : '');
    if (t) return 'untriggered · ' + t.kind + ' ' + (t.name || '');
    if (g.reason === 'missing_parent') return (opts && opts.live ? 'awaiting trigger' : 'untriggered') + ' · parent span ' + g.missing + ' not in session';
    return 'untriggered · ' + (g.reason === 'cycle' ? 'parent cycle' : 'deep call chain');
  }
  function mutationText(ms) {
    const by = new Map();
    ms.forEach((m) => { if (!by.has(m.store)) by.set(m.store, []); by.get(m.store).push(m); });
    return Array.from(by.keys()).sort().map((st) => st + ': ' + by.get(st).map((m) => {
      const ops = ['insert', 'update', 'delete', 'upsert', 'other'].filter((o) => m.ops[o]).map((o) => OP_SYM[o] + m.ops[o]).join(' ');
      return m.entity + ' ' + ops + (m.rolledBack ? ' (' + m.rolledBack + ' rolled back)' : '');
    }).join(', ')).join('; ');
  }
  /* Entry point of a session for list/header subtitles: first non-async trigger plus how many other triggers ran. */
  function entryOf(events, partial) {
    if (!events || !events.length) return null;
    const gs = groupEvents(events, { live: false }).groups.filter((g) => g.trigger); // skip groups whose trigger has not arrived
    if (!gs.length) return null;
    const first = gs.find((g) => g.kind !== 'worker' && g.kind !== 'consume') || gs[0];
    return { title: first.title, more: gs.length - 1, partial: !!partial };
  }
  const entryText = (en) => (en ? 'entry: ' + en.title + (en.more > 0 ? ' (+' + en.more + (en.partial ? '+' : '') + ' more trigger' + (en.more > 1 ? 's' : '') + ')' : '') : '');
  const bindingText = (b) => b.name + ': ' + b.value;
  // Text a session-list search matches: id, name, initiator, mode, binding names/values, entry point.
  function sessionHay(s, en) {
    return [s.id, s.name, s.initiated_by, s.mode].concat((s.bindings || []).map((b) => b.source + ' ' + b.name + ' ' + b.value), en ? [en.title] : []).join(' ').toLowerCase();
  }
  // One-line "3 db.read · 5 db.write" summary of a group's counts.
  const chipsText = (g) => g.chips.map((c) => c.n + ' ' + c.label).join(' · ');

  const api = { entryOf, entryText, bindingText, sessionHay, cmpStart, groupEvents, groupTitle, mutationText, chipsText, httpStatus, fmtClock, kindGroup, fmtAgo, opClass, MARK, isRedactedValue, isoFromNs, unsafeNs, fmtNs, relNs, parsePK, pkLabel, pkRaw, unresolvedMut, hasUnresolved,
    mergeEvents, ndjson, sessionWarnings };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.TSUtil = api;
})(typeof window !== 'undefined' ? window : globalThis);
