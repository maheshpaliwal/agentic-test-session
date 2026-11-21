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
    // The thresholds sit where the smaller unit would round up to 1000, so 1 ms never reads "1000 µs".
    if (a >= 999.95e6) return (ns / 1e9).toFixed(2) + ' s';
    if (a >= 999.5e3) return (ns / 1e6).toFixed(1) + ' ms';
    if (a >= 999.5) return (ns / 1e3).toFixed(0) + ' µs';
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
    return k.length ? k.map((x) => x + '=' + (typeof o[x] === 'object' ? JSON.stringify(o[x]) : o[x])).join(', ') : '(key unknown)';
  }
  const hasPayload = (p) => !!p && (p.inline !== undefined || !!p.blob_hash);
  /* An update whose row could not be read back: the before image is known, the new values are not. Its diff, if an
     older SDK sent one, lists every field as removed and must not be shown. Mirrors core.Mutation.AfterUnavailable. */
  const afterUnavailable = (mu) => !!mu && mu.op === 'update' && hasPayload(mu.before) && !hasPayload(mu.after);
  // Canonical form used in URL paths (the collector accepts the canonical JSON string).
  function pkRaw(pk) {
    if (pk === undefined || pk === null) return '';
    if (typeof pk === 'string') return pk;
    if (typeof pk === 'object') { const o = {}; Object.keys(pk).sort().forEach((k) => { o[k] = pk[k]; }); return JSON.stringify(o); }
    return String(pk);
  }

  // "image_skipped:deadline" is the flag image_skipped with a reason.
  const flagBase = (f) => String(f).split(':')[0];
  /* Event flags that say the recorded key or row image of the event's mutations is incomplete or uncertain.
     Mirrors core.UncertainFlag (the collector counts the same flags in an entity's `uncertain`). */
  const UNCERTAIN = { pk_unknown: 1, pk_not_unique: 1, pk_lookup_failed: 1, after_unavailable: 1, after_from_args: 1, after_failed: 1,
    returning_partial: 1, image_skipped: 1, image_timeout: 1, image_failed: 1, image_truncated: 1, diff_truncated: 1 };
  const uncertainFlag = (f) => UNCERTAIN[flagBase(f)] === 1;
  // How many entries of an entity history carry such a flag (entries of an older collector have no flags: 0).
  const uncertainCount = (ms) => (ms || []).filter((m) => m && Array.isArray(m.flags) && m.flags.some(uncertainFlag)).length;

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

  /* ---------- copy as curl ----------
     Everything below is built from recorded traffic and ends up pasted into a terminal, so no
     recorded byte is emitted outside quotes and nothing that could act as a curl option is passed through. */
  const CURL_SKIP = { host: 1, 'content-length': 1, connection: 1, 'accept-encoding': 1, 'transfer-encoding': 1, traceparent: 1 };
  const curlSkipped = (name) => { const n = name.toLowerCase(); return CURL_SKIP[n] === 1 || n.indexOf('x-tsession-') === 0; };
  const HDR_NAME = /^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/; // RFC 9110 token
  const CTL = /[\u0000-\u001f\u007f-\u009f]/;
  const CTL_NOT_LF = /[\u0000-\u0009\u000b-\u001f\u007f-\u009f]/;
  const MARK_Q = '%5BREDACTED%5D'; // the marker as the recorder writes it into a query string
  const hex2 = (n) => (n < 16 ? '0' : '') + n.toString(16);
  // POSIX single quotes: everything is literal; an embedded quote closes, escapes and reopens.
  function shQuote(s) { return "'" + s.replace(/'/g, "'\\''") + "'"; }
  // bash/zsh $'...': backslash, quote and every control character are escaped (C1 controls as their UTF-8 bytes).
  // '!' is escaped too, because some shells run history expansion inside $'...'.
  // A quote is written as \x27, never as \': a shell without $'...' (dash, POSIX sh) reads the text as one
  // ordinary single-quoted word, and a literal quote character in it would end that word and let the rest run
  // as a command.
  function shAnsi(s) {
    return "$'" + s.replace(/[\\'!\u0000-\u001f\u007f-\u009f]/g, (c) => {
      if (c === '\\') return '\\\\';
      if (c === "'") return '\\x27';
      if (c === '\n') return '\\n';
      if (c === '\r') return '\\r';
      if (c === '\t') return '\\t';
      const n = c.charCodeAt(0);
      return n < 0x80 ? '\\x' + hex2(n) : '\\xc2\\x' + hex2(n);
    }) + "'";
  }
  function utf8Len(s) { return typeof TextEncoder !== 'undefined' ? new TextEncoder().encode(s).length : s.length; }
  const curlList = (names) => names.filter((n, i) => names.indexOf(n) === i).join(', ');
  /* curl command for an HTTP event (api.in / api.out) whose request payload has a method and a URL
     or path; null otherwise. notes says what was changed and why the command may not replay as is. */
  function curlFor(e) {
    if (!e || (e.kind !== 'api.in' && e.kind !== 'api.out') || !e.req) return null;
    const p = e.req.inline;
    if (!p || typeof p !== 'object' || Array.isArray(p)) return null; // absent, or cut by the inline cap
    const method = p.method, url = typeof p.url === 'string' && p.url ? p.url : p.path;
    if (typeof method !== 'string' || !/^[A-Za-z]+$/.test(method)) return null;
    if (typeof url !== 'string' || !url || CTL.test(url)) return null;
    const absolute = /^https?:[/][/]/i.test(url); // written without a double slash: the asset test reads one as an external URL
    if (!absolute && url[0] !== '/') return null; // neither a URL nor a path; curl could read it as an option
    const notes = [];
    let first = 'curl ' + (method === 'HEAD' ? '--head' : '-X ' + method);
    if (/[[\]{}]/.test(url)) first += ' --globoff';
    first += ' ' + (absolute ? '' : '"$BASE_URL"') + shQuote(url);
    const parts = [first];
    if (!absolute) notes.push('The recorded URL has no scheme or host: set BASE_URL first, for example export BASE_URL=http://127.0.0.1:8080.');

    const skipped = [], dropped = [], redactedH = [];
    let hasType = false;
    const hs = p.headers && typeof p.headers === 'object' && !Array.isArray(p.headers) ? p.headers : {};
    Object.keys(hs).forEach((name) => {
      const label = JSON.stringify(name.length > 40 ? name.slice(0, 40) + '…' : name);
      if (!HDR_NAME.test(name)) { dropped.push(label); return; }
      if (curlSkipped(name)) { skipped.push(name); return; }
      (Array.isArray(hs[name]) ? hs[name] : [hs[name]]).forEach((v) => {
        if (typeof v !== 'string' || CTL.test(v)) { dropped.push(label); return; }
        if (v.indexOf(MARK) >= 0) redactedH.push(name);
        if (name.toLowerCase() === 'content-type') hasType = true;
        parts.push('-H ' + shQuote(v === '' ? name + ';' : name + ': ' + v)); // "Name;" is how curl sends an empty header
      });
    });
    if (skipped.length) notes.push('Left out headers that curl sets itself or that would attribute the replay to the recorded session: ' + curlList(skipped) + '.');
    if (dropped.length) notes.push('Dropped headers whose name or value cannot be passed to a shell safely: ' + curlList(dropped) + '.');

    let bodyRedacted = false;
    const body = p.body;
    if (body !== undefined && body !== null && body !== '') {
      const text = typeof body === 'string' ? body : JSON.stringify(body);
      if (text.indexOf('\u0000') >= 0) notes.push('The body contains a NUL byte, which no shell argument can carry: it was left out.');
      else {
        const ansi = CTL_NOT_LF.test(text);
        parts.push('--data-raw ' + (ansi ? shAnsi(text) : shQuote(text)));
        if (ansi) notes.push("The body has control characters, so it is quoted as $'...': run the command in bash or zsh.");
        if (!hasType) notes.push('No Content-Type header was recorded: curl will send application/x-www-form-urlencoded.');
        bodyRedacted = text.indexOf(MARK) >= 0;
        const flagged = !!e.req.truncated || (e.flags || []).indexOf('truncated') >= 0;
        if (typeof body === 'string' && (body === '<binary>' || /^<[^<>]{1,80}, \d+ bytes>$/.test(body))) notes.push('Not replayable as is: the body was not recorded, only the placeholder ' + body + '.');
        else if (typeof body === 'string' && flagged && typeof p.body_size === 'number' && p.body_size > utf8Len(body)) notes.push('Not replayable as is: the body is truncated (' + utf8Len(body) + ' of ' + p.body_size + ' bytes recorded).');
        else if (e.req.truncated) notes.push('Not replayable as is: the body is truncated.');
      }
    }
    const urlRedacted = url.indexOf(MARK) >= 0 || url.indexOf(MARK_Q) >= 0;
    if (redactedH.length) notes.push('Not replayable as is: redacted headers still hold ' + MARK + ' (' + curlList(redactedH) + '). Put the real values back.');
    if (urlRedacted) notes.push('Not replayable as is: the URL holds redacted values (' + MARK + ').');
    if (bodyRedacted) notes.push('Not replayable as is: the body holds redacted values (' + MARK + ').');
    if (e.req.redacted && !redactedH.length && !urlRedacted && !bodyRedacted) notes.push('The recorder masked sensitive values in this request: the replay may differ from the original.');
    return { command: parts.join(' \\\n  '), notes: notes };
  }

  // connectedText says which services are connected to the control plane, for
  // the "no events yet" state: an empty session looks the same whether the
  // service is not connected or simply got no traffic. subs is the reply of
  // GET /v1/subscribers; anything that is not an array (endpoint missing,
  // request failed) gives '' so nothing is claimed.
  function connectedText(subs) {
    if (!Array.isArray(subs)) return '';
    if (!subs.length) return 'No instrumented service is connected to the control plane, so nothing can be recorded. Start the service with TSESSION_ENABLED=true and point its TSESSION_CONTROLPLANE_URL at the control plane.';
    const counts = new Map();
    let unnamed = 0;
    for (const s of subs) {
      const name = s && typeof s.service === 'string' ? s.service : '';
      if (!name) unnamed++;
      else counts.set(name, (counts.get(name) || 0) + 1);
    }
    const names = Array.from(counts.keys()).sort();
    const shown = names.slice(0, 8).map((n) => (counts.get(n) > 1 ? n + ' (' + counts.get(n) + ')' : n));
    if (names.length > shown.length) shown.push((names.length - shown.length) + ' more');
    if (unnamed) shown.push(unnamed + ' unnamed');
    return 'Connected to the control plane: ' + shown.join(', ') + '.';
  }

  /* ---------- the session list, loaded a page at a time ----------
     The control plane lists sessions newest first by creation time, then id (docs/PROTOCOL.md).
     The page keeps the sessions it has loaded in that order and merges what it fetches into them. */
  const PAGE_DEFAULT = 200, PAGE_MAX = 1000;
  // Page size from the #/?page=N parameter; anything else gives the default.
  function pageSize(v) {
    const n = /^\d+$/.test(String(v || '')) ? Number(v) : 0;
    return n >= 1 && n <= PAGE_MAX ? n : PAGE_DEFAULT;
  }
  // Query string of a list request. before is the cursor of the previous page.
  function listQuery(o) {
    const q = ['limit=' + o.limit];
    if (o.state) q.push('state=' + encodeURIComponent(o.state));
    if (o.before) q.push('before=' + encodeURIComponent(o.before));
    return '?' + q.join('&');
  }
  // A count sent in a response header: a non-negative integer, or null when absent or malformed.
  function headerCount(v) { return typeof v === 'string' && /^\d+$/.test(v) ? Number(v) : null; }
  /* The collector's event counts are asked for by session id (GET /v1/sessions?ids=, docs/PROTOCOL.md): what a
     refresh costs the collector follows the sessions shown, not everything it stores.
     idChunks: the distinct ids, split into the requests they take. One request carries at most PAGE_MAX ids,
     the collector's limit and the largest page, so a page is always one request. */
  function idChunks(ids, size) {
    const n = size >= 1 ? Math.min(size, PAGE_MAX) : PAGE_MAX, uniq = Array.from(new Set(ids)), out = [];
    for (let i = 0; i < uniq.length; i += n) out.push(uniq.slice(i, i + n));
    return out;
  }
  // Query string of a count request for ids (one chunk of idChunks).
  function idsQuery(ids) { return '?ids=' + ids.map(encodeURIComponent).join(','); }
  /* The counts (a Map of id -> {event_count, service_count}) after the answer to a request for the ids in asked.
     list is the answer's array, or null when the request failed. unanswered is its X-Tsession-Unanswered header:
     how many of the ids the collector could not look up (null when the header is absent).
     An asked id in the answer takes the answer's numbers. One that is missing has no events when the collector
     answered for every id; when it did not (or the request failed) the id keeps what it had and is returned in
     missing, to be asked again. Sessions in the answer that were not asked for are ignored: a collector older
     than the ids parameter answers with every session it holds. */
  function mergeCounts(counts, asked, list, unanswered) {
    const out = new Map(counts), missing = [];
    if (!Array.isArray(list)) return { counts: out, missing: Array.from(new Set(asked)) };
    const got = new Map();
    list.forEach((x) => { if (x && typeof x.id === 'string') got.set(x.id, x); });
    new Set(asked).forEach((id) => {
      const x = got.get(id);
      if (x) out.set(id, x.service_count === undefined ? { event_count: x.event_count } : { event_count: x.event_count, service_count: x.service_count });
      else if (unanswered > 0) missing.push(id);
      else out.delete(id);
    });
    return { counts: out, missing: missing };
  }
  // created_at as [milliseconds, nanoseconds below the second]. The fraction is compared by itself:
  // timestamps come with zero to nine fractional digits, which neither Date nor a string compare orders.
  function createdParts(v) {
    const m = /^(.*?)(?:\.(\d+))?(Z|[+-]\d\d:?\d\d)?$/.exec(String(v || ''));
    const ms = m ? Date.parse(m[1] + (m[3] || 'Z')) : NaN;
    return [isNaN(ms) ? -Infinity : ms, m && m[2] ? Number((m[2] + '00000000').slice(0, 9)) : 0];
  }
  // Sort order of the list: newest first, the id deciding between sessions created at the same instant.
  function sessionOrder(a, b) {
    const x = createdParts(a.created_at), y = createdParts(b.created_at);
    if (x[0] !== y[0]) return x[0] < y[0] ? 1 : -1;
    if (x[1] !== y[1]) return x[1] < y[1] ? 1 : -1;
    const i = String(a.id), j = String(b.id);
    return i === j ? 0 : i < j ? 1 : -1;
  }
  // The loaded rows after a refresh of the first page. page is the newest sessions as the server
  // has them now and more says whether it has older ones. Rows as new as the page are replaced by
  // it (one that is not on it any more was deleted or left the filter); rows older than the page
  // were loaded with "Load more" and stay as they are. older is how many of those there are.
  // When the page ends above the newest loaded row (more sessions arrived than a page holds, as
  // after the tab was in the background), sessions in between may not be loaded: the list would
  // have a hole that "Load more" never fills, so it starts over from the page.
  function mergeFirstPage(rows, page, more) {
    const fresh = page.slice().sort(sessionOrder);
    if (!more) return { rows: fresh, older: 0 };
    if (!fresh.length) return { rows: rows || [], older: (rows || []).length };
    const last = fresh[fresh.length - 1];
    if (!rows || !rows.length || sessionOrder(last, rows[0]) < 0) return { rows: fresh, older: 0 };
    const older = rows.filter((s) => sessionOrder(last, s) < 0);
    return { rows: fresh.concat(older), older: older.length };
  }
  // The loaded rows with more sessions merged in by id (a later copy replaces an earlier one).
  function mergeRows(rows, more) {
    const byId = new Map();
    (rows || []).concat(more).forEach((s) => byId.set(s.id, s));
    return Array.from(byId.values()).sort(sessionOrder);
  }
  // The line under the page title. o: loaded (sessions loaded), shown (of those, passing the search),
  // total (matching the state filter on the server; null if unknown), more (older ones not loaded),
  // state (the state filter, '' for none). Search only ever looks at loaded sessions, and says so
  // whenever that is not all of them.
  // below (optional) is how many loaded rows sit under the first page: the periodic refresh covers
  // the first page and running sessions only, and the line says so while such rows are shown.
  function listSummary(o) {
    const plural = (n) => n + ' session' + (n === 1 ? '' : 's');
    const kind = o.state ? o.state + ' ' : '';
    const stale = o.below > 0 ? ' Finished sessions below the newest page are not refreshed; reload the page to update them.' : '';
    if (!o.more) {
      if (o.shown === o.loaded) return (o.state ? plural(o.loaded).replace(' ', ' ' + kind) : plural(o.loaded)) + (stale ? '.' + stale : '');
      return o.shown + ' of ' + o.loaded + ' ' + kind + 'sessions' + (stale ? '.' + stale : '');
    }
    const scope = 'the newest ' + o.loaded + (o.total !== null && o.total >= o.loaded ? ' of ' + o.total : '') + ' ' + kind + 'sessions';
    if (o.shown === o.loaded) return 'Showing ' + scope + '. Search looks only at the sessions that are loaded.' + stale;
    return o.shown + ' match among ' + scope + '. Search looks only at the sessions that are loaded: load more to search older ones.' + stale;
  }
  // The notice shown when the control plane left sessions out of a list because it could not read them.
  function unreadableText(n) {
    return n + ' archived session' + (n === 1 ? '' : 's') + ' could not be read by the control plane and ' + (n === 1 ? 'is' : 'are') +
      ' missing from this list. The control plane log names the first one; tsession_controlplane_store_read_errors_total counts them.';
  }

  const api = { pageSize, listQuery, headerCount, idChunks, idsQuery, mergeCounts, sessionOrder, mergeFirstPage, mergeRows, listSummary, unreadableText, entryOf, entryText, bindingText, sessionHay, cmpStart, groupEvents, groupTitle, mutationText, chipsText, httpStatus, fmtClock, kindGroup, fmtAgo, opClass, MARK, isRedactedValue, isoFromNs, unsafeNs, fmtNs, relNs, parsePK, pkLabel, pkRaw, afterUnavailable, flagBase, uncertainFlag, uncertainCount, unresolvedMut, hasUnresolved,
    mergeEvents, ndjson, sessionWarnings, curlFor, connectedText };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.TSUtil = api;
})(typeof window !== 'undefined' ? window : globalThis);
