'use strict';
/* tsession dashboard. Vanilla JS, no external requests. All dynamic data is
   inserted via textContent / createTextNode (no markup strings). */
(function () {
  // ---------- tiny DOM helpers ----------
  function h(tag, props) {
    const el = document.createElement(tag);
    if (props) {
      for (const k of Object.keys(props)) {
        const v = props[k];
        if (v === undefined || v === null || v === false) continue;
        if (k === 'class') el.className = v;
        else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
        else if (k === 'text') el.textContent = v;
        else if (k === 'value') el.value = v;
        else if (k === 'checked' || k === 'disabled' || k === 'hidden') el[k] = !!v;
        else if (k === 'style') continue; // blocked by CSP; use classes
        else el.setAttribute(k, v === true ? '' : String(v));
      }
    }
    for (let i = 2; i < arguments.length; i++) add(el, arguments[i]);
    return el;
  }
  function add(el, c) {
    if (c === undefined || c === null || c === false) return;
    if (Array.isArray(c)) c.forEach((x) => add(el, x));
    else if (c instanceof Node) el.appendChild(c);
    else el.appendChild(document.createTextNode(String(c)));
  }
  function clear(el) { while (el.firstChild) el.removeChild(el.firstChild); return el; }
  function span(cls, text) { return h('span', { class: cls, text: text }); }
  function badge(text, cls, title) { return h('span', { class: 'badge ' + (cls || ''), text: text, title: title }); }

  // ---------- formatting ----------
  function fmtNs(ns) {
    if (ns === undefined || ns === null || isNaN(ns)) return '-';
    const a = Math.abs(ns);
    if (a >= 1e9) return (ns / 1e9).toFixed(2) + ' s';
    if (a >= 1e6) return (ns / 1e6).toFixed(1) + ' ms';
    if (a >= 1e3) return (ns / 1e3).toFixed(0) + ' \u00b5s';
    return ns + ' ns';
  }
  function fmtTime(s) { if (!s) return '-'; const d = new Date(s); return isNaN(d) ? String(s) : d.toLocaleString(); }
  function kindClass(k) {
    const p = String(k || '').split('.')[0];
    return 'kind k-' + (['api', 'db', 'cache', 'queue', 'worker', 'session', 'sdk'].indexOf(p) >= 0 ? p : 'session');
  }
  function stateBadge(st) {
    const cls = { ACTIVE: 'live', DRAINING: 'warn', SEALED: 'ok', ABORTED: 'err', EXPIRED: 'err', CREATED: '' }[st] || '';
    return badge(st || '?', cls);
  }
  const isLive = (s) => s && (s.state === 'ACTIVE' || s.state === 'DRAINING');
  const evDur = (e) => (e.end_ns || 0) - (e.start_ns || 0);
  const rolledBack = (e) => (e.mutations || []).some((m) => m.effective === false);

  // ---------- api ----------
  async function api(path, opts) {
    let r;
    try {
      r = await fetch(path, Object.assign({ cache: 'no-store', credentials: 'omit' }, opts || {}));
    } catch (e) {
      setConn(false);
      throw new Error('network error: ' + e.message);
    }
    setConn(true);
    const txt = await r.text();
    let body = null;
    try { body = txt ? JSON.parse(txt) : null; } catch (e) { body = txt; }
    if (!r.ok) {
      const msg = body && typeof body === 'object' && body.error ? body.error : String(body || r.statusText).slice(0, 200);
      const err = new Error(r.status + ': ' + msg);
      err.status = r.status;
      throw err;
    }
    return body;
  }
  const cp = (p, o) => api('/api/cp' + p, o);
  const col = (p, o) => api('/api/col' + p, o);
  const postJSON = (p, body) => cp(p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
  const asList = (b, key) => (Array.isArray(b) ? b : b && Array.isArray(b[key]) ? b[key] : []);

  function setConn(ok) {
    const c = document.getElementById('conn');
    c.textContent = ok ? 'connected' : 'offline';
  }
  function showError(msg) {
    const b = document.getElementById('banner');
    b.className = 'banner';
    b.textContent = msg;
    b.hidden = !msg;
  }
  function showInfo(msg) {
    const b = document.getElementById('banner');
    b.className = 'banner info';
    b.textContent = msg;
    b.hidden = !msg;
  }

  // ---------- JSON rendering ----------
  const TOK = /("(?:\\.|[^"\\])*")(\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
  function colorize(text) {
    const frag = document.createDocumentFragment();
    let last = 0, m;
    TOK.lastIndex = 0;
    while ((m = TOK.exec(text))) {
      if (m.index > last) frag.appendChild(document.createTextNode(text.slice(last, m.index)));
      if (m[1] !== undefined) {
        if (m[2]) { frag.appendChild(span('j-k', m[1])); frag.appendChild(document.createTextNode(m[2])); }
        else frag.appendChild(span(/REDACTED|\*{3,}/i.test(m[1]) ? 'j-red' : 'j-s', m[1]));
      } else frag.appendChild(span(/^[tfn]/.test(m[0]) ? 'j-b' : 'j-n', m[0]));
      last = TOK.lastIndex;
    }
    if (last < text.length) frag.appendChild(document.createTextNode(text.slice(last)));
    return frag;
  }
  function payloadBlock(label, p) {
    if (!p || (p.inline === undefined && !p.blob_hash)) return null;
    const wrap = h('div', null, h('h3', { text: label }));
    const flags = h('div', { class: 'meta' });
    if (p.redacted) flags.appendChild(badge('redacted', 'warn', 'sensitive values were masked by the SDK'));
    if (p.truncated) flags.appendChild(badge('truncated', 'warn', 'payload exceeded the inline cap'));
    if (p.size) flags.appendChild(badge(p.size + ' bytes'));
    if (p.blob_hash) flags.appendChild(badge('blob ' + p.blob_hash));
    wrap.appendChild(flags);
    if (p.inline !== undefined) {
      const text = typeof p.inline === 'string' && p.truncated ? p.inline : JSON.stringify(p.inline, null, 2);
      wrap.appendChild(h('pre', { class: 'json' }, colorize(text)));
    }
    return wrap;
  }

  // ---------- diff ----------
  function flatten(v, prefix, out) {
    if (v !== null && typeof v === 'object') {
      const keys = Object.keys(v);
      if (!keys.length) { out[prefix || '(root)'] = Array.isArray(v) ? '[]' : '{}'; return out; }
      keys.forEach((k) => flatten(v[k], prefix ? prefix + (Array.isArray(v) ? '[' + k + ']' : '.' + k) : k, out));
    } else out[prefix || '(root)'] = JSON.stringify(v);
    return out;
  }
  function diffRows(mu) {
    const hasB = mu.before && mu.before.inline !== undefined, hasA = mu.after && mu.after.inline !== undefined;
    const rows = [];
    if (hasB || hasA) {
      const b = hasB ? flatten(mu.before.inline, '', {}) : {};
      const a = hasA ? flatten(mu.after.inline, '', {}) : {};
      const keys = Object.keys(b);
      Object.keys(a).forEach((k) => { if (!(k in b)) keys.push(k); });
      keys.forEach((k) => {
        let kind = 'same';
        if (!(k in b)) kind = 'add'; else if (!(k in a)) kind = hasA ? 'del' : 'del';
        else if (a[k] !== b[k]) kind = 'chg';
        rows.push({ field: k, old: b[k], nw: a[k], kind: kind });
      });
      return rows;
    }
    (mu.diff || []).forEach((d) => {
      const o = d.old === undefined || d.old === null ? undefined : JSON.stringify(d.old);
      const n = d.new === undefined || d.new === null ? undefined : JSON.stringify(d.new);
      rows.push({ field: d.field, old: o, nw: n, kind: o === undefined ? 'add' : n === undefined ? 'del' : 'chg' });
    });
    return rows;
  }
  function diffTable(rows) {
    const t = h('table', { class: 'diff' }, h('thead', null, h('tr', null, h('th', { text: 'field' }), h('th', { text: 'before' }), h('th', { text: 'after' }))));
    const tb = h('tbody');
    rows.forEach((r) => {
      tb.appendChild(h('tr', { class: r.kind === 'same' ? 'same' : '' },
        h('td', { text: r.field }),
        h('td', { class: r.kind === 'del' || r.kind === 'chg' ? 'del' : '', text: r.old === undefined ? '' : r.old }),
        h('td', { class: r.kind === 'add' || r.kind === 'chg' ? 'add' : '', text: r.nw === undefined ? '' : r.nw })));
    });
    t.appendChild(tb);
    return t;
  }
  function diffView(mu) {
    const rows = diffRows(mu);
    const changed = rows.filter((r) => r.kind !== 'same'), same = rows.filter((r) => r.kind === 'same');
    const box = h('div');
    if (changed.length) box.appendChild(diffTable(changed));
    else box.appendChild(h('div', { class: 'muted', text: 'no field changes recorded' }));
    if (same.length) box.appendChild(h('details', null, h('summary', { text: same.length + ' unchanged field(s)' }), diffTable(same)));
    return box;
  }
  function effBadge(eff, txid) {
    if (eff === false) return badge('rolled back (effective=false)', 'err');
    if (eff === true) return badge('effective', 'ok');
    return badge(txid ? 'tx unresolved' : 'effective unknown', 'warn');
  }
  function pkText(pk) {
    if (pk === undefined || pk === null) return '';
    if (typeof pk !== 'object') return String(pk);
    const k = Object.keys(pk);
    return k.length === 1 ? String(pk[k[0]]) : k.sort().map((x) => x + '=' + pk[x]).join(',');
  }
  function mutCard(mu, extra) {
    const card = h('div', { class: 'mutcard' + (mu.effective === false ? ' rolled' : '') });
    card.appendChild(h('div', { class: 'meta' },
      extra || null,
      badge(mu.store || '?'), h('strong', { text: mu.entity || '' }), span('mono', pkText(mu.pk)),
      badge(mu.op || '?'), effBadge(mu.effective, mu.tx), mu.source ? badge('src: ' + mu.source) : null));
    card.appendChild(diffView(mu));
    const raw = h('details', null, h('summary', { text: 'raw before / after' }));
    const b = payloadBlock('before', mu.before), a = payloadBlock('after', mu.after);
    if (b) raw.appendChild(b); if (a) raw.appendChild(a);
    if (!b && !a) raw.appendChild(h('div', { class: 'muted', text: 'no snapshots captured' }));
    card.appendChild(raw);
    return card;
  }

  // ---------- state ----------
  const S = { timer: null, token: 0, D: null, health: {} };
  function stopTimers() { if (S.timer) { clearInterval(S.timer); S.timer = null; } }

  // ---------- routing ----------
  function route() {
    const parts = (location.hash.replace(/^#\/?/, '') || '').split('/').map(decodeURIComponent);
    stopTimers();
    S.token++;
    closeDrawer(true);
    showError('');
    const main = clear(document.getElementById('main'));
    if (parts[0] === 's' && parts[1]) viewSession(main, parts[1], parts[2] || 'timeline', S.token);
    else viewSessions(main, S.token);
  }
  window.addEventListener('hashchange', route);

  // ---------- sessions list ----------
  function viewSessions(main, token) {
    document.title = 'Sessions - tsession';
    const f = { state: '', q: '', auto: true };
    let rows = [];
    const stateSel = h('select', { 'aria-label': 'Filter by state', onchange: (e) => { f.state = e.target.value; paint(); } },
      h('option', { value: '', text: 'All states' }),
      ['CREATED', 'ACTIVE', 'DRAINING', 'SEALED', 'ABORTED', 'EXPIRED'].map((s) => h('option', { value: s, text: s })));
    const q = h('input', { type: 'search', placeholder: 'Filter id / name / initiator', 'aria-label': 'Filter sessions', oninput: (e) => { f.q = e.target.value.toLowerCase(); paint(); } });
    const name = h('input', { type: 'text', placeholder: 'session name', 'aria-label': 'New session name', maxlength: 120 });
    const mode = h('select', { 'aria-label': 'Session mode' }, h('option', { value: 'header', text: 'header' }), h('option', { value: 'exclusive', text: 'exclusive' }));
    const startBtn = h('button', { class: 'primary', text: 'Start session', onclick: async () => {
      startBtn.disabled = true;
      try {
        const s = await postJSON('/v1/sessions', { name: name.value.trim(), mode: mode.value, initiated_by: 'dashboard' });
        showError('');
        name.value = '';
        if (s && s.id) location.hash = '#/s/' + encodeURIComponent(s.id) + '/timeline'; else load();
      } catch (e) { showError('Start failed: ' + e.message); } finally { startBtn.disabled = false; }
    } });
    const auto = h('input', { type: 'checkbox', checked: true, onchange: (e) => { f.auto = e.target.checked; } });
    const tbody = h('tbody');
    const count = h('span', { class: 'muted' });
    main.appendChild(h('h1', { text: 'Test sessions' }));
    main.appendChild(h('div', { class: 'toolbar' }, stateSel, q, h('span', { class: 'sep' }), name, mode, startBtn,
      h('label', { class: 'chk' }, auto, 'auto-refresh'), h('button', { text: 'Refresh', onclick: () => load() }), count));
    main.appendChild(h('div', { class: 'tablewrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Session', 'Name', 'State', 'Mode', 'Initiated by', 'Started', 'Duration', 'Events', 'Flags', ''].map((x) => h('th', { text: x })))), tbody)));

    function paint() {
      clear(tbody);
      const vis = rows.filter((s) => (!f.state || s.state === f.state) &&
        (!f.q || [s.id, s.name, s.initiated_by, s.mode].join(' ').toLowerCase().indexOf(f.q) >= 0));
      count.textContent = vis.length + ' of ' + rows.length;
      if (!vis.length) tbody.appendChild(h('tr', null, h('td', { colspan: 10, class: 'empty', text: rows.length ? 'No sessions match the filter.' : 'No sessions yet. Start one above.' })));
      vis.forEach((s) => {
        const open = () => { location.hash = '#/s/' + encodeURIComponent(s.id) + '/timeline'; };
        const start = Date.parse(s.started_at || s.created_at), end = s.ended_at ? Date.parse(s.ended_at) : Date.now();
        const dur = isNaN(start) ? '-' : fmtNs(Math.max(0, end - start) * 1e6);
        const stop = isLive(s) && s.state === 'ACTIVE' ? h('button', { class: 'danger', text: 'Stop', onclick: async (e) => {
          e.stopPropagation();
          try { await postJSON('/v1/sessions/' + encodeURIComponent(s.id) + '/stop'); load(); } catch (er) { showError('Stop failed: ' + er.message); }
        } }) : null;
        const tr = h('tr', { class: 'row', tabindex: 0, onclick: open, onkeydown: (e) => { if (e.key === 'Enter') open(); } },
          h('td', null, h('a', { href: '#/s/' + encodeURIComponent(s.id) + '/timeline', class: 'mono', text: s.id, onclick: (e) => e.stopPropagation() })),
          h('td', { class: 'wrap', text: s.name || '(unnamed)' }), h('td', null, stateBadge(s.state)), h('td', { text: s.mode }),
          h('td', { text: s.initiated_by }), h('td', { text: fmtTime(s.started_at || s.created_at) }), h('td', { class: 'num', text: dur }),
          h('td', { class: 'num', text: s.event_count === undefined ? '-' : s.event_count }),
          h('td', null, s.incomplete ? badge('incomplete', 'warn', 'some async work or events are missing') : null, ' ',
            s.dropped_events ? badge(s.dropped_events + ' dropped', 'err') : null),
          h('td', null, stop));
        tbody.appendChild(tr);
      });
    }
    async function load() {
      try {
        const [a, b] = await Promise.all([cp('/v1/sessions'), col('/v1/sessions').catch(() => [])]);
        if (token !== S.token) return;
        const counts = {};
        asList(b, 'sessions').forEach((x) => { counts[x.id] = x.event_count; });
        rows = asList(a, 'sessions').map((s) => Object.assign({}, s, { event_count: counts[s.id] }));
        rows.sort((x, y) => String(y.created_at).localeCompare(String(x.created_at)));
        showError('');
        paint();
      } catch (e) { if (token === S.token) showError('Cannot load sessions: ' + e.message); }
    }
    load();
    S.timer = setInterval(() => { if (f.auto && !document.hidden) load(); }, 3000);
  }

  // ---------- session detail ----------
  const TABS = [['timeline', 'Timeline'], ['tree', 'Call tree'], ['entities', 'Entities'], ['summary', 'Summary']];

  function viewSession(main, id, tab, token) {
    document.title = id + ' - tsession';
    if (!S.D || S.D.id !== id) {
      S.D = { id: id, sess: null, events: [], bySeq: new Map(), bySpan: new Map(), maxSeq: 0, t0: 0,
        f: { service: '', kind: '', err: false, entity: '', q: '' }, collapsed: new Set(), follow: true };
    }
    const D = S.D;
    D.token = token; D.tab = tab; D.dirty = true;
    const head = h('div', { class: 'panel' });
    const tabs = h('nav', { class: 'tabs', 'aria-label': 'Session views' }, TABS.map((t) =>
      h('a', { href: '#/s/' + encodeURIComponent(id) + '/' + t[0], text: t[1], 'aria-current': t[0] === tab ? 'page' : null })));
    const body = h('div', { id: 'tabbody' });
    main.appendChild(h('p', null, h('a', { href: '#/', text: '\u2190 All sessions' })));
    main.appendChild(head); main.appendChild(tabs); main.appendChild(body);
    D.head = head; D.body = body;
    D.view = null;

    const makers = { timeline: mkTimeline, tree: mkTree, entities: mkEntities, summary: mkSummary };
    const mk = makers[tab] || mkTimeline;
    D.view = mk(D, body);

    async function first() {
      try {
        D.sess = await cp('/v1/sessions/' + encodeURIComponent(id));
        paintHead(D);
        await fetchEvents(D);
        if (token !== S.token) return;
        D.view.update(true);
        paintHead(D);
      } catch (e) { if (token === S.token) showError('Cannot load session: ' + e.message); return; }
      if (isLive(D.sess)) S.timer = setInterval(tick, 1500);
    }
    let busy = false;
    async function tick() {
      if (busy || document.hidden) return;
      busy = true;
      try {
        const s = await cp('/v1/sessions/' + encodeURIComponent(id));
        if (token !== S.token) return;
        const added = await fetchEvents(D);
        D.sess = s;
        paintHead(D);
        D.view.update(added > 0);
        if (!isLive(s)) { stopTimers(); }
      } catch (e) { if (token === S.token) showError('Live update failed: ' + e.message); } finally { busy = false; }
    }
    first();
  }

  async function fetchEvents(D) {
    let added = 0, guard = 0;
    for (;;) {
      const r = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/events?from_seq=' + D.maxSeq + '&limit=1000');
      const evs = (r && r.events) || [];
      let progressed = false;
      evs.forEach((e) => {
        if (D.bySeq.has(e.seq)) return;
        D.bySeq.set(e.seq, e); D.bySpan.set(e.span_id, e); D.events.push(e);
        if (e.seq > D.maxSeq) { D.maxSeq = e.seq; progressed = true; }
        if (!D.t0 || e.start_ns < D.t0) D.t0 = e.start_ns;
        added++;
      });
      if (r && r.next_seq > D.maxSeq) { D.maxSeq = r.next_seq; progressed = true; }
      if (evs.length < 1000 || !progressed || ++guard > 200) break;
    }
    if (added) D.events.sort((a, b) => a.seq - b.seq);
    return added;
  }

  function paintHead(D) {
    const s = D.sess; if (!s) return;
    const head = clear(D.head);
    const start = Date.parse(s.started_at || s.created_at), end = s.ended_at ? Date.parse(s.ended_at) : Date.now();
    head.appendChild(h('div', { class: 'meta' }, h('h1', { text: s.name || '(unnamed session)' }), stateBadge(s.state),
      isLive(s) ? badge('\u25cf live tail', 'live') : null,
      s.incomplete ? badge('incomplete', 'warn', 'some async work or events are missing') : null,
      s.dropped_events ? badge(s.dropped_events + ' dropped events', 'err') : null,
      h('span', { class: 'grow' }),
      s.state === 'ACTIVE' ? h('button', { class: 'danger', text: 'Stop session', onclick: async () => {
        try { await postJSON('/v1/sessions/' + encodeURIComponent(s.id) + '/stop'); } catch (e) { showError('Stop failed: ' + e.message); }
      } }) : null));
    head.appendChild(h('div', { class: 'meta muted' }, span('mono', s.id), 'mode: ' + s.mode, 'initiated by: ' + s.initiated_by,
      'started: ' + fmtTime(s.started_at || s.created_at), 'duration: ' + (isNaN(start) ? '-' : fmtNs(Math.max(0, end - start) * 1e6)),
      D.events.length + ' events', s.bundle_uri ? 'bundle: ' + s.bundle_uri : null));
    if (s.incomplete) showInfo('This session is marked incomplete: some events or async work may be missing' + (s.dropped_events ? ' (' + s.dropped_events + ' events dropped).' : '.'));
  }

  // ---------- event drawer ----------
  let lastFocus = null;
  function closeDrawer(silent) {
    const d = document.getElementById('drawer');
    if (d.hidden) return;
    d.hidden = true; clear(d);
    document.querySelectorAll('tr.sel').forEach((r) => r.classList.remove('sel'));
    if (!silent && lastFocus && document.contains(lastFocus)) lastFocus.focus();
  }
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });

  function spanRef(D, id) {
    const e = D.bySpan.get(id);
    if (!e) return span('mono', id + ' (not in session)');
    return h('button', { class: 'mono', text: '#' + e.seq + ' ' + e.kind, title: id, onclick: () => openDrawer(D, e) });
  }
  function openDrawer(D, e, row) {
    const d = document.getElementById('drawer');
    lastFocus = document.activeElement;
    clear(d);
    document.querySelectorAll('tr.sel').forEach((r) => r.classList.remove('sel'));
    if (row) row.classList.add('sel');
    d.appendChild(h('header', null, span(kindClass(e.kind), e.kind), h('h2', { id: 'drawer-title', text: e.name || '(no name)' }),
      h('button', { text: 'Close (Esc)', onclick: () => closeDrawer() })));
    const kv = h('dl', { class: 'kv' });
    const row2 = (k, v) => { if (v === undefined || v === null || v === '' || (Array.isArray(v) && !v.length)) return; kv.appendChild(h('dt', { text: k })); kv.appendChild(h('dd', null, v)); };
    row2('seq', String(e.seq));
    row2('service', e.service + (e.instance ? ' (' + e.instance + ')' : ''));
    row2('start', '+' + fmtNs(e.start_ns - D.t0) + ' (' + new Date(e.start_ns / 1e6).toISOString() + ')');
    row2('duration', fmtNs(evDur(e)));
    row2('status', e.err ? badge('error', 'err') : badge('ok', 'ok'));
    row2('error', e.err ? span('j-red', e.err) : null);
    row2('attempt', e.attempt ? String(e.attempt) : null);
    row2('tx_id', e.tx_id);
    row2('span_id', span('mono', e.span_id));
    row2('trace_id', span('mono', e.trace_id));
    row2('parent', e.parent_id ? spanRef(D, e.parent_id) : null);
    row2('linked from', (e.link_ids || []).map((l) => spanRef(D, l)));
    row2('hlc', e.hlc + ' \u2192 ' + e.end_hlc + ', local_seq ' + e.local_seq);
    if (e.attrs) Object.keys(e.attrs).sort().forEach((k) => row2('attr ' + k, e.attrs[k]));
    d.appendChild(kv);
    if (e.flags && e.flags.length) d.appendChild(h('div', { class: 'meta' }, e.flags.map((f) => badge(f, 'warn'))));
    [payloadBlock('Request', e.req), payloadBlock('Response', e.resp)].forEach((b) => b && d.appendChild(b));
    if (e.mutations && e.mutations.length) {
      d.appendChild(h('h3', { text: 'Mutations (' + e.mutations.length + ')' }));
      e.mutations.forEach((m) => d.appendChild(mutCard(Object.assign({ tx: e.tx_id }, m))));
    }
    if (e.reads && e.reads.length) {
      d.appendChild(h('h3', { text: 'Reads (' + e.reads.length + ')' }));
      e.reads.forEach((r) => {
        const c = h('div', { class: 'mutcard' }, h('div', { class: 'meta' }, badge(r.store || '?'), h('strong', { text: r.entity || '' }),
          r.row_count !== undefined ? badge(r.row_count + ' row(s)') : null), r.statement ? h('pre', { class: 'json', text: r.statement }) : null,
          r.pks && r.pks.length ? h('div', null, 'pks: ', span('mono', r.pks.join(', '))) : null);
        [payloadBlock('args', r.args), payloadBlock('sample', r.sample)].forEach((b) => b && c.appendChild(b));
        d.appendChild(c);
      });
    }
    d.hidden = false;
    d.querySelector('button').focus();
  }

  // ---------- timeline ----------
  function mkTimeline(D, root) {
    const f = D.f;
    const svc = h('select', { 'aria-label': 'Filter by service', onchange: (e) => { f.service = e.target.value; paint(); } });
    const kind = h('select', { 'aria-label': 'Filter by kind', onchange: (e) => { f.kind = e.target.value; paint(); } });
    const err = h('input', { type: 'checkbox', checked: f.err, onchange: (e) => { f.err = e.target.checked; paint(); } });
    const ent = h('input', { type: 'search', placeholder: 'entity / store / pk', value: f.entity, 'aria-label': 'Filter by entity', oninput: (e) => { f.entity = e.target.value.toLowerCase(); paint(); } });
    const q = h('input', { type: 'search', placeholder: 'search name, error, payloads, ids', value: f.q, 'aria-label': 'Search events', oninput: (e) => { f.q = e.target.value.toLowerCase(); paint(); } });
    const follow = h('input', { type: 'checkbox', checked: D.follow, onchange: (e) => { D.follow = e.target.checked; } });
    const count = h('span', { class: 'muted' });
    const tbody = h('tbody');
    root.appendChild(h('div', { class: 'toolbar' }, svc, kind, h('label', { class: 'chk' }, err, 'errors only'), ent, q, count,
      h('label', { class: 'chk followbox' }, follow, 'follow tail')));
    root.appendChild(h('div', { class: 'tablewrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Seq', 'Time', 'Service', 'Kind', 'Name', 'Duration', 'Status'].map((x) => h('th', { text: x })))), tbody)));
    let optKey = '';
    function syncOptions() {
      const ss = Array.from(new Set(D.events.map((e) => e.service))).sort(), ks = Array.from(new Set(D.events.map((e) => e.kind))).sort();
      const key = ss.join('|') + '#' + ks.join('|');
      if (key === optKey) return;
      optKey = key;
      const fill = (sel, label, vals, cur) => { clear(sel); sel.appendChild(h('option', { value: '', text: label })); vals.forEach((v) => sel.appendChild(h('option', { value: v, text: v }))); sel.value = cur; };
      fill(svc, 'All services', ss, f.service); fill(kind, 'All kinds', ks, f.kind);
    }
    const hay = (e) => {
      if (e._h === undefined) {
        e._h = [e.service, e.kind, e.name, e.err, e.span_id, e.trace_id, e.tx_id, JSON.stringify(e.attrs || ''),
          e.req && JSON.stringify(e.req.inline), e.resp && JSON.stringify(e.resp.inline)].join(' ').toLowerCase();
      }
      return e._h;
    };
    const entHay = (e) => (e.mutations || []).map((m) => [m.store, m.entity, pkText(m.pk)].join(' ')).concat((e.reads || []).map((r) => [r.store, r.entity, (r.pks || []).join(' ')].join(' '))).join(' ').toLowerCase();
    const MAX = 5000;
    function paint() {
      syncOptions();
      const vis = D.events.filter((e) => (!f.service || e.service === f.service) && (!f.kind || e.kind === f.kind) && (!f.err || e.err) &&
        (!f.entity || entHay(e).indexOf(f.entity) >= 0) && (!f.q || hay(e).indexOf(f.q) >= 0));
      count.textContent = vis.length + ' of ' + D.events.length + ' events';
      clear(tbody);
      if (!vis.length) tbody.appendChild(h('tr', null, h('td', { colspan: 7, class: 'empty', text: D.events.length ? 'No events match the filters.' : 'No events recorded yet.' })));
      const frag = document.createDocumentFragment();
      vis.slice(-MAX).forEach((e) => {
        const open = () => openDrawer(D, e, tr);
        const status = e.err ? badge('error', 'err', e.err) : rolledBack(e) ? badge('rolled back', 'warn') : badge('ok', 'ok');
        const tr = h('tr', { class: 'row' + (e.err ? ' err' : ''), tabindex: 0, onclick: open, onkeydown: (ev) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); open(); } } },
          h('td', { class: 'num', text: e.seq }), h('td', { class: 'num mono', text: '+' + fmtNs(e.start_ns - D.t0) }), h('td', { text: e.service }),
          h('td', null, span(kindClass(e.kind), e.kind)), h('td', { class: 'wrap', text: (e.attempt ? '[#' + e.attempt + '] ' : '') + (e.name || '') }),
          h('td', { class: 'num', text: fmtNs(evDur(e)) }), h('td', null, status, e.flags && e.flags.length ? ' ' : null, (e.flags || []).map((x) => badge(x))));
        frag.appendChild(tr);
      });
      tbody.appendChild(frag);
      if (vis.length > MAX) tbody.insertBefore(h('tr', null, h('td', { colspan: 7, class: 'muted', text: 'Showing the latest ' + MAX + ' matching events; narrow the filters to see earlier ones.' })), tbody.firstChild);
    }
    return { update(changed) {
      follow.parentNode.hidden = !isLive(D.sess);
      if (changed || D.dirty) { D.dirty = false; paint(); if (D.follow && isLive(D.sess) && changed) window.scrollTo(0, document.body.scrollHeight); }
    } };
  }

  // ---------- call tree ----------
  function mkTree(D, root) {
    const box = h('div', { class: 'panel' });
    const info = h('span', { class: 'muted' });
    root.appendChild(h('div', { class: 'toolbar' },
      h('button', { text: 'Expand all', onclick: () => { D.collapsed.clear(); paint(); } }),
      h('button', { text: 'Collapse all', onclick: () => { walk(D.trace || [], (n) => { if (n.children.length || n.linked.length) D.collapsed.add(n.event.span_id); }); paint(); } }), info));
    root.appendChild(box);
    D.trace = null;
    const walk = (nodes, fn) => nodes.forEach((n) => { fn(n); walk(n.children || [], fn); walk(n.linked || [], fn); });
    function norm(n) { n.children = n.children || []; n.linked = n.linked || []; n.children.forEach(norm); n.linked.forEach(norm); }
    let t0 = 0, total = 1;
    function paint() {
      clear(box);
      const roots = D.trace || [];
      if (!roots.length) { box.appendChild(h('div', { class: 'empty', text: 'No spans yet.' })); return; }
      let mn = Infinity, mx = 0, count = 0;
      walk(roots, (n) => { count++; mn = Math.min(mn, n.event.start_ns); mx = Math.max(mx, n.event.end_ns); });
      t0 = mn; total = Math.max(1, mx - mn);
      info.textContent = count + ' spans, ' + fmtNs(total) + ' wall time';
      const ul = h('ul', { class: 'tree', role: 'tree' });
      roots.forEach((n) => ul.appendChild(nodeEl(n, false)));
      box.appendChild(ul);
    }
    function descendants(n, kind) { let c = 0; walk([n], (x) => { if (x.event.kind === kind) c++; }); return c; }
    function nodeEl(n, async_) {
      const e = n.event, id = e.span_id, kids = n.children.length + n.linked.length;
      const collapsed = D.collapsed.has(id);
      const li = h('li', { class: 'tnode', role: 'treeitem', 'aria-expanded': kids ? String(!collapsed) : null });
      const tw = kids ? h('button', { class: 'twist', text: collapsed ? '\u25b8' : '\u25be', 'aria-label': collapsed ? 'expand' : 'collapse', onclick: () => { if (collapsed) D.collapsed.delete(id); else D.collapsed.add(id); paint(); } }) : h('span');
      const left = Math.max(0, (e.start_ns - t0) / total * 100), width = Math.max(0.5, evDur(e) / total * 100);
      const bar = h('div', { class: 'track' }, h('i'));
      bar.firstChild.style.left = Math.min(left, 99.5) + '%';
      bar.firstChild.style.width = Math.min(width, 100 - Math.min(left, 99.5)) + '%';
      const content = h('div', { class: 'tcontent' }, span(kindClass(e.kind), e.kind),
        h('button', { class: 'name', text: e.name || '(no name)', title: e.err || e.name, onclick: () => openDrawer(D, e) }),
        h('span', { class: 'svc', text: e.service }),
        e.attempt ? badge('attempt ' + e.attempt) : null,
        async_ && e.kind === 'worker.start' ? badge('async', 'async', 'runs asynchronously after the publisher returned') : null,
        e.err ? badge('error', 'err', e.err) : null,
        rolledBack(e) ? badge('rolled back', 'warn') : null,
        e.kind === 'worker.dlq' ? badge('DLQ', 'err') : null);
      if (e.kind === 'queue.publish') {
        const workers = n.linked.filter((x) => x.event.kind === 'worker.start');
        if (!n.linked.length) content.appendChild(isLive(D.sess) ? badge('pending async', 'warn', 'no worker has consumed this message yet') : badge('orphaned async', 'err', 'no worker ever processed this message'));
        else {
          if (workers.length > 1) content.appendChild(badge(workers.length + ' attempts'));
          if (descendants(n, 'worker.dlq')) content.appendChild(badge('dead-lettered', 'err'));
        }
      }
      const row = h('div', { class: 'trow' + (e.err ? ' err' : '') }, tw, content, bar, h('span', { class: 'num mono', text: fmtNs(evDur(e)) }));
      li.appendChild(row);
      if (kids && !collapsed) {
        const ul = h('ul', { role: 'group' });
        n.children.forEach((c) => ul.appendChild(nodeEl(c, async_)));
        li.appendChild(ul);
        if (n.linked.length) {
          const g = h('div', { class: 'async-group' }, h('div', { class: 'async-label', text: '\u21aa async: worker span(s) linked from this ' + e.kind }));
          const gu = h('ul', { class: 'tree', role: 'group' });
          n.linked.forEach((c) => gu.appendChild(nodeEl(c, true)));
          g.appendChild(gu); li.appendChild(g);
        }
      }
      return li;
    }
    return { async update(changed) {
      if (!changed && D.trace) return;
      try {
        const r = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/trace');
        if (D.token !== S.token) return;
        D.trace = (r && r.roots) || []; D.trace.forEach(norm); paint();
      } catch (e) { clear(box); box.appendChild(h('div', { class: 'empty', text: 'Cannot load trace: ' + e.message })); }
    } };
  }

  // ---------- entities ----------
  function mkEntities(D, root) {
    const list = h('ul', { class: 'list' }), detail = h('div', { class: 'panel' }, h('div', { class: 'empty', text: 'Select an entity to see its mutation history.' }));
    const q = h('input', { type: 'search', placeholder: 'filter store / entity / pk', 'aria-label': 'Filter entities', oninput: (e) => { D.entQ = e.target.value.toLowerCase(); paintList(); } });
    root.appendChild(h('div', { class: 'toolbar' }, q));
    root.appendChild(h('div', { class: 'split' }, h('div', { class: 'panel' }, list), detail));
    D.ents = D.ents || null;
    function paintList() {
      clear(list);
      const items = (D.ents || []).filter((x) => !D.entQ || [x.store, x.entity, pkText(x.pk)].join(' ').toLowerCase().indexOf(D.entQ) >= 0);
      if (!items.length) list.appendChild(h('li', { class: 'empty', text: D.ents ? 'No mutations recorded.' : 'Loading...' }));
      items.forEach((x) => {
        const key = [x.store, x.entity, pkText(x.pk)].join('\u0000');
        list.appendChild(h('li', null, h('button', { 'aria-current': D.entSel === key ? 'true' : null, onclick: () => { D.entSel = key; paintList(); loadDetail(x); } },
          h('div', null, badge(x.store), ' ', h('strong', { text: x.entity })), h('div', { class: 'mono muted', text: pkText(x.pk) + '  \u00b7 ' + x.mutation_count + ' mutation(s)' }))));
      });
    }
    async function loadDetail(x) {
      clear(detail).appendChild(h('div', { class: 'empty', text: 'Loading...' }));
      try {
        const r = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/entities/' + encodeURIComponent(x.store) + '/' + encodeURIComponent(x.entity) + '/' + encodeURIComponent(pkText(x.pk)));
        clear(detail);
        detail.appendChild(h('div', { class: 'meta' }, badge(x.store), h('h2', { text: x.entity }), span('mono', pkText(x.pk))));
        const ms = asList(r, 'mutations');
        if (!ms.length) detail.appendChild(h('div', { class: 'empty', text: 'No mutations.' }));
        ms.forEach((it) => {
          const m = Object.assign({}, it.mutation || {}, it);
          delete m.mutation;
          const ev = it.seq !== undefined ? D.bySeq.get(it.seq) : null;
          const lead = [h('strong', { text: '#' + it.seq }), badge(m.service || (ev && ev.service) || ''), ev ? h('button', { text: 'open event', onclick: () => openDrawer(D, ev) }) : null];
          detail.appendChild(mutCard(Object.assign({ store: x.store, entity: x.entity, pk: x.pk }, m, { effective: it.effective !== undefined ? it.effective : (it.mutation || {}).effective }), lead));
        });
      } catch (e) { clear(detail).appendChild(h('div', { class: 'empty', text: 'Cannot load entity: ' + e.message })); }
    }
    return { async update(changed) {
      if (!changed && D.ents) return;
      try {
        D.ents = asList(await col('/v1/sessions/' + encodeURIComponent(D.id) + '/entities'), 'entities');
        if (D.token === S.token) paintList();
      } catch (e) { clear(list).appendChild(h('li', { class: 'empty', text: 'Cannot load entities: ' + e.message })); }
    } };
  }

  // ---------- summary ----------
  function mkSummary(D, root) {
    const box = h('div');
    root.appendChild(box);
    D.srv = D.srv || null;
    function countTable(title, obj) {
      const keys = Object.keys(obj).sort((a, b) => obj[b] - obj[a]);
      const max = Math.max(1, ...keys.map((k) => obj[k]));
      const t = h('table', null, h('thead', null, h('tr', null, h('th', { text: title }), h('th', { class: 'num', text: 'count' }), h('th'))));
      const tb = h('tbody');
      keys.forEach((k) => {
        const bar = h('span', { class: 'bar' });
        bar.style.width = Math.max(2, Math.round(obj[k] / max * 120)) + 'px';
        tb.appendChild(h('tr', null, h('td', { text: k }), h('td', { class: 'num', text: obj[k] }), h('td', null, bar)));
      });
      t.appendChild(tb);
      return h('div', { class: 'tablewrap' }, t);
    }
    function paint() {
      clear(box);
      const evs = D.events, s = D.sess || {}, srv = D.srv || {};
      const byKind = {}, bySvc = {}, muts = {}, fails = [], closed = new Set(), flagc = {};
      let dropped = s.dropped_events || 0, minS = Infinity, maxE = 0;
      evs.forEach((e) => {
        byKind[e.kind] = (byKind[e.kind] || 0) + 1; bySvc[e.service] = (bySvc[e.service] || 0) + 1;
        if (e.err) fails.push(e);
        (e.mutations || []).forEach((m) => { const k = m.store + ' / ' + m.entity; const o = muts[k] || (muts[k] = { n: 0, ops: {}, rb: 0 }); o.n++; o.ops[m.op] = (o.ops[m.op] || 0) + 1; if (m.effective === false) o.rb++; });
        (e.flags || []).forEach((f) => { flagc[f] = (flagc[f] || 0) + 1; });
        if (e.kind === 'worker.end') (e.link_ids || []).forEach((l) => closed.add(l));
        if (e.kind === 'sdk.dropped' && e.attrs && !s.dropped_events) dropped += parseInt(e.attrs.count, 10) || 0;
        minS = Math.min(minS, e.start_ns); maxE = Math.max(maxE, e.end_ns);
      });
      const pending = evs.filter((e) => e.kind === 'queue.publish' && !closed.has(e.span_id));
      const rbCount = Object.keys(muts).reduce((a, k) => a + muts[k].rb, 0);
      const dur = evs.length ? maxE - minS : 0;
      if (s.incomplete) box.appendChild(h('div', { class: 'banner info', text: 'Incomplete session: some events or async work are missing. Treat the history below as a lower bound.' }));
      const card = (label, v, cls) => h('div', { class: 'card ' + (cls || '') }, h('div', { class: 'muted', text: label }), h('div', { class: 'v', text: v }));
      box.appendChild(h('div', { class: 'cards' }, card('Events', evs.length), card('Duration', fmtNs(dur)), card('Services', Object.keys(bySvc).length),
        card('Failures', fails.length, fails.length ? 'bad' : ''), card('Rolled-back mutations', rbCount, rbCount ? 'warnc' : ''),
        card(isLive(s) ? 'Pending async' : 'Orphaned async', pending.length, pending.length ? (isLive(s) ? 'warnc' : 'bad') : ''),
        card('Dropped events', dropped, dropped ? 'bad' : '')));
      box.appendChild(h('div', { class: 'grid2' }, h('div', null, h('h2', { text: 'Events by kind' }), countTable('kind', byKind)),
        h('div', null, h('h2', { text: 'Events by service' }), countTable('service', bySvc))));
      box.appendChild(h('h2', { text: 'Mutations by store / entity' }));
      const mk = Object.keys(muts).sort();
      if (!mk.length) box.appendChild(h('div', { class: 'muted', text: 'No mutations recorded.' }));
      else {
        const tb = h('tbody');
        mk.forEach((k) => tb.appendChild(h('tr', null, h('td', { text: k }), h('td', { class: 'num', text: muts[k].n }),
          h('td', { text: Object.keys(muts[k].ops).sort().map((o) => o + ' \u00d7' + muts[k].ops[o]).join(', ') }),
          h('td', null, muts[k].rb ? badge(muts[k].rb + ' rolled back', 'warn') : null))));
        box.appendChild(h('div', { class: 'tablewrap' }, h('table', null, h('thead', null, h('tr', null, ['store / entity', 'mutations', 'ops', ''].map((x) => h('th', { text: x })))), tb)));
      }
      box.appendChild(h('h2', { text: 'Failures (' + fails.length + ')' }));
      if (!fails.length) box.appendChild(h('div', { class: 'muted', text: 'No failed spans.' }));
      else {
        const tb = h('tbody');
        fails.forEach((e) => tb.appendChild(h('tr', { class: 'row err', tabindex: 0, onclick: () => openDrawer(D, e), onkeydown: (ev) => { if (ev.key === 'Enter') openDrawer(D, e); } },
          h('td', { class: 'num', text: e.seq }), h('td', { text: e.service }), h('td', null, span(kindClass(e.kind), e.kind)), h('td', { class: 'wrap', text: e.name }), h('td', { class: 'wrap', text: e.err }))));
        box.appendChild(h('div', { class: 'tablewrap' }, h('table', null, h('thead', null, h('tr', null, ['seq', 'service', 'kind', 'name', 'error'].map((x) => h('th', { text: x })))), tb)));
      }
      box.appendChild(h('h2', { text: (isLive(s) ? 'Pending' : 'Orphaned') + ' async work (' + pending.length + ')' }));
      if (!pending.length) box.appendChild(h('div', { class: 'muted', text: 'Every queue.publish has a matching worker.end.' }));
      else pending.forEach((e) => box.appendChild(h('div', null, isLive(s) ? badge('pending', 'warn') : badge('orphaned', 'err'), ' ', h('button', { text: '#' + e.seq + ' ' + e.name, onclick: () => openDrawer(D, e) }))));
      const fk = Object.keys(flagc);
      if (fk.length) box.appendChild(h('div', { class: 'meta' }, h('h2', { text: 'Flags' }), fk.sort().map((f) => badge(f + ' \u00d7' + flagc[f], 'warn'))));
      if (D.srv) box.appendChild(h('details', null, h('summary', { text: 'Raw collector summary' }), h('pre', { class: 'json' }, colorize(JSON.stringify(srv, null, 2)))));
    }
    return { async update(changed) {
      if (changed || D.dirty || !D.srv) {
        D.dirty = false;
        try { D.srv = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/summary'); } catch (e) { D.srv = D.srv || null; }
      }
      if (D.token === S.token) paint();
    } };
  }

  // ---------- boot ----------
  fetch('/healthz', { cache: 'no-store' }).then((r) => r.json()).then((j) => {
    document.getElementById('mode-note').textContent = j.demo ? 'demo mode - synthetic data' : '';
  }).catch(() => {});
  route();
})();
