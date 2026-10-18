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
        else if (k === 'style') continue; /* blocked by CSP; use classes or CSSOM */
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

  /* SVG helpers. The namespace string is assembled at runtime so no URL literal ships in the assets. */
  const SVGNS = 'http:' + ['', '', 'www.w3.org', '2000', 'svg'].join('/');
  function sv(tag, attrs) {
    const el = document.createElementNS(SVGNS, tag);
    if (attrs) for (const k of Object.keys(attrs)) { const v = attrs[k]; if (v !== undefined && v !== null && v !== false) el.setAttribute(k, String(v)); }
    for (let i = 2; i < arguments.length; i++) { const c = arguments[i]; if (c === null || c === undefined || c === false) continue; (Array.isArray(c) ? c : [c]).forEach((x) => el.appendChild(typeof x === 'string' ? document.createTextNode(x) : x)); }
    return el;
  }
  function icon(name, cls) {
    const s = sv('svg', { class: 'ico' + (cls ? ' ' + cls : ''), 'aria-hidden': 'true', focusable: 'false' });
    s.appendChild(sv('use', { href: '#i-' + name }));
    return s;
  }
  function illustration(name) {
    const s = sv('svg', { class: 'ill', viewBox: '0 0 160 110', 'aria-hidden': 'true', focusable: 'false' });
    s.appendChild(sv('use', { href: '#i-ill-' + name }));
    return s;
  }

  // ---------- formatting ----------
  const U = window.TSUtil;
  const fmtNs = U.fmtNs;
  function fmtTime(s) { if (!s) return '-'; const d = new Date(s); return isNaN(d) ? String(s) : d.toLocaleString(); }
  function fmtAgoIso(s) { const t = Date.parse(s); return isNaN(t) ? '-' : U.fmtAgo(Date.now() - t); }
  const KIND_ICON = { api: 'api', db: 'db', cache: 'cache', queue: 'queue', worker: 'worker', retry: 'retry', dlq: 'dlq', tx: 'tx', sys: 'sys' };
  /* Chip = glyph + full kind text, so the kind is never conveyed by colour alone. */
  function kchip(kind, isErr) {
    const g = U.kindGroup(kind);
    return h('span', { class: 'kchip kg-' + g + (isErr ? ' err' : ''), title: g + ' event' }, icon(KIND_ICON[g]), span('', kind || '?'));
  }
  const FLAG_CLS = { tx_unresolved: 't-warn', orphaned: 't-err', oversize_dropped: 't-err', image_skipped: '', propagation_skipped: '' };
  const FLAG_HELP = { tx_unresolved: 'no commit/rollback seen for this transaction', orphaned: 'async work with no matching counterpart',
    image_skipped: 'before/after image not captured', oversize_dropped: 'payload dropped for exceeding size limits',
    propagation_skipped: 'session context was not propagated downstream', unattributed: 'event could not be attributed to a session with certainty' };
  function tag(text, tone, title, ic) { return h('span', { class: 'tag' + (tone ? ' ' + tone : ''), title: title }, ic ? icon(ic) : null, text); }
  function redChip() { return h('span', { class: 'redchip', title: 'value masked by the SDK' }, icon('lock'), '[REDACTED]'); }
  const ST_CLS = { ACTIVE: 'st-active', DRAINING: 'st-draining', SEALED: 'st-sealed', ABORTED: 'st-dead', EXPIRED: 'st-dead', CREATED: 'st-created' };
  const ST_HELP = { ACTIVE: 'accepting events', DRAINING: 'waiting for in-flight async work to finish', SEALED: 'finalised; history is complete',
    ABORTED: 'stopped before sealing', EXPIRED: 'ran past its TTL', CREATED: 'created, no traffic yet' };
  function statePill(st) {
    return h('span', { class: 'pill ' + (ST_CLS[st] || 'st-dead'), title: ST_HELP[st] || '' }, h('span', { class: 'dot', 'aria-hidden': 'true' }), st || '?');
  }
  const isLive = (s) => s && (s.state === 'ACTIVE' || s.state === 'DRAINING');
  const evDur = (e) => (e.end_ns || 0) - (e.start_ns || 0);
  const rel = (e) => U.relNs(e.start_ns, S.D ? S.D.t0 : 0);
  const rolledBack = (e) => (e.mutations || []).some((m) => m.effective === false);
  const idOf = (p, x) => p + String(x).replace(/[^\w-]/g, '_');

  // ---------- toasts, copy, theme, connection ----------
  function toast(msg, tone) {
    const box = document.getElementById('toasts');
    const t = h('div', { class: 'toast ' + (tone || '') }, icon(tone === 'err' ? 'err' : 'check'), span('', msg));
    box.appendChild(t);
    requestAnimationFrame(() => t.classList.add('in'));
    setTimeout(() => { t.classList.remove('in'); setTimeout(() => { if (t.parentNode) t.parentNode.removeChild(t); }, 200); }, tone === 'err' ? 4200 : 2200);
  }
  async function copyText(text, label) {
    try { await navigator.clipboard.writeText(text); toast('Copied ' + (label || 'to clipboard'), 'ok'); return true; }
    catch (e) { toast('Copy failed: clipboard unavailable', 'err'); return false; }
  }
  function copyBtn(getText, label, cls) {
    const b = h('button', { type: 'button', class: 'icon-btn sm ' + (cls || ''), title: 'Copy ' + label, 'aria-label': 'Copy ' + label, onclick: async (ev) => {
      ev.stopPropagation();
      if (await copyText(typeof getText === 'function' ? getText() : getText, label)) {
        b.replaceChild(icon('check'), b.firstChild);
        setTimeout(() => { if (b.firstChild) b.replaceChild(icon('copy'), b.firstChild); }, 1200);
      }
    } }, icon('copy'));
    return b;
  }
  function currentTheme() { return document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark'; }
  function paintThemeBtn() {
    const b = document.getElementById('theme-btn'), light = currentTheme() === 'light';
    clear(b).appendChild(icon(light ? 'moon' : 'sun'));
    const l = light ? 'Switch to dark theme' : 'Switch to light theme';
    b.setAttribute('aria-label', l); b.title = l;
  }
  function toggleTheme() {
    const t = currentTheme() === 'light' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', t);
    try { localStorage.setItem('tsession.theme', t); } catch (e) { /* ignore */ }
    paintThemeBtn();
  }
  function paintConn() {
    const c = document.getElementById('conn');
    c.className = 'conn ' + (S.online === false ? 'bad' : S.live ? 'live' : S.online ? 'ok' : '');
    c.querySelector('.conn-text').textContent = S.online === false ? 'Offline' : S.live ? 'Live' : S.online ? 'Connected' : 'Connecting';
  }
  function setConn(ok) { if (S.online !== ok) { S.online = ok; paintConn(); } }
  function setLive(on) { if (S.live !== !!on) { S.live = !!on; paintConn(); } }
  function showError(msg) {
    const b = document.getElementById('banner');
    b.className = 'banner';
    clear(b);
    if (msg) b.appendChild(icon('err'));
    b.appendChild(document.createTextNode(msg || ''));
    b.hidden = !msg;
  }

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

  // ---------- JSON viewer (DOM nodes only) ----------
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
  function scalar(v) {
    if (typeof v === 'string') {
      if (U.isRedactedValue(v)) return redChip();
      return span(/REDACTED|\*{3,}/i.test(v) ? 'j-red' : 'j-s', JSON.stringify(v));
    }
    if (typeof v === 'number') return span('j-n', String(v));
    return span('j-b', String(v));
  }
  const JSON_CAP = 200;
  function jnode(key, val, depth) {
    const isObj = val !== null && typeof val === 'object', isArr = Array.isArray(val);
    const keyEl = key === null ? null : [typeof key === 'number' ? span('j-idx', key) : span('j-k', JSON.stringify(key)), span('j-p', ': ')];
    if (!isObj) return h('div', { class: 'jl' }, keyEl, scalar(val));
    const keys = Object.keys(val);
    if (!keys.length) return h('div', { class: 'jl' }, keyEl, span('j-p', isArr ? '[]' : '{}'));
    const open = depth < 2 && keys.length <= 40;
    const node = h('div', { class: 'jn' + (open ? '' : ' collapsed') });
    const tog = h('button', { type: 'button', class: 'jt', 'aria-expanded': String(open), 'aria-label': 'Toggle ' + (key === null ? 'root' : key), onclick: () => {
      node.classList.toggle('collapsed');
      tog.setAttribute('aria-expanded', String(!node.classList.contains('collapsed')));
    } }, icon('chev-r'));
    node.appendChild(h('div', { class: 'jl' }, tog, keyEl, span('j-p', isArr ? '[' : '{'),
      span('j-sum', ' ' + keys.length + (isArr ? (keys.length === 1 ? ' item ' : ' items ') : (keys.length === 1 ? ' key ' : ' keys ')) + (isArr ? ']' : '}'))));
    const kids = h('div', { class: 'jc' });
    keys.slice(0, JSON_CAP).forEach((k) => kids.appendChild(jnode(isArr ? Number(k) : k, val[k], depth + 1)));
    if (keys.length > JSON_CAP) kids.appendChild(h('div', { class: 'jl j-sum', text: '\u2026 ' + (keys.length - JSON_CAP) + ' more' }));
    node.appendChild(kids);
    node.appendChild(h('div', { class: 'jl jend' }, span('j-p', isArr ? ']' : '}')));
    return node;
  }
  function jsonViewer(value, label) {
    const body = h('div', { class: 'jv-body' }, jnode(null, value, 0));
    const setAll = (open) => body.querySelectorAll('.jn').forEach((n) => { n.classList.toggle('collapsed', !open); const t = n.querySelector('.jt'); if (t) t.setAttribute('aria-expanded', String(open)); });
    const isC = value !== null && typeof value === 'object';
    return h('div', { class: 'jv' }, h('div', { class: 'jv-bar' }, span('', label || 'JSON'), h('span', { class: 'grow' }),
      isC ? h('button', { type: 'button', class: 'btn ghost sm', text: 'Expand all', onclick: () => setAll(true) }) : null,
      isC ? h('button', { type: 'button', class: 'btn ghost sm', text: 'Collapse all', onclick: () => setAll(false) }) : null,
      copyBtn(() => JSON.stringify(value, null, 2), label || 'JSON')), body);
  }
  function payloadBlock(label, p) {
    if (!p || (p.inline === undefined && !p.blob_hash)) return null;
    const wrap = h('div', { class: 'pv' });
    const flags = h('div', { class: 'pv-meta' });
    if (p.redacted) flags.appendChild(tag('redacted', 't-warn', 'sensitive values were masked by the SDK', 'lock'));
    if (p.truncated) flags.appendChild(tag('truncated', 't-warn', 'payload exceeded the inline cap'));
    if (p.size) flags.appendChild(tag(p.size + ' bytes'));
    if (p.blob_hash) flags.appendChild(tag('blob ' + p.blob_hash, 't-mono'));
    if (flags.firstChild) wrap.appendChild(flags);
    if (typeof p.inline === 'string' && U.isRedactedValue(p.inline)) wrap.appendChild(h('div', null, redChip()));
    else if (typeof p.inline === 'string' && p.truncated) wrap.appendChild(h('pre', { class: 'code' }, colorize(p.inline)));
    else if (p.inline !== undefined) wrap.appendChild(jsonViewer(p.inline, label));
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
        if (!(k in b)) kind = 'add'; else if (!(k in a)) kind = 'del';
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
  function valCell(cls, sign, v) {
    const td = h('td', { class: 'dv ' + cls });
    if (cls) td.appendChild(h('span', { class: 'sg', 'aria-hidden': 'true', text: sign }));
    if (v === undefined) { td.appendChild(span('faint', cls ? '(absent)' : '')); return td; }
    if (U.isRedactedValue(v)) td.appendChild(redChip()); else td.appendChild(document.createTextNode(v));
    if (cls) td.setAttribute('aria-label', (cls === 'del' ? 'before: ' : 'after: ') + (v === undefined ? 'absent' : v));
    return td;
  }
  function diffTable(rows) {
    const t = h('table', { class: 'diff' }, h('colgroup', null, h('col'), h('col'), h('col')),
      h('thead', null, h('tr', null, h('th', { text: 'Field' }), h('th', { text: 'Before' }), h('th', { text: 'After' }))));
    const tb = h('tbody');
    rows.forEach((r) => {
      tb.appendChild(h('tr', { class: r.kind === 'same' ? 'same' : 'chg' },
        h('td', { class: 'f', text: r.field }),
        valCell(r.kind === 'del' || r.kind === 'chg' ? 'del' : '', '\u2212', r.old),
        valCell(r.kind === 'add' || r.kind === 'chg' ? 'add' : '', '+', r.nw)));
    });
    t.appendChild(tb);
    return t;
  }
  function diffView(mu) {
    const rows = diffRows(mu);
    const changed = rows.filter((r) => r.kind !== 'same'), same = rows.filter((r) => r.kind === 'same');
    const box = h('div', { class: 'sec' });
    if (changed.length) box.appendChild(diffTable(changed));
    else box.appendChild(h('div', { class: 'note', text: 'No field changes recorded.' }));
    if (same.length) box.appendChild(h('details', { class: 'more' }, h('summary', { text: same.length + ' unchanged field' + (same.length === 1 ? '' : 's') }), diffTable(same)));
    return box;
  }
  const OP_GLYPH = { insert: '+', update: '~', delete: '\u2212', upsert: '\u21c5', other: '\u2022' };
  function opTag(op) {
    const c = U.opClass(op);
    return h('span', { class: 'op op-' + c, title: 'operation: ' + (op || '?') }, span('', OP_GLYPH[c]), op || '?');
  }
  function effLabel(eff, txid) {
    if (eff === false) return h('span', { class: 'state-lbl rb', title: 'the transaction rolled back; this change did not take effect' }, icon('retry'), 'Rolled back');
    if (eff === true) return h('span', { class: 'state-lbl ok' }, icon('check'), 'Committed');
    return h('span', { class: 'state-lbl un', title: txid ? 'no commit or rollback has been seen for this transaction' : 'effective state unknown' }, icon('clock'), txid ? 'Tx unresolved' : 'Effect unknown');
  }
  function mutCard(mu, lead) {
    const rolled = mu.effective === false;
    const card = h('div', { class: 'mut' + (rolled ? ' rolled' : '') });
    card.appendChild(h('div', { class: 'mut-head' }, lead || null, opTag(mu.op), tag(mu.store || '?'), h('strong', { text: mu.entity || '' }),
      span('mono', U.pkLabel(mu.pk)), h('span', { class: 'grow' }), effLabel(mu.effective, mu.tx),
      mu.source ? tag('src: ' + mu.source) : null, mu.attrs && mu.attrs.commit_failed ? tag('commit failed', 't-err') : null));
    const body = h('div', { class: 'mut-body' });
    if (rolled) body.appendChild(h('div', { class: 'note', text: 'Rolled back: the values below were never applied.' }));
    body.appendChild(diffView(mu));
    const b = payloadBlock('before', mu.before), a = payloadBlock('after', mu.after);
    const raw = h('details', { class: 'more' }, h('summary', { text: 'Raw before / after' }));
    if (b || a) raw.appendChild(h('div', { class: 'snap' }, h('div', null, b), h('div', null, a)));
    else raw.appendChild(h('div', { class: 'note', text: 'No snapshots captured.' }));
    body.appendChild(raw);
    card.appendChild(body);
    return card;
  }

  // ---------- state ----------
  const S = { timer: null, token: 0, D: null, online: null, live: false, nav: null, activate: null, search: null, lateral: null, onResize: null, keys: null };
  function stopTimers() { if (S.timer) { clearInterval(S.timer); S.timer = null; } }

  // ---------- routing ----------
  function parseHash() {
    const raw = location.hash.replace(/^#/, ''), qi = raw.indexOf('?');
    const path = qi >= 0 ? raw.slice(0, qi) : raw;
    const dec = (x) => { try { return decodeURIComponent(x); } catch (e) { return x; } };
    return { parts: path.split('/').filter(Boolean).map(dec), query: new URLSearchParams(qi >= 0 ? raw.slice(qi + 1) : '') };
  }
  /* Update one query parameter of the hash without triggering a route change. */
  function setHashQuery(k, v) {
    const raw = location.hash.replace(/^#/, ''), qi = raw.indexOf('?');
    const path = qi >= 0 ? raw.slice(0, qi) : raw, q = new URLSearchParams(qi >= 0 ? raw.slice(qi + 1) : '');
    if (v === null || v === undefined) q.delete(k); else q.set(k, v);
    const qs = q.toString();
    history.replaceState(null, '', location.pathname + location.search + '#' + path + (qs ? '?' + qs : ''));
  }
  function route() {
    const r = parseHash(), parts = r.parts;
    stopTimers();
    S.token++;
    S.nav = S.activate = S.search = S.lateral = S.onResize = S.keys = null;
    closeDrawer(true);
    showError('');
    setLive(false);
    const main = clear(document.getElementById('main'));
    const inSession = (parts[0] === 's' || parts[0] === 'sessions') && parts[1];
    document.getElementById('nav-sessions').setAttribute('aria-current', 'page');
    if (inSession) viewSession(main, parts[1], TAB_IDS.indexOf(parts[2]) >= 0 ? parts[2] : 'timeline', S.token, r.query);
    else viewSessions(main, S.token, r.query);
  }
  window.addEventListener('hashchange', route);
  const sessionHref = (id, tab) => '#/s/' + encodeURIComponent(id) + '/' + (tab || 'timeline');

  function skeletonRows(n) {
    return h('div', { class: 'skel-rows', 'aria-hidden': 'true' }, Array.from({ length: n }, (_, i) =>
      h('div', { class: 'skel-row' }, h('div', { class: 'skel', 'data-w': 'a' }), h('div', { class: 'skel' }), h('div', { class: 'skel' }), h('div', { class: 'skel' }))));
  }
  function emptyState(ill, title, text, extra) {
    return h('div', { class: 'empty' + (ill === 'error' ? ' err' : '') }, illustration(ill), h('h2', { text: title }), text ? h('p', null, text) : null, extra || null);
  }

  // ---------- dialogs ----------
  function makeDialog(title, id) {
    const d = h('dialog', { id: id, 'aria-labelledby': id + '-t' });
    d.appendChild(h('div', { class: 'dlg-head' }, h('h2', { id: id + '-t', text: title }),
      h('button', { type: 'button', class: 'icon-btn', 'aria-label': 'Close', onclick: () => d.close() }, icon('x'))));
    d.addEventListener('click', (e) => { if (e.target === d) d.close(); });
    d.addEventListener('close', () => { if (d.parentNode) d.parentNode.removeChild(d); });
    return d;
  }
  function openStartDialog(prefill) {
    if (document.getElementById('start-dlg')) return;
    const d = makeDialog('Start a test session', 'start-dlg');
    const name = h('input', { class: 'field', type: 'text', placeholder: 'e.g. checkout regression', maxlength: 120, 'aria-label': 'Session name' });
    const modeBtns = ['header', 'exclusive'].map((m) => h('button', { type: 'button', 'aria-pressed': m === 'header' ? 'true' : 'false', text: m, onclick: () => setMode(m) }));
    let mode = 'header';
    const modeHelp = h('span', { class: 'hint' });
    const HELP = { header: 'Only requests carrying the x-tsession-id header are captured.', exclusive: 'All traffic is attributed to this session. Use on isolated test environments.' };
    function setMode(m) {
      mode = m;
      modeBtns.forEach((b) => b.setAttribute('aria-pressed', String(b.textContent === m)));
      modeHelp.textContent = bindEls.length ? 'Bindings attribute traffic by attribute, so header mode is used.' : HELP[m];
    }
    const bindRows = h('div', { class: 'bind-rows' });
    const bindEls = [];
    const warn = h('div', { class: 'callout warn', role: 'note', hidden: true }, icon('warn'),
      h('div', { class: 'ct' }, h('strong', { text: 'jwt_claim bindings can be spoofed' }),
        h('span', { text: 'The JWT signature is not verified, so anyone can forge the claim. Use only in test environments.' })));
    const err = h('div', { class: 'form-err', role: 'alert' });
    function refresh() {
      warn.hidden = !bindEls.some((r) => r.src.value === 'jwt_claim');
      modeBtns.forEach((b) => { b.disabled = bindEls.length > 0; });
      if (bindEls.length) mode = 'header';
      setMode(mode);
    }
    function addBind(pre) {
      if (bindEls.length >= 8) return;
      const src = h('select', { class: 'field', 'aria-label': 'Binding source', onchange: refresh }, ['header', 'jwt_claim', 'cookie', 'query'].map((x) => h('option', { value: x, text: x })));
      const bn = h('input', { class: 'field', type: 'text', placeholder: 'name (x-user-id, sub)', 'aria-label': 'Binding name', maxlength: 64 });
      const bv = h('input', { class: 'field', type: 'text', placeholder: 'value', 'aria-label': 'Binding value', maxlength: 256 });
      if (pre) { src.value = pre.source; bn.value = pre.name; bv.value = pre.value; }
      const row = { src: src, bn: bn, bv: bv };
      row.el = h('div', { class: 'bind-row' }, src, bn, bv, h('button', { type: 'button', class: 'icon-btn', 'aria-label': 'Remove binding', onclick: () => {
        bindEls.splice(bindEls.indexOf(row), 1); bindRows.removeChild(row.el); refresh();
      } }, icon('x')));
      bindEls.push(row); bindRows.appendChild(row.el); refresh();
    }
    const startBtn = h('button', { type: 'submit', class: 'btn primary', text: 'Start session' });
    const form = h('form', { onsubmit: async (e) => {
      e.preventDefault();
      startBtn.disabled = true; err.textContent = '';
      try {
        const body = { name: name.value.trim(), mode: mode, initiated_by: 'dashboard' };
        const bindings = bindEls.map((r) => ({ source: r.src.value, name: r.bn.value.trim(), value: r.bv.value.trim() }));
        if (bindings.some((b) => !b.name || !b.value)) throw new Error('every binding needs a name and a value');
        if (bindings.length) body.bindings = bindings;
        const s = await postJSON('/v1/sessions', body);
        d.close();
        toast('Session started', 'ok');
        if (s && s.id) location.hash = sessionHref(s.id);
        else route();
      } catch (ex) { err.textContent = 'Could not start the session: ' + ex.message; } finally { startBtn.disabled = false; }
    } },
      h('div', { class: 'dlg-body' },
        h('label', { class: 'lbl' }, 'Name', name),
        h('div', { class: 'lbl' }, 'Capture mode', h('div', { class: 'seg', role: 'group', 'aria-label': 'Capture mode' }, modeBtns), modeHelp),
        h('div', { class: 'lbl' }, h('span', null, 'Attribute bindings ', h('span', { class: 'hint', text: '(optional, up to 8)' })),
          h('span', { class: 'hint', text: 'Capture one tester\'s traffic without sending x-tsession-id, by matching a header, JWT claim, cookie or query value.' }),
          bindRows, h('div', null, h('button', { type: 'button', class: 'btn sm', onclick: () => addBind() }, icon('plus', 'sm'), 'Add binding'))),
        warn, err),
      h('div', { class: 'dlg-foot' }, h('button', { type: 'button', class: 'btn ghost', text: 'Cancel', onclick: () => d.close() }), startBtn));
    d.appendChild(form);
    document.body.appendChild(d);
    setMode('header');
    if (prefill && prefill.bindings) prefill.bindings.forEach(addBind);
    d.showModal();
    name.focus();
  }
  function openShortcuts() {
    if (document.getElementById('sc-dlg')) return;
    const d = makeDialog('Keyboard shortcuts', 'sc-dlg');
    const rows = [[['j', 'k'], 'Move down / up in the list'], [['Enter'], 'Open the selected item (again to focus the panel)'], [['Esc'], 'Close the panel or dialog'],
      [['[', ']'], 'Previous / next trigger group'], [['e', 'c'], 'Expand / collapse all groups (Enter toggles one)'], [['/'], 'Focus search'], [['1', '2', '3', '4'], 'Timeline, Call tree, Entities, Summary'], [['\u2190', '\u2192'], 'Switch tabs, or collapse / expand a tree node'], [['?'], 'Show this help']];
    const dl = h('dl', { class: 'sc-list' });
    rows.forEach((r) => { dl.appendChild(h('dt', null, r[0].map((k) => span('kbd', k)))); dl.appendChild(h('dd', { text: r[1] })); });
    d.appendChild(dl); document.body.appendChild(d); d.showModal();
  }

  // ---------- session naming ----------
  const SRC_HELP = { header: 'matched by request header', jwt_claim: 'JWT claim, UNVERIFIED (can be spoofed)', cookie: 'matched by cookie', query: 'matched by query parameter' };
  function bindingChip(b) {
    const jwt = b.source === 'jwt_claim';
    return h('span', { class: 'bchip' + (jwt ? ' jwt' : ''), title: b.source + ': ' + SRC_HELP[b.source] }, icon(jwt ? 'lock' : 'link', 'sm'), span('bk', b.name + ':'), span('bv', b.value), jwt ? span('bu', 'unverified') : null);
  }
  const autoTag = () => tag('auto', 't-auto', 'name generated by the server from the session bindings or initiator; rename to replace it');
  /* Inline rename input. o.apply(name, auto) updates data and repaints; o.cancel() just repaints. PATCH goes through /api/cp. */
  function renameField(s, o) {
    const old = s.name || '', oldAuto = !!s.name_auto;
    let done = false;
    const finish = () => { if (!done) { done = true; o.cancel(); } };
    const inp = h('input', { class: 'field rename-in', type: 'text', value: old, maxlength: 120, 'aria-label': 'Session name', onclick: (e) => e.stopPropagation(),
      onkeydown: async (e) => {
        e.stopPropagation();
        if (e.key === 'Escape') { e.preventDefault(); finish(); return; }
        if (e.key !== 'Enter') return;
        e.preventDefault();
        const nm = inp.value.trim();
        if (!nm) { toast('Name cannot be empty', 'err'); return; }
        if (nm === old) { finish(); return; }
        done = true;
        o.apply(nm, false);
        try {
          const r = await cp('/v1/sessions/' + encodeURIComponent(s.id), { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: nm }) });
          if (r && r.name && r.name !== nm) o.apply(r.name, !!r.name_auto);
          toast('Renamed', 'ok');
        } catch (ex) { o.apply(old, oldAuto); toast('Rename failed: ' + ex.message, 'err'); }
      }, onblur: finish });
    setTimeout(() => { inp.focus(); inp.select(); }, 0);
    return inp;
  }
  const renameBtn = (onclick) => h('button', { type: 'button', class: 'icon-btn sm', title: 'Rename session', 'aria-label': 'Rename session', onclick: (e) => { e.stopPropagation(); onclick(); } }, icon('edit'));
  /* Entry point of a session (first trigger), cached per session and event count. */
  const ENTRY = new Map();

  // ---------- sessions list ----------
  const STATES = ['ACTIVE', 'DRAINING', 'SEALED', 'ABORTED', 'EXPIRED', 'CREATED'];
  function viewSessions(main, token, query) {
    document.title = 'Sessions - tsession';
    const f = { state: STATES.indexOf(query.get('state')) >= 0 ? query.get('state') : '', q: (query.get('q') || '').toLowerCase(), auto: true };
    let rows = null, loadErr = null, lastSig = '', editing = null, entryVer = 0, entryBusy = false;
    const pendingNames = new Map();
    const q = h('input', { class: 'field', type: 'search', placeholder: 'Search id, name, binding, entry point', 'aria-label': 'Search sessions', value: query.get('q') || '', oninput: (e) => { f.q = e.target.value.toLowerCase(); paint(true); } });
    const chipsEl = h('div', { class: 'chips', role: 'group', 'aria-label': 'Filter by state' });
    const auto = h('input', { type: 'checkbox', checked: true, onchange: (e) => { f.auto = e.target.checked; } });
    const body = h('div', { class: 'card' });
    const sub = h('div', { class: 'sub' });
    S.search = q;
    main.appendChild(h('div', { class: 'page-head' }, h('div', null, h('h1', { text: 'Test sessions' }), sub), h('span', { class: 'grow' }),
      h('button', { type: 'button', class: 'btn primary', onclick: () => openStartDialog() }, icon('plus', 'sm'), 'Start session')));
    main.appendChild(h('div', { class: 'toolbar' },
      h('div', { class: 'search' }, icon('search', 'sm'), q, span('kbd', '/')), chipsEl, h('span', { class: 'grow' }),
      h('label', { class: 'switch' }, auto, h('span', { class: 'track', 'aria-hidden': 'true' }), 'Auto-refresh'),
      h('button', { type: 'button', class: 'btn', onclick: () => load() }, icon('retry', 'sm'), 'Refresh')));
    main.appendChild(body);
    clear(body).appendChild(skeletonRows(4));

    function paintChips() {
      clear(chipsEl);
      const counts = {};
      (rows || []).forEach((s) => { counts[s.state] = (counts[s.state] || 0) + 1; });
      const mk = (val, label, n) => h('button', { type: 'button', class: 'chip', 'aria-pressed': String(f.state === val), onclick: () => { f.state = val; paintChips(); paint(true); } }, label, h('span', { class: 'n', text: n }));
      chipsEl.appendChild(mk('', 'All', rows ? rows.length : 0));
      STATES.forEach((st) => { if (counts[st] || f.state === st) chipsEl.appendChild(mk(st, st.charAt(0) + st.slice(1).toLowerCase(), counts[st] || 0)); });
    }
    function nameBlock(s) {
      const en = ENTRY.get(s.id), et = en && en.entry ? U.entryText(en.entry) : '';
      const nameLine = h('div', { class: 's-name' });
      if (editing === s.id) {
        nameLine.appendChild(renameField(s, { apply: (n, a) => { editing = null; s.name = n; s.name_auto = a; pendingNames.set(s.id, { n: n, a: a }); paint(true); }, cancel: () => { editing = null; paint(true); } }));
      } else {
        nameLine.appendChild(h('a', { href: sessionHref(s.id), text: s.name || '(unnamed)', class: 's-link', onclick: (e) => e.stopPropagation() }));
        if (s.name_auto) nameLine.appendChild(autoTag());
        nameLine.appendChild(renameBtn(() => { editing = s.id; paint(true); }));
      }
      return h('div', null, nameLine,
        s.bindings && s.bindings.length ? h('div', { class: 'bchips' }, s.bindings.map(bindingChip)) : null,
        et ? h('div', { class: 's-entry', title: et, text: et }) : null,
        h('div', { class: 's-id' }, span('mono', s.id), copyBtn(s.id, 'session id')));
    }
    function sessionRow(s, now) {
      const open = () => { location.hash = sessionHref(s.id); };
      const start = Date.parse(s.started_at || s.created_at), end = s.ended_at ? Date.parse(s.ended_at) : now;
      const dur = isNaN(start) ? '-' : fmtNs(Math.max(0, end - start) * 1e6);
      const stop = s.state === 'ACTIVE' ? h('button', { type: 'button', class: 'btn danger sm', onclick: async (e) => {
        e.stopPropagation();
        try { await postJSON('/v1/sessions/' + encodeURIComponent(s.id) + '/stop'); toast('Stopping ' + s.id, 'ok'); load(); } catch (er) { showError('Stop failed: ' + er.message); }
      } }, icon('stop', 'sm'), 'Stop') : null;
      const bound = null;
      return h('tr', { class: 'row', tabindex: 0, 'data-id': s.id, onclick: open, onkeydown: (e) => { if (e.key === 'Enter' && e.target === e.currentTarget) open(); } },
        h('td', null, statePill(s.state)),
        h('td', null, nameBlock(s)),
        h('td', { class: 'hide-sm' }, h('div', null, s.mode), bound ? h('div', null, bound) : null),
        h('td', { class: 'hide-sm', text: s.initiated_by || '-' }),
        h('td', { title: fmtTime(s.started_at || s.created_at) }, h('div', { text: fmtAgoIso(s.started_at || s.created_at) })),
        h('td', null, h('div', { class: 'stats' }, h('span', { title: 'events captured' }, h('b', { text: s.event_count === undefined ? '-' : s.event_count }), 'events'),
          s.service_count !== undefined ? h('span', { title: 'services seen' }, h('b', { text: s.service_count }), 'services') : null,
          h('span', { title: 'wall-clock duration' }, icon('clock', 'sm'), h('b', { text: dur })))),
        h('td', null, h('div', { class: 'cell-flags' }, s.incomplete ? tag('incomplete', 't-warn', 'some async work or events are missing', 'warn') : null,
          s.dropped_events ? tag(s.dropped_events + ' dropped', 't-err') : null, s.seal_error ? tag('seal error', 't-err', s.seal_error) : null)),
        h('td', { class: 'num' }, stop));
    }
    function paint(force) {
      const liveBucket = (rows || []).some(isLive) ? Math.floor(Date.now() / 5000) : 0;
      if (editing && !force) return;
      const sig = JSON.stringify([rows, loadErr, f.state, f.q, liveBucket, entryVer, editing]);
      if (!force && sig === lastSig) return;
      lastSig = sig;
      const focusId = document.activeElement && document.activeElement.getAttribute && document.activeElement.closest && document.activeElement.closest('tr.row') ? document.activeElement.closest('tr.row').getAttribute('data-id') : null;
      clear(body);
      if (loadErr) {
        body.appendChild(emptyState('error', 'Cannot load sessions', 'The control plane or collector did not answer: ' + loadErr + '. Check that both are running and that the dashboard can reach them.',
          h('button', { type: 'button', class: 'btn', onclick: () => load() }, icon('retry', 'sm'), 'Try again')));
        sub.textContent = '';
        return;
      }
      if (!rows) return;
      paintChips();
      const vis = rows.filter((s) => (!f.state || s.state === f.state) && (!f.q || U.sessionHay(s, (ENTRY.get(s.id) || {}).entry).indexOf(f.q) >= 0));
      sub.textContent = vis.length === rows.length ? rows.length + ' session' + (rows.length === 1 ? '' : 's') : vis.length + ' of ' + rows.length + ' sessions';
      if (!rows.length) {
        body.appendChild(emptyState('sessions', 'No sessions yet', 'A session captures every event produced by your test traffic. Start one, then send requests with the x-tsession-id header.',
          h('button', { type: 'button', class: 'btn primary', onclick: () => openStartDialog() }, icon('plus', 'sm'), 'Start your first session')));
        return;
      }
      if (!vis.length) {
        body.appendChild(emptyState('sessions', 'No sessions match', 'Try a different search term or clear the state filter.',
          h('button', { type: 'button', class: 'btn', onclick: () => { f.state = ''; f.q = ''; q.value = ''; paint(true); } }, 'Clear filters')));
        return;
      }
      const now = Date.now();
      body.appendChild(h('div', { class: 'tablewrap' }, h('table', { class: 'stable' },
        h('thead', null, h('tr', null, [['State', ''], ['Session', ''], ['Mode', 'hide-sm'], ['Initiated by', 'hide-sm'], ['Started', ''], ['Activity', ''], ['Flags', ''], ['', '']].map((x) => h('th', { class: x[1], text: x[0] })))),
        h('tbody', null, vis.map((s) => sessionRow(s, now))))));
      if (focusId) { const el = body.querySelector('tr.row[data-id="' + focusId.replace(/["\\]/g, '') + '"]'); if (el) el.focus(); }
    }
    async function load() {
      try {
        const [a, b] = await Promise.all([cp('/v1/sessions'), col('/v1/sessions').catch(() => [])]);
        if (token !== S.token) return;
        const counts = {}, svcs = {};
        asList(b, 'sessions').forEach((x) => { counts[x.id] = x.event_count; if (x.service_count !== undefined) svcs[x.id] = x.service_count; });
        rows = asList(a, 'sessions').map((s) => { const pn = pendingNames.get(s.id); return Object.assign({}, s, { event_count: counts[s.id], service_count: svcs[s.id] }, pn && pn.n !== s.name ? { name: pn.n, name_auto: pn.a } : null); });
        pendingNames.forEach((v, k) => { const r = rows.find((x) => x.id === k); if (!r || r.name === v.n) pendingNames.delete(k); });
        rows.sort((x, y) => String(y.created_at).localeCompare(String(x.created_at)));
        loadErr = null; showError('');
        setLive(rows.some((s) => s.state === 'ACTIVE'));
      } catch (e) { if (token !== S.token) return; loadErr = e.message; }
      paint();
      loadEntries();
    }
    /* Entry points are derived from each session's first events; only the first ~10 visible sessions, one at a time, never blocking the list. */
    async function loadEntries() {
      if (entryBusy || !rows) return;
      entryBusy = true;
      try {
        const vis = rows.filter((s) => (!f.state || s.state === f.state)).slice(0, 10);
        for (const s of vis) {
          if (token !== S.token) return;
          const c = ENTRY.get(s.id);
          if (c && c.n === s.event_count) continue;
          if (!s.event_count) { ENTRY.set(s.id, { n: s.event_count, entry: null }); continue; }
          try {
            const r = await col('/v1/sessions/' + encodeURIComponent(s.id) + '/events?from_seq=0&limit=400');
            ENTRY.set(s.id, { n: s.event_count, entry: U.entryOf((r && r.events) || [], ((r && r.events) || []).length >= 400 && s.event_count > 400) });
          } catch (e) { ENTRY.set(s.id, { n: s.event_count, entry: null }); }
          entryVer++;
          if (token === S.token) paint();
        }
      } finally { entryBusy = false; }
    }
    S.nav = (dir) => {
      const trs = Array.from(body.querySelectorAll('tr.row'));
      if (!trs.length) return;
      const i = trs.indexOf(document.activeElement.closest ? document.activeElement.closest('tr.row') : null);
      trs[Math.max(0, Math.min(trs.length - 1, i < 0 ? (dir > 0 ? 0 : trs.length - 1) : i + dir))].focus();
    };
    S.activate = () => { const tr = document.activeElement && document.activeElement.closest ? document.activeElement.closest('tr.row') : null; if (tr) tr.click(); };
    paintChips();
    load();
    S.timer = setInterval(() => { if (f.auto && !document.hidden) load(); }, 3000);
    if (query.get('new')) {
      const pre = query.get('new') === 'jwt' ? { bindings: [{ source: 'jwt_claim', name: 'sub', value: 'user_synth_alice' }] } : null;
      setTimeout(() => { if (token === S.token) openStartDialog(pre); }, 0);
    }
  }

  // ---------- session detail ----------
  const TABS = [['timeline', 'Timeline', 'timeline'], ['tree', 'Call tree', 'tree'], ['entities', 'Entities', 'layers'], ['summary', 'Summary', 'chart']];
  const TAB_IDS = TABS.map((t) => t[0]);

  function viewSession(main, id, tab, token, query) {
    document.title = id + ' - tsession';
    if (!S.D || S.D.id !== id) {
      S.D = { id: id, sess: null, events: [], bySeq: new Map(), bySpan: new Map(), maxSeq: 0, t0: 0, hay: new Map(),
        f: { service: '', kind: '', err: false, entity: '', q: '' }, collapsed: new Set(), follow: true, dtab: 'overview', evVer: 0 };
    }
    const D = S.D;
    const qg = query && query.get('group');
    if (qg === 'trigger' || qg === 'none') D.groupMode = qg;
    else if (!D.groupMode) D.groupMode = lsGet('tsession.group') === 'none' ? 'none' : 'trigger';
    if (query && query.get('collapse') === 'all') { D.gDefaultOpen = false; if (D.gExplicit) D.gExplicit.clear(); }
    if (D.newestFirst === undefined) D.newestFirst = lsGet('tsession.gorder') === 'newest';
    D.token = token; D.tab = tab; D.dirty = true; D.headSig = '';
    const head = h('div', { class: 'card shead' });
    const crumbs = h('nav', { class: 'crumbs', 'aria-label': 'Breadcrumb' });
    const tabsEl = h('nav', { class: 'tabs', 'aria-label': 'Session views', onkeydown: (e) => {
      const links = Array.from(tabsEl.querySelectorAll('a')), i = links.indexOf(document.activeElement);
      if (i < 0) return;
      const j = e.key === 'ArrowRight' ? (i + 1) % links.length : e.key === 'ArrowLeft' ? (i - 1 + links.length) % links.length : e.key === 'Home' ? 0 : e.key === 'End' ? links.length - 1 : -1;
      if (j >= 0) { e.preventDefault(); links[j].click(); }
    } }, TABS.map((t) => h('a', { href: sessionHref(id, t[0]), 'aria-current': t[0] === tab ? 'page' : null }, icon(t[2]), t[1])));
    const content = h('div', { id: 'tabbody', hidden: true });
    const loading = h('div', { class: 'card' }, skeletonRows(5));
    const body = h('div', null, loading, content);
    main.appendChild(crumbs); main.appendChild(head); main.appendChild(tabsEl); main.appendChild(body);
    D.head = head; D.crumbs = crumbs; D.body = content; D.tabsEl = tabsEl;
    paintCrumbs(D);
    if (D.sess) paintHead(D);
    else head.appendChild(skeletonRows(1));
    S.lateral = (dir) => { const i = TAB_IDS.indexOf(tab); location.hash = sessionHref(id, TAB_IDS[(i + dir + TAB_IDS.length) % TAB_IDS.length]); };
    D.view = ({ timeline: mkTimeline, tree: mkTree, entities: mkEntities, summary: mkSummary }[tab] || mkTimeline)(D, content);
    D.view.n = null;

    function reveal() {
      loading.hidden = true; content.hidden = false;
      const ev = query && query.get('event') ? D.bySeq.get(Number(query.get('event'))) : null;
      if (query && query.get('dtab')) D.dtab = query.get('dtab');
      if (query && query.get('ent')) D.entPick = Number(query.get('ent'));
      if (ev && D.drawerSeq !== ev.seq) { D.view.select && D.view.select(ev); openDrawer(D, ev, { focus: false }); }
    }
    let busy = false, ticks = 0;
    async function first() {
      try {
        if (D.events.length) { content.hidden = false; loading.hidden = true; }
        D.sess = await cp('/v1/sessions/' + encodeURIComponent(id));
        setLive(isLive(D.sess));
        paintHead(D);
        await fetchEvents(D, true);
        if (token !== S.token) return;
        reveal();
        D.view.update(true);
        paintHead(D);
        if (query && query.get('event')) reveal();
      } catch (e) {
        if (token !== S.token) return;
        clear(loading).appendChild(emptyState('error', e.status === 404 ? 'Session not found' : 'Cannot load this session', (e.status === 404 ? id + ' does not exist on the control plane. It may have been purged.' : e.message),
          h('a', { class: 'btn', href: '#/' }, 'Back to sessions')));
        loading.hidden = false; content.hidden = true;
        return;
      }
      /* SEALED sessions are final: one full fetch. Anything else may still resolve mutations. */
      if (D.sess.state !== 'SEALED') S.timer = setInterval(tick, 1500);
    }
    async function tick() {
      if (busy || document.hidden) return;
      busy = true;
      try {
        const s = await cp('/v1/sessions/' + encodeURIComponent(id));
        if (token !== S.token) return;
        const was = D.sess;
        let n = await fetchEvents(D, false);
        const ended = was && isLive(was) && !isLive(s);
        const unresolved = U.hasUnresolved(D.events);
        if (ended || TERMINAL[s.state] || (unresolved && ++ticks % 3 === 0)) n += await fetchEvents(D, true);
        if (token !== S.token) return;
        D.sess = s;
        setLive(isLive(s));
        paintHead(D);
        D.view.update(n > 0);
        if (TERMINAL[s.state] && (s.state === 'SEALED' || !U.hasUnresolved(D.events))) stopTimers();
      } catch (e) { if (token === S.token) showError('Live update failed: ' + e.message); } finally { busy = false; }
    }
    first();
  }
  function paintCrumbs(D) {
    const names = { timeline: 'Timeline', tree: 'Call tree', entities: 'Entities', summary: 'Summary' };
    clear(D.crumbs).appendChild(h('a', { href: '#/', text: 'Sessions' }));
    D.crumbs.appendChild(span('sep', '/'));
    D.crumbs.appendChild(h('a', { href: sessionHref(D.id, 'timeline'), class: 'cur', text: D.sess && D.sess.name ? D.sess.name : D.id }));
    D.crumbs.appendChild(span('sep', '/'));
    D.crumbs.appendChild(h('span', { class: 'cur', 'aria-current': 'page', text: names[D.tab] || D.tab }));
  }

  /* Fetch events. The collector resolves Mutation.Effective at read time, so a cached copy of
     a db.write fetched before its commit/rollback was recorded stays unresolved; `full` re-reads
     from seq 0 and replaces cached copies (newest wins). Returns the number of added+changed events. */
  async function fetchEvents(D, full) {
    let n = 0, guard = 0, from = full ? 0 : D.maxSeq;
    for (;;) {
      const r = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/events?from_seq=' + from + '&limit=1000');
      const evs = (r && r.events) || [];
      const m = U.mergeEvents(D, evs);
      evs.forEach((e) => D.hay.delete(e.seq));
      n += m.added + m.changed;
      if (m.added + m.changed) D.evVer++;
      let next = from;
      evs.forEach((e) => { if (e.seq > next) next = e.seq; });
      if (r && r.next_seq > next) next = r.next_seq;
      const progressed = next > from;
      from = next;
      if (evs.length < 1000 || !progressed || ++guard > 200) break;
    }
    return n;
  }
  const TERMINAL = { SEALED: 1, ABORTED: 1, EXPIRED: 1 };

  function download(name, text, type) {
    const a = h('a', { href: URL.createObjectURL(new Blob([text], { type: type })), download: name });
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    toast('Downloaded ' + name, 'ok');
  }
  async function exportEvents(D) {
    await fetchEvents(D, true); /* same-origin re-read so the export carries current effective values */
    return D.events;
  }
  function fact(k, v, small) { return h('div', { class: 'fact' }, h('div', { class: 'k', text: k }), h('div', { class: 'v' }, v, small ? h('small', { text: ' ' + small }) : null)); }
  function paintHead(D) {
    const s = D.sess; if (!s) return;
    if (D.renaming) return;
    const errs = D.events.filter((e) => e.err).length;
    const en = D.events.length ? U.entryOf(D.events) : null;
    const sig = JSON.stringify([s, D.events.length, errs, isLive(s) ? Math.floor(Date.now() / 1000) : 0, en]);
    if (sig === D.headSig) return;
    D.headSig = sig;
    paintCrumbs(D);
    const head = clear(D.head);
    const start = Date.parse(s.started_at || s.created_at), end = s.ended_at ? Date.parse(s.ended_at) : Date.now();
    const services = new Set(D.events.map((e) => e.service)).size;
    const nameEl = D.renaming ? null : h('h1', { text: s.name || '(unnamed session)' });
    head.appendChild(h('div', { class: 'shead-top' }, nameEl, s.name_auto ? autoTag() : null,
      renameBtn(() => {
        D.renaming = true; D.headSig = '';
        const top = D.head.querySelector('.shead-top'), h1 = top.querySelector('h1');
        const end = () => { D.renaming = false; D.headSig = ''; paintHead(D); };
        top.replaceChild(renameField(s, { apply: (n, a) => { D.renaming = false; D.sess = Object.assign({}, D.sess, { name: n, name_auto: a }); D.headSig = ''; paintHead(D); document.title = D.id + ' - tsession'; }, cancel: end }), h1);
      }), statePill(s.state),
      isLive(s) ? h('span', { class: 'live-tag' }, h('span', { class: 'dot', 'aria-hidden': 'true' }), 'live tail') : null,
      s.incomplete ? tag('incomplete', 't-warn', 'some async work or events are missing', 'warn') : null,
      s.reason ? tag('reason: ' + s.reason, 't-warn', 'why the session ended') : null,
      h('span', { class: 'grow' }),
      h('div', { class: 'shead-actions' },
        h('button', { type: 'button', class: 'btn', onclick: async () => {
          try { const evs = await exportEvents(D); copyText(JSON.stringify({ session: s, events: evs }, null, 2), 'session JSON'); } catch (e) { showError('Export failed: ' + e.message); }
        } }, icon('copy', 'sm'), 'Copy JSON'),
        h('button', { type: 'button', class: 'btn', onclick: async () => {
          try { const evs = await exportEvents(D); download(s.id + '.events.ndjson', U.ndjson(evs), 'application/x-ndjson'); } catch (e) { showError('Export failed: ' + e.message); }
        } }, icon('download', 'sm'), 'Events (ndjson)'),
        s.state === 'ACTIVE' ? h('button', { type: 'button', class: 'btn danger', onclick: async () => {
          try { await postJSON('/v1/sessions/' + encodeURIComponent(s.id) + '/stop'); toast('Stopping session', 'ok'); } catch (e) { showError('Stop failed: ' + e.message); }
        } }, icon('stop', 'sm'), 'Stop session') : null)));
    if (s.bindings && s.bindings.length) head.appendChild(h('div', { class: 'bchips shead-b' }, s.bindings.map(bindingChip)));
    if (en) head.appendChild(h('div', { class: 's-entry', text: U.entryText(en), title: U.entryText(en) }));
    head.appendChild(h('div', { class: 'shead-id' }, span('mono', s.id), copyBtn(s.id, 'session id')));
    head.appendChild(h('div', { class: 'facts' },
      fact('Mode', s.mode || '-'), fact('Initiated by', s.initiated_by || '-'),
      fact('Started', fmtAgoIso(s.started_at || s.created_at), fmtTime(s.started_at || s.created_at)),
      fact('Duration', isNaN(start) ? '-' : fmtNs(Math.max(0, end - start) * 1e6)),
      fact('Events', String(D.events.length)), fact('Services', String(services)),
      fact('Errors', String(errs)), s.bundle_uri ? (() => { const f = fact('Bundle', s.bundle_uri); f.classList.add('wide'); return f; })() : null));
    if (s.bindings && s.bindings.length) {
      if (s.bindings.some((b) => b.source === 'jwt_claim')) {
        head.appendChild(h('div', { class: 'callout warn', role: 'note' }, icon('warn'), h('div', { class: 'ct' }, h('strong', { text: 'jwt_claim binding' }),
          h('span', { text: 'The JWT signature is not verified, so traffic can be spoofed. Test environments only.' }))));
      }
    }
    if (s.seal_error) head.appendChild(h('div', { class: 'callout err', role: 'alert' }, icon('err'), h('div', { class: 'ct' }, h('strong', { text: 'Seal failed' }), h('span', { text: s.seal_error }))));
    if (s.incomplete || s.dropped_events) {
      const w = U.sessionWarnings(Object.assign({}, s, { seal_error: '' }));
      head.appendChild(h('div', { class: 'callout warn', role: 'alert' }, icon('warn'), h('div', { class: 'ct' },
        h('strong', { text: s.incomplete ? 'Incomplete session' : 'Dropped events' }), w.map((x) => h('span', { text: x })))));
    }
  }

  // ---------- event drawer ----------
  let lastFocus = null;
  const drawerEl = () => document.getElementById('drawer');
  const drawerOpen = () => drawerEl().classList.contains('open');
  function closeDrawer(silent) {
    const d = drawerEl();
    if (!d.classList.contains('open')) return;
    d.classList.remove('open'); d.setAttribute('aria-hidden', 'true'); d.inert = true;
    if (S.D) { S.D.drawerSeq = null; if (S.D.view && S.D.view.refreshSel) S.D.view.refreshSel(); }
    if (!silent) {
      setHashQuery('event', null);
      if (lastFocus && document.contains(lastFocus)) lastFocus.focus();
    }
  }
  function focusDrawer() { const b = drawerEl().querySelector('[role=tab][aria-selected=true]') || drawerEl().querySelector('button'); if (b) b.focus(); }

  function spanRef(D, id) {
    const e = D.bySpan.get(id);
    if (!e) return span('mono', id + ' (not in session)');
    return h('button', { type: 'button', class: 'linkbtn', text: '#' + e.seq + ' ' + e.kind, title: id, onclick: () => { if (D.view.select) D.view.select(e); openDrawer(D, e, { focus: false }); } });
  }
  function drawerTabs(e) {
    const t = [['overview', 'Overview', null]];
    if (e.req && (e.req.inline !== undefined || e.req.blob_hash)) t.push(['request', 'Request', null]);
    if (e.resp && (e.resp.inline !== undefined || e.resp.blob_hash)) t.push(['response', 'Response', null]);
    const nm = (e.mutations || []).length, nr = (e.reads || []).length;
    if (nm || nr) t.push(['mutations', 'Mutations', nm || nr]);
    t.push(['raw', 'Raw', null]);
    return t;
  }
  function openDrawer(D, e, opts) {
    opts = opts || {};
    const d = drawerEl();
    if (!drawerOpen()) lastFocus = document.activeElement;
    D.drawerSeq = e.seq; D.drawerEv = e;
    if (drawerTabs(e).map((t) => t[0]).indexOf(D.dtab) < 0) D.dtab = 'overview';
    paintDrawer(D);
    d.inert = false; d.setAttribute('aria-hidden', 'false'); d.classList.add('open');
    setHashQuery('event', String(e.seq));
    if (D.view && D.view.refreshSel) D.view.refreshSel();
    if (opts.focus) focusDrawer();
  }
  function paintDrawer(D) {
    const d = clear(drawerEl()), e = D.drawerEv;
    if (!e) return;
    const tabs = drawerTabs(e);
    const dtabs = h('div', { class: 'dr-tabs', role: 'tablist', 'aria-label': 'Event details', onkeydown: (ev) => {
      const bs = Array.from(dtabs.querySelectorAll('[role=tab]')), i = bs.indexOf(document.activeElement);
      const j = ev.key === 'ArrowRight' ? (i + 1) % bs.length : ev.key === 'ArrowLeft' ? (i - 1 + bs.length) % bs.length : -1;
      if (i >= 0 && j >= 0) { ev.preventDefault(); bs[j].click(); bs[j].focus(); }
    } }, tabs.map((t) => h('button', { type: 'button', role: 'tab', id: 'dt-' + t[0], 'aria-selected': String(t[0] === D.dtab), 'aria-controls': 'dp', tabindex: t[0] === D.dtab ? 0 : -1,
      onclick: () => { D.dtab = t[0]; paintDrawer(D); const b = drawerEl().querySelector('[role=tab][aria-selected=true]'); if (b) b.focus(); } }, t[1], t[2] ? span('n', t[2]) : null)));
    d.appendChild(h('div', { class: 'dr-head' }, kchip(e.kind, !!e.err), h('h2', { id: 'drawer-title', text: e.name || '(no name)' }),
      h('button', { type: 'button', class: 'icon-btn', 'aria-label': 'Previous event (k)', title: 'Previous event (k)', onclick: () => S.nav && S.nav(-1) }, icon('chev-u')),
      h('button', { type: 'button', class: 'icon-btn', 'aria-label': 'Next event (j)', title: 'Next event (j)', onclick: () => S.nav && S.nav(1) }, icon('chev-d')),
      h('button', { type: 'button', class: 'icon-btn', 'aria-label': 'Close panel (Esc)', title: 'Close (Esc)', onclick: () => closeDrawer() }, icon('x'))));
    d.appendChild(h('div', { class: 'dr-sub' }, span('mono', '#' + e.seq), span('svc', e.service || '?'), span('mono', '+' + fmtNs(rel(e))), span('mono', fmtNs(evDur(e))),
      e.err ? tag('error', 't-err', e.err, 'err') : tag('ok', 't-ok')));
    d.appendChild(dtabs);
    const pane = h('div', { class: 'dr-body', role: 'tabpanel', id: 'dp', 'aria-labelledby': 'dt-' + D.dtab, tabindex: 0 });
    ({ overview: drOverview, request: drReq, response: drResp, mutations: drMut, raw: drRaw }[D.dtab] || drOverview)(D, e, pane);
    d.appendChild(pane);
  }
  function idRow(v) { return v ? [span('mono', v), copyBtn(v, 'id')] : null; }
  function drOverview(D, e, pane) {
    if (e.err) pane.appendChild(h('div', { class: 'callout err', role: 'note' }, icon('err'), h('div', { class: 'ct' }, h('strong', { text: 'Error' }), h('span', { class: 'mono', text: e.err }))));
    const kv = h('dl', { class: 'kv' });
    const row = (k, v) => { if (v === undefined || v === null || v === '' || (Array.isArray(v) && !v.length)) return; kv.appendChild(h('dt', { text: k })); kv.appendChild(h('dd', null, v)); };
    row('Completed at', span('mono', '#' + e.seq + ' (completion order)'));
    const gg = getGroups(D), gk = gg.keyOf.get(e.seq), grp = gk === undefined ? null : gg.byKey.get(gk);
    row('Trigger', grp && grp.trigger && grp.trigger !== e ? spanRef(D, grp.trigger.span_id) : null);
    row('Service', e.service + (e.instance ? ' (' + e.instance + ')' : ''));
    const iso = U.isoFromNs(e.start_ns);
    row('Start', [span('mono', '+' + fmtNs(U.relNs(e.start_ns, D.t0))), span('muted', iso || 'invalid timestamp')]);
    if (U.unsafeNs(e.start_ns) || U.unsafeNs(e.end_ns)) row('Precision', 'ns timestamps exceed 2^53 and are approximate; relative times are exact to ~1 µs');
    row('Duration', span('mono', fmtNs(evDur(e))));
    row('Attempt', e.attempt ? String(e.attempt) + (e.attrs && e.attrs.retry_pending ? ' (retry pending)' : '') : null);
    row('Tx id', e.tx_id ? idRow(e.tx_id) : null);
    row('Span id', idRow(e.span_id));
    row('Trace id', idRow(e.trace_id));
    row('Parent', e.parent_id ? spanRef(D, e.parent_id) : null);
    row('Linked from', (e.link_ids || []).map((l) => spanRef(D, l)));
    row('HLC', span('mono', String(e.hlc) + ' → ' + String(e.end_hlc) + ', local_seq ' + String(e.local_seq)));
    if (e.attrs) Object.keys(e.attrs).sort().forEach((k) => row('attr ' + k, span('mono', typeof e.attrs[k] === 'object' ? JSON.stringify(e.attrs[k]) : String(e.attrs[k]))));
    pane.appendChild(kv);
    if (e.flags && e.flags.length) {
      pane.appendChild(h('div', { class: 'sec' }, h('h3', { text: 'Flags' }), e.flags.map((x) => h('div', { class: 'flag-row' }, tag(x, FLAG_CLS[x] || 't-warn'), FLAG_HELP[x] || ''))));
    }
  }
  function drPayload(label, p, pane) {
    const b = payloadBlock(label, p);
    pane.appendChild(b || emptyState('events', 'No ' + label.toLowerCase() + ' captured', 'The SDK did not record a payload for this event.'));
  }
  const drReq = (D, e, pane) => drPayload('Request', e.req, pane);
  const drResp = (D, e, pane) => drPayload('Response', e.resp, pane);
  function drMut(D, e, pane) {
    const ms = e.mutations || [];
    if (ms.length) {
      pane.appendChild(h('h3', { text: 'Mutations (' + ms.length + ')' }));
      ms.forEach((m) => pane.appendChild(mutCard(Object.assign({ tx: e.tx_id }, m))));
    }
    if (e.reads && e.reads.length) {
      pane.appendChild(h('h3', { text: 'Reads (' + e.reads.length + ')' }));
      e.reads.forEach((r) => {
        const c = h('div', { class: 'mut' }, h('div', { class: 'mut-head' }, tag(r.store || '?'), h('strong', { text: r.entity || '' }),
          r.row_count !== undefined ? tag(r.row_count + ' row(s)') : null), h('div', { class: 'mut-body' },
          r.statement ? h('pre', { class: 'code', text: r.statement }) : null,
          r.pks && r.pks.length ? h('div', null, 'pks: ', span('mono', r.pks.join(', '))) : null,
          payloadBlock('args', r.args), payloadBlock('sample', r.sample)));
        pane.appendChild(c);
      });
    }
  }
  function drRaw(D, e, pane) {
    const text = JSON.stringify(e, null, 2);
    pane.appendChild(h('div', { class: 'jv' }, h('div', { class: 'jv-bar' }, span('', 'event #' + e.seq + ' as JSON'), h('span', { class: 'grow' }), copyBtn(text, 'event JSON')),
      h('pre', { class: 'code' }, colorize(text))));
  }

  // ---------- timeline (trigger groups + windowed rendering) ----------
  const ROW_H = 38, GROW_H = 36, GHEAD_H = 64, MORE_H = 28, GAP_H = 12, HEAD_H = 32, OVER = 8;
  const lsGet = (k) => { try { return localStorage.getItem(k); } catch (e) { return null; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (e) { /* ignore */ } };
  /* Groups are derived from D.events; recomputed only when the fetched events changed (D.evVer) or liveness flipped. */
  function getGroups(D) {
    const live = isLive(D.sess), c = D.gcache;
    if (!c || c.ver !== D.evVer || c.n !== D.events.length || c.live !== live) D.gcache = { ver: D.evVer, n: D.events.length, live: live, res: U.groupEvents(D.events, { live: live }) };
    return D.gcache.res;
  }
  const GFLAG = { rolled_back: ['rolled back', 't-warn', 'a mutation in this group was rolled back', 'retry'], tx_unresolved: ['tx unresolved', 't-warn', FLAG_HELP.tx_unresolved, 'clock'],
    orphaned: ['orphaned', 't-err', FLAG_HELP.orphaned, 'warn'], retry_pending: ['retry pending', 't-retry', 'a retry is scheduled but has not run yet', 'retry'] };
  const GKIND_CHIP = { api: 'api.in', worker: 'worker.start', consume: 'queue.consume' };

  function mkTimeline(D, root) {
    const f = D.f;
    D.paintGen = D.paintGen || 0;
    if (!D.gExplicit) { D.gExplicit = new Map(); D.gReveal = new Set(); }
    const grouped = () => D.groupMode === 'trigger';
    const svc = h('select', { class: 'field', 'aria-label': 'Filter by service', onchange: (e) => { f.service = e.target.value; paint(true); } });
    const kind = h('select', { class: 'field', 'aria-label': 'Filter by kind', onchange: (e) => { f.kind = e.target.value; paint(true); } });
    const errChip = h('button', { type: 'button', class: 'chip', 'aria-pressed': String(f.err), title: 'Flat: only failing events. Grouped: healthy groups collapse, failing groups stay open.', onclick: () => {
      f.err = !f.err; errChip.setAttribute('aria-pressed', String(f.err));
      if (f.err && grouped()) getGroups(D).groups.forEach((g) => { if (!g.errCount) D.gExplicit.delete(g.key); });
      paint(true);
    } }, icon('err', 'sm'), 'Errors only');
    const ent = h('input', { class: 'field', type: 'search', placeholder: 'Entity / store / pk', value: f.entity, 'aria-label': 'Filter by entity', oninput: (e) => { f.entity = e.target.value.toLowerCase(); paint(true); } });
    const q = h('input', { class: 'field', type: 'search', placeholder: 'Search name, error, payload, ids', value: f.q, 'aria-label': 'Search events', oninput: (e) => { f.q = e.target.value.toLowerCase(); paint(true); } });
    S.search = q;
    const follow = h('input', { type: 'checkbox', checked: D.follow, onchange: (e) => { D.follow = e.target.checked; } });
    const followBox = h('label', { class: 'switch' }, follow, h('span', { class: 'track', 'aria-hidden': 'true' }), 'Follow tail');
    const count = h('span', { class: 'count' });
    const clearBtn = h('button', { type: 'button', class: 'btn ghost sm', text: 'Clear filters', onclick: () => {
      f.service = f.kind = f.entity = f.q = ''; f.err = false; q.value = ent.value = ''; errChip.setAttribute('aria-pressed', 'false'); optKey = ''; D.gReveal.clear(); paint(true);
    } });
    const segBtns = [['trigger', 'Trigger'], ['none', 'None (flat)']].map((m) => h('button', { type: 'button', 'aria-pressed': String(D.groupMode === m[0]), 'data-mode': m[0], text: m[1], onclick: () => setMode(m[0]) }));
    const orderBtn = h('button', { type: 'button', class: 'chip', 'aria-pressed': String(!!D.newestFirst), title: 'Order groups by trigger start: oldest first (default) or newest first', onclick: () => {
      D.newestFirst = !D.newestFirst; lsSet('tsession.gorder', D.newestFirst ? 'newest' : 'oldest'); orderBtn.setAttribute('aria-pressed', String(D.newestFirst)); paint(true);
    } }, icon('chev-u', 'sm'), 'Newest first');
    const expBtn = h('button', { type: 'button', class: 'btn ghost sm', text: 'Expand all', title: 'Expand all groups (e)', onclick: () => expandAll(true) });
    const colBtn = h('button', { type: 'button', class: 'btn ghost sm', text: 'Collapse all', title: 'Collapse all groups (c)', onclick: () => expandAll(false) });
    const groupCtl = h('div', { class: 'gctl' }, orderBtn, expBtn, colBtn);
    function setMode(m) {
      if (D.groupMode === m) return;
      D.groupMode = m; lsSet('tsession.group', m); setHashQuery('group', m);
      D.sel = null; syncControls(); paint(true);
    }
    function syncControls() {
      segBtns.forEach((b) => b.setAttribute('aria-pressed', String(b.getAttribute('data-mode') === D.groupMode)));
      groupCtl.hidden = !grouped();
      vTime.textContent = grouped() ? 'Offset' : 'Time';
    }
    root.appendChild(h('div', { class: 'filterbar' }, h('div', { class: 'search' }, icon('search', 'sm'), q, span('kbd', '/')), svc, kind, errChip, ent, clearBtn,
      h('div', { class: 'gby' }, span('lbl-s', 'Group by'), h('div', { class: 'seg', role: 'group', 'aria-label': 'Group by' }, segBtns)), groupCtl, h('span', { class: 'grow' }), count, followBox));
    const sizer = h('div', { class: 'vsizer' }), rowsEl = h('div', { class: 'vrows' });
    const emptyEl = h('div', { class: 'empty', hidden: true });
    const stickyBox = h('div', { class: 'vsticky', hidden: true });
    sizer.appendChild(rowsEl);
    const vTime = h('span', { class: 'num', text: 'Time' });
    const list = h('div', { class: 'vlist navable', tabindex: 0, role: 'listbox', 'aria-label': 'Events (j and k to move, Enter to open or toggle a group, [ and ] to jump between groups, e and c to expand or collapse all)' },
      h('div', { class: 'vhead', 'aria-hidden': 'true' }, span('', 'Seq'), span('', 'Kind'), span('', 'Name'), span('', 'Service'), vTime, span('num', 'Duration')), stickyBox, sizer, emptyEl);
    root.appendChild(list);
    let optKey = '', items = [], sels = [], idxOf = new Map(), idMap = new Map(), gHead = new Map(), gGap = new Map(), maxDur = 1, lastRange = '', raf = 0, stickyKey = '', G = null;
    function syncOptions() {
      const ss = Array.from(new Set(D.events.map((e) => e.service))).sort(), ks = Array.from(new Set(D.events.map((e) => e.kind))).sort();
      const key = ss.join('|') + '#' + ks.join('|');
      if (key === optKey) return;
      optKey = key;
      const fill = (sel, label, vals, cur) => { clear(sel); sel.appendChild(h('option', { value: '', text: label })); vals.forEach((v) => sel.appendChild(h('option', { value: v, text: v }))); sel.value = cur; };
      fill(svc, 'All services', ss, f.service); fill(kind, 'All kinds', ks, f.kind);
    }
    const hay = (e) => {
      let v = D.hay.get(e.seq);
      if (v === undefined) {
        v = [e.service, e.kind, e.name, e.err, e.span_id, e.trace_id, e.tx_id, JSON.stringify(e.attrs || ''),
          e.req && JSON.stringify(e.req.inline), e.resp && JSON.stringify(e.resp.inline)].join(' ').toLowerCase();
        D.hay.set(e.seq, v);
      }
      return v;
    };
    const entHay = (e) => (e.mutations || []).map((m) => [m.store, m.entity, U.pkLabel(m.pk)].join(' ')).concat((e.reads || []).map((r) => [r.store, r.entity, (r.pks || []).join(' ')].join(' '))).join(' ').toLowerCase();
    /* Flat mode also filters on errors; grouped mode keeps the whole group and collapses healthy ones instead. */
    const baseActive = () => !!(f.service || f.kind || f.entity || f.q);
    const matcher = () => (e) => (!f.service || e.service === f.service) && (!f.kind || e.kind === f.kind) && (grouped() || !f.err || e.err) &&
      (!f.entity || entHay(e).indexOf(f.entity) >= 0) && (!f.q || hay(e).indexOf(f.q) >= 0);
    const isOpen = (g) => (D.gExplicit.has(g.key) ? D.gExplicit.get(g.key) : (f.err && !g.errCount ? false : D.gDefaultOpen !== false));

    function build() {
      items = []; sels = []; idxOf = new Map(); idMap = new Map(); gHead = new Map(); gGap = new Map();
      let y = 0, nRows = 0, nGroups = 0;
      const push = (it, hh, id, selId) => {
        it.top = y; it.h = hh; y += hh; it.i = items.length; it.id = id; items.push(it); idMap.set(id, it.i);
        if (selId) { it.sid = selId; idxOf.set(selId, it.i); sels.push(it.i); }
        return it;
      };
      const m = matcher(), active = grouped() ? baseActive() : (baseActive() || f.err);
      G = grouped() ? getGroups(D) : null;
      if (!grouped()) {
        D.events.forEach((e) => { if (m(e)) { push({ t: 'row', e: e }, ROW_H, 'e:' + e.seq, 'e:' + e.seq); nRows++; } });
      } else {
        const gs = D.newestFirst ? G.groups.slice().reverse() : G.groups;
        gs.forEach((g) => {
          const rows = active ? g.rows.filter((r) => m(r.e)) : g.rows;
          if (!rows.length) return;
          nGroups++; nRows += rows.length;
          const open = isOpen(g);
          gHead.set(g.key, push({ t: 'head', g: g, matched: rows.length, open: open }, GHEAD_H, 'g:' + g.key, 'g:' + g.key).i);
          if (open) {
            const reveal = D.gReveal.has(g.key) && rows.length < g.rows.length;
            (reveal ? g.rows : rows).forEach((r) => push({ t: 'row', e: r.e, g: g, r: r, dim: reveal && !m(r.e), hit: active && m(r.e) }, GROW_H, 'e:' + r.e.seq, 'e:' + r.e.seq));
            if (rows.length < g.rows.length) push({ t: 'more', g: g, n: g.rows.length - rows.length, revealed: reveal }, MORE_H, 'm:' + g.key);
          }
          gGap.set(g.key, push({ t: 'gap', g: g }, GAP_H, 'f:' + g.key).i);
        });
      }
      maxDur = 1; D.events.forEach((e) => { maxDur = Math.max(maxDur, evDur(e)); });
      return { total: y, nRows: nRows, nGroups: nGroups, active: active };
    }

    function findIdx(y) {
      let lo = 0, hi = items.length - 1;
      if (hi < 0) return -1;
      while (lo < hi) { const mid = (lo + hi + 1) >> 1; if (items[mid].top <= y) lo = mid; else hi = mid - 1; }
      return lo;
    }
    function rowEl(it) {
      const e = it.e, g = it.g, G2 = G, gk = U.kindGroup(e.kind), sel = D.sel === 'e:' + e.seq, isTrig = g && g.trigger === e;
      const tags = [];
      if (e.err) tags.push(tag('error', 't-err', e.err, 'err'));
      if (rolledBack(e)) tags.push(tag('rolled back', 't-warn', 'a mutation in this event was rolled back', 'retry'));
      if (e.attempt > 1) tags.push(tag('attempt ' + e.attempt, 't-retry'));
      if (e.attrs && e.attrs.retry_pending) tags.push(tag('retry pending', 't-retry'));
      if (e.attrs && e.attrs.commit_failed) tags.push(tag('commit failed', 't-err'));
      (e.flags || []).forEach((x) => tags.push(tag(x, FLAG_CLS[x] || 't-warn', FLAG_HELP[x])));
      if (g && G2 && e.kind === 'queue.publish') {
        const ws = G2.pubWorkers.get(e.span_id);
        if (ws && ws.length) tags.push(h('button', { type: 'button', class: 'linkbtn jump', tabindex: -1, 'data-jump': 'g:' + ws[0], title: 'Scroll to the worker group that processed this message' + (ws.length > 1 ? ' (first of ' + ws.length + ' attempts)' : '') }, icon('async', 'sm'), 'jump to worker group' + (ws.length > 1 ? ' (' + ws.length + ')' : '')));
      }
      const pct = Math.max(3, Math.round(Math.sqrt(Math.max(0, evDur(e)) / maxDur) * 100));
      const bar = h('i'); bar.style.width = pct + '%';
      const guides = [];
      if (it.r) for (let d = 1; d < it.r.depth; d++) guides.push(h('i', { class: 'ind', 'aria-hidden': 'true' }));
      const nameCell = h('span', { class: 'c-name' }, guides, h('span', { class: 'nm', text: e.name || '', title: e.name || '' }), tags);
      const tx = it.r && it.r.tx;
      const r = h('div', { class: 'vrow' + (g ? ' ig' : '') + (e.err ? ' err' : '') + (rolledBack(e) ? ' rolled' : '') + (sel ? ' sel' : '') + (isTrig ? ' vtrig' : '') + (it.dim ? ' dim' : '') + (it.hit ? ' hit' : '') +
        (tx ? ' txr tx-' + tx.pos + (tx.rolledBack ? ' tx-rb' : '') : ''), id: 'ev-' + e.seq, role: 'option', 'aria-selected': String(sel), 'data-seq': e.seq, 'aria-level': g ? (it.r.depth + 1) : null,
        title: tx ? 'inside transaction ' + tx.id + (tx.rolledBack ? ' (rolled back)' : '') : null },
        span('c-seq', e.seq), h('span', null, kchip(e.kind, !!e.err)), nameCell,
        h('span', null, h('span', { class: 'svc', text: e.service, title: e.instance || e.service })),
        span('c-time', '+' + fmtNs(g ? U.relNs(e.start_ns, g.start) : rel(e))),
        h('span', { class: 'c-dur kg-' + gk }, h('span', { class: 'dbar' }, bar), span('dtxt', fmtNs(evDur(e)))));
      r.style.top = it.top + 'px'; r.style.height = it.h + 'px';
      if (it.r && it.r.depth > 1) nameCell.style.paddingLeft = '0px';
      return r;
    }
    function headEl(it, sticky) {
      const g = it.g, open = it.open, sel = D.sel === 'g:' + g.key, t = g.trigger;
      const chipKind = t ? (GKIND_CHIP[g.kind] || t.kind) : null;
      const status = g.status === 'error' ? tag(g.httpStatus ? 'error · ' + g.httpStatus : 'error', 't-err', g.errCount + ' event(s) in this group failed', 'err') : tag(g.httpStatus ? 'ok · ' + g.httpStatus : 'ok', 't-ok');
      const meta = h('span', { class: 'gmeta' }, span('mono', U.fmtClock(g.start)), span('mono', fmtNs(g.dur)), t ? span('mono faint', '#' + t.seq) : null);
      meta.title = 'started ' + (U.isoFromNs(g.start) || '?') + ' · duration ' + fmtNs(g.dur) + (t ? ' · trigger completed at #' + t.seq : '');
      const line1 = h('div', { class: 'gl1' },
        h('button', { type: 'button', class: 'twist', tabindex: -1, 'aria-label': open ? 'Collapse group' : 'Expand group', 'data-toggle': g.key }, icon(open ? 'chev-d' : 'chev-r')),
        chipKind ? kchip(chipKind, false) : tag('untriggered', 't-warn', 'no api.in or worker.start ancestor in this session', 'warn'),
        h('strong', { class: 'gtitle', text: g.title, title: g.title }), status,
        g.kind === 'untriggered' && t ? tag('untriggered', 't-warn', 'no api.in or worker.start ancestor in this session', 'warn') : null,
        g.flags.map((x) => tag.apply(null, GFLAG[x] || [x, 't-warn'])),
        h('span', { class: 'grow' }), meta);
      const line2 = h('div', { class: 'gl2' },
        g.asyncLabel ? h('span', { class: 'gasync', title: g.asyncLabel }, icon('async', 'sm'), g.asyncLabel) : null,
        g.triggeredBy ? h('button', { type: 'button', class: 'linkbtn jump', tabindex: -1, 'data-jump': 'e:' + g.triggeredBy.seq, title: 'Scroll to the queue.publish that started this job' }, 'go to publisher') : null,
        g.chips.map((c) => h('span', { class: 'gchip kg-' + (c.k === 'cache' ? 'cache' : c.k === 'err' ? 'dlq' : U.kindGroup(c.k)) }, h('b', { text: c.n }), ' ' + c.label)),
        g.mutText ? h('span', { class: 'gmut', title: g.mutText, text: g.mutText }) : null,
        h('span', { class: 'grow' }),
        it.matched < g.rows.length ? span('gcount', it.matched + ' of ' + g.rows.length + ' match') : span('gcount', g.rows.length + (g.rows.length === 1 ? ' event' : ' events')));
      const el = h('div', { class: 'ghead gk-' + g.kind + (g.status === 'error' ? ' err' : '') + (sel ? ' sel' : '') + (open ? '' : ' collapsed') + (sticky ? ' sticky' : ''),
        role: sticky ? null : 'option', 'aria-selected': sticky ? null : String(sel), 'aria-expanded': sticky ? null : String(open), 'data-gkey': g.key, id: sticky ? null : idOf('gh-', g.key) }, line1, line2);
      if (!sticky) { el.style.top = it.top + 'px'; el.style.height = it.h + 'px'; }
      return el;
    }
    function moreEl(it) {
      const b = h('div', { class: 'vmore', 'data-more': it.g.key, role: 'presentation' }, it.revealed ? '− hide ' + it.n + ' non-matching events' : '+' + it.n + ' hidden by filters · show');
      b.style.top = it.top + 'px'; b.style.height = it.h + 'px';
      return b;
    }
    function gapEl(it) { const b = h('div', { class: 'vgap', 'aria-hidden': 'true' }); b.style.top = it.top + 'px'; b.style.height = it.h + 'px'; return b; }
    function renderWindow() {
      raf = 0;
      const n = items.length, st = list.scrollTop, vh = list.clientHeight || 640, a = st - HEAD_H;
      const fi = findIdx(a), li = findIdx(st + vh - HEAD_H);
      const first = Math.max(0, fi - OVER), last = Math.min(n, li + 1 + OVER);
      const key = first + ':' + last + ':' + D.sel + ':' + n + ':' + D.paintGen;
      // sticky header of the group containing the first visible row, pushed up by the next group
      let sk = '', off = 0, shown = null;
      if (grouped() && fi >= 0 && items[fi].g) {
        const hd = items[gHead.get(items[fi].g.key)], gp = gGap.get(items[fi].g.key);
        if (hd && hd.top < a) { shown = hd; off = Math.min(0, items[gp].top - a - GHEAD_H); sk = hd.g.key + ':' + D.paintGen + ':' + D.sel; }
      }
      stickyBox.hidden = !shown;
      if (shown && sk !== stickyKey) { stickyKey = sk; clear(stickyBox).appendChild(headEl(shown, true)); }
      if (shown && stickyBox.firstChild) stickyBox.firstChild.style.top = off + 'px';
      if (!shown) stickyKey = '';
      if (key === lastRange) return;
      lastRange = key;
      clear(rowsEl);
      const frag = document.createDocumentFragment();
      for (let i = first; i < last; i++) {
        const it = items[i];
        frag.appendChild(it.t === 'row' ? rowEl(it) : it.t === 'head' ? headEl(it, false) : it.t === 'more' ? moreEl(it) : gapEl(it));
      }
      rowsEl.appendChild(frag);
    }
    const schedule = () => { if (!raf) raf = requestAnimationFrame(renderWindow); };
    list.addEventListener('scroll', schedule);
    S.onResize = schedule;
    function captureAnchor() {
      const st = list.scrollTop;
      if (st <= 0 || !items.length) return null;
      const i = findIdx(st - HEAD_H); if (i < 0) return null;
      const it = items[i];
      return { id: it.id, gkey: it.g ? it.g.key : null, off: (st - HEAD_H) - it.top };
    }
    function restoreAnchor(a) {
      if (!a) return;
      let i = idMap.get(a.id), off = a.off;
      if (i === undefined && a.gkey !== null && gHead.has(a.gkey)) { i = gHead.get(a.gkey); off = 0; }
      if (i !== undefined) list.scrollTop = Math.max(0, items[i].top + off + HEAD_H);
    }
    function paint(reset) {
      syncOptions(); syncControls();
      const anchor = reset ? null : captureAnchor();
      const b = build();
      clearBtn.hidden = !(b.active || f.err);
      if (grouped()) {
        const total = G.groups.length;
        count.textContent = (b.active || f.err) && b.nGroups !== total ? b.nGroups + ' of ' + total + ' triggers · ' + b.nRows + ' events' : total + (total === 1 ? ' trigger' : ' triggers') + ' · ' + D.events.length + ' events';
      } else count.textContent = b.nRows + ' of ' + D.events.length + ' events';
      sizer.style.height = b.total + 'px';
      sizer.hidden = !items.length;
      emptyEl.hidden = !!items.length;
      if (!items.length) {
        clear(emptyEl).appendChild(D.events.length
          ? emptyState('events', 'No events match the filters', 'Loosen the search or clear the filters to see all ' + D.events.length + ' events.')
          : emptyState('events', 'No events recorded yet', 'Send traffic through the instrumented services with this session\'s x-tsession-id header and events will appear here.'));
      }
      D.paintGen = (D.paintGen || 0) + 1;
      if (reset) list.scrollTop = 0; else restoreAnchor(anchor);
      renderWindow();
    }
    function ensureVisible(i) {
      const it = items[i]; if (!it) return;
      const pad = it.t === 'row' && grouped() ? GHEAD_H : 0, y = HEAD_H + it.top, vh = list.clientHeight || 640;
      if (y - HEAD_H - pad < list.scrollTop) list.scrollTop = Math.max(0, y - HEAD_H - pad);
      else if (y + it.h > list.scrollTop + vh) list.scrollTop = y + it.h - vh;
    }
    const elIdOf = (it) => (it.t === 'head' ? idOf('gh-', it.g.key) : 'ev-' + it.e.seq);
    function selectItem(i, open) {
      const it = items[i]; if (!it || !it.sid) return;
      D.sel = it.sid;
      ensureVisible(i);
      D.paintGen++;
      renderWindow();
      list.setAttribute('aria-activedescendant', elIdOf(it));
      if (open) { if (it.t === 'row') openDrawer(D, it.e, { focus: false }); else if (it.g.trigger) openDrawer(D, it.g.trigger, { focus: false }); }
    }
    function toggleGroup(key, want) {
      const g = G && G.byKey.get(key); if (!g) return;
      const open = isOpen(g);
      D.gExplicit.set(key, want === undefined ? !open : want);
      paint(false);
    }
    function expandAll(open) {
      D.gDefaultOpen = open; D.gExplicit.clear();
      if (open) D.gReveal.clear();
      paint(false);
      const i = idxOf.get(D.sel);
      if (i !== undefined) ensureVisible(i);
    }
    const curPos = () => { const i = idxOf.get(D.sel); return i === undefined ? -1 : sels.indexOf(i); };
    function jumpGroup(dir) {
      if (!grouped() || !sels.length) return;
      const cur = items[idxOf.get(D.sel)], heads = sels.filter((i) => items[i].t === 'head');
      if (!heads.length) return;
      let target;
      if (!cur) target = dir > 0 ? heads[0] : heads[heads.length - 1];
      else {
        const hi = gHead.get(cur.g.key);
        if (dir < 0) target = cur.t === 'row' ? hi : heads[Math.max(0, heads.indexOf(hi) - 1)];
        else target = heads[Math.min(heads.length - 1, heads.indexOf(hi) + 1)];
      }
      selectItem(target, drawerOpen());
    }
    /* Scroll to and select a group header ('g:KEY') or an event row ('e:SEQ'), expanding its group if needed. */
    function jump(spec) {
      if (spec.indexOf('e:') === 0) {
        const e = D.bySeq.get(Number(spec.slice(2)));
        if (!e) return;
        view.select(e);
        const i = idxOf.get('e:' + e.seq);
        if (i === undefined) { toast('That event is hidden by the current filters', 'err'); return; }
        selectItem(i, drawerOpen());
      } else if (spec.indexOf('g:') === 0) {
        const key = spec.slice(2), g = G && G.byKey.get(key);
        if (!g) return;
        if (!gHead.has(key)) { toast('That group is hidden by the current filters', 'err'); return; }
        list.focus({ preventScroll: true });
        selectItem(gHead.get(key), drawerOpen());
        const hd = items[gHead.get(key)];
        list.scrollTop = Math.max(0, hd.top);
        D.paintGen++; renderWindow();
      }
    }
    S.nav = (dir) => {
      if (!sels.length) return;
      const p = curPos();
      selectItem(sels[p < 0 ? (dir > 0 ? 0 : sels.length - 1) : Math.max(0, Math.min(sels.length - 1, p + dir))], drawerOpen());
    };
    S.activate = () => {
      if (!sels.length) return;
      if (curPos() < 0) selectItem(sels[0], false);
      const it = items[idxOf.get(D.sel)];
      if (!it) return;
      if (it.t === 'head') { toggleGroup(it.g.key); return; }
      if (drawerOpen() && D.drawerSeq === it.e.seq) focusDrawer(); else openDrawer(D, it.e, { focus: false });
    };
    S.keys = { '[': () => jumpGroup(-1), ']': () => jumpGroup(1), e: () => grouped() && expandAll(true), c: () => grouped() && expandAll(false) };
    list.addEventListener('keydown', (ev) => {
      const p = Math.max(0, curPos());
      const j = ev.key === 'Home' ? 0 : ev.key === 'End' ? sels.length - 1 : ev.key === 'PageDown' ? p + 10 : ev.key === 'PageUp' ? p - 10 : null;
      if (j !== null && sels.length) { ev.preventDefault(); selectItem(sels[Math.max(0, Math.min(sels.length - 1, j))], drawerOpen()); }
    });
    list.addEventListener('click', (ev) => {
      const t = ev.target; if (!t.closest) return;
      const jb = t.closest('[data-jump]');
      if (jb) { ev.stopPropagation(); jump(jb.getAttribute('data-jump')); return; }
      const mo = t.closest('.vmore');
      if (mo) { const k = mo.getAttribute('data-more'); if (D.gReveal.has(k)) D.gReveal.delete(k); else D.gReveal.add(k); paint(false); return; }
      const gh = t.closest('.ghead');
      if (gh) {
        const key = gh.getAttribute('data-gkey');
        list.focus({ preventScroll: true });
        D.sel = 'g:' + key;
        toggleGroup(key);
        list.setAttribute('aria-activedescendant', idOf('gh-', key));
        return;
      }
      const r = t.closest('.vrow');
      if (!r) return;
      const e = D.bySeq.get(Number(r.getAttribute('data-seq')));
      if (!e) return;
      list.focus({ preventScroll: true });
      D.sel = 'e:' + e.seq; D.paintGen++; renderWindow();
      list.setAttribute('aria-activedescendant', 'ev-' + e.seq);
      openDrawer(D, e, { focus: false });
    });
    syncControls();
    const view = {
      /* Select an event (deep link, drawer cross-reference, jump button), opening its group first. */
      select(e) {
        if (grouped()) {
          const gr = getGroups(D), k = gr.keyOf.get(e.seq), g = k === undefined ? null : gr.byKey.get(k);
          if (g && !isOpen(g)) D.gExplicit.set(g.key, true);
        }
        D.sel = 'e:' + e.seq;
        paint(false);
        const i = idxOf.get(D.sel);
        if (i !== undefined) { ensureVisible(i); D.paintGen++; renderWindow(); }
      },
      refreshSel() { D.paintGen = (D.paintGen || 0) + 1; renderWindow(); },
      update(changed) {
        followBox.hidden = !isLive(D.sess);
        if (changed || D.dirty) {
          D.dirty = false; paint(false);
          if (D.sel && idxOf.has(D.sel) && D.drawerSeq !== undefined && D.sel === 'e:' + D.drawerSeq) ensureVisible(idxOf.get(D.sel));
          if (D.follow && isLive(D.sess) && changed && !drawerOpen()) { list.scrollTop = grouped() && D.newestFirst ? 0 : list.scrollHeight; renderWindow(); }
        }
      },
    };
    return view;
  }

  // ---------- call tree ----------
  function mkTree(D, root) {
    const info = h('span', { class: 'count' });
    const axis = h('div', { class: 'axis' });
    const tbody = h('div', { class: 'tbody' });
    const box = h('div', { class: 'treewrap navable', tabindex: 0, role: 'tree', 'aria-label': 'Call tree (j and k to move, arrow keys to collapse or expand, Enter to open)' },
      h('div', { class: 'thead', 'aria-hidden': 'true' }, span('', 'Span'), span('', 'Service'), axis, span('num', 'Duration')), tbody);
    root.appendChild(h('div', { class: 'filterbar' },
      h('button', { type: 'button', class: 'btn', text: 'Expand all', onclick: () => { D.collapsed.clear(); paint(); } }),
      h('button', { type: 'button', class: 'btn', text: 'Collapse all', onclick: () => { walk(D.trace || [], (n) => { if (n.children.length || n.linked.length) D.collapsed.add(n.event.span_id); }); paint(); } }),
      h('span', { class: 'grow' }), tag('async linked subtree', 't-async', 'dashed connector: worker spans linked from a queue publish, running after the publisher returned', 'async'), info));
    root.appendChild(box);
    D.trace = null;
    const walk = (nodes, fn) => nodes.forEach((n) => { fn(n); walk(n.children || [], fn); walk(n.linked || [], fn); });
    function norm(n) { n.children = n.children || []; n.linked = n.linked || []; n.children.forEach(norm); n.linked.forEach(norm); }
    let t0 = 0, total = 1, flat = [], flatIdx = new Map();
    function descendants(n, kind) { let c = 0; walk([n], (x) => { if (x.event.kind === kind) c++; }); return c; }
    function addNode(n, guides, asyncRoot, inAsync, out) {
      out.push({ n: n, guides: guides, asyncRoot: asyncRoot, inAsync: inAsync });
      if (D.collapsed.has(n.event.span_id)) return;
      const items = n.children.map((c) => ({ n: c, a: false })).concat(n.linked.map((c) => ({ n: c, a: true })));
      items.forEach((it, i) => addNode(it.n, guides.concat([{ cont: i < items.length - 1, dashed: it.a || inAsync }]), it.a, inAsync || it.a, out));
    }
    function rowEl(it) {
      const n = it.n, e = n.event, id = e.span_id, kids = n.children.length + n.linked.length, collapsed = D.collapsed.has(id);
      const g = U.kindGroup(e.kind), retry = e.kind === 'worker.retry' || (e.attrs && e.attrs.retry_pending);
      const tw = kids ? h('button', { type: 'button', class: 'twist', tabindex: -1, 'aria-label': collapsed ? 'Expand' : 'Collapse', onclick: (ev) => { ev.stopPropagation(); toggle(id); } }, icon(collapsed ? 'chev-r' : 'chev-d')) : h('span', { class: 'twist-sp' });
      const guides = it.guides.map((gd, i) => h('span', { class: 'g' + (i === it.guides.length - 1 ? ' elbow' + (gd.cont ? '' : ' end') : gd.cont ? ' pass' : '') + (gd.dashed ? ' dashed' : '') }));
      const tags = [];
      if (it.asyncRoot) tags.push(tag('async', 't-async', 'runs asynchronously after the publisher returned', 'async'));
      if (e.attempt) tags.push(tag('attempt ' + e.attempt, retry ? 't-retry' : ''));
      if (retry) tags.push(tag('retry', 't-retry', 'this attempt failed and will be retried', 'retry'));
      if (e.kind === 'worker.dlq') tags.push(tag('DLQ', 't-err', 'message moved to the dead-letter queue', 'dlq'));
      if (e.err) tags.push(tag('error', 't-err', e.err, 'err'));
      if (rolledBack(e)) tags.push(tag('rolled back', 't-warn', null, 'retry'));
      if (e.attrs && e.attrs.commit_failed) tags.push(tag('commit failed', 't-err'));
      (e.flags || []).forEach((x) => tags.push(tag(x, FLAG_CLS[x] || 't-warn', FLAG_HELP[x])));
      if (e.kind === 'queue.publish') {
        const workers = n.linked.filter((x) => x.event.kind === 'worker.start');
        if (!n.linked.length) tags.push(isLive(D.sess) ? tag('pending async', 't-warn', 'no worker has consumed this message yet', 'clock') : tag('orphaned async', 't-err', 'no worker ever processed this message', 'warn'));
        else {
          if (workers.length > 1) tags.push(tag(workers.length + ' attempts'));
          if (descendants(n, 'worker.dlq')) tags.push(tag('dead-lettered', 't-err', null, 'dlq'));
        }
      }
      const gg = D.events.length ? getGroups(D).byKey.get(id) : null;
      if (gg && gg.trigger === e) {
        tags.push(gg.status === 'error' ? tag('group error', 't-err', 'a span in this trigger failed', 'err') : tag('group ok', 't-ok'));
        if (gg.chips.length) tags.push(h('span', { class: 'gsum', title: gg.mutText || '', text: U.chipsText(gg) }));
      }
      const left = Math.min(99.5, Math.max(0, (e.start_ns - t0) / total * 100)), width = Math.max(0.5, evDur(e) / total * 100);
      const bar = h('i', { class: (it.inAsync ? 'async ' : '') + (e.err ? 'err' : '') });
      bar.style.left = left + '%'; bar.style.width = Math.min(width, 100 - left) + '%';
      const sel = D.treeSel === id;
      return h('div', { class: 'trow' + (e.err ? ' err' : '') + (it.inAsync ? ' async' : '') + (sel ? ' sel' : ''), id: idOf('tr-', id), role: 'treeitem', 'aria-level': it.guides.length + 1,
        'aria-expanded': kids ? String(!collapsed) : null, 'aria-selected': String(sel), 'data-span': id },
        h('div', { class: 'tname' }, guides, tw, h('div', { class: 'body' }, kchip(e.kind, !!e.err), h('span', { class: 'nm', text: e.name || '(no name)', title: e.err || e.name }), tags)),
        h('span', null, h('span', { class: 'svc', text: e.service })),
        h('div', { class: 'wf kg-' + g, title: 'starts +' + fmtNs(e.start_ns - t0) + ', ' + fmtNs(evDur(e)) }, bar),
        span('tdur', fmtNs(evDur(e))));
    }
    function toggle(id) { if (D.collapsed.has(id)) D.collapsed.delete(id); else D.collapsed.add(id); paint(); }
    function paint() {
      const keepTop = box.scrollTop;
      clear(tbody);
      const roots = D.trace || [];
      info.textContent = '';
      if (!roots.length) { tbody.appendChild(emptyState('events', 'No spans yet', 'Spans appear here once the session has captured traced events.')); return; }
      let mn = Infinity, mx = 0, count = 0;
      walk(roots, (n) => { count++; mn = Math.min(mn, n.event.start_ns); mx = Math.max(mx, n.event.end_ns); });
      t0 = mn; total = Math.max(1, mx - mn);
      info.textContent = count + ' spans, ' + fmtNs(total) + ' wall time';
      clear(axis);
      for (let i = 0; i <= 4; i++) axis.appendChild(span('', i === 0 ? '0' : fmtNs(total * i / 4)));
      flat = []; roots.forEach((r) => addNode(r, [], false, false, flat));
      flatIdx = new Map(); flat.forEach((it, i) => flatIdx.set(it.n.event.span_id, i));
      const frag = document.createDocumentFragment();
      flat.forEach((it) => frag.appendChild(rowEl(it)));
      tbody.appendChild(frag);
      box.scrollTop = keepTop;
      if (D.treeSel && flatIdx.has(D.treeSel)) box.setAttribute('aria-activedescendant', idOf('tr-', D.treeSel));
    }
    function markSel() {
      box.querySelectorAll('.trow.sel').forEach((r) => { r.classList.remove('sel'); r.setAttribute('aria-selected', 'false'); });
      const el = D.treeSel ? document.getElementById(idOf('tr-', D.treeSel)) : null;
      if (el) {
        el.classList.add('sel'); el.setAttribute('aria-selected', 'true'); box.setAttribute('aria-activedescendant', el.id);
        const bt = box.scrollTop, hh = 32, top = el.offsetTop, hgt = el.offsetHeight;
        if (top - hh < bt) box.scrollTop = Math.max(0, top - hh); else if (top + hgt > bt + box.clientHeight) box.scrollTop = top + hgt - box.clientHeight;
      }
    }
    function selectIdx(i, open) {
      i = Math.max(0, Math.min(flat.length - 1, i));
      const it = flat[i]; if (!it) return;
      D.treeSel = it.n.event.span_id; markSel();
      if (open) openDrawer(D, it.n.event, { focus: false });
    }
    S.nav = (dir) => { if (!flat.length) return; const i = flatIdx.has(D.treeSel) ? flatIdx.get(D.treeSel) : -1; selectIdx(i < 0 ? (dir > 0 ? 0 : flat.length - 1) : i + dir, drawerOpen()); };
    S.activate = () => {
      if (!flat.length) return;
      if (!flatIdx.has(D.treeSel)) selectIdx(0, false);
      const e = D.bySpan.get(D.treeSel);
      if (e) { if (drawerOpen() && D.drawerSeq === e.seq) focusDrawer(); else openDrawer(D, e, { focus: false }); }
    };
    box.addEventListener('keydown', (ev) => {
      const i = flatIdx.has(D.treeSel) ? flatIdx.get(D.treeSel) : -1, it = i >= 0 ? flat[i] : null;
      if (ev.key === 'ArrowRight' && it) { ev.preventDefault(); if (D.collapsed.has(D.treeSel)) toggle(D.treeSel); else selectIdx(i + 1, drawerOpen()); }
      else if (ev.key === 'ArrowLeft' && it) {
        ev.preventDefault();
        const hasKids = it.n.children.length + it.n.linked.length;
        if (hasKids && !D.collapsed.has(D.treeSel)) toggle(D.treeSel);
        else { for (let j = i - 1; j >= 0; j--) if (flat[j].guides.length < it.guides.length) { selectIdx(j, drawerOpen()); break; } }
      } else if (ev.key === 'Home') { ev.preventDefault(); selectIdx(0, drawerOpen()); } else if (ev.key === 'End') { ev.preventDefault(); selectIdx(flat.length - 1, drawerOpen()); }
    });
    tbody.addEventListener('click', (ev) => {
      const r = ev.target.closest ? ev.target.closest('.trow') : null;
      if (!r) return;
      const e = D.bySpan.get(r.getAttribute('data-span'));
      if (!e) return;
      box.focus({ preventScroll: true });
      D.treeSel = e.span_id; markSel(); openDrawer(D, e, { focus: false });
    });
    return {
      select(e) { D.treeSel = e.span_id; markSel(); },
      refreshSel() { markSel(); },
      async update(changed) {
        if (!changed && D.trace) return;
        try {
          const r = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/trace');
          if (D.token !== S.token) return;
          D.trace = (r && r.roots) || []; D.trace.forEach(norm); paint();
        } catch (e) { clear(tbody).appendChild(emptyState('error', 'Cannot load the call tree', e.message)); }
      },
    };
  }

  // ---------- entities ----------
  function mkEntities(D, root) {
    const list = h('div', { class: 'elist', role: 'listbox', 'aria-label': 'Entities' });
    const detail = h('div', { class: 'card pad' }, emptyState('events', 'Select an entity', 'Pick an entity on the left to see its ordered mutation history.'));
    const q = h('input', { class: 'field', type: 'search', placeholder: 'Filter store, entity, pk', 'aria-label': 'Filter entities', value: D.entQ || '', oninput: (e) => { D.entQ = e.target.value.toLowerCase(); paintList(); } });
    S.search = q;
    root.appendChild(h('div', { class: 'toolbar' }, h('div', { class: 'search' }, icon('search', 'sm'), q, span('kbd', '/'))));
    root.appendChild(h('div', { class: 'split' }, h('div', { class: 'card' }, list), detail));
    D.ents = D.ents || null;
    let items = [], seqNo = 0;
    const keyOf = (x) => [x.store, x.entity, U.pkRaw(x.pk)].join('\u0000');
    function paintList() {
      clear(list); items = [];
      const vis = (D.ents || []).filter((x) => !D.entQ || [x.store, x.entity, U.pkLabel(x.pk)].join(' ').toLowerCase().indexOf(D.entQ) >= 0);
      if (!vis.length) { list.appendChild(h('div', { class: 'empty' }, h('p', { text: D.ents ? (D.ents.length ? 'No entities match.' : 'No mutations recorded in this session.') : 'Loading entities…' }))); return; }
      vis.forEach((x) => {
        const b = h('button', { type: 'button', class: 'elist-item', role: 'option', 'aria-current': D.entSel === keyOf(x) ? 'true' : null, onclick: () => selectEnt(x) },
          h('div', { class: 'l1' }, tag(x.store), h('strong', { text: x.entity })),
          h('div', { class: 'l2' }, span('mono', U.pkLabel(x.pk)), span('note', x.mutation_count + (x.mutation_count === 1 ? ' mutation' : ' mutations'))));
        b._x = x; items.push(b); list.appendChild(b);
      });
    }
    function selectEnt(x) {
      D.entSel = keyOf(x);
      items.forEach((b) => b.setAttribute('aria-current', String(keyOf(b._x) === D.entSel)));
      loadDetail(x);
    }
    async function loadDetail(x) {
      const my = ++seqNo;
      clear(detail).appendChild(skeletonRows(3));
      try {
        const r = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/entities/' + encodeURIComponent(x.store) + '/' + encodeURIComponent(x.entity) + '/' + encodeURIComponent(U.pkRaw(x.pk)));
        if (my !== seqNo) return;
        clear(detail);
        const ms = asList(r, 'mutations');
        const rbN = ms.filter((it) => (it.effective !== undefined ? it.effective : (it.mutation || {}).effective) === false).length;
        detail.appendChild(h('div', { class: 'edetail-head' }, tag(x.store), h('h2', { text: x.entity }), span('mono', U.pkLabel(x.pk)), h('span', { class: 'grow' }),
          tag(ms.length + (ms.length === 1 ? ' mutation' : ' mutations')), rbN ? tag(rbN + ' rolled back', 't-warn', null, 'retry') : null));
        if (!ms.length) detail.appendChild(emptyState('events', 'No mutations', 'Nothing recorded for this entity.'));
        const rail = h('ol', { class: 'rail', 'aria-label': 'Mutation history, oldest first' });
        ms.forEach((it) => {
          const m = Object.assign({}, it.mutation || {}, it);
          delete m.mutation;
          const eff = it.effective !== undefined ? it.effective : (it.mutation || {}).effective;
          const ev = it.seq !== undefined ? D.bySeq.get(it.seq) : null;
          const lead = [span('mono', '#' + it.seq), ev ? h('button', { type: 'button', class: 'linkbtn', text: 'open event', onclick: () => openDrawer(D, ev, { focus: false }) }) : null];
          const mu = Object.assign({ store: x.store, entity: x.entity, pk: x.pk }, m, { effective: eff });
          rail.appendChild(h('li', { class: 'op-' + U.opClass(mu.op) },
            h('span', { class: 'rail-dot' + (eff === false ? ' rolled' : ''), 'aria-hidden': 'true' }),
            h('div', { class: 'rail-meta' }, m.service || (ev && ev.service) ? span('svc', m.service || ev.service) : null, ev ? span('mono', '+' + fmtNs(rel(ev))) : null), mutCard(mu, lead)));
        });
        detail.appendChild(rail);
      } catch (e) { if (my === seqNo) clear(detail).appendChild(emptyState('error', 'Cannot load this entity', e.message)); }
    }
    S.nav = (dir) => {
      if (!items.length) return;
      const i = items.findIndex((b) => keyOf(b._x) === D.entSel);
      const b = items[Math.max(0, Math.min(items.length - 1, i < 0 ? 0 : i + dir))];
      selectEnt(b._x); b.focus(); b.scrollIntoView({ block: 'nearest' });
    };
    return { async update(changed) {
      if (!changed && D.ents) return;
      try {
        D.ents = asList(await col('/v1/sessions/' + encodeURIComponent(D.id) + '/entities'), 'entities');
        if (D.token !== S.token) return;
        paintList();
        if (D.entPick !== undefined && items[D.entPick]) { selectEnt(items[D.entPick]._x); D.entPick = undefined; }
        else if (!D.entSel && items.length) selectEnt(items[0]._x);
        else if (D.entSel) { const cur = items.find((b) => keyOf(b._x) === D.entSel); if (cur && changed) loadDetail(cur._x); else if (cur && !detail.querySelector('.rail')) loadDetail(cur._x); }
      } catch (e) { clear(list).appendChild(emptyState('error', 'Cannot load entities', e.message)); }
    } };
  }

  // ---------- summary ----------
  let chartSeq = 0;
  const ellipsize = (t, n) => (t.length > n ? t.slice(0, Math.max(1, n - 1)) + '…' : t);
  /* Horizontal bar chart. Every bar carries a text label and a numeric value, so colour is never the only channel. */
  function barChart(width, items, o) {
    const rowH = 28, pad = 4, labelW = Math.min(190, Math.max(110, Math.floor(width * 0.36))), valW = o.valueW || 44;
    const x0 = labelW + 8, bw = Math.max(40, width - x0 - valW), H = items.length * rowH + pad * 2, max = Math.max(1, ...items.map((i) => i.value));
    const id = 'ch' + (++chartSeq);
    const svg = sv('svg', { class: 'chart', role: 'img', viewBox: '0 0 ' + width + ' ' + H, width: width, height: H, 'aria-labelledby': id + 't ' + id + 'd' },
      sv('title', { id: id + 't' }, o.title), sv('desc', { id: id + 'd' }, o.desc + ': ' + items.map((i) => i.label + ' ' + i.value).join(', ') + '.'));
    items.forEach((it, k) => {
      const y = pad + k * rowH, g = sv('g', { class: it.group ? 'kg-' + it.group : '' });
      const ix = it.group ? 20 : 0;
      if (it.group) g.appendChild(sv('use', { href: '#i-' + KIND_ICON[it.group], x: 0, y: y + 7, width: 14, height: 14, class: 'ico-u' }));
      const t = sv('text', { class: 'lbl-t', x: ix, y: y + 18 }, ellipsize(it.label, Math.floor((labelW - ix) / 6.4)));
      if (it.label.length * 6.4 > labelW - ix) t.appendChild(sv('title', null, it.label));
      g.appendChild(t);
      g.appendChild(sv('rect', { class: 'track', x: x0, y: y + 9, width: bw, height: 10, rx: 5 }));
      g.appendChild(sv('rect', { class: o.cls || 'b-k', x: x0, y: y + 9, width: Math.max(4, it.value / max * bw), height: 10, rx: 5 }));
      g.appendChild(sv('text', { class: 'val', x: width, y: y + 18, 'text-anchor': 'end' }, String(it.value)));
      svg.appendChild(g);
    });
    return svg;
  }
  /* Stacked committed / rolled-back bars per store and entity. Rolled-back segments are hatched. */
  function mutationChart(width, rows) {
    const rowH = 28, pad = 4, labelW = Math.min(210, Math.max(120, Math.floor(width * 0.4))), valW = 96, x0 = labelW + 8, bw = Math.max(40, width - x0 - valW);
    const H = rows.length * rowH + pad * 2, max = Math.max(1, ...rows.map((r) => r.n)), id = 'ch' + (++chartSeq);
    const svg = sv('svg', { class: 'chart', role: 'img', viewBox: '0 0 ' + width + ' ' + H, width: width, height: H, 'aria-labelledby': id + 't ' + id + 'd' },
      sv('title', { id: id + 't' }, 'Mutations by store and entity'),
      sv('desc', { id: id + 'd' }, 'Committed and rolled-back mutations per entity: ' + rows.map((r) => r.label + ' ' + r.n + ' total, ' + r.rb + ' rolled back').join('; ') + '.'),
      sv('defs', null, sv('pattern', { id: id + 'p', width: 6, height: 6, patternUnits: 'userSpaceOnUse', patternTransform: 'rotate(45)' },
        sv('rect', { class: 'hatch-bg', width: 6, height: 6 }), sv('line', { class: 'hatch-ln', x1: 0, y1: 0, x2: 0, y2: 6 }))));
    rows.forEach((r, k) => {
      const y = pad + k * rowH, ok = Math.max(0, r.n - r.rb), wOk = ok / max * bw, wRb = r.rb / max * bw;
      const t = sv('text', { class: 'lbl-t', x: 0, y: y + 18 }, ellipsize(r.label, Math.floor(labelW / 6.4)));
      if (r.label.length * 6.4 > labelW) t.appendChild(sv('title', null, r.label));
      svg.appendChild(t);
      svg.appendChild(sv('rect', { class: 'track', x: x0, y: y + 9, width: bw, height: 10, rx: 5 }));
      if (wOk > 0) svg.appendChild(sv('rect', { class: 'b-ok', x: x0, y: y + 9, width: Math.max(3, wOk), height: 10, rx: 3 }));
      if (wRb > 0) svg.appendChild(sv('rect', { x: x0 + wOk, y: y + 9, width: Math.max(3, wRb), height: 10, rx: 3, fill: 'url(#' + id + 'p)', stroke: 'var(--warn)', 'stroke-width': 1 }));
      svg.appendChild(sv('text', { class: 'val', x: width, y: y + 18, 'text-anchor': 'end' }, r.rb ? r.n + ' (' + r.rb + ' rb)' : String(r.n)));
    });
    return svg;
  }
  /* Column chart of events over wall time; errors stacked in red. */
  function activityChart(width, evs) {
    const H = 150, L = 36, B = 24, T = 8, plotW = Math.max(60, width - L - 4), plotH = H - B - T;
    let mn = Infinity, mx = 0; evs.forEach((e) => { mn = Math.min(mn, e.start_ns); mx = Math.max(mx, e.start_ns); });
    const span_ = Math.max(1, mx - mn), nb = Math.max(12, Math.min(60, Math.floor(plotW / 12))), ok = new Array(nb).fill(0), er = new Array(nb).fill(0);
    evs.forEach((e) => { const b = Math.min(nb - 1, Math.floor((e.start_ns - mn) / span_ * nb)); if (e.err) er[b]++; else ok[b]++; });
    const peak = Math.max(1, ...ok.map((v, i) => v + er[i])), id = 'ch' + (++chartSeq), bwid = plotW / nb;
    const pk = ok.map((v, i) => v + er[i]).indexOf(peak);
    const svg = sv('svg', { class: 'chart', role: 'img', viewBox: '0 0 ' + width + ' ' + H, width: width, height: H, 'aria-labelledby': id + 't ' + id + 'd' },
      sv('title', { id: id + 't' }, 'Events over time'),
      sv('desc', { id: id + 'd' }, evs.length + ' events over ' + fmtNs(span_) + ' in ' + nb + ' time buckets; busiest bucket has ' + peak + ' events at about +' + fmtNs(pk / nb * span_) + '; ' + er.reduce((a, b) => a + b, 0) + ' error events.'));
    [0, 0.5, 1].forEach((p) => { const y = T + plotH * (1 - p); svg.appendChild(sv('line', { class: p ? 'grid-l' : 'axis-l', x1: L, x2: L + plotW, y1: y, y2: y })); });
    svg.appendChild(sv('text', { x: L - 6, y: T + 4, 'text-anchor': 'end' }, String(peak)));
    svg.appendChild(sv('text', { x: L - 6, y: T + plotH + 4, 'text-anchor': 'end' }, '0'));
    ok.forEach((v, i) => {
      const hOk = v / peak * plotH, hEr = er[i] / peak * plotH, x = L + i * bwid + 1;
      if (v) svg.appendChild(sv('rect', { class: 'b-acc', x: x, y: T + plotH - hOk, width: Math.max(1, bwid - 2), height: hOk, rx: 1.5 }));
      if (er[i]) svg.appendChild(sv('rect', { class: 'b-err', x: x, y: T + plotH - hOk - hEr, width: Math.max(1, bwid - 2), height: hEr, rx: 1.5 }));
    });
    svg.appendChild(sv('text', { x: L, y: H - 6 }, '+0'));
    svg.appendChild(sv('text', { x: L + plotW, y: H - 6, 'text-anchor': 'end' }, '+' + fmtNs(span_)));
    return svg;
  }
  function mkSummary(D, root) {
    const box = h('div');
    root.appendChild(box);
    D.srv = D.srv || null;
    let charts = [];
    function stat(ic, label, v, sub, cls) {
      return h('div', { class: 'card stat ' + (cls || '') }, h('div', { class: 'lb' }, icon(ic, 'sm'), label), h('div', { class: 'vv', text: v }), sub ? h('div', { class: 'ss', text: sub }) : null);
    }
    function chartCard(title, sub, build, legend) {
      const c = h('div', { class: 'card chart-card' }, h('h2', { text: title }), h('div', { class: 'sub', text: sub }));
      const host = h('div');
      c.appendChild(host);
      if (legend) c.appendChild(legend);
      charts.push({ host: host, card: c, build: build });
      return c;
    }
    function drawCharts() {
      charts.forEach((c) => {
        const w = Math.floor(c.card.clientWidth - 32) || 420;
        clear(c.host).appendChild(c.build(Math.max(280, w)));
      });
    }
    function paint() {
      clear(box); charts = [];
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
      /* Prefer the collector's own numbers when it provided them; fall back to client-side counts. */
      const has = (k) => D.srv && D.srv[k] !== undefined && D.srv[k] !== null;
      const obj = (k) => (has(k) && typeof D.srv[k] === 'object' && !Array.isArray(D.srv[k]) ? D.srv[k] : null);
      const kindC = obj('by_kind') || byKind, svcC = obj('by_service') || bySvc;
      const flagsC = obj('flags') || flagc;
      if (obj('mutations')) {
        Object.keys(srv.mutations).forEach((st) => Object.keys(srv.mutations[st] || {}).forEach((en) => {
          const k = st + ' / ' + en, o = muts[k] || (muts[k] = { n: 0, ops: {}, rb: 0 });
          o.n = srv.mutations[st][en];
        }));
      }
      const failN = has('failures') && Array.isArray(srv.failures) ? srv.failures.length : fails.length;
      if (has('dropped_events')) dropped = srv.dropped_events;
      const pending = evs.filter((e) => e.kind === 'queue.publish' && !closed.has(e.span_id));
      const rbCount = Object.keys(muts).reduce((a, k) => a + muts[k].rb, 0);
      const mutTotal = Object.keys(muts).reduce((a, k) => a + muts[k].n, 0);
      const dur = has('duration_ns') ? srv.duration_ns : (evs.length ? maxE - minS : 0);
      const pendN = has('pending_async') ? srv.pending_async : pending.length;
      const nEv = has('event_count') ? srv.event_count : evs.length;
      box.appendChild(h('div', { class: 'stat-cards' },
        stat('timeline', 'Events', String(nEv), Object.keys(kindC).length + ' kinds'), stat('clock', 'Duration', fmtNs(dur), 'first to last event'),
        stat('layers', 'Services', String(Object.keys(svcC).length), 'distinct services'), stat('err', 'Failures', String(failN), failN ? 'spans with an error' : 'no failed spans', failN ? 'bad' : ''),
        stat('retry', 'Rolled-back mutations', String(rbCount), 'of ' + mutTotal + ' total', rbCount ? 'warnc' : ''),
        stat('async', isLive(s) ? 'Pending async' : 'Orphaned async', String(pendN), isLive(s) ? 'awaiting a worker' : 'publishes with no worker', pendN ? (isLive(s) ? 'warnc' : 'bad') : ''),
        stat('warn', 'Dropped events', String(dropped), dropped ? 'history is a lower bound' : 'none dropped', dropped ? 'bad' : '')));
      const kItems = Object.keys(kindC).sort((a, b) => kindC[b] - kindC[a]).map((k) => ({ label: k, value: kindC[k], group: U.kindGroup(k) }));
      const sItems = Object.keys(svcC).sort((a, b) => svcC[b] - svcC[a]).map((k) => ({ label: k, value: svcC[k] }));
      const grid = h('div', { class: 'charts' });
      box.appendChild(grid);
      if (kItems.length) grid.appendChild(chartCard('Events by kind', 'Count of events per kind', (w) => barChart(w, kItems, { title: 'Events by kind', desc: 'Number of events per kind' })));
      if (sItems.length) grid.appendChild(chartCard('Events by service', 'Count of events per service', (w) => barChart(w, sItems, { title: 'Events by service', desc: 'Number of events per service', cls: 'b-acc' })));
      if (evs.length > 1) grid.appendChild(chartCard('Activity over time', 'Events per time bucket; errors stacked in red', (w) => activityChart(w, evs),
        h('div', { class: 'legend' }, h('span', null, h('i', { class: 'b-acc' }), 'events'), h('span', null, h('i', { class: 'b-err' }), 'errors'))));
      const mk = Object.keys(muts).sort((a, b) => muts[b].n - muts[a].n);
      if (mk.length) {
        const mrows = mk.map((k) => ({ label: k, n: muts[k].n, rb: muts[k].rb }));
        grid.appendChild(chartCard('Mutations by store / entity', 'Committed versus rolled back', (w) => mutationChart(w, mrows),
          h('div', { class: 'legend' }, h('span', null, h('i', { class: 'b-ok' }), 'committed or unresolved'), h('span', null, h('i', { class: 'hatch' }), 'rolled back (hatched)'))));
      }
      if (mk.length) {
        const tb = h('tbody');
        mk.forEach((k) => tb.appendChild(h('tr', null, h('td', { text: k }), h('td', { class: 'num', text: muts[k].n }),
          h('td', null, Object.keys(muts[k].ops).sort().map((o) => [opTag(o), span('mono', '×' + muts[k].ops[o] + ' ')])),
          h('td', null, muts[k].rb ? tag(muts[k].rb + ' rolled back', 't-warn', null, 'retry') : null))));
        box.appendChild(h('div', { class: 'section' }, h('h2', { text: 'Mutation breakdown' }),
          h('div', { class: 'card tablewrap' }, h('table', { class: 'dtable' }, h('thead', null, h('tr', null, [['Store / entity', ''], ['Mutations', 'num'], ['Operations', ''], ['', '']].map((x) => h('th', { class: x[1], text: x[0] })))), tb))));
      } else box.appendChild(h('div', { class: 'section' }, h('h2', { text: 'Mutations' }), h('div', { class: 'card' }, emptyState('events', 'No mutations recorded', 'No database or cache writes were captured in this session.'))));
      const fsec = h('div', { class: 'section' }, h('h2', { text: 'Failures (' + fails.length + ')' }));
      if (!fails.length) fsec.appendChild(h('div', { class: 'note', text: 'No failed spans.' }));
      else {
        const tb = h('tbody');
        fails.forEach((e) => tb.appendChild(h('tr', { class: 'row', tabindex: 0, onclick: () => openDrawer(D, e, { focus: false }), onkeydown: (ev) => { if (ev.key === 'Enter') openDrawer(D, e, { focus: true }); } },
          h('td', { class: 'num mono', text: '#' + e.seq }), h('td', null, span('svc', e.service)), h('td', null, kchip(e.kind, true)), h('td', { class: 'wrap', text: e.name }), h('td', { class: 'wrap mono', text: e.err }))));
        fsec.appendChild(h('div', { class: 'card tablewrap' }, h('table', { class: 'dtable' }, h('thead', null, h('tr', null, ['Seq', 'Service', 'Kind', 'Name', 'Error'].map((x) => h('th', { text: x })))), tb)));
      }
      box.appendChild(fsec);
      const asec = h('div', { class: 'section' }, h('h2', { text: (isLive(s) ? 'Pending' : 'Orphaned') + ' async work (' + pendN + ')' }));
      if (!pending.length) asec.appendChild(h('div', { class: 'note', text: 'Every queue.publish has a matching worker.end.' }));
      else asec.appendChild(h('div', { class: 'card pad' }, pending.map((e) => h('div', { class: 'flag-row' }, isLive(s) ? tag('pending', 't-warn', null, 'clock') : tag('orphaned', 't-err', null, 'warn'),
        h('button', { type: 'button', class: 'linkbtn', text: '#' + e.seq + ' ' + e.name, onclick: () => openDrawer(D, e, { focus: false }) })))));
      box.appendChild(asec);
      const fk = Object.keys(flagsC);
      if (fk.length) box.appendChild(h('div', { class: 'section' }, h('h2', { text: 'Flags' }), h('div', { class: 'pv-meta' }, fk.sort().map((f) => tag(f + ' ×' + flagsC[f], FLAG_CLS[f] || 't-warn', FLAG_HELP[f])))));
      if (D.srv) box.appendChild(h('div', { class: 'section' }, h('details', { class: 'more' }, h('summary', { text: 'Raw collector summary' }), jsonViewer(srv, 'collector summary'))));
      drawCharts();
    }
    S.onResize = () => { if (charts.length) drawCharts(); };
    return { async update(changed) {
      if (changed || D.dirty || !D.srv) {
        D.dirty = false;
        try { D.srv = await col('/v1/sessions/' + encodeURIComponent(D.id) + '/summary'); } catch (e) { D.srv = D.srv || null; }
      }
      if (D.token === S.token) paint();
    } };
  }

  // ---------- keyboard ----------
  document.addEventListener('keydown', (e) => {
    if (e.defaultPrevented || e.ctrlKey || e.metaKey || e.altKey) return;
    const t = e.target, tn = t && t.tagName;
    const editable = !!t && (tn === 'INPUT' || tn === 'TEXTAREA' || tn === 'SELECT' || t.isContentEditable);
    if (e.key === 'Escape') {
      if (document.querySelector('dialog[open]')) return;
      if (editable) { t.blur(); return; }
      closeDrawer();
      return;
    }
    if (editable || document.querySelector('dialog[open]')) return;
    const cl = t && t.classList;
    const atRoot = t === document.body || t === document.getElementById('main') || (cl && cl.contains('navable'));
    const inList = atRoot || (t && t.matches && t.matches('tr.row, .elist-item'));
    if (e.key === '/' && S.search) { e.preventDefault(); S.search.focus(); S.search.select(); }
    else if (e.key === '?') { e.preventDefault(); openShortcuts(); }
    else if ((e.key === 'j' || (e.key === 'ArrowDown' && inList)) && S.nav) { e.preventDefault(); S.nav(1); }
    else if ((e.key === 'k' || (e.key === 'ArrowUp' && inList)) && S.nav) { e.preventDefault(); S.nav(-1); }
    else if (e.key === 'Enter' && atRoot && S.activate) { e.preventDefault(); S.activate(); }
    else if (S.keys && S.keys[e.key] && !e.shiftKey && e.key.length === 1) { e.preventDefault(); S.keys[e.key](); }
    else if (/^[1-4]$/.test(e.key) && S.D && S.lateral && !e.shiftKey) { location.hash = sessionHref(S.D.id, TAB_IDS[Number(e.key) - 1]); }
  });
  let resizeT = 0;
  window.addEventListener('resize', () => { clearTimeout(resizeT); resizeT = setTimeout(() => { if (S.onResize) S.onResize(); }, 120); });

  // ---------- boot ----------
  document.getElementById('theme-btn').addEventListener('click', toggleTheme);
  document.getElementById('kbd-btn').addEventListener('click', openShortcuts);
  paintThemeBtn();
  paintConn();
  fetch('/healthz', { cache: 'no-store' }).then((r) => r.json()).then((j) => {
    document.getElementById('mode-note').textContent = j.demo ? 'demo mode · synthetic data' : '';
  }).catch(() => {});
  route();
})();
