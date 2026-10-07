/* Lighter BTC Scalper control panel. Plain JavaScript, no dependencies, no inline script. */
'use strict';

const $ = (id) => document.getElementById(id);
const SVG_NS = 'http://www.w3.org/2000/svg';

const store = {
  csrf: null,
  tab: 'overview',
  overview: null,
  settings: null,
  dirty: {},
  curve: [],
  tick: 0,
  timer: null,
  hoverIndex: null,
};

const BLOCK_TEXT = {
  PAUSED: 'Paused from this panel',
  SYNCING: 'Reconciling with the exchange',
  LEVERAGE_UNCONFIRMED: 'Leverage not confirmed on the exchange',
  ACCOUNT_STREAM_DOWN: 'Account stream not connected',
  MARKET_DATA_STALE: 'Market data is stale',
  FOREIGN_ORDERS: 'A BTC order the bot did not create is resting',
  LOOP_STALL: 'The server was briefly overloaded',
  MARKET_NOT_TRADEABLE: 'The BTC market is not tradeable right now',
  SHUTDOWN: 'The bot is shutting down',
  HALTED: 'Trading is halted',
};

const LATENCY_ROWS = [
  ['signal_to_send_ms', 'Signal to order sent'],
  ['send_to_ack_ms', 'Order sent to accepted'],
  ['send_to_fill_ms', 'Order sent to filled'],
  ['profit_to_exit_send_ms', 'Green to exit sent'],
  ['exit_send_to_flat_ms', 'Exit sent to flat'],
  ['api_read_ms', 'API read'],
  ['ws_ping_market_ms', 'WebSocket ping'],
];

/* ------------------------------------------------------------------ helpers */

class ApiError extends Error {
  constructor(message, status, payload) {
    super(message);
    this.status = status;
    this.payload = payload;
  }
}

async function api(path, options = {}) {
  const init = { method: options.method || 'GET', headers: {}, credentials: 'same-origin' };
  if (options.body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(options.body);
  }
  if (init.method !== 'GET' && store.csrf) init.headers['X-CSRF-Token'] = store.csrf;
  let response;
  try {
    response = await fetch(path, init);
  } catch (error) {
    throw new ApiError('The control panel server cannot be reached.', 0, null);
  }
  let payload = null;
  try { payload = await response.json(); } catch (error) { payload = null; }
  if (response.status === 401 && !options.isAuth) {
    showAuth();
    throw new ApiError('Not logged in.', 401, payload);
  }
  if (!response.ok) {
    const message = (payload && (payload.error || payload.message)) || `Request failed (${response.status}).`;
    throw new ApiError(message, response.status, payload);
  }
  return payload;
}

function setText(el, text) {
  const value = text === null || text === undefined ? '–' : String(text);
  if (el.textContent !== value) el.textContent = value;
}

function setAttr(el, name, value) {
  if (value === null || value === undefined) {
    if (el.hasAttribute(name)) el.removeAttribute(name);
  } else if (el.getAttribute(name) !== String(value)) {
    el.setAttribute(name, String(value));
  }
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

const isNum = (value) => typeof value === 'number' && Number.isFinite(value);

function usd(value, digits = 4, signed = true) {
  if (!isNum(value)) return '–';
  const sign = value > 0 ? (signed ? '+' : '') : value < 0 ? '−' : '';
  return `${sign}$${Math.abs(value).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
}

function signOf(value) {
  return !isNum(value) || value === 0 ? null : value > 0 ? 'up' : 'down';
}

function price(value) {
  return isNum(value) ? value.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 3 }) : '–';
}

function fixed(value, digits = 1, unit = '') {
  return isNum(value) ? `${value.toFixed(digits)}${unit}` : '–';
}

function duration(ms) {
  if (!isNum(ms)) return '–';
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s`;
}

function clock(iso) {
  return typeof iso === 'string' && iso.length >= 19 ? iso.slice(11, 19) : '–';
}

function utcDate(offsetDays = 0) {
  return new Date(Date.now() + offsetDays * 86400000).toISOString().slice(0, 10);
}

function toast(message, tone = 'info', holdMs = 6000) {
  const node = $('toast');
  node.textContent = message;
  node.dataset.tone = tone;
  node.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { node.hidden = true; }, holdMs);
}

function confirmDialog({ title, body, list, okLabel, danger }) {
  return new Promise((resolve) => {
    const dialog = $('confirm');
    $('confirm-title').textContent = title;
    $('confirm-body').textContent = body;
    const listNode = $('confirm-list');
    listNode.replaceChildren(...(list || []).map((item) => el('li', '', item)));
    listNode.hidden = !(list && list.length);
    const ok = $('confirm-ok');
    ok.textContent = okLabel;
    ok.className = danger ? 'btn btn-danger' : 'btn btn-primary';
    dialog.returnValue = 'cancel';
    dialog.addEventListener('close', () => resolve(dialog.returnValue === 'ok'), { once: true });
    dialog.showModal();
    $('confirm-cancel').focus();
  });
}

/* --------------------------------------------------------------------- auth */

let authMode = 'login';

function showAuth(session) {
  stopPolling();
  $('app').hidden = true;
  $('auth').hidden = false;
  const setup = session ? session.setup_required : authMode === 'setup';
  authMode = setup ? 'setup' : 'login';
  $('auth-intro').textContent = setup
    ? 'First run. Choose a password for this control panel. It can start live trading, so pick one only you know.'
    : 'Enter the control panel password.';
  $('auth-label').textContent = setup ? 'New password (at least 8 characters)' : 'Password';
  $('auth-password').autocomplete = setup ? 'new-password' : 'current-password';
  $('auth-confirm-row').hidden = !setup;
  $('auth-submit').textContent = setup ? 'Set password and continue' : 'Log in';
  $('auth-error').hidden = true;
  $('auth-password').value = '';
  $('auth-confirm').value = '';
  $('auth-password').focus();
}

async function submitAuth(event) {
  event.preventDefault();
  const password = $('auth-password').value;
  const error = $('auth-error');
  error.hidden = true;
  if (authMode === 'setup' && password !== $('auth-confirm').value) {
    error.textContent = 'The two passwords do not match.';
    error.hidden = false;
    return;
  }
  try {
    const session = await api(authMode === 'setup' ? '/api/setup' : '/api/login', {
      method: 'POST', body: { password }, isAuth: true,
    });
    store.csrf = session.csrf;
    enterApp();
  } catch (failure) {
    error.textContent = failure.message;
    error.hidden = false;
  }
}

function enterApp() {
  $('auth').hidden = true;
  $('app').hidden = false;
  selectTab(store.tab);
  startPolling();
}

async function boot() {
  applyTheme(readTheme());
  try {
    const session = await api('/api/session', { isAuth: true });
    if (session.authenticated) {
      store.csrf = session.csrf;
      enterApp();
    } else {
      showAuth(session);
    }
  } catch (failure) {
    showAuth({ setup_required: false });
    $('auth-error').textContent = failure.message;
    $('auth-error').hidden = false;
  }
}

/* -------------------------------------------------------------------- theme */

function readTheme() {
  try { return localStorage.getItem('scalper-theme') || 'auto'; } catch (error) { return 'auto'; }
}

function applyTheme(theme) {
  if (theme === 'auto') delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
  $('btn-theme').textContent = `Theme: ${theme}`;
  try { localStorage.setItem('scalper-theme', theme); } catch (error) { /* private window: keep in memory */ }
  renderChart();
}

function cycleTheme() {
  const order = ['auto', 'light', 'dark'];
  applyTheme(order[(order.indexOf(readThemeAttr()) + 1) % order.length]);
}

function readThemeAttr() {
  return document.documentElement.dataset.theme || 'auto';
}

/* ------------------------------------------------------------------ polling */

function startPolling() {
  stopPolling();
  store.tick = 0;
  poll();
}

function stopPolling() {
  clearTimeout(store.timer);
  store.timer = null;
}

async function poll() {
  const tick = store.tick++;
  try {
    const overview = await api('/api/overview');
    store.overview = overview;
    renderOverview(overview);
    const slow = tick % 5 === 0;
    if (store.tab === 'overview' && slow) await Promise.all([loadCurve(), loadEvents()]);
    if (store.tab === 'trades' && slow) await loadTrades();
    if (store.tab === 'logs' && $('logs-auto').checked && tick % 2 === 0) await loadLogs();
  } catch (failure) {
    if (failure.status === 401) return;
    setText($('updated'), failure.message);
  }
  if (!$('app').hidden) store.timer = setTimeout(poll, document.hidden ? 5000 : 1000);
}

/* ----------------------------------------------------------------- overview */

function pill(id, tone, text) {
  const node = $(id);
  setAttr(node, 'data-tone', tone);
  setText(node.querySelector('.pill-text'), text);
}

const SERVICE_TONE = { running: 'good', starting: 'warn', stopping: 'warn', stopped: 'idle', failed: 'bad' };
const SERVICE_TEXT = { running: 'Running', starting: 'Starting', stopping: 'Stopping', stopped: 'Stopped', failed: 'Failed' };

function stateTone(state, paused) {
  if (!state) return 'idle';
  if (state === 'HALTED') return 'bad';
  if (state === 'RECOVERY' || state === 'SYNCING' || state === 'STARTING' || paused) return 'warn';
  return 'good';
}

function renderOverview(o) {
  const status = o.status;
  const live = o.bot_live;
  const service = o.service.state;

  pill('pill-service', SERVICE_TONE[service] || 'idle', `Service: ${SERVICE_TEXT[service] || service}`);
  const botState = live ? status.state : null;
  const paused = live ? !!(status.control && status.control.paused) : o.paused;
  pill('pill-state', stateTone(botState, paused), botState ? `Bot: ${botState.replace('_', ' ')}${paused ? ' (paused)' : ''}` : 'Bot: not running');
  pill('pill-market', live ? (status.market_ws === 'connected' ? 'good' : 'bad') : 'idle',
    live ? `Market data: ${status.market_ws}` : 'Market data: –');
  pill('pill-account', live ? (status.account_ws === 'connected' ? 'good' : 'bad') : 'idle',
    live ? `Account stream: ${status.account_ws}` : 'Account stream: –');
  setText($('updated'), live ? `Updated ${clock(status.ts)} UTC` : `Panel time ${clock(o.now)} UTC`);

  $('btn-start').disabled = !(service === 'stopped' || service === 'failed');
  $('btn-stop').disabled = !(service === 'running' || service === 'starting');
  $('btn-restart').disabled = service !== 'running';
  const pauseButton = $('btn-pause');
  setText(pauseButton, paused ? 'Resume entries' : 'Pause entries');
  setAttr(pauseButton, 'data-active', paused ? 'true' : null);

  renderBanners(o, live, paused);
  renderNumbers(live ? status.metrics : (o.last_status && o.last_status.metrics), live);
  renderPosition(live ? status : null, o);
  renderMarket(live ? status : null);
  renderEntry(live ? status : null, o, paused);
  renderLatency(live ? status.metrics : null);
  renderLimits(live ? status : null);
}

function renderBanners(o, live, paused) {
  const items = [];
  if (!o.config.ready) {
    items.push({ tone: 'warn', title: 'The bot cannot start yet.', list: o.config.blockers, link: 'Open Settings' });
  }
  if (o.service.state === 'failed') items.push({ tone: 'bad', title: 'The service stopped with an error.', text: o.service.detail });
  if (o.service.state === 'starting' || o.service.state === 'stopping') items.push({ tone: 'info', title: o.service.detail });
  if (live && o.status.state === 'HALTED') {
    items.push({ tone: 'bad', title: 'Trading is halted.', text: (o.status.blocks && o.status.blocks.HALTED) || '' });
  }
  if (paused) {
    items.push({
      tone: 'warn',
      title: 'New entries are paused.',
      text: live ? 'An open position is still managed and closed by its rules.' : 'The bot will stay paused when it starts.',
    });
  }
  const key = JSON.stringify(items);
  const host = $('banners');
  if (host.dataset.key === key) return;
  host.dataset.key = key;
  host.replaceChildren(...items.map((item) => {
    const node = el('div', 'banner');
    node.dataset.tone = item.tone;
    const body = el('div');
    body.append(el('strong', '', item.title));
    if (item.text) body.append(document.createTextNode(` ${item.text}`));
    if (item.list && item.list.length) {
      const list = el('ul');
      item.list.forEach((line) => list.append(el('li', '', line)));
      body.append(list);
    }
    if (item.link) {
      const link = el('button', 'link', item.link);
      link.type = 'button';
      link.addEventListener('click', () => selectTab('settings'));
      body.append(link);
    }
    node.append(body);
    return node;
  }));
}

function renderNumbers(metrics, live) {
  const hero = $('hero-value');
  setText($('hero-label'), live ? 'Realized P&L today (UTC)' : 'Realized P&L today (UTC), last known');
  if (!metrics) {
    setText(hero, '–');
    setAttr(hero, 'data-sign', null);
    setText($('hero-sub'), 'No data yet. Numbers appear once the bot has run.');
    ['tile-trades', 'tile-wl', 'tile-winrate', 'tile-fees', 'tile-hold', 'tile-tpm'].forEach((id) => setText($(id), '–'));
    return;
  }
  setText(hero, usd(metrics.realized_pnl_usd, 4));
  setAttr(hero, 'data-sign', signOf(metrics.realized_pnl_usd));
  setText($('hero-sub'), `Gross ${usd(metrics.gross_pnl_usd, 4)}, from confirmed exit fills only`);
  setText($('tile-trades'), metrics.trades_today);
  setText($('tile-wl'), `${metrics.wins} / ${metrics.losses}`);
  setText($('tile-winrate'), isNum(metrics.win_pct) ? `${metrics.win_pct.toFixed(1)}%` : '–');
  setText($('tile-fees'), usd(metrics.fees_usd, 4, false));
  const hold = metrics.latency && metrics.latency.hold_ms;
  setText($('tile-hold'), hold ? duration(hold.p50) : '–');
  setText($('tile-tpm'), metrics.trades_per_minute);
}

function setMeter(id, fraction, level) {
  const meter = $(id);
  const clamped = Math.max(0, Math.min(1, fraction || 0));
  meter.querySelector('.meter-fill').style.width = `${(clamped * 100).toFixed(1)}%`;
  setAttr(meter, 'data-level', level || null);
}

function renderPosition(status, o) {
  const position = status && status.position;
  $('position-open').hidden = !position;
  $('position-flat').hidden = !!position;
  if (!position) {
    setText($('position-flat-title'), status ? 'Flat' : 'Not running');
    let sub = 'No open position.';
    if (!status) sub = 'The bot is not running, so its position view is unavailable.';
    else if (isNum(status.exchange_position) && status.exchange_position !== 0) {
      sub = `The exchange reports ${status.exchange_position} BTC that the bot is not managing.`;
    }
    setText($('position-flat-sub'), sub);
    return;
  }
  const chip = $('position-side');
  setText(chip, position.side);
  setAttr(chip, 'data-side', position.side);
  setText($('position-size'), `${position.size} BTC`);
  $('position-adopted').hidden = !position.adopted;
  setText($('position-entry'), price(position.entry));
  setText($('position-exit'), price(position.executable_exit));
  const pnl = $('position-pnl');
  setText(pnl, isNum(position.estimated_pnl_usd) ? usd(position.estimated_pnl_usd, 4) : 'not fully closeable now');
  setAttr(pnl, 'data-sign', signOf(position.estimated_pnl_usd));
  const max = status.limits ? status.limits.max_hold_ms : null;
  setText($('position-hold-text'), max ? `${duration(position.hold_ms)} of ${duration(max)}` : duration(position.hold_ms));
  const used = max ? position.hold_ms / max : 0;
  setMeter('position-hold-meter', used, used >= 0.9 ? 'bad' : used >= 0.6 ? 'warn' : null);
}

function renderMarket(status) {
  setText($('market-bid'), status ? price(status.bid) : '–');
  setText($('market-ask'), status ? price(status.ask) : '–');
  setText($('market-spread'), status && isNum(status.spread_bps) ? `${status.spread_bps.toFixed(3)} bps` : '–');
  setText($('market-age'), status && isNum(status.book_age_ms) ? `${Math.round(status.book_age_ms)} ms` : '–');
  setText($('market-lag'), status && isNum(status.feed_lag_ms) ? `${status.feed_lag_ms} ms` : '–');
  setText($('market-balance'), status && isNum(status.available_usd) ? usd(status.available_usd, 2, false) : '–');

  const score = status && isNum(status.signal_score) ? Math.max(-1, Math.min(1, status.signal_score)) : null;
  const threshold = status && status.limits ? status.limits.entry_score_threshold : null;
  const fill = $('signal-fill');
  if (score === null) {
    fill.style.width = '0';
    setText($('signal-text'), '–');
  } else {
    const half = Math.abs(score) * 50;
    fill.style.width = `${half.toFixed(1)}%`;
    fill.style.left = score >= 0 ? '50%' : `${(50 - half).toFixed(1)}%`;
    setAttr(fill, 'data-side', score >= 0 ? 'long' : 'short');
    const direction = score > 0 ? 'toward long' : score < 0 ? 'toward short' : 'neutral';
    setText($('signal-text'), `${score >= 0 ? '+' : '−'}${Math.abs(score).toFixed(2)} ${direction}${isNum(threshold) ? `, enters at ±${threshold.toFixed(2)}` : ''}`);
  }
  const low = $('signal-tick-low');
  const high = $('signal-tick-high');
  low.hidden = high.hidden = !isNum(threshold);
  if (isNum(threshold)) {
    low.style.left = `${(50 - threshold * 50).toFixed(1)}%`;
    high.style.left = `${(50 + threshold * 50).toFixed(1)}%`;
  }
}

function renderEntry(status, o, paused) {
  const head = $('entry-status');
  const list = $('blocks');
  const blocks = status ? Object.entries(status.blocks || {}) : [];
  let tone = 'idle';
  let text = 'The bot is not running.';
  if (status) {
    if (status.position) { tone = 'good'; text = 'Managing an open position.'; }
    else if (blocks.length) { tone = status.state === 'HALTED' ? 'bad' : 'warn'; text = 'Entries are blocked.'; }
    else if (status.state === 'FLAT') { tone = 'good'; text = 'Ready. Waiting for a qualifying signal.'; }
    else { tone = 'warn'; text = `Busy: ${status.state.replace('_', ' ').toLowerCase()}.`; }
  } else if (o.service.state === 'starting') { tone = 'warn'; text = 'Starting up.'; }
  setText(head, text);
  setAttr(head, 'data-tone', tone);

  const key = JSON.stringify(blocks);
  if (list.dataset.key !== key) {
    list.dataset.key = key;
    list.replaceChildren(...blocks.map(([name, detail]) => {
      const item = el('li');
      item.append(el('div', 'block-name', BLOCK_TEXT[name] || name), el('div', 'block-detail', detail));
      return item;
    }));
  }

  const skips = status && status.metrics ? Object.entries(status.metrics.skips || {}).sort((a, b) => b[1] - a[1]) : [];
  $('skips-wrap').hidden = !skips.length;
  const skipKey = JSON.stringify(skips);
  const skipList = $('skips');
  if (skipList.dataset.key !== skipKey) {
    skipList.dataset.key = skipKey;
    skipList.replaceChildren(...skips.slice(0, 6).map(([reason, count]) => {
      const item = el('li');
      item.append(el('span', '', reason.replace(/_/g, ' ').toLowerCase()), el('span', 'num', count.toLocaleString('en-US')));
      return item;
    }));
  }
}

function renderLatency(metrics) {
  const body = $('latency');
  if (!body.children.length) {
    LATENCY_ROWS.forEach(([key, label]) => {
      const row = el('tr');
      row.dataset.key = key;
      row.append(el('th', '', label), el('td', 'num'), el('td', 'num'), el('td', 'num'));
      row.firstChild.scope = 'row';
      body.append(row);
    });
  }
  Array.from(body.children).forEach((row) => {
    const stat = metrics && metrics.latency ? metrics.latency[row.dataset.key] : null;
    ['last', 'p50', 'p95'].forEach((field, index) => {
      setText(row.children[index + 1], stat && isNum(stat[field]) ? stat[field].toFixed(1) : '–');
    });
  });
}

function renderLimits(status) {
  const limit = status && status.rate_limit;
  if (limit && isNum(limit.request_capacity) && limit.request_capacity > 0) {
    const used = 1 - limit.request_headroom / limit.request_capacity;
    setMeter('limit-meter', used, used >= 0.85 ? 'bad' : used >= 0.6 ? 'warn' : null);
    setText($('limit-text'), `${(limit.request_capacity - limit.request_headroom).toLocaleString('en-US')} of ${limit.request_capacity.toLocaleString('en-US')} per minute`);
    setText($('limit-tier'), limit.tier);
    const cap = status.limits ? status.limits.max_entries_per_minute : null;
    setText($('limit-entries'), isNum(cap) ? `${limit.entries_last_minute} of ${cap}` : limit.entries_last_minute);
  } else {
    setMeter('limit-meter', 0, null);
    ['limit-text', 'limit-tier', 'limit-entries'].forEach((id) => setText($(id), '–'));
  }
  setText($('limit-leverage'), status ? `${status.leverage}x${status.leverage_confirmed ? '' : ' (not confirmed)'}` : '–');
  setText($('limit-notional'), status && status.limits ? usd(status.limits.notional_usd, 2, false) : '–');
}

async function loadEvents() {
  const { events } = await api('/api/events?limit=8');
  const list = $('events');
  const key = JSON.stringify(events);
  if (list.dataset.key === key) return;
  list.dataset.key = key;
  if (!events.length) {
    list.replaceChildren(el('li', 'muted', 'No incidents recorded.'));
    return;
  }
  list.replaceChildren(...events.map((event) => {
    const item = el('li');
    const text = el('span');
    text.append(el('span', 'event-kind', event.kind.replace(/_/g, ' ').toLowerCase()));
    if (event.summary) text.append(el('span', 'event-summary', ` ${event.summary.replace(/_/g, ' ').toLowerCase()}`));
    const time = el('time', '', `${event.ts.slice(5, 10)} ${clock(event.ts)}`);
    item.append(text, time);
    return item;
  }));
}

/* -------------------------------------------------------------------- chart */

async function loadCurve() {
  const { points } = await api(`/api/pnl?date=${utcDate()}`);
  store.curve = points;
  renderChart();
}

function niceStep(span, target) {
  const raw = span / target;
  const power = Math.pow(10, Math.floor(Math.log10(raw)));
  const unit = raw / power;
  return (unit >= 5 ? 10 : unit >= 2 ? 5 : unit >= 1 ? 2 : 1) * power;
}

function svg(tag, attrs, text) {
  const node = document.createElementNS(SVG_NS, tag);
  Object.entries(attrs || {}).forEach(([name, value]) => node.setAttribute(name, value));
  if (text !== undefined) node.textContent = text;
  return node;
}

function chartGeometry() {
  const host = $('chart');
  const points = store.curve;
  const width = Math.max(320, host.clientWidth);
  const height = 240;
  const pad = { top: 16, right: 76, bottom: 26, left: 64 };
  const values = points.map((point) => point.cum);
  let low = Math.min(0, ...values);
  let high = Math.max(0, ...values);
  if (high === low) { high += 0.01; low -= 0.01; }
  const step = niceStep(high - low, 4);
  low = Math.floor(low / step) * step;
  high = Math.ceil(high / step) * step;
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  // X is the trade sequence: trades cluster in time, so even spacing keeps every one readable.
  const x = (index) => pad.left + (points.length <= 1 ? plotWidth : (index / (points.length - 1)) * plotWidth);
  const y = (value) => pad.top + (1 - (value - low) / (high - low)) * plotHeight;
  return { width, height, pad, low, high, step, x, y, plotWidth };
}

function renderChart() {
  const host = $('chart');
  if (!host || $('app').hidden) return;
  const points = store.curve;
  const old = host.querySelector('svg');
  if (old) old.remove();
  $('chart-empty').hidden = points.length > 0;
  $('chart-tip').hidden = true;
  if (!points.length) return;

  const g = chartGeometry();
  const root = svg('svg', { viewBox: `0 0 ${g.width} ${g.height}`, 'aria-hidden': 'true' });
  const digits = g.step < 0.01 ? 4 : g.step < 1 ? 2 : 0;
  for (let value = g.low; value <= g.high + g.step / 2; value += g.step) {
    const rounded = Math.abs(value) < g.step / 1000 ? 0 : value;
    const yy = g.y(rounded);
    root.append(svg('line', { class: rounded === 0 ? 'zero-line' : 'grid-line', x1: g.pad.left, x2: g.width - g.pad.right, y1: yy, y2: yy }));
    root.append(svg('text', { class: 'axis-text', x: g.pad.left - 8, y: yy + 4, 'text-anchor': 'end' }, usd(rounded, digits, false)));
  }
  const coords = points.map((point, index) => [g.x(index), g.y(point.cum)]);
  const line = coords.map(([px, py], index) => `${index ? 'L' : 'M'}${px.toFixed(1)},${py.toFixed(1)}`).join(' ');
  const zero = g.y(0);
  root.append(svg('path', { class: 'series-area', d: `M${coords[0][0].toFixed(1)},${zero.toFixed(1)} ${line.replace('M', 'L')} L${coords[coords.length - 1][0].toFixed(1)},${zero.toFixed(1)} Z` }));
  root.append(svg('path', { class: 'series-line', d: line }));
  const [endX, endY] = coords[coords.length - 1];
  root.append(svg('circle', { class: 'end-dot', cx: endX, cy: endY, r: 5 }));
  root.append(svg('text', { class: 'end-label', x: endX + 10, y: endY + 4 }, usd(points[points.length - 1].cum, 4)));
  root.append(svg('text', { class: 'axis-text', x: g.pad.left, y: g.height - 6 }, `first trade ${clock(points[0].t)}`));
  root.append(svg('text', { class: 'axis-text', x: g.width - g.pad.right, y: g.height - 6, 'text-anchor': 'end' }, `last trade ${clock(points[points.length - 1].t)} UTC`));
  const crosshair = svg('line', { class: 'crosshair', y1: g.pad.top, y2: g.height - g.pad.bottom, visibility: 'hidden' });
  const marker = svg('circle', { class: 'end-dot', r: 5, visibility: 'hidden' });
  root.append(crosshair, marker);
  host.prepend(root);
  store.chart = { g, coords, crosshair, marker };
  if (store.hoverIndex !== null) showPoint(Math.min(store.hoverIndex, points.length - 1));
}

function showPoint(index) {
  const chart = store.chart;
  const points = store.curve;
  if (!chart || !points.length) return;
  store.hoverIndex = index;
  const [px, py] = chart.coords[index];
  chart.crosshair.setAttribute('x1', px);
  chart.crosshair.setAttribute('x2', px);
  chart.crosshair.setAttribute('visibility', 'visible');
  chart.marker.setAttribute('cx', px);
  chart.marker.setAttribute('cy', py);
  chart.marker.setAttribute('visibility', 'visible');
  const point = points[index];
  const tip = $('chart-tip');
  const value = el('div', 'tip-value');
  value.append(el('span', 'tip-key'), document.createTextNode(usd(point.cum, 4)));
  tip.replaceChildren(value, el('div', 'tip-meta', `after trade ${point.n} (${usd(point.net, 4)}) at ${clock(point.t)} UTC`));
  tip.hidden = false;
  const host = $('chart');
  const scale = host.clientWidth / chart.g.width;
  const left = px * scale + 12;
  tip.style.left = `${Math.min(left, host.clientWidth - tip.offsetWidth - 4)}px`;
}

function hidePoint() {
  store.hoverIndex = null;
  if (store.chart) {
    store.chart.crosshair.setAttribute('visibility', 'hidden');
    store.chart.marker.setAttribute('visibility', 'hidden');
  }
  $('chart-tip').hidden = true;
}

function chartPointer(event) {
  const chart = store.chart;
  if (!chart || !store.curve.length) return;
  const host = $('chart');
  const bounds = host.getBoundingClientRect();
  const px = ((event.clientX - bounds.left) / bounds.width) * chart.g.width;
  let best = 0;
  chart.coords.forEach(([cx], index) => { if (Math.abs(cx - px) < Math.abs(chart.coords[best][0] - px)) best = index; });
  showPoint(best);
}

function chartKeys(event) {
  const count = store.curve.length;
  if (!count) return;
  const current = store.hoverIndex === null ? count - 1 : store.hoverIndex;
  if (event.key === 'ArrowLeft') showPoint(Math.max(0, current - 1));
  else if (event.key === 'ArrowRight') showPoint(Math.min(count - 1, current + 1));
  else if (event.key === 'Home') showPoint(0);
  else if (event.key === 'End') showPoint(count - 1);
  else if (event.key === 'Escape') hidePoint();
  else return;
  event.preventDefault();
}

/* ------------------------------------------------------------------- trades */

function cell(text, className, sign) {
  const node = el('td', className || '', text);
  if (sign) node.dataset.sign = sign;
  return node;
}

async function loadTrades() {
  const choice = $('trades-date').value;
  const date = choice === 'today' ? utcDate() : choice === 'yesterday' ? utcDate(-1) : 'all';
  const [{ trades }, { days }] = await Promise.all([api(`/api/trades?date=${date}&limit=200`), api('/api/daily')]);
  const body = $('trades');
  const key = JSON.stringify(trades.map((trade) => trade.trade_id));
  if (body.dataset.key !== key) {
    body.dataset.key = key;
    body.replaceChildren(...trades.map((trade) => {
      const row = el('tr');
      row.append(
        cell(`${(trade.closed_at || '').slice(0, 10)} ${clock(trade.closed_at)}`),
        cell(trade.side + (trade.adopted ? ' (adopted)' : '')),
        cell(trade.size, 'num'),
        cell(price(trade.avg_entry), 'num'),
        cell(price(trade.avg_exit), 'num'),
        cell(duration(trade.holding_ms), 'num'),
        cell(usd(trade.gross_pnl_usd, 4), 'num'),
        cell(usd(trade.fees_usd, 4, false), 'num'),
        cell(usd(trade.realized_pnl_usd, 4), 'num', signOf(trade.realized_pnl_usd)),
        cell((trade.exit_reason || '').replace(/_/g, ' ').toLowerCase()),
      );
      return row;
    }));
  }
  $('trades-empty').hidden = trades.length > 0;
  const net = trades.reduce((sum, trade) => sum + (trade.realized_pnl_usd || 0), 0);
  setText($('trades-summary'), trades.length ? `${trades.length} trade${trades.length === 1 ? '' : 's'}, net ${usd(net, 4)}` : '');

  const daily = $('daily');
  const dailyKey = JSON.stringify(days);
  if (daily.dataset.key !== dailyKey) {
    daily.dataset.key = dailyKey;
    daily.replaceChildren(...days.map((day) => {
      const row = el('tr');
      row.append(
        cell(day.date),
        cell(day.trades, 'num'),
        cell(day.wins, 'num'),
        cell(day.losses, 'num'),
        cell(isNum(day.win_pct) ? `${day.win_pct.toFixed(1)}%` : '–', 'num'),
        cell(usd(day.fees_usd, 4, false), 'num'),
        cell(usd(day.net_pnl_usd, 4), 'num', signOf(day.net_pnl_usd)),
        cell(usd(day.best_usd, 4), 'num'),
        cell(usd(day.worst_usd, 4), 'num'),
        cell(duration(day.avg_hold_ms), 'num'),
      );
      return row;
    }));
  }
}

/* ----------------------------------------------------------------- settings */

function fieldInput(field, value) {
  const wrap = el('div', 'setting');
  wrap.dataset.key = field.key;
  let input;
  if (field.kind === 'bool' || field.kind === 'gate') {
    const label = el('label', 'setting-toggle');
    input = el('input');
    input.type = 'checkbox';
    input.checked = !!value;
    label.append(input, el('span', 'setting-label', field.label));
    wrap.append(label);
  } else {
    const id = `setting-${field.key}`;
    const label = el('label', 'setting-label', field.label);
    label.htmlFor = id;
    const row = el('div', 'setting-input');
    if (field.kind === 'choice') {
      input = el('select');
      field.choices.forEach((choice) => {
        const option = el('option', '', choice);
        option.value = choice;
        input.append(option);
      });
      const current = String(value || '');
      const match = field.choices.find((choice) => choice.toLowerCase() === current.toLowerCase());
      input.value = match || field.choices[0];
    } else {
      input = el('input');
      input.type = field.kind === 'secret' ? 'password' : 'text';
      input.inputMode = field.kind === 'int' ? 'numeric' : field.kind === 'number' ? 'decimal' : 'text';
      input.autocomplete = field.kind === 'secret' ? 'new-password' : 'off';
      input.spellcheck = false;
      if (field.kind === 'secret') input.placeholder = value ? 'Stored. Type a new key to replace it.' : 'Not set';
      else {
        input.value = value || '';
        if (field.placeholder) input.placeholder = `default ${field.placeholder}`;
      }
    }
    input.id = id;
    row.append(input);
    if (field.unit) row.append(el('span', 'setting-unit', field.unit));
    wrap.append(label, row);
  }
  if (field.help) wrap.append(el('div', 'setting-help', field.help));
  const initial = input.type === 'checkbox' ? input.checked : input.value;
  const onChange = () => {
    const now = input.type === 'checkbox' ? input.checked : input.value.trim();
    const changed = field.kind === 'secret' ? now !== '' : now !== initial;
    if (changed) store.dirty[field.key] = now; else delete store.dirty[field.key];
    wrap.dataset.dirty = changed ? 'true' : 'false';
    updateSaveBar();
  };
  input.addEventListener('input', onChange);
  input.addEventListener('change', onChange);
  return wrap;
}

function updateSaveBar() {
  const count = Object.keys(store.dirty).length;
  $('settings-save').disabled = count === 0;
  $('settings-reset').disabled = count === 0;
  const running = store.settings && store.settings.restart_needed;
  setText($('settings-note'), count
    ? `${count} unsaved change${count === 1 ? '' : 's'}.`
    : running ? 'The bot is running: saved changes apply after a restart.' : 'Changes take effect the next time the bot starts.');
}

function renderCheck(check, saved) {
  const host = $('settings-check');
  const title = el('h2', 'card-title', check.ready ? 'Ready to start' : 'Not ready to start');
  const nodes = [title];
  if (check.ready) nodes.push(el('p', 'muted', 'The configuration is complete and valid. Starting is still your decision.'));
  else {
    nodes.push(el('p', 'muted', 'These must be fixed before the bot will start:'));
    const list = el('ul', 'check-list');
    check.start_blockers.forEach((line) => list.append(el('li', '', line)));
    nodes.push(list);
  }
  if (saved && saved.length) nodes.push(el('p', 'muted', `Saved: ${saved.join(', ')}.`));
  host.replaceChildren(...nodes);
}

function renderSettings(payload, saved) {
  store.settings = payload;
  store.dirty = {};
  renderCheck(payload.check, saved);
  const form = $('settings-form');
  form.replaceChildren(...payload.groups.map((group) => {
    const card = el(group.advanced ? 'details' : 'article', 'card settings-group');
    if (group.advanced) card.append(el('summary', '', group.title));
    else card.append(el('h2', 'card-title', group.title));
    card.append(el('p', 'settings-desc', group.description));
    const fields = el('div', 'settings-fields');
    group.fields.forEach((field) => fields.append(fieldInput(field, payload.values[field.key])));
    card.append(fields);
    return card;
  }));
  updateSaveBar();
}

async function loadSettings() {
  if (Object.keys(store.dirty).length) return;
  renderSettings(await api('/api/settings'));
}

async function saveSettings() {
  const values = { ...store.dirty };
  try {
    const payload = await api('/api/settings', { method: 'POST', body: { values } });
    renderSettings(payload, payload.saved);
    toast(payload.restart_needed ? 'Saved. Restart the bot to apply the changes.' : 'Saved.');
  } catch (failure) {
    const errors = failure.payload && failure.payload.errors;
    toast(errors && errors.length ? `Not saved:\n${errors.join('\n')}` : failure.message, 'bad', 12000);
  }
}

async function changePassword(event) {
  event.preventDefault();
  try {
    const result = await api('/api/password', { method: 'POST', body: { current: $('pw-current').value, new: $('pw-new').value } });
    toast(result.message);
    showAuth({ setup_required: false });
  } catch (failure) {
    toast(failure.message, 'bad');
  }
}

/* --------------------------------------------------------------------- logs */

function logLevel(line) {
  if (/ (ERROR|CRITICAL) /.test(line)) return 'bad';
  if (/ WARNING /.test(line)) return 'warn';
  return 'info';
}

async function loadLogs() {
  const { lines } = await api(`/api/logs?lines=${$('logs-lines').value}`);
  const onlyWarnings = $('logs-warn').checked;
  const shown = onlyWarnings ? lines.filter((line) => logLevel(line) !== 'info') : lines;
  const host = $('logs');
  const key = `${shown.length}:${shown[shown.length - 1] || ''}:${onlyWarnings}`;
  if (host.dataset.key === key) return;
  host.dataset.key = key;
  const atBottom = host.scrollHeight - host.scrollTop - host.clientHeight < 40;
  host.replaceChildren(...shown.map((line) => {
    const row = el('div', 'log-line', line);
    row.dataset.level = logLevel(line);
    return row;
  }));
  if (!shown.length) host.append(el('div', 'log-line', lines.length ? 'No warnings or errors in the last lines.' : 'No log yet. The log appears once the bot has started.'));
  if (atBottom || !host.dataset.scrolled) host.scrollTop = host.scrollHeight;
  host.dataset.scrolled = 'true';
  setText($('logs-count'), `${shown.length} line${shown.length === 1 ? '' : 's'}`);
}

/* --------------------------------------------------------------------- tabs */

function selectTab(name) {
  store.tab = name;
  document.querySelectorAll('.tab').forEach((tab) => tab.setAttribute('aria-selected', String(tab.dataset.tab === name)));
  ['overview', 'trades', 'settings', 'logs'].forEach((tab) => { $(`tab-${tab}`).hidden = tab !== name; });
  const load = { overview: () => Promise.all([loadCurve(), loadEvents()]), trades: loadTrades, settings: loadSettings, logs: loadLogs }[name];
  load().catch((failure) => { if (failure.status !== 401) toast(failure.message, 'bad'); });
}

/* ------------------------------------------------------------------ actions */

async function act(path, successTone = 'info') {
  try {
    const result = await api(path, { method: 'POST', body: {} });
    toast(result.message, successTone, 8000);
  } catch (failure) {
    const blockers = failure.payload && failure.payload.blockers;
    toast(blockers && blockers.length ? `${failure.message}\n${blockers.join('\n')}` : failure.message, 'bad', 12000);
  }
  startPolling(); // refresh right away so the buttons and pills reflect the action
}

async function startBot() {
  const o = store.overview;
  if (o && !o.config.ready) {
    toast(`The bot cannot start yet:\n${o.config.blockers.join('\n')}`, 'bad', 12000);
    selectTab('settings');
    return;
  }
  let list = [];
  try {
    const { values } = await api('/api/settings');
    const size = values.POSITION_MODE === 'fixed_notional'
      ? `$${values.NOTIONAL_PER_TRADE_USD} notional per trade`
      : `$${values.MARGIN_PER_TRADE_USD} margin per trade`;
    list = [`Account index ${values.LIGHTER_ACCOUNT_INDEX}`, `${values.LEVERAGE}x leverage, ${size}`,
      `Max loss $${values.MAX_LOSS_USD || '–'} or ${values.MAX_ADVERSE_MOVE_BPS || '–'} bps, max hold ${values.MAX_HOLD_MS} ms`];
  } catch (failure) { /* the confirmation still works without the summary */ }
  if (o && o.paused) list.push('Entries are paused: it will start paused.');
  const ok = await confirmDialog({
    title: 'Start live trading?',
    body: 'The bot will trade the BTC perpetual on Lighter mainnet with real funds, using the saved settings.',
    list,
    okLabel: 'Start live trading',
  });
  if (ok) await act('/api/service/start');
}

async function stopBot() {
  const ok = await confirmDialog({
    title: 'Stop the bot?',
    body: 'It finishes any order in flight and then closes an open position if the settings say so (the default), before it exits.',
    okLabel: 'Stop',
  });
  if (ok) await act('/api/service/stop');
}

async function restartBot() {
  const ok = await confirmDialog({
    title: 'Restart the bot?',
    body: 'It stops as above, starts again, and reconciles with the exchange before it trades. Saved settings take effect.',
    okLabel: 'Restart',
  });
  if (ok) await act('/api/service/restart');
}

async function togglePause() {
  const o = store.overview;
  const paused = o && (o.bot_live ? o.status.control && o.status.control.paused : o.paused);
  if (!paused) {
    await act('/api/bot/pause');
    return;
  }
  const ok = await confirmDialog({
    title: 'Resume opening positions?',
    body: 'The bot will again enter trades on qualifying signals.',
    okLabel: 'Resume entries',
  });
  if (ok) await act('/api/bot/resume');
}

async function flatten() {
  const ok = await confirmDialog({
    title: 'Flatten now?',
    body: 'Cancels the bot’s BTC orders, closes the BTC position at market with a reduce-only order, and pauses new entries. Entries stay paused until you resume them.',
    okLabel: 'Flatten now',
    danger: true,
  });
  if (ok) await act('/api/bot/flatten');
}

async function logout() {
  try { await api('/api/logout', { method: 'POST', body: {} }); } catch (failure) { /* already logged out */ }
  store.csrf = null;
  showAuth({ setup_required: false });
}

/* --------------------------------------------------------------------- init */

function init() {
  $('auth-form').addEventListener('submit', submitAuth);
  document.querySelectorAll('.tab').forEach((tab) => tab.addEventListener('click', () => selectTab(tab.dataset.tab)));
  $('btn-start').addEventListener('click', startBot);
  $('btn-stop').addEventListener('click', stopBot);
  $('btn-restart').addEventListener('click', restartBot);
  $('btn-pause').addEventListener('click', togglePause);
  $('btn-flatten').addEventListener('click', flatten);
  $('btn-theme').addEventListener('click', cycleTheme);
  $('btn-logout').addEventListener('click', logout);
  $('trades-date').addEventListener('change', () => { $('trades').dataset.key = ''; loadTrades().catch(() => {}); });
  $('chart-table-link').addEventListener('click', () => selectTab('trades'));
  $('settings-save').addEventListener('click', saveSettings);
  $('settings-reset').addEventListener('click', () => { store.dirty = {}; loadSettings().catch(() => {}); });
  $('password-form').addEventListener('submit', changePassword);
  ['logs-warn', 'logs-lines', 'logs-auto'].forEach((id) => $(id).addEventListener('change', () => { $('logs').dataset.key = ''; loadLogs().catch(() => {}); }));
  const chart = $('chart');
  chart.addEventListener('pointermove', chartPointer);
  chart.addEventListener('pointerleave', hidePoint);
  chart.addEventListener('keydown', chartKeys);
  chart.addEventListener('blur', hidePoint);
  let resizeTimer = null;
  window.addEventListener('resize', () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(renderChart, 120); });
  window.addEventListener('beforeunload', (event) => {
    if (Object.keys(store.dirty).length) { event.preventDefault(); event.returnValue = ''; }
  });
  boot();
}

document.addEventListener('DOMContentLoaded', init);
