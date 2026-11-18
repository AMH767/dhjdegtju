#!/usr/bin/env node
'use strict';
/*
 * SCALP-AI · торговый бот 24/7 для GitHub Actions
 * Запускается по cron каждые 5 минут: анализирует топ-20 USDT-перпетуалов Bybit
 * (стакан, дельта, FVG, ликвидность, тренд, новости, вол-радар PUMP/DUMP),
 * ведёт виртуальный счёт 30 → 500 USDT (кросс-маржа, плечо до x10),
 * сохраняет состояние в bot/state.json и дописывает сделки в bot/history.txt,
 * затем коммитит их в репозиторий. Сайт на GitHub Pages читает state.json
 * и показывает статистику, даже когда вкладка закрыта.
 *
 * Зависимостей нет — только встроенный fetch (Node 18+) и git.
 */
const fs = require('fs');
const path = require('path');
const cp = require('child_process');

const DIR = __dirname;
const ROOT = path.join(DIR, '..');
const STATE_FILE = path.join(DIR, 'state.json');
const HIST_FILE = path.join(DIR, 'history.txt');

// ---------- параметры (как на сайте, реальный режим) ----------
const GOAL = 500, START_BAL = 30;
const FEE = 0.00055;           // taker-комиссия Bybit за сторону
const MMR = 0.005;             // поддерживающая маржа ~0.5%
const LEV_CAP = 10;            // кросс-маржа, макс. плечо одной сделки
const CFG = { threshold: 55, gap: 25, maxSpread: 0.12, obBand: 0.1 };
const MAX_HOLD = 30;           // минут до принудительного закрытия
const COOLDOWN = 300000;       // 5 мин между сигналами одной монеты/стороны
const TOP_N = 20;

// ---------- утилиты ----------
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const now = () => Date.now();
function fp(p) {
  if (p == null || !isFinite(p)) return '—';
  const a = Math.abs(p);
  if (a >= 100) return p.toFixed(2);
  if (a >= 10) return p.toFixed(3);
  if (a >= 1) return p.toFixed(4);
  if (a >= 0.01) return p.toFixed(5);
  return p.toFixed(7);
}
function fusd(v) {
  const a = Math.abs(v); const s = v < 0 ? '-' : '';
  if (a >= 1e9) return s + (a / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return s + (a / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return s + (a / 1e3).toFixed(1) + 'K';
  return s + a.toFixed(0);
}
function fqty(v) {
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  if (v < 0.01) return v.toFixed(4);
  if (v < 1) return v.toFixed(3);
  return v < 10 ? v.toFixed(2) : v.toFixed(0);
}
const pstr = v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
function ts(t) { return new Date(t).toLocaleTimeString('ru-RU', { hour12: false }); }
function log(msg) { console.log('[' + ts(now()) + '] ' + msg); }

const RU_FACT = { ob: 'стакан', delta: 'дельта', fvg: 'FVG', liq: 'ликвидность', trend: 'тренд', news: 'новости', vol: 'волатильность' };

// ---------- API Bybit с фолбэком хоста ----------
const HOSTS = ['https://api.bybit.com', 'https://api.bytick.com'];
let hostIdx = 0;
async function api(pathname) {
  let lastErr = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    const hi = (hostIdx + attempt) % HOSTS.length;
    try {
      const ctl = new AbortController();
      const to = setTimeout(() => ctl.abort(), 8000);
      const res = await fetch(HOSTS[hi] + pathname, { signal: ctl.signal });
      clearTimeout(to);
      const j = await res.json();
      if (j.retCode !== 0) throw new Error(j.retMsg || 'retCode ' + j.retCode);
      hostIdx = hi;
      return j.result;
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error('fetch failed');
}
async function pool(items, fn, lim) {
  lim = lim || 6;
  const q = items.slice(); const ws = [];
  for (let i = 0; i < Math.min(lim, q.length); i++)
    ws.push((async () => { while (q.length) { const it = q.shift(); try { await fn(it); } catch (e) {} } })());
  await Promise.all(ws);
}

// ---------- состояние ----------
function freshState() {
  return {
    version: 1,
    updatedAt: now(), startedAt: now(), lastCommitAt: 0,
    goal: GOAL, start: START_BAL,
    acc: { bal: START_BAL, peak: START_BAL, maxDD: 0, fees: 0, done: false, blown: false,
      risk: 0.01, riskBase: 0.01, streak: 0, eq: [START_BAL], tradesN: 0, winsN: 0 },
    paper: { weights: { ob: 1, delta: 1, fvg: 1, liq: 1, trend: 1, news: 1, vol: 1 }, adj: 0, seq: 0, lastSig: {}, lessons: [] },
    open: [], trades: []
  };
}
let ST = null;
function loadState() {
  try {
    const s = fs.readFileSync(STATE_FILE, 'utf8');
    ST = JSON.parse(s);
    const fresh = freshState();
    // мягкая миграция недостающих полей
    for (const k of Object.keys(fresh)) if (ST[k] === undefined) ST[k] = fresh[k];
    for (const k of Object.keys(fresh.acc)) if (ST.acc[k] === undefined) ST.acc[k] = fresh.acc[k];
    for (const k of Object.keys(fresh.paper)) if (ST.paper[k] === undefined) ST.paper[k] = fresh.paper[k];
    if (!ST.paper.weights.vol) ST.paper.weights.vol = 1;
  } catch (e) {
    ST = freshState();
    log('Состояние не найдено — создаю новый счёт: стартовый депозит ' + START_BAL + ' USDT, цель ' + GOAL + ' USDT');
  }
}
function saveState() {
  ST.updatedAt = now();
  fs.writeFileSync(STATE_FILE, JSON.stringify(ST, null, 1));
}

// ---------- журнал history.txt ----------
function histHeader() {
  return '# SCALP-AI · журнал сделок бота 24/7 (paper) · старт ' + START_BAL + ' USDT · цель ' + GOAL + ' USDT · кросс-маржа, макс. плечо x' + LEV_CAP + '\n';
}
function histLine(tr) {
  const d = new Date(tr.exitT || tr.t);
  const pad = n => (n < 10 ? '0' : '') + n;
  const dt = d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  return dt + ' | #' + tr.id + ' ' + tr.sym + ' ' + tr.side + ' | x' + (tr.lev || 1)
    + ' | вход ' + fp(tr.entry) + ' | выход ' + fp(tr.exit) + ' | ' + tr.outcome
    + ' | R ' + (tr.R >= 0 ? '+' : '') + tr.R.toFixed(2)
    + ' | PnL ' + (tr.pnl$ >= 0 ? '+' : '-') + Math.abs(tr.pnl$ || 0).toFixed(2)
    + ' | объём ' + (tr.notional || 0).toFixed(0)
    + ' | деп ' + (tr.bal != null ? tr.bal.toFixed(2) : ST.acc.bal.toFixed(2));
}
function histAppend(tr) {
  try {
    let txt = fs.readFileSync(HIST_FILE, 'utf8');
    if (!txt.trim()) txt = histHeader();
    if (!txt.endsWith('\n')) txt += '\n';
    txt += histLine(tr) + '\n';
    fs.writeFileSync(HIST_FILE, txt);
  } catch (e) {
    fs.writeFileSync(HIST_FILE, histHeader() + histLine(tr) + '\n');
  }
}

// ----------Risk-менеджмент и счёт ----------
function accRiskPct() {
  const A = ST.acc;
  let r = A.riskBase + Math.min(Math.max(A.streak, 0) * 0.01, 0.03);
  const dd = 1 - A.bal / A.peak;
  if (dd > 0.3) r *= 0.35; else if (dd > 0.15) r *= 0.6;
  return clamp(r, 0.015, 0.07);
}
function accCanOpen() {
  const A = ST.acc;
  if (A.done || A.blown || A.bal < 5) return false;
  if ((A.bal * 0.9 - openMarginUsed()) * LEV_CAP < 8) return false;
  if (ST.open.reduce((a, p) => a + (p.riskPct || 0), 0) >= 0.12) return false;
  return true;
}
function openMarginUsed() { return ST.open.reduce((a, p) => a + (p.margin || 0), 0); }
function accMarkEquity() {
  const A = ST.acc;
  A.eq.push(+A.bal.toFixed(2));
  if (A.eq.length > 400) A.eq.shift();
  if (A.bal > A.peak) A.peak = A.bal;
  const dd = 1 - A.bal / A.peak;
  if (dd > A.maxDD) A.maxDD = dd;
}
function accSize(p) {
  const A = ST.acc;
  const stopPct = p.stopPctN / 100;
  const risk$0 = A.bal * accRiskPct();
  const marginFree = Math.max(0, (A.bal * 0.9 - openMarginUsed()) * LEV_CAP);
  const notional = Math.min(risk$0 / stopPct, marginFree, A.bal * LEV_CAP * 0.85);
  if (notional < 5) return false;
  p.qty = notional / p.entry;
  p.notional = notional;
  p.lev = Math.min(LEV_CAP, Math.max(1, Math.round(notional / Math.max(A.bal * 0.6, 1))));
  p.risk$ = notional * stopPct;
  p.riskPct = p.risk$ / A.bal;
  p.feeOpen = notional * FEE;
  p.margin = notional / p.lev;
  A.bal -= p.feeOpen; A.fees += p.feeOpen;
  return true;
}
function accRealize(p, frac, exit) {
  const dir = p.side === 'LONG' ? 1 : -1;
  const q = (p.qty || 0) * frac;
  const net = q * (exit - p.entry) * dir - q * exit * FEE;
  ST.acc.bal += net; ST.acc.fees += q * exit * FEE;
  p.realized = (p.realized || 0) + net;
  p.qty -= q;
  accMarkEquity();
  return net;
}
function liqPx(p) {
  const lev = p.lev || LEV_CAP;
  return p.side === 'LONG' ? p.entry * (1 - 1 / lev + MMR) : p.entry * (1 + 1 / lev - MMR);
}

// ---------- самообучение (как на сайте) ----------
function learn(tr) {
  const P = ST.paper;
  const strong = [], weak = [];
  for (const k in tr.factors) {
    if (tr.factors[k] >= 0.5) strong.push(k);
    if (tr.factors[k] < 0.2) weak.push(k);
  }
  const y = tr.outcome === 'win' ? 1 : tr.outcome === 'loss' ? -1 : 0;
  for (const k of strong) P.weights[k] = clamp(+(P.weights[k] + y * 0.03).toFixed(3), 0.6, 1.4);
  const last10 = ST.trades.slice(-10);
  const wr = last10.filter(x => x.outcome === 'win').length / Math.max(1, last10.length);
  const target = wr < 0.4 ? 5 : wr >= 0.65 ? -5 : 0;
  P.adj = clamp(+(P.adj * 0.8 + target * 0.2).toFixed(1), -15, 15);
  let msg = null;
  if (tr.outcome === 'loss') {
    msg = '🤓 ' + tr.side + ' ' + tr.sym + ' в минус: опирался на ' + (strong.map(k => RU_FACT[k]).join(', ') || 'ничего сильного')
      + (weak.length ? ', но ' + weak.map(k => RU_FACT[k]).join(', ') + ' не подтверждали' : '')
      + ' — вес' + (strong.length > 1 ? 'ы' : '') + ' скорректирован' + (strong.length > 1 ? 'ы' : '') + ', адапт. порог ' + (P.adj > 0 ? '+' : '') + P.adj;
  } else if (tr.outcome === 'win') {
    msg = '🧠 ' + tr.side + ' ' + tr.sym + ' в плюс: сработали ' + (strong.map(k => RU_FACT[k]).join(', ') || 'слабые факторы') + ' — подкрепляю';
  }
  if (msg) {
    msg += ' (' + (tr.pnl$ >= 0 ? '+' : '−') + '$' + Math.abs(tr.pnl$ || 0).toFixed(2) + ')';
    P.lessons.unshift({ t: tr.exitT, msg: msg });
    if (P.lessons.length > 30) P.lessons.length = 30;
    log('Урок: ' + msg);
  }
}

// ---------- индикаторы ----------
function emaArr(arr, p) {
  const k = 2 / (p + 1), out = new Array(arr.length);
  let e = arr[0];
  for (let i = 0; i < arr.length; i++) { e = i ? arr[i] * k + e * (1 - k) : arr[i]; out[i] = e; }
  return out;
}
function atr(kl, n) {
  const s = kl.slice(-(n || 10));
  let a = 0;
  for (const x of s) a += x.h - x.l;
  return a / (s.length || 1);
}
function swings(kl, k) {
  const out = []; const n = kl.length;
  if (n < 2 * k + 1) return out;
  for (let i = k; i < n - k; i++) {
    let hi = true, lo = true;
    for (let j = 1; j <= k; j++) {
      if (kl[i].h < kl[i - j].h || kl[i].h < kl[i + j].h) hi = false;
      if (kl[i].l > kl[i - j].l || kl[i].l > kl[i + j].l) lo = false;
    }
    if (hi) out.push({ t: 'h', p: kl[i].h, i: i });
    if (lo) out.push({ t: 'l', p: kl[i].l, i: i });
  }
  return out;
}
function findPools(kl) {
  const sw = swings(kl, 2), tol = 0.0015, pools = [];
  for (const tt of ['h', 'l']) {
    const pts = sw.filter(s => s.t === tt).sort((a, b) => a.p - b.p);
    let grp = [];
    const flush = () => { if (grp.length >= 2) pools.push({ t: tt, p: grp.reduce((a, b) => a + b.p, 0) / grp.length, touches: grp.length }); grp = []; };
    for (const s of pts) {
      if (!grp.length || Math.abs(s.p - grp[grp.length - 1].p) / s.p <= tol) grp.push(s);
      else { flush(); grp = [s]; }
    }
    flush();
  }
  return pools.slice(-6);
}
function findFVGs(kl, tf, lookback) {
  const out = []; const n = kl.length;
  if (n < 10) return out;
  lookback = lookback || 70;
  for (let i = Math.max(2, n - lookback); i < n; i++) {
    const h2 = kl[i - 2].h, l2 = kl[i - 2].l, h = kl[i].h, l = kl[i].l;
    if (l > h2) out.push({ dir: 1, top: l, bot: h2, i: i, tf: tf, filled: false });
    else if (l2 > h) out.push({ dir: -1, top: l2, bot: h, i: i, tf: tf, filled: false });
  }
  for (const g of out) for (let j = g.i + 1; j < n; j++) {
    if (g.dir === 1 && kl[j].l <= g.bot) { g.filled = true; break; }
    if (g.dir === -1 && kl[j].h >= g.top) { g.filled = true; break; }
  }
  return out.filter(g => !g.filled).slice(-3);
}

// ---------- анализ монеты ----------
function analyzeOB(c) {
  c.obRes = null;
  if (!c.ob || !c.ob.bids || !c.ob.asks || !c.ob.bids.length || !c.ob.asks.length) return;
  const bids = c.ob.bids.map(x => [+x[0], +x[1]]), asks = c.ob.asks.map(x => [+x[0], +x[1]]);
  const bb = bids[0][0], ba = asks[0][0], mid = (bb + ba) / 2;
  if (!(mid > 0)) return;
  const spreadPct = (ba - bb) / mid * 100;
  const band = mid * CFG.obBand / 100;
  let bv = 0, av = 0, bMax = 0, bMaxP = 0, aMax = 0, aMaxP = 0, bN = 0, aN = 0;
  for (const q of bids) if (q[0] >= mid - band) { bv += q[1]; bN++; if (q[1] > bMax) { bMax = q[1]; bMaxP = q[0]; } }
  for (const q of asks) if (q[0] <= mid + band) { av += q[1]; aN++; if (q[1] > aMax) { aMax = q[1]; aMaxP = q[0]; } }
  const imb = (bv - av) / ((bv + av) || 1);
  let wallBid = null, wallAsk = null;
  if (bN && bMax > bv / bN * 6) wallBid = { p: bMaxP, q: bMax };
  if (aN && aMax > av / aN * 6) wallAsk = { p: aMaxP, q: aMax };
  c.obRes = { mid, spreadPct, imb, bv, av, wallBid, wallAsk };
}
function obPart(c) {
  const o = c.obRes;
  if (!o) return { l: 0, s: 0, notes: [] };
  const notes = [];
  let v = clamp(o.imb / 0.6, -1, 1) * 18, w = 0;
  if (o.wallBid) { w += 6; notes.push('Крупная стена на биде ' + fp(o.wallBid.p) + ' (' + fqty(o.wallBid.q) + ') — поддержка'); }
  if (o.wallAsk) { w -= 6; notes.push('Крупная стена на аске ' + fp(o.wallAsk.p) + ' (' + fqty(o.wallAsk.q) + ') — сопротивление'); }
  if (Math.abs(o.imb) >= 0.6) notes.push('Перевес в стакане ' + Math.round(Math.abs(o.imb) * 100) + '% в ' + (o.imb > 0 ? 'бидах' : 'асках'));
  const s = clamp(v + w, -25, 25);
  return s >= 0 ? { l: s, s: 0, notes } : { l: 0, s: -s, notes };
}
function analyzeTrades(c) {
  c.deltaInfo = null;
  if (!c.trades || !c.trades.length) return;
  const t = now(); const tr = c.trades.filter(x => t - x.time < 180000);
  if (tr.length < 10) return;
  let bv = 0, sv = 0, d1 = 0, d2 = 0;
  for (const x of tr) {
    const usd = x.size * x.price, buy = x.side === 'Buy';
    if (buy) bv += usd; else sv += usd;
    const dt = t - x.time, sgn = buy ? usd : -usd;
    if (dt < 60000) d1 += sgn; else d2 += sgn;
  }
  const tot = bv + sv;
  const ratio = tot ? (bv - sv) / tot : 0;
  const mom = d1 - d2 / 2;
  const norm = (Math.abs(d1) + Math.abs(d2) / 2) || 1;
  const slope = clamp(mom / norm, -1, 1);
  c.deltaInfo = { usd: bv - sv, ratio, slope, fresh: true, buysPct: tot ? bv / tot * 100 : 50 };
}
function deltaPart(c) {
  const d = c.deltaInfo;
  if (!d) return { l: 0, s: 0, notes: [] };
  const notes = [];
  let s = (clamp(d.ratio / 0.5, -1, 1) * 15 + clamp(d.slope / 0.5, -1, 1) * 10) * (d.fresh ? 1 : 0.35);
  if (Math.abs(d.ratio) >= 0.35) notes.push('Дельта ' + (d.usd >= 0 ? '+' : '') + fusd(d.usd) + ' за 3 мин (' + Math.round(d.buysPct) + '% покупок), импульс ' + (d.slope >= 0 ? 'растёт' : 'гаснет'));
  return s >= 0 ? { l: s, s: 0, notes } : { l: 0, s: -s, notes };
}
function fvgPart(c) {
  const price = c.ticker ? +c.ticker.price : 0;
  if (!price) return { l: 0, s: 0, notes: [] };
  const notes = []; let l = 0, s = 0;
  for (const g of c.fvg5.concat(c.fvg15)) {
    if (g.filled) continue;
    const w = g.tf === '15' ? 1.25 : 1;
    if (g.dir === 1) {
      if (price <= g.top && price >= g.bot) { l = Math.max(l, 13 * w); notes.push('Цена внутри бычьего FVG ' + g.tf + 'м ' + fp(g.bot) + '–' + fp(g.top) + ' — поддержка'); }
      else if (price > g.top && price <= g.top * 1.0015) { l = Math.max(l, 9 * w); notes.push('Цена на верхней границе бычьего FVG ' + g.tf + 'м'); }
    } else {
      if (price <= g.top && price >= g.bot) { s = Math.max(s, 13 * w); notes.push('Цена внутри медвежьего FVG ' + g.tf + 'м ' + fp(g.bot) + '–' + fp(g.top) + ' — сопротивление'); }
      else if (price < g.bot && price >= g.bot * 0.9985) { s = Math.max(s, 9 * w); notes.push('Цена на нижней границе медвежьего FVG ' + g.tf + 'м'); }
    }
  }
  return { l: clamp(l, 0, 20), s: clamp(s, 0, 20), notes: notes };
}
function sweepPart(c) {
  const price = c.ticker ? +c.ticker.price : 0;
  if (!price) return { l: 0, s: 0, notes: [] };
  const notes = []; let l = 0, s = 0;
  const check = (kl, pools, tf, w) => {
    const n = kl.length;
    if (!n) return;
    for (const pl of pools) for (let j = Math.max(0, n - 6); j < n; j++) {
      const cd = kl[j];
      if (pl.t === 'h' && cd.h > pl.p && cd.c < pl.p) { s = Math.max(s, 12 * w); notes.push('Снятие ликвидности над ' + fp(pl.p) + ' (' + tf + ') — ложный пробой вверх'); }
      if (pl.t === 'l' && cd.l < pl.p && cd.c > pl.p) { l = Math.max(l, 12 * w); notes.push('Снятие ликвидности под ' + fp(pl.p) + ' (' + tf + ') — ложный пробой вниз'); }
    }
  };
  check(c.k5, c.pools5, '5м', 1);
  check(c.k15, c.pools15, '15м', 1.2);
  let best = null;
  for (const pl of c.pools5.concat(c.pools15)) {
    const d = Math.abs(pl.p - price) / price * 100;
    if (!best || d < best.d) best = { t: pl.t, p: pl.p, d: d };
  }
  if (best && best.d < 0.3) {
    if (best.p > price) { l = Math.max(l, 6); notes.push('Пул ликвидности (равные хаи) ' + fp(best.p) + ' над ценой — магнит вверх (' + best.d.toFixed(2) + '%)'); }
    else { s = Math.max(s, 6); notes.push('Пул ликвидности (равные лои) ' + fp(best.p) + ' под ценой — магнит вниз (' + best.d.toFixed(2) + '%)'); }
  }
  return { l: clamp(l, 0, 20), s: clamp(s, 0, 20), notes: notes };
}
function trendPart(c) {
  const notes = []; let l = 0, s = 0;
  const parts = [[c.k15, 4], [c.k5, 3], [c.k1, 3]];
  for (const pt of parts) {
    const kl = pt[0], w = pt[1];
    if (kl.length < 40) continue;
    const closes = kl.map(x => x.c);
    const e9 = emaArr(closes, 9), e21 = emaArr(closes, 21);
    const i = closes.length - 1;
    if (closes[i] > e9[i] && e9[i] > e21[i]) l += w;
    else if (closes[i] < e9[i] && e9[i] < e21[i]) s += w;
  }
  if (l >= 7) notes.push('Тренд вверх на старших ТФ (EMA9>EMA21)');
  if (s >= 7) notes.push('Тренд вниз на старших ТФ (EMA9<EMA21)');
  return { l: clamp(l, 0, 10), s: clamp(s, 0, 10), notes: notes };
}
function analyzeVol(c) {
  c.vol = null;
  if (!c.k1 || c.k1.length < 20 || !c.ticker) return;
  const k = c.k1, last = k[k.length - 1], price = last.c;
  const aPct = atr(k, 12) / price * 100;
  let avgV = 0; const n = Math.min(20, k.length - 1);
  for (let i = k.length - n; i < k.length; i++) avgV += k[i].v;
  avgV /= n || 1;
  const spike = avgV > 0 ? last.v / avgV : 1;
  const i5 = Math.max(0, k.length - 6);
  const chg5 = (last.c - k[i5].c) / k[i5].c * 100;
  const vol = clamp(aPct / 0.35, 0, 1) * 4 + clamp((spike - 1) / 2, 0, 1) * 3 + clamp(Math.abs(chg5) / 0.8, 0, 1) * 3;
  let imp = null;
  if (spike >= 1.6 && chg5 >= 0.9) imp = 'PUMP';
  else if (spike >= 1.6 && chg5 <= -0.9) imp = 'DUMP';
  c.vol = { atr: aPct, spike: spike, chg5: chg5, score: clamp(vol, 0, 10), imp: imp };
  if (imp) log((imp === 'PUMP' ? '🚀 ' : '🩸 ') + c.sym + ': ' + imp + '-импульс ' + pstr(chg5) + ' за 5м, объём x' + spike.toFixed(1) + ' к среднему');
}
function volPart(c) {
  const v = c.vol;
  if (!v) return { l: 0, s: 0, notes: [] };
  const notes = [];
  let l = 0, s = 0;
  const act = clamp(v.spike - 1, 0, 2) / 2 * 3 + clamp(v.atr / 0.5, 0, 1) * 2;
  l += act; s += act;
  if (v.imp === 'PUMP') { l += 4; notes.push('PUMP-импульс: ' + pstr(v.chg5) + ' за 5м, объём x' + v.spike.toFixed(1) + ' — движение активное'); }
  if (v.imp === 'DUMP') { s += 4; notes.push('DUMP-импульс: ' + pstr(v.chg5) + ' за 5м, объём x' + v.spike.toFixed(1) + ' — движение активное'); }
  return { l: clamp(l, 0, 10), s: clamp(s, 0, 10), notes: notes };
}

// ---------- новости ----------
const AN_NEG_RE = /delist|remove from listing|discontinu/i;
const AN_POS_RE = /new listing|will list|spot listing|perpetual listing/i;
const AN_SUS_RE = /suspend|maintenance of deposits|pause deposit/i;
const AN_MID_RE = /airdrop|distribution|deposit and withdrawal|withdrawal open/i;
function annScore(title, desc, tags) {
  const txt = (title || '') + ' ' + (desc || '') + ' ' + (tags || []).join(' ');
  if (AN_NEG_RE.test(txt)) return -2.5;
  if (AN_POS_RE.test(txt)) return 2;
  if (AN_SUS_RE.test(txt)) return -1;
  if (AN_MID_RE.test(txt)) return 0.5;
  return 0;
}
async function fetchAnnouncements() {
  const offs = [0, 20, 40];
  const arts = [];
  await Promise.all(offs.map(off => api('/v5/announcements/index?locale=en-US&limit=20&offset=' + off).then(r => {
    (r.list || []).forEach(a => {
      const str = (a.title || '') + ' ' + (a.description || '') + ' ' + (a.tags || []).join(' ');
      const syms = []; let m;
      const re = /\b([A-Z0-9]{2,10})USDT\b/g;
      while ((m = re.exec(str))) syms.push(m[1]);
      arts.push({ t: +a.dateTimestamp || now(), title: a.title || '', desc: (a.description || '').slice(0, 200), tags: a.tags || [], score: annScore(a.title, a.description, a.tags), syms: syms });
    });
  }).catch(() => {})));
  return arts;
}
async function fetchTrending() {
  try {
    const ctl = new AbortController();
    const to = setTimeout(() => ctl.abort(), 8000);
    const res = await fetch('https://api.coingecko.com/api/v3/search/trending', { signal: ctl.signal });
    clearTimeout(to);
    const j = await res.json();
    return (j.coins || []).slice(0, 10).map((x, i) => ({ sym: (x.item && x.item.symbol || '').toUpperCase(), rank: i + 1 }));
  } catch (e) { return []; }
}
function applyNews(coins, ann, trend) {
  const t = now();
  for (const sym of Object.keys(coins)) {
    const c = coins[sym];
    const base = sym.replace('USDT', '');
    let sum = 0; const hits = [];
    for (const a of ann) {
      if (t - a.t > 48 * 3600e3) continue;
      if (a.syms.indexOf(base) < 0) continue;
      sum += a.score * Math.pow(0.5, (t - a.t) / 28800000);
      if (Math.abs(a.score) >= 1) hits.push({ t: a.t, title: a.title, sc: a.score });
    }
    let score = clamp(sum / 2.5, -1, 1);
    let trending = false;
    const tr = (trend || []).find(x => x.sym === base);
    if (tr) {
      trending = true;
      if (c.k5.length > 30) {
        const closes = c.k5.map(x => x.c);
        const mom = closes[closes.length - 1] - closes[closes.length - 7];
        score = clamp(score + Math.sign(mom) * 0.3 * (1.1 - tr.rank / 10), -1, 1);
      }
    }
    c.news = { score: score, hits: hits.slice(0, 3), trending: trending };
  }
}
function newsPart(c) {
  const n = c.news;
  if (!n) return { l: 0, s: 0, notes: [] };
  const notes = [];
  if (n.trending) notes.push('Монета в трендовых CoinGecko — повышенное внимание рынка');
  for (const h of n.hits) notes.push((h.sc > 0 ? '📈 ' : '📉 ') + h.title.slice(0, 90));
  return { l: clamp(n.score, 0, 1) * 10, s: clamp(-n.score, 0, 1) * 10, notes: notes };
}

// ---------- скоринг монеты ----------
function analyzeCoin(c) {
  const price = c.ticker ? +c.ticker.price : null;
  if (!price) { c.scores = null; return; }
  analyzeVol(c);
  analyzeOB(c); analyzeTrades(c);
  const ob = obPart(c), de = deltaPart(c), fv = fvgPart(c), lq = sweepPart(c), tr = trendPart(c), nw = newsPart(c), vl = volPart(c);
  const W = ST.paper.weights;
  const freshK = c.deltaInfo && c.deltaInfo.fresh ? 1 : 0.35;
  const long = clamp(ob.l * W.ob + de.l * freshK * W.delta + fv.l * W.fvg + lq.l * W.liq + tr.l * W.trend + nw.l * W.news + vl.l * W.vol, 0, 100);
  const short = clamp(ob.s * W.ob + de.s * freshK * W.delta + fv.s * W.fvg + lq.s * W.liq + tr.s * W.trend + nw.s * W.news + vl.s * W.vol, 0, 100);
  const notes = ob.notes.concat(de.notes, fv.notes, lq.notes, tr.notes, nw.notes, vl.notes)
    .filter(function (n, i, a) { return a.indexOf(n) === i; }).slice(0, 6);
  c.scores = { long: long, short: short, parts: { ob: ob, delta: de, fvg: fv, liq: lq, trend: tr, news: nw, vol: vl }, notes: notes };
}

// ---------- сигналы и входы ----------
function swingLowPx(c) {
  if (c.k1.length < 12) return c.ticker ? +c.ticker.price * 0.995 : 0;
  return Math.min.apply(null, c.k1.slice(-12).map(x => x.l));
}
function swingHighPx(c) {
  if (c.k1.length < 12) return c.ticker ? +c.ticker.price * 1.005 : 0;
  return Math.max.apply(null, c.k1.slice(-12).map(x => x.h));
}
function nearestPool(c, price, dir) {
  let best = null;
  for (const pl of c.pools5.concat(c.pools15)) {
    if (dir === 1 && pl.p > price * 1.001 && (!best || pl.p < best.p)) best = pl;
    if (dir === -1 && pl.p < price * 0.999 && (!best || pl.p > best.p)) best = pl;
  }
  return best;
}
function atrPct1m(c) {
  if (!c.k1 || c.k1.length < 12) return 0.25;
  return atr(c.k1, 10) / c.k1[c.k1.length - 1].c * 100;
}
function effThreshold() { return clamp(Math.round(CFG.threshold + ST.paper.adj), 45, 80); }
function computeRawSignals(coins, order) {
  const out = []; const t = now();
  for (const sym of order) {
    const c = coins[sym];
    if (!c || !c.scores || !c.ticker) continue;
    const sc = c.scores, price = +c.ticker.price;
    if (c.obRes && c.obRes.spreadPct > CFG.maxSpread) continue;
    const longOk = sc.long >= effThreshold() && sc.long - sc.short >= CFG.gap;
    const shortOk = sc.short >= effThreshold() && sc.short - sc.long >= CFG.gap;
    if (!longOk && !shortOk) continue;
    const side = longOk && (!shortOk || sc.long >= sc.short) ? 'LONG' : 'SHORT';
    if (t - ((ST.paper.lastSig[sym + ':' + side]) || 0) < COOLDOWN) continue;
    const aP = atrPct1m(c);
    let stop, tp1, tp2, stP, tpP, rr;
    if (side === 'LONG') {
      stP = clamp((price - Math.min(swingLowPx(c), price * (1 - clamp(aP * 1.6, 0.15, 0.8) / 100))) / price * 100, 0.15, 0.8);
      stop = price * (1 - stP / 100);
      const pl = nearestPool(c, price, 1);
      tp1 = pl ? pl.p : price * (1 + stP * 1.7 / 100);
      tpP = (tp1 - price) / price * 100;
      if (tpP < stP * 1.15) { tp1 = price * (1 + stP * 1.7 / 100); tpP = (tp1 - price) / price * 100; }
      tp2 = price * (1 + tpP * 1.8 / 100);
    } else {
      stP = clamp((Math.max(swingHighPx(c), price * (1 + clamp(aP * 1.6, 0.15, 0.8) / 100)) - price) / price * 100, 0.15, 0.8);
      stop = price * (1 + stP / 100);
      const pl = nearestPool(c, price, -1);
      tp1 = pl ? pl.p : price * (1 - stP * 1.7 / 100);
      tpP = (price - tp1) / price * 100;
      if (tpP < stP * 1.15) { tp1 = price * (1 - stP * 1.7 / 100); tpP = (price - tp1) / price * 100; }
      tp2 = price * (1 - tpP * 1.8 / 100);
    }
    rr = tpP / stP;
    if (rr < 1.05 || tpP < 0.18) continue;
    ST.paper.lastSig[sym + ':' + side] = t;
    out.push({ sym: sym, side: side, score: Math.round(side === 'LONG' ? sc.long : sc.short),
      parts: sc.parts, notes: sc.notes, entry: price, stop: stop, tp1: tp1, tp2: tp2, rr: rr,
      stopPctN: stP, tpPctN: tpP, time: t, volScore: c.vol ? +c.vol.score.toFixed(1) : 0 });
  }
  // приоритет волатильных монет (охота на пампы/дампы)
  out.sort((a, b) => (b.score + (b.volScore || 0) * 2) - (a.score + (a.volScore || 0) * 2));
  return out.slice(0, 8);
}
function fillFactors(p, coins) {
  const c = coins[p.sym];
  if (!c || !c.scores) return;
  const P = c.scores.parts, long = p.side === 'LONG';
  const val = k => { const x = P[k]; return x ? (long ? x.l : x.s) : 0; };
  const mx = { ob: 25, delta: 25, fvg: 20, liq: 20, trend: 10, news: 10, vol: 10 };
  p.factors = {};
  for (const k in mx) p.factors[k] = clamp(val(k) / mx[k], 0, 1);
}
function openPaperTrade(s, coins) {
  if (ST.open.length >= 8) return;
  const c = coins[s.sym];
  if (!c || !c.k1.length || !c.scores) return;
  if (!accCanOpen()) return;
  const long = s.side === 'LONG';
  const P = s.parts;
  const obV = long ? P.ob.l : P.ob.s, deV = long ? P.delta.l : P.delta.s;
  if (obV < 3 && deV < 3) return;
  if (c.obRes && ((long && c.obRes.imb < -0.45) || (!long && c.obRes.imb > 0.45))) return;
  ST.paper.seq++;
  const p = { id: ST.paper.seq, sym: s.sym, side: s.side, entry: s.entry, stop: s.stop, tp1: s.tp1, tp2: s.tp2,
    stopPctN: s.stopPctN, tpPctN: s.tpPctN, score: s.score,
    factors: { ob: 0, delta: 0, fvg: 0, liq: 0, trend: 0, news: 0, vol: 0 },
    t: now(), entryT: c.k1[c.k1.length - 1].t, maxHold: MAX_HOLD, realized: 0, partial: false, be: false };
  fillFactors(p, coins);
  if (!accSize(p)) { ST.paper.seq--; return; }
  ST.open.push(p);
  accMarkEquity();
  log('🤖 Вход: ' + (long ? '▲ ЛОНГ ' : '▼ ШОРТ ') + s.sym + ' по ' + fp(s.entry) + ' · объём ' + fusd(p.notional) + '$ · плечо x' + p.lev + ' (кросс) · маржа $' + p.margin.toFixed(2) + ' · риск ' + (p.riskPct * 100).toFixed(1) + '% · ликв ~' + fp(liqPx(p)) + ' · стоп ' + fp(s.stop) + ' → тейк ' + fp(s.tp1) + '/' + fp(s.tp2) + ' · скоринг ' + s.score);
}

// ---------- ведение и закрытие позиций ----------
function closePaper(p, exit, outcome, exitT, coins) {
  accRealize(p, 1, exit);
  const pnl$ = p.realized || 0;
  const R = p.risk$ ? pnl$ / p.risk$ : 0;
  const A = ST.acc;
  A.tradesN++;
  if (pnl$ > 0) A.streak++; else if (pnl$ < 0) A.streak = 0;
  A.risk = accRiskPct();
  accMarkEquity();
  const tr = { id: p.id, sym: p.sym, side: p.side, entry: p.entry, exit: exit, outcome: outcome, R: R, pnl$: pnl$,
    notional: p.notional || 0, lev: p.lev || 1, bal: +A.bal.toFixed(2),
    t: p.t, exitT: exitT, durMin: Math.max(0, Math.round((exitT - p.entryT) / 60000)), score: p.score, factors: p.factors };
  ST.trades.push(tr);
  if (ST.trades.length > 200) ST.trades = ST.trades.slice(-200);
  ST.open = ST.open.filter(x => x.id !== p.id);
  learn(tr);
  histAppend(tr);
  const emo = outcome === 'win' ? '✅' : outcome === 'loss' ? '❌' : outcome === 'be' ? '➖' : '⏱';
  log(emo + ' Закрыто ' + tr.side + ' ' + tr.sym + ': ' + outcome.toUpperCase() + ' R=' + R.toFixed(2) + ' (' + (pnl$ >= 0 ? '+' : '−') + '$' + Math.abs(pnl$).toFixed(2) + '), депозит ' + A.bal.toFixed(2) + ' USDT');
  if (!A.done && !A.blown && A.bal < 2) {
    A.blown = true; A.done = true;
    log('💀 Депозит слит — разбор ошибок в уроках.');
  }
  if (!A.done && A.bal >= GOAL) {
    A.done = true;
    log('🏆 ЦЕЛЬ ' + GOAL + ' USDT ДОСТИГНУТА! Депозит ' + A.bal.toFixed(2) + ' USDT за ' + A.tradesN + ' сделок. Новые входы остановлены — прибыль зафиксирована.');
  }
}
function trackPaper(coins) {
  for (const p of ST.open.slice()) {
    const c = coins[p.sym];
    if (!c || !c.k1.length) continue;
    const after = c.k1.filter(x => x.t > p.entryT);
    if (!after.length) continue;
    const long = p.side === 'LONG';
    let done = null;
    for (const d of after) {
      const stopPx = p.be ? p.entry : p.stop;
      if (long ? d.l <= stopPx : d.h >= stopPx) {
        done = { exit: stopPx, outcome: p.be ? 'be' : 'loss', exitT: d.t }; break;
      }
      if (!p.partial && (long ? d.h >= p.tp1 : d.l <= p.tp1)) {
        p.partial = true; p.be = true;
        accRealize(p, 0.5, p.tp1);
        log('💰 ' + p.sym + ': зафиксировал 50% на тейке 1 (' + fp(p.tp1) + '), стоп перенесён в безубыток');
      }
      if (p.partial && (long ? d.h >= p.tp2 : d.l <= p.tp2)) {
        done = { exit: p.tp2, outcome: 'win', exitT: d.t }; break;
      }
    }
    if (!done && after.length >= p.maxHold) {
      const last = after[after.length - 1];
      done = { exit: last.c, outcome: p.partial ? 'be' : 'timeout', exitT: last.t };
    }
    const pr = c.ticker ? +c.ticker.price : null;
    if (!done && pr && now() - p.t > p.maxHold * 60000 * 1.5) done = { exit: pr, outcome: 'timeout', exitT: now() };
    if (done) closePaper(p, done.exit, done.outcome, done.exitT, coins);
  }
}

// ---------- загрузка рыночных данных ----------
async function fetchData() {
  const coins = {}, order = [];
  const r = await api('/v5/market/tickers?category=linear');
  const rows = (r.list || []).filter(x => x.symbol && x.symbol.endsWith('USDT') && x.symbol.indexOf('USDC') < 0 && +x.turnover24h > 0)
    .sort((a, b) => +b.turnover24h - +a.turnover24h).slice(0, TOP_N);
  for (const x of rows) {
    const sym = x.symbol;
    order.push(sym);
    coins[sym] = { sym: sym, ticker: { price: +x.lastPrice, chg: +x.price24hPcnt * 100, turnover: +x.turnover24h, bid: +x.bid1Price, ask: +x.ask1Price },
      k1: [], k5: [], k15: [], fvg5: [], fvg15: [], pools5: [], pools15: [], trades: null, deltaInfo: null, news: null, vol: null, obRes: null };
  }
  log('Данные: ' + order.length + ' монет — ' + order.slice(0, 5).map(s => s.replace('USDT', '')).join(', ') + '…');
  const [ann, trend] = await Promise.all([fetchAnnouncements(), fetchTrending()]);
  log('Новости: ' + ann.length + ' анонсов Bybit, ' + trend.length + ' трендовых CoinGecko');
  await pool(order, async sym => {
    const c = coins[sym];
    const k1 = await api('/v5/market/kline?category=linear&symbol=' + sym + '&interval=1&limit=200');
    c.k1 = (k1.list || []).map(a => ({ t: +a[0], o: +a[1], h: +a[2], l: +a[3], c: +a[4], v: +a[5] })).reverse();
    const k5 = await api('/v5/market/kline?category=linear&symbol=' + sym + '&interval=5&limit=200');
    c.k5 = (k5.list || []).map(a => ({ t: +a[0], o: +a[1], h: +a[2], l: +a[3], c: +a[4], v: +a[5] })).reverse();
    const k15 = await api('/v5/market/kline?category=linear&symbol=' + sym + '&interval=15&limit=200');
    c.k15 = (k15.list || []).map(a => ({ t: +a[0], o: +a[1], h: +a[2], l: +a[3], c: +a[4], v: +a[5] })).reverse();
    c.fvg5 = findFVGs(c.k5, '5'); c.fvg15 = findFVGs(c.k15, '15');
    c.pools5 = findPools(c.k5.slice(-120)); c.pools15 = findPools(c.k15.slice(-120));
  }, 5);
  await pool(order, async sym => {
    const c = coins[sym];
    const ob = await api('/v5/market/orderbook?category=linear&symbol=' + sym + '&limit=50');
    c.ob = { bids: ob.b || ob.bids || [], asks: ob.a || ob.asks || [] };
  }, 8);
  await pool(order, async sym => {
    const c = coins[sym];
    const tr = await api('/v5/market/recent-trade?category=linear&symbol=' + sym + '&limit=1000');
    const list = (tr.list || []).map(x => ({ price: +x.price, size: +x.size, side: x.side, time: +x.time }));
    list.sort((a, b) => a.time - b.time);
    c.trades = list;
  }, 6);
  applyNews(coins, ann, trend);
  return { coins, order };
}

// ---------- коммит в репозиторий ----------
function gitCommit(msg) {
  if (process.env.BOT_NO_COMMIT) { log('BOT_NO_COMMIT: коммит пропущен'); return; }
  try {
    cp.execSync('git add bot/state.json bot/history.txt', { cwd: ROOT, stdio: 'ignore' });
    const st = cp.execSync('git status --porcelain bot/state.json bot/history.txt', { cwd: ROOT }).toString();
    if (!st.trim()) { log('Изменений для коммита нет'); return; }
    cp.execSync('git -c user.name="scalp-bot" -c user.email="actions@github.com" commit -m "' + msg.replace(/"/g, "'") + '"', { cwd: ROOT, stdio: 'ignore' });
    cp.execSync('git push', { cwd: ROOT, stdio: 'ignore' });
    ST.lastCommitAt = now();
    log('Изменения закоммичены: ' + msg);
  } catch (e) {
    log('Не удалось закоммитить: ' + (e.message || e));
  }
}

// ---------- главный цикл ----------
(async function main() {
  const t0 = now();
  loadState();
  const A = ST.acc;
  log('=== SCALP-AI bot · депозит ' + A.bal.toFixed(2) + ' USDT (старт ' + START_BAL + ') · цель ' + GOAL + ' · открытых позиций ' + ST.open.length + ' ===');
  if (A.done) {
    log(A.blown ? 'Счёт слит — новых входов нет. Удалите bot/state.json, чтобы начать заново.' : '🏆 Цель достигнута, входы остановлены. Удалите bot/state.json для нового забега.');
    saveState();
    return;
  }
  let closedN = 0;
  try {
    const { coins, order } = await fetchData();
    // 1) ведение открытых позиций по свежим свечам 1м
    const before = ST.open.length;
    trackPaper(coins);
    closedN = before - ST.open.length;
    // 2) поиск новых входов
    for (const sym of order) analyzeCoin(coins[sym]);
    const sigs = computeRawSignals(coins, order);
    for (const s of sigs.slice(0, 3)) {
      if (!accCanOpen()) break;
      openPaperTrade(s, coins);
    }
    // 3) карточка активных сигналов в лог
    for (const s of sigs.slice(0, 3)) log('Сигнал: ' + s.side + ' ' + s.sym + ' скоринг ' + s.score + ' · вол ' + (s.volScore || 0) + '/10 · R:R ' + s.rr.toFixed(2));
    if (!sigs.length) log('Активных сигналов нет — порог ' + effThreshold() + ', движок ждёт.');
  } catch (e) {
    log('ОШИБКА данных: ' + (e.message || e));
    saveState();
    process.exit(1);
  }
  accMarkEquity();
  saveState();
  log('Депозит: ' + A.bal.toFixed(2) + ' USDT · сделок: ' + A.tradesN + ' · просадка макс ' + (A.maxDD * 100).toFixed(1) + '% · комиссий ' + A.fees.toFixed(2) + '$ · порог ' + effThreshold() + ' · завершено за ' + ((now() - t0) / 1000).toFixed(1) + 'с');
  // коммит: при закрытых сделках всегда, иначе сердцебиение раз в 30 минут
  if (closedN > 0) gitCommit('bot: ' + closedN + ' сделок закрыто, депозит ' + A.bal.toFixed(2) + ' USDT');
  else if (now() - (ST.lastCommitAt || 0) > 30 * 60000) gitCommit('bot: heartbeat, депозит ' + A.bal.toFixed(2) + ' USDT, позиций ' + ST.open.length);
})().catch(e => { console.error(e); process.exit(1); });
