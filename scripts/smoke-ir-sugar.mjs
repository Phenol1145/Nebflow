#!/usr/bin/env node
// smoke-ir-sugar.mjs — 人侧 argv 糖（Command IR Router P1-1）WS 三腿冒烟（2026-09-29）。
//
// P1-1 验收面（.zcode/plans/command-ir-standard.md §9 ingress #1 + 方案 precedenceNails）：
// `/dev:fs:ls …` 这类「未被既有闸消费的 /…」经 handleMessage 分流闸 lower 成 IR 并执行。
//
// 钉八件事（三腿全覆盖：typeless / immediateInput / userMessage；REST handleMessagePublic 同走
// handleMessage，不在本脚本单列）：
//   1. 三腿各发一次 `/dev:fs:ls` ⇒ irResult status=done / exit=0 / results[0].command=dev:fs:ls；
//   2. argv 双形态：`--path .`（flag）与 `.`（positional，依赖本批 DevCommands params 声明）；
//   3. 记录面：user 原文气泡 + ir.sugar 系统气泡（getHistory → historyPage）；
//   4. `/clear` 不 lower（保留名 ⇒ 白名单的补）：窗口内无 irResult，historyPage 见 user 原文；
//   5. `//dev:fs:ls` 不 lower（// 转义显式排除）：同上；
//   6. 坏名 `/dev:FS:ls` ⇒ invalid / exit=2 / router.schema.name_invalid + ir.sugar 气泡；
//   7. 未注册 `/dev:fs:nope` ⇒ invalid / exit=127 / router.unknown_command（与直连腿同码）；
//   8. `--json {} --path .` ⇒ invalid / exit=2 / router.invalid_args(reason=json_exclusive)。
//
// Run: NEBFLOW_URL=http://localhost:8099 NEBFLOW_HOME_DIR=/tmp/nebflow-ir-smoke \
//        node scripts/smoke-ir-sugar.mjs

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const URL_BASE = process.env.NEBFLOW_URL || 'http://localhost:8099';
const HOME = process.env.NEBFLOW_HOME_DIR;
if (!HOME) {
  console.error('FAIL  NEBFLOW_HOME_DIR is required (isolated instance home)');
  process.exit(1);
}
const TOKEN = JSON.parse(readFileSync(join(HOME, 'auth.json'), 'utf8'));

let failed = 0;
function check(name, ok, extra = '') {
  if (!ok) failed++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${extra ? '  — ' + extra : ''}`);
}

function wsConnect() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${URL_BASE.replace('http', 'ws')}/ws?token=${encodeURIComponent(TOKEN)}`);
    const inbox = [];
    const waiters = [];
    ws.addEventListener('message', (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch { return; }
      inbox.push(msg);
      for (let i = waiters.length - 1; i >= 0; i--) {
        const w = waiters[i];
        if (w.pred(msg)) { waiters.splice(i, 1); w.resolve(msg); }
      }
    });
    ws.addEventListener('open', () => resolve({ ws, inbox, waiters }));
    ws.addEventListener('error', (e) => reject(new Error('WS connect failed: ' + (e.message || 'error'))));
  });
}

function waitFor(conn, pred, timeoutMs, label) {
  const hit = conn.inbox.find(pred);
  if (hit) return Promise.resolve(hit);
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`timeout waiting for ${label}`)), timeoutMs);
    conn.waiters.push({ pred, resolve: (m) => { clearTimeout(t); resolve(m); } });
  });
}

const SESSION = 'smoke-ir-sugar';

/** 糖腿的 requestId 是服务端 UUID —— 按「下一个新到的 irResult」逐 turn 对齐（turn 串行）。 */
async function nextIrResult(conn) {
  const before = conn.inbox.filter((m) => m.type === 'irResult').length;
  return waitFor(
    conn,
    (m) => m.type === 'irResult' && conn.inbox.filter((x) => x.type === 'irResult').length > before,
    10000,
    'irResult (sugar turn)'
  );
}

/** 窗口内无新 irResult（未 lower 的负证：/clear、//…）。 */
async function noIrResultFor(conn, ms) {
  const before = conn.inbox.filter((m) => m.type === 'irResult').length;
  await new Promise((r) => setTimeout(r, ms));
  return conn.inbox.filter((m) => m.type === 'irResult').length === before;
}

/** 每次请求都回一帧新 historyPage——按「下一帧新到的」对齐（避免命中 inbox 里的旧帧）。 */
async function getHistory(conn) {
  const before = conn.inbox.filter((m) => m.type === 'historyPage' && m.sessionId === SESSION).length;
  conn.ws.send(JSON.stringify({ type: 'getHistory', sessionId: SESSION, limit: 200 }));
  return waitFor(
    conn,
    (m) =>
      m.type === 'historyPage' &&
      m.sessionId === SESSION &&
      conn.inbox.filter((x) => x.type === 'historyPage' && x.sessionId === SESSION).length > before,
    10000,
    'historyPage'
  );
}

/**
 * 轮询断言：落盘/缓存是最终一致（appendUiMessages 走 per-session semaphore + 防抖刷盘），
 * 断言前的写入可能晚于首次 historyPage 到达——重取至多 3s，命中即真。
 */
async function historyHas(conn, pred) {
  for (let i = 0; i < 6; i++) {
    const h = await getHistory(conn);
    if ((h.messages || []).some(pred)) return h;
    await new Promise((r) => setTimeout(r, 500));
  }
  const h = await getHistory(conn);
  return (h.messages || []).some(pred) ? h : null;
}

const conn = await wsConnect();
try {
  // ── 1. 三腿各一次 /dev:fs:ls（typeless / immediateInput / userMessage）──────────
  const legs = [
    ['typeless', JSON.stringify({ sessionId: SESSION, content: '/dev:fs:ls' })],
    ['immediateInput', JSON.stringify({ type: 'immediateInput', sessionId: SESSION, content: '/dev:fs:ls' })],
    ['userMessage', JSON.stringify({ type: 'userMessage', sessionId: SESSION, content: '/dev:fs:ls' })],
  ];
  for (const [leg, frame] of legs) {
    conn.ws.send(frame);
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      `${leg} leg /dev:fs:ls → status=done / exit=0 / results[0].command=dev:fs:ls`,
      p.status === 'done' && p.exit === 0 && p.results?.[0]?.command === 'dev:fs:ls',
      `status=${p.status} exit=${p.exit} cmd=${p.results?.[0]?.command}`
    );
  }

  // ── 2. argv 双形态：--path .（flag）与 .（positional）────────────────────────
  for (const content of ['/dev:fs:ls --path .', '/dev:fs:ls .']) {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      `'${content}' → done / exit=0 (both argv forms bind path)`,
      p.status === 'done' && p.exit === 0 && p.results?.[0]?.exit === 0,
      `status=${p.status} exit=${p.exit} nodeExit=${p.results?.[0]?.exit}`
    );
  }

  // ── 3. 记录面：user 原文 + ir.sugar 系统气泡（getHistory → historyPage）────────
  {
    const h = await historyHas(conn, (m) => m.type === 'user' && m.text === '/dev:fs:ls');
    check('historyPage: user row keeps the verbatim "/dev:fs:ls" original', h !== null);
    const msgs = (h?.messages) || [];
    const sugarRows = msgs.filter((m) => m.type === 'system' && m.i18nKey === 'ir.sugar');
    check(
      'historyPage: ir.sugar system bubble carries name/status/exit',
      sugarRows.some((m) => m.content.includes('dev:fs:ls') && m.content.includes('done') && m.content.includes('exit=0')),
      JSON.stringify(sugarRows[0]?.content || '')
    );
  }

  // ── 4. /clear 不 lower（保留名 = 白名单的补；既有斜杠通道优先消费）──────────────
  conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/clear' }));
  check('/clear → no irResult within window (not lowered)', await noIrResultFor(conn, 800));
  // 注：/clear 落普通消息腿会在隔离实例 spawn 一个 agent turn（无 LLM key 时以错误 turn
  // 收场）——属方案认可的噪音，不进断言面。

  // ── 5. //dev:fs:ls 不 lower（// 转义显式排除）──────────────────────────────
  conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '//dev:fs:ls' }));
  check('//dev:fs:ls → no irResult within window (escape, not lowered)', await noIrResultFor(conn, 800));

  {
    const okClear = await historyHas(conn, (m) => m.type === 'user' && m.text === '/clear');
    check('historyPage: "/clear" kept as a plain user message', okClear !== null);
    const okEscape = await historyHas(conn, (m) => m.type === 'user' && m.text === '//dev:fs:ls');
    check('historyPage: "//dev:fs:ls" kept as a plain user message', okEscape !== null);
  }

  // ── 6. 坏名 /dev:FS:ls ⇒ name_invalid（exit 2）+ ir.sugar 气泡 ────────────────
  conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:FS:ls' }));
  {
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      '/dev:FS:ls → status=invalid / exit=2 / router.schema.name_invalid',
      p.status === 'invalid' && p.exit === 2 && p.error?.code === 'router.schema.name_invalid',
      `status=${p.status} exit=${p.exit} code=${p.error?.code}`
    );
  }
  {
    const h = await historyHas(
      conn,
      (m) => m.type === 'system' && m.i18nKey === 'ir.sugar' && m.content.includes('router.schema.name_invalid')
    );
    const bad = ((h?.messages) || []).find(
      (m) => m.type === 'system' && m.i18nKey === 'ir.sugar' && m.content.includes('router.schema.name_invalid')
    );
    check('historyPage: ir.sugar bubble records the name_invalid failure', h !== null, JSON.stringify(bad?.content || ''));
  }

  // ── 7. 未注册 /dev:fs:nope ⇒ unknown_command（exit 127，与直连腿同码）──────────
  conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:fs:nope' }));
  {
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      '/dev:fs:nope → status=invalid / exit=127 / router.unknown_command',
      p.status === 'invalid' && p.exit === 127 && p.error?.code === 'router.unknown_command',
      `status=${p.status} exit=${p.exit} code=${p.error?.code}`
    );
  }

  // ── 8. --json 互斥（C24 糖面镜像）────────────────────────────────────────────
  conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:fs:ls --json {} --path .' }));
  {
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      "'/dev:fs:ls --json {} --path .' → invalid / exit=2 / router.invalid_args(json_exclusive)",
      p.status === 'invalid' && p.exit === 2 && p.error?.code === 'router.invalid_args' &&
        p.error?.details?.reason === 'json_exclusive',
      `status=${p.status} exit=${p.exit} code=${p.error?.code} reason=${p.error?.details?.reason}`
    );
  }

  // ── 9.（可选）mcp 面只走 --json：无该命令注册则 SKIP ─────────────────────────
  conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/mcp:x:y --key k v' }));
  {
    const r = await nextIrResult(conn);
    const p = r.response || {};
    if (p.error?.code === 'router.unknown_command') {
      console.log('SKIP  mcp face check — no mcp command registered in this instance');
    } else {
      check(
        "'/mcp:x:y --key k v' → invalid_args(unknown_flag) (foreign face is --json only)",
        p.status === 'invalid' && p.error?.code === 'router.invalid_args' && p.error?.details?.reason === 'unknown_flag',
        `status=${p.status} code=${p.error?.code} reason=${p.error?.details?.reason}`
      );
    }
  }
} finally {
  conn.ws.close();
}

console.log(failed === 0 ? '# ALL PASS' : `# FAILED: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
