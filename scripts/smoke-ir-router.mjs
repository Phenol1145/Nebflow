#!/usr/bin/env node
// smoke-ir-router.mjs — 命令 IR 路由层（Command IR Router P0）WS ingress 冒烟（2026-09-28）。
//
// P0 验收面（.zcode/plans/command-ir-router.md §八）：IR 类型 + 注册表 + 策略 + `dev:` 执行器
// + 两个参考命令 + **一条管道** + 策略拒绝路径 + 审计落账；「手工走通一次 WS 调用」= 本脚本。
//
// 钉四件事（都走 `ir` 帧 → `irResult` 帧）：
//   1. 管道 `dev:fs:ls | dev:fs:cat` ⇒ status=done / exit=0 / 两节点结果 / 末节点 jsonl→text；
//   2. 未知命令 ⇒ status=invalid / exit=127 / router.unknown_command（零结果）；
//   3. 路径越根 ⇒ status=invalid / exit=2 / router.schema.bad_value（[P7] 第一级）；
//   4. 审计落盘：`<home>/logs/ir/<yyyyMMdd>.jsonl` 出现该 requestId 的节点记录（每节点一条）。
//
// Run: NEBFLOW_URL=http://localhost:8099 NEBFLOW_HOME_DIR=/tmp/nebflow-ir-smoke \
//        node scripts/smoke-ir-router.mjs

import { readFileSync, existsSync, readdirSync } from 'node:fs';
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

/** 信封（标准 §0）：sessionId 由帧补，requestId 每次唯一。 */
function envelope(plan, requestId) {
  return {
    ir: 1,
    plan,
    tenant: { kind: 'human', user: 'local' },
    ingress: 'human',
    sessionId: 'smoke-ir',
    requestId,
  };
}

function irFrame(plan, requestId) {
  return JSON.stringify({ type: 'ir', sessionId: 'smoke-ir', request: envelope(plan, requestId) });
}

const conn = await wsConnect();
try {
  // ── 1. 管道 ls | cat ────────────────────────────────────────────
  const rid1 = 'smoke-ir-pipe';
  conn.ws.send(irFrame({
    pipe: {
      stages: [
        { call: { name: 'dev:fs:ls', args: {} } },
        { call: { name: 'dev:fs:cat', args: {} } },
      ],
    },
  }, rid1));
  const r1 = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === rid1, 10000, 'irResult (pipe)');
  const p1 = r1.response || {};
  check('pipe ls|cat → status=done / exit=0',
    p1.status === 'done' && p1.exit === 0, `status=${p1.status} exit=${p1.exit}`);
  check('pipe → two node results in execution order (0.0, 0.1)',
    Array.isArray(p1.results) && p1.results.map((r) => r.node).join(',') === '0.0,0.1',
    JSON.stringify(p1.results?.map((r) => r.node)));
  check('pipe → downstream saw the upstream JSONL (jsonl→text coercion)',
    typeof p1.final?.text === 'string' && p1.final.text.includes('"name"'),
    JSON.stringify(p1.final).slice(0, 120));

  // ── 2. 未知命令 ────────────────────────────────────────────────
  const rid2 = 'smoke-ir-unknown';
  conn.ws.send(irFrame({ call: { name: 'dev:fs:nope', args: {} } }, rid2));
  const r2 = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === rid2, 10000, 'irResult (unknown)');
  const p2 = r2.response || {};
  check('unknown command → status=invalid / exit=127 / router.unknown_command',
    p2.status === 'invalid' && p2.exit === 127 && p2.error?.code === 'router.unknown_command',
    `status=${p2.status} exit=${p2.exit} code=${p2.error?.code}`);
  check('unknown command → zero node results',
    Array.isArray(p2.results) && p2.results.length === 0, `results=${p2.results?.length}`);

  // ── 3. 路径越根（[P7] 第一级：计划非法，不是策略拒绝）──────────────
  const rid3 = 'smoke-ir-escape';
  conn.ws.send(irFrame({ call: { name: 'dev:fs:cat', args: { path: '../../etc/passwd' } } }, rid3));
  const r3 = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === rid3, 10000, 'irResult (escape)');
  const p3 = r3.response || {};
  check('path escape → status=invalid / exit=2 / router.schema.bad_value',
    p3.status === 'invalid' && p3.exit === 2 && p3.error?.code === 'router.schema.bad_value',
    `status=${p3.status} exit=${p3.exit} code=${p3.error?.code}`);

  // ── 4. 审计落盘（§8.6：每节点一条，含 Deny/Ask；记摘要不记 args 原文）────
  await new Promise((r) => setTimeout(r, 500));
  const auditDir = join(HOME, 'logs', 'ir');
  const files = existsSync(auditDir) ? readdirSync(auditDir).filter((f) => f.endsWith('.jsonl')) : [];
  const lines = files.flatMap((f) =>
    readFileSync(join(auditDir, f), 'utf8').split('\n').filter((l) => l.trim())
  ).map((l) => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  const mine = lines.filter((l) => l.requestId === rid1);
  check('audit ledger written per node (2 records for the pipe)',
    mine.length === 2, `records=${mine.length} files=${files.length}`);
  check('audit records carry decision/exit/caps/argsDigest (no args plaintext)',
    mine.length > 0 && mine.every((l) => l.decision === 'allow' && l.exit === 0 && Array.isArray(l.caps) && l.argsDigest?.length === 64),
    JSON.stringify(mine[0] || {}).slice(0, 160));
  check('audit never stores the args path verbatim',
    lines.every((l) => !JSON.stringify(l).includes('../../etc/passwd')));
} finally {
  conn.ws.close();
}

console.log(failed === 0 ? '# ALL PASS' : `# FAILED: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
