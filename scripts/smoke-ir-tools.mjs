#!/usr/bin/env node
// smoke-ir-tools.mjs — P1-2 批 A「内置 Tool → IR 命令桥接」活实例冒烟（2026-09-29）。
//
// 验收面（.zcode/plans/command-ir-standard.md §7/§8 + P1-2 批 A 方案）：
// dev:tool:* 七件注册进 IR 命令表、经糖腿/直连 ir 帧真执行；LLM 面不开侧门；
// dev:tool:* 一律 Ask（auto-all 折 Allow，confirm-edits 成 await_approval）；审计落账。
//
// 钉六件事：
//   1. 糖腿 /dev:tool:nodelist（typeless，零参）⇒ done / exit=1 / command.failed +
//      "Missing 'project' parameter"（默认 auto-all 档把 askRule 的 Ask 折 Allow ⇒
//      真执行到工具内确定性错误——桥接链路真通，非 stub）；
//   2. 糖腿 /dev:tool:read --file_path <HOME>/auth.json（named 形参，P1-2 全 named 纪律）
//      ⇒ done / exit=0 / final.text 以 "1\t" 行号形态（真 ReadTool cat -n 输出）；
//      附：--limit 5（number 属性不进糖）⇒ invalid + router.invalid_args(unknown_flag)；
//   3. LLM 侧门钉：直连 ir 帧（tenant=llm / ingress=llm）调 dev:tool:read ⇒
//      status=invalid + router.invalid_args 且 details.reason=audience（§7.4 检查点①）；
//   4. 档位自适应审批腿：直连 ir 帧（human/console）⇒ 回 await_approval（实例被配成
//      confirm-edits）则取审批体铸凭据重提交 ⇒ done；回 done（默认 auto-all）则 PASS 附注；
//   5. 审计：<HOME>/logs/ir/<yyyyMMdd>.jsonl 存在 command=dev:tool:nodelist 的记录，
//      decision/caps（含 FsRead(*)）/argsDigest 齐备、无 args 原文（§8.6）；
//   6. dev:fs:* 零扰动：/dev:fs:ls 仍 done/exit=0（askRule 只罩 dev:tool:*）。
//
// Run: NEBFLOW_URL=http://localhost:8099 NEBFLOW_HOME_DIR=/tmp/nebflow-ir-smoke \
//        node scripts/smoke-ir-tools.mjs
// 注：腿 2 的路径要求绝对且不含空格（糖 tokenize 按空白切分，无引号）。

import { readFileSync, existsSync } from 'node:fs';
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

const SESSION = 'smoke-ir-tools';
const AUTH_JSON = join(HOME, 'auth.json').replace(/\\/g, '/');

/** 糖腿的 requestId 是服务端 UUID —— 按「下一个新到的 irResult」对齐（turn 串行）。 */
async function nextIrResult(conn) {
  const before = conn.inbox.filter((m) => m.type === 'irResult').length;
  return waitFor(
    conn,
    (m) => m.type === 'irResult' && conn.inbox.filter((x) => x.type === 'irResult').length > before,
    15000,
    'irResult (turn)'
  );
}

/** 直连 ir 帧（标准 §0 信封；sessionId 由帧补，requestId 每次唯一）。 */
function envelope(plan, requestId, extra = {}) {
  return {
    ir: 1,
    plan,
    tenant: { kind: 'human', user: 'console' },
    ingress: 'human',
    sessionId: SESSION,
    requestId,
    ...extra,
  };
}

function irFrame(plan, requestId, extra = {}) {
  return JSON.stringify({ type: 'ir', sessionId: SESSION, request: envelope(plan, requestId, extra) });
}

async function submitDirect(conn, plan, requestId, extra = {}) {
  conn.ws.send(irFrame(plan, requestId, extra));
  const r = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === requestId, 15000, `irResult ${requestId}`);
  return r.response || {};
}

const conn = await wsConnect();
try {
  // ── 1. 糖腿 /dev:tool:nodelist（零参 → 工具内确定性错误）─────────────────────
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:tool:nodelist' }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:nodelist → done / exit=1 / command.failed / Missing \'project\'',
      p.status === 'done' && p.exit === 1 && p.error?.code === 'command.failed' &&
        (p.error?.message || '').includes("Missing 'project' parameter"),
      `status=${p.status} exit=${p.exit} code=${p.error?.code}`
    );
  }

  // ── 2. 糖腿 /dev:tool:read --file_path <HOME>/auth.json（named 形参真读文件）────
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: `/dev:tool:read --file_path ${AUTH_JSON}` }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:read --file_path auth.json → done / exit=0 / final.text "1\\t" cat -n form',
      p.status === 'done' && p.exit === 0 && typeof p.final?.text === 'string' && p.final.text.startsWith('1\t'),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 2b. number 属性不进糖（机械 params 纪律的糖面镜像）───────────────────────
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: `/dev:tool:read --file_path ${AUTH_JSON} --limit 5` }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar ... --limit 5 → invalid / exit=2 / router.invalid_args(unknown_flag) (number props stay --json-only)',
      p.status === 'invalid' && p.exit === 2 && p.error?.code === 'router.invalid_args' &&
        p.error?.details?.reason === 'unknown_flag',
      `status=${p.status} exit=${p.exit} reason=${p.error?.details?.reason}`
    );
  }

  // ── 3. LLM 侧门钉：llm ingress 直连 ir 帧 ⇒ audience 拒（§7.4 检查点①）────────
  {
    const rid = 'smoke-ir-tools-llm';
    conn.ws.send(JSON.stringify({
      type: 'ir',
      sessionId: SESSION,
      request: {
        ir: 1,
        plan: { call: { name: 'dev:tool:read', args: { file_path: AUTH_JSON } } },
        tenant: { kind: 'llm', session: SESSION, agent: 'smoke' },
        ingress: 'llm',
        sessionId: SESSION,
        requestId: rid,
      },
    }));
    const r = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === rid, 15000, 'irResult (llm gate)');
    const p = r.response || {};
    check(
      'llm ingress dev:tool:read → invalid / router.invalid_args(reason=audience) (no LLM side door)',
      p.status === 'invalid' && p.exit === 2 && p.error?.code === 'router.invalid_args' &&
        p.error?.details?.reason === 'audience',
      `status=${p.status} code=${p.error?.code} reason=${p.error?.details?.reason}`
    );
  }

  // ── 4. 档位自适应审批腿（await_approval ⇒ 铸凭据重提交；done ⇒ 附注）──────────
  {
    const plan = { call: { name: 'dev:tool:read', args: { file_path: AUTH_JSON } } };
    const p1 = await submitDirect(conn, plan, 'smoke-ir-tools-ask-1');
    if (p1.status === 'await_approval') {
      const ap = p1.approval || {};
      const now = new Date();
      const exp = new Date(now.getTime() + 10 * 60 * 1000);
      const cred = {
        planDigest: ap.planDigest,
        capsDigest: ap.capsDigest,
        approvedBy: 'human:console',
        approvedAt: now.toISOString(),
        expiresAt: exp.toISOString(),
        nonce: 'smoke-' + Math.random().toString(36).slice(2),
      };
      const p2 = await submitDirect(conn, plan, 'smoke-ir-tools-ask-2', { approval: cred });
      check(
        'confirm-edits: await_approval → approve & resubmit → done (Ask is the only gate, execution is real)',
        p2.status === 'done' && p2.exit === 0,
        `status=${p2.status} exit=${p2.exit}`
      );
      check('confirm-edits: approval body carries command/caps/rule',
        (ap.nodes || [])[0]?.command === 'dev:tool:read' && JSON.stringify(ap.nodes || []).includes('FsRead(*)') &&
          ap.rule === 'bridge:opaque-host',
        `rule=${ap.rule}`);
    } else {
      check(
        'policy Ask leg: auto-all default folds Ask→Allow (done directly) — confirm-edits path pinned by spec',
        p1.status === 'done' && p1.exit === 0,
        `status=${p1.status} exit=${p1.exit}`
      );
      console.log('NOTE  instance runs auto-all (startup default) — the await_approval leg is covered by IrToolBridgeSpec (ConfirmEdits)');
    }
  }

  // ── 5. 审计一条：<HOME>/logs/ir/<yyyyMMdd>.jsonl（§8.6：digest 在、args 原文不在）──
  {
    const day = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const auditPath = join(HOME, 'logs', 'ir', `${day}.jsonl`);
    let rec = null;
    if (existsSync(auditPath)) {
      const lines = readFileSync(auditPath, 'utf8').split('\n').filter((l) => l.trim());
      for (const line of lines) {
        try {
          const j = JSON.parse(line);
          if (j.command === 'dev:tool:nodelist') rec = j;
        } catch { /* skip partial line */
        }
      }
    }
    check(
      `audit ${day}.jsonl: dev:tool:nodelist record with decision/caps/argsDigest, no raw args`,
      rec !== null && ['allow', 'ask'].includes(rec.decision) && Array.isArray(rec.caps) &&
        rec.caps.includes('FsRead(*)') && typeof rec.argsDigest === 'string' && rec.argsDigest.length > 0 &&
        !('args' in rec) && typeof rec.requestId === 'string' && rec.requestId.length > 0,
      rec ? `decision=${rec.decision} caps=${JSON.stringify(rec.caps)}` : `no record in ${auditPath}`
    );
  }

  // ── 6. dev:fs:* 零扰动（askRule 名模式只罩 dev:tool:*）────────────────────────
  {
    const p = await submitDirect(conn, { call: { name: 'dev:fs:ls', args: {} } }, 'smoke-ir-tools-fs');
    check('dev:fs:ls unchanged → done / exit=0 (askRule scopes to dev:tool:* only)',
      p.status === 'done' && p.exit === 0,
      `status=${p.status} exit=${p.exit}`);
  }
} finally {
  conn.ws.close();
}

console.log(failed === 0 ? '# ALL PASS' : `# FAILED: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
