#!/usr/bin/env node
// smoke-ir-tools.mjs — P1-2 批 A（只读七件）+ 批 B（写面八件）+ 批 C（高能力面十五件）
// 「内置 Tool → IR 命令桥接」活实例冒烟（2026-09-29）+ P1-3（LLM ingress 改道）面钉。
//
// 验收面（.zcode/plans/command-ir-standard.md §7/§8 + P1-2 批 A/B/C 方案 + P1-3 方案）：
// dev:tool:* 三十件注册进 IR 命令表、经糖腿/直连 ir 帧真执行；P1-3 audiences 翻转后
// llm 租户直帧过受众面进**策略面**（Ask 把守，与 human 同信任边界——改道闸 IrRoutePort
// 是 llm 面的派发通道）；dev:tool:* 一律 Ask（auto-all 折 Allow，confirm-edits 成
// await_approval）；审计落账；AskUserQuestion 不注册（缺 agentActorRef 槽，缺席优于假门
// ——spec 反向断言钉）。
//
// 钉二十一件事：
//   1. 糖腿 /dev:tool:nodelist（typeless，零参）⇒ done / exit=1 / command.failed +
//      "Missing 'project' parameter"（默认 auto-all 档把 askRule 的 Ask 折 Allow ⇒
//      真执行到工具内确定性错误——桥接链路真通，非 stub）；
//   2. 糖腿 /dev:tool:read --file_path <HOME>/auth.json（named 形参，P1-2 全 named 纪律）
//      ⇒ done / exit=0 / final.text 以 "1\t" 行号形态（真 ReadTool cat -n 输出）；
//      附：--limit 5（number 属性不进糖）⇒ invalid + router.invalid_args(unknown_flag)；
//   3. LLM 面开钉（P1-3，移至腿 4 后跑）：直连 ir 帧（tenant=llm / ingress=llm）调
//      dev:tool:read ⇒ 过受众面进策略面，绝不回 reason=audience：confirm-edits ⇒
//      await_approval（rule=bridge:opaque-host、零执行）；auto-all ⇒ done（同 human 腿）；
//   4. 档位自适应审批腿：直连 ir 帧（human/console）⇒ 回 await_approval（实例被配成
//      confirm-edits）则取审批体铸凭据重提交 ⇒ done；回 done（默认 auto-all）则 PASS 附注；
//   5. 审计：<HOME>/logs/ir/<yyyyMMdd>.jsonl 存在 command=dev:tool:nodelist 的记录，
//      decision/caps（含 FsRead(*)）/argsDigest 齐备、无 args 原文（§8.6）；
//   6. dev:fs:* 零扰动：/dev:fs:ls 仍 done/exit=0（askRule 只罩 dev:tool:*）。
//  ── 批 B（写面八件，7-13）──────────────────────────────────────────────────
//   7. 糖腿 /dev:tool:write --file_path <HOME>/ir-smoke-batchb.txt --content …
//      ⇒ done / exit=0 / final.text 以 OK:CREATED 开头（真 WriteTool 落盘）；
//   8. 回读对账：/dev:tool:read 同路径 ⇒ "1\tir-smoke-batchb"（写-读经 IR 全链闭环）；
//   9. 写面档位自适应审批腿（同 4 的两态自适应，载荷换 dev:tool:write）：confirm-edits
//      ⇒ await_approval（审批体含 FsWrite(*)/bridge:opaque-host）⇒ 铸凭据重提交 ⇒ done
//      + 文件真落盘；auto-all ⇒ 直接 done 附 NOTE（Ask 折 Allow 是档位语义）；
//  10. 糖腿 /dev:tool:tasklist --action list ⇒ done / exit=0 / final.text 以 TaskList
//      开头（真实 TaskListTool 读腿，存储根 = 实例 dataRoot）；
//  11. 糖腿 /dev:tool:pop --filePath https://example.com ⇒ done / exit=1 /
//      command.failed / POP_NEBULA_ONLY（桥模板无身份 ⇒ 身份闸 fail-closed 恒拒、零副作用）；
//  12. LLM 面开钉（写面，P1-3）：tenant=llm / ingress=llm 直连 ir 帧调 dev:tool:write ⇒
//      策略把守：confirm-edits ⇒ await_approval 且目标文件零落盘；auto-all ⇒ done 且真写；
//  13. 审计（写面）：<HOME>/logs/ir/<yyyyMMdd>.jsonl 存在 command=dev:tool:write 记录，
//      decision∈{allow,ask}、caps 含 FsWrite(*)、argsDigest 非空、无 args 原文（§8.6）。
//  ── 批 C（高能力面十五件，14-20）───────────────────────────────────────────
//  14. 糖腿 /dev:tool:bash --command hostname ⇒ done / exit=0 / final.text 含 "(cwd:"
//      （真 BashTool 前台腿；糖 tokenize 无引号 ⇒ 命令取单词面，断言锚定 formatResult
//      恒存的 cwd 行而非命令输出内容）；
//  15. 直连 ir 帧真执行：dev:tool:bash args={command:"echo ir-smoke-batchc"} ⇒ exit=0
//      + final.text 含 "ir-smoke-batchc"（直连帧可带空格命令，补糖面之短）；
//  16. 糖腿 /dev:tool:teamtasklist --team ir-smoke-team ⇒ exit=0 + final.text 含
//      "Team tasks"（真实 FileTaskStore 读腿——taskStore 模板槽；隔离实例空队列为
//      期望态，"No team tasks for 'ir-smoke-team'" 亦 PASS）；
//  17. 糖腿 /dev:tool:subtask --prompt smoke --description smoke ⇒ done / exit=1 /
//      command.failed + "No agent definition available"（身份闸先于副作用，零 spawn；
//      注：SubTask schema required=[prompt,description]，两键齐传才进到工具）；
//  18. LLM 面开钉（高能力面，P1-3）：tenant=llm / ingress=llm 直连 ir 帧调 dev:tool:bash ⇒
//      策略把守（confirm-edits ⇒ await_approval 零执行；auto-all ⇒ done；绝不 audience 拒）；
//  19. 审计（高能力面）：<HOME>/logs/ir/<yyyyMMdd>.jsonl 存在 command=dev:tool:bash 记录，
//      decision∈{allow,ask}、caps 含 Exec 与 FsWrite(*)、argsDigest 非空、无 args 原文
//      （§8.6——args.command 是敏感原文，digest-only 断言尤重）；
//  20. dev:fs:* 零扰动复钉（/dev:fs:ls 二跑仍 done/exit=0——批 C 十五件注册不扰旧腿；
//      批 A/B 既有 13 腿在本脚本同跑即复跑）+ 总计数自检（本脚本执行的检查项总数与
//      期望值逐项对账——P1-3 后基线 23/25（档位两态：固定 13 + 4/9 腿 2|4 + 批 C 7 + 自检 1））。
// ── P1-3（LLM ingress 改道，19b）────────────────────────────────────────────
// 19b. 审计（llm 租户）：logs/ir/<yyyyMMdd>.jsonl 存在 ingress=llm 的 dev:tool:read 记录，
//      tenant="llm:<sess>/smoke"、command=IR 名（dev:tool:read）、无 args 原文（§8.6）。
//      注：LLM 改道闸本体（executeTool → IrRoutePort）需真实 LLM 轮，活实例冒烟不驱动
//      模型面——该面的行为钉由 IrLlmGateSpec/IrLlmRouteSpec/IrLlmContractSpec 覆盖；
//      本脚本对 llm 面只钉「直帧腿策略把守 + 审计留痕」两个活实例可证的事实。
//  ── P1-4（策略表数据化 + ext: 过渡注册，21-23）────────────────────────────
//  21. ext: 热装载：写 <HOME>/tools/ir-smoke-ext.json（name "Ir Smoke Ext" ⇒ 规范化
//      ext:tool:ir_smoke_ext）⇒ watcher→reload→钩重装载后糖腿可达——confirm-edits ⇒
//      await_approval（rule=default:no-rule，四件套不⊆{FsRead} 的缺省 Ask）；
//      auto-all ⇒ done/exit=126/router.binding_unavailable/message 含 registration-only
//      （注册面存在、执行面诚实拒答）；轮询至不再是 unknown_command（watcher 500ms
//      debounce + reload）；
// 21b. 审批闭环后仍 126（confirm-edits 态）：铸凭据重提交 ⇒ Ask 折 Allow ⇒ 执行面
//      拒答依旧（126 不是审批能翻的门——binding 缺席非策略判定）；
//  22. ext: 命名空间活且不发明名字：/ext:tool:nope ⇒ invalid/exit=127/
//      router.unknown_command；
//  23. ir.policy 热规则双向：nebflow.json 写 Deny(ext:tool:ir_smoke_ext) ⇒ rejected/
//      exit=125/policy.denied+details.rule=配置串；还原配置后回到缺省 Ask 腿（await_
//      approval 或 done+126）——策略表逐请求热读、改完即生效（写面=读改写保留原键）。
//
// Run: NEBFLOW_URL=http://localhost:8099 NEBFLOW_HOME_DIR=/tmp/nebflow-ir-smoke \
//        node scripts/smoke-ir-tools.mjs
// 注：腿 2/7 的路径要求绝对且不含空格（糖 tokenize 按空白切分，无引号）；
//     腿 21 的 --json 值必须无空格紧凑单 token（同因）。

import { readFileSync, writeFileSync, existsSync, mkdirSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';

const URL_BASE = process.env.NEBFLOW_URL || 'http://localhost:8099';
const HOME = process.env.NEBFLOW_HOME_DIR;
if (!HOME) {
  console.error('FAIL  NEBFLOW_HOME_DIR is required (isolated instance home)');
  process.exit(1);
}
const TOKEN = JSON.parse(readFileSync(join(HOME, 'auth.json'), 'utf8'));

let failed = 0;
let executed = 0;
/** 档位两态标记：腿 4/9 走 await_approval 分支时置 true（各多一条 check）。 */
let confirmMode = false;
function check(name, ok, extra = '') {
  executed++;
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

  // ── 3. LLM 面开钉（P1-3 audiences 翻转）——移至档位判定（腿 4）之后：断言依赖 confirmMode ──

  // ── 4. 档位自适应审批腿（await_approval ⇒ 铸凭据重提交；done ⇒ 附注）──────────
  {
    const plan = { call: { name: 'dev:tool:read', args: { file_path: AUTH_JSON } } };
    const p1 = await submitDirect(conn, plan, 'smoke-ir-tools-ask-1');
    if (p1.status === 'await_approval') {
      confirmMode = true;
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

  // ── 3(移后). LLM 面开钉（P1-3 audiences 翻转）：llm ingress 直连 ir 帧过受众面进──
  //    策略面（不再被 reason=audience 拒；改道闸 IrRoutePort 是 llm 面的派发通道，直帧
  //    腿与 human 同信任边界——策略面把守：confirm-edits ⇒ await_approval，auto-all ⇒ done）
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
    const audienceRejected = p.error?.code === 'router.invalid_args' && p.error?.details?.reason === 'audience';
    check(
      'llm ingress dev:tool:read → policy face (P1-3 flip): confirm-edits ⇒ await_approval / auto-all ⇒ done, never audience-rejected',
      !audienceRejected &&
        (confirmMode
          ? p.status === 'await_approval' && p.approval?.rule === 'bridge:opaque-host'
          : p.status === 'done' && p.exit === 0),
      `status=${p.status} code=${p.error?.code} reason=${p.error?.details?.reason}`
    );
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

  // ── 7. 批 B 糖腿 /dev:tool:write（真写宿主文件，OK:CREATED 回执）────────────────
  const WRITE_FILE = join(HOME, 'ir-smoke-batchb.txt');
  {
    conn.ws.send(JSON.stringify({
      sessionId: SESSION,
      content: `/dev:tool:write --file_path ${WRITE_FILE.replace(/\\/g, '/')} --content ir-smoke-batchb`,
    }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:write --file_path --content → done / exit=0 / final.text OK:CREATED (real WriteTool)',
      p.status === 'done' && p.exit === 0 && typeof p.final?.text === 'string' && p.final.text.startsWith('OK:CREATED'),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 8. 回读对账：写-读经 IR 全链闭环（"1\tir-smoke-batchb" 行号形态）───────────
  {
    conn.ws.send(JSON.stringify({
      sessionId: SESSION,
      content: `/dev:tool:read --file_path ${WRITE_FILE.replace(/\\/g, '/')}`,
    }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:read back → done / exit=0 / final.text "1\\tir-smoke-batchb" (write-read roundtrip)',
      p.status === 'done' && p.exit === 0 && typeof p.final?.text === 'string' &&
        p.final.text.startsWith('1\tir-smoke-batchb'),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 9. 写面档位自适应审批腿（两态：confirm-edits ⇒ 审批闭环；auto-all ⇒ 直接 done）──
  {
    const askFile = join(HOME, 'ir-smoke-batchb-ask.txt');
    const plan = { call: { name: 'dev:tool:write', args: { file_path: askFile.replace(/\\/g, '/'), content: 'ask-leg' } } };
    const p1 = await submitDirect(conn, plan, 'smoke-ir-tools-write-ask-1');
    if (p1.status === 'await_approval') {
      confirmMode = true;
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
      const p2 = await submitDirect(conn, plan, 'smoke-ir-tools-write-ask-2', { approval: cred });
      check(
        'confirm-edits write: await_approval → approve & resubmit → done + file really written',
        p2.status === 'done' && p2.exit === 0 && existsSync(askFile),
        `status=${p2.status} exit=${p2.exit} file=${existsSync(askFile)}`
      );
      check('confirm-edits write: approval body carries command/caps/rule',
        (ap.nodes || [])[0]?.command === 'dev:tool:write' && JSON.stringify(ap.nodes || []).includes('FsWrite(*)') &&
          ap.rule === 'bridge:opaque-host',
        `rule=${ap.rule}`);
    } else {
      check(
        'write Ask leg: auto-all default folds Ask→Allow (done directly) — confirm-edits path pinned by spec',
        p1.status === 'done' && p1.exit === 0 && existsSync(askFile),
        `status=${p1.status} exit=${p1.exit} file=${existsSync(askFile)}`
      );
      console.log('NOTE  instance runs auto-all (startup default) — the await_approval write leg is covered by IrToolBridgeSpec (ConfirmEdits)');
    }
  }

  // ── 10. 糖腿 /dev:tool:tasklist --action list（真实 TaskListTool 读腿）──────────
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:tool:tasklist --action list' }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:tasklist --action list → done / exit=0 / final.text "TaskList" header (real store read)',
      p.status === 'done' && p.exit === 0 && typeof p.final?.text === 'string' && p.final.text.startsWith('TaskList'),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 11. 糖腿 /dev:tool:pop（桥模板无身份 ⇒ POP_NEBULA_ONLY 恒拒、零副作用）──────
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:tool:pop --filePath https://example.com' }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:pop → done / exit=1 / command.failed / POP_NEBULA_ONLY (identity gate fail-closed)',
      p.status === 'done' && p.exit === 1 && p.error?.code === 'command.failed' &&
        (p.error?.message || '').includes('POP_NEBULA_ONLY'),
      `status=${p.status} exit=${p.exit} code=${p.error?.code}`
    );
  }

  // ── 12. LLM 面开钉（写面，P1-3）：llm ingress 直连 ir 帧 ⇒ 策略面把守────────────
  //    （audiences 翻转后不再 audience 拒：confirm-edits ⇒ await_approval 且零写；
  //     auto-all ⇒ done 且文件真落盘——与 human 腿同信任边界、同策略）
  {
    const rid = 'smoke-ir-tools-llm-write';
    const LLM_WRITE = join(HOME, 'ir-smoke-llm-write.txt').replace(/\\/g, '/');
    conn.ws.send(JSON.stringify({
      type: 'ir',
      sessionId: SESSION,
      request: {
        ir: 1,
        plan: { call: { name: 'dev:tool:write', args: { file_path: LLM_WRITE, content: 'llm-smoke' } } },
        tenant: { kind: 'llm', session: SESSION, agent: 'smoke' },
        ingress: 'llm',
        sessionId: SESSION,
        requestId: rid,
      },
    }));
    const r = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === rid, 15000, 'irResult (llm write gate)');
    const p = r.response || {};
    const audienceRejected = p.error?.code === 'router.invalid_args' && p.error?.details?.reason === 'audience';
    check(
      'llm ingress dev:tool:write → policy face (P1-3 flip): confirm-edits ⇒ await_approval+zero write / auto-all ⇒ done+real write',
      !audienceRejected &&
        (confirmMode
          ? p.status === 'await_approval' && !existsSync(LLM_WRITE)
          : p.status === 'done' && p.exit === 0 && existsSync(LLM_WRITE)),
      `status=${p.status} code=${p.error?.code} reason=${p.error?.details?.reason} file=${existsSync(LLM_WRITE)}`
    );
  }

  // ── 13. 审计（写面）：<HOME>/logs/ir/<yyyyMMdd>.jsonl 的 dev:tool:write 记录（§8.6）──
  {
    const day = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const auditPath = join(HOME, 'logs', 'ir', `${day}.jsonl`);
    let rec = null;
    if (existsSync(auditPath)) {
      const lines = readFileSync(auditPath, 'utf8').split('\n').filter((l) => l.trim());
      for (const line of lines) {
        try {
          const j = JSON.parse(line);
          if (j.command === 'dev:tool:write') rec = j;
        } catch { /* skip partial line */
        }
      }
    }
    check(
      `audit ${day}.jsonl: dev:tool:write record with decision/caps/argsDigest, no raw args`,
      rec !== null && ['allow', 'ask'].includes(rec.decision) && Array.isArray(rec.caps) &&
        rec.caps.includes('FsWrite(*)') && typeof rec.argsDigest === 'string' && rec.argsDigest.length > 0 &&
        !('args' in rec) && typeof rec.requestId === 'string' && rec.requestId.length > 0,
      rec ? `decision=${rec.decision} caps=${JSON.stringify(rec.caps)}` : `no record in ${auditPath}`
    );
  }

  // ── 14. 批 C 糖腿 /dev:tool:bash（真 BashTool 前台腿，锚定恒存 cwd 行）──────────
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:tool:bash --command hostname' }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:bash --command hostname → done / exit=0 / final.text has "(cwd:" (real BashTool foreground)',
      p.status === 'done' && p.exit === 0 && typeof p.final?.text === 'string' && p.final.text.includes('(cwd:'),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 15. 直连 ir 帧真执行：dev:tool:bash echo（帧内命令可带空格，补糖面之短）───────
  {
    const p = await submitDirect(
      conn,
      { call: { name: 'dev:tool:bash', args: { command: 'echo ir-smoke-batchc' } } },
      'smoke-ir-tools-bash-direct'
    );
    check(
      "direct ir dev:tool:bash {command:'echo ir-smoke-batchc'} → done / exit=0 / marker in final.text",
      p.status === 'done' && p.exit === 0 && typeof p.final?.text === 'string' &&
        p.final.text.includes('ir-smoke-batchc'),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 16. 糖腿 /dev:tool:teamtasklist --team ir-smoke-team（真实 FileTaskStore 读腿）──
  {
    conn.ws.send(JSON.stringify({ sessionId: SESSION, content: '/dev:tool:teamtasklist --team ir-smoke-team' }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    const t = typeof p.final?.text === 'string' ? p.final.text : '';
    check(
      "sugar /dev:tool:teamtasklist --team ir-smoke-team → done / exit=0 / team render (empty queue is the expected state)",
      p.status === 'done' && p.exit === 0 && (t.includes("No team tasks for 'ir-smoke-team'") || t.includes('Team tasks')),
      `status=${p.status} exit=${p.exit} final=${JSON.stringify(p.final || {}).slice(0, 60)}`
    );
  }

  // ── 17. 糖腿 /dev:tool:subtask（确定性拒答：身份闸先于副作用，零 spawn）───────────
  {
    conn.ws.send(JSON.stringify({
      sessionId: SESSION,
      content: '/dev:tool:subtask --prompt smoke --description smoke',
    }));
    const r = await nextIrResult(conn);
    const p = r.response || {};
    check(
      'sugar /dev:tool:subtask → done / exit=1 / command.failed / "No agent definition available" (identity gate, zero spawn)',
      p.status === 'done' && p.exit === 1 && p.error?.code === 'command.failed' &&
        (p.error?.message || '').includes('No agent definition available'),
      `status=${p.status} exit=${p.exit} code=${p.error?.code}`
    );
  }

  // ── 18. LLM 面开钉（高能力面，P1-3）：llm ingress 直连 ir 帧调 dev:tool:bash ⇒ 策略把守──
  {
    const rid = 'smoke-ir-tools-llm-bash';
    conn.ws.send(JSON.stringify({
      type: 'ir',
      sessionId: SESSION,
      request: {
        ir: 1,
        plan: { call: { name: 'dev:tool:bash', args: { command: 'echo side-door' } } },
        tenant: { kind: 'llm', session: SESSION, agent: 'smoke' },
        ingress: 'llm',
        sessionId: SESSION,
        requestId: rid,
      },
    }));
    const r = await waitFor(conn, (m) => m.type === 'irResult' && m.requestId === rid, 15000, 'irResult (llm bash gate)');
    const p = r.response || {};
    const audienceRejected = p.error?.code === 'router.invalid_args' && p.error?.details?.reason === 'audience';
    check(
      'llm ingress dev:tool:bash → policy face (P1-3 flip): confirm-edits ⇒ await_approval / auto-all ⇒ done, never audience-rejected',
      !audienceRejected &&
        (confirmMode
          ? p.status === 'await_approval'
          : p.status === 'done' && p.exit === 0),
      `status=${p.status} code=${p.error?.code} reason=${p.error?.details?.reason}`
    );
  }

  // ── 19. 审计（高能力面）：dev:tool:bash 记录（caps 含 Exec 与 FsWrite(*)；§8.6 digest-only）──
  {
    const day = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const auditPath = join(HOME, 'logs', 'ir', `${day}.jsonl`);
    let rec = null;
    if (existsSync(auditPath)) {
      const lines = readFileSync(auditPath, 'utf8').split('\n').filter((l) => l.trim());
      for (const line of lines) {
        try {
          const j = JSON.parse(line);
          if (j.command === 'dev:tool:bash') rec = j;
        } catch { /* skip partial line */
        }
      }
    }
    check(
      `audit ${day}.jsonl: dev:tool:bash record with decision/caps(Exec+FsWrite(*))/argsDigest, no raw args`,
      rec !== null && ['allow', 'ask'].includes(rec.decision) && Array.isArray(rec.caps) &&
        rec.caps.includes('Exec') && rec.caps.includes('FsWrite(*)') &&
        typeof rec.argsDigest === 'string' && rec.argsDigest.length > 0 &&
        !('args' in rec) && typeof rec.requestId === 'string' && rec.requestId.length > 0,
      rec ? `decision=${rec.decision} caps=${JSON.stringify(rec.caps)}` : `no record in ${auditPath}`
    );
  }

  // ── 19b. 审计（P1-3）：llm 租户直帧腿的记录 tenant=llm:<sess>/<agent>、ingress=llm ──
  {
    const day = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const auditPath = join(HOME, 'logs', 'ir', `${day}.jsonl`);
    let rec = null;
    if (existsSync(auditPath)) {
      const lines = readFileSync(auditPath, 'utf8').split('\n').filter((l) => l.trim());
      for (const line of lines) {
        try {
          const j = JSON.parse(line);
          if (j.command === 'dev:tool:read' && j.ingress === 'llm') rec = j;
        } catch { /* skip partial line */
        }
      }
    }
    check(
      `audit ${day}.jsonl: llm-tenant dev:tool:read record with tenant=llm:${SESSION}/smoke, ingress=llm, command=IR name`,
      rec !== null && rec.tenant === `llm:${SESSION}/smoke` && rec.ingress === 'llm' &&
        rec.command === 'dev:tool:read' && ['allow', 'ask'].includes(rec.decision) &&
        !('args' in rec),
      rec ? `tenant=${rec.tenant} decision=${rec.decision}` : `no record in ${auditPath}`
    );
  }

  // ── 21. P1-4 ext: 过渡注册：热装载 tool.json ⇒ watcher→reload→钩 ⇒ ext: 名在册 ──
  //    执行面诚实拒答（126/binding_unavailable），策略面照常把守（缺省 default:no-rule Ask）
  const EXT_NAME = 'ext:tool:ir_smoke_ext'; // Names.normalize("Ir Smoke Ext")
  let extResp = null;
  {
    const extJson = join(HOME, 'tools', 'ir-smoke-ext.json');
    mkdirSync(dirname(extJson), { recursive: true });
    writeFileSync(extJson, JSON.stringify({
      name: 'Ir Smoke Ext',
      description: 'P1-4 smoke ext tool (registration-only execution face)',
      command: 'echo ext',
      inputSchema: { type: 'object', properties: {} },
    }), 'utf8');
    // watcher 500ms debounce + reload；轮询糖腿直到不再是 unknown_command（最多 ~9s）
    for (let i = 0; i < 10; i++) {
      conn.ws.send(JSON.stringify({ sessionId: SESSION, content: `/${EXT_NAME} --json {}` })); // 糖腿须 / 前缀
      const r = await nextIrResult(conn); // sugar: /ext:tool:…（首段 ext ∈ Namespaces）
      extResp = r.response || {};
      if (extResp.status !== 'invalid') break;
      await new Promise((res) => setTimeout(res, 800));
    }
    check(
      `sugar ${EXT_NAME} hot-loaded → confirm-edits ⇒ await_approval(default:no-rule) / auto-all ⇒ done+126 binding_unavailable`,
      extResp.status !== 'invalid' && (confirmMode
        ? extResp.status === 'await_approval' && extResp.approval?.rule === 'default:no-rule'
        : extResp.status === 'done' && extResp.exit === 126 &&
          extResp.error?.code === 'router.binding_unavailable' &&
          (extResp.error?.message || '').includes('registration-only')),
      `status=${extResp?.status} exit=${extResp?.exit} code=${extResp?.error?.code} rule=${extResp?.approval?.rule || ''}`
    );
  }

  // ── 21b. 审批闭环后仍 126（confirm-edits 态）：Ask 折 Allow ⇒ 执行面拒答依旧 ──────
  {
    if (confirmMode && extResp?.status === 'await_approval') {
      const ap = extResp.approval || {};
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
      const p = await submitDirect(conn, { call: { name: EXT_NAME, args: {} } }, 'smoke-ir-ext-approve', { approval: cred });
      check(
        'ext approve & resubmit → done / exit=126 / binding_unavailable (binding absence is not an approval gate)',
        p.status === 'done' && p.exit === 126 && p.error?.code === 'router.binding_unavailable' &&
          (p.error?.message || '').includes('registration-only'),
        `status=${p.status} exit=${p.exit} code=${p.error?.code}`
      );
    }
  }

  // ── 22. ext: 命名空间活且不发明名字（unknown ext ⇒ 127，与 dev: 同码）──────────
  {
    const p = await submitDirect(conn, { call: { name: 'ext:tool:nope', args: {} } }, 'smoke-ir-ext-unknown');
    check('ext namespace live: unknown ext name → invalid / exit=127 / router.unknown_command',
      p.status === 'invalid' && p.exit === 127 && p.error?.code === 'router.unknown_command',
      `status=${p.status} exit=${p.exit} code=${p.error?.code}`);
  }

  // ── 23. ir.policy 热规则双向（读改写保留原键；还原后回到缺省腿）──────────────────
  {
    const cfgPath = join(HOME, 'nebflow.json');
    const existed = existsSync(cfgPath);
    const original = existed ? readFileSync(cfgPath, 'utf8') : null;
    let cfgObj = {};
    try { cfgObj = existed ? JSON.parse(original) : {}; } catch { cfgObj = {}; }
    cfgObj.ir = {
      ...(cfgObj.ir || {}),
      policy: { rules: [{ name: EXT_NAME, decision: 'deny', reason: 'smoke deny', rule: 's:ext' }] },
    };
    writeFileSync(cfgPath, JSON.stringify(cfgObj), 'utf8');
    const p = await submitDirect(conn, { call: { name: EXT_NAME, args: {} } }, 'smoke-ir-ext-deny');
    check('ir.policy hot rules: Deny(ext:tool:ir_smoke_ext) → rejected / exit=125 / policy.denied with configured rule',
      p.status === 'rejected' && p.exit === 125 && p.error?.code === 'policy.denied' &&
        p.error?.details?.rule === 's:ext',
      `status=${p.status} exit=${p.exit} code=${p.error?.code} rule=${p.error?.details?.rule}`);
    // 还原（热读双向：deny 移除即回到缺省 Ask 腿）
    if (existed) writeFileSync(cfgPath, original, 'utf8'); else rmSync(cfgPath);
    const p2 = await submitDirect(conn, { call: { name: EXT_NAME, args: {} } }, 'smoke-ir-ext-restore');
    check('ir.policy restore: deny removed → back to default:no-rule leg (await_approval | done+126)',
      confirmMode
        ? p2.status === 'await_approval' && p2.approval?.rule === 'default:no-rule'
        : p2.status === 'done' && p2.exit === 126 && p2.error?.code === 'router.binding_unavailable',
      `status=${p2.status} exit=${p2.exit} rule=${p2.approval?.rule || p2.error?.code}`);
  }

  // ── 20. dev:fs:* 零扰动复钉（批 C 十五件注册不扰旧腿）+ 总计数自检 ───────────────
  {
    const p = await submitDirect(conn, { call: { name: 'dev:fs:ls', args: {} } }, 'smoke-ir-tools-fs-batchc');
    check('dev:fs:ls unchanged after batch C registration → done / exit=0 (askRule scopes to dev:tool:* only)',
      p.status === 'done' && p.exit === 0,
      `status=${p.status} exit=${p.exit}`);
    // 总计数自检：固定腿 13（1,2,2b,3,5,6,7,8,10,11,12,13,19b）+ 档位两态腿 4/9
    // （auto-all 各 1、confirm-edits 各 2）+ 批 C 七腿（14-20）+ P1-4 五腿
    // （21 一条 + 21b confirm-only 一条 + 22 一条 + 23 两条）+ 本自检 1。
    // P1-3：腿 3/12/18 仍各恰一条 check（断言按档位两态分叉），新增 19b 一条。
    const expected = 13 + (confirmMode ? 4 : 2) + 7 + (4 + (confirmMode ? 1 : 0)) + 1;
    check(`self-count: executed checks (${executed + 1}) === expected (${expected}, mode=${confirmMode ? 'confirm-edits' : 'auto-all'})`,
      executed + 1 === expected,
      `executed=${executed} expected=${expected}`);
  }
} finally {
  conn.ws.close();
}

console.log(failed === 0 ? `# ALL PASS (${executed} checks)` : `# FAILED: ${failed}/${executed}`);
process.exit(failed === 0 ? 0 : 1);
