// sidebar.js 拆分(FE组件化批次二 2026-09-27):预设管理 UI 可复用模块(行为保持)。
// Preset management (P3) 段:presetUsedBy/renderPresetCard/loadPresetsSection/
// showMigrationDialog/showPresetModal(sidebar.js 原 :1007-1260)。
import { t } from '../i18n.js';
import * as presets from '../presets.js';
import { escapeHtml } from './providersUi.js';

// ---------- Preset management section (P3) ----------

/** Reverse-lookup: which agents reference a given preset name. */
function presetUsedBy(agentsMap, presetName) {
  return Object.entries(agentsMap || {})
    .filter(([, v]) => v === presetName)
    .map(([k]) => k)
    .sort();
}

function renderPresetCard(p, defaultPreset, agentsMap) {
  const isDefault = p.name === defaultPreset;
  const usedBy = presetUsedBy(agentsMap, p.name);
  const chain = [p.preferred, ...(p.fallbacks || [])].filter(Boolean);
  const usedByHtml = usedBy.length > 0
    ? `<span class="preset-card-agents" title="${escapeHtml(usedBy.join(', '))}">${t('preset.usedBy', { n: usedBy.length })}</span>`
    : `<span class="preset-card-agents">${t('preset.usedByNone')}</span>`;
  return `
    <div class="cfg-card preset-card" data-preset="${escapeHtml(p.name)}">
      <div class="cfg-card-header">
        <span class="cfg-card-title">${escapeHtml(p.name)}</span>
        ${isDefault ? `<span class="preset-default-badge">${t('preset.default')}</span>` : ''}
      </div>
      <div class="preset-card-body">
        ${p.description ? `<div class="preset-card-desc">${escapeHtml(p.description)}</div>` : ''}
        ${presets.presetChainHtml(chain, '')}
        <div class="preset-card-footer">
          ${usedByHtml}
          <span class="preset-card-actions">
            <button class="cfg-btn cfg-btn-sm" data-action="default" ${isDefault ? 'disabled' : ''}>${t('preset.setDefault')}</button>
            <button class="cfg-btn cfg-btn-sm" data-action="edit">${t('preset.edit')}</button>
            <button class="cfg-btn cfg-btn-sm" data-action="delete" ${isDefault ? 'disabled' : ''}>${t('preset.delete')}</button>
          </span>
        </div>
      </div>
    </div>`;
}

/** Load presets + agent mapping, render cards, bind section events. */
async function loadPresetsSection() {
  const listEl = document.getElementById('preset-list');
  if (!listEl) return; // settings panel not open

  const data = await presets.fetchPresets();
  if (!document.getElementById('preset-list')) return; // panel closed while fetching
  if (!data) {
    listEl.innerHTML = `<div class="cfg-empty">${t('preset.loadFailed')}</div>`;
    return;
  }

  const presetList = data.presets || [];
  const defaultPreset = data.defaultPreset || '';
  const agentsMap = data.agents || {};

  listEl.innerHTML = presetList.length > 0
    ? presetList.map(p => renderPresetCard(p, defaultPreset, agentsMap)).join('')
    : `<div class="cfg-empty">${t('preset.empty')}</div>`;

  // Card action buttons (delegation on the list container)
  listEl.onclick = async (e) => {
    const btn = e.target.closest('button[data-action]');
    if (!btn || btn.disabled) return;
    const card = btn.closest('.preset-card');
    const name = card?.dataset.preset;
    const preset = presetList.find(p => p.name === name);
    if (!preset) return;

    if (btn.dataset.action === 'default') {
      try {
        await presets.setDefaultPreset(name);
        loadPresetsSection();
      } catch (err) { window.__showToast?.(err.message, 'error'); }
    } else if (btn.dataset.action === 'edit') {
      showPresetModal(preset, () => loadPresetsSection());
    } else if (btn.dataset.action === 'delete') {
      const usedBy = presetUsedBy(agentsMap, name);
      const msg = usedBy.length > 0
        ? t('preset.deleteConfirm', { name, n: usedBy.length })
        : t('preset.deleteConfirmNone', { name });
      window.__showConfirm?.(t('preset.deleteTitle'), msg, async () => {
        try {
          await presets.deletePreset(name);
          loadPresetsSection();
        } catch (err) { window.__showToast?.(err.message, 'error'); }
      });
    }
  };

  // Add-preset button (recreated each renderSettings — bind here)
  const addBtn = document.getElementById('btn-add-preset');
  if (addBtn) addBtn.onclick = () => showPresetModal(null, () => loadPresetsSection());

  // Legacy migration banner → P5 migration dialog
  presets.detectLegacyAgents().then(legacy => {
    const banner = document.getElementById('preset-migrate-banner');
    if (!banner) return;
    if (legacy.length === 0) { banner.style.display = 'none'; return; }
    banner.style.display = '';
    banner.innerHTML = `
      <div class="preset-migrate-inner">
        <span class="preset-migrate-text">⚠ ${t('preset.migrateBanner', { n: legacy.length })}</span>
        <button class="cfg-btn cfg-btn-sm" id="btn-migrate-legacy">${t('preset.migrateAction')}</button>
      </div>`;
    banner.querySelector('#btn-migrate-legacy')?.addEventListener('click', () => {
      showMigrationDialog(legacy, () => loadPresetsSection());
    });
  });
}

/**
 * Legacy migration dialog (P5). Stage 1: local preview grouped by config
 * fingerprint. Stage 2: POST /api/presets/migrate-legacy, then refresh.
 */
async function showMigrationDialog(legacyAgents, onDone) {
  document.getElementById('cfg-modal')?.remove();

  const overlay = document.createElement('div');
  overlay.id = 'cfg-modal';
  overlay.className = 'cfg-modal-overlay';
  overlay.innerHTML = `
    <div class="cfg-modal">
      <div class="cfg-modal-title">${t('preset.migrateTitle')}</div>
      <div class="cfg-modal-body" id="preset-migrate-body">
        <div class="cfg-empty">Loading…</div>
      </div>
      <div class="cfg-modal-actions">
        <button class="cfg-btn cfg-btn-cancel" id="cfg-modal-cancel">${t('modal.cancel')}</button>
        <button class="cfg-btn cfg-btn-save" id="preset-migrate-run">${t('preset.migrateExecute')}</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.querySelector('#cfg-modal-cancel').addEventListener('click', close);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

  // Stage 1 — preview (local computation; existing names needed for mig-<n>)
  const body = overlay.querySelector('#preset-migrate-body');
  const presetData = await presets.fetchPresets();
  if (!overlay.isConnected) return;
  const groups = presets.previewMigration(legacyAgents, presetData);
  const newCount = groups.filter(g => !g.reused).length;

  body.innerHTML = `
    <div class="preset-migrate-summary">${t('preset.migrateSummary', { agents: legacyAgents.length, presets: newCount })}</div>
    ${groups.map(g => `
      <div class="preset-migrate-group">
        <div class="preset-migrate-group-head">
          <span class="preset-migrate-group-name">「${escapeHtml(g.presetName)}」</span>
          ${g.reused ? `<span class="preset-migrate-reuse">${t('preset.migrateReuse')}</span>` : ''}
          <span class="preset-migrate-group-arrow">←</span>
          <span class="preset-migrate-group-agents">${escapeHtml(g.agents.join(', '))}</span>
        </div>
        ${presets.presetChainHtml([g.preferred, ...g.fallbacks].filter(Boolean), '')}
      </div>`).join('')}
    <div class="preset-migrate-note">${t('preset.migrateNote')}</div>
    <div class="preset-migrate-error" id="preset-migrate-error" style="display:none"></div>`;

  // Stage 2 — execute
  const runBtn = overlay.querySelector('#preset-migrate-run');
  runBtn.addEventListener('click', async () => {
    runBtn.disabled = true;
    overlay.querySelector('#cfg-modal-cancel').disabled = true;
    runBtn.textContent = t('preset.migrateRunning');
    const errEl = overlay.querySelector('#preset-migrate-error');
    errEl.style.display = 'none';
    try {
      const result = await presets.migrateLegacy(legacyAgents.map(a => a.name));
      const migrated = result?.migratedAgents?.length ?? legacyAgents.length;
      close();
      window.__showToast?.(t('preset.migrateDone', { n: migrated }), 'success');
      onDone?.();
    } catch (err) {
      errEl.textContent = `${t('preset.migrateFailed')}: ${err.message}`;
      errEl.style.display = '';
      runBtn.disabled = false;
      overlay.querySelector('#cfg-modal-cancel').disabled = false;
      runBtn.textContent = t('preset.migrateExecute');
    }
  });
}

/**
 * Preset create/edit modal. Reuses the cfg-modal skeleton; the model chain
 * editor is the shared drag-to-reorder component from presets.js.
 */
function showPresetModal(existing, onSaved) {
  const isEdit = !!existing;
  document.getElementById('cfg-modal')?.remove();

  const overlay = document.createElement('div');
  overlay.id = 'cfg-modal';
  overlay.className = 'cfg-modal-overlay';
  overlay.innerHTML = `
    <div class="cfg-modal">
      <div class="cfg-modal-title">${isEdit ? t('preset.editTitle', { name: existing.name }) : t('preset.addTitle')}</div>
      <div class="cfg-modal-body">
        <div class="cfg-form-group">
          <label class="cfg-label">${t('preset.fieldName')}</label>
          <input class="cfg-input" data-field="name" type="text" value="${escapeHtml(existing?.name || '')}" placeholder="vision" ${isEdit ? 'disabled' : ''} autocomplete="off">
        </div>
        <div class="cfg-form-group">
          <label class="cfg-label">${t('preset.fieldDescription')}</label>
          <input class="cfg-input" data-field="description" type="text" value="${escapeHtml(existing?.description || '')}" autocomplete="off">
          <div class="cfg-hint">${t('preset.descriptionHint')}</div>
        </div>
        <div class="cfg-form-group">
          <label class="cfg-label">${t('preset.fieldChain')}</label>
          <div class="preset-drag-hint">${t('preset.dragHint')}</div>
          <div class="preset-chain-editor" id="preset-chain-editor"></div>
        </div>
      </div>
      <div class="cfg-modal-actions">
        <button class="cfg-btn cfg-btn-cancel" id="cfg-modal-cancel">${t('modal.cancel')}</button>
        <button class="cfg-btn cfg-btn-save" id="cfg-modal-save">${t('settings.save')}</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);

  const initialChain = existing ? [existing.preferred, ...(existing.fallbacks || [])].filter(Boolean) : [];
  const editor = presets.renderChainEditor(
    overlay.querySelector('#preset-chain-editor'),
    initialChain,
    presets.getAllModelRefs(),
  );

  const close = () => overlay.remove();
  overlay.querySelector('#cfg-modal-cancel').addEventListener('click', close);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

  overlay.querySelector('#cfg-modal-save').addEventListener('click', async () => {
    const name = overlay.querySelector('[data-field="name"]').value.trim();
    const description = overlay.querySelector('[data-field="description"]').value.trim();
    if (!isEdit) {
      if (!name) { window.__showToast?.(t('preset.nameRequired'), 'error'); return; }
      if (/\s/.test(name)) { window.__showToast?.(t('preset.noSpaces'), 'error'); return; }
    }
    const chain = editor.getChain();
    const body = {
      name: isEdit ? existing.name : name,
      description,
      preferred: chain[0] || null,
      fallbacks: chain.slice(1),
    };
    try {
      if (isEdit) await presets.updatePreset(existing.name, body);
      else await presets.createPreset(body);
      close();
      onSaved?.();
    } catch (err) {
      window.__showToast?.(err.message, 'error');
    }
  });
}

// sidebar.js 的 renderSettings 段(留存主体)单向消费 loadPresetsSection。
export { loadPresetsSection };
