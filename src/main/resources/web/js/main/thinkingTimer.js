// main.js 抽取(FE组件化批次七 2026-09-28):thinking 计时器族 模块(行为保持)。
import state from '../state.js';
import { activeView } from '../chatView.js';
import { t } from '../i18n.js';
import { formatLiveDuration } from '../chat.js';

// ---------- Live thinking timer ----------
let _thinkingTimerInterval = null;
let _thinkingTimerEl = null;

export function startThinkingTimer() {
  stopThinkingTimer(); // clean any previous
  const sid = state.activeSessionId;
  const startTime = state.turnStartTimes[sid];
  if (!startTime) return;

  // Insert timer element into the existing thinking placeholder
  const placeholder = activeView.dom.chat.querySelector('.thinking-placeholder');
  if (!placeholder) return;

  // Remove old thinking text, replace with indicator structure
  placeholder.innerHTML = '';
  const indicator = document.createElement('div');
  indicator.className = 'thinking-indicator';

  // Animated dots
  for (let i = 0; i < 3; i++) {
    const dot = document.createElement('span');
    dot.className = 'thinking-dot';
    if (i === 1) dot.style.animationDelay = '0.15s';
    if (i === 2) dot.style.animationDelay = '0.3s';
    indicator.appendChild(dot);
  }

  // Label
  const label = document.createElement('span');
  label.className = 'thinking-indicator-label';
  label.textContent = t('chat.thinking.now') || '思考中';
  indicator.appendChild(label);

  // Live timer
  const timer = document.createElement('span');
  timer.className = 'thinking-timer';
  timer.textContent = formatLiveDuration(Date.now() - startTime);
  indicator.appendChild(timer);

  placeholder.appendChild(indicator);
  _thinkingTimerEl = timer;

  // Tick every second
  _thinkingTimerInterval = setInterval(() => {
    if (_thinkingTimerEl) {
      _thinkingTimerEl.textContent = formatLiveDuration(Date.now() - startTime);
    }
  }, 1000);
}

export function stopThinkingTimer() {
  if (_thinkingTimerInterval) {
    clearInterval(_thinkingTimerInterval);
    _thinkingTimerInterval = null;
  }
  _thinkingTimerEl = null;
}
