const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const state = {
  selected: null,
  initData: tg?.initData || "",
};

const el = {
  userLine: document.getElementById('userLine'),
  totalValue: document.getElementById('totalValue'),
  totalPnl: document.getElementById('totalPnl'),
  positions: document.getElementById('positions'),
  alerts: document.getElementById('alerts'),
  assetType: document.getElementById('assetType'),
  searchInput: document.getElementById('searchInput'),
  searchResults: document.getElementById('searchResults'),
  targetPrice: document.getElementById('targetPrice'),
  rangePercent: document.getElementById('rangePercent'),
  addAlertBtn: document.getElementById('addAlertBtn'),
  selectedLine: document.getElementById('selectedLine'),
};

function money(v) {
  const n = Number(v || 0);
  return n.toLocaleString('ru-RU', { maximumFractionDigits: 2 }) + ' ₽';
}

function pct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return 'н/д';
  const n = Number(v);
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`;
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.initData) headers.set('X-Telegram-Init-Data', state.initData);
  const res = await fetch(path, { ...options, headers });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const body = await res.json();
  if (!body.ok) throw new Error('API error');
  return body.data;
}

function renderPositions(rows) {
  el.positions.innerHTML = '';
  if (!rows.length) {
    el.positions.innerHTML = '<p class="hint">Позиции не найдены</p>';
    return;
  }
  rows.slice(0, 20).forEach((row) => {
    const item = document.createElement('div');
    item.className = 'item';
    item.innerHTML = `
      <div class="left">
        <div class="name">${row.name || row.ticker}</div>
        <div class="sub">${row.ticker} · ${row.qty?.toFixed ? row.qty.toFixed(2) : row.qty}</div>
      </div>
      <div class="right">
        <div>${money(row.value)}</div>
        <div class="pnl ${Number(row.ret_30d || 0) >= 0 ? 'plus' : 'minus'}">${pct(row.ret_30d)}</div>
      </div>
    `;
    el.positions.appendChild(item);
  });
}

function renderAlerts(alerts) {
  el.alerts.innerHTML = '';
  if (!alerts.length) {
    el.alerts.innerHTML = '<p class="hint">Нет активных алертов</p>';
    return;
  }
  alerts.forEach((a) => {
    const item = document.createElement('div');
    item.className = 'item';
    const label = `${a.shortname || a.secid} (${a.secid})`;
    const range = Number(a.range_percent || 0) > 0 ? `±${a.range_percent}%` : 'точно';
    item.innerHTML = `
      <div class="left">
        <div class="name">${label}</div>
        <div class="sub">${money(a.target_price)} · ${range}</div>
      </div>
      <div class="right">
        <button class="btn danger" data-id="${a.id}">Отключить</button>
      </div>
    `;
    item.querySelector('button').addEventListener('click', async () => {
      try {
        await api(`/api/miniapp/alerts/${a.id}`, { method: 'DELETE' });
        await loadAlerts();
      } catch (e) {
        alert('Не удалось отключить алерт');
      }
    });
    el.alerts.appendChild(item);
  });
}

function renderSearch(items) {
  el.searchResults.innerHTML = '';
  items.forEach((s) => {
    const item = document.createElement('div');
    item.className = 'item';
    const label = `${s.shortname || s.name || s.secid} (${s.secid})`;
    item.innerHTML = `
      <div class="left"><div class="name">${label}</div><div class="sub">${s.boardid || ''}</div></div>
      <div class="right"><button class="btn primary">Выбрать</button></div>
    `;
    item.querySelector('button').addEventListener('click', () => {
      state.selected = s;
      el.selectedLine.textContent = `Выбрано: ${label}`;
    });
    el.searchResults.appendChild(item);
  });
}

async function loadPortfolio() {
  const data = await api('/api/miniapp/portfolio');
  el.totalValue.textContent = money(data.summary.total_value || 0);
  const pnl = Number(data.summary.pnl_pct || 0);
  el.totalPnl.textContent = pct(pnl);
  el.totalPnl.style.color = pnl >= 0 ? '#1ea86c' : '#d24a79';
  renderPositions(data.positions || []);
}

async function loadAlerts() {
  const alerts = await api('/api/miniapp/alerts');
  renderAlerts(alerts || []);
}

let searchTimer = null;
el.searchInput.addEventListener('input', () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(async () => {
    const q = el.searchInput.value.trim();
    if (!q) {
      el.searchResults.innerHTML = '';
      return;
    }
    try {
      const data = await api(`/api/miniapp/search?q=${encodeURIComponent(q)}&asset_type=${encodeURIComponent(el.assetType.value)}`);
      renderSearch(data || []);
    } catch (_) {
      el.searchResults.innerHTML = '<p class="hint">Ошибка поиска</p>';
    }
  }, 350);
});

el.addAlertBtn.addEventListener('click', async () => {
  if (!state.selected) {
    alert('Сначала выберите инструмент');
    return;
  }
  const targetPrice = Number(el.targetPrice.value);
  const rangePercent = Number(el.rangePercent.value || 5);
  if (!targetPrice || targetPrice <= 0) {
    alert('Введите корректную цену');
    return;
  }
  try {
    await api('/api/miniapp/alerts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        secid: state.selected.secid,
        shortname: state.selected.shortname || state.selected.name || state.selected.secid,
        isin: state.selected.isin,
        boardid: state.selected.boardid,
        asset_type: el.assetType.value,
        target_price: targetPrice,
        range_percent: rangePercent,
      }),
    });
    el.targetPrice.value = '';
    el.rangePercent.value = '5';
    state.selected = null;
    el.selectedLine.textContent = 'Инструмент не выбран';
    el.searchResults.innerHTML = '';
    el.searchInput.value = '';
    await loadAlerts();
  } catch (e) {
    alert('Не удалось создать алерт');
  }
});

(async function init() {
  try {
    const me = await api('/api/miniapp/me');
    el.userLine.textContent = `Telegram ID: ${me.user_id}`;
    await loadPortfolio();
    await loadAlerts();
  } catch (e) {
    el.userLine.textContent = 'Ошибка авторизации Mini App';
    console.error(e);
  }
})();
