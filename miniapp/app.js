const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
  try {
    tg.disableVerticalSwipes?.();
  } catch (_) {}
}

const state = {
  initData: tg?.initData || "",
  selectedTrade: null,
  selectedLookup: null,
  selectedAlert: null,
  searchControllers: {},
  moversLoaded: false,
  usdLoaded: false,
  loadingCount: 0,
  loadingShowTimer: null,
  loadingVisibleAt: 0,
};

const POPULAR_FIAT_ALERT_PAIRS = [
  { secid: "USDRUB_TOM", boardid: "CETS", shortname: "Доллар США / Российский рубль" },
  { secid: "EURRUB_TOM", boardid: "CETS", shortname: "Евро / Российский рубль" },
  { secid: "CNYRUB_TOM", boardid: "CETS", shortname: "Китайский юань / Российский рубль" },
  { secid: "BYNRUB_TOM", boardid: "CETS", shortname: "Белорусский рубль / Российский рубль" },
  { secid: "KZTRUB_TOM", boardid: "CETS", shortname: "Казахстанский тенге / Российский рубль" },
  { secid: "TRYRUB_TOM", boardid: "CETS", shortname: "Турецкая лира / Российский рубль" },
  { secid: "AEDRUB_TOM", boardid: "CETS", shortname: "Дирхам ОАЭ / Российский рубль" },
];

const el = {
  app: document.getElementById("app"),
  content: document.getElementById("content"),
  globalLoader: document.getElementById("globalLoader"),
  globalLoaderText: document.getElementById("globalLoaderText"),
  toast: document.getElementById("toast"),
  refreshBtn: document.getElementById("refreshBtn"),
  userLine: document.getElementById("userLine"),
  totalValue: document.getElementById("totalValue"),
  totalPnl: document.getElementById("totalPnl"),
  positions: document.getElementById("positions"),

  tradeSide: document.getElementById("tradeSide"),
  tradeAssetType: document.getElementById("tradeAssetType"),
  tradeDate: document.getElementById("tradeDate"),
  tradeSearch: document.getElementById("tradeSearch"),
  tradeSearchClear: document.getElementById("tradeSearchClear"),
  tradeSearchResults: document.getElementById("tradeSearchResults"),
  tradeSelected: document.getElementById("tradeSelected"),
  tradeQty: document.getElementById("tradeQty"),
  tradePrice: document.getElementById("tradePrice"),
  tradeCommission: document.getElementById("tradeCommission"),
  saveTradeBtn: document.getElementById("saveTradeBtn"),

  lookupAssetType: document.getElementById("lookupAssetType"),
  lookupSearch: document.getElementById("lookupSearch"),
  lookupSearchClear: document.getElementById("lookupSearchClear"),
  lookupSearchResults: document.getElementById("lookupSearchResults"),
  lookupSelected: document.getElementById("lookupSelected"),
  lookupBtn: document.getElementById("lookupBtn"),
  lookupResult: document.getElementById("lookupResult"),

  moversDate: document.getElementById("moversDate"),
  moversTop: document.getElementById("moversTop"),
  moversBottom: document.getElementById("moversBottom"),

  alertAssetType: document.getElementById("alertAssetType"),
  alertSearch: document.getElementById("alertSearch"),
  alertSearchClear: document.getElementById("alertSearchClear"),
  alertSearchResults: document.getElementById("alertSearchResults"),
  alertSelected: document.getElementById("alertSelected"),
  alertSelectedPrice: document.getElementById("alertSelectedPrice"),
  alertTargetPrice: document.getElementById("alertTargetPrice"),
  alertRange: document.getElementById("alertRange"),
  addAlertBtn: document.getElementById("addAlertBtn"),
  alerts: document.getElementById("alerts"),

  usdRubRate: document.getElementById("usdRubRate"),
  usdRubAsOf: document.getElementById("usdRubAsOf"),
  usdRubBtn: document.getElementById("usdRubBtn"),
  openCloseToggle: document.getElementById("openCloseToggle"),
  xmlForm: document.getElementById("xmlForm"),
  xmlFile: document.getElementById("xmlFile"),
  xmlResult: document.getElementById("xmlResult"),
  clearPortfolioBtn: document.getElementById("clearPortfolioBtn"),
  articles: document.getElementById("articles"),
  articleText: document.getElementById("articleText"),
};

function toast(msg) {
  el.toast.textContent = msg;
  el.toast.classList.add("show");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.toast.classList.remove("show"), 2300);
}

const LOADER_SHOW_DELAY_MS = 140;
const LOADER_MIN_VISIBLE_MS = 240;

function showLoader(text = "Загрузка…") {
  if (!el.globalLoader) return;
  if (el.globalLoaderText) {
    el.globalLoaderText.textContent = text;
  }
  el.globalLoader.classList.add("show");
  el.globalLoader.setAttribute("aria-hidden", "false");
  state.loadingVisibleAt = Date.now();
}

function hideLoader() {
  if (!el.globalLoader) return;
  const elapsed = Date.now() - state.loadingVisibleAt;
  const delay = elapsed >= LOADER_MIN_VISIBLE_MS ? 0 : (LOADER_MIN_VISIBLE_MS - elapsed);
  window.setTimeout(() => {
    if (state.loadingCount > 0) return;
    el.globalLoader.classList.remove("show");
    el.globalLoader.setAttribute("aria-hidden", "true");
  }, delay);
}

function beginLoading(text) {
  state.loadingCount += 1;
  if (state.loadingCount > 1) {
    if (el.globalLoader?.classList.contains("show") && el.globalLoaderText && text) {
      el.globalLoaderText.textContent = text;
    }
    return;
  }
  window.clearTimeout(state.loadingShowTimer);
  state.loadingShowTimer = window.setTimeout(() => {
    if (state.loadingCount > 0) showLoader(text);
  }, LOADER_SHOW_DELAY_MS);
}

function endLoading() {
  state.loadingCount = Math.max(0, state.loadingCount - 1);
  if (state.loadingCount !== 0) return;
  window.clearTimeout(state.loadingShowTimer);
  hideLoader();
}

function money(v) {
  const n = Number(v || 0);
  return n.toLocaleString("ru-RU", { maximumFractionDigits: 2 }) + " ₽";
}

function formatQty(value, assetType) {
  const n = Number(value || 0);
  const text = Number.isInteger(n)
    ? n.toLocaleString("ru-RU", { maximumFractionDigits: 0 })
    : n.toLocaleString("ru-RU", { maximumFractionDigits: 4 });
  if (assetType === "metal") return `${text} грамм`;
  if (assetType === "stock") return `${text} акций`;
  return text;
}

function pct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "н/д";
  const n = Number(v);
  return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
}

function fmtDate(dd = 0) {
  const d = new Date();
  d.setDate(d.getDate() - dd);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function todayDdmmyyyy() {
  const d = new Date();
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

function updateViewport() {
  let h = window.innerHeight;
  if (tg?.viewportStableHeight) h = Math.max(320, Math.floor(tg.viewportStableHeight));
  if (window.visualViewport?.height) h = Math.max(320, Math.floor(window.visualViewport.height));
  document.documentElement.style.setProperty("--app-height", `${h}px`);
}

function ensureActiveFieldVisible() {
  const active = document.activeElement;
  if (!(active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement || active instanceof HTMLSelectElement)) return;
  const vv = window.visualViewport;
  const rect = active.getBoundingClientRect();
  const visibleTop = vv ? 8 : 8;
  const visibleBottom = vv ? vv.height - 14 : window.innerHeight - 14;
  if (rect.bottom > visibleBottom) {
    el.content.scrollBy({ top: rect.bottom - visibleBottom + 12, behavior: "smooth" });
  } else if (rect.top < visibleTop) {
    el.content.scrollBy({ top: rect.top - visibleTop - 12, behavior: "smooth" });
  }
}

function setupKeyboardBehavior() {
  updateViewport();
  window.addEventListener("resize", () => {
    updateViewport();
    ensureActiveFieldVisible();
  });
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", () => {
      updateViewport();
      ensureActiveFieldVisible();
    });
    window.visualViewport.addEventListener("scroll", () => {
      updateViewport();
      ensureActiveFieldVisible();
    });
  }
  tg?.onEvent?.("viewportChanged", () => {
    updateViewport();
    ensureActiveFieldVisible();
  });

  document.addEventListener("focusin", (ev) => {
    const t = ev.target;
    if (!(t instanceof HTMLInputElement || t instanceof HTMLTextAreaElement || t instanceof HTMLSelectElement)) return;
    setTimeout(() => {
      try {
        t.scrollIntoView({ block: "center", behavior: "smooth" });
      } catch (_) {}
      ensureActiveFieldVisible();
    }, 140);
  });
}

async function api(path, options = {}) {
  const { skipLoader, loadingText, ...fetchOptions } = options;
  const headers = new Headers(options.headers || {});
  if (state.initData) headers.set("X-Telegram-Init-Data", state.initData);
  if (!skipLoader) beginLoading(loadingText || "Загрузка…");
  try {
    const res = await fetch(path, { ...fetchOptions, headers });
    if (!res.ok) throw new Error(await res.text());
    const body = await res.json();
    if (!body.ok) throw new Error("API error");
    return body.data;
  } finally {
    if (!skipLoader) endLoading();
  }
}

function setTab(name) {
  document.querySelectorAll(".tab-view").forEach((x) => x.classList.remove("active"));
  document.querySelectorAll(".tab-btn").forEach((x) => x.classList.remove("active"));
  document.getElementById(`tab-${name}`)?.classList.add("active");
  document.querySelector(`.tab-btn[data-tab='${name}']`)?.classList.add("active");
  el.content.scrollTo({ top: 0, behavior: "smooth" });
  if (name === "movers" && !state.moversLoaded) {
    loadMovers(0).then(() => {
      state.moversLoaded = true;
    }).catch(() => {});
  }
  if (name === "more" && !state.usdLoaded) {
    loadUsdRub().then(() => {
      state.usdLoaded = true;
    }).catch(() => {});
  }
}

function renderEmpty(container, text) {
  container.innerHTML = `<p class="hint">${text}</p>`;
}

function renderSearchList(container, items, onPick) {
  container.innerHTML = "";
  if (!items.length) {
    renderEmpty(container, "Ничего не найдено");
    return;
  }
  items.forEach((s) => {
    const item = document.createElement("div");
    item.className = "item";
    const label = `${s.shortname || s.name || s.secid} (${s.secid})`;
    item.innerHTML = `<div class="left"><div class="name">${label}</div><div class="sub">${s.boardid || ""}</div></div><div class="right"><button class="btn ghost">Выбрать</button></div>`;
    item.querySelector("button").addEventListener("click", () => onPick(s, label));
    container.appendChild(item);
  });
}

function fiatPrettyName(candidate) {
  const secid = String(candidate?.secid || "").toUpperCase();
  const fallback = String(candidate?.shortname || candidate?.name || secid);
  const map = {
    USDRUB_TOM: "Доллар США / Российский рубль",
    USD000UTSTOM: "Доллар США / Российский рубль",
    EURRUB_TOM: "Евро / Российский рубль",
    EUR000UTSTOM: "Евро / Российский рубль",
    CNYRUB_TOM: "Китайский юань / Российский рубль",
    CNY000TOM: "Китайский юань / Российский рубль",
  };
  return map[secid] || fallback;
}

function buildCandidateLabel(candidate, assetType) {
  const secid = String(candidate?.secid || "").trim();
  if (assetType === "fiat") {
    const name = String(candidate?.shortname || "").trim() || fiatPrettyName(candidate);
    return `${name} (${secid})`;
  }
  return `${candidate.shortname || candidate.name || secid} (${secid})`;
}

async function loadCurrentPrice({ secid, boardid, asset_type }) {
  return api("/api/miniapp/price", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ secid, boardid, asset_type }),
  });
}

function bindSearch({
  inputEl,
  clearEl,
  typeEl,
  resultEl,
  selectedEl,
  getSelected,
  setSelected,
  onSelected,
  onCleared,
  searchFn,
  alwaysShowWhenEmpty,
}) {
  const showClear = (show) => {
    if (!clearEl) return;
    clearEl.classList.toggle("show", !!show);
    const wrapper = clearEl.closest(".search-control");
    if (wrapper) wrapper.classList.toggle("has-clear", !!show);
  };

  const clearSelection = ({ focus = false } = {}) => {
    setSelected(null);
    inputEl.value = "";
    resultEl.innerHTML = "";
    selectedEl.textContent = "Инструмент не выбран";
    showClear(false);
    if (onCleared) onCleared();
    if (focus) {
      inputEl.focus();
    }
  };

  if (clearEl) {
    clearEl.addEventListener("click", () => clearSelection({ focus: true }));
  }

  let timer = null;
  inputEl.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(async () => {
      const q = inputEl.value.trim();
      const selected = getSelected();
      if (selected && q !== String(selected.secid || "").trim()) {
        setSelected(null);
        selectedEl.textContent = "Инструмент не выбран";
        showClear(false);
        if (onCleared) onCleared();
      }
      if (!q && !(alwaysShowWhenEmpty && typeEl.value === "fiat")) {
        resultEl.innerHTML = "";
        return;
      }
      try {
        const data = searchFn
          ? await searchFn(q, typeEl.value)
          : await api(`/api/miniapp/search?q=${encodeURIComponent(q)}&asset_type=${encodeURIComponent(typeEl.value)}`);
        renderSearchList(resultEl, data || [], (picked) => {
          const label = buildCandidateLabel(picked, typeEl.value);
          setSelected(picked);
          inputEl.value = String(picked.secid || "").trim() || "";
          selectedEl.textContent = `Выбрано: ${label}`;
          resultEl.innerHTML = "";
          showClear(true);
          if (onSelected) onSelected(picked, label);
        });
      } catch (e) {
        renderEmpty(resultEl, "Ошибка поиска");
      }
    }, 300);
  });

  inputEl.addEventListener("focus", () => {
    if (!alwaysShowWhenEmpty) return;
    inputEl.dispatchEvent(new Event("input"));
  });

  return { clearSelection };
}

function renderPositions(rows) {
  el.positions.innerHTML = "";
  if (!rows.length) return renderEmpty(el.positions, "Портфель пуст");
  rows.slice(0, 30).forEach((row) => {
    const pnlVal = Number(row.ret_30d || 0);
    const qtyText = formatQty(row.qty, row.asset_type);
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `<div class="left"><div class="name">${row.name || row.ticker}</div><div class="sub">${row.ticker} · ${qtyText}</div></div><div class="right"><div>${money(row.value)}</div><div class="pnl ${pnlVal >= 0 ? "plus" : "minus"}">${pct(pnlVal)}</div></div>`;
    el.positions.appendChild(item);
  });
}

async function loadPortfolio() {
  const data = await api("/api/miniapp/portfolio");
  el.totalValue.textContent = money(data.summary?.total_value || 0);
  const totalPnl = Number(data.summary?.pnl_pct || 0);
  el.totalPnl.textContent = pct(totalPnl);
  el.totalPnl.style.color = totalPnl >= 0 ? "var(--ok)" : "var(--bad)";
  renderPositions(data.positions || []);
}

async function loadAlerts() {
  const rows = await api("/api/miniapp/alerts");
  el.alerts.innerHTML = "";
  if (!rows.length) return renderEmpty(el.alerts, "Нет активных алертов");
  rows.forEach((a) => {
    const item = document.createElement("div");
    item.className = "item";
    const label = `${a.shortname || a.secid} (${a.secid})`;
    const range = Number(a.range_percent || 0) > 0 ? `±${a.range_percent}%` : "точно";
    item.innerHTML = `<div class="left"><div class="name">${label}</div><div class="sub">${money(a.target_price)} · ${range}</div></div><div class="right"><button class="btn danger">Отключить</button></div>`;
    item.querySelector("button").addEventListener("click", async () => {
      try {
        await api(`/api/miniapp/alerts/${a.id}`, { method: "DELETE" });
        toast("Алерт отключен");
        await loadAlerts();
      } catch (_) {
        toast("Не удалось отключить алерт");
      }
    });
    el.alerts.appendChild(item);
  });
}

function renderLookup(data) {
  const dyn = data.dynamics || [];
  const labels = {
    week: "За неделю",
    month: "За месяц",
    half_year: "За 6 месяцев",
    year: "За год",
  };
  const lines = dyn.map((d) => {
    if (d.pct === null || d.pct === undefined) return `${labels[d.period] || d.period}: нет данных`;
    const up = Number(d.pct) >= 0;
    return `${labels[d.period] || d.period}: ${up ? "▲" : "▼"} ${pct(d.pct)}`;
  }).join("\n");

  el.lookupResult.innerHTML = `
    <div class="plain"><strong>${data.name} (${data.secid})</strong>\nТекущая цена: ${data.last ? money(data.last) : "н/д"}\n\n${lines}</div>
  `;
}

function renderMoversList(container, rows, type) {
  container.innerHTML = "";
  if (!rows.length) return renderEmpty(container, "Нет данных");
  rows.slice(0, 10).forEach((r) => {
    const v = Number(r.pct || 0);
    const last = r.last === null || r.last === undefined ? "н/д" : money(r.last);
    const volume = r.val_today === null || r.val_today === undefined ? "н/д" : money(r.val_today);
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `<div class="left"><div class="name">${r.shortname || r.secid}</div><div class="sub">${r.secid} · Цена: ${last} · Объём: ${volume}</div></div><div class="right"><div class="pnl ${v >= 0 ? "plus" : "minus"}">${pct(v)}</div></div>`;
    container.appendChild(item);
  });
}

async function loadMovers(dayOffset = 0) {
  const d = fmtDate(dayOffset);
  const data = await api(`/api/miniapp/top_movers?date=${encodeURIComponent(d)}`);
  el.moversDate.textContent = `Дата: ${data.date}`;
  renderMoversList(el.moversTop, data.top || [], "top");
  renderMoversList(el.moversBottom, data.bottom || [], "bottom");
}

async function loadUsdRub() {
  const data = await api("/api/miniapp/usd_rub");
  if (data.rate === null || data.rate === undefined) {
    el.usdRubRate.textContent = "Нет данных";
    return;
  }
  el.usdRubRate.textContent = `${Number(data.rate).toFixed(4)} ₽`;
  el.usdRubAsOf.textContent = `Обновлено: ${new Date(data.as_of).toLocaleString("ru-RU")}`;
}

async function loadOpenCloseSetting() {
  const data = await api("/api/miniapp/settings/open_close");
  el.openCloseToggle.checked = !!data.open_close_enabled;
}

async function loadArticles() {
  const rows = await api("/api/miniapp/articles");
  el.articles.innerHTML = "";
  if (!rows.length) return renderEmpty(el.articles, "Материалы пока не настроены");
  rows.forEach((r) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `<div class="left"><div class="name">${r.button_name}</div></div><div class="right"><button class="btn ghost">Открыть</button></div>`;
    item.querySelector("button").addEventListener("click", async () => {
      try {
        const article = await api(`/api/miniapp/articles/${encodeURIComponent(r.text_code)}`);
        el.articleText.textContent = article.value || "";
      } catch (_) {
        toast("Не удалось загрузить текст");
      }
    });
    el.articles.appendChild(item);
  });
}

async function saveTrade() {
  if (!state.selectedTrade) {
    toast("Сначала выберите инструмент");
    return;
  }
  const qty = Number(el.tradeQty.value);
  const price = Number(el.tradePrice.value);
  const commission = Number(el.tradeCommission.value || 0);
  if (!qty || qty <= 0 || !price || price <= 0) {
    toast("Проверьте количество и цену");
    return;
  }
  await api("/api/miniapp/trades", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      side: el.tradeSide.value,
      asset_type: el.tradeAssetType.value,
      trade_date: (el.tradeDate.value || "").trim() || todayDdmmyyyy(),
      secid: state.selectedTrade.secid,
      shortname: state.selectedTrade.shortname || state.selectedTrade.name || state.selectedTrade.secid,
      isin: state.selectedTrade.isin,
      boardid: state.selectedTrade.boardid,
      qty,
      price,
      commission,
    }),
  });

  el.tradeQty.value = "";
  el.tradePrice.value = "";
  el.tradeCommission.value = "0";
  state.searchControllers.trade?.clearSelection();
  toast("Сделка сохранена");
  await loadPortfolio();
}

function setupEvents() {
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => setTab(btn.dataset.tab));
  });

  state.searchControllers.trade = bindSearch({
    inputEl: el.tradeSearch,
    clearEl: el.tradeSearchClear,
    typeEl: el.tradeAssetType,
    resultEl: el.tradeSearchResults,
    selectedEl: el.tradeSelected,
    getSelected: () => state.selectedTrade,
    setSelected: (s) => { state.selectedTrade = s; },
    onCleared: () => {},
  });
  state.searchControllers.lookup = bindSearch({
    inputEl: el.lookupSearch,
    clearEl: el.lookupSearchClear,
    typeEl: el.lookupAssetType,
    resultEl: el.lookupSearchResults,
    selectedEl: el.lookupSelected,
    getSelected: () => state.selectedLookup,
    setSelected: (s) => { state.selectedLookup = s; },
    onCleared: () => {},
  });
  const resetAlertPrice = () => {
    el.alertSelectedPrice.textContent = "";
    el.alertSelectedPrice.classList.remove("price");
  };
  state.searchControllers.alert = bindSearch({
    inputEl: el.alertSearch,
    clearEl: el.alertSearchClear,
    typeEl: el.alertAssetType,
    resultEl: el.alertSearchResults,
    selectedEl: el.alertSelected,
    getSelected: () => state.selectedAlert,
    setSelected: (s) => { state.selectedAlert = s; },
    searchFn: async (q, assetType) => {
      if (assetType !== "fiat") {
        if (!q.trim()) return [];
        return api(`/api/miniapp/search?q=${encodeURIComponent(q)}&asset_type=${encodeURIComponent(assetType)}`);
      }
      const needle = q.trim().toLowerCase();
      if (!needle) return POPULAR_FIAT_ALERT_PAIRS;
      return POPULAR_FIAT_ALERT_PAIRS.filter((row) => {
        const label = `${row.shortname} ${row.secid}`.toLowerCase();
        return label.includes(needle);
      });
    },
    alwaysShowWhenEmpty: true,
    onSelected: async (picked) => {
      try {
        const data = await loadCurrentPrice({
          secid: picked.secid,
          boardid: picked.boardid,
          asset_type: el.alertAssetType.value,
        });
        if (data?.price === null || data?.price === undefined) {
          el.alertSelectedPrice.textContent = "Текущая цена: нет данных";
        } else {
          el.alertSelectedPrice.textContent = `Текущая цена: ${money(data.price)}`;
        }
      } catch (_) {
        el.alertSelectedPrice.textContent = "Текущая цена: не удалось загрузить";
      }
      el.alertSelectedPrice.classList.add("price");
    },
    onCleared: resetAlertPrice,
  });
  el.tradeAssetType.addEventListener("change", () => state.searchControllers.trade?.clearSelection());
  el.lookupAssetType.addEventListener("change", () => state.searchControllers.lookup?.clearSelection());
  el.alertAssetType.addEventListener("change", () => {
    state.searchControllers.alert?.clearSelection();
    resetAlertPrice();
  });

  el.tradeDate.value = todayDdmmyyyy();
  el.refreshBtn.addEventListener("click", async () => {
    try {
      await Promise.all([loadPortfolio(), loadAlerts(), loadUsdRub(), loadOpenCloseSetting()]);
      toast("Обновлено");
    } catch (e) {
      toast("Ошибка обновления");
    }
  });

  el.saveTradeBtn.addEventListener("click", async () => {
    try {
      await saveTrade();
    } catch (e) {
      toast("Не удалось сохранить сделку");
    }
  });

  el.lookupBtn.addEventListener("click", async () => {
    if (!state.selectedLookup) {
      toast("Выберите инструмент");
      return;
    }
    try {
      const data = await api("/api/miniapp/asset_lookup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          secid: state.selectedLookup.secid,
          boardid: state.selectedLookup.boardid,
          asset_type: el.lookupAssetType.value,
          shortname: state.selectedLookup.shortname || state.selectedLookup.name,
          name: state.selectedLookup.name,
        }),
      });
      renderLookup(data);
    } catch (_) {
      toast("Не удалось получить динамику");
    }
  });

  document.querySelectorAll(".chip").forEach((chip) => {
    chip.addEventListener("click", async () => {
      document.querySelectorAll(".chip").forEach((c) => c.classList.remove("active"));
      chip.classList.add("active");
      try {
        await loadMovers(Number(chip.dataset.day || "0"));
      } catch (_) {
        toast("Ошибка загрузки рынка");
      }
    });
  });

  el.addAlertBtn.addEventListener("click", async () => {
    if (!state.selectedAlert) {
      toast("Сначала выберите инструмент");
      return;
    }
    const targetPrice = Number(el.alertTargetPrice.value);
    const rangeRaw = (el.alertRange.value || "").trim();
    if (!targetPrice || targetPrice <= 0) {
      toast("Введите корректную цену");
      return;
    }
    if (rangeRaw) {
      const rangePercentCheck = Number(rangeRaw);
      if (!Number.isFinite(rangePercentCheck) || rangePercentCheck < 0 || rangePercentCheck > 50) {
        toast("Диапазон должен быть числом от 0 до 50");
        return;
      }
    }
    try {
      const payload = {
        secid: state.selectedAlert.secid,
        shortname: state.selectedAlert.shortname || state.selectedAlert.name || state.selectedAlert.secid,
        isin: state.selectedAlert.isin,
        boardid: state.selectedAlert.boardid,
        asset_type: el.alertAssetType.value,
        target_price: targetPrice,
      };
      if (rangeRaw) {
        payload.range_percent = Number(rangeRaw);
      }
      await api("/api/miniapp/alerts", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      el.alertTargetPrice.value = "";
      el.alertRange.value = "";
      state.searchControllers.alert?.clearSelection();
      await loadAlerts();
      toast("Алерт создан");
    } catch (_) {
      toast("Не удалось создать алерт");
    }
  });

  el.usdRubBtn.addEventListener("click", async () => {
    try {
      await loadUsdRub();
      state.usdLoaded = true;
    } catch (_) {
      toast("Ошибка USD/RUB");
    }
  });

  el.openCloseToggle.addEventListener("change", async () => {
    const enabled = el.openCloseToggle.checked;
    try {
      await api("/api/miniapp/settings/open_close", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled }),
      });
      toast(enabled ? "Отчеты дня включены" : "Отчеты дня выключены");
    } catch (_) {
      el.openCloseToggle.checked = !enabled;
      toast("Не удалось сохранить настройку");
    }
  });

  el.xmlForm.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const file = el.xmlFile.files?.[0];
    if (!file) {
      toast("Выберите XML файл");
      return;
    }
    const form = new FormData();
    form.append("file", file);
    try {
      const data = await api("/api/miniapp/import/xml", { method: "POST", body: form });
      el.xmlResult.textContent = [
        `Файл: ${data.file}`,
        `Сделок в выписке: ${data.rows}`,
        `Добавлено: ${data.imported}`,
        `Дубликаты: ${data.duplicates}`,
        `Пропущено: ${data.skipped}`,
      ].join("\n");
      await loadPortfolio();
      toast("Импорт завершен");
    } catch (_) {
      toast("Не удалось импортировать XML");
    }
  });

  el.clearPortfolioBtn.addEventListener("click", async () => {
    if (!window.confirm("Удалить все сделки и очистить портфель?")) return;
    try {
      const data = await api("/api/miniapp/portfolio/clear", { method: "POST" });
      toast(`Удалено сделок: ${data.deleted_trades}`);
      await Promise.all([loadPortfolio(), loadAlerts()]);
    } catch (_) {
      toast("Не удалось очистить портфель");
    }
  });
}

(async function init() {
  setupKeyboardBehavior();
  setupEvents();
  try {
    const me = await api("/api/miniapp/me");
    el.userLine.textContent = `Telegram ID: ${me.user_id}`;
    await Promise.all([
      loadPortfolio(),
      loadAlerts(),
      loadOpenCloseSetting(),
      loadArticles(),
    ]);
  } catch (e) {
    el.userLine.textContent = "Ошибка авторизации Mini App";
    toast("Не удалось инициализировать Mini App");
  }
})();
