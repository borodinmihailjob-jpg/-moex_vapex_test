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
  mode: "mode",
  appMode: "mode",
  lastMode: null,
  selectedTrade: null,
  selectedLookup: null,
  selectedAlert: null,
  searchControllers: {},
  moversLoaded: false,
  usdLoaded: false,
  loadingCount: 0,
  loadingShowTimer: null,
  loadingVisibleAt: 0,
  budget: {
    activeTab: "overview",
    onboardingMode: null,
    onboardingCompleted: false,
    step: 1,
    obligations: [],
    savings: [],
    profile: null,
    dashboard: null,
    strategyDraft: null,
  },
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
  tabbarExchange: document.getElementById("tabbarExchange"),
  tabbarBudget: document.getElementById("tabbarBudget"),
  globalLoader: document.getElementById("globalLoader"),
  globalLoaderText: document.getElementById("globalLoaderText"),
  toast: document.getElementById("toast"),
  refreshBtn: document.getElementById("refreshBtn"),
  switchModeBtn: document.getElementById("switchModeBtn"),
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

  openExchangeBtn: document.getElementById("openExchangeBtn"),
  openBudgetBtn: document.getElementById("openBudgetBtn"),
  openLastModeBtn: document.getElementById("openLastModeBtn"),
  lastModeLine: document.getElementById("lastModeLine"),
  startQuickOnboardingBtn: document.getElementById("startQuickOnboardingBtn"),
  startPreciseOnboardingBtn: document.getElementById("startPreciseOnboardingBtn"),
  budgetWelcomeCard: document.getElementById("budgetWelcomeCard"),
  budgetWizardCard: document.getElementById("budgetWizardCard"),
  budgetResultCard: document.getElementById("budgetResultCard"),
  budgetDashboardCard: document.getElementById("budgetDashboardCard"),
  budgetIncomeCard: document.getElementById("budgetIncomeCard"),
  budgetExpensesCard: document.getElementById("budgetExpensesCard"),
  budgetFundsCard: document.getElementById("budgetFundsCard"),
  budgetMonthCloseCard: document.getElementById("budgetMonthCloseCard"),
  budgetLoanCalcCard: document.getElementById("budgetLoanCalcCard"),
  budgetWizardTitle: document.getElementById("budgetWizardTitle"),
  budgetStepProgress: document.getElementById("budgetStepProgress"),
  budgetWizardSubtitle: document.getElementById("budgetWizardSubtitle"),
  budgetWizardBody: document.getElementById("budgetWizardBody"),
  budgetBackBtn: document.getElementById("budgetBackBtn"),
  budgetSkipBtn: document.getElementById("budgetSkipBtn"),
  budgetNextBtn: document.getElementById("budgetNextBtn"),
  budgetResultBody: document.getElementById("budgetResultBody"),
  budgetResultActions: document.getElementById("budgetResultActions"),
  budgetCurrentMonth: document.getElementById("budgetCurrentMonth"),
  editBudgetBtn: document.getElementById("editBudgetBtn"),
  budgetIncomeTypeInput: document.getElementById("budgetIncomeTypeInput"),
  budgetPaydayInput: document.getElementById("budgetPaydayInput"),
  budgetIncomeInput: document.getElementById("budgetIncomeInput"),
  budgetIncomeSaveBtn: document.getElementById("budgetIncomeSaveBtn"),
  budgetExpensesInput: document.getElementById("budgetExpensesInput"),
  budgetExpensesSaveBtn: document.getElementById("budgetExpensesSaveBtn"),
  budgetFundsList: document.getElementById("budgetFundsList"),
  planExpenseBtn: document.getElementById("planExpenseBtn"),
  saveTargetBtn: document.getElementById("saveTargetBtn"),
  budgetFundPlannerCard: document.getElementById("budgetFundPlannerCard"),
  fundTitleInput: document.getElementById("fundTitleInput"),
  fundTargetMonthInput: document.getElementById("fundTargetMonthInput"),
  fundTargetAmountInput: document.getElementById("fundTargetAmountInput"),
  fundAlreadySavedInput: document.getElementById("fundAlreadySavedInput"),
  fundPriorityInput: document.getElementById("fundPriorityInput"),
  fundCalcBtn: document.getElementById("fundCalcBtn"),
  fundSaveBtn: document.getElementById("fundSaveBtn"),
  fundCancelBtn: document.getElementById("fundCancelBtn"),
  fundStrategyResult: document.getElementById("fundStrategyResult"),
  closeMonthOpenBtn: document.getElementById("closeMonthOpenBtn"),
  monthCloseForm: document.getElementById("monthCloseForm"),
  monthCloseActualInput: document.getElementById("monthCloseActualInput"),
  monthCloseExtraInput: document.getElementById("monthCloseExtraInput"),
  monthCloseSubmitBtn: document.getElementById("monthCloseSubmitBtn"),
  monthCloseCancelBtn: document.getElementById("monthCloseCancelBtn"),
  monthCloseBody: document.getElementById("monthCloseBody"),
  loanAmountInput: document.getElementById("loanAmountInput"),
  loanRateInput: document.getElementById("loanRateInput"),
  loanMonthsInput: document.getElementById("loanMonthsInput"),
  loanTypeInput: document.getElementById("loanTypeInput"),
  loanCalcBtn: document.getElementById("loanCalcBtn"),
  loanCalcResult: document.getElementById("loanCalcResult"),
};

function toast(msg) {
  const text = String(msg || "").trim();
  if (!text) {
    el.toast.textContent = "";
    el.toast.classList.remove("show");
    return;
  }
  el.toast.textContent = text;
  el.toast.classList.add("show");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => {
    el.toast.classList.remove("show");
    el.toast.textContent = "";
  }, 1000);
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

function parseMoneyInput(raw) {
  const value = String(raw ?? "").trim().toLowerCase().replace(/₽/g, "").replace(/\s+/g, "").replace(/_/g, "").replace(",", ".");
  if (!value) throw new Error("Введите сумму числом. Например: 120000");
  let text = value;
  let mult = 1;
  if (text.endsWith("млн")) {
    text = text.slice(0, -3);
    mult = 1_000_000;
  } else if (text.endsWith("м") || text.endsWith("m")) {
    text = text.slice(0, -1);
    mult = 1_000_000;
  }
  const n = Number(text) * mult;
  if (!Number.isFinite(n)) throw new Error("Введите сумму числом. Например: 120000");
  if (n <= 0) throw new Error("Сумма должна быть больше 0");
  return n;
}

function monthLabel(monthKey) {
  const [y, m] = String(monthKey || "").split("-");
  const d = new Date(Number(y), Number(m) - 1, 1);
  if (Number.isNaN(d.getTime())) return monthKey || "";
  return d.toLocaleString("ru-RU", { month: "long", year: "numeric" });
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
  state.mode = name;
  document.querySelectorAll(".tab-view").forEach((x) => x.classList.remove("active"));
  document.querySelectorAll("#tabbarExchange .tab-btn[data-tab]").forEach((x) => x.classList.remove("active"));
  document.querySelectorAll("#tabbarBudget .tab-btn[data-tab]").forEach((x) => x.classList.remove("active"));
  document.getElementById(`tab-${name}`)?.classList.add("active");
  document.querySelector(`#tabbarExchange .tab-btn[data-tab='${name}']`)?.classList.add("active");
  document.querySelector(`#tabbarBudget .tab-btn[data-tab='${name}']`)?.classList.add("active");
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
  if (name === "budget") {
    loadBudgetDashboard().catch(() => {});
  }
}

function setAppMode(mode) {
  state.appMode = mode;
  if (mode === "exchange") {
    el.tabbarExchange?.classList.remove("hidden");
    el.tabbarBudget?.classList.add("hidden");
  } else if (mode === "budget") {
    el.tabbarExchange?.classList.add("hidden");
    el.tabbarBudget?.classList.remove("hidden");
  } else {
    el.tabbarExchange?.classList.remove("hidden");
    el.tabbarBudget?.classList.add("hidden");
  }
}

function setBudgetTab(tab) {
  state.budget.activeTab = tab;
  document.querySelectorAll("#tabbarBudget .tab-btn[data-budget-tab]").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.budgetTab === tab);
  });
  const show = (node, visible) => {
    if (!node) return;
    node.style.display = visible ? "" : "none";
  };
  const onboardingIncomplete = !state.budget.onboardingCompleted;
  if (onboardingIncomplete) {
    show(el.budgetWelcomeCard, true);
    show(el.budgetWizardCard, false);
    show(el.budgetResultCard, false);
    show(el.budgetDashboardCard, false);
    show(el.budgetIncomeCard, false);
    show(el.budgetExpensesCard, false);
    show(el.budgetFundsCard, false);
    show(el.budgetFundPlannerCard, false);
    show(el.budgetMonthCloseCard, false);
    show(el.budgetLoanCalcCard, false);
    return;
  }

  show(el.budgetWelcomeCard, false);
  show(el.budgetWizardCard, false);
  show(el.budgetResultCard, false);
  show(el.budgetDashboardCard, tab === "overview");
  show(el.budgetIncomeCard, tab === "income");
  show(el.budgetExpensesCard, tab === "expenses");
  show(el.budgetFundsCard, tab === "funds");
  if (tab !== "funds") {
    show(el.budgetFundPlannerCard, false);
  }
  show(el.budgetMonthCloseCard, tab === "close");
  show(el.budgetLoanCalcCard, tab === "loans");
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

async function loadModePreference() {
  const data = await api("/api/miniapp/mode");
  state.lastMode = data.last_mode || null;
  if (state.lastMode === "exchange") {
    el.lastModeLine.textContent = "Последний режим: Биржа";
    el.openLastModeBtn.style.display = "";
  } else if (state.lastMode === "budget") {
    el.lastModeLine.textContent = "Последний режим: Бюджет";
    el.openLastModeBtn.style.display = "";
  } else {
    el.lastModeLine.textContent = "";
    el.openLastModeBtn.style.display = "none";
  }
}

async function setModePreference(mode) {
  await api("/api/miniapp/mode", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode }),
  });
  state.lastMode = mode;
}

async function loadBudgetDashboard() {
  const data = await api("/api/miniapp/budget/dashboard", { loadingText: "Считаю…" });
  state.budget.dashboard = data;
  state.budget.profile = data.profile || {};
  state.budget.onboardingCompleted = !!data.profile?.onboarding_completed;

  const onboardingCompleted = state.budget.onboardingCompleted;
  el.budgetWizardCard.style.display = "none";
  el.budgetResultCard.style.display = "none";

  if (onboardingCompleted) {
    const free = Number(data.free || 0);
    el.budgetCurrentMonth.textContent = [
      `${monthLabel(data.month)}`,
      `Доход: ${money(data.income)}`,
      `Обязательные: ${money(data.obligations_total)}`,
      `На жизнь (план): ${money(data.expenses_base)}`,
      `${free >= 0 ? "Свободно" : "Дефицит"}: ${money(Math.abs(free))}/мес`,
    ].join("\n");
    renderBudgetFunds(data.funds || []);
    el.budgetIncomeTypeInput.value = data.profile?.income_type || "fixed";
    el.budgetPaydayInput.value = data.profile?.payday_day || "";
    el.budgetIncomeInput.value = data.income ? String(Math.round(Number(data.income))) : "";
    el.budgetExpensesInput.value = data.expenses_base ? String(Math.round(Number(data.expenses_base))) : "";
  }
  setBudgetTab(state.budget.activeTab || "overview");
}

function renderBudgetFunds(rows) {
  el.budgetFundsList.innerHTML = "";
  if (!rows.length) {
    renderEmpty(el.budgetFundsList, "Пока нет фондов. Добавьте цель или крупную трату.");
    return;
  }
  rows.forEach((fund) => {
    const item = document.createElement("div");
    item.className = "item";
    const months = Number(fund.months_left || 0);
    item.innerHTML = `<div class="left"><div class="name">${fund.title}</div><div class="sub">нужно ${money(fund.required_per_month)}/мес • осталось ${months} мес</div></div><div class="right"><button class="btn ghost">Пополнить</button></div>`;
    item.querySelector("button").addEventListener("click", async () => {
      const raw = window.prompt("Сумма пополнения, ₽");
      if (!raw) return;
      try {
        const amount = parseMoneyInput(raw);
        await api(`/api/miniapp/budget/funds/${fund.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "topup", amount }),
        });
        toast("Фонд пополнен");
        await loadBudgetDashboard();
      } catch (e) {
        toast(e?.message || "Ошибка пополнения");
      }
    });
    el.budgetFundsList.appendChild(item);
  });
}

function openBudgetWizard(onboardingMode) {
  state.budget.onboardingMode = onboardingMode;
  state.budget.step = 1;
  state.budget.obligations = [];
  state.budget.savings = [];
  state.budget.selectedObligationKind = "rent";
  state.budget.selectedObligationTitle = "Аренда";
  state.budget.selectedSavingKind = "cash";
  state.budget.selectedSavingTitle = "Подушка (наличные/карта)";
  el.budgetWelcomeCard.style.display = "none";
  el.budgetWizardCard.style.display = "";
  el.budgetResultCard.style.display = "none";
  el.budgetDashboardCard.style.display = "none";
  el.budgetFundsCard.style.display = "none";
  el.budgetMonthCloseCard.style.display = "none";
  state.budget.onboardingCompleted = false;
  renderBudgetWizardStep();
}

function renderBudgetWizardStep() {
  const step = state.budget.step;
  el.budgetStepProgress.textContent = `Шаг ${step} из 5`;
  el.budgetBackBtn.style.display = step > 1 ? "" : "none";
  el.budgetSkipBtn.style.display = step === 3 ? "" : "none";
  el.budgetNextBtn.textContent = step === 5 ? "Сохранить" : "Дальше";

  if (step === 1) {
    const total = state.budget.obligations.reduce((acc, x) => acc + Number(x.amount_monthly || 0), 0);
    const selectedKind = state.budget.selectedObligationKind || "rent";
    const selectedTitle = state.budget.selectedObligationTitle || "";
    el.budgetWizardTitle.textContent = "Обязательные платежи";
    el.budgetWizardSubtitle.textContent = "Добавьте то, что вы платите каждый месяц в первую очередь.";
    el.budgetWizardBody.innerHTML = `
      <div class="chips">
        <button class="chip ${selectedKind === "rent" ? "active" : ""}" data-obligation-kind="rent" data-obligation-label="Аренда">Аренда</button>
        <button class="chip ${selectedKind === "mortgage" ? "active" : ""}" data-obligation-kind="mortgage" data-obligation-label="Ипотека">Ипотека</button>
        <button class="chip ${selectedKind === "loan" ? "active" : ""}" data-obligation-kind="loan" data-obligation-label="Кредит">Кредит</button>
        <button class="chip ${selectedKind === "installment" ? "active" : ""}" data-obligation-kind="installment" data-obligation-label="Рассрочка">Рассрочка</button>
        <button class="chip ${selectedKind === "alimony" ? "active" : ""}" data-obligation-kind="alimony" data-obligation-label="Алименты/обязательные переводы">Алименты/обязательные переводы</button>
        <button class="chip ${selectedKind === "other" ? "active" : ""}" data-obligation-kind="other" data-obligation-label="Другое">Другое</button>
      </div>
      <div class="form-grid two">
        <label>Название
          <input id="wizardObligationTitle" type="text" placeholder="Например: аренда квартиры" value="${selectedTitle}" />
        </label>
        <label>Сумма в месяц, ₽
          <input id="wizardObligationAmount" type="text" placeholder="Например: 45000" />
        </label>
      </div>
      <div id="wizardDebtExtra" class="form-grid three" style="${selectedKind === "mortgage" || selectedKind === "loan" ? "" : "display:none"}">
        <label>Остаток долга, ₽
          <input id="wizardDebtAmount" type="text" placeholder="Например: 2100000" />
        </label>
        <label>Ставка, % годовых
          <input id="wizardDebtRate" type="number" step="0.01" min="0" placeholder="Например: 14.9" />
        </label>
        <label>Срок, мес
          <input id="wizardDebtMonths" type="number" step="1" min="1" placeholder="Например: 120" />
        </label>
      </div>
      <div id="wizardDebtHint" class="hint" style="${selectedKind === "mortgage" || selectedKind === "loan" ? "" : "display:none"}">Это нужно для расчёта переплаты и досрочного погашения.</div>
      <div class="wizard-actions" style="${selectedKind === "mortgage" || selectedKind === "loan" ? "" : "display:none"}">
        <button id="wizardDebtCalcBtn" class="btn ghost" type="button">Рассчитать платёж</button>
      </div>
      <div id="wizardDebtCalcResult" class="plain"></div>
      <button id="wizardObligationAddBtn" class="btn primary">Сохранить платёж</button>
      <div id="wizardObligationsList" class="plain">${state.budget.obligations.length ? state.budget.obligations.map((x) => `• ${x.title}: ${money(x.amount_monthly)}`).join("\n") : "Пока ничего не добавили. Нажмите на пункт выше, чтобы добавить платёж."}</div>
      <p class="hint">Сейчас обязательные платежи: <strong>${money(total)}/мес</strong></p>
    `;
    el.budgetWizardBody.querySelectorAll("[data-obligation-kind]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.budget.selectedObligationKind = btn.dataset.obligationKind;
        state.budget.selectedObligationTitle = btn.dataset.obligationLabel || btn.textContent.trim();
        renderBudgetWizardStep();
      });
    });
    el.budgetWizardBody.querySelector("#wizardObligationAddBtn")?.addEventListener("click", async () => {
      const title = (document.getElementById("wizardObligationTitle")?.value || "").trim();
      const raw = (document.getElementById("wizardObligationAmount")?.value || "").trim();
      if (!title) {
        toast("Введите название платежа");
        return;
      }
      try {
        let amount = parseMoneyInput(raw);
        let debtDetails = null;
        if (selectedKind === "mortgage" || selectedKind === "loan") {
          const debtAmountRaw = (document.getElementById("wizardDebtAmount")?.value || "").trim();
          const debtRateRaw = (document.getElementById("wizardDebtRate")?.value || "").trim();
          const debtMonthsRaw = (document.getElementById("wizardDebtMonths")?.value || "").trim();
          if (debtAmountRaw && debtRateRaw && debtMonthsRaw) {
            const calc = calculateLoanMetrics({
              amount: parseMoneyInput(debtAmountRaw),
              annualRate: Number(debtRateRaw),
              months: Number(debtMonthsRaw),
              paymentType: "annuity",
            });
            amount = calc.monthly_payment || amount;
            debtDetails = {
              debt_amount: parseMoneyInput(debtAmountRaw),
              annual_rate: Number(debtRateRaw),
              months: Number(debtMonthsRaw),
              monthly_payment: calc.monthly_payment,
              overpayment: calc.overpayment,
              total_payment: calc.total_payment,
            };
          }
        }
        await api("/api/miniapp/budget/obligations", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title, kind: selectedKind, amount_monthly: amount, debt_details: debtDetails }),
        });
        const listData = await api("/api/miniapp/budget/obligations");
        state.budget.obligations = listData.items || [];
        toast("Платёж добавлен ✅");
        state.budget.selectedObligationTitle = "";
        renderBudgetWizardStep();
      } catch (e) {
        toast(e?.message || "Ошибка добавления");
      }
    });
    el.budgetWizardBody.querySelector("#wizardDebtCalcBtn")?.addEventListener("click", () => {
      try {
        const debtAmountRaw = (document.getElementById("wizardDebtAmount")?.value || "").trim();
        const debtRateRaw = (document.getElementById("wizardDebtRate")?.value || "").trim();
        const debtMonthsRaw = (document.getElementById("wizardDebtMonths")?.value || "").trim();
        const calc = calculateLoanMetrics({
          amount: parseMoneyInput(debtAmountRaw),
          annualRate: Number(debtRateRaw),
          months: Number(debtMonthsRaw),
          paymentType: "annuity",
        });
        const out = document.getElementById("wizardDebtCalcResult");
        if (out) {
          out.textContent = [
            `Ежемесячный платёж: ${money(calc.monthly_payment)}`,
            `Переплата: ${money(calc.overpayment)}`,
            `Итоговая выплата: ${money(calc.total_payment)}`,
          ].join("\n");
        }
        const amountInput = document.getElementById("wizardObligationAmount");
        if (amountInput) amountInput.value = String(Math.round(Number(calc.monthly_payment || 0)));
      } catch (e) {
        toast(e?.message || "Ошибка расчёта кредита");
      }
    });
    return;
  }

  if (step === 2) {
    el.budgetWizardTitle.textContent = "Доход";
    el.budgetWizardSubtitle.textContent = "Сколько денег приходит к вам в среднем за месяц?";
    const currentType = state.budget.profile?.income_type || "fixed";
    el.budgetWizardBody.innerHTML = `
      <div class="form-grid two">
        <label>Режим
          <select id="wizardIncomeType">
            <option value="fixed" ${currentType === "fixed" ? "selected" : ""}>Фиксированный</option>
            <option value="irregular" ${currentType === "irregular" ? "selected" : ""}>Нерегулярный</option>
          </select>
        </label>
        <label id="wizardPaydayWrap">День зарплаты
          <input id="wizardPayday" type="number" min="1" max="31" placeholder="Например: 10" />
        </label>
      </div>
      <label>${currentType === "fixed" ? "Доход в месяц, ₽" : "Средний доход, ₽/мес"}
        <input id="wizardIncome" type="text" placeholder="Например: ${currentType === "fixed" ? "150000" : "120000"}" />
      </label>
      <p class="hint">${currentType === "fixed" ? "Нужно для “сколько можно тратить в день”." : "Мы будем считать бюджет по среднему, а премии/кэшбек учтём в конце месяца."}</p>
    `;
    return;
  }

  if (step === 3) {
    const total = state.budget.savings.reduce((acc, x) => acc + Number(x.amount || 0), 0);
    const selectedSavingKind = state.budget.selectedSavingKind || "cash";
    const selectedSavingTitle = state.budget.selectedSavingTitle || "";
    el.budgetWizardTitle.textContent = "Накопления";
    el.budgetWizardSubtitle.textContent = "Это нужно только для оценки “запаса прочности”. Можно пропустить.";
    el.budgetWizardBody.innerHTML = `
      <div class="chips">
        <button class="chip ${selectedSavingKind === "cash" ? "active" : ""}" data-saving-kind="cash" data-saving-label="Подушка (наличные/карта)">Подушка (наличные/карта)</button>
        <button class="chip ${selectedSavingKind === "deposit" ? "active" : ""}" data-saving-kind="deposit" data-saving-label="Вклад">Вклад</button>
        <button class="chip ${selectedSavingKind === "investments" ? "active" : ""}" data-saving-kind="investments" data-saving-label="Инвестиции (акции/ОФЗ)">Инвестиции (акции/ОФЗ)</button>
        <button class="chip ${selectedSavingKind === "crypto" ? "active" : ""}" data-saving-kind="crypto" data-saving-label="Крипта">Крипта</button>
        <button class="chip ${selectedSavingKind === "other" ? "active" : ""}" data-saving-kind="other" data-saving-label="Другое">Другое</button>
      </div>
      <div class="form-grid two">
        <label>Название
          <input id="wizardSavingTitle" type="text" placeholder="Например: вклад" value="${selectedSavingTitle}" />
        </label>
        <label>Сумма, ₽
          <input id="wizardSavingAmount" type="text" placeholder="Например: 100000" />
        </label>
      </div>
      <button id="wizardSavingAddBtn" class="btn primary">Добавить ещё</button>
      <div class="plain">${state.budget.savings.length ? state.budget.savings.map((x) => `• ${x.title}: ${money(x.amount)}`).join("\n") : "Нет накоплений"}</div>
      <p class="hint">Всего накоплений: ${money(total)}</p>
    `;
    el.budgetWizardBody.querySelectorAll("[data-saving-kind]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.budget.selectedSavingKind = btn.dataset.savingKind;
        state.budget.selectedSavingTitle = btn.dataset.savingLabel || btn.textContent.trim();
        renderBudgetWizardStep();
      });
    });
    el.budgetWizardBody.querySelector("#wizardSavingAddBtn")?.addEventListener("click", async () => {
      const title = (document.getElementById("wizardSavingTitle")?.value || "").trim();
      const raw = (document.getElementById("wizardSavingAmount")?.value || "").trim();
      if (!title) {
        toast("Введите название накопления");
        return;
      }
      try {
        const amount = parseMoneyInput(raw);
        await api("/api/miniapp/budget/savings", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ kind: selectedSavingKind, title, amount }),
        });
        const listData = await api("/api/miniapp/budget/savings");
        state.budget.savings = listData.items || [];
        state.budget.selectedSavingTitle = "";
        renderBudgetWizardStep();
      } catch (e) {
        toast(e?.message || "Ошибка сохранения");
      }
    });
    return;
  }

  if (step === 4) {
    el.budgetWizardTitle.textContent = "Расходы на жизнь";
    el.budgetWizardSubtitle.textContent = "Сколько уходит в месяц на всё остальное — еда, транспорт, покупки и т.д.";
    el.budgetWizardBody.innerHTML = `
      <label>Сумма в месяц, ₽
        <input id="wizardExpensesBase" type="text" placeholder="Например: 90000" />
      </label>
      <p class="hint">Можно примерно — уточним через “закрытие месяца”.</p>
    `;
    return;
  }

  el.budgetWizardTitle.textContent = "Цели";
  el.budgetWizardSubtitle.textContent = "Хотите поставить цель или запланировать крупную трату?";
  el.budgetWizardBody.innerHTML = `
    <div class="chips">
      <button class="chip" data-goal-action="fund">Накопить сумму</button>
      <button class="chip" data-goal-action="expense">Запланировать трату</button>
      <button class="chip" data-goal-action="none">Пока без целей</button>
    </div>
    <p class="hint">Цели можно добавить позже в разделе “Фонды”.</p>
  `;
  el.budgetWizardBody.querySelectorAll("[data-goal-action]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      if (btn.dataset.goalAction === "fund" || btn.dataset.goalAction === "expense") {
        await openFundPlanFlow();
      } else {
        toast("Продолжим без целей");
      }
    });
  });
}

async function completeBudgetOnboarding() {
  const dashboard = await api("/api/miniapp/budget/dashboard");
  const free = Number(dashboard.free || 0);
  el.budgetWizardCard.style.display = "none";
  el.budgetResultCard.style.display = "";
  state.budget.onboardingCompleted = true;
  el.budgetResultBody.textContent = [
    `Доход: ${money(dashboard.income)}`,
    `Обязательные платежи: ${money(dashboard.obligations_total)}`,
    `Расходы на жизнь: ${money(dashboard.expenses_base)}`,
    `${free >= 0 ? "Свободно" : "Дефицит"}: ${money(Math.abs(free))}/мес`,
  ].join("\n");
  el.budgetResultActions.innerHTML = "";
  const actions = free >= 0
    ? ["Запланировать трату", "Создать подушку", "Перейти в бюджет"]
    : ["Сократить расходы", "Найти доп. доход", "Смешанный план"];
  actions.forEach((label) => {
    const btn = document.createElement("button");
    btn.className = "chip";
    btn.textContent = label;
    btn.addEventListener("click", async () => {
      if (label === "Перейти в бюджет") {
        await loadBudgetDashboard();
      } else if (label.includes("трат")) {
        await openFundPlanFlow();
      } else {
        toast("Сохранено");
      }
    });
    el.budgetResultActions.appendChild(btn);
  });
}

async function openFundPlanFlow() {
  el.budgetFundPlannerCard.style.display = "";
  el.fundTitleInput.value = el.fundTitleInput.value || "Отпуск";
  el.fundTargetMonthInput.value = el.fundTargetMonthInput.value || "2026-12";
  el.fundTargetAmountInput.value = el.fundTargetAmountInput.value || "";
  el.fundAlreadySavedInput.value = el.fundAlreadySavedInput.value || "0";
  el.fundStrategyResult.textContent = "";
  el.fundTitleInput.focus();
}

async function openMonthCloseFlow() {
  if (state.budget.dashboard && !state.budget.dashboard.need_close_previous_month) {
    toast("Прошлый месяц уже закрыт");
    return;
  }
  el.monthCloseForm.style.display = "";
  el.monthCloseBody.style.display = "none";
  el.monthCloseActualInput.value = el.monthCloseActualInput.value || "";
  el.monthCloseExtraInput.value = el.monthCloseExtraInput.value || "0";
  el.monthCloseActualInput.focus();
}

async function calcFundStrategyFromForm() {
  const title = (el.fundTitleInput.value || "").trim();
  const whenRaw = (el.fundTargetMonthInput.value || "").trim();
  const amountRaw = (el.fundTargetAmountInput.value || "").trim();
  const alreadyRaw = (el.fundAlreadySavedInput.value || "0").trim();
  const priority = (el.fundPriorityInput.value || "medium").toLowerCase();
  if (!title) throw new Error("Заполните поле «На что?»");
  if (!whenRaw) throw new Error("Заполните поле «Когда?»");
  const target_amount = parseMoneyInput(amountRaw);
  const already_saved = Number(alreadyRaw || 0);
  if (!Number.isFinite(already_saved) || already_saved < 0) {
    throw new Error("Уже накоплено должно быть числом от 0");
  }
  const strategy = await api("/api/miniapp/budget/funds/strategy", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title,
      target_month: whenRaw,
      target_amount,
      already_saved,
      priority,
    }),
  });
  state.budget.strategyDraft = { title, whenRaw, target_amount, already_saved, priority, strategy };
  el.fundStrategyResult.textContent = [
    "Стратегия накопления",
    `Сейчас свободно: ${money(strategy.budget_now.free)}/мес`,
    `Нужно накопить: ${money(strategy.need)}`,
    `Осталось: ${strategy.months_left} мес`,
    `Рекомендуемый взнос: ${money(strategy.required_per_month)}/мес`,
    strategy.is_feasible ? "✅ План реалистичен" : `⚠️ Не хватает ${money(strategy.gap)}/мес`,
  ].join("\n");
}

async function saveFundFromForm() {
  if (!state.budget.strategyDraft) {
    await calcFundStrategyFromForm();
  }
  const draft = state.budget.strategyDraft;
  await api("/api/miniapp/budget/funds", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title: draft.title,
      target_month: draft.whenRaw,
      target_amount: draft.target_amount,
      already_saved: draft.already_saved,
      priority: draft.priority,
    }),
  });
  el.budgetFundPlannerCard.style.display = "none";
  el.fundStrategyResult.textContent = "";
  state.budget.strategyDraft = null;
  toast("Фонд сохранён");
  await loadBudgetDashboard();
}

async function submitMonthCloseFromForm() {
  try {
    const dashboard = await api("/api/miniapp/budget/dashboard");
    const planned = Number(dashboard.expenses_base || 0);
    const actual = parseMoneyInput(el.monthCloseActualInput.value || "");
    const extraRaw = (el.monthCloseExtraInput.value || "0").trim();
    const extra = extraRaw ? Number(extraRaw.replace(/\s+/g, "").replace(",", ".")) : 0;
    if (!Number.isFinite(extra) || extra < 0) {
      throw new Error("Доп. доходы должны быть числом от 0");
    }
    const extraItems = extra > 0 ? [{ type: "Другое", amount: extra }] : [];
    const res = await api("/api/miniapp/budget/month-close", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        month_key: dashboard.previous_month,
        planned_expenses_base: planned,
        actual_expenses_base: actual,
        extra_income_items: extraItems,
      }),
    });
    el.monthCloseForm.style.display = "none";
    el.monthCloseBody.style.display = "";
    const delta = Number(res.delta_expenses || 0);
    el.monthCloseBody.textContent = [
      "Месяц закрыт ✅",
      `План расходов: ${money(res.planned_expenses_base)}`,
      `Факт расходов: ${money(res.actual_expenses_base)}`,
      `Разница: ${money(delta)}`,
      `Доп. доходы: ${money(res.extra_income_total)}`,
    ].join("\n");
    toast("Закрытие месяца сохранено");
    await loadBudgetDashboard();
  } catch (e) {
    toast(e?.message || "Не удалось закрыть месяц");
  }
}

function calculateLoanMetrics({ amount, annualRate, months, paymentType }) {
  const principal = Number(amount || 0);
  const rateYear = Number(annualRate || 0);
  const termMonths = Number(months || 0);
  if (!Number.isFinite(principal) || principal <= 0) throw new Error("Сумма кредита должна быть больше 0");
  if (!Number.isFinite(rateYear) || rateYear < 0) throw new Error("Ставка должна быть числом от 0");
  if (!Number.isFinite(termMonths) || termMonths <= 0) throw new Error("Срок должен быть больше 0");
  const i = rateYear / 100 / 12;
  if (paymentType === "diff") {
    const principalPart = principal / termMonths;
    let total = 0;
    let first = 0;
    let last = 0;
    let remain = principal;
    for (let m = 1; m <= termMonths; m += 1) {
      const pay = principalPart + remain * i;
      if (m === 1) first = pay;
      if (m === termMonths) last = pay;
      total += pay;
      remain = Math.max(0, remain - principalPart);
    }
    return {
      monthly_payment: null,
      first_payment: first,
      last_payment: last,
      total_payment: total,
      overpayment: total - principal,
    };
  }
  let monthly = 0;
  if (i === 0) {
    monthly = principal / termMonths;
  } else {
    const factor = Math.pow(1 + i, termMonths);
    monthly = principal * i * factor / (factor - 1);
  }
  const total = monthly * termMonths;
  return {
    monthly_payment: monthly,
    first_payment: monthly,
    last_payment: monthly,
    total_payment: total,
    overpayment: total - principal,
  };
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
  document.querySelectorAll("#tabbarExchange .tab-btn[data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => setTab(btn.dataset.tab));
  });
  document.querySelectorAll("#tabbarBudget .tab-btn[data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setAppMode("mode");
      setTab("mode");
    });
  });
  document.querySelectorAll("#tabbarBudget .tab-btn[data-budget-tab]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      setAppMode("budget");
      setTab("budget");
      await loadBudgetDashboard();
      setBudgetTab(btn.dataset.budgetTab || "overview");
    });
  });
  el.switchModeBtn?.addEventListener("click", () => {
    setAppMode("mode");
    setTab("mode");
  });
  el.openExchangeBtn?.addEventListener("click", async () => {
    await setModePreference("exchange");
    setAppMode("exchange");
    setTab("dashboard");
  });
  el.openBudgetBtn?.addEventListener("click", async () => {
    await setModePreference("budget");
    setAppMode("budget");
    setTab("budget");
    await loadBudgetDashboard();
    setBudgetTab("overview");
  });
  el.openLastModeBtn?.addEventListener("click", async () => {
    if (!state.lastMode) return;
    if (state.lastMode === "budget") {
      setAppMode("budget");
      setTab("budget");
      await loadBudgetDashboard();
      setBudgetTab("overview");
      return;
    }
    setAppMode("exchange");
    setTab("dashboard");
  });
  el.startQuickOnboardingBtn?.addEventListener("click", async () => {
    try {
      await api("/api/miniapp/budget/profile", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ onboarding_mode: "quick" }),
      });
      openBudgetWizard("quick");
    } catch (_) {
      toast("Не удалось начать онбординг");
    }
  });
  el.startPreciseOnboardingBtn?.addEventListener("click", async () => {
    try {
      await api("/api/miniapp/budget/profile", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ onboarding_mode: "precise" }),
      });
      openBudgetWizard("precise");
    } catch (_) {
      toast("Не удалось начать онбординг");
    }
  });
  el.budgetBackBtn?.addEventListener("click", () => {
    state.budget.step = Math.max(1, state.budget.step - 1);
    renderBudgetWizardStep();
  });
  el.budgetSkipBtn?.addEventListener("click", () => {
    state.budget.step = Math.min(5, state.budget.step + 1);
    renderBudgetWizardStep();
  });
  el.budgetNextBtn?.addEventListener("click", async () => {
    try {
      if (state.budget.step === 2) {
        const incomeType = document.getElementById("wizardIncomeType")?.value || "fixed";
        const incomeRaw = document.getElementById("wizardIncome")?.value || "";
        const paydayRaw = document.getElementById("wizardPayday")?.value || "";
        const income = parseMoneyInput(incomeRaw);
        await api("/api/miniapp/budget/profile", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            income_type: incomeType,
            income_monthly: income,
            payday_day: paydayRaw ? Number(paydayRaw) : null,
          }),
        });
      }
      if (state.budget.step === 4) {
        const expRaw = document.getElementById("wizardExpensesBase")?.value || "";
        const expenses = parseMoneyInput(expRaw);
        await api("/api/miniapp/budget/profile", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ expenses_base: expenses }),
        });
      }
      if (state.budget.step >= 5) {
        await api("/api/miniapp/budget/profile", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ onboarding_completed: true }),
        });
        await completeBudgetOnboarding();
        return;
      }
      state.budget.step += 1;
      renderBudgetWizardStep();
    } catch (e) {
      toast(e?.message || "Проверьте данные");
    }
  });
  el.editBudgetBtn?.addEventListener("click", () => {
    state.budget.step = 1;
    el.budgetDashboardCard.style.display = "none";
    el.budgetFundsCard.style.display = "none";
    el.budgetMonthCloseCard.style.display = "none";
    el.budgetWizardCard.style.display = "";
    renderBudgetWizardStep();
  });
  el.planExpenseBtn?.addEventListener("click", openFundPlanFlow);
  el.saveTargetBtn?.addEventListener("click", openFundPlanFlow);
  el.closeMonthOpenBtn?.addEventListener("click", openMonthCloseFlow);
  el.budgetIncomeSaveBtn?.addEventListener("click", async () => {
    try {
      const incomeType = el.budgetIncomeTypeInput.value || "fixed";
      const income = parseMoneyInput(el.budgetIncomeInput.value || "");
      const payday = (el.budgetPaydayInput.value || "").trim();
      await api("/api/miniapp/budget/profile", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          income_type: incomeType,
          income_monthly: income,
          payday_day: payday ? Number(payday) : null,
        }),
      });
      toast("Доходы сохранены");
      await loadBudgetDashboard();
      setBudgetTab("income");
    } catch (e) {
      toast(e?.message || "Не удалось сохранить доходы");
    }
  });
  el.budgetExpensesSaveBtn?.addEventListener("click", async () => {
    try {
      const expenses = parseMoneyInput(el.budgetExpensesInput.value || "");
      await api("/api/miniapp/budget/profile", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ expenses_base: expenses }),
      });
      toast("Расходы сохранены");
      await loadBudgetDashboard();
      setBudgetTab("expenses");
    } catch (e) {
      toast(e?.message || "Не удалось сохранить расходы");
    }
  });
  el.fundCalcBtn?.addEventListener("click", async () => {
    try {
      await calcFundStrategyFromForm();
    } catch (e) {
      toast(e?.message || "Ошибка расчёта");
    }
  });
  el.fundSaveBtn?.addEventListener("click", async () => {
    try {
      await saveFundFromForm();
    } catch (e) {
      toast(e?.message || "Не удалось сохранить фонд");
    }
  });
  el.fundCancelBtn?.addEventListener("click", () => {
    el.budgetFundPlannerCard.style.display = "none";
    state.budget.strategyDraft = null;
    el.fundStrategyResult.textContent = "";
  });
  el.monthCloseSubmitBtn?.addEventListener("click", submitMonthCloseFromForm);
  el.monthCloseCancelBtn?.addEventListener("click", () => {
    el.monthCloseForm.style.display = "none";
  });
  el.loanCalcBtn?.addEventListener("click", () => {
    try {
      const calc = calculateLoanMetrics({
        amount: parseMoneyInput(el.loanAmountInput.value || ""),
        annualRate: Number(el.loanRateInput.value || 0),
        months: Number(el.loanMonthsInput.value || 0),
        paymentType: el.loanTypeInput.value || "annuity",
      });
      el.loanCalcResult.textContent = [
        calc.monthly_payment !== null ? `Ежемесячный платёж: ${money(calc.monthly_payment)}` : `Первый платёж: ${money(calc.first_payment)}`,
        calc.monthly_payment === null ? `Последний платёж: ${money(calc.last_payment)}` : "",
        `Переплата: ${money(calc.overpayment)}`,
        `Итоговая выплата: ${money(calc.total_payment)}`,
      ].filter(Boolean).join("\n");
    } catch (e) {
      toast(e?.message || "Ошибка расчёта");
    }
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
      await Promise.all([loadPortfolio(), loadAlerts(), loadUsdRub(), loadOpenCloseSetting(), loadModePreference()]);
      if (state.mode === "budget") {
        await loadBudgetDashboard();
      }
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

  document.querySelectorAll("#tab-movers .chip").forEach((chip) => {
    chip.addEventListener("click", async () => {
      document.querySelectorAll("#tab-movers .chip").forEach((c) => c.classList.remove("active"));
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
      loadModePreference(),
    ]);
    if (state.lastMode === "exchange") {
      setAppMode("exchange");
      setTab("dashboard");
    } else if (state.lastMode === "budget") {
      setAppMode("budget");
      setTab("budget");
      await loadBudgetDashboard();
      setBudgetTab("overview");
    } else {
      setAppMode("mode");
      setTab("mode");
    }
  } catch (e) {
    el.userLine.textContent = "Ошибка авторизации Mini App";
    toast("Не удалось инициализировать Mini App");
  }
})();
