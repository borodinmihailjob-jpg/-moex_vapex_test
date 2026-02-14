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
    expenseEditingId: null,
    expenseRateRows: [],
    goalEditingId: null,
    goalChecklistItems: [],
    activeLoanId: null,
    activeLoan: null,
    activeLoanSummary: null,
    loanView: "list",
    loanRateRows: [],
  },
};

function clamp255(value) {
  return Math.max(0, Math.min(255, Math.round(value)));
}

function normalizeHex(color, fallback) {
  const raw = String(color || "").trim();
  if (!raw) return fallback;
  const hex = raw.startsWith("#") ? raw.slice(1) : raw;
  if (/^[0-9a-fA-F]{3}$/.test(hex)) {
    return `#${hex.split("").map((c) => c + c).join("").toLowerCase()}`;
  }
  if (/^[0-9a-fA-F]{6}$/.test(hex)) {
    return `#${hex.toLowerCase()}`;
  }
  return fallback;
}

function hexToRgb(hex) {
  const n = normalizeHex(hex, "#000000").slice(1);
  return {
    r: parseInt(n.slice(0, 2), 16),
    g: parseInt(n.slice(2, 4), 16),
    b: parseInt(n.slice(4, 6), 16),
  };
}

function rgbToHex({ r, g, b }) {
  return `#${clamp255(r).toString(16).padStart(2, "0")}${clamp255(g).toString(16).padStart(2, "0")}${clamp255(b).toString(16).padStart(2, "0")}`;
}

function mixHex(base, withHex, ratio) {
  const a = hexToRgb(base);
  const b = hexToRgb(withHex);
  const p = Math.max(0, Math.min(1, ratio));
  return rgbToHex({
    r: a.r + (b.r - a.r) * p,
    g: a.g + (b.g - a.g) * p,
    b: a.b + (b.b - a.b) * p,
  });
}

function applyTelegramTheme() {
  const root = document.documentElement;
  const params = tg?.themeParams || {};
  const bg = normalizeHex(params.bg_color || params.secondary_bg_color, "#0b0d10");
  const surface = mixHex(bg, "#ffffff", 0.06);
  const elevated = mixHex(bg, "#ffffff", 0.1);
  const text = normalizeHex(params.text_color, "#f7f8fa");
  const hint = normalizeHex(params.hint_color, "#a6afbc");
  const button = normalizeHex(params.button_color || params.link_color, "#1db954");
  const buttonText = normalizeHex(params.button_text_color, "#041008");
  root.style.setProperty("--bg", bg);
  root.style.setProperty("--surface", surface);
  root.style.setProperty("--surface-elevated", elevated);
  root.style.setProperty("--text-primary", text);
  root.style.setProperty("--text-secondary", hint);
  root.style.setProperty("--separator", "rgba(255,255,255,0.12)");
  root.style.setProperty("--accent", button);
  root.style.setProperty("--accent-text", buttonText);
}

applyTelegramTheme();
tg?.onEvent?.("themeChanged", applyTelegramTheme);

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
  budgetLoansCard: document.getElementById("budgetLoansCard"),
  budgetLoanCreateCard: document.getElementById("budgetLoanCreateCard"),
  budgetLoanCard: document.getElementById("budgetLoanCard"),
  budgetLoanScheduleCard: document.getElementById("budgetLoanScheduleCard"),
  budgetLoanExtraCard: document.getElementById("budgetLoanExtraCard"),
  budgetLoanScenarioCard: document.getElementById("budgetLoanScenarioCard"),
  budgetLoanTipsCard: document.getElementById("budgetLoanTipsCard"),
  budgetMonthCloseCard: document.getElementById("budgetMonthCloseCard"),
  budgetSavingsCard: document.getElementById("budgetSavingsCard"),
  budgetSettingsCard: document.getElementById("budgetSettingsCard"),
  budgetWizardTitle: document.getElementById("budgetWizardTitle"),
  budgetStepProgress: document.getElementById("budgetStepProgress"),
  budgetWizardSubtitle: document.getElementById("budgetWizardSubtitle"),
  budgetWizardBody: document.getElementById("budgetWizardBody"),
  budgetBackBtn: document.getElementById("budgetBackBtn"),
  budgetSkipBtn: document.getElementById("budgetSkipBtn"),
  budgetNextBtn: document.getElementById("budgetNextBtn"),
  budgetResultBody: document.getElementById("budgetResultBody"),
  budgetResultActions: document.getElementById("budgetResultActions"),
  budgetOverviewDonut: document.getElementById("budgetOverviewDonut"),
  budgetOverviewShare: document.getElementById("budgetOverviewShare"),
  budgetCurrentMonth: document.getElementById("budgetCurrentMonth"),
  budgetGoalsOverview: document.getElementById("budgetGoalsOverview"),
  editBudgetBtn: document.getElementById("editBudgetBtn"),
  budgetIncomeTypeInput: document.getElementById("budgetIncomeTypeInput"),
  budgetIncomeOpenAddBtn: document.getElementById("budgetIncomeOpenAddBtn"),
  budgetIncomeBackBtn: document.getElementById("budgetIncomeBackBtn"),
  budgetIncomeFormWrap: document.getElementById("budgetIncomeFormWrap"),
  budgetIncomeTitleInput: document.getElementById("budgetIncomeTitleInput"),
  budgetIncomeInput: document.getElementById("budgetIncomeInput"),
  budgetIncomeSaveBtn: document.getElementById("budgetIncomeSaveBtn"),
  budgetIncomeList: document.getElementById("budgetIncomeList"),
  budgetResetBtn: document.getElementById("budgetResetBtn"),
  budgetNotifSummaryToggle: document.getElementById("budgetNotifSummaryToggle"),
  budgetNotifGoalsToggle: document.getElementById("budgetNotifGoalsToggle"),
  budgetNotifMonthCloseToggle: document.getElementById("budgetNotifMonthCloseToggle"),
  budgetNotifSaveBtn: document.getElementById("budgetNotifSaveBtn"),
  budgetExpenseTypeInput: document.getElementById("budgetExpenseTypeInput"),
  budgetExpensesOpenAddBtn: document.getElementById("budgetExpensesOpenAddBtn"),
  budgetExpensesBackBtn: document.getElementById("budgetExpensesBackBtn"),
  budgetExpensesFormWrap: document.getElementById("budgetExpensesFormWrap"),
  budgetExpenseTitleInput: document.getElementById("budgetExpenseTitleInput"),
  expenseFieldsRent: document.getElementById("expenseFieldsRent"),
  expenseFieldsMortgage: document.getElementById("expenseFieldsMortgage"),
  expenseFieldsLoan: document.getElementById("expenseFieldsLoan"),
  expenseFieldsUtilities: document.getElementById("expenseFieldsUtilities"),
  expenseFieldsOther: document.getElementById("expenseFieldsOther"),
  expenseRentDateInput: document.getElementById("expenseRentDateInput"),
  expenseRentAmountInput: document.getElementById("expenseRentAmountInput"),
  expenseMortgageStartInput: document.getElementById("expenseMortgageStartInput"),
  expenseMortgageEndInput: document.getElementById("expenseMortgageEndInput"),
  expenseMortgagePrincipalInput: document.getElementById("expenseMortgagePrincipalInput"),
  expenseMortgageMonthsInput: document.getElementById("expenseMortgageMonthsInput"),
  expenseMortgagePaymentTypeInput: document.getElementById("expenseMortgagePaymentTypeInput"),
  expenseLoanStartInput: document.getElementById("expenseLoanStartInput"),
  expenseLoanEndInput: document.getElementById("expenseLoanEndInput"),
  expenseLoanPrincipalInput: document.getElementById("expenseLoanPrincipalInput"),
  expenseLoanMonthsInput: document.getElementById("expenseLoanMonthsInput"),
  expenseLoanPaymentTypeInput: document.getElementById("expenseLoanPaymentTypeInput"),
  expenseUtilitiesDateInput: document.getElementById("expenseUtilitiesDateInput"),
  expenseUtilitiesAmountInput: document.getElementById("expenseUtilitiesAmountInput"),
  expenseOtherAmountInput: document.getElementById("expenseOtherAmountInput"),
  expenseRatePeriodsCard: document.getElementById("expenseRatePeriodsCard"),
  expenseRatePeriodsList: document.getElementById("expenseRatePeriodsList"),
  expenseAddRateBtn: document.getElementById("expenseAddRateBtn"),
  budgetExpensesCancelEditBtn: document.getElementById("budgetExpensesCancelEditBtn"),
  budgetExpensesCalcResult: document.getElementById("budgetExpensesCalcResult"),
  budgetExpensesList: document.getElementById("budgetExpensesList"),
  budgetExpensesSaveBtn: document.getElementById("budgetExpensesSaveBtn"),
  budgetFundsList: document.getElementById("budgetFundsList"),
  loansOpenCreateBtn: document.getElementById("loansOpenCreateBtn"),
  loansList: document.getElementById("loansList"),
  loanNameInput: document.getElementById("loanNameInput"),
  loanPrincipalInput: document.getElementById("loanPrincipalInput"),
  loanCurrentPrincipalInput: document.getElementById("loanCurrentPrincipalInput"),
  loanAnnualRateInput: document.getElementById("loanAnnualRateInput"),
  loanPaymentTypeInput: document.getElementById("loanPaymentTypeInput"),
  loanTermMonthsInput: document.getElementById("loanTermMonthsInput"),
  loanFirstPaymentDateInput: document.getElementById("loanFirstPaymentDateInput"),
  loanIssueDateInput: document.getElementById("loanIssueDateInput"),
  loanCurrencyInput: document.getElementById("loanCurrencyInput"),
  loanRatePeriodsList: document.getElementById("loanRatePeriodsList"),
  loanAddRatePeriodBtn: document.getElementById("loanAddRatePeriodBtn"),
  loanCreateError: document.getElementById("loanCreateError"),
  loanCreateSaveBtn: document.getElementById("loanCreateSaveBtn"),
  loanCreateCancelBtn: document.getElementById("loanCreateCancelBtn"),
  loanCardTitle: document.getElementById("loanCardTitle"),
  loanCardSubtitle: document.getElementById("loanCardSubtitle"),
  loanKeyStats: document.getElementById("loanKeyStats"),
  loanStructureDonut: document.getElementById("loanStructureDonut"),
  loanStructureShare: document.getElementById("loanStructureShare"),
  loanBalanceLine: document.getElementById("loanBalanceLine"),
  loanNextPayment: document.getElementById("loanNextPayment"),
  loanActionExtraBtn: document.getElementById("loanActionExtraBtn"),
  loanActionScheduleBtn: document.getElementById("loanActionScheduleBtn"),
  loanActionScenarioBtn: document.getElementById("loanActionScenarioBtn"),
  loanActionTipsBtn: document.getElementById("loanActionTipsBtn"),
  loanBackToListBtn: document.getElementById("loanBackToListBtn"),
  loanSchedulePageInput: document.getElementById("loanSchedulePageInput"),
  loanSchedulePageSizeInput: document.getElementById("loanSchedulePageSizeInput"),
  loanScheduleApplyBtn: document.getElementById("loanScheduleApplyBtn"),
  loanScheduleMeta: document.getElementById("loanScheduleMeta"),
  loanScheduleList: document.getElementById("loanScheduleList"),
  loanScheduleBackBtn: document.getElementById("loanScheduleBackBtn"),
  loanExtraAmountInput: document.getElementById("loanExtraAmountInput"),
  loanExtraDateInput: document.getElementById("loanExtraDateInput"),
  loanExtraModeInput: document.getElementById("loanExtraModeInput"),
  loanExtraStrategyInput: document.getElementById("loanExtraStrategyInput"),
  loanExtraPreviewBtn: document.getElementById("loanExtraPreviewBtn"),
  loanExtraPreview: document.getElementById("loanExtraPreview"),
  loanExtraSaveBtn: document.getElementById("loanExtraSaveBtn"),
  loanExtraBackBtn: document.getElementById("loanExtraBackBtn"),
  loanScenarioResult: document.getElementById("loanScenarioResult"),
  loanScenarioSchedule: document.getElementById("loanScenarioSchedule"),
  loanScenarioBackBtn: document.getElementById("loanScenarioBackBtn"),
  loanTipsList: document.getElementById("loanTipsList"),
  loanTipsBackBtn: document.getElementById("loanTipsBackBtn"),
  budgetSavingTypeInput: document.getElementById("budgetSavingTypeInput"),
  budgetSavingsOpenAddBtn: document.getElementById("budgetSavingsOpenAddBtn"),
  budgetSavingsBackBtn: document.getElementById("budgetSavingsBackBtn"),
  budgetSavingsFormWrap: document.getElementById("budgetSavingsFormWrap"),
  budgetSavingTitleInput: document.getElementById("budgetSavingTitleInput"),
  budgetSavingAmountInput: document.getElementById("budgetSavingAmountInput"),
  budgetSavingAddBtn: document.getElementById("budgetSavingAddBtn"),
  budgetSavingsList: document.getElementById("budgetSavingsList"),
  budgetHistoryList: document.getElementById("budgetHistoryList"),
  addGoalBtn: document.getElementById("addGoalBtn"),
  goalDetailCard: document.getElementById("goalDetailCard"),
  goalTitleInput: document.getElementById("goalTitleInput"),
  goalTargetDateInput: document.getElementById("goalTargetDateInput"),
  goalDescriptionInput: document.getElementById("goalDescriptionInput"),
  goalTargetAmountInput: document.getElementById("goalTargetAmountInput"),
  goalTopupAmountInput: document.getElementById("goalTopupAmountInput"),
  goalTopupBtn: document.getElementById("goalTopupBtn"),
  goalSaveBtn: document.getElementById("goalSaveBtn"),
  goalDeleteBtn: document.getElementById("goalDeleteBtn"),
  goalCancelBtn: document.getElementById("goalCancelBtn"),
  goalProgressText: document.getElementById("goalProgressText"),
  goalProgressFill: document.getElementById("goalProgressFill"),
  goalChecklistList: document.getElementById("goalChecklistList"),
  goalChecklistInput: document.getElementById("goalChecklistInput"),
  goalChecklistTemplateSelect: document.getElementById("goalChecklistTemplateSelect"),
  goalChecklistAddBtn: document.getElementById("goalChecklistAddBtn"),
  closeMonthOpenBtn: document.getElementById("closeMonthOpenBtn"),
  monthCloseForm: document.getElementById("monthCloseForm"),
  monthCloseActualInput: document.getElementById("monthCloseActualInput"),
  monthCloseExtraInput: document.getElementById("monthCloseExtraInput"),
  monthCloseSubmitBtn: document.getElementById("monthCloseSubmitBtn"),
  monthCloseCancelBtn: document.getElementById("monthCloseCancelBtn"),
  monthCloseBody: document.getElementById("monthCloseBody"),
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

function getBudgetGoals() {
  const rows = state.budget.dashboard?.funds || [];
  return rows.filter((x) => String(x?.status || "active") !== "deleted");
}

function addMonthsYmd(ymd, monthsToAdd) {
  const text = String(ymd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return "";
  const [y, m, d] = text.split("-").map((x) => Number(x));
  const base = new Date(Date.UTC(y, m - 1, d));
  if (Number.isNaN(base.getTime())) return "";
  const shift = Number(monthsToAdd || 0);
  const monthIndex = (m - 1) + shift;
  const targetYear = y + Math.floor(monthIndex / 12);
  const targetMonth = ((monthIndex % 12) + 12) % 12;
  const lastDay = new Date(Date.UTC(targetYear, targetMonth + 1, 0)).getUTCDate();
  const day = Math.min(d, lastDay);
  const result = new Date(Date.UTC(targetYear, targetMonth, day));
  const yy = result.getUTCFullYear();
  const mm = String(result.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(result.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function addDaysYmd(ymd, daysToAdd) {
  const text = String(ymd || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return "";
  const [y, m, d] = text.split("-").map((x) => Number(x));
  const dt = new Date(Date.UTC(y, m - 1, d));
  if (Number.isNaN(dt.getTime())) return "";
  dt.setUTCDate(dt.getUTCDate() + Number(daysToAdd || 0));
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
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
  show(el.budgetWelcomeCard, false);
  show(el.budgetWizardCard, false);
  show(el.budgetResultCard, false);
  show(el.budgetDashboardCard, tab === "overview");
  show(el.budgetIncomeCard, tab === "income");
  show(el.budgetExpensesCard, tab === "expenses");
  show(el.budgetLoansCard, tab === "loans");
  show(el.budgetLoanCreateCard, false);
  show(el.budgetLoanCard, false);
  show(el.budgetLoanScheduleCard, false);
  show(el.budgetLoanExtraCard, false);
  show(el.budgetLoanScenarioCard, false);
  show(el.budgetLoanTipsCard, false);
  show(el.budgetFundsCard, tab === "funds");
  if (tab !== "funds") {
    show(el.goalDetailCard, false);
  }
  show(el.budgetMonthCloseCard, tab === "close");
  show(el.budgetSavingsCard, tab === "savings");
  show(el.budgetSettingsCard, tab === "settings");
  setBudgetIncomeFormOpen(false);
  setBudgetExpensesFormOpen(false);
  setBudgetSavingsFormOpen(false);
  if (tab === "income") {
    loadBudgetIncomes().catch(() => {});
  }
  if (tab === "expenses") {
    loadBudgetExpenses().catch(() => {});
  }
  if (tab === "funds") {
    renderBudgetGoalsList(getBudgetGoals());
  }
  if (tab === "loans") {
    state.budget.loanView = "list";
    loadLoansList().catch(() => {});
  }
  if (tab === "savings") {
    loadBudgetSavings().catch(() => {});
    loadBudgetHistory().catch(() => {});
  }
  if (tab === "settings") {
    loadBudgetNotificationSettings().catch(() => {});
  }
}

function setBudgetIncomeFormOpen(open) {
  if (el.budgetIncomeFormWrap) el.budgetIncomeFormWrap.style.display = open ? "" : "none";
  if (el.budgetIncomeOpenAddBtn) el.budgetIncomeOpenAddBtn.style.display = open ? "none" : "";
  if (el.budgetIncomeList) el.budgetIncomeList.style.display = open ? "none" : "";
}

function setBudgetExpensesFormOpen(open) {
  if (el.budgetExpensesFormWrap) el.budgetExpensesFormWrap.style.display = open ? "" : "none";
  if (el.budgetExpensesOpenAddBtn) el.budgetExpensesOpenAddBtn.style.display = open ? "none" : "";
  if (el.budgetExpensesList) el.budgetExpensesList.style.display = open ? "none" : "";
}

function setBudgetSavingsFormOpen(open) {
  if (el.budgetSavingsFormWrap) el.budgetSavingsFormWrap.style.display = open ? "" : "none";
  if (el.budgetSavingsOpenAddBtn) el.budgetSavingsOpenAddBtn.style.display = open ? "none" : "";
  if (el.budgetSavingsList) el.budgetSavingsList.style.display = open ? "none" : "";
}

function renderEmpty(container, text) {
  container.innerHTML = `
    <div class="state">
      <p class="state-title">Пока пусто</p>
      <p class="state-text">${text}</p>
    </div>
  `;
}

function renderError(container, text) {
  container.innerHTML = `
    <div class="state">
      <p class="state-title">Не удалось загрузить</p>
      <p class="state-text">${text}</p>
    </div>
  `;
}

function renderSkeletonList(container, rows = 4) {
  container.innerHTML = `<div class="skeleton-list">${Array.from({ length: rows }).map(() => `
    <div class="skeleton-item">
      <div>
        <div class="skeleton-line"></div>
        <div class="skeleton-line short" style="margin-top:8px;"></div>
      </div>
      <div>
        <div class="skeleton-line"></div>
      </div>
    </div>
  `).join("")}</div>`;
}

function createListRow({ title, subtitle = "", right = "", rightClass = "", actionLabel = "", actionClass = "ghost", onAction = null }) {
  const item = document.createElement("div");
  item.className = "item";
  item.innerHTML = `
    <div class="left">
      <div class="name">${title}</div>
      ${subtitle ? `<div class="sub">${subtitle}</div>` : ""}
    </div>
    <div class="right ${rightClass}">
      ${right ? `<div>${right}</div>` : ""}
      ${actionLabel ? `<button class="btn ${actionClass}">${actionLabel}</button>` : ""}
    </div>
  `;
  if (actionLabel && onAction) {
    item.querySelector("button")?.addEventListener("click", onAction);
  }
  return item;
}

function renderSearchList(container, items, onPick) {
  container.innerHTML = "";
  if (!items.length) {
    renderEmpty(container, "Ничего не найдено");
    return;
  }
  items.forEach((s) => {
    const label = `${s.shortname || s.name || s.secid} (${s.secid})`;
    const item = createListRow({
      title: label,
      subtitle: s.boardid || "Основной рынок",
      actionLabel: "Выбрать",
      onAction: () => onPick(s, label),
    });
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
        renderError(resultEl, "Поиск временно недоступен. Попробуйте снова.");
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
    const item = createListRow({
      title: row.name || row.ticker,
      subtitle: `${row.ticker} · ${qtyText}`,
      right: `${money(row.value)}<div class="pnl ${pnlVal >= 0 ? "plus" : "minus"}">${pct(pnlVal)}</div>`,
    });
    el.positions.appendChild(item);
  });
}

async function loadPortfolio() {
  renderSkeletonList(el.positions, 4);
  try {
    const data = await api("/api/miniapp/portfolio");
    el.totalValue.textContent = money(data.summary?.total_value || 0);
    const totalPnl = Number(data.summary?.pnl_pct || 0);
    el.totalPnl.textContent = pct(totalPnl);
    el.totalPnl.style.color = totalPnl >= 0 ? "var(--success)" : "var(--danger)";
    renderPositions(data.positions || []);
  } catch (_) {
    renderError(el.positions, "Проверьте соединение и обновите экран.");
    throw _;
  }
}

async function loadAlerts() {
  renderSkeletonList(el.alerts, 3);
  try {
    const rows = await api("/api/miniapp/alerts");
    el.alerts.innerHTML = "";
    if (!rows.length) return renderEmpty(el.alerts, "Активных алертов пока нет.");
    rows.forEach((a) => {
      const label = `${a.shortname || a.secid} (${a.secid})`;
      const range = Number(a.range_percent || 0) > 0 ? `±${a.range_percent}%` : "Точная цена";
      const item = createListRow({
        title: label,
        subtitle: `${money(a.target_price)} · ${range}`,
        actionLabel: "Отключить",
        actionClass: "danger",
        onAction: async () => {
          try {
            await api(`/api/miniapp/alerts/${a.id}`, { method: "DELETE" });
            toast("Алерт отключён");
            await loadAlerts();
          } catch (_) {
            toast("Не удалось отключить алерт");
          }
        },
      });
      el.alerts.appendChild(item);
    });
  } catch (_) {
    renderError(el.alerts, "Не удалось получить список алертов.");
    throw _;
  }
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
    const item = createListRow({
      title: r.shortname || r.secid,
      subtitle: `${r.secid} · Цена: ${last} · Объём: ${volume}`,
      right: `<div class="pnl ${v >= 0 ? "plus" : "minus"}">${pct(v)}</div>`,
    });
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
  renderSkeletonList(el.articles, 3);
  try {
    const rows = await api("/api/miniapp/articles");
    el.articles.innerHTML = "";
    if (!rows.length) return renderEmpty(el.articles, "Материалы появятся позже.");
    rows.forEach((r) => {
      const item = createListRow({
        title: r.button_name,
        actionLabel: "Открыть",
        onAction: async () => {
          try {
            const article = await api(`/api/miniapp/articles/${encodeURIComponent(r.text_code)}`);
            el.articleText.textContent = article.value || "";
          } catch (_) {
            toast("Не удалось открыть материал");
          }
        },
      });
      el.articles.appendChild(item);
    });
  } catch (_) {
    renderError(el.articles, "Не удалось загрузить материалы.");
    throw _;
  }
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

  el.budgetWizardCard.style.display = "none";
  el.budgetResultCard.style.display = "none";
  const income = Number(data.income || 0);
  const expensesTotal = Number(data.expenses_total || 0);
  const savings = Number(data.savings_total || 0);
  const free = income - expensesTotal;

  renderBudgetOverviewMix({
    income,
    expenses: expensesTotal,
    savings,
  });
  el.budgetCurrentMonth.textContent = [
    `${monthLabel(data.month)}`,
    `Доходы в месяц: ${money(income)}`,
    `Расходы в месяц: ${money(expensesTotal)}`,
    `Накопления: ${money(savings)}`,
    `${free >= 0 ? "Свободно в бюджете" : "Дефицит бюджета"}: ${money(Math.abs(free))}/мес`,
  ].join("\n");
  renderBudgetOverviewGoals(getBudgetGoals());
  renderBudgetFunds(getBudgetGoals());
  resetIncomeForm();
  resetSavingForm();
  if (el.budgetExpensesCalcResult) {
    el.budgetExpensesCalcResult.textContent = "";
  }
  setBudgetTab(state.budget.activeTab || "overview");
}

function renderBudgetOverviewGoals(funds) {
  if (!el.budgetGoalsOverview) return;
  el.budgetGoalsOverview.innerHTML = "";
  if (!funds.length) {
    renderEmpty(el.budgetGoalsOverview, "Пока нет целей. Добавьте первую цель в разделе «Цели».");
    return;
  }
  funds.forEach((fund) => {
    const target = Number(fund.target_amount || 0);
    const saved = Number(fund.already_saved || 0);
    const pctRaw = target > 0 ? (saved / target) * 100 : 0;
    const pctVal = Math.max(0, Math.min(100, pctRaw));
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="left">
        <div class="name">${fund.title}</div>
        <div class="sub">${money(saved)} из ${money(target)} • осталось ${fund.months_left} мес</div>
        <div class="progress-line"><div class="progress-fill" style="width:${pctVal.toFixed(1)}%"></div></div>
      </div>
      <div class="right"><div>${pctVal.toFixed(0)}%</div></div>
    `;
    el.budgetGoalsOverview.appendChild(item);
  });
}

function renderBudgetOverviewMix({ income, expenses, savings }) {
  const donut = el.budgetOverviewDonut;
  const share = el.budgetOverviewShare;
  if (!donut || !share) return;
  const safeIncome = Math.max(0, Number(income || 0));
  const safeExpenses = Math.max(0, Number(expenses || 0));
  const safeSavings = Math.max(0, Number(savings || 0));
  const total = safeIncome + safeExpenses + safeSavings;
  const pctIncome = total > 0 ? (safeIncome / total) * 100 : 0;
  const pctExpenses = total > 0 ? (safeExpenses / total) * 100 : 0;
  const pctSavings = total > 0 ? (safeSavings / total) * 100 : 0;
  const degIncomeEnd = pctIncome * 3.6;
  const degExpensesEnd = (pctIncome + pctExpenses) * 3.6;
  donut.style.background = total > 0
    ? `conic-gradient(#2c8fdf 0deg ${degIncomeEnd.toFixed(2)}deg, #d1497a ${degIncomeEnd.toFixed(2)}deg ${degExpensesEnd.toFixed(2)}deg, #1ea86c ${degExpensesEnd.toFixed(2)}deg 360deg)`
    : "conic-gradient(rgba(32,52,94,0.12) 0deg 360deg)";

  share.innerHTML = "";
  const rows = [
    { cls: "income", label: "Доходы", amount: safeIncome, pct: pctIncome },
    { cls: "expenses", label: "Расходы", amount: safeExpenses, pct: pctExpenses },
    { cls: "savings", label: "Накопления", amount: safeSavings, pct: pctSavings },
  ];
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="left">
        <span class="dot ${row.cls}"></span>
        <div>
          <div class="name">${row.label}</div>
          <div class="sub">${money(row.amount)}</div>
        </div>
      </div>
      <div class="right"><div>${row.pct.toFixed(1)}%</div></div>
    `;
    share.appendChild(item);
  });
}

function incomeKindLabel(kind) {
  const map = {
    salary: "Зарплата",
    freelance: "Фриланс",
    business: "Бизнес",
    rent: "Аренда",
    passive: "Пассивный",
    other: "Другое",
  };
  return map[String(kind || "").toLowerCase()] || "Другое";
}

function savingKindLabel(kind) {
  const map = {
    deposit: "Депозит",
    stocks: "Акции",
    crypto: "Крипта",
    cash: "Наличные",
    other: "Другое",
  };
  return map[String(kind || "").toLowerCase()] || "Другое";
}

function resetIncomeForm() {
  if (el.budgetIncomeTypeInput && !el.budgetIncomeTypeInput.value) {
    el.budgetIncomeTypeInput.value = "salary";
  }
  if (el.budgetIncomeTitleInput) {
    el.budgetIncomeTitleInput.value = incomeKindLabel(el.budgetIncomeTypeInput?.value || "salary");
    el.budgetIncomeTitleInput.dataset.autoTitle = "1";
  }
  if (el.budgetIncomeInput) {
    el.budgetIncomeInput.value = "";
  }
}

function resetSavingForm() {
  if (el.budgetSavingTypeInput && !el.budgetSavingTypeInput.value) {
    el.budgetSavingTypeInput.value = "deposit";
  }
  if (el.budgetSavingTitleInput) {
    el.budgetSavingTitleInput.value = savingKindLabel(el.budgetSavingTypeInput?.value || "deposit");
    el.budgetSavingTitleInput.dataset.autoTitle = "1";
  }
  if (el.budgetSavingAmountInput) {
    el.budgetSavingAmountInput.value = "";
  }
}

function formatDateRu(isoDate) {
  const text = String(isoDate || "").trim();
  if (!text) return "—";
  const d = new Date(`${text}T00:00:00`);
  if (Number.isNaN(d.getTime())) return text;
  return d.toLocaleDateString("ru-RU", { day: "2-digit", month: "long", year: "numeric" });
}

function setLoanView(view) {
  state.budget.loanView = view;
  const show = (node, visible) => {
    if (!node) return;
    node.style.display = visible ? "" : "none";
  };
  show(el.budgetLoansCard, view === "list");
  show(el.budgetLoanCreateCard, view === "create");
  show(el.budgetLoanCard, view === "card");
  show(el.budgetLoanScheduleCard, view === "schedule");
  show(el.budgetLoanExtraCard, view === "extra");
  show(el.budgetLoanScenarioCard, view === "scenario");
  show(el.budgetLoanTipsCard, view === "tips");
}

function setLoanActionChip(active) {
  const pairs = [
    [el.loanActionExtraBtn, "extra"],
    [el.loanActionScheduleBtn, "schedule"],
    [el.loanActionScenarioBtn, "scenario"],
    [el.loanActionTipsBtn, "tips"],
  ];
  pairs.forEach(([btn, key]) => {
    if (!btn) return;
    btn.classList.toggle("active", key === active);
  });
}

function resetLoanCreateForm() {
  if (el.loanNameInput) el.loanNameInput.value = "";
  if (el.loanPrincipalInput) el.loanPrincipalInput.value = "";
  if (el.loanCurrentPrincipalInput) el.loanCurrentPrincipalInput.value = "";
  if (el.loanAnnualRateInput) el.loanAnnualRateInput.value = "";
  if (el.loanPaymentTypeInput) el.loanPaymentTypeInput.value = "ANNUITY";
  if (el.loanTermMonthsInput) el.loanTermMonthsInput.value = "240";
  if (el.loanFirstPaymentDateInput) el.loanFirstPaymentDateInput.value = addMonthsYmd(fmtDate(0), 1) || fmtDate(0);
  if (el.loanIssueDateInput) el.loanIssueDateInput.value = fmtDate(0);
  if (el.loanCurrencyInput) el.loanCurrencyInput.value = "RUB";
  const start = el.loanFirstPaymentDateInput?.value || fmtDate(0);
  const end = addMonthsYmd(start, Number(el.loanTermMonthsInput?.value || 240) - 1) || start;
  state.budget.loanRateRows = [{ start_date: start, end_date: end, annual_rate: "" }];
  renderLoanRateRows();
  if (el.loanCreateError) {
    el.loanCreateError.style.display = "none";
    el.loanCreateError.textContent = "";
  }
}

function addDaysYmdSafe(ymd, days) {
  return addDaysYmd(ymd, days) || ymd;
}

function normalizeLoanRateRows(rows, fallbackRate, firstPaymentDate, termMonths) {
  const out = (rows || []).map((x) => ({
    start_date: String(x.start_date || "").trim(),
    end_date: String(x.end_date || "").trim(),
    annual_rate: Number(x.annual_rate),
  })).filter((x) => x.start_date && x.end_date && Number.isFinite(x.annual_rate));
  if (!out.length) {
    const end = addMonthsYmd(firstPaymentDate, termMonths - 1) || firstPaymentDate;
    return [{ start_date: firstPaymentDate, end_date: end, annual_rate: fallbackRate }];
  }
  out.sort((a, b) => a.start_date.localeCompare(b.start_date));
  return out;
}

function renderLoanRateRows() {
  if (!el.loanRatePeriodsList) return;
  el.loanRatePeriodsList.innerHTML = "";
  if (!state.budget.loanRateRows.length) {
    state.budget.loanRateRows = [{ start_date: "", end_date: "", annual_rate: "" }];
  }
  state.budget.loanRateRows.forEach((row, idx) => {
    const wrap = document.createElement("div");
    wrap.className = "form-grid three";
    wrap.innerHTML = `
      <label>Ставка, %\n<input type=\"number\" step=\"0.01\" min=\"0\" max=\"100\" data-loan-rate-field=\"annual_rate\" data-loan-rate-idx=\"${idx}\" value=\"${row.annual_rate}\"></label>
      <label>Начало периода\n<input type=\"date\" data-loan-rate-field=\"start_date\" data-loan-rate-idx=\"${idx}\" value=\"${row.start_date}\"></label>
      <label>Конец периода\n<input type=\"date\" data-loan-rate-field=\"end_date\" data-loan-rate-idx=\"${idx}\" value=\"${row.end_date}\"></label>
      <div class=\"wizard-actions\"><button class=\"btn ghost\" data-loan-rate-remove=\"${idx}\" type=\"button\">Удалить период</button></div>
    `;
    el.loanRatePeriodsList.appendChild(wrap);
  });
  el.loanRatePeriodsList.querySelectorAll("[data-loan-rate-field]").forEach((input) => {
    input.addEventListener("input", () => {
      const idx = Number(input.dataset.loanRateIdx || -1);
      const key = input.dataset.loanRateField;
      if (!Number.isInteger(idx) || idx < 0 || !key) return;
      state.budget.loanRateRows[idx][key] = input.value;
    });
  });
  el.loanRatePeriodsList.querySelectorAll("[data-loan-rate-remove]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const idx = Number(btn.dataset.loanRateRemove || -1);
      if (!Number.isInteger(idx) || idx < 0) return;
      state.budget.loanRateRows.splice(idx, 1);
      renderLoanRateRows();
    });
  });
}

function showLoanCreateError(text) {
  if (!el.loanCreateError) return;
  el.loanCreateError.style.display = "";
  el.loanCreateError.textContent = String(text || "Проверьте данные");
  el.loanCreateError.style.color = "var(--danger)";
}

async function apiLoanCreate(payload) {
  const headers = new Headers({ "Content-Type": "application/json" });
  if (state.initData) headers.set("X-Telegram-Init-Data", state.initData);
  beginLoading("Сохраняю кредит…");
  try {
    const res = await fetch("/api/miniapp/loans", {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(body?.message || body?.error_code || "Не удалось создать кредит");
    }
    return body;
  } finally {
    endLoading();
  }
}

function renderLoanListRows(items) {
  if (!el.loansList) return;
  el.loansList.innerHTML = "";
  if (!items.length) {
    renderEmpty(el.loansList, "Тут появятся ваши кредиты. Добавьте ипотеку, и я соберу план выплат и подсказки.");
    return;
  }
  items.forEach((loan) => {
    const row = createListRow({
      title: String(loan.name || `Кредит #${loan.id}`),
      subtitle: `Остаток ${money(loan.current_principal || loan.principal)} из ${money(loan.principal)} • ${Number(loan.annual_rate).toFixed(2)}% • ${loan.term_months} мес.`,
      actionLabel: "Открыть",
      onAction: async () => {
        await openLoanCard(Number(loan.id));
      },
    });
    el.loansList.appendChild(row);
  });
}

async function loadLoansList() {
  if (!el.loansList) return;
  renderSkeletonList(el.loansList, 3);
  const data = await api("/api/miniapp/loans", { skipLoader: true });
  const items = data.items || [];
  state.budget.loans = items;
  renderLoanListRows(items);
}

function renderLoanStructure(summary) {
  if (!el.loanStructureDonut || !el.loanStructureShare) return;
  const interest = Math.max(0, Number(summary.total_interest || 0));
  const principal = Math.max(0, Number(summary.total_principal_paid || 0));
  const total = Math.max(0.01, interest + principal);
  const intPct = (interest / total) * 100;
  const princPct = 100 - intPct;
  const intDeg = intPct * 3.6;
  el.loanStructureDonut.style.background =
    `conic-gradient(#d1497a 0deg ${intDeg.toFixed(2)}deg, #1ea86c ${intDeg.toFixed(2)}deg 360deg)`;
  el.loanStructureShare.innerHTML = "";
  [
    { label: "Проценты", amount: interest, pct: intPct, cls: "expenses" },
    { label: "Тело долга", amount: principal, pct: princPct, cls: "savings" },
  ].forEach((x) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="left">
        <span class="dot ${x.cls}"></span>
        <div>
          <div class="name">${x.label}</div>
          <div class="sub">${money(x.amount)}</div>
        </div>
      </div>
      <div class="right"><div>${x.pct.toFixed(1)}%</div></div>
    `;
    el.loanStructureShare.appendChild(item);
  });
}

function renderLoanBalanceBars(schedule) {
  if (!el.loanBalanceLine) return;
  el.loanBalanceLine.innerHTML = "";
  if (!Array.isArray(schedule) || !schedule.length) return;
  const points = schedule.filter((_, idx) => idx % Math.max(1, Math.floor(schedule.length / 24)) === 0).slice(0, 24);
  const maxBalance = Math.max(...points.map((x) => Number(x.balance || 0)), 1);
  points.forEach((p) => {
    const h = Math.max(4, Math.round((Number(p.balance || 0) / maxBalance) * 100));
    const bar = document.createElement("div");
    bar.className = "bar";
    bar.style.height = `${h}%`;
    bar.title = `${formatDateRu(p.date)}: ${money(p.balance)}`;
    el.loanBalanceLine.appendChild(bar);
  });
}

function renderLoanCard(loan, summary, schedulePreview) {
  if (el.loanCardTitle) el.loanCardTitle.textContent = String(loan.name || `Кредит #${loan.id}`);
  if (el.loanCardSubtitle) {
    el.loanCardSubtitle.textContent = `Вы платите ${money(summary.monthly_payment)} в месяц. Закрытие: ${formatDateRu(summary.payoff_date)}.`;
  }
  if (el.loanKeyStats) {
    const alreadyPaidPrincipal = Number(summary.paid_principal_to_date || 0);
    const items = [
      { title: "Остаток долга", value: money(summary.remaining_balance) },
      { title: "Ежемесячный платеж", value: money(summary.monthly_payment) },
      { title: "Переплата по процентам", value: money(summary.total_interest) },
      { title: "Выплачено тела долга", value: `${money(alreadyPaidPrincipal)} • осталось ${summary.payments_count || 0} платежей` },
    ];
    el.loanKeyStats.innerHTML = "";
    items.forEach((x) => {
      const row = createListRow({ title: x.title, right: x.value });
      el.loanKeyStats.appendChild(row);
    });
  }
  renderLoanStructure(summary);
  renderLoanBalanceBars(schedulePreview || []);
  if (el.loanNextPayment) {
    const n = summary.next_payment || {};
    if (!n.date) {
      el.loanNextPayment.textContent = "Кредит закрыт.";
    } else {
      el.loanNextPayment.textContent = [
        `Следующий платеж: ${formatDateRu(n.date)}`,
        `В нем: проценты ${money(n.interest)}, тело ${money(n.principal)}`,
        `После платежа остаток: ${money(n.balance)}`,
      ].join("\n");
    }
  }
}

async function openLoanCard(loanId) {
  state.budget.activeLoanId = Number(loanId);
  const data = await api(`/api/miniapp/loans/${loanId}`, { loadingText: "Собираю карту кредита…" });
  state.budget.activeLoan = data.loan || null;
  state.budget.activeLoanSummary = data.summary || null;
  const sch = await api(`/api/miniapp/loans/${loanId}/schedule?page=1&page_size=24`, { skipLoader: true });
  renderLoanCard(data.loan, data.summary, sch.items || []);
  setLoanView("card");
  setLoanActionChip("extra");
}

async function loadLoanSchedule() {
  const loanId = Number(state.budget.activeLoanId || 0);
  if (!loanId) return;
  const page = Math.max(1, Number(el.loanSchedulePageInput?.value || 1));
  const pageSize = Math.min(120, Math.max(10, Number(el.loanSchedulePageSizeInput?.value || 60)));
  const data = await api(`/api/miniapp/loans/${loanId}/schedule?page=${page}&page_size=${pageSize}`, { loadingText: "Загружаю график…" });
  if (el.loanScheduleMeta) {
    el.loanScheduleMeta.textContent = `Версия ${data.version} • Периодов: ${data.total} • Стр. ${data.page}`;
  }
  if (el.loanScheduleList) {
    el.loanScheduleList.innerHTML = "";
    (data.items || []).forEach((row) => {
      const node = createListRow({
        title: formatDateRu(row.date),
        subtitle: `Проценты ${money(row.interest)} • Тело ${money(row.principal)}`,
        right: `${money(row.payment)}\nОстаток ${money(row.balance)}`,
      });
      el.loanScheduleList.appendChild(node);
    });
    if (!(data.items || []).length) {
      renderEmpty(el.loanScheduleList, "Список платежей пуст.");
    }
  }
}

async function previewExtraPayment() {
  const loanId = Number(state.budget.activeLoanId || 0);
  if (!loanId || !el.loanExtraPreview) return;
  const amount = parseMoneyInput(el.loanExtraAmountInput?.value || "");
  const date = String(el.loanExtraDateInput?.value || "").trim();
  const mode = String(el.loanExtraModeInput?.value || "ONE_TIME");
  const strategy = String(el.loanExtraStrategyInput?.value || "REDUCE_TERM");
  const data = await api(`/api/miniapp/loans/${loanId}/scenarios/preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      events: [{ type: "EXTRA_PAYMENT", date, amount, mode, strategy }],
    }),
    loadingText: "Считаю эффект досрочки…",
  });
  el.loanExtraPreview.textContent = [
    `Минус: ${Math.max(0, Number(data.months_diff || 0))} мес`,
    `Экономия процентов: ${money(data.interest_saving || 0)}`,
    `Новая дата закрытия: ${formatDateRu(data.scenario_summary?.payoff_date)}`,
    `Новый платеж: ${money(data.scenario_summary?.monthly_payment || 0)}`,
  ].join("\n");
}

async function saveExtraPayment() {
  const loanId = Number(state.budget.activeLoanId || 0);
  if (!loanId) return;
  const amount = parseMoneyInput(el.loanExtraAmountInput?.value || "");
  const date = String(el.loanExtraDateInput?.value || "").trim();
  const mode = String(el.loanExtraModeInput?.value || "ONE_TIME");
  const strategy = String(el.loanExtraStrategyInput?.value || "REDUCE_TERM");
  await api(`/api/miniapp/loans/${loanId}/events/extra-payment`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `loan-extra-${loanId}-${date}-${amount}-${mode}-${strategy}`,
    },
    body: JSON.stringify({ date, amount, mode, strategy }),
    loadingText: "Сохраняю досрочку…",
  });
}

function buildLoanPresetEvent(preset) {
  const baseDate = el.loanExtraDateInput?.value || fmtDate(0);
  if (preset === "plus5k") {
    return [{ type: "EXTRA_PAYMENT", date: baseDate, amount: 5000, mode: "MONTHLY", strategy: "REDUCE_TERM" }];
  }
  if (preset === "one100k") {
    return [{ type: "EXTRA_PAYMENT", date: baseDate, amount: 100000, mode: "ONE_TIME", strategy: "REDUCE_TERM" }];
  }
  if (preset === "rateMinus2") {
    const d = addMonthsYmd(baseDate, 6);
    const currentRate = Number(state.budget.activeLoan?.annual_rate || 0);
    const nextRate = Math.max(0, currentRate - 2);
    return [{ type: "RATE_CHANGE", date: d, annual_rate: Number(nextRate.toFixed(2)) }];
  }
  if (preset === "holiday3") {
    const start = baseDate;
    const end = addMonthsYmd(baseDate, 2);
    return [{ type: "HOLIDAY", start_date: start, end_date: end, holiday_type: "INTEREST_ONLY" }];
  }
  return [];
}

async function runScenarioPreset(preset) {
  const loanId = Number(state.budget.activeLoanId || 0);
  if (!loanId) return;
  const events = buildLoanPresetEvent(preset);
  const data = await api(`/api/miniapp/loans/${loanId}/scenarios/preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ events }),
    loadingText: "Считаю сценарий…",
  });
  if (el.loanScenarioResult) {
    el.loanScenarioResult.textContent = [
      `Экономия процентов: ${money(data.interest_saving || 0)}`,
      `На сколько раньше закрытие: ${Math.max(0, Number(data.months_diff || 0))} мес`,
      `Новая дата закрытия: ${formatDateRu(data.scenario_summary?.payoff_date)}`,
      `Новый платеж: ${money(data.scenario_summary?.monthly_payment || 0)}`,
    ].join("\n");
  }
  if (el.loanScenarioSchedule) {
    el.loanScenarioSchedule.innerHTML = "";
    (data.schedule_preview || []).slice(0, 12).forEach((row) => {
      const node = createListRow({
        title: formatDateRu(row.date),
        subtitle: `Проценты ${money(row.interest)} • Тело ${money(row.principal)}`,
        right: `${money(row.payment)}`,
      });
      el.loanScenarioSchedule.appendChild(node);
    });
  }
}

async function loadLoanTips() {
  const loanId = Number(state.budget.activeLoanId || 0);
  if (!loanId || !el.loanTipsList) return;
  const data = await api(`/api/miniapp/loans/${loanId}/tips`, { loadingText: "Готовлю советы…" });
  el.loanTipsList.innerHTML = "";
  (data.tips || []).forEach((tip) => {
    const node = createListRow({
      title: String(tip.title || "Совет"),
      subtitle: String(tip.text || ""),
    });
    el.loanTipsList.appendChild(node);
  });
  if (!(data.tips || []).length) {
    renderEmpty(el.loanTipsList, "Советы появятся после первого расчета.");
  }
}

async function loadBudgetSavings() {
  const data = await api("/api/miniapp/budget/savings", { skipLoader: true });
  renderBudgetSavings(data.items || []);
}

function renderBudgetSavings(items) {
  if (!el.budgetSavingsList) return;
  el.budgetSavingsList.innerHTML = "";
  if (!items.length) {
    renderEmpty(el.budgetSavingsList, "Пока тут пусто");
    return;
  }
  items.forEach((row) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="left">
        <div class="name">${row.title}</div>
        <div class="sub">${savingKindLabel(row.kind)} • ${money(row.amount)}</div>
      </div>
      <div class="right">
        <button class="btn ghost" data-saving-action="edit">Изменить</button>
        <button class="btn ghost" data-saving-action="topup">Пополнить</button>
        <button class="btn ghost" data-saving-action="spend">Потратить</button>
        <button class="btn danger" data-saving-action="delete">Удалить</button>
      </div>
    `;
    item.querySelector("[data-saving-action='edit']")?.addEventListener("click", async () => {
      const nextKind = window.prompt("Тип (deposit/stocks/crypto/cash/other)", row.kind || "other");
      if (nextKind === null) return;
      const nextTitle = window.prompt("Название", row.title || "");
      if (nextTitle === null) return;
      try {
        await api(`/api/miniapp/budget/savings/${row.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "edit", kind: String(nextKind).trim().toLowerCase(), title: String(nextTitle).trim() }),
        });
        toast("Накопление обновлено");
        await loadBudgetDashboard();
        await loadBudgetSavings();
        await loadBudgetHistory();
      } catch (e) {
        toast(e?.message || "Не удалось обновить накопление");
      }
    });
    item.querySelector("[data-saving-action='topup']")?.addEventListener("click", async () => {
      const amountRaw = window.prompt("Сумма пополнения, ₽", "");
      if (amountRaw === null) return;
      try {
        const amount = parseMoneyInput(amountRaw);
        await api(`/api/miniapp/budget/savings/${row.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "topup", amount }),
        });
        toast("Накопление пополнено");
        await loadBudgetDashboard();
        await loadBudgetSavings();
        await loadBudgetHistory();
      } catch (e) {
        toast(e?.message || "Не удалось пополнить накопление");
      }
    });
    item.querySelector("[data-saving-action='spend']")?.addEventListener("click", async () => {
      const amountRaw = window.prompt("Сумма списания, ₽", "");
      if (amountRaw === null) return;
      try {
        const amount = parseMoneyInput(amountRaw);
        await api(`/api/miniapp/budget/savings/${row.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "spend", amount }),
        });
        toast("Списание выполнено");
        await loadBudgetDashboard();
        await loadBudgetSavings();
        await loadBudgetHistory();
      } catch (e) {
        toast(e?.message || "Не удалось списать накопление");
      }
    });
    item.querySelector("[data-saving-action='delete']")?.addEventListener("click", async () => {
      if (!window.confirm("Удалить это накопление?")) return;
      try {
        await api(`/api/miniapp/budget/savings/${row.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "delete" }),
        });
        toast("Накопление удалено");
        await loadBudgetDashboard();
        await loadBudgetSavings();
        await loadBudgetHistory();
      } catch (e) {
        toast(e?.message || "Не удалось удалить накопление");
      }
    });
    el.budgetSavingsList.appendChild(item);
  });
}

function historyActionLabel(row) {
  const map = {
    create: "Создание",
    edit: "Изменение",
    delete: "Удаление",
    topup: "Пополнение",
    spend: "Списание",
    close: "Закрытие месяца",
    update: "Обновление",
    reset: "Сброс бюджета",
    paused: "Пауза",
    deleted: "Удаление",
    autopilot: "Автопилот",
    notification_update: "Настройки уведомлений",
  };
  const entityMap = {
    income: "доход",
    expense: "расход",
    saving: "накопление",
    goal: "цель",
    profile: "профиль",
    month_close: "месяц",
    settings: "настройки",
    budget: "бюджет",
    obligation: "обязательство",
  };
  return `${map[row.action] || row.action} · ${entityMap[row.entity] || row.entity}`;
}

async function loadBudgetHistory() {
  const data = await api("/api/miniapp/budget/history?limit=120", { skipLoader: true });
  renderBudgetHistory(data.items || []);
}

function renderBudgetHistory(items) {
  if (!el.budgetHistoryList) return;
  el.budgetHistoryList.innerHTML = "";
  if (!items.length) {
    renderEmpty(el.budgetHistoryList, "История пока пуста");
    return;
  }
  items.forEach((row) => {
    const item = document.createElement("div");
    item.className = "item";
    const dt = row.created_at ? new Date(row.created_at).toLocaleString("ru-RU") : "";
    item.innerHTML = `
      <div class="left">
        <div class="name">${historyActionLabel(row)}</div>
        <div class="sub">${dt}</div>
      </div>
    `;
    el.budgetHistoryList.appendChild(item);
  });
}

async function loadBudgetIncomes() {
  const data = await api("/api/miniapp/budget/incomes", { skipLoader: true });
  renderBudgetIncomes(data.items || []);
}

function renderBudgetIncomes(items) {
  if (!el.budgetIncomeList) return;
  el.budgetIncomeList.innerHTML = "";
  if (!items.length) {
    renderEmpty(el.budgetIncomeList, "Пока тут пусто");
    return;
  }
  items.forEach((row) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="left">
        <div class="name">${row.title}</div>
        <div class="sub">${incomeKindLabel(row.kind)} • ${money(row.amount_monthly)}/мес</div>
      </div>
      <div class="right">
        <button class="btn ghost" data-action="edit">Изменить</button>
        <button class="btn danger" data-action="delete">Удалить</button>
      </div>
    `;
    item.querySelector("[data-action='edit']")?.addEventListener("click", async () => {
      const editWrap = document.createElement("div");
      editWrap.className = "form-grid three";
      editWrap.innerHTML = `
        <label>Тип
          <select id="incomeEditKind-${row.id}">
            <option value="salary" ${row.kind === "salary" ? "selected" : ""}>Зарплата</option>
            <option value="freelance" ${row.kind === "freelance" ? "selected" : ""}>Фриланс</option>
            <option value="business" ${row.kind === "business" ? "selected" : ""}>Бизнес</option>
            <option value="rent" ${row.kind === "rent" ? "selected" : ""}>Аренда</option>
            <option value="passive" ${row.kind === "passive" ? "selected" : ""}>Пассивный</option>
            <option value="other" ${row.kind === "other" ? "selected" : ""}>Другое</option>
          </select>
        </label>
        <label>Название
          <input id="incomeEditTitle-${row.id}" type="text" value="${row.title}" />
        </label>
        <label>Сумма, ₽/мес
          <input id="incomeEditAmount-${row.id}" type="text" value="${Math.round(Number(row.amount_monthly || 0))}" />
        </label>
      `;
      const actions = document.createElement("div");
      actions.className = "wizard-actions";
      actions.innerHTML = `<button class="btn primary">Сохранить</button><button class="btn ghost">Отмена</button>`;
      actions.querySelector(".btn.primary")?.addEventListener("click", async () => {
        try {
          const kind = document.getElementById(`incomeEditKind-${row.id}`)?.value || "other";
          const title = (document.getElementById(`incomeEditTitle-${row.id}`)?.value || "").trim();
          const amount = parseMoneyInput(document.getElementById(`incomeEditAmount-${row.id}`)?.value || "");
          await api(`/api/miniapp/budget/incomes/${row.id}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "edit", kind, title, amount_monthly: amount }),
          });
          toast("Доход обновлён");
          await loadBudgetDashboard();
          await loadBudgetIncomes();
        } catch (e) {
          toast(e?.message || "Не удалось обновить доход");
        }
      });
      actions.querySelector(".btn.ghost")?.addEventListener("click", () => {
        item.querySelector(".left").style.display = "";
        item.querySelector(".right").style.display = "";
        editWrap.remove();
        actions.remove();
      });
      item.querySelector(".left").style.display = "none";
      item.querySelector(".right").style.display = "none";
      item.appendChild(editWrap);
      item.appendChild(actions);
    });
    item.querySelector("[data-action='delete']")?.addEventListener("click", async () => {
      try {
        await api(`/api/miniapp/budget/incomes/${row.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "delete" }),
        });
        toast("Доход удалён");
        await loadBudgetDashboard();
        await loadBudgetIncomes();
      } catch (e) {
        toast(e?.message || "Не удалось удалить доход");
      }
    });
    el.budgetIncomeList.appendChild(item);
  });
}

function expenseKindLabel(kind) {
  const map = {
    rent: "Аренда",
    mortgage: "Ипотека",
    loan: "Кредит",
    utilities: "ЖКХ",
    other: "Прочие расходы",
  };
  return map[String(kind || "").toLowerCase()] || "Прочие расходы";
}

function showExpenseFields(kind) {
  const show = (node, visible) => {
    if (!node) return;
    node.style.display = visible ? "" : "none";
  };
  show(el.expenseFieldsRent, kind === "rent");
  show(el.expenseFieldsMortgage, kind === "mortgage");
  show(el.expenseFieldsLoan, kind === "loan");
  show(el.expenseFieldsUtilities, kind === "utilities");
  show(el.expenseFieldsOther, kind === "other");
  show(el.expenseRatePeriodsCard, kind === "mortgage" || kind === "loan");
}

function getLoanRange(kind) {
  const startDate = kind === "mortgage"
    ? (el.expenseMortgageStartInput?.value || "")
    : (el.expenseLoanStartInput?.value || "");
  const months = Number(kind === "mortgage"
    ? (el.expenseMortgageMonthsInput?.value || 0)
    : (el.expenseLoanMonthsInput?.value || 0));
  if (!startDate || !Number.isFinite(months) || months <= 0) {
    return { startDate, months, endDate: "" };
  }
  return { startDate, months, endDate: addMonthsYmd(startDate, months - 1) };
}

function syncLoanEndDateFields() {
  const mortgage = getLoanRange("mortgage");
  const loan = getLoanRange("loan");
  if (el.expenseMortgageEndInput) el.expenseMortgageEndInput.value = mortgage.endDate || "";
  if (el.expenseLoanEndInput) el.expenseLoanEndInput.value = loan.endDate || "";

  const activeKind = el.budgetExpenseTypeInput?.value || "rent";
  if ((activeKind === "mortgage" || activeKind === "loan") && state.budget.expenseRateRows.length === 1) {
    const row = state.budget.expenseRateRows[0];
    const range = getLoanRange(activeKind);
    if (row && row.auto_range && range.startDate && range.endDate) {
      row.start_date = range.startDate;
      row.end_date = range.endDate;
      renderExpenseRateRows();
    }
  }
}

function normalizeAndValidateRatePeriods(rows, loanStart, loanEnd) {
  const parsed = rows.map((row) => {
    const annualRaw = String(row?.annual_rate ?? "").trim();
    const startRaw = String(row?.start_date ?? "").trim();
    const endRaw = String(row?.end_date ?? "").trim();
    if (!annualRaw) throw new Error("Укажите ставку для каждого периода");
    if (!startRaw || !endRaw) throw new Error("Укажите даты начала и конца для каждого периода ставки");
    const annualRate = Number(annualRaw.replace(",", "."));
    if (!Number.isFinite(annualRate) || annualRate < 0) throw new Error("Ставка должна быть числом от 0");
    if (startRaw > endRaw) throw new Error("Дата начала периода не может быть позже даты конца");
    return { annual_rate: annualRate, start_date: startRaw, end_date: endRaw };
  }).sort((a, b) => a.start_date.localeCompare(b.start_date));

  if (!parsed.length) throw new Error("Добавьте хотя бы одну ставку");
  if (parsed[0].start_date > loanStart) throw new Error("Периоды ставок должны начинаться не позже даты старта кредита/ипотеки");
  let prevEnd = "";
  parsed.forEach((row, idx) => {
    if (idx === 0) {
      prevEnd = row.end_date;
      return;
    }
    const expectedStart = addDaysYmd(prevEnd, 1);
    if (row.start_date < expectedStart) throw new Error("Периоды ставок пересекаются");
    if (row.start_date > expectedStart) throw new Error(`Есть разрыв между периодами ставок (${prevEnd} → ${row.start_date})`);
    prevEnd = row.end_date;
  });
  if (prevEnd < loanEnd) {
    throw new Error(`Периоды ставок должны покрывать срок до ${loanEnd}`);
  }
  return parsed;
}

function addExpenseRateRow(row = null) {
  state.budget.expenseRateRows.push({
    annual_rate: row?.annual_rate ?? "",
    start_date: row?.start_date ?? "",
    end_date: row?.end_date ?? "",
    auto_range: !!row?.auto_range,
  });
  renderExpenseRateRows();
}

function renderExpenseRateRows() {
  if (!el.expenseRatePeriodsList) return;
  el.expenseRatePeriodsList.innerHTML = "";
  state.budget.expenseRateRows.forEach((row, idx) => {
    const wrap = document.createElement("div");
    wrap.className = "form-grid three";
    wrap.innerHTML = `
      <label>Ставка, % годовых
        <input type="number" step="0.01" min="0" value="${row.annual_rate}" data-rate-field="annual_rate" data-rate-idx="${idx}" />
      </label>
      <label>Начало периода
        <input type="date" value="${row.start_date}" data-rate-field="start_date" data-rate-idx="${idx}" />
      </label>
      <label>Конец периода
        <input type="date" value="${row.end_date}" data-rate-field="end_date" data-rate-idx="${idx}" />
      </label>
      <div><button class="btn danger" type="button" data-rate-remove="${idx}">Удалить ставку</button></div>
    `;
    wrap.querySelectorAll("[data-rate-field]").forEach((input) => {
      input.addEventListener("input", () => {
        const i = Number(input.dataset.rateIdx);
        const field = input.dataset.rateField;
        if (Number.isNaN(i) || !field) return;
        state.budget.expenseRateRows[i][field] = input.value;
        if (field === "start_date" || field === "end_date") {
          state.budget.expenseRateRows[i].auto_range = false;
        }
      });
    });
    wrap.querySelector("[data-rate-remove]")?.addEventListener("click", () => {
      state.budget.expenseRateRows.splice(idx, 1);
      renderExpenseRateRows();
    });
    el.expenseRatePeriodsList.appendChild(wrap);
  });
}

function clearExpenseForm() {
  state.budget.expenseEditingId = null;
  state.budget.expenseRateRows = [];
  if (el.budgetExpenseTypeInput) el.budgetExpenseTypeInput.value = "rent";
  if (el.budgetExpenseTitleInput) {
    el.budgetExpenseTitleInput.value = "Аренда";
    el.budgetExpenseTitleInput.dataset.autoTitle = "1";
  }
  if (el.expenseRentDateInput) el.expenseRentDateInput.value = "";
  if (el.expenseRentAmountInput) el.expenseRentAmountInput.value = "";
  if (el.expenseMortgageStartInput) el.expenseMortgageStartInput.value = "";
  if (el.expenseMortgageEndInput) el.expenseMortgageEndInput.value = "";
  if (el.expenseMortgagePrincipalInput) el.expenseMortgagePrincipalInput.value = "";
  if (el.expenseMortgageMonthsInput) el.expenseMortgageMonthsInput.value = "";
  if (el.expenseMortgagePaymentTypeInput) el.expenseMortgagePaymentTypeInput.value = "annuity";
  if (el.expenseLoanStartInput) el.expenseLoanStartInput.value = "";
  if (el.expenseLoanEndInput) el.expenseLoanEndInput.value = "";
  if (el.expenseLoanPrincipalInput) el.expenseLoanPrincipalInput.value = "";
  if (el.expenseLoanMonthsInput) el.expenseLoanMonthsInput.value = "";
  if (el.expenseLoanPaymentTypeInput) el.expenseLoanPaymentTypeInput.value = "annuity";
  if (el.expenseUtilitiesDateInput) el.expenseUtilitiesDateInput.value = "";
  if (el.expenseUtilitiesAmountInput) el.expenseUtilitiesAmountInput.value = "";
  if (el.expenseOtherAmountInput) el.expenseOtherAmountInput.value = "";
  if (el.budgetExpensesSaveBtn) el.budgetExpensesSaveBtn.textContent = "Сохранить";
  if (el.budgetExpensesCancelEditBtn) el.budgetExpensesCancelEditBtn.style.display = "none";
  if (el.budgetExpensesCalcResult) el.budgetExpensesCalcResult.textContent = "";
  showExpenseFields("rent");
  renderExpenseRateRows();
  syncLoanEndDateFields();
}

function buildExpensePayloadFromForm() {
  const kind = el.budgetExpenseTypeInput?.value || "rent";
  const title = (el.budgetExpenseTitleInput?.value || "").trim() || expenseKindLabel(kind);
  if (kind === "rent") {
    return {
      kind,
      title,
      payment_date: el.expenseRentDateInput?.value || "",
      amount_monthly: parseMoneyInput(el.expenseRentAmountInput?.value || ""),
    };
  }
  if (kind === "utilities") {
    return {
      kind,
      title,
      payment_date: el.expenseUtilitiesDateInput?.value || "",
      amount_monthly: parseMoneyInput(el.expenseUtilitiesAmountInput?.value || ""),
    };
  }
  if (kind === "other") {
    return {
      kind,
      title,
      amount_monthly: parseMoneyInput(el.expenseOtherAmountInput?.value || ""),
    };
  }
  if (!state.budget.expenseRateRows.length) {
    throw new Error("Добавьте хотя бы одну ставку");
  }
  const base = kind === "mortgage"
    ? {
        start_date: el.expenseMortgageStartInput?.value || "",
        principal: parseMoneyInput(el.expenseMortgagePrincipalInput?.value || ""),
        months: Number(el.expenseMortgageMonthsInput?.value || 0),
        payment_type: el.expenseMortgagePaymentTypeInput?.value || "annuity",
      }
    : {
        start_date: el.expenseLoanStartInput?.value || "",
        principal: parseMoneyInput(el.expenseLoanPrincipalInput?.value || ""),
        months: Number(el.expenseLoanMonthsInput?.value || 0),
        payment_type: el.expenseLoanPaymentTypeInput?.value || "annuity",
      };
  if (!base.start_date) throw new Error("Укажите дату начала");
  if (!Number.isFinite(base.months) || base.months <= 0) throw new Error("Укажите срок в месяцах");
  const loanEnd = addMonthsYmd(base.start_date, base.months - 1);
  const rows = state.budget.expenseRateRows.map((row) => ({ ...row }));
  if (rows.length === 1) {
    if (!rows[0].start_date) rows[0].start_date = base.start_date;
    if (!rows[0].end_date) rows[0].end_date = loanEnd;
  }
  const rate_periods = normalizeAndValidateRatePeriods(rows, base.start_date, loanEnd);
  return { kind, title, ...base, rate_periods };
}

async function loadBudgetExpenses() {
  const data = await api("/api/miniapp/budget/expenses", { skipLoader: true });
  renderBudgetExpenses(data.items || []);
}

function fillExpenseFormFromRow(row) {
  const kind = String(row.kind || "other");
  const payload = row.payload || {};
  if (el.budgetExpenseTypeInput) el.budgetExpenseTypeInput.value = kind;
  if (el.budgetExpenseTitleInput) {
    el.budgetExpenseTitleInput.value = row.title || expenseKindLabel(kind);
    el.budgetExpenseTitleInput.dataset.autoTitle = "0";
  }
  showExpenseFields(kind);
  if (kind === "rent") {
    el.expenseRentDateInput.value = payload.payment_date || "";
    el.expenseRentAmountInput.value = String(Math.round(Number(row.amount_monthly || 0)));
  } else if (kind === "utilities") {
    el.expenseUtilitiesDateInput.value = payload.payment_date || "";
    el.expenseUtilitiesAmountInput.value = String(Math.round(Number(row.amount_monthly || 0)));
  } else if (kind === "other") {
    el.expenseOtherAmountInput.value = String(Math.round(Number(row.amount_monthly || 0)));
  } else if (kind === "mortgage") {
    el.expenseMortgageStartInput.value = payload.start_date || "";
    el.expenseMortgagePrincipalInput.value = String(Math.round(Number(payload.principal || 0)));
    el.expenseMortgageMonthsInput.value = String(Number(payload.months || 0) || "");
    if (el.expenseMortgageEndInput) {
      el.expenseMortgageEndInput.value = payload.start_date && payload.months
        ? addMonthsYmd(payload.start_date, Number(payload.months) - 1)
        : "";
    }
    el.expenseMortgagePaymentTypeInput.value = payload.payment_type || "annuity";
    state.budget.expenseRateRows = Array.isArray(payload.rate_periods)
      ? payload.rate_periods.map((x) => ({ ...x, auto_range: false }))
      : [];
    renderExpenseRateRows();
  } else if (kind === "loan") {
    el.expenseLoanStartInput.value = payload.start_date || "";
    el.expenseLoanPrincipalInput.value = String(Math.round(Number(payload.principal || 0)));
    el.expenseLoanMonthsInput.value = String(Number(payload.months || 0) || "");
    if (el.expenseLoanEndInput) {
      el.expenseLoanEndInput.value = payload.start_date && payload.months
        ? addMonthsYmd(payload.start_date, Number(payload.months) - 1)
        : "";
    }
    el.expenseLoanPaymentTypeInput.value = payload.payment_type || "annuity";
    state.budget.expenseRateRows = Array.isArray(payload.rate_periods)
      ? payload.rate_periods.map((x) => ({ ...x, auto_range: false }))
      : [];
    renderExpenseRateRows();
  }
  syncLoanEndDateFields();
}

function renderBudgetExpenses(items) {
  if (!el.budgetExpensesList) return;
  el.budgetExpensesList.innerHTML = "";
  if (!items.length) {
    renderEmpty(el.budgetExpensesList, "Пока тут пусто");
    return;
  }
  items.forEach((row) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="left">
        <div class="name">${row.title}</div>
        <div class="sub">${expenseKindLabel(row.kind)} • ${money(row.amount_monthly)}/мес</div>
      </div>
      <div class="right">
        <button class="btn ghost" data-expense-action="edit">Изменить</button>
        <button class="btn danger" data-expense-action="delete">Удалить</button>
      </div>
    `;
    item.querySelector("[data-expense-action='edit']")?.addEventListener("click", () => {
      state.budget.expenseEditingId = row.id;
      fillExpenseFormFromRow(row);
      el.budgetExpensesSaveBtn.textContent = "Сохранить изменения";
      el.budgetExpensesCancelEditBtn.style.display = "";
      el.budgetExpensesCalcResult.textContent = "";
      setBudgetExpensesFormOpen(true);
      el.content.scrollTo({ top: 0, behavior: "smooth" });
    });
    item.querySelector("[data-expense-action='delete']")?.addEventListener("click", async () => {
      try {
        await api(`/api/miniapp/budget/expenses/${row.id}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "delete" }),
        });
        toast("Расход удалён");
        await loadBudgetDashboard();
        await loadBudgetExpenses();
      } catch (e) {
        toast(e?.message || "Не удалось удалить расход");
      }
    });
    el.budgetExpensesList.appendChild(item);
  });
}

function renderBudgetGoalsList(rows) {
  el.budgetFundsList.innerHTML = "";
  if (!rows.length) {
    renderEmpty(el.budgetFundsList, "Пока нет целей. Нажмите «Добавить цель».");
    return;
  }
  rows.forEach((goal) => {
    const item = document.createElement("div");
    item.className = "item";
    const months = Number(goal.months_left || 0);
    const progress = Number(goal.progress_pct || 0);
    item.innerHTML = `
      <div class="left">
        <div class="name">${goal.title}</div>
        <div class="sub">${money(goal.already_saved)} из ${money(goal.target_amount)} • осталось ${months} мес</div>
        <div class="progress-line"><div class="progress-fill" style="width:${Math.max(0, Math.min(100, progress)).toFixed(1)}%"></div></div>
      </div>
      <div class="right"><button class="btn ghost">Открыть</button></div>
    `;
    item.querySelector("button")?.addEventListener("click", () => openGoalDetail(goal));
    el.budgetFundsList.appendChild(item);
  });
}

function renderBudgetFunds(rows) {
  renderBudgetGoalsList(rows);
}

async function loadBudgetNotificationSettings() {
  const data = await api("/api/miniapp/budget/settings/notifications", { skipLoader: true });
  if (el.budgetNotifSummaryToggle) el.budgetNotifSummaryToggle.checked = !!data.budget_summary_enabled;
  if (el.budgetNotifGoalsToggle) el.budgetNotifGoalsToggle.checked = !!data.goal_deadline_enabled;
  if (el.budgetNotifMonthCloseToggle) el.budgetNotifMonthCloseToggle.checked = !!data.month_close_enabled;
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
    <p class="hint">Цели можно добавить позже в разделе “Цели”.</p>
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
  openGoalDetail(null);
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

function renderGoalChecklist() {
  const items = state.budget.goalChecklistItems || [];
  if (!items.length) {
    el.goalChecklistList.textContent = "Пока нет чекбоксов.";
    return;
  }
  el.goalChecklistList.innerHTML = "";
  items.forEach((row, idx) => {
    const line = document.createElement("label");
    line.style.display = "block";
    line.innerHTML = `<input type="checkbox" ${row.done ? "checked" : ""} data-goal-check="${idx}" /> ${row.text}`;
    line.querySelector("input")?.addEventListener("change", (ev) => {
      state.budget.goalChecklistItems[idx].done = !!ev.target.checked;
    });
    el.goalChecklistList.appendChild(line);
  });
}

function openGoalDetail(goal) {
  state.budget.goalEditingId = goal ? Number(goal.id) : null;
  state.budget.goalChecklistItems = Array.isArray(goal?.checklist)
    ? goal.checklist.map((x) => (typeof x === "string" ? { text: x, done: false } : { text: String(x.text || ""), done: !!x.done }))
    : [];
  el.goalTitleInput.value = goal?.title || "";
  el.goalTargetDateInput.value = goal?.target_date || "";
  el.goalDescriptionInput.value = goal?.description || "";
  el.goalTargetAmountInput.value = goal ? String(Math.round(Number(goal.target_amount || 0))) : "";
  el.goalTopupAmountInput.value = "";
  const progress = Number(goal?.progress_pct || 0);
  const saved = Number(goal?.already_saved || 0);
  const target = Number(goal?.target_amount || 0);
  el.goalProgressText.textContent = goal ? `Прогресс: ${progress.toFixed(1)}% (${money(saved)} из ${money(target)})` : "Прогресс: 0%";
  if (el.goalProgressFill) {
    el.goalProgressFill.style.width = `${Math.max(0, Math.min(100, progress)).toFixed(1)}%`;
  }
  renderGoalChecklist();
  el.goalDetailCard.style.display = "";
  el.goalTitleInput.focus();
}

function collectGoalChecklistPayload() {
  return (state.budget.goalChecklistItems || [])
    .map((x) => ({ text: String(x.text || "").trim(), done: !!x.done }))
    .filter((x) => x.text);
}

async function saveGoalDetail() {
  const title = (el.goalTitleInput.value || "").trim();
  const target_date = (el.goalTargetDateInput.value || "").trim();
  const description = (el.goalDescriptionInput.value || "").trim();
  const target_amount = parseMoneyInput(el.goalTargetAmountInput.value || "");
  if (!title) throw new Error("Введите наименование цели");
  if (!target_date) throw new Error("Введите дату достижения цели");
  const checklist = collectGoalChecklistPayload();
  if (!state.budget.goalEditingId) {
    await api("/api/miniapp/budget/funds", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title,
        target_date,
        target_amount,
        already_saved: 0,
        priority: "medium",
        description,
        checklist,
      }),
    });
    toast("Цель добавлена");
  } else {
    await api(`/api/miniapp/budget/funds/${state.budget.goalEditingId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "edit",
        title,
        target_date,
        target_amount,
        description,
        checklist,
      }),
    });
    toast("Цель обновлена");
  }
  await loadBudgetDashboard();
  setBudgetTab("funds");
}

async function topupGoal() {
  if (!state.budget.goalEditingId) {
    throw new Error("Сначала сохраните цель");
  }
  const amount = parseMoneyInput(el.goalTopupAmountInput.value || "");
  await api(`/api/miniapp/budget/funds/${state.budget.goalEditingId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "topup", amount }),
  });
  el.goalTopupAmountInput.value = "";
  toast("Сумма добавлена");
  await loadBudgetDashboard();
  const goal = (state.budget.dashboard?.funds || []).find((x) => Number(x.id) === Number(state.budget.goalEditingId));
  if (goal) openGoalDetail(goal);
}

async function deleteGoal() {
  if (!state.budget.goalEditingId) return;
  await api(`/api/miniapp/budget/funds/${state.budget.goalEditingId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "delete" }),
  });
  state.budget.goalEditingId = null;
  state.budget.goalChecklistItems = [];
  el.goalDetailCard.style.display = "none";
  toast("Цель удалена");
  await loadBudgetDashboard();
  setBudgetTab("funds");
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
    setBudgetTab("income");
  });
  el.loansOpenCreateBtn?.addEventListener("click", () => {
    resetLoanCreateForm();
    setLoanView("create");
  });
  const syncLoanCreateRatePeriodRange = () => {
    const firstPaymentDate = String(el.loanFirstPaymentDateInput?.value || "").trim();
    const termMonths = Math.max(1, Number(el.loanTermMonthsInput?.value || 1));
    const end = addMonthsYmd(firstPaymentDate, termMonths - 1) || firstPaymentDate;
    if (!state.budget.loanRateRows.length) {
      state.budget.loanRateRows = [{ start_date: firstPaymentDate, end_date: end, annual_rate: el.loanAnnualRateInput?.value || "" }];
    }
    if (state.budget.loanRateRows.length === 1) {
      state.budget.loanRateRows[0].start_date = firstPaymentDate;
      state.budget.loanRateRows[0].end_date = end;
      if (!String(state.budget.loanRateRows[0].annual_rate || "").trim()) {
        state.budget.loanRateRows[0].annual_rate = el.loanAnnualRateInput?.value || "";
      }
    }
    renderLoanRateRows();
  };
  el.loanFirstPaymentDateInput?.addEventListener("change", syncLoanCreateRatePeriodRange);
  el.loanTermMonthsInput?.addEventListener("input", syncLoanCreateRatePeriodRange);
  el.loanAnnualRateInput?.addEventListener("input", () => {
    if (state.budget.loanRateRows.length === 1) {
      state.budget.loanRateRows[0].annual_rate = el.loanAnnualRateInput?.value || "";
      renderLoanRateRows();
    }
  });
  el.loanAddRatePeriodBtn?.addEventListener("click", () => {
    const rows = state.budget.loanRateRows || [];
    const firstPaymentDate = String(el.loanFirstPaymentDateInput?.value || "").trim();
    const termMonths = Math.max(1, Number(el.loanTermMonthsInput?.value || 1));
    const loanEnd = addMonthsYmd(firstPaymentDate, termMonths - 1) || firstPaymentDate;
    const last = rows[rows.length - 1];
    const nextStart = last?.end_date ? addDaysYmdSafe(last.end_date, 1) : firstPaymentDate;
    rows.push({ start_date: nextStart, end_date: loanEnd, annual_rate: el.loanAnnualRateInput?.value || "" });
    state.budget.loanRateRows = rows;
    renderLoanRateRows();
  });
  el.loanCreateCancelBtn?.addEventListener("click", () => {
    setLoanView("list");
  });
  el.loanCreateSaveBtn?.addEventListener("click", async () => {
    try {
      const principal = parseMoneyInput(el.loanPrincipalInput?.value || "");
      const currentPrincipal = parseMoneyInput(el.loanCurrentPrincipalInput?.value || "");
      if (currentPrincipal > principal) {
        throw new Error("Остаток основного долга не может быть больше суммы кредита");
      }
      const annualRate = Number(String(el.loanAnnualRateInput?.value || "").replace(",", "."));
      const termMonths = Number(el.loanTermMonthsInput?.value || 0);
      const paymentType = String(el.loanPaymentTypeInput?.value || "ANNUITY");
      const firstPaymentDate = String(el.loanFirstPaymentDateInput?.value || "").trim();
      const issueDate = String(el.loanIssueDateInput?.value || "").trim();
      if (!Number.isFinite(annualRate) || annualRate < 0 || annualRate > 100) {
        throw new Error("Ставка должна быть от 0 до 100");
      }
      if (!Number.isInteger(termMonths) || termMonths < 1 || termMonths > 600) {
        throw new Error("Срок должен быть от 1 до 600 месяцев");
      }
      if (!/^\d{4}-\d{2}-\d{2}$/.test(firstPaymentDate)) {
        throw new Error("Укажите дату первого платежа");
      }
      if (issueDate && !/^\d{4}-\d{2}-\d{2}$/.test(issueDate)) {
        throw new Error("Проверьте дату выдачи");
      }
      if (issueDate && firstPaymentDate <= issueDate) {
        throw new Error("Дата первого платежа должна быть после даты выдачи");
      }
      const ratePeriods = normalizeLoanRateRows(
        state.budget.loanRateRows,
        annualRate,
        firstPaymentDate,
        termMonths,
      );
      const created = await apiLoanCreate({
        name: (el.loanNameInput?.value || "").trim(),
        principal: principal.toFixed(2),
        current_principal: currentPrincipal.toFixed(2),
        annual_rate: annualRate.toFixed(2),
        payment_type: paymentType,
        term_months: termMonths,
        first_payment_date: firstPaymentDate,
        issue_date: issueDate || null,
        currency: String(el.loanCurrencyInput?.value || "RUB"),
        rate_periods: ratePeriods,
      });
      toast("Кредит добавлен");
      await loadLoansList();
      await openLoanCard(Number(created.loan_id));
    } catch (e) {
      showLoanCreateError(e?.message || "Не удалось сохранить кредит");
    }
  });
  el.loanBackToListBtn?.addEventListener("click", async () => {
    setLoanView("list");
    await loadLoansList();
  });
  el.loanActionScheduleBtn?.addEventListener("click", async () => {
    setLoanActionChip("schedule");
    setLoanView("schedule");
    await loadLoanSchedule();
  });
  el.loanActionExtraBtn?.addEventListener("click", async () => {
    setLoanActionChip("extra");
    setLoanView("extra");
    if (el.loanExtraDateInput && !el.loanExtraDateInput.value) {
      el.loanExtraDateInput.value = fmtDate(0);
    }
    if (el.loanExtraPreview) el.loanExtraPreview.textContent = "";
  });
  el.loanActionScenarioBtn?.addEventListener("click", () => {
    setLoanActionChip("scenario");
    setLoanView("scenario");
    if (el.loanScenarioResult) el.loanScenarioResult.textContent = "";
    if (el.loanScenarioSchedule) el.loanScenarioSchedule.innerHTML = "";
  });
  el.loanActionTipsBtn?.addEventListener("click", async () => {
    setLoanActionChip("tips");
    setLoanView("tips");
    await loadLoanTips();
  });
  el.loanScheduleApplyBtn?.addEventListener("click", async () => {
    await loadLoanSchedule();
  });
  el.loanScheduleBackBtn?.addEventListener("click", async () => {
    const loanId = Number(state.budget.activeLoanId || 0);
    if (loanId) await openLoanCard(loanId);
  });
  el.loanExtraPreviewBtn?.addEventListener("click", async () => {
    try {
      await previewExtraPayment();
    } catch (e) {
      toast(e?.message || "Не удалось посчитать превью");
    }
  });
  el.loanExtraSaveBtn?.addEventListener("click", async () => {
    try {
      await saveExtraPayment();
      toast("Досрочка сохранена");
      const loanId = Number(state.budget.activeLoanId || 0);
      if (loanId) await openLoanCard(loanId);
    } catch (e) {
      toast(e?.message || "Не удалось сохранить досрочку");
    }
  });
  el.loanExtraBackBtn?.addEventListener("click", async () => {
    const loanId = Number(state.budget.activeLoanId || 0);
    if (loanId) await openLoanCard(loanId);
  });
  document.querySelectorAll("[data-loan-preset]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      document.querySelectorAll("[data-loan-preset]").forEach((x) => x.classList.remove("active"));
      btn.classList.add("active");
      try {
        await runScenarioPreset(btn.dataset.loanPreset || "");
      } catch (e) {
        toast(e?.message || "Не удалось рассчитать сценарий");
      }
    });
  });
  el.loanScenarioBackBtn?.addEventListener("click", async () => {
    const loanId = Number(state.budget.activeLoanId || 0);
    if (loanId) await openLoanCard(loanId);
  });
  el.loanTipsBackBtn?.addEventListener("click", async () => {
    const loanId = Number(state.budget.activeLoanId || 0);
    if (loanId) await openLoanCard(loanId);
  });
  el.addGoalBtn?.addEventListener("click", () => openGoalDetail(null));
  el.closeMonthOpenBtn?.addEventListener("click", openMonthCloseFlow);
  el.budgetIncomeOpenAddBtn?.addEventListener("click", () => {
    resetIncomeForm();
    setBudgetIncomeFormOpen(true);
    el.budgetIncomeTitleInput?.focus();
  });
  el.budgetIncomeBackBtn?.addEventListener("click", () => {
    setBudgetIncomeFormOpen(false);
  });
  el.budgetIncomeTypeInput?.addEventListener("change", () => {
    if (!el.budgetIncomeTitleInput) return;
    if (el.budgetIncomeTitleInput.dataset.autoTitle === "1" || !el.budgetIncomeTitleInput.value.trim()) {
      el.budgetIncomeTitleInput.value = incomeKindLabel(el.budgetIncomeTypeInput?.value || "salary");
      el.budgetIncomeTitleInput.dataset.autoTitle = "1";
    }
  });
  el.budgetIncomeTitleInput?.addEventListener("input", () => {
    if (!el.budgetIncomeTitleInput) return;
    const expected = incomeKindLabel(el.budgetIncomeTypeInput?.value || "salary");
    el.budgetIncomeTitleInput.dataset.autoTitle = el.budgetIncomeTitleInput.value.trim() === expected ? "1" : "0";
  });
  el.budgetIncomeSaveBtn?.addEventListener("click", async () => {
    try {
      const kind = el.budgetIncomeTypeInput.value || "other";
      const title = (el.budgetIncomeTitleInput.value || "").trim() || incomeKindLabel(kind);
      const amount = parseMoneyInput(el.budgetIncomeInput.value || "");
      await api("/api/miniapp/budget/incomes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          kind,
          title,
          amount_monthly: amount,
        }),
      });
      resetIncomeForm();
      toast("Доход добавлен");
      await loadBudgetDashboard();
      await loadBudgetIncomes();
      setBudgetIncomeFormOpen(false);
      setBudgetTab("income");
    } catch (e) {
      toast(e?.message || "Не удалось добавить доход");
    }
  });
  el.budgetSavingsOpenAddBtn?.addEventListener("click", () => {
    resetSavingForm();
    setBudgetSavingsFormOpen(true);
    el.budgetSavingTitleInput?.focus();
  });
  el.budgetSavingsBackBtn?.addEventListener("click", () => {
    setBudgetSavingsFormOpen(false);
  });
  el.budgetSavingTypeInput?.addEventListener("change", () => {
    if (!el.budgetSavingTitleInput) return;
    if (el.budgetSavingTitleInput.dataset.autoTitle === "1" || !el.budgetSavingTitleInput.value.trim()) {
      el.budgetSavingTitleInput.value = savingKindLabel(el.budgetSavingTypeInput?.value || "deposit");
      el.budgetSavingTitleInput.dataset.autoTitle = "1";
    }
  });
  el.budgetSavingTitleInput?.addEventListener("input", () => {
    if (!el.budgetSavingTitleInput) return;
    const expected = savingKindLabel(el.budgetSavingTypeInput?.value || "deposit");
    el.budgetSavingTitleInput.dataset.autoTitle = el.budgetSavingTitleInput.value.trim() === expected ? "1" : "0";
  });
  el.budgetSavingAddBtn?.addEventListener("click", async () => {
    try {
      const kind = el.budgetSavingTypeInput?.value || "other";
      const title = (el.budgetSavingTitleInput?.value || "").trim() || savingKindLabel(kind);
      const amount = parseMoneyInput(el.budgetSavingAmountInput?.value || "");
      await api("/api/miniapp/budget/savings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind, title, amount }),
      });
      resetSavingForm();
      toast("Накопление добавлено");
      await loadBudgetDashboard();
      await loadBudgetSavings();
      await loadBudgetHistory();
      setBudgetSavingsFormOpen(false);
      setBudgetTab("savings");
    } catch (e) {
      toast(e?.message || "Не удалось добавить накопление");
    }
  });
  el.budgetExpensesOpenAddBtn?.addEventListener("click", () => {
    clearExpenseForm();
    setBudgetExpensesFormOpen(true);
    el.budgetExpenseTitleInput?.focus();
  });
  el.budgetExpensesBackBtn?.addEventListener("click", () => {
    clearExpenseForm();
    setBudgetExpensesFormOpen(false);
  });
  el.budgetExpenseTypeInput?.addEventListener("change", () => {
    const kind = el.budgetExpenseTypeInput.value || "rent";
    if (el.budgetExpenseTitleInput && (el.budgetExpenseTitleInput.dataset.autoTitle === "1" || !el.budgetExpenseTitleInput.value.trim())) {
      el.budgetExpenseTitleInput.value = expenseKindLabel(kind);
      el.budgetExpenseTitleInput.dataset.autoTitle = "1";
    }
    showExpenseFields(kind);
    if ((kind === "mortgage" || kind === "loan") && !state.budget.expenseRateRows.length) {
      const range = getLoanRange(kind);
      state.budget.expenseRateRows = [{
        annual_rate: "",
        start_date: range.startDate || "",
        end_date: range.endDate || "",
        auto_range: true,
      }];
      renderExpenseRateRows();
    }
    syncLoanEndDateFields();
  });
  el.budgetExpenseTitleInput?.addEventListener("input", () => {
    if (!el.budgetExpenseTitleInput) return;
    const expected = expenseKindLabel(el.budgetExpenseTypeInput?.value || "rent");
    el.budgetExpenseTitleInput.dataset.autoTitle = el.budgetExpenseTitleInput.value.trim() === expected ? "1" : "0";
  });
  el.expenseMortgageStartInput?.addEventListener("change", syncLoanEndDateFields);
  el.expenseMortgageMonthsInput?.addEventListener("input", syncLoanEndDateFields);
  el.expenseLoanStartInput?.addEventListener("change", syncLoanEndDateFields);
  el.expenseLoanMonthsInput?.addEventListener("input", syncLoanEndDateFields);
  el.expenseAddRateBtn?.addEventListener("click", () => {
    const kind = el.budgetExpenseTypeInput?.value || "rent";
    const range = getLoanRange(kind);
    let start = "";
    let end = range.endDate || "";
    if (state.budget.expenseRateRows.length) {
      const sorted = state.budget.expenseRateRows
        .map((x) => ({ ...x }))
        .filter((x) => String(x.end_date || "").trim())
        .sort((a, b) => String(a.end_date).localeCompare(String(b.end_date)));
      const last = sorted[sorted.length - 1];
      if (last?.end_date) {
        start = addDaysYmd(String(last.end_date), 1);
      }
    }
    if (!start) start = range.startDate || "";
    addExpenseRateRow({ annual_rate: "", start_date: start, end_date: end, auto_range: false });
  });
  el.budgetExpensesCancelEditBtn?.addEventListener("click", () => clearExpenseForm());
  el.budgetResetBtn?.addEventListener("click", async () => {
    if (!window.confirm("Удалить текущий бюджет? Это полностью удалит все данные бюджета, включая цели.")) return;
    try {
      const result = await api("/api/miniapp/budget/reset", { method: "POST" });
      state.budget.goalEditingId = null;
      state.budget.goalChecklistItems = [];
      if (el.goalDetailCard) el.goalDetailCard.style.display = "none";
      toast(`Бюджет очищен (доходов: ${result.incomes}, целей: ${result.funds})`);
      await loadBudgetDashboard();
      setBudgetTab("overview");
    } catch (e) {
      toast(e?.message || "Не удалось удалить бюджет");
    }
  });
  el.budgetNotifSaveBtn?.addEventListener("click", async () => {
    try {
      await api("/api/miniapp/budget/settings/notifications", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          budget_summary_enabled: !!el.budgetNotifSummaryToggle?.checked,
          goal_deadline_enabled: !!el.budgetNotifGoalsToggle?.checked,
          month_close_enabled: !!el.budgetNotifMonthCloseToggle?.checked,
        }),
      });
      toast("Настройки уведомлений сохранены");
    } catch (e) {
      toast(e?.message || "Не удалось сохранить настройки");
    }
  });
  el.budgetExpensesSaveBtn?.addEventListener("click", async () => {
    try {
      const payload = buildExpensePayloadFromForm();
      const path = state.budget.expenseEditingId
        ? `/api/miniapp/budget/expenses/${state.budget.expenseEditingId}`
        : "/api/miniapp/budget/expenses";
      const body = state.budget.expenseEditingId
        ? { action: "edit", ...payload }
        : payload;
      const result = await api(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (payload.kind === "mortgage" || payload.kind === "loan") {
        const calc = result?.details?.calculation || {};
        el.budgetExpensesCalcResult.textContent = [
          calc.monthly_payment !== null && calc.monthly_payment !== undefined ? `Ежемесячный платёж: ${money(calc.monthly_payment)}` : `Первый платёж: ${money(calc.first_payment)}`,
          calc.monthly_payment === null ? `Последний платёж: ${money(calc.last_payment)}` : "",
          `Переплата: ${money(calc.overpayment)}`,
          `Итоговая выплата: ${money(calc.total_payment)}`,
        ].filter(Boolean).join("\n");
      } else {
        el.budgetExpensesCalcResult.textContent = "";
      }
      toast(state.budget.expenseEditingId ? "Расход обновлён" : "Расход добавлен");
      clearExpenseForm();
      await loadBudgetDashboard();
      await loadBudgetExpenses();
      setBudgetExpensesFormOpen(false);
      setBudgetTab("expenses");
    } catch (e) {
      toast(e?.message || "Не удалось сохранить расход");
    }
  });
  el.goalTopupBtn?.addEventListener("click", async () => {
    try {
      await topupGoal();
    } catch (e) {
      toast(e?.message || "Не удалось добавить сумму");
    }
  });
  el.goalSaveBtn?.addEventListener("click", async () => {
    try {
      await saveGoalDetail();
    } catch (e) {
      toast(e?.message || "Не удалось сохранить цель");
    }
  });
  el.goalDeleteBtn?.addEventListener("click", async () => {
    if (!state.budget.goalEditingId) return;
    if (!window.confirm("Удалить эту цель?")) return;
    try {
      await deleteGoal();
    } catch (e) {
      toast(e?.message || "Не удалось удалить цель");
    }
  });
  el.goalCancelBtn?.addEventListener("click", () => {
    el.goalDetailCard.style.display = "none";
  });
  el.goalChecklistTemplateSelect?.addEventListener("change", () => {
    const value = (el.goalChecklistTemplateSelect.value || "").trim();
    if (!value) return;
    el.goalChecklistInput.value = value;
  });
  el.goalChecklistAddBtn?.addEventListener("click", () => {
    const text = (el.goalChecklistInput.value || "").trim();
    if (!text) {
      toast("Введите чекбокс цели");
      return;
    }
    state.budget.goalChecklistItems.push({ text, done: false });
    el.goalChecklistInput.value = "";
    el.goalChecklistTemplateSelect.value = "";
    renderGoalChecklist();
  });
  el.monthCloseSubmitBtn?.addEventListener("click", submitMonthCloseFromForm);
  el.monthCloseCancelBtn?.addEventListener("click", () => {
    el.monthCloseForm.style.display = "none";
  });
  clearExpenseForm();
  resetIncomeForm();
  resetSavingForm();
  resetLoanCreateForm();

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
