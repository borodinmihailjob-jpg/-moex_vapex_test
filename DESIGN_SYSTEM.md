# DESIGN SYSTEM — Telegram Mini App (Apple Clean × Spotify Layers)

## 1. Экранная карта

### Роутинг/вкладки Mini App
- `mode`: выбор пространства (Биржа / Бюджет)
- `dashboard`: портфель
- `trade`: добавление сделки
- `lookup`: поиск цены и динамики
- `movers`: топ роста/падения
- `alerts`: создание и управление алертами
- `more`: USD/RUB, XML импорт, статьи, очистка портфеля
- `budget` (внутренние вкладки):
  - `overview`
  - `income`
  - `expenses`
  - `funds`
  - `savings`
  - `close`
  - `settings`

## 2. Токены

Источник: `miniapp/tokens.css`

### Цвета (семантика)
- `--bg`
- `--surface`
- `--surface-elevated`
- `--text-primary`
- `--text-secondary`
- `--separator`
- `--accent`
- `--accent-text`
- `--success`
- `--warning`
- `--danger`

### Радиусы
- `--r8`, `--r12`, `--r16`, `--r20`, `--r24`

### Отступы
- `--s4`, `--s8`, `--s12`, `--s16`, `--s20`, `--s24`, `--s32`

### Типографика
- `--title-size`
- `--headline-size`
- `--body-size`
- `--subhead-size`
- `--caption-size`
- `--lh-tight`, `--lh-base`

### Тени/уровни
- `--shadow-1`
- `--shadow-2`

### Motion
- `--motion-fast` (150ms)
- `--motion-base` (200ms)
- `--motion-ease`

## 3. Theme Bridge (Telegram)

Источник: `miniapp/app.js` (`applyTelegramTheme`)

- Базовые цвета берутся из Telegram `themeParams`:
  - `bg_color`
  - `text_color`
  - `hint_color`
  - `button_color`
  - `button_text_color`
- Семантические слои (`surface`, `surface-elevated`) строятся из `bg_color` с мягким осветлением 6–10%.
- Обновление темы в runtime: `themeChanged`.

## 4. UI-компоненты

### Визуальные компоненты (CSS)
- `App Shell`: `.app-shell`, `.app-header`, `.content`
- `Card`: `.card`, `.mode-card`, `.card-inline`
- `Button`: `.btn.primary`, `.btn.ghost`, `.btn.tertiary`, `.btn.danger`
- `List Row`: `.list .item`, `.left`, `.right`, `.name`, `.sub`, `.pnl.plus/.minus`
- `Section controls`: `.form-grid`, `.chips`, `.chip`, `.switch`, `.tabbar`, `.tab-btn`
- `Feedback`: `.toast`, `.global-loader`
- `Loading`: `.skeleton-list`, `.skeleton-item`, `.skeleton-line`
- `State`: `.state`, `.state-title`, `.state-text`

### JS-компоненты (helpers)
Источник: `miniapp/app.js`
- `createListRow(...)`
- `renderEmpty(container, text)`
- `renderError(container, text)`
- `renderSkeletonList(container, rows)`

## 5. Принципы

- Все цвета и размеры идут через токены.
- Минимум эффектов: 2 уровня тени, без тяжёлых blur/3D.
- Контроль контраста и tap-target >= 44px.
- Для денег и процентов используется `font-variant-numeric: tabular-nums`.

## 6. Где менять дальше

- Токены: `miniapp/tokens.css`
- Общий стиль: `miniapp/styles.css`
- Theme bridge и компонентные helper-ы: `miniapp/app.js`
- Разметка экранов: `miniapp/index.html`
