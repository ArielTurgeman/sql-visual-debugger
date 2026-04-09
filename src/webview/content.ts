/**
 * Returns the full HTML for the SQL Debugger webview panel.
 *
 * What changed from the monolith:
 *  - Removed: textarea editor, .editor-wrap overlay, Run Debugger button
 *  - Removed: all textarea-related JS (scroll sync, input reset, highlightClauseInEditor)
 *  - Added:   #queryMeta (source file + connection info) and #sqlDisplay (read-only SQL)
 *  - Added:   highlightClause() that operates on the stored currentSql string
 *  - All debug panels (flow, step, join insight, result table) are unchanged
 */
export function getWebviewContent(): string {
    return `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8" />
<style>
body { font-family: Inter, Arial, sans-serif; padding: 18px; color: #e8e8e8; background: #111827; }
h1 { font-size: 22px; margin: 0 0 6px 0; }
p.sub { margin: 0 0 18px 0; color: #9ca3af; }

/* ── Read-only SQL display ── */
.sql-display-wrap {
  background: #020617; border: 1px solid #334155; border-radius: 12px;
  padding: 12px; margin: 10px 0; overflow: auto; max-height: 200px;
}
#sqlDisplay {
  margin: 0; font-family: Consolas, monospace; font-size: 13px; line-height: 1.6;
  color: white; white-space: pre-wrap; word-wrap: break-word;
}
#queryMeta { font-size: 11px; color: #64748b; margin-bottom: 6px; }

button {
  background: #2563eb; color: white; border: none; border-radius: 10px; padding: 10px 14px;
  cursor: pointer; font-weight: 600; transition: transform .15s ease, opacity .2s ease, background .2s ease;
}
button:hover { background: #1d4ed8; transform: translateY(-1px); }
button:disabled { opacity: .45; cursor: not-allowed; transform: none; }
.row { display: flex; gap: 10px; align-items: center; margin-top: 12px; flex-wrap: wrap; }
.card {
  background: rgba(17, 24, 39, 0.9); border: 1px solid #374151; border-radius: 16px;
  padding: 14px; margin-top: 16px; box-shadow: 0 10px 25px rgba(0,0,0,0.18);
}
.flow { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.stage {
  display:flex; align-items:center; gap:8px; padding: 8px 12px; border-radius: 999px;
  border: 1px solid #374151; color: #94a3b8; background: #111827; transition: all .22s ease;
}
.stage.completed { background: rgba(5, 150, 105, .16); color: #a7f3d0; border-color: rgba(16, 185, 129, .45); }
.stage.active { background: rgba(37, 99, 235, .2); color: #dbeafe; border-color: #60a5fa; transform: translateY(-1px); box-shadow: 0 0 0 1px rgba(96,165,250,.15) inset; }
.stage.upcoming { opacity: .6; }
.stage.clickable { cursor: pointer; }
.stage-badge { font-size: 11px; line-height: 1; padding: 4px 6px; border-radius: 999px; background: rgba(255,255,255,.07); color:#cbd5e1; border:1px solid rgba(255,255,255,.08); }
.arrow { color: #64748b; }
.meta-grid { display: grid; grid-template-columns: repeat(3, minmax(120px, 1fr)); gap: 10px; }
.metric { background: #0f172a; border: 1px solid #334155; border-radius: 12px; padding: 10px; transition: all .18s ease; }
.metric-label { font-size: 12px; color: #94a3b8; margin-bottom: 6px; }
.metric-value { font-size: 18px; font-weight: 700; }
.explain { line-height: 1.55; color: #d1d5db; transition: opacity .18s ease; }
.query-viewer {
  background: #020617; border: 1px solid #334155; border-radius: 12px; padding: 12px;
  overflow: auto; white-space: pre-wrap; font-family: Consolas, monospace; font-size: 13px;
  transition: opacity .18s ease, transform .18s ease;
}
.sql-highlight {
  background: rgba(250, 204, 21, .22); color: #fef08a;
  border-radius: 3px; outline: 1px solid rgba(250, 204, 21, .45);
}
.table-wrap {
  border: 1px solid #334155; border-radius: 12px; overflow: auto; max-height: 380px;
  transition: opacity .18s ease, transform .18s ease;
}
table { border-collapse: collapse; width: max-content; min-width: 100%; background: #0b1220; }
th, td { padding: 9px 10px; text-align: left; border-bottom: 1px solid #1f2937; white-space: nowrap; }
th { position: sticky; top: 0; background: #172033; color: #e5e7eb; z-index: 1; }
th.joined-col { background: #1d2943; box-shadow: inset 0 -2px 0 rgba(96,165,250,.45); }
td.joined-col { background: rgba(59,130,246,.06); }
.col-badge { margin-left:6px; font-size:10px; padding:2px 5px; border-radius:999px; border:1px solid rgba(96,165,250,.35); color:#bfdbfe; background:rgba(59,130,246,.12); vertical-align:middle; }
tr:nth-child(even) td { background: rgba(255,255,255,.01); }
.status { color: #93c5fd; min-height: 22px; }
.error { color: #fca5a5; background: rgba(127,29,29,.18); border: 1px solid rgba(248,113,113,.28); border-radius: 12px; padding: 12px; white-space: pre-wrap; }
.small { color: #94a3b8; font-size: 12px; }
.join-section { display:none; width:100%; }
/* ── Compact insight bar ── */
.join-insight-bar { display:flex; flex-wrap:wrap; gap:6px; align-items:center; margin-bottom:10px; }
.j-badge { display:inline-flex; align-items:center; gap:4px; padding:3px 9px; border-radius:999px; font-size:12px; font-weight:600; white-space:nowrap; line-height:1.5; }
.j-badge.type  { background:rgba(59,130,246,.15);  color:#93c5fd;  border:1px solid rgba(96,165,250,.3); }
.j-badge.cond  { background:rgba(250,204,21,.08);  color:#fde68a;  border:1px solid rgba(250,204,21,.22); font-family:Consolas,monospace; font-size:11px; }
.j-badge.count { background:rgba(15,23,42,.9);     color:#94a3b8;  border:1px solid #334155; }
.j-badge.rel   { background:rgba(16,185,129,.08);  color:#6ee7b7;  border:1px solid rgba(16,185,129,.22); }
.j-badge.impact-pos     { background:rgba(16,185,129,.08); color:#6ee7b7; border:1px solid rgba(16,185,129,.22); }
.j-badge.impact-neg     { background:rgba(239,68,68,.08);  color:#fca5a5; border:1px solid rgba(239,68,68,.2); }
.j-badge.impact-neutral { background:rgba(15,23,42,.9);    color:#94a3b8; border:1px solid #334155; }
/* ── Preview card ── */
.join-card { background:#0f172a; border:1px solid #334155; border-radius:14px; padding:10px 12px; width:100%; box-sizing:border-box; }
.join-title { font-size:12px; color:#94a3b8; margin-bottom:8px; }
.preview-header { display:flex; align-items:center; gap:8px; margin-bottom:10px; flex-wrap:wrap; }
.preview-header-left { display:flex; align-items:center; gap:8px; flex:1; flex-wrap:wrap; min-width:0; }
.selection-chip { font-size:12px; padding:3px 9px; border-radius:999px; background:rgba(250,204,21,.12); color:#fde68a; border:1px solid rgba(250,204,21,.28); }
.toggle-cols-btn { background:none; border:1px solid #334155; border-radius:6px; color:#64748b; font-size:11px; padding:2px 8px; cursor:pointer; font-weight:normal; white-space:nowrap; flex-shrink:0; }
.toggle-cols-btn:hover { background:rgba(255,255,255,.05); color:#94a3b8; transform:none; }
.preview-layout { display:grid; grid-template-columns: minmax(0,1fr) auto minmax(0,1fr); gap:16px; align-items:start; width:100%; }
.mini-card { background:#0b1220; border:1px solid #334155; border-radius:12px; padding:10px; display:flex; flex-direction:column; justify-content:flex-start; }
.mini-name { font-size:12px; color:#cbd5e1; margin-bottom:8px; display:flex; justify-content:space-between; gap:8px; }
.mini-table-wrap { width:100%; overflow:auto; }
.mini-table { width:100%; border-collapse:collapse; table-layout:fixed; }
.mini-table th, .mini-table td { padding:5px 8px; font-size:12px; border-bottom:1px solid #1f2937; white-space:nowrap; vertical-align:middle; overflow:hidden; text-overflow:ellipsis; }
.mini-table th { position:static; background:#111827; }
/* Join column emphasis: left border stripe instead of full background */
.mini-table th.join-col { background:#111827; border-left:2px solid rgba(250,204,21,.55); color:#fde68a; }
.mini-table td.join-col { background:rgba(250,204,21,.07); border-left:2px solid rgba(250,204,21,.35); color:#fde68a; cursor:pointer; }
.mini-table td.join-col:hover { background:rgba(250,204,21,.15); }
.mini-table td.join-col.active { background:rgba(250,204,21,.28); }
.mini-table .mini-row-selected td { background:rgba(59,130,246,.18); }
.mini-table .mini-row-match td { background:rgba(16,185,129,.12); }
.preview-bridge { display:flex; flex-direction:column; align-items:center; justify-content:flex-start; gap:6px; color:#94a3b8; padding-top:32px; }
.preview-bridge .bridge-label { font-size:10px; color:#475569; text-transform:uppercase; letter-spacing:.08em; }
.preview-bridge .bridge-pill { font-size:11px; padding:5px 10px; border-radius:999px; border:1px solid rgba(96,165,250,.3); background:rgba(59,130,246,.1); color:#dbeafe; text-align:center; max-width:160px; line-height:1.35; font-family:Consolas,monospace; }
.helper-text { margin-top:6px; font-size:11px; color:#64748b; }
@media (max-width: 900px) {
  .preview-layout { grid-template-columns: 1fr; gap:10px; }
  .preview-bridge { padding-top:0; flex-direction:row; justify-content:center; }
}
.fade-in { animation: fadeIn .22s ease; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(3px); } to { opacity: 1; transform: translateY(0); } }
.info-btn {
  background: none; color: #94a3b8; border: none; border-radius: 6px;
  padding: 2px 5px; font-size: 15px; line-height: 1; font-weight: normal;
  cursor: pointer; opacity: 0.7; transition: opacity .15s ease;
}
.info-btn:hover { background: rgba(255,255,255,.07); transform: none; color: #94a3b8; opacity: 1; }
.info-popup {
  position: absolute; top: calc(100% + 6px); right: 0;
  width: 260px; background: #1e293b; border: 1px solid #334155;
  border-radius: 12px; padding: 14px 16px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.45);
  z-index: 50; color: #cbd5e1; font-size: 12px; line-height: 1.55;
}
.info-popup-title { font-weight: 700; margin-bottom: 8px; color: #f1f5f9; font-size: 13px; }
.info-popup-section { font-weight: 600; margin: 10px 0 5px 0; color: #f1f5f9; }
.info-popup ul { margin: 0; padding-left: 16px; }
.info-popup li { margin-bottom: 3px; }
.info-popup p { margin: 0; }
</style>
</head>
<body>
  <h1>SQL Debugger</h1>
  <p class="sub">Step through your SQL and see exactly how each clause changes the data.</p>

  <div style="position:fixed;top:18px;right:18px;z-index:100;">
    <button id="infoBtn" class="info-btn" title="What is this?">ℹ️</button>
    <div id="infoPopup" class="info-popup" style="display:none;">
      <div class="info-popup-title">What is this?</div>
      <p>This tool steps through your SQL query and shows how each clause changes the data in real time.</p>
      <div class="info-popup-section">Why it's useful:</div>
      <ul>
        <li>Understand JOIN behavior visually</li>
        <li>See exactly which rows a WHERE filter removes</li>
        <li>Debug row count surprises quickly</li>
      </ul>
      <div class="info-popup-section">How to use:</div>
      <p>Write a query in your <strong>.sql</strong> file, then right-click → <strong>Debug SQL Query</strong> (or use the Command Palette).</p>
    </div>
  </div>

  <!-- ── Query header card ─────────────────────────────────────────────── -->
  <div class="card">
    <div class="small" id="queryMeta">Loading query…</div>
    <div class="sql-display-wrap">
      <pre id="sqlDisplay"></pre>
    </div>
    <div class="row">
      <button id="prevBtn" disabled>← Previous</button>
      <button id="nextBtn" disabled>Next →</button>
      <div class="status" id="status">Connecting to MySQL and running query…</div>
    </div>
  </div>

  <div id="errorBox" class="error" style="display:none;margin-top:12px;"></div>

  <!-- ── Execution flow ────────────────────────────────────────────────── -->
  <div class="card" id="flowCard" style="display:none;">
    <div class="small" style="margin-bottom:8px;">Execution flow</div>
    <div class="flow" id="flow"></div>
  </div>

  <!-- ── Step explanation ──────────────────────────────────────────────── -->
  <div class="card fade-in" id="stepCard" style="display:none;">
    <div class="small" style="margin-bottom:10px;">Step explanation</div>
    <div class="meta-grid" id="metrics"></div>
    <div class="explain" id="explanation" style="margin-top:12px;"></div>
    <div class="query-viewer" id="sqlFragment" style="margin-top:12px;"></div>
  </div>

  <!-- ── JOIN insight ───────────────────────────────────────────────────── -->
  <div class="card fade-in join-section" id="joinSection">
    <div class="small" style="margin-bottom:10px;">JOIN insight + preview</div>
    <div id="joinSectionHost"></div>
  </div>

  <!-- ── Intermediate result table ─────────────────────────────────────── -->
  <div class="card fade-in" id="resultCard" style="display:none;">
    <div class="small" style="margin-bottom:8px;">Intermediate result</div>
    <div class="small" id="resultCaption" style="margin-bottom:8px;"></div>
    <div class="table-wrap" id="tableWrap">
      <div id="tableHost"></div>
    </div>
  </div>

<script>
const vscode = acquireVsCodeApi();

// ── State ──────────────────────────────────────────────────────────────────
let steps = [];
let currentIndex = 0;
let joinPreviewSelection = null;
let joinPreviewExpanded = false;  // toggled by "Show all columns" button
let currentSql = '';              // set when debuggerResult arrives

// ── Utilities ──────────────────────────────────────────────────────────────
function escapeHtml(value) {
  return String(value === null || value === undefined ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// ── SQL highlighting ────────────────────────────────────────────────────────
// Operates on the stored currentSql string and updates the read-only #sqlDisplay element.
function highlightClause(clauseName) {
  var display = document.getElementById('sqlDisplay');
  if (!currentSql) { display.innerHTML = ''; return; }

  var patterns = {
    'FROM':     /(\bFROM\b[\s\S]*?)(?=\bJOIN\b|\bWHERE\b|\bORDER\s+BY\b|\bLIMIT\b|$)/i,
    'JOIN':     /(\bJOIN\b[\s\S]*?)(?=\bWHERE\b|\bORDER\s+BY\b|\bLIMIT\b|$)/i,
    'WHERE':    /(\bWHERE\b[\s\S]*?)(?=\bORDER\s+BY\b|\bLIMIT\b|$)/i,
    'SELECT':   /(\bSELECT\b[\s\S]*?)(?=\bFROM\b|$)/i,
    'ORDER BY': /(\bORDER\s+BY\b[\s\S]*?)(?=\bLIMIT\b|$)/i,
    'LIMIT':    /(\bLIMIT\b[\s\S]*?$)/i
  };

  var pattern = patterns[clauseName];
  if (!pattern) { display.innerHTML = escapeHtml(currentSql); return; }

  var match = currentSql.match(pattern);
  if (!match) { display.innerHTML = escapeHtml(currentSql); return; }

  display.innerHTML =
    escapeHtml(currentSql.slice(0, match.index)) +
    '<span class="sql-highlight">' + escapeHtml(match[0]) + '</span>' +
    escapeHtml(currentSql.slice(match.index + match[0].length));
}

function clearHighlight() {
  var display = document.getElementById('sqlDisplay');
  display.innerHTML = currentSql ? escapeHtml(currentSql) : '';
}

// ── Execution flow ──────────────────────────────────────────────────────────
function renderFlow() {
  const host = document.getElementById('flow');
  let html = '';
  const joinCount = steps.filter(function(s) { return s.name === 'JOIN'; }).length;
  let joinIdx = 0;
  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    let label = step.name;
    if (step.name === 'JOIN') {
      joinIdx++;
      label = joinCount > 1 ? 'JOIN ' + joinIdx : 'JOIN';
    }
    let cls = 'stage upcoming';
    if (i < currentIndex) { cls = 'stage completed'; }
    if (i === currentIndex) { cls = 'stage active'; }
    cls += ' clickable';
    html += '<div class="' + cls + '" data-step-index="' + i + '">' + escapeHtml(label);
    if (step.summaryBadge) {
      html += '<span class="stage-badge">' + escapeHtml(step.summaryBadge) + '</span>';
    }
    html += '</div>';
    if (i < steps.length - 1) { html += '<div class="arrow">→</div>'; }
  }
  host.innerHTML = html;
  host.querySelectorAll('.stage.clickable').forEach(function(el) {
    el.addEventListener('click', function() {
      const idx = parseInt(el.getAttribute('data-step-index'), 10);
      if (isNaN(idx) || idx < 0 || idx >= steps.length) { return; }
      if (idx === currentIndex) { return; }
      currentIndex = idx;
      if (steps[currentIndex].name !== 'JOIN') { joinPreviewSelection = null; joinPreviewExpanded = false; }
      renderStep();
    });
  });
}

// ── Metrics ─────────────────────────────────────────────────────────────────
function renderMetrics(step) {
  const host = document.getElementById('metrics');
  let html =
    '<div class="metric"><div class="metric-label">Rows before</div><div class="metric-value">' + step.rowsBefore + '</div></div>' +
    '<div class="metric"><div class="metric-label">Rows after</div><div class="metric-value">' + step.rowsAfter + '</div></div>';

  if (step.name === 'WHERE') {
    const removed = typeof step.rowsRemoved === 'number' ? step.rowsRemoved : Math.max(0, step.rowsBefore - step.rowsAfter);
    html += '<div class="metric"><div class="metric-label">Rows removed</div><div class="metric-value">' + removed + '</div></div>';
  }

  const stepLabel = (step.name === 'JOIN' && step.joinType) ? step.joinType : step.name;
  html += '<div class="metric"><div class="metric-label">Current step</div><div class="metric-value" style="font-size:16px;">' + escapeHtml(stepLabel) + '</div></div>';
  host.innerHTML = html;
}

// ── Main result table ────────────────────────────────────────────────────────
function renderTable(rows) {
  const host = document.getElementById('tableHost');
  const step = steps[currentIndex];
  const joinedColumns = step && step.name === 'JOIN' && Array.isArray(step.joinedColumns) ? step.joinedColumns : [];

  if (!Array.isArray(rows) || rows.length === 0) {
    host.innerHTML = '<div style="padding:14px;" class="small">No rows to show at this step.</div>';
    return;
  }

  const cols = Object.keys(rows[0]);
  let html = '<table><thead><tr>';
  for (const c of cols) {
    const joined = joinedColumns.includes(String(c).toLowerCase());
    html += '<th' + (joined ? ' class="joined-col"' : '') + '>' + escapeHtml(c);
    if (joined) { html += '<span class="col-badge">joined</span>'; }
    html += '</th>';
  }
  html += '</tr></thead><tbody>';
  for (const row of rows) {
    html += '<tr>';
    for (const c of cols) {
      const joined = joinedColumns.includes(String(c).toLowerCase());
      const value = row[c];
      html += '<td' + (joined ? ' class="joined-col"' : '') + '>' +
        escapeHtml(value === null || value === undefined ? 'NULL' : value) + '</td>';
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  host.innerHTML = html;
}

// ── JOIN mini preview table ──────────────────────────────────────────────────

// Returns at most 3 columns: one before the join field, the join field itself,
// and one after. If the join field is the first column the two slots after it
// are used so the table always has context around the key.
function getPreviewColumns(columns, joinField) {
  const idx = columns.indexOf(joinField);
  if (idx === -1) { return columns.slice(0, 3); }          // join field not found — best effort
  if (idx === 0)  { return columns.slice(0, Math.min(3, columns.length)); }  // at start: key + 2 after
  const result = [columns[idx - 1], columns[idx]];         // 1 before + key
  if (idx + 1 < columns.length) { result.push(columns[idx + 1]); }          // 1 after (if exists)
  return result;
}

function normalizeJoinValue(value) {
  return String(value === null || value === undefined ? '' : value);
}

function formatPreviewCell(value) {
  const raw = value === null || value === undefined ? 'NULL' : String(value);
  return raw.length > 10 ? raw.slice(0, 10) + '\u2026' : raw;
}

function renderMiniPreviewTable(rows, columns, joinField, side, selectedValue, matchInfo) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="small">No preview rows available.</div>';
  }

  let html = '<div class="mini-table-wrap"><table class="mini-table"><thead><tr>';
  for (const col of columns) {
    const cls = col === joinField ? ' class="join-col"' : '';
    const headerLabel = col.split('.').pop() || col;
    html += '<th' + cls + ' title="' + escapeHtml(headerLabel) + '">' + escapeHtml(headerLabel) + '</th>';
  }
  html += '</tr></thead><tbody>';

  const selectedSide = matchInfo && matchInfo.selectedSide ? matchInfo.selectedSide : null;

  for (const row of rows) {
    const rowJoinValue = normalizeJoinValue(row[joinField]);
    let rowClass = '';

    if (selectedValue && rowJoinValue === selectedValue && selectedSide) {
      if (selectedSide === 'left') {
        rowClass = side === 'left' ? 'mini-row-match' : 'mini-row-selected';
      } else if (selectedSide === 'right') {
        rowClass = side === 'right' ? 'mini-row-selected' : 'mini-row-match';
      }
    }

    html += '<tr' + (rowClass ? ' class="' + rowClass + '"' : '') + '>';
    for (const col of columns) {
      const value = row[col];
      const displayValue = formatPreviewCell(value);
      const titleValue = value === null || value === undefined ? 'NULL' : String(value);
      if (col === joinField) {
        const active = selectedValue && rowJoinValue === selectedValue ? ' active' : '';
        html += '<td class="join-col' + active + '" title="' + escapeHtml(titleValue) +
          '" data-side="' + escapeHtml(side) + '" data-value="' + escapeHtml(rowJoinValue) + '">' +
          escapeHtml(displayValue) + '</td>';
      } else {
        html += '<td title="' + escapeHtml(titleValue) + '">' + escapeHtml(displayValue) + '</td>';
      }
    }
    html += '</tr>';
  }

  html += '</tbody></table></div>';
  return html;
}

// ── JOIN insight section ─────────────────────────────────────────────────────
function renderJoinSection(step) {
  const wrap = document.getElementById('joinSection');
  const host = document.getElementById('joinSectionHost');
  if (!step || step.name !== 'JOIN' || !step.joinMeta) {
    wrap.style.display = 'none';
    host.innerHTML = '';
    joinPreviewSelection = null;
    return;
  }

  wrap.style.display = 'block';
  const meta = step.joinMeta;
  const selectedValue = joinPreviewSelection ? joinPreviewSelection.value : null;
  const selectedSide  = joinPreviewSelection ? joinPreviewSelection.side  : null;
  const matchInfo = { selectedSide };

  const rightTotalMatches = selectedValue ? (meta.rightMatchCounts[selectedValue] || 0) : 0;
  const leftTotalMatches  = selectedValue ? (meta.leftMatchCounts[selectedValue]  || 0) : 0;
  const rightShown = selectedValue ? meta.previewRightRows.filter(r => normalizeJoinValue(r[meta.rightJoinField]) === selectedValue).length : 0;
  const leftShown  = selectedValue ? meta.previewLeftRows.filter(r  => normalizeJoinValue(r[meta.leftJoinField])  === selectedValue).length : 0;
  const truncatedRight = Math.max(0, rightTotalMatches - rightShown);
  const truncatedLeft  = Math.max(0, leftTotalMatches  - leftShown);

  // ── Compact insight bar ──────────────────────────────────────────────────
  const deltaStr = (meta.rowDelta >= 0 ? '+' : '') + meta.rowDelta;
  const impactCls = meta.rowDelta > 0 ? 'impact-pos' : meta.rowDelta < 0 ? 'impact-neg' : 'impact-neutral';
  const jType = step.joinType ? step.joinType.toUpperCase() : 'JOIN';

  let html = '<div class="join-insight-bar">';
  html += '<span class="j-badge type">' + escapeHtml(step.joinType || 'JOIN') + '</span>';
  html += '<span class="j-badge cond">' + escapeHtml(meta.leftTable + '.' + meta.leftJoinField) + ' \u21c4 ' + escapeHtml(meta.rightTable + '.' + meta.rightJoinField) + '</span>';
  html += '<span class="j-badge count" title="' + escapeHtml(meta.leftTable) + ' rows \u00d7 ' + escapeHtml(meta.rightTable) + ' rows">' + meta.leftRows + ' \u00d7 ' + meta.rightRows + '</span>';
  html += '<span class="j-badge rel" title="' + escapeHtml(meta.relationshipNote) + '">' + escapeHtml(meta.relationship) + '</span>';
  html += '<span class="j-badge ' + impactCls + '" title="Result: ' + meta.resultRows + ' rows \u00b7 Growth: ' + escapeHtml(meta.growthText) + '">' + deltaStr + ' rows</span>';
  html += '</div>';

  // ── Preview card ─────────────────────────────────────────────────────────
  const leftCols  = joinPreviewExpanded ? meta.previewLeftColumns  : getPreviewColumns(meta.previewLeftColumns,  meta.leftJoinField);
  const rightCols = joinPreviewExpanded ? meta.previewRightColumns : getPreviewColumns(meta.previewRightColumns, meta.rightJoinField);
  const toggleLabel = joinPreviewExpanded ? 'Focused view' : 'All columns';

  html += '<div class="join-card">';
  html += '<div class="preview-header">';
  html += '<div class="preview-header-left">';
  if (selectedValue) {
    html += '<span class="selection-chip">' + escapeHtml(meta.leftJoinField) + ' = ' + escapeHtml(selectedValue) + '</span>';
  } else {
    html += '<span class="small">Click a \u{1F7E1} cell to trace matches</span>';
  }
  html += '</div>';
  html += '<button class="toggle-cols-btn" id="joinToggleBtn">' + toggleLabel + '</button>';
  html += '</div>';

  html += '<div class="preview-layout">';

  // Left mini table
  html += '<div class="mini-card"><div class="mini-name"><span>' + escapeHtml(meta.leftTable) + '</span><span class="small">' + meta.leftRows + ' rows</span></div>';
  html += renderMiniPreviewTable(meta.previewLeftRows, leftCols, meta.leftJoinField, 'left', selectedValue, matchInfo);
  if (selectedValue && truncatedLeft > 0) {
    html += '<div class="helper-text">+' + truncatedLeft + ' more match(es) not shown</div>';
  }
  html += '</div>';

  html += '<div class="preview-bridge"><div class="bridge-label">on</div><div class="bridge-pill">' + escapeHtml(meta.conditionLabel) + '</div></div>';

  // Right mini table
  html += '<div class="mini-card"><div class="mini-name"><span>' + escapeHtml(meta.rightTable) + '</span><span class="small">' + meta.rightRows + ' rows</span></div>';
  html += renderMiniPreviewTable(meta.previewRightRows, rightCols, meta.rightJoinField, 'right', selectedValue, matchInfo);
  if (selectedValue && truncatedRight > 0) {
    html += '<div class="helper-text">+' + truncatedRight + ' more match(es) not shown</div>';
  }
  html += '</div>';

  html += '</div></div>';
  host.innerHTML = html;

  host.querySelectorAll('td.join-col').forEach(function(td) {
    td.addEventListener('click', function(event) {
      const target = event.currentTarget;
      if (!target) { return; }
      const side  = target.getAttribute('data-side');
      const value = target.getAttribute('data-value');
      if (!side || value === null) { return; }
      if (joinPreviewSelection && joinPreviewSelection.side === side && joinPreviewSelection.value === value) {
        joinPreviewSelection = null;
      } else {
        joinPreviewSelection = { side, value };
      }
      renderStep();
    });
  });

  const toggleBtn = host.querySelector('#joinToggleBtn');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', function() {
      joinPreviewExpanded = !joinPreviewExpanded;
      renderStep();
    });
  }
}

// ── Step renderer ────────────────────────────────────────────────────────────
function renderStep() {
  if (!steps.length) { return; }
  const step = steps[currentIndex];
  renderFlow();
  renderMetrics(step);
  highlightClause(step.name);

  let impactText = 'Rows changed from ' + step.rowsBefore + ' to ' + step.rowsAfter + '.';
  if (step.name === 'SELECT') {
    impactText = 'Row count stayed at ' + step.rowsAfter + ', but the visible columns were reduced to the ones requested in SELECT.';
  } else if (step.name === 'ORDER BY') {
    impactText = 'Row count stayed at ' + step.rowsAfter + '. Only the order changed.';
  } else if (step.name === 'WHERE') {
    const removed = typeof step.rowsRemoved === 'number' ? step.rowsRemoved : Math.max(0, step.rowsBefore - step.rowsAfter);
    const removedPct = step.rowsBefore > 0 ? Math.round((removed / step.rowsBefore) * 100) : 0;
    impactText = 'Rows reduced from ' + step.rowsBefore + ' to ' + step.rowsAfter + '. ' +
      removed + ' rows removed (' + removedPct + '%).';
  } else if (step.name === 'JOIN') {
    const delta = step.rowsAfter - step.rowsBefore;
    const deltaStr = (delta >= 0 ? '+' : '') + delta;
    const jType = (step.joinType || 'JOIN').toUpperCase();
    if (jType.startsWith('LEFT')) {
      impactText = 'All ' + step.rowsBefore + ' left-table rows were preserved. Result has ' + step.rowsAfter + ' rows (' + deltaStr + ').';
    } else if (jType.startsWith('RIGHT')) {
      impactText = 'All right-table rows were preserved. Result has ' + step.rowsAfter + ' rows (' + deltaStr + ').';
    } else if (jType.startsWith('FULL')) {
      impactText = 'Both tables combined. Result has ' + step.rowsAfter + ' rows (' + deltaStr + ').';
    } else {
      impactText = 'Only matched rows kept. Row count changed from ' + step.rowsBefore + ' to ' + step.rowsAfter + ' (' + deltaStr + ').';
    }
  }

  const captionLabel = (step.name === 'JOIN' && step.joinType) ? step.joinType : step.name;
  document.getElementById('explanation').innerHTML =
    '<strong>What happened:</strong><br>' + escapeHtml(step.explanation) +
    '<br><br><strong>Impact:</strong><br>' + escapeHtml(impactText) +
    '<br><br><strong>SQL responsible:</strong>';
  document.getElementById('sqlFragment').textContent = step.sqlFragment;
  document.getElementById('resultCaption').textContent =
    'Showing ' + Math.min(step.rows.length, 50) + ' row(s) for the ' + captionLabel + ' step.';

  renderJoinSection(step);
  renderTable(step.rows);

  document.getElementById('prevBtn').disabled = currentIndex === 0;
  document.getElementById('nextBtn').disabled = currentIndex === steps.length - 1;
  document.getElementById('status').textContent = 'Step ' + (currentIndex + 1) + ' of ' + steps.length;
}

// ── Nav buttons ──────────────────────────────────────────────────────────────
document.getElementById('prevBtn').addEventListener('click', function() {
  if (currentIndex > 0) {
    currentIndex -= 1;
    if (steps[currentIndex].name !== 'JOIN') { joinPreviewSelection = null; joinPreviewExpanded = false; }
    renderStep();
  }
});

document.getElementById('nextBtn').addEventListener('click', function() {
  if (currentIndex < steps.length - 1) {
    currentIndex += 1;
    if (steps[currentIndex].name !== 'JOIN') { joinPreviewSelection = null; joinPreviewExpanded = false; }
    renderStep();
  }
});

// ── Info popup ───────────────────────────────────────────────────────────────
document.getElementById('infoBtn').addEventListener('click', function(e) {
  e.stopPropagation();
  var p = document.getElementById('infoPopup');
  p.style.display = p.style.display === 'none' ? 'block' : 'none';
});
document.getElementById('infoPopup').addEventListener('click', function(e) { e.stopPropagation(); });
document.addEventListener('click', function() { document.getElementById('infoPopup').style.display = 'none'; });

// ── Panel visibility helpers ─────────────────────────────────────────────────
function setDebugPanelsVisible(show) {
  var display = show ? '' : 'none';
  ['flowCard', 'stepCard', 'resultCard'].forEach(function(id) {
    document.getElementById(id).style.display = display;
  });
  if (!show) { document.getElementById('joinSection').style.display = 'none'; }
}

// ── Message handler (from extension host) ────────────────────────────────────
window.addEventListener('message', function(event) {
  const message = event.data;

  if (message.command === 'loading') {
    document.getElementById('status').textContent = 'Connecting to MySQL and running query\u2026';
    document.getElementById('errorBox').style.display = 'none';
    setDebugPanelsVisible(false);
    return;
  }

  if (message.command === 'debuggerResult') {
    document.getElementById('errorBox').style.display = 'none';

    // Store and display the SQL
    currentSql = message.sql || '';
    document.getElementById('sqlDisplay').innerHTML = escapeHtml(currentSql);

    // Update the meta line
    var meta = [];
    if (message.source) { meta.push('Source: ' + message.source); }
    if (message.connectionInfo) { meta.push('Connected to: ' + message.connectionInfo); }
    document.getElementById('queryMeta').textContent = meta.join('  \u00b7  ');

    steps = message.steps || [];
    currentIndex = 0;
    joinPreviewSelection = null;
    joinPreviewExpanded = false;

    if (!steps.length) {
      document.getElementById('status').textContent = 'No supported steps found.';
      setDebugPanelsVisible(false);
      clearHighlight();
      return;
    }

    setDebugPanelsVisible(true);
    renderStep();
    return;
  }

  if (message.command === 'error') {
    steps = [];
    currentIndex = 0;
    joinPreviewSelection = null;
    document.getElementById('prevBtn').disabled = true;
    document.getElementById('nextBtn').disabled = true;
    document.getElementById('status').textContent = '';
    setDebugPanelsVisible(false);
    clearHighlight();

    const box = document.getElementById('errorBox');
    box.textContent = message.message;
    box.style.display = 'block';
    return;
  }
});
</script>
</body>
</html>`;
}
