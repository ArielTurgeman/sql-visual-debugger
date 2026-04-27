import * as vscode from 'vscode';
import type { DebugStep } from '../engine/stepEngine';

let currentPanel: vscode.WebviewPanel | undefined;

export function getOrCreatePanel(context: vscode.ExtensionContext): vscode.WebviewPanel {
  if (currentPanel) {
    currentPanel.reveal(vscode.ViewColumn.Beside);
    return currentPanel;
  }

  currentPanel = vscode.window.createWebviewPanel(
    'sqlDebugger',
    'SQL Debugger',
    vscode.ViewColumn.Beside,
    { enableScripts: true, retainContextWhenHidden: true }
  );

  currentPanel.onDidDispose(() => {
    currentPanel = undefined;
  }, null, context.subscriptions);

  return currentPanel;
}

export function recreatePanel(context: vscode.ExtensionContext): vscode.WebviewPanel {
  currentPanel?.dispose();
  return getOrCreatePanel(context);
}

export function sendLoading(panel: vscode.WebviewPanel): void {
  panel.webview.html = renderShell({ state: 'loading' });
}

export function sendError(
  panel: vscode.WebviewPanel,
  message: string,
  knownDatabases?: string[],
  activeDatabase?: string,
): void {
  panel.webview.html = renderShell({ state: 'error', message, knownDatabases, activeDatabase });
}

export function sendResult(
  panel: vscode.WebviewPanel,
  sql: string,
  source: string,
  connectionLabel: string,
  steps: DebugStep[],
  activeDatabase: string,
  knownDatabases: string[],
): void {
  panel.webview.html = renderApp({ sql, source, connectionLabel, steps, activeDatabase, knownDatabases });
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderShell(
  input:
    | { state: 'loading' }
    | { state: 'error'; message: string; knownDatabases?: string[]; activeDatabase?: string }
): string {
  let body: string;
  let script = '';

  if (input.state === 'loading') {
    body = '<div class="empty">Running debugger…</div>';
  } else {
    // Build the known-databases select only if there are options to show
    const known = input.knownDatabases ?? [];
    const knownSelectHtml = known.length > 0
      ? `<div class="switchDbRow">
           <select class="dbDropSelect" id="errorDbSelect">
             <option value="">— select a database —</option>
             ${known.map(db => `<option value="${escapeHtml(db)}">${escapeHtml(db)}</option>`).join('')}
           </select>
           <button class="switchBtn" id="switchKnownBtn">Switch</button>
         </div>`
      : '';

    body = `
      <div class="error">
        <div class="errorMsg">${escapeHtml(input.message)}</div>
        <div class="switchDbPanel">
          <div class="switchDbTitle">Switch Database</div>
          ${knownSelectHtml}
          <button class="switchNewBtn" id="switchNewBtn">+ Enter new database name…</button>
          <button class="reconfigBtn" id="reconfigBtn">Change server / credentials</button>
        </div>
      </div>`;

    script = `
      <script>
        const vscode = acquireVsCodeApi();
        const knownSelect  = document.getElementById('errorDbSelect');
        const switchKnownBtn = document.getElementById('switchKnownBtn');
        const switchNewBtn = document.getElementById('switchNewBtn');
        const reconfigBtn  = document.getElementById('reconfigBtn');

        if (switchKnownBtn) {
          switchKnownBtn.addEventListener('click', function () {
            const db = knownSelect ? knownSelect.value : '';
            if (db) { vscode.postMessage({ command: 'switchDatabase', database: db }); }
          });
        }
        switchNewBtn.addEventListener('click', function () {
          vscode.postMessage({ command: 'promptDatabase' });
        });
        reconfigBtn.addEventListener('click', function () {
          vscode.postMessage({ command: 'changeConnection' });
        });
      ${'<'}/script>`;
  }

  return `<!DOCTYPE html>
  <html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <style>${styles()}</style>
  </head>
  <body>${body}${script}</body>
  </html>`;
}

function renderApp(input: { sql: string; source: string; connectionLabel: string; steps: DebugStep[]; activeDatabase: string; knownDatabases: string[] }): string {
  const payload = JSON.stringify(input).replace(/</g, '\\u003c');
  return `<!DOCTYPE html>
  <html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <style>${styles()}</style>
  </head>
  <body>
    <div id="app"></div>
    <script>
      const state = ${payload};
      const vscode = acquireVsCodeApi();

      let currentStepIndex = 0;
      let activeWindowColumn = null;
      let activeCaseCell = null;
      let distinctPanelOpen = false;
      const app = document.getElementById('app');

      function getBlockGroups() {
        const groups = [];
        let cteCounter = 0;
        let subqueryCounter = 0;

        for (const [idx, step] of state.steps.entries()) {
          const key = \`\${step.blockType}:\${step.blockName}\`;
          const current = groups[groups.length - 1];
          if (!current || current.key !== key) {
            if (step.blockType === 'cte') {
              cteCounter += 1;
            }
            if (step.blockType === 'subquery') {
              subqueryCounter += 1;
            }
            groups.push({
              key,
              blockType: step.blockType,
              blockName: step.blockName,
              cteNumber: step.blockType === 'cte' ? cteCounter : null,
              subqueryNumber: step.blockType === 'subquery' ? subqueryCounter : null,
              steps: [{ step, idx }],
            });
          } else {
            current.steps.push({ step, idx });
          }
        }

        return groups;
      }

      function hasNestedQueryContext() {
        const groups = getBlockGroups();
        return groups.length > 1 || groups.some(group => group.blockType !== 'main');
      }

      function formatBlockLabel(step) {
        if (step.blockType === 'cte') {
          const group = getBlockGroups().find(g => g.key === \`\${step.blockType}:\${step.blockName}\`);
          return \`CTE \${group?.cteNumber ?? 1}: \${step.blockName}\`;
        }
        if (step.blockType === 'subquery') {
          const group = getBlockGroups().find(g => g.key === \`\${step.blockType}:\${step.blockName}\`);
          return \`Subquery \${group?.subqueryNumber ?? 1}: \${step.blockName}\`;
        }
        return 'MAIN QUERY';
      }

      function renderBlockSummary(step) {
        if (!hasNestedQueryContext()) {
          return \`<div class="groupedFlowShell">\${renderFlowBlocks()}</div>\`;
        }

        const deps = Array.isArray(step.blockDependencies) && step.blockDependencies.length > 0
          ? \`<div class="blockDeps">Depends on: \${escapeHtml(step.blockDependencies.join(', '))}</div>\`
          : '';

        return \`
          <div class="blockSummary">
            <span class="blockBadge \${step.blockType === 'cte' ? 'cteBadge' : step.blockType === 'subquery' ? 'subqueryBadge' : 'mainBadge'}">\${escapeHtml(step.blockType === 'cte' ? 'CTE Block' : step.blockType === 'subquery' ? 'Subquery Block' : 'Main Query')}</span>
            \${deps}
          </div>
          <div class="groupedFlowShell">\${renderFlowBlocks()}</div>\`;
      }

      function renderFlowBlocks() {
        const groups = getBlockGroups();
        const activeStep = state.steps[currentStepIndex];
        const activeKey = \`\${activeStep.blockType}:\${activeStep.blockName}\`;
        const showGroupLabels = hasNestedQueryContext();

        return groups.map(group => \`
          <div class="flowBlock flowBlock\${group.blockType === 'cte' ? 'Cte' : group.blockType === 'subquery' ? 'Subquery' : 'Main'} \${group.key === activeKey ? 'activeBlock' : ''}">
            \${showGroupLabels ? \`<div class="flowBlockLabel">\${escapeHtml(group.blockType === 'cte' ? \`CTE \${group.cteNumber}: \${group.blockName}\` : group.blockType === 'subquery' ? \`SUBQUERY \${group.subqueryNumber}: \${group.blockName}\` : 'MAIN QUERY')}</div>\` : ''}
            <div class="flowBlockNodes">
              \${group.steps.map(({ step, idx }) => \`<button type="button" class="flowNode flowNodeBtn \${idx === currentStepIndex ? "active" : ""}" data-step-index="\${idx}">\${escapeHtml(step.title || step.name)}</button>\`).join(\`<span class="arrow">ג†’</span>\`)}
            </div>
          </div>\`
        ).join('');
      }

      function render() {
        const step = state.steps[currentStepIndex];
        const showBlockContext = hasNestedQueryContext();
        const deltaBaseline = step.sourceRows !== undefined ? step.sourceRows : step.rowsBefore;
        const rowDelta = step.rowsAfter - deltaBaseline;
        if (step.name !== 'SELECT') {
          activeWindowColumn = null;
          activeCaseCell = null;
        }
        if (step.name !== 'SELECT' || !step.distinctMeta) {
          distinctPanelOpen = false;
        }
        if (step.name !== 'SELECT' || !Array.isArray(step.caseColumns) || step.caseColumns.length === 0) {
          activeCaseCell = null;
        }
        app.innerHTML = \`
          <div class="topbar card">
            <div class="topbarInfo">
              <div class="title">SQL Debugger</div>
              \${showBlockContext ? \`<div class="sub blockContextLine">\${escapeHtml(formatBlockLabel(step))}</div>\` : ''}
              <div class="sub"><span class="connLabel">\${escapeHtml(state.connectionLabel)}</span> · \${escapeHtml(state.source)}</div>
            </div>
            <div class="topbarControls">
              <div class="nav">
                <button id="prevBtn" \${currentStepIndex === 0 ? "disabled" : ""}>Previous</button>
                <div class="stepBadge">Step \${currentStepIndex + 1} of \${state.steps.length}</div>
                <button id="nextBtn" \${currentStepIndex === state.steps.length - 1 ? "disabled" : ""}>Next</button>
              </div>
            </div>
          </div>

          <div class="flow card">\${state.steps.map((s, idx) => \`<button type="button" class="flowNode flowNodeBtn \${idx === currentStepIndex ? "active" : ""}" data-step-index="\${idx}">\${escapeHtml(s.title || s.name)}</button>\`).join(\`<span class="arrow">→</span>\`)}</div>

          <div class="card">
            \${renderBlockSummary(step)}
            <label class="sqlLabel">SQL responsible</label>
            <div class="sqlBox sqlBoxHero">\${escapeHtml(step.sqlFragment)}</div>
            <div class="stats statsBelow">
              <div class="stat"><label>\${step.sourceRows !== undefined ? 'Source rows' : 'Rows before'}</label><strong>\${formatNumber(step.sourceRows !== undefined ? step.sourceRows : step.rowsBefore)}</strong></div>
              <div class="stat"><label>Rows after</label><strong>\${formatNumber(step.rowsAfter)}</strong></div>
              <div class="stat"><label>Row change</label><strong class="\${rowDelta > 0 ? 'deltaPos' : rowDelta < 0 ? 'deltaNeg' : 'deltaZero'}">\${formatSigned(rowDelta)}</strong></div>
            </div>
            \${step.sourceLabel ? \`<div class="sourceNote">\${escapeHtml(step.sourceLabel)}</div>\` : ''}
            \${step.whereInSubquery ? renderWhereInSubqueryPreview(step.whereInSubquery) : ''}
            \${step.whereScalarSubquery ? renderWhereScalarSubqueryCard(step.whereScalarSubquery) : ''}
            \${step.preFilterRows ? renderFilteredView(step) : ''}
          </div>

          \${step.joinMeta ? renderJoinPanel(step) : ""}
          \${renderDistinctPanel(step)}
          \${renderIntermediate(step)}
        \`;

        document.getElementById('prevBtn')?.addEventListener('click', () => {
          if (currentStepIndex > 0) {
            currentStepIndex -= 1;
            render();
          }
        });
        document.getElementById('nextBtn')?.addEventListener('click', () => {
          if (currentStepIndex < state.steps.length - 1) {
            currentStepIndex += 1;
            render();
          }
        });

        bindJoinClicks(step);
        bindGroupBreakdown(step);
        bindWindowDetails(step);
        bindCaseDetails(step);
        bindDistinctPanel(step);
        bindFlowClicks();
        autoScrollRelevantColumns(step);

        // Notify the extension host which step is now active so it can apply
        // the corresponding editor decoration.  Sent after every render(),
        // which covers: initial load, prevBtn, nextBtn, and jumpToStep().
        vscode.postMessage({ command: 'activeStep', index: currentStepIndex });
      }

      function renderJoinPanel(step) {
        const jm = step.joinMeta;
        const joinTypeLabel = jm.joinType || step.title || 'JOIN';
        const joinTypeExplanation = describeJoinType(jm.joinType, jm.leftTable, jm.rightTable);
        return \`
          <div class="card">
            <div class="sectionTitle">\${escapeHtml(joinTypeLabel)} preview</div>
            <div class="joinHint">Click a <span class="yellowDot"></span> row to filter matches in the other table</div>
            <div class="joinPreviewGrid">
              <div class="previewPane" data-pane-side="left">
                <div class="previewHeader">
                  <span class="previewPaneName">\${escapeHtml(jm.leftTable)}</span>
                  <span class="paneRowCount">\${jm.leftRows.length < jm.allLeftRows.length ? \`Showing \${formatNumber(jm.leftRows.length)} of \${formatNumber(jm.allLeftRows.length)} rows <span class="truncatedHint">(display limit reached)</span>\` : \`\${formatNumber(jm.leftRows.length)} rows\`}</span>
                </div>
                <div class="paneFilterBar">
                  <span class="paneFilterLabel"></span>
                  <button class="backToFullBtn">↩ Full table</button>
                </div>
                \${renderPreviewTable(jm.leftRows, jm.leftColumns, jm.leftKeyCol, "left")}
              </div>
              <div class="joinBridge">
                \${escapeHtml(joinTypeLabel)} ON
                <br/>
                <span>\${escapeHtml(jm.leftKey)} ⇄ \${escapeHtml(jm.rightKey)}</span>
                <br/>
                <span class="joinBridgeRel">\${escapeHtml(jm.relationship)}</span>
                <br/>
                <span class="joinBridgeTypeHelp">\${escapeHtml(joinTypeExplanation)}</span>
              </div>
              <div class="previewPane" data-pane-side="right">
                <div class="previewHeader">
                  <span class="previewPaneName">\${escapeHtml(jm.rightTable)}</span>
                  <span class="paneRowCount">\${jm.rightRows.length < jm.allRightRows.length ? \`Showing \${formatNumber(jm.rightRows.length)} of \${formatNumber(jm.allRightRows.length)} rows <span class="truncatedHint">(display limit reached)</span>\` : \`\${formatNumber(jm.rightRows.length)} rows\`}</span>
                </div>
                <div class="paneFilterBar">
                  <span class="paneFilterLabel"></span>
                  <button class="backToFullBtn">↩ Full table</button>
                </div>
                \${renderPreviewTable(jm.rightRows, jm.rightColumns, jm.rightKeyCol, "right")}
              </div>
            </div>
          </div>\`;
      }

      function describeJoinType(joinType, leftTable, rightTable) {
        const normalized = String(joinType || '').toUpperCase();
        if (normalized === 'LEFT JOIN') {
          return \`Keeps all rows from \${leftTable}, even when there is no match in \${rightTable}.\`;
        }
        if (normalized === 'RIGHT JOIN') {
          return \`Keeps all rows from \${rightTable}, even when there is no match in \${leftTable}.\`;
        }
        if (normalized === 'FULL OUTER JOIN') {
          return \`Keeps matched rows plus unmatched rows from both \${leftTable} and \${rightTable}.\`;
        }
        return \`Keeps only rows where \${leftTable} and \${rightTable} match on the join keys.\`;
      }

      /** Builds <tr> elements for a preview table. Rows carry data-join-value for click handling. */
      function buildPreviewRowsHtml(rows, columns, joinKey, side) {
        return rows.map((row, rowIdx) => {
          const joinValue = row[joinKey];
          const joinComparableKey = getJoinComparableKey(joinValue);
          const cells = columns.map(col =>
            \`<td\${col === joinKey ? ' class="joinCell"' : ''}>\${renderCellValue(row[col])}</td>\`
          ).join('');
          return \`<tr data-side="\${side}" data-row-index="\${rowIdx}" data-join-value="\${escapeAttr(joinValue)}" data-join-key="\${escapeAttr(joinComparableKey ?? '')}" data-join-null="\${joinComparableKey === null ? '1' : '0'}">\${cells}</tr>\`;
        }).join('');
      }

      function renderPreviewTable(rows, columns, joinKey, side) {
        const emptyRow = rows.length === 0
          ? \`<tr><td colspan="\${columns.length}" class="noMatchCell">No rows</td></tr>\`
          : '';
        return \`
          <div class="tableWrap previewWrap" data-join-preview-wrap="\${escapeAttr(side)}">
            <table>
              <thead>
                <tr>\${columns.map(c => \`<th class="\${c === joinKey ? 'joinColHead' : ''}" data-column-name="\${escapeAttr(c)}">\${escapeHtml(c)}</th>\`).join('')}</tr>
              </thead>
              <tbody>
                \${emptyRow || buildPreviewRowsHtml(rows, columns, joinKey, side)}
              </tbody>
            </table>
          </div>\`;
      }

      function getJoinComparableKey(value) {
        if (value === null || value === undefined) {
          return null;
        }
        return typeof value === 'string'
          ? \`str:\${value}\`
          : \`\${typeof value}:\${String(value)}\`;
      }

      function buildTypedRowFingerprint(row, columns) {
        return columns.map((column) => {
          const value = row[column];
          if (value === null) return 'null';
          if (value === undefined) return 'undefined';
          if (typeof value === 'string') return \`string:\${value}\`;
          if (typeof value === 'number') return \`number:\${value}\`;
          if (typeof value === 'boolean') return \`boolean:\${value}\`;
          return \`\${typeof value}:\${String(value)}\`;
        }).join('\x00');
      }

      function bindJoinClicks(step) {
        if (!step.joinMeta) return;
        const jm = step.joinMeta;
        const MAX_PREVIEW = 200;

        // Track which side + value is currently selected (null = no selection).
        let activeSide  = null;
        let activeValue = null;
        let activeNull  = false;

        // ── DOM helpers ────────────────────────────────────────────────────────
        function getPane(side) {
          return document.querySelector(\`[data-pane-side="\${side}"]\`);
        }

        function setFilterBar(side, label) {
          const pane = getPane(side);
          const bar  = pane?.querySelector('.paneFilterBar');
          const lbl  = pane?.querySelector('.paneFilterLabel');
          if (label !== null) {
            bar?.classList.add('visible');
            if (lbl) lbl.textContent = label;
          } else {
            bar?.classList.remove('visible');
            if (lbl) lbl.textContent = '';
          }
        }

        // Restore a pane to its original full preview rows.
        function resetPane(side) {
          const pane    = getPane(side);
          const rows    = side === 'left' ? jm.leftRows    : jm.rightRows;
          const cols    = side === 'left' ? jm.leftColumns : jm.rightColumns;
          const joinKey = side === 'left' ? jm.leftKeyCol  : jm.rightKeyCol;
          const tbody   = pane?.querySelector('tbody');
          if (tbody) tbody.innerHTML = buildPreviewRowsHtml(rows, cols, joinKey, side);
          const allRows = side === 'left' ? jm.allLeftRows : jm.allRightRows;
          const countEl = pane?.querySelector('.paneRowCount');
          if (countEl) countEl.innerHTML = rows.length < allRows.length
            ? \`Showing \${formatNumber(rows.length)} of \${formatNumber(allRows.length)} rows <span class="truncatedHint">(display limit reached)</span>\`
            : \`\${formatNumber(rows.length)} rows\`;
          setFilterBar(side, null);
        }

        // Replace the other pane with only the rows that match the selected comparable join key.
        function filterPane(side, joinComparableKey, joinIsNull) {
          const pane     = getPane(side);
          const allRows  = side === 'left' ? jm.allLeftRows  : jm.allRightRows;
          const cols     = side === 'left' ? jm.leftColumns  : jm.rightColumns;
          const joinKey  = side === 'left' ? jm.leftKeyCol   : jm.rightKeyCol;
          const matches  = joinIsNull || joinComparableKey === null
            ? []
            : allRows.filter(r => getJoinComparableKey(r[joinKey]) === joinComparableKey);
          const total    = matches.length;
          const display  = matches.slice(0, MAX_PREVIEW);

          const tbody = pane?.querySelector('tbody');
          if (tbody) {
            tbody.innerHTML = total === 0
              ? \`<tr><td colspan="\${cols.length}" class="noMatchCell">No matching rows</td></tr>\`
              : buildPreviewRowsHtml(display, cols, joinKey, side);
          }
          const countEl = pane?.querySelector('.paneRowCount');
          if (countEl) countEl.textContent = \`Matched: \${formatNumber(total)} row\${total === 1 ? '' : 's'}\`;

          const barLabel = total === 0
            ? 'No matching rows'
            : total > MAX_PREVIEW
              ? \`Showing first \${MAX_PREVIEW} of \${formatNumber(total)}\`
              : 'Showing matches for selected row';
          setFilterBar(side, barLabel);
        }

        function resetBoth() {
          activeSide  = null;
          activeValue = null;
          activeNull  = false;
          resetPane('left');
          resetPane('right');
        }

        // ── Row click handler ──────────────────────────────────────────────────
        function handleRowClick(tr) {
          const side      = tr.dataset.side;
          const joinValue = tr.dataset.joinValue;
          const joinKey   = tr.dataset.joinKey ?? '';
          const joinIsNull = tr.dataset.joinNull === '1';
          const otherSide = side === 'left' ? 'right' : 'left';

          // Clicking the same value again → deselect and reset.
          if (activeSide === side && activeValue === joinKey && activeNull === joinIsNull) {
            resetBoth();
            return;
          }

          // Reset both panes to full content (clears any previous filter state).
          resetPane('left');
          resetPane('right');

          activeSide  = side;
          activeValue = joinKey;
          activeNull  = joinIsNull;

          // Highlight the clicked row (and any sibling with the same join value) in source pane.
          getPane(side)?.querySelectorAll(\`tr[data-side="\${side}"]\`).forEach(r => {
            if (r.dataset.joinKey === activeValue && (r.dataset.joinNull === '1') === activeNull) {
              r.classList.add('selectedRow');
            }
          });

          // Filter the other pane.
          filterPane(otherSide, joinKey, joinIsNull);
        }

        // ── Event delegation — one listener per pane, covers all current/future rows ──
        ['left', 'right'].forEach(side => {
          const pane = getPane(side);
          if (!pane) return;
          pane.addEventListener('click', (e) => {
            // "Back to full table" button
            if (e.target.closest('.backToFullBtn')) { resetBoth(); return; }
            // Any row click (including in filtered pane)
            const tr = e.target.closest('tr[data-side]');
            if (tr) handleRowClick(tr);
          });
        });
      }

      // ─── Group Breakdown ─────────────────────────────────────────────────────

      /**
       * Builds the HTML for the group breakdown card.
       * Filters step.preGroupRows to rows whose group-key columns match the clicked
       * grouped result row, then renders a scrollable table of those source rows.
       */
      function renderGroupBreakdown(step, rowIdx) {
        const groupRow  = step.data[rowIdx];
        const preRows   = step.preGroupRows   || [];
        const preCols   = step.preGroupColumns || (preRows.length > 0 ? Object.keys(preRows[0]) : []);
        const breakdownLimit = 200;
        const groupMappings = (step.groupByColumns || []).map((groupCol) => {
          const sourceColumn = resolveColumnReference(preCols, groupCol);
          const outputColumn = resolveColumnReference(step.columns || [], groupCol) || groupCol;
          return { sourceColumn, outputColumn };
        }).filter((mapping) => mapping.sourceColumn && mapping.outputColumn);

        // Filter source rows that contributed to this group.
        const matching = preRows.filter(row =>
          groupMappings.every(({ sourceColumn, outputColumn }) =>
            String(row[sourceColumn] ?? '') === String(groupRow[outputColumn] ?? '')
          )
        );

        // Human-readable group label for the panel title.
        const groupLabel = groupMappings
          .map(({ outputColumn }) => escapeHtml(String(groupRow[outputColumn] ?? '')))
          .join(', ');

        if (preCols.length === 0 || matching.length === 0) {
          return \`<div class="card groupBreakdownCard">
            <div class="groupBreakdownTitle">GROUP BREAKDOWN — \${groupLabel}</div>
            <div class="groupBreakdownSub">No source rows found for this group.</div>
          </div>\`;
        }

        // Build a map: bare source column name (lowercase) → aggregation function name.
        // e.g. "playerid" → "SUM" for SUM(playerinfo.PlayerId).
        const aggSrcMap = new Map();
        (step.aggColumns || []).forEach(a => {
          if (a.srcCol) {
            const resolvedSource = resolveColumnReference(preCols, a.srcCol);
            if (!resolvedSource) return;
            const existingFns = aggSrcMap.get(resolvedSource) || [];
            if (!existingFns.includes(a.fn)) {
              existingFns.push(a.fn);
            }
            aggSrcMap.set(resolvedSource, existingFns);
          }
        });

        const headerHtml = preCols.map(c => {
          const aggFns = aggSrcMap.get(c);
          // Aggregation source column: violet tint header + reuse existing .aggBadge pill.
          if (aggFns && aggFns.length > 0) {
            return \`<th class="bdAggSrcHead" data-column-name="\${escapeAttr(c)}">\${escapeHtml(c)}\${aggFns.map(fn => \`<span class="aggBadge">\${escapeHtml(fn)}</span>\`).join('')}</th>\`;
          }
          // All other columns (including GROUP BY key) are plain — the breakdown's job is to
          // highlight the calculation source, not to re-emphasise the grouping key.
          return \`<th data-column-name="\${escapeAttr(c)}">\${escapeHtml(c)}</th>\`;
        }).join('');

        const breakdownRows = matching.slice(0, breakdownLimit);
        const bodyHtml = breakdownRows.map(row =>
          \`<tr>\${preCols.map(c => {
            const isAgg = aggSrcMap.has(c);
            return \`<td\${isAgg ? ' class="bdAggSrcCell"' : ''}>\${renderCellValue(row[c])}</td>\`;
          }).join('')}</tr>\`
        ).join('');
        const countLabel = matching.length > breakdownRows.length
          ? \`Showing \${formatNumber(breakdownRows.length)} of \${formatNumber(matching.length)} row(s) <span class="truncatedHint">(display limit reached)</span>\`
          : \`\${matching.length} row\${matching.length === 1 ? '' : 's'} contributed to this group\`;

        return \`
          <div class="card groupBreakdownCard">
            <div class="groupBreakdownTitle">GROUP BREAKDOWN — \${groupLabel}</div>
            <div class="groupBreakdownSub">\${countLabel}</div>
            <div class="tableWrap breakdownWrap" id="groupBreakdownTableWrap">
              <table>
                <thead><tr>\${headerHtml}</tr></thead>
                <tbody>\${bodyHtml}</tbody>
              </table>
            </div>
          </div>\`;
      }

      /**
       * Wires click handlers onto every .groupRow in the current GROUP BY step.
       * Follows the same pattern as bindJoinClicks — called once per render().
       */
      function bindGroupBreakdown(step) {
        if (step.name !== 'GROUP BY' || !step.preGroupRows) return;
        const container = document.getElementById('groupBreakdownContainer');
        if (!container) return;

        document.querySelectorAll('.groupByClickableCell').forEach((cell) => {
          cell.addEventListener('click', () => {
            const tr = cell.closest('.groupRow');
            if (!tr) return;
            // Deselect all, select clicked row.
            document.querySelectorAll('.groupRow').forEach(r => r.classList.remove('activeGroupRow'));
            tr.classList.add('activeGroupRow');

            const rowIdx = parseInt(tr.getAttribute('data-rowindex') || '0', 10);
            container.innerHTML = renderGroupBreakdown(step, rowIdx);
            autoScrollGroupBreakdown(step);
          });
        });
      }

      function autoScrollGroupBreakdown(step) {
        const wrapper = document.getElementById('groupBreakdownTableWrap');
        if (!wrapper) return;

        const aggSourceColumns = (step.aggColumns || [])
          .map((agg) => agg.srcCol)
          .filter(Boolean);
        if (aggSourceColumns.length === 0) return;

        const header = findRelevantHeader(wrapper, aggSourceColumns);
        scrollHeaderIntoView(wrapper, header);
      }

      // ─────────────────────────────────────────────────────────────────────────

      function bindWindowDetails(step) {
        if (step.name !== 'SELECT' || !Array.isArray(step.windowColumns) || step.windowColumns.length === 0) {
          return;
        }
        const container = document.getElementById('windowFunctionDetail');
        if (!container) return;

        if (activeWindowColumn) {
          container.innerHTML = renderWindowDetail(step, activeWindowColumn);
        }

        document.querySelectorAll('[data-window-column]').forEach((el) => {
          el.addEventListener('click', () => {
            const column = el.getAttribute('data-window-column');
            if (!column) return;
            activeWindowColumn = activeWindowColumn === column ? null : column;
            container.innerHTML = activeWindowColumn ? renderWindowDetail(step, activeWindowColumn) : '';
            document.querySelectorAll('[data-window-column]').forEach(node => node.classList.remove('activeWindowHeader'));
            if (activeWindowColumn) {
              el.classList.add('activeWindowHeader');
            }
          });
        });
      }

      function renderWindowDetail(step, outputColumn) {
        const meta = (step.windowColumns || []).find(col => col.outputColumn === outputColumn);
        if (!meta) return '';

        const previewColumns = [
          ...(meta.partitionBy || []),
          ...((meta.orderByTerms || []).map(term => term.column)),
          meta.outputColumn
        ].filter((value, index, arr) => arr.findIndex(item => item.toLowerCase() === value.toLowerCase()) === index);

        const summary = formatWindowSummary(meta);
        const orderMeta = (meta.orderByTerms || []).map(term => \`\${term.column} \${term.direction}\`).join(', ');
        const metaLine = [
          meta.partitionBy.length > 0 ? \`Partition by: \${meta.partitionBy.join(', ')}\` : null,
          meta.orderByTerms.length > 0 ? \`Order by: \${orderMeta}\` : null
        ].filter(Boolean).join(' • ');

        const visibleColumns = new Set((step.columns || []).map(col => String(col).toLowerCase()));
        const canUseVisibleRows = previewColumns.every(col => visibleColumns.has(String(col).toLowerCase()));
        const previewSourceRows = canUseVisibleRows
          ? (step.data || [])
          : ((meta.previewRows || []).length > 0 ? meta.previewRows : (step.data || []));
        const previewRows = previewSourceRows.map(row => {
          const preview = {};
          previewColumns.forEach(col => {
            preview[col] = row[col];
          });
          return preview;
        });
        const previewScopeNote = canUseVisibleRows && step.data.length < step.rowsAfter
          ? \`Preview follows the \${formatNumber(step.data.length)} visible row(s) shown above. Additional partition changes may be outside the current display limit.\`
          : !canUseVisibleRows && (meta.previewRows || []).length > 0
            ? 'Preview includes supporting partition/order columns that are not visible in the intermediate result.'
            : '';

        const partitionKeys = [];
        for (const row of previewRows) {
          const key = (meta.partitionBy || []).map(col => String(row[col] ?? '')).join('||') || '__all__';
          if (!partitionKeys.includes(key)) partitionKeys.push(key);
        }

        let previousPartitionKey = null;
        const previewBody = previewRows.map(row => {
          const currentPartitionKey = (meta.partitionBy || []).map(col => String(row[col] ?? '')).join('||');
          const shouldSeparate = meta.partitionBy.length > 0 && previousPartitionKey !== null && previousPartitionKey !== currentPartitionKey;
          previousPartitionKey = currentPartitionKey;
          const tintClass = \`windowPartitionTint\${partitionKeys.indexOf(currentPartitionKey || '__all__') % 5}\`;
          const rowHtml = \`<tr class="\${shouldSeparate ? 'windowPartitionBreak ' : ''}\${tintClass}" data-partition-key="\${escapeAttr(currentPartitionKey || '__all__')}">\${previewColumns.map(col => \`<td>\${renderCellValue(row[col])}</td>\`).join('')}</tr>\`;
          return rowHtml;
        }).join('');

        const previewHeader = previewColumns.map(col => \`<th>\${escapeHtml(col)}</th>\`).join('');

        return \`
          <div class="card windowDetailCard">
            <div class="sectionTitle">Window function preview</div>
            <div class="windowSummaryLine">\${escapeHtml(summary)}</div>
            \${metaLine ? \`<div class="windowMetaLine">\${escapeHtml(metaLine)}</div>\` : ''}
            \${previewScopeNote ? \`<div class="windowHint">\${escapeHtml(previewScopeNote)}</div>\` : ''}
            <div class="tableWrap breakdownWrap">
              <table>
                <thead><tr>\${previewHeader}</tr></thead>
                <tbody>\${previewBody}</tbody>
              </table>
            </div>
          </div>\`;
      }

      function bindCaseDetails(step) {
        if (step.name !== 'SELECT' || !Array.isArray(step.caseColumns) || step.caseColumns.length === 0) {
          return;
        }

        const container = document.getElementById('caseWhenDetail');
        if (!container) return;

        if (activeCaseCell) {
          container.innerHTML = renderCaseDetail(step, activeCaseCell.column, activeCaseCell.rowIndex);
        }

        document.querySelectorAll('[data-case-column]').forEach((el) => {
          el.addEventListener('click', () => {
            const column = el.getAttribute('data-case-column');
            const rowIndex = parseInt(el.getAttribute('data-case-row') || '-1', 10);
            if (!column || rowIndex < 0) return;
            const sameCell = activeCaseCell && activeCaseCell.column === column && activeCaseCell.rowIndex === rowIndex;
            activeCaseCell = sameCell ? null : { column, rowIndex };
            container.innerHTML = activeCaseCell ? renderCaseDetail(step, activeCaseCell.column, activeCaseCell.rowIndex) : '';
            document.querySelectorAll('[data-case-column]').forEach(node => node.classList.remove('activeCaseCell'));
            if (activeCaseCell) {
              el.classList.add('activeCaseCell');
            }
          });
        });
      }

      function renderCaseDetail(step, outputColumn, rowIndex) {
        const meta = (step.caseColumns || []).find(col => col.outputColumn === outputColumn);
        const rowMeta = meta?.rowExplanations?.[rowIndex];
        if (!meta || !rowMeta) return '';

        const inputValuesHtml = rowMeta.inputValues.length > 0
          ? rowMeta.inputValues.map(item =>
              \`<div class="caseInputItem"><span class="caseInputName">\${escapeHtml(item.column)}</span><span class="caseInputValue">\${renderCellValue(item.value)}</span></div>\`
            ).join('')
          : '<div class="caseInputEmpty">No direct input columns were captured for this CASE condition.</div>';

        return \`
          <div class="card caseDetailCard">
            <div class="sectionTitle">CASE WHEN explanation</div>
            <div class="caseDetailSentence">This row matched a CASE branch that returned the selected value shown in \${escapeHtml(outputColumn)}.</div>
            <div class="caseDetailGrid">
              <div class="caseDetailBlock">
                <div class="caseDetailLabel">Relevant inputs</div>
                <div class="caseInputList">\${inputValuesHtml}</div>
              </div>
              <div class="caseDetailBlock">
                <div class="caseDetailLabel">Matched rule</div>
                <div class="caseRuleValue">\${escapeHtml(rowMeta.matchedRule)}</div>
              </div>
              <div class="caseDetailBlock">
                <div class="caseDetailLabel">Returned value</div>
                <div class="caseReturnedValue">\${renderCellValue(rowMeta.returnedValue)}</div>
              </div>
            </div>
          </div>\`;
      }

      function formatWindowSummary(meta) {
        const partitionText = meta.partitionBy.length > 0 ? meta.partitionBy.join(', ') : 'all rows';
        const orderText = meta.orderByTerms.length > 0
          ? meta.orderByTerms.map(term => \`\${term.column} \${term.direction}\`).join(', ')
          : 'the current row order';
        if (meta.functionName === 'ROW_NUMBER') {
          return \`Partitions rows by \${partitionText} and orders them by \${orderText}, then assigns row numbers.\`;
        }
        if (meta.functionName === 'RANK') {
          return \`Partitions rows by \${partitionText} and orders them by \${orderText}, with gaps after ties.\`;
        }
        if (meta.functionName === 'DENSE_RANK') {
          return \`Partitions rows by \${partitionText} and orders them by \${orderText}, without gaps after ties.\`;
        }
        return \`Partitions rows by \${partitionText} and orders them by \${orderText}, then computes \${meta.functionName} for each row.\`;
      }

      // ─────────────────────────────────────────────────────────────────────────

      function renderFilteredView(step) {
        const cols = step.preFilterColumns && step.preFilterColumns.length
          ? step.preFilterColumns
          : step.columns;
        const whereCols = new Set(step.whereColumns || []);
        // Build a fingerprint for every row that passed the WHERE filter so we can
        // quickly classify each pre-filter row as passed or failed.
        const passFingerprints = new Map();
        (step.data || []).forEach(r => {
          const fp = cols.map(c => String(r[c] ?? '')).join('\x00');
          passFingerprints.set(fp, (passFingerprints.get(fp) || 0) + 1);
        });

        // WHERE column header: yellow tint (opaque background — solid base + transparent overlay
        // avoids header text bleeding when rows scroll beneath a sticky th).
        // Yellow reads as "this column is the active filter", not as "this column has errors".
        const whereHeaderStyle = 'background:linear-gradient(rgba(246,223,108,.14),rgba(246,223,108,.14)) #182142;color:#c9b840;';
        const headerHtml = cols.map(c => {
          const isWhere = whereCols.has(c);
          return \`<th data-column-name="\${escapeAttr(c)}" style="\${isWhere ? whereHeaderStyle : ''}">\${escapeHtml(c)}</th>\`;
        }).join('');

        const bodyHtml = step.preFilterRows.map(row => {
          const fp = cols.map(c => String(row[c] ?? '')).join('\x00');
          const remaining = passFingerprints.get(fp) || 0;
          const passed = remaining > 0;
          if (passed) passFingerprints.set(fp, remaining - 1);

          if (passed) {
            // Passing rows: default table styling — no red, no strikethrough, nothing
            // that reads as an error.  WHERE column cells get only a hairline inset
            // shadow on the left edge so the column alignment is obvious without any
            // colour connotation (box-shadow does not affect layout/column widths).
            const cells = cols.map(c => {
              const isWhere = whereCols.has(c);
              return \`<td style="\${isWhere ? 'box-shadow:inset 2px 0 0 rgba(246,223,108,.28);' : ''}">\${renderCellValue(row[c])}</td>\`;
            }).join('');
            return \`<tr>\${cells}</tr>\`;
          } else {
            // Failing rows: red blush on every cell, stronger red on the WHERE column
            // that drove the failure, strikethrough on every cell, slight opacity drop
            // so passing rows visually "float" above the removed ones.
            const cells = cols.map(c => {
              const isWhere = whereCols.has(c);
              const bg = isWhere
                ? 'background:rgba(255,80,80,.28);'
                : 'background:rgba(255,80,80,.10);';
              return \`<td style="\${bg}text-decoration:line-through;">\${renderCellValue(row[c])}</td>\`;
            }).join('');
            return \`<tr style="opacity:.62;">\${cells}</tr>\`;
          }
        }).join('');

        // The filtered preview is intentionally capped, so its row count cannot be
        // used to summarize the actual effect of the filter. Use the step totals.
        const removedCount = Math.max(0, (step.rowsBefore || 0) - (step.rowsAfter || 0));
        const previewCount = step.preFilterRows.length;
        const totalBefore = step.rowsBefore || previewCount;
        const previewLabel = previewCount < totalBefore
          ? \`Showing \${formatNumber(previewCount)} of \${formatNumber(totalBefore)}\`
          : formatNumber(previewCount);
        const removedLabel = removedCount > 0
          ? \`<span class="filteredViewMetaNeg">\${formatNumber(removedCount)} removed total</span>\`
          : \`<span class="filteredViewMetaPos">none removed</span>\`;
        // WHERE filters individual rows; HAVING filters aggregated groups.
        // Use the appropriate noun so the label stays accurate in both steps.
        const noun = step.name === 'HAVING' ? 'groups' : 'rows';
        const previewSuffix = previewCount < totalBefore
          ? \` row(s) <span class="truncatedHint">(display limit reached)</span>\`
          : ' row(s)';

        return \`
          <div class="filteredViewBlock">
            <div class="sectionTitle">Filtered \${noun} preview <span class="subtle">\${previewLabel}\${previewSuffix}, \${removedLabel}</span></div>
            <div class="tableWrap filteredWrap" id="filteredViewTableWrap">
              <table>
                <thead><tr>\${headerHtml}</tr></thead>
                <tbody>\${bodyHtml}</tbody>
              </table>
            </div>
          </div>\`;
      }

      function renderWhereInSubqueryPreview(meta) {
        const columns = meta.columns || [];
        const headerHtml = columns.map(c => \`<th>\${escapeHtml(c)}</th>\`).join('');
        const bodyHtml = (meta.rows || []).length > 0
          ? meta.rows.map(row => \`<tr>\${columns.map(c => \`<td>\${renderCellValue(row[c])}</td>\`).join('')}</tr>\`).join('')
          : \`<tr><td colspan="\${Math.max(columns.length, 1)}" class="noMatchCell">No rows</td></tr>\`;
        const countLabel = meta.rows.length < meta.totalRows
          ? \`Showing \${formatNumber(meta.rows.length)} of \${formatNumber(meta.totalRows)} row(s) <span class="truncatedHint">(display limit reached)</span>\`
          : \`\${formatNumber(meta.totalRows)} row(s)\`;

        return \`
          <div class="whereSubqueryBlock">
            <div class="whereSubqueryExplanation">\${escapeHtml(meta.explanation)}</div>
            <div class="sectionTitle">Subquery result <span class="subtle">\${countLabel}</span></div>
            <div class="tableWrap subqueryWrap">
              <table>
                <thead><tr>\${headerHtml}</tr></thead>
                <tbody>\${bodyHtml}</tbody>
              </table>
            </div>
          </div>\`;
      }

      function renderWhereScalarSubqueryCard(meta) {
        return \`
          <div class="whereSubqueryBlock">
            <div class="whereSubqueryExplanation">\${escapeHtml(meta.explanation)}</div>
            <div class="whereScalarCard">
              <div class="whereScalarLabel">Subquery value</div>
              <div class="whereScalarValue">\${renderCellValue(meta.value)}</div>
              <div class="whereScalarMeta">\${escapeHtml(meta.columnLabel)}</div>
            </div>
          </div>\`;
      }

      function renderDistinctPanel(step) {
        if (step.name !== 'SELECT' || !step.distinctMeta) {
          return '';
        }

        return \`
          <div class="distinctPanelSlot">
            <button type="button" class="distinctBadge \${distinctPanelOpen ? 'active' : ''}" id="distinctBadgeToggle">DISTINCT</button>
            \${distinctPanelOpen ? renderDistinctDetail(step.distinctMeta) : ''}
          </div>\`;
      }

      function bindDistinctPanel(step) {
        if (step.name !== 'SELECT' || !step.distinctMeta) {
          return;
        }

        document.getElementById('distinctBadgeToggle')?.addEventListener('click', () => {
          distinctPanelOpen = !distinctPanelOpen;
          render();
        });
      }

      function renderDistinctDetail(meta) {
        const cols = meta.columns || [];
        const rows = meta.rows || [];
        const groupMap = new Map();

        rows.forEach((row) => {
          const fingerprint = buildTypedRowFingerprint(row, cols);
          const existing = groupMap.get(fingerprint);
          if (existing) {
            existing.push(row);
          } else {
            groupMap.set(fingerprint, [row]);
          }
        });

        const groupedRows = Array.from(groupMap.values());
        const headerHtml = cols.map(c => \`<th>\${escapeHtml(c)}</th>\`).join('');
        const bodyHtml = groupedRows.length > 0
          ? groupedRows.map((groupRows) =>
          groupRows.map((row, idx) => {
            const removed = idx > 0;
            const cells = cols.map(c => {
              const style = removed
                ? 'background:rgba(255,80,80,.10);text-decoration:line-through;'
                : '';
              return \`<td style="\${style}">\${renderCellValue(row[c])}</td>\`;
            }).join('');
            return \`<tr\${removed ? ' style="opacity:.62;"' : ''}>\${cells}</tr>\`;
          }).join('')
        ).join('')
          : \`<tr><td colspan="\${Math.max(cols.length, 1)}" class="noMatchCell">No selected rows available</td></tr>\`;

        return \`
          <div class="distinctDetailBlock">
            <div class="distinctDetailSentence">DISTINCT kept one copy of each unique selected row and removed the duplicates.</div>
            <div class="tableWrap filteredWrap">
              <table>
                <thead><tr>\${headerHtml}</tr></thead>
                <tbody>\${bodyHtml}</tbody>
              </table>
            </div>
          </div>\`;
      }

      function renderIntermediate(step) {
        // For non-JOIN steps that follow a JOIN, schemaContext carries the accumulated
        // indicator columns so that "joined" / "duplicate" badges remain visible.
        const joinIndicatorColumns =
          step.joinMeta?.joinIndicatorColumns ||
          step.schemaContext?.joinIndicatorColumns ||
          [];
        const leftKeyCol = step.joinMeta?.leftKeyCol || '';
        const rightKeyCol = step.joinMeta?.rightKeyCol || '';
        // Qualified duplicate columns contain a dot (e.g. "prfos.PlayerId")
        // and get a special "dupe" badge to explain why the column appears twice.
        const isDupeCol = (c) => joinIndicatorColumns.includes(c) && c.includes('.');
        // ORDER BY sort-key columns — highlighted to show which column drives the ordering.
        const sortColSet    = new Set(step.sortColumns || []);
        // GROUP BY key columns — teal highlight so group keys stand out at a glance.
        const groupByColSet = new Set(step.groupByColumns || []);
        // Aggregation columns — badge shows the function name (COUNT, SUM, …); no cell tint.
        const aggColMap     = new Map((step.aggColumns || []).map(a => [a.col, a.fn]));
        const windowColMap  = new Map((step.windowColumns || []).map(w => [w.outputColumn, w]));
        const caseColSet    = new Set((step.caseColumns || []).map(c => c.outputColumn));
        const limitedRows = step.data;
        const headerHtml = step.columns.map((c) => {
          const isJoined  = joinIndicatorColumns.includes(c);
          const isDupe    = isDupeCol(c);
          const isSort    = sortColSet.has(c);
          const isGroupBy = groupByColSet.has(c);
          const isAgg     = aggColMap.has(c);
          const isWindow  = step.name === 'SELECT' && windowColMap.has(c);
          // Badge priority: dupe > joined > agg.  Group-key and sort columns get no badge.
          const badge = isDupe   ? '<span class="joinedBadge dupeBadge">duplicate</span>'
                      : isJoined ? '<span class="joinedBadge">joined</span>'
                      : isAgg    ? \`<span class="aggBadge">\${escapeHtml(aggColMap.get(c))}</span>\`
                      : '';
          const cls = [
            isDupe ? 'joinedColHead dupeHead' : '',
            !isDupe && isJoined ? 'joinedColHead' : '',
            isWindow ? 'windowColHead windowSelectableHead' : '',
            isGroupBy ? 'groupByColHead' : '',
            isSort ? 'sortColHead' : '',
          ].filter(Boolean).join(' ');
          return isWindow
            ? \`<th class="\${cls} windowHeaderCell" data-window-column="\${escapeAttr(c)}" data-column-name="\${escapeAttr(c)}">\${escapeHtml(c)}\${badge}</th>\`
            : \`<th class="\${cls}" data-column-name="\${escapeAttr(c)}">\${escapeHtml(c)}\${badge}</th>\`;
        }).join('');

        // For GROUP BY: each body row is clickable — mark with class + index for breakdown.
        const isGroupByStep = step.name === 'GROUP BY' && !!step.preGroupRows;
        const bodyHtml = limitedRows.map((row, rowIdx) => {
          const cells = step.columns.map((col) => {
            const isJoined  = joinIndicatorColumns.includes(col);
            const isDupe    = isDupeCol(col);
            const isSort    = sortColSet.has(col);
            const isGroupBy = groupByColSet.has(col);
            const isCase    = step.name === 'SELECT' && caseColSet.has(col);
            const cls = [
              isDupe ? 'dupeColCell' : '',
              !isDupe && isJoined ? 'joinedColCell' : '',
              isGroupBy ? 'groupByColCell' : '',
              isSort ? 'sortColCell' : '',
            ].filter(Boolean).join(' ');
            return isCase
              ? \`<td class="\${cls} caseCell \${activeCaseCell && activeCaseCell.column === col && activeCaseCell.rowIndex === rowIdx ? 'activeCaseCell' : ''}" data-case-column="\${escapeAttr(col)}" data-case-row="\${rowIdx}">\${renderCellValue(row[col])}</td>\`
              : \`<td class="\${cls}\${isGroupByStep && isGroupBy ? ' groupByClickableCell' : ''}">\${renderCellValue(row[col])}</td>\`;
          }).join('');
          const rowAttrs = isGroupByStep
            ? \`class="groupRow" data-rowindex="\${rowIdx}"\`
            : '';
          return \`<tr \${rowAttrs}>\${cells}</tr>\`;
        }).join('');

        // Compact "Aggregations:" hint shown only on GROUP BY steps.
        const aggHint = step.aggSummary
          ? \`<div class="aggHint"><span class="aggHintLabel">Aggregations</span>\${escapeHtml(step.aggSummary)}</div>\`
          : '';
        const windowHint = step.name === 'SELECT' && windowColMap.size > 0
          ? '<div class="windowHint">Click a highlighted window-function column name to see how it was computed.</div>'
          : '';
        const caseHint = step.name === 'SELECT' && caseColSet.size > 0
          ? '<div class="windowHint">Click a highlighted CASE WHEN result cell to see why that value was returned.</div>'
          : '';

        // Breakdown container rendered as a sibling card below the grouped table.
        // bindGroupBreakdown() injects content into it when a row is clicked.
        const breakdownSlot = isGroupByStep
          ? '<div id="groupBreakdownContainer"></div>'
          : '';
        const windowDetailSlot = step.name === 'SELECT' && windowColMap.size > 0
          ? '<div id="windowFunctionDetail"></div>'
          : '';
        const caseDetailSlot = step.name === 'SELECT' && caseColSet.size > 0
          ? '<div id="caseWhenDetail"></div>'
          : '';

        return \`
          <div class="card">
            <div class="sectionTitle">Intermediate result <span class="subtle">\${step.data.length < step.rowsAfter ? \`Showing \${formatNumber(step.data.length)} of \${formatNumber(step.rowsAfter)}\` : formatNumber(step.data.length)} row(s)\${step.data.length < step.rowsAfter ? ' <span class="truncatedHint">(display limit reached)</span>' : ''}</span></div>
            \${aggHint}
            \${windowHint}
            \${caseHint}
            \${isGroupByStep ? '<div class="groupClickHint">Click a <span class="groupDot"></span> row to expand its source rows</div>' : ''}
            <div class="tableWrap resultWrap" id="intermediateTableWrap">
              <table>
                <thead>
                  <tr>\${headerHtml}</tr>
                </thead>
                <tbody>
                  \${bodyHtml}
                </tbody>
              </table>
            </div>
          </div>
          \${windowDetailSlot}
          \${caseDetailSlot}
          \${breakdownSlot}\`;
      }

      function escapeHtml(value) {
        return String(value ?? '')
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }

      function escapeAttr(value) {
        return escapeHtml(value).replace(/\`/g, '&#96;');
      }

      function formatNumber(value) {
        return Number(value || 0).toLocaleString();
      }

      function formatSigned(value) {
        const n = Number(value || 0);
        return n > 0 ? \`+\${n}\` : \`\${n}\`;
      }

      function renderCellValue(value) {
        if (value === null || value === undefined) {
          return '<span class="nullValue">NULL</span>';
        }
        return escapeHtml(value);
      }

      function normalizeColumnName(value) {
        return String(value ?? '')
          .replace(/[\u0060"]/g, '')
          .trim()
          .toLowerCase();
      }

      function getBareColumnName(value) {
        const normalized = normalizeColumnName(value);
        if (!normalized) {
          return '';
        }
        const parts = normalized.split('.');
        return parts[parts.length - 1];
      }

      function resolveColumnReference(columns, target) {
        if (!Array.isArray(columns) || columns.length === 0) {
          return null;
        }

        const normalizedTarget = normalizeColumnName(target);
        const bareTarget = getBareColumnName(target);
        if (!normalizedTarget) {
          return null;
        }

        const exactMatch = columns.find((column) => normalizeColumnName(column) === normalizedTarget);
        if (exactMatch) {
          return exactMatch;
        }

        const bareMatches = columns.filter((column) => getBareColumnName(column) === bareTarget);
        if (bareMatches.length === 1) {
          return bareMatches[0];
        }

        return null;
      }

      function findRelevantHeader(wrapper, columns) {
        if (!wrapper || !Array.isArray(columns) || columns.length === 0) {
          return null;
        }

        const headers = Array.from(wrapper.querySelectorAll('thead th'));
        const headerNames = headers.map(header => header.getAttribute('data-column-name') || header.textContent || '');

        for (const column of columns) {
          const resolvedHeaderName = resolveColumnReference(headerNames, column);
          if (!resolvedHeaderName) continue;
          const resolvedHeader = headers.find((header) => {
            const headerName = header.getAttribute('data-column-name') || header.textContent || '';
            return normalizeColumnName(headerName) === normalizeColumnName(resolvedHeaderName);
          });
          if (resolvedHeader) {
            return resolvedHeader;
          }
        }

        return null;
      }

      function scrollHeaderIntoView(wrapper, header) {
        if (!wrapper || !header) {
          return;
        }

        const visibleLeft = wrapper.scrollLeft;
        const visibleRight = visibleLeft + wrapper.clientWidth;
        const headerLeft = header.offsetLeft;
        const headerRight = headerLeft + header.offsetWidth;
        const padding = 24;

        if (headerLeft >= visibleLeft && headerRight <= visibleRight) {
          return;
        }

        const nextLeft = headerLeft < visibleLeft
          ? Math.max(0, headerLeft - padding)
          : Math.max(0, headerRight - wrapper.clientWidth + padding);

        wrapper.scrollTo({ left: nextLeft, behavior: 'smooth' });
      }

      function autoScrollRelevantColumns(step) {
        if (step.name === 'JOIN' && step.joinMeta) {
          const leftWrapper = document.querySelector('[data-join-preview-wrap="left"]');
          const rightWrapper = document.querySelector('[data-join-preview-wrap="right"]');
          const leftHeader = findRelevantHeader(leftWrapper, [step.joinMeta.leftKeyCol]);
          const rightHeader = findRelevantHeader(rightWrapper, [step.joinMeta.rightKeyCol]);
          scrollHeaderIntoView(leftWrapper, leftHeader);
          scrollHeaderIntoView(rightWrapper, rightHeader);
          return;
        }

        const relevantColumns =
          step.name === 'ORDER BY' ? (step.sortColumns || []) :
          step.name === 'GROUP BY' ? [
            ...((step.aggColumns || []).map((agg) => agg.col)),
            ...(step.groupByColumns || []),
          ] :
          step.name === 'WHERE' || step.name === 'HAVING' ? (step.whereColumns || []) :
          [];

        if (relevantColumns.length === 0) {
          return;
        }

        const wrapperId =
          step.name === 'ORDER BY' || step.name === 'GROUP BY'
            ? 'intermediateTableWrap'
            : 'filteredViewTableWrap';

        const wrapper = document.getElementById(wrapperId);
        const header = findRelevantHeader(wrapper, relevantColumns);
        scrollHeaderIntoView(wrapper, header);
      }

      function jumpToStep(idx) {
        currentStepIndex = idx;
        render();
      }

      function bindFlowClicks() {
        document.querySelectorAll('.flowNodeBtn[data-step-index]').forEach((node) => {
          node.addEventListener('click', () => {
            const idx = Number(node.getAttribute('data-step-index'));
            if (!Number.isInteger(idx) || idx < 0 || idx >= state.steps.length || idx === currentStepIndex) {
              return;
            }
            jumpToStep(idx);
          });
        });
      }

      render();
    </script>
  </body>
  </html>`;
}

function styles(): string {
  return `
    :root {
      color-scheme: dark;
      --bg: #0b1020;
      --card: #101833;
      --card2: #0d1530;
      --line: #2a3560;
      --text: #eef3ff;
      --muted: #9fb0d8;
      --accent: #7aa2ff;
      --yellow: #f6df6c;
      --yellowBg: rgba(246, 223, 108, 0.18);
      --greenBg: rgba(67, 200, 130, 0.22);
      --blueBg: rgba(90, 145, 255, 0.22);
      --amberBg: rgba(246, 180, 60, 0.18);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 8px 10px;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
      font-size: 12px;
      overflow-y: auto;
    }
    .card {
      background: linear-gradient(180deg, var(--card), var(--card2));
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 14px;
      margin-bottom: 7px;
      box-shadow: 0 4px 16px rgba(0,0,0,.15);
    }
    .title { font-size: 15px; font-weight: 700; }
    .sub, .subtle, .joinHint, label { color: var(--muted); font-size: 11px; }
    .truncatedHint { color: var(--accent); font-style: italic; }
    .blockContextLine { margin-top: 2px; }
    .topbar {
      display: flex;
      flex-direction: column;
      gap: 8px;
      /* Sticky — stays visible while the step content (especially long JOIN previews)
         scrolls beneath it.  top: 8px preserves the body padding gap so the bar never
         sits flush against the viewport edge. z-index: 20 ensures it always layers
         above table headers (z-index: 2) and every other positioned element. */
      position: sticky;
      top: 8px;
      z-index: 20;
      /* Reinforce the shadow so there is a clear visual plane separation between
         the sticky bar and the content scrolling underneath. */
      box-shadow: 0 6px 24px rgba(0, 0, 0, .45), 0 1px 0 rgba(255,255,255,.04);
    }
    .topbarInfo { min-width: 0; }
    .topbarControls { display: flex; align-items: center; gap: 8px; }
    .connLabel { color: var(--accent); font-weight: 600; }
    .blockSummary {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
      margin-bottom: 8px;
    }
    .blockBadge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      border: 1px solid #3e4c80;
    }
    .cteBadge {
      background: rgba(122,162,255,.16);
      color: #dbe5ff;
    }
    .subqueryBadge {
      background: rgba(246,196,60,.14);
      color: #f4d68a;
      border-color: rgba(246,196,60,.35);
    }
    .mainBadge {
      background: rgba(67,200,130,.16);
      color: #bff1d4;
      border-color: rgba(67,200,130,.35);
    }
    .blockName {
      font-size: 12px;
      font-weight: 700;
      color: var(--text);
    }
    .blockDeps {
      width: 100%;
      font-size: 10px;
      color: var(--muted);
    }
    .flow.card { display: none; }
    .groupedFlowShell {
      margin-bottom: 10px;
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 10px;
    }
    .flowBlock {
      display: inline-flex;
      flex-direction: column;
      align-items: flex-start;
      width: fit-content;
      max-width: 100%;
      padding: 10px 12px;
      background: rgba(24, 29, 68, 0.92);
      border: 1px solid rgba(77, 91, 164, 0.62);
      border-radius: 12px;
      box-shadow:
        inset 0 1px 0 rgba(255,255,255,.03),
        0 6px 18px rgba(0,0,0,.14);
    }
    .flowBlockCte {
      background: rgba(24, 29, 68, 0.92);
    }
    .flowBlockSubquery {
      background: rgba(24, 29, 68, 0.92);
    }
    .flowBlockMain {
      background: rgba(24, 29, 68, 0.92);
    }
    .flowBlockLabel {
      display: inline-flex;
      align-items: center;
      padding: 2px 8px;
      border-radius: 999px;
      border: none;
      background: transparent;
      font-size: 9px;
      font-weight: 700;
      letter-spacing: 0.07em;
      text-transform: uppercase;
      color: #dbe5ff;
      margin-bottom: 8px;
    }
    .activeBlock {
      background: rgba(34, 40, 88, 0.96);
      box-shadow:
        inset 0 1px 0 rgba(255,255,255,.04),
        0 10px 22px rgba(0,0,0,.2);
    }
    .activeBlock .flowBlockLabel {
      color: var(--text);
      background: transparent;
    }
    .flowBlockNodes {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }
    /* ── Database dropdown (topbar) ── */
    .dbDropWrap { display: flex; align-items: center; gap: 5px; }
    .dbDropLabel { font-size: 10px; color: var(--muted); white-space: nowrap; }
    .dbDropSelect {
      background: rgba(122,162,255,.10);
      border: 1px solid #3e5090;
      border-radius: 8px;
      color: var(--accent);
      font-size: 11px;
      font-weight: 600;
      padding: 3px 6px;
      cursor: pointer;
      max-width: 160px;
      /* Inherit the dark colour-scheme so the native dropdown matches the panel */
      color-scheme: dark;
    }
    .dbDropSelect:hover { background: rgba(122,162,255,.22); border-color: #5a78c8; }
    .dbDropSelect option { background: #0b1020; color: var(--text); }
    /* ── Switch-database panel (error state) ── */
    .switchDbPanel {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid rgba(255,255,255,.08);
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 8px;
    }
    .switchDbTitle {
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .switchDbRow { display: flex; gap: 6px; align-items: center; }
    .switchBtn {
      background: #1d2f60;
      border: 1px solid #4a60a0;
      border-radius: 8px;
      color: var(--text);
      font-size: 11px;
      padding: 4px 12px;
      font-weight: 500;
      cursor: pointer;
    }
    .switchBtn:hover { background: #253876; border-color: #5a72b8; }
    .switchNewBtn {
      background: rgba(122,162,255,.10);
      border: 1px solid #3e5090;
      border-radius: 8px;
      color: var(--muted);
      font-size: 11px;
      padding: 4px 10px;
      cursor: pointer;
    }
    .switchNewBtn:hover { background: rgba(122,162,255,.20); color: var(--text); border-color: #5a78c8; }
    .reconfigBtn {
      background: none;
      border: none;
      color: var(--muted);
      font-size: 10px;
      padding: 2px 0;
      cursor: pointer;
      text-decoration: underline;
      opacity: 0.55;
    }
    .reconfigBtn:hover { opacity: 1; }
    .nav { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
    button {
      background: #18254d;
      color: var(--text);
      border: 1px solid #344373;
      border-radius: 8px;
      padding: 4px 10px;
      font-size: 11px;
      cursor: pointer;
    }
    /* Nav Prev / Next are the primary controls — give them slightly more weight
       than generic buttons elsewhere in the panel. */
    .nav button {
      background: #1d2f60;
      border-color: #4a60a0;
      padding: 5px 14px;
      font-weight: 500;
      letter-spacing: 0.01em;
    }
    .nav button:not([disabled]):hover { background: #253876; border-color: #5a72b8; }
    button[disabled] { opacity: 0.4; cursor: default; }
    .stepBadge, .pill, .joinedBadge {
      border: 1px solid #3e4c80;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 11px;
      display: inline-flex;
      align-items: center;
      gap: 3px;
    }
    .flowNode {
      border: 1px solid #2e3a68;
      border-radius: 999px;
      padding: 3px 10px;
      font-size: 11px;
      display: inline-flex;
      align-items: center;
      cursor: pointer;
      user-select: none;
      transition: background 0.1s, opacity 0.1s;
      /* Non-active chips are secondary context — fade them back so they don't
         compete visually with the sticky Prev / Next controls above. */
      color: var(--muted);
      opacity: 0.7;
    }
    .flowNodeBtn {
      background: transparent;
      font: inherit;
      font-weight: 500;
    }
    .flowNode:hover { background: rgba(122,162,255,.10); opacity: 1; color: var(--text); }
    .flow { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .flowNode.active {
      background: rgba(122,162,255,.18);
      color: #dbe5ff;
      border-color: #5a78c8;
      opacity: 1;
    }
    .arrow { color: var(--muted); font-size: 11px; opacity: 0.5; }
    .groupedFlowShell .arrow {
      font-size: 0;
      opacity: 0.45;
    }
    .groupedFlowShell .arrow::before {
      content: '→';
      font-size: 11px;
    }
    .sqlBox {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 11px;
      background: rgba(0,0,0,.25);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 6px 10px;
      margin-top: 5px;
    }
    /* Hero variant — used when the SQL box is the first element in the card.
       Sized to ~3× the base monospace size (12px → 36px) so the SQL clause
       immediately dominates the visual hierarchy the moment a step is rendered. */
    .sqlBoxHero {
      font-size: 19px;
      font-weight: 700;
      line-height: 1.25;
      padding: 14px 16px;
      /* Left rail gives the eye an anchor point without adding colour noise */
      border-left: 4px solid rgba(90, 145, 255, 0.55);
      background: rgba(0,0,0,.35);
      letter-spacing: -0.01em;   /* tighten tracking at large size */
      white-space: pre-wrap;
      word-break: break-word;
      margin-bottom: 10px;       /* breathing room before the metrics row */
    }
    .sectionTitle { font-weight: 700; font-size: 12px; margin-bottom: 6px; display: flex; align-items: baseline; gap: 8px; }
    .stats {
      display: flex;
      gap: 7px;
      margin-bottom: 0;
    }
    /* When stats follow the SQL box they need top breathing room instead of bottom */
    .statsBelow { margin-top: 10px; }
    .sourceNote {
      margin-top: 7px;
      font-size: 10px;
      color: var(--muted);
    }
    .stat {
      flex: 1;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 7px 10px;
      background: rgba(8, 16, 40, 0.45);
    }
    .stat strong { display: block; font-size: 18px; font-weight: 700; margin-top: 2px; line-height: 1.2; }
    /* Step explanation block */
    .expBlock { margin: 5px 0 4px; }
    .expWhat  { font-size: 12px; line-height: 1.5; color: var(--text); }
    .expImpact { font-size: 11px; color: var(--muted); line-height: 1.4; margin-top: 3px; }
    .sqlLabel { font-size: 10px; color: var(--muted); margin-top: 6px; margin-bottom: 1px; display: block; letter-spacing: 0.02em; text-transform: uppercase; }
    .nullValue {
      color: #8b93a7;
      font-weight: 600;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      letter-spacing: 0.01em;
    }
    /* Row-delta colour coding */
    .deltaPos { color: #43c882; }
    .deltaNeg { color: #ff7070; }
    .deltaZero { color: var(--muted); }
    .pillWrap { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; margin-top: 4px; }
    .joinSwap { color: var(--muted); }
    /* yellow = join-key (clickable cells in preview tables only) */
    .joinKey, .joinCell { background: var(--yellowBg); }
    /* joinedColCell / dupeColCell / joinKeyColCell: classes kept for any residual references,
       but no column-wide background tints — the header badge carries the signal. */
    .dupeBadge { background: var(--amberBg) !important; border-color: rgba(246,180,60,.5) !important; color: #f6c43c !important; }
    .joinPreviewGrid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 130px minmax(0, 1fr);
      gap: 10px;
      align-items: start;
      margin-top: 8px;
    }
    .previewPane {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px;
      min-width: 0;
    }
    .previewHeader {
      display: flex;
      align-items: center;
      gap: 6px;
      justify-content: space-between;
      margin-bottom: 4px;
      font-weight: 600;
      font-size: 11px;
    }
    .previewPaneName { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
    .paneRowCount { font-size: 10px; font-weight: 400; color: var(--muted); white-space: nowrap; flex-shrink: 0; }
    /* Filter status bar — hidden by default, shown when pane is in filtered state */
    .paneFilterBar {
      display: none;
      align-items: center;
      gap: 6px;
      margin-bottom: 5px;
      min-height: 20px;
    }
    .paneFilterBar.visible { display: flex; }
    .paneFilterLabel {
      font-size: 10px;
      color: var(--accent);
      font-style: italic;
      flex: 1;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .backToFullBtn {
      flex-shrink: 0;
      font-size: 10px;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(122,162,255,.10);
      color: var(--accent);
      cursor: pointer;
      white-space: nowrap;
    }
    .backToFullBtn:hover { background: rgba(122,162,255,.22); }
    .noMatchCell { text-align: center; opacity: 0.45; font-style: italic; padding: 12px 0 !important; }
    .joinBridge {
      text-align: center;
      color: var(--muted);
      border: 1px solid #33437a;
      border-radius: 999px;
      padding: 10px 8px;
      font-size: 10px;
      align-self: center;
    }
    .joinBridge span { color: var(--text); font-size: 10px; }
    .joinBridgeRel {
      display: inline-block;
      margin-top: 5px;
      color: var(--muted) !important;
      font-size: 9px !important;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }
    .joinBridgeTypeHelp {
      display: inline-block;
      margin-top: 5px;
      color: var(--muted);
      font-size: 9px;
      line-height: 1.35;
      max-width: 160px;
    }
    .joinHint { font-size: 10px; color: var(--muted); margin-bottom: 4px; }
    .tableWrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
    }
    /* Preview tables: fixed height, scroll both axes; table expands to content width */
    .previewWrap { max-height: 160px; overflow-x: auto; overflow-y: auto; }
    .previewWrap table { width: max-content; }
    /* Intermediate result: fixed height ~10 rows, scroll both axes */
    .resultWrap { max-height: 260px; overflow-x: auto; overflow-y: auto; }
    .resultWrap table { width: max-content; min-width: 100%; }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 480px;
    }
    th, td {
      padding: 5px 9px;
      border-bottom: 1px solid rgba(65, 83, 136, 0.45);
      text-align: left;
      white-space: nowrap;
      font-size: 11px;
    }
    th {
      position: sticky;
      top: 0;
      /* Solid base colour — must never be transparent.
         When a subclass overrides this with an rgba tint it must layer the tint
         ON TOP of a solid base (see class rules below) so that the sticky header
         remains fully opaque while rows scroll beneath it. */
      background: #182142;
      z-index: 2;
      font-size: 11px;
    }
    .joinedBadge {
      margin-left: 4px;
      font-size: 9px;
      padding: 1px 5px;
      background: rgba(122,162,255,.16);
      border-radius: 999px;
      border: 1px solid #3e4c80;
    }
    .windowColHead {
      cursor: pointer;
    }
    .windowSelectableHead {
      background: linear-gradient(rgba(122,162,255,.16), rgba(122,162,255,.16)) #182142;
      color: #dbe5ff;
    }
    .windowHeaderCell:hover {
      background: linear-gradient(rgba(122,162,255,.22), rgba(122,162,255,.22)) #182142;
    }
    .activeWindowHeader {
      background: linear-gradient(rgba(122,162,255,.28), rgba(122,162,255,.28)) #182142;
      color: #dbe5ff;
    }
    .windowDetailCard {
      margin-top: 0;
    }
    .windowSummaryLine {
      font-size: 12px;
      color: var(--text);
      line-height: 1.45;
      margin-bottom: 6px;
    }
    .windowMetaLine {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 8px;
    }
    .windowHint {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .distinctPanelSlot {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 8px;
      margin: -2px 0 10px;
    }
    .distinctBadge {
      padding: 3px 10px;
      border-radius: 999px;
      border: 1px solid rgba(67,200,130,.35);
      background: rgba(67,200,130,.12);
      color: #bff1d4;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .distinctBadge:hover {
      background: rgba(67,200,130,.18);
      border-color: rgba(67,200,130,.5);
    }
    .distinctBadge.active {
      background: rgba(67,200,130,.22);
      border-color: rgba(67,200,130,.58);
    }
    .distinctDetailBlock {
      width: 100%;
    }
    .distinctDetailSentence {
      font-size: 11px;
      color: var(--text);
      margin-bottom: 8px;
      line-height: 1.45;
    }
    .caseCell {
      cursor: pointer;
      box-shadow: inset 0 0 0 1px rgba(90,145,255,.12);
    }
    .caseCell:hover {
      background: rgba(122,162,255,.10);
    }
    td.activeCaseCell {
      background: rgba(122,162,255,.18);
      box-shadow: inset 0 0 0 1px rgba(122,162,255,.42);
    }
    .caseDetailCard {
      margin-top: 0;
    }
    .caseDetailSentence {
      font-size: 11px;
      color: var(--text);
      line-height: 1.45;
      margin-bottom: 8px;
    }
    .caseDetailGrid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .caseDetailBlock {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: rgba(8, 16, 40, 0.45);
      padding: 10px 12px;
    }
    .caseDetailLabel {
      font-size: 9px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 6px;
    }
    .caseInputList {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .caseInputItem {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      font-size: 11px;
    }
    .caseInputName {
      color: var(--muted);
    }
    .caseInputValue, .caseRuleValue, .caseReturnedValue {
      color: var(--text);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      word-break: break-word;
    }
    .caseInputEmpty {
      font-size: 11px;
      color: var(--muted);
      line-height: 1.4;
    }
    .windowPartitionBreak td {
      border-top: 3px solid rgba(122,162,255,.45);
    }
    .windowPartitionTint0 td { background: rgba(122,162,255,.14); }
    .windowPartitionTint1 td { background: rgba(67,200,130,.14); }
    .windowPartitionTint2 td { background: rgba(246,223,108,.14); }
    .windowPartitionTint3 td { background: rgba(180,120,255,.14); }
    .windowPartitionTint4 td { background: rgba(90,145,255,.12); }
    /* Join-key cells: yellow tint + inset border to mark the key column */
    .joinCell { box-shadow: inset 0 0 0 1px rgba(246,223,108,.3); }
    /* Entire preview rows are clickable */
    tr[data-side] { cursor: pointer; }
    tr[data-side]:hover td { background: rgba(255,255,255,.04); }
    tr[data-side]:hover .joinCell { background: rgba(246,223,108,.20); }
    tr.selectedRow td { background: var(--greenBg) !important; }
    .yellowDot {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--yellow);
      vertical-align: middle;
      margin: 0 3px;
    }
    .empty, .error {
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: var(--card);
    }
    .error { color: #ffb8b8; }
    /* GROUP BY clickable rows */
    .groupRow { transition: background 0.08s; }
    .groupRow.activeGroupRow { background: rgba(80,200,180,.15); outline: 1px solid rgba(80,200,180,.35); outline-offset: -1px; }
    .groupByClickableCell { cursor: pointer; }
    .groupByClickableCell:hover { background: rgba(80,200,180,.13); }
    /* Hint above the grouped table — mirrors .joinHint so it's equally prominent */
    .groupClickHint {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 6px;
      display: flex;
      align-items: center;
    }
    .groupDot {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: #5dcfbe;
      vertical-align: middle;
      margin: 0 3px;
      flex-shrink: 0;
    }
    /* Group Breakdown panel */
    .groupBreakdownCard { margin-top: 0; } /* gap already provided by .card margin-bottom */
    .groupBreakdownTitle {
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.03em;
      color: #5dcfbe;
      margin-bottom: 3px;
    }
    .groupBreakdownSub {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 8px;
    }
    .breakdownWrap { max-height: 220px; overflow-x: auto; overflow-y: auto; }
    .breakdownWrap table { width: max-content; min-width: 100%; }
    /* Aggregation source column in GROUP BREAKDOWN table.
       Violet matches the existing .aggBadge colour so the badge and the column
       tint share the same visual language — "this feeds the aggregation". */
    .bdAggSrcHead {
      background: linear-gradient(rgba(180,120,255,.12), rgba(180,120,255,.12)) #182142;
      color: #c8a0ff;
    }
    .bdAggSrcCell { background: rgba(180,120,255,.06); }
    /* GROUP BY key-column highlight — teal, visually distinct from yellow (sort/join-key) */
    .groupByColHead {
      background: linear-gradient(rgba(80,200,180,.12), rgba(80,200,180,.12)) #182142;
      color: #5dcfbe;
    }
    .groupByColCell { background: rgba(80,200,180,.07); }
    /* Aggregation function badge — soft violet, distinct from join/dupe/sort indicators */
    .aggBadge {
      display: inline-flex;
      align-items: center;
      margin-left: 4px;
      padding: 1px 5px;
      font-size: 9px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      border-radius: 999px;
      border: 1px solid rgba(180,120,255,.40);
      background: rgba(180,120,255,.14);
      color: #c8a0ff;
      letter-spacing: 0.01em;
    }
    /* Compact "Aggregations: COUNT(x), SUM(y)" hint line in GROUP BY step */
    .aggHint {
      font-size: 10px;
      color: var(--muted);
      margin-bottom: 6px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .aggHintLabel {
      font-family: Inter, ui-sans-serif, system-ui, sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-size: 9px;
      color: var(--muted);
      margin-right: 5px;
      opacity: 0.7;
    }
    /* ORDER BY sort-key column highlight — same yellow language as WHERE/HAVING filter columns */
    .sortColHead {
      /* Opaque composite background: solid base + transparent yellow overlay avoids
         header text bleeding when table rows scroll beneath a sticky <th>. */
      background: linear-gradient(rgba(246,223,108,.14), rgba(246,223,108,.14)) #182142;
      color: #c9b840;
    }
    .sortColCell { background: rgba(246,223,108,.07); }
    /* Filtered Rows View — WHERE and HAVING steps pre/post filter visualisation */
    .filteredViewBlock { margin: 6px 0 4px; }
    .filteredViewBlock .sectionTitle { margin-bottom: 4px; }
    .filteredViewMetaNeg { color: #ff7070; }
    .filteredViewMetaPos { color: #43c882; }
    .filteredWrap { max-height: 180px; overflow-x: auto; overflow-y: auto; }
    .filteredWrap table { width: max-content; min-width: 100%; }
    .whereSubqueryBlock { margin-top: 10px; }
    .whereSubqueryExplanation {
      font-size: 12px;
      color: var(--text);
      line-height: 1.45;
      margin-bottom: 8px;
    }
    .whereScalarCard {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: rgba(8, 16, 40, 0.45);
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 4px;
      margin-bottom: 8px;
    }
    .whereScalarLabel {
      font-size: 10px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .whereScalarValue {
      font-size: 22px;
      font-weight: 700;
      line-height: 1.2;
      color: var(--text);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    .whereScalarMeta {
      font-size: 10px;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    .subqueryWrap { max-height: 180px; overflow-x: auto; overflow-y: auto; margin-bottom: 8px; }
    .subqueryWrap table { width: max-content; min-width: 100%; }
    @media (max-width: 900px) {
      .joinPreviewGrid { grid-template-columns: 1fr; }
      .stats { flex-wrap: wrap; }
      .caseDetailGrid { grid-template-columns: 1fr; }
    }
  `;
}
