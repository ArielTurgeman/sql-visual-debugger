const Module = require('node:module');
const fs = require('node:fs');
const { executeDebugSteps } = require('../out/engine/stepEngine');

function loadPanelModule() {
  const originalLoad = Module._load;
  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === 'vscode') {
      return {
        ViewColumn: { Beside: 2 },
        window: {
          createWebviewPanel() {
            throw new Error('createWebviewPanel should not be called in this unit test.');
          },
        },
      };
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  try {
    delete require.cache[require.resolve('../out/webview/panel')];
    return require('../out/webview/panel');
  } finally {
    Module._load = originalLoad;
  }
}

class FakeRunner {
  constructor(handlers) {
    this.handlers = handlers;
  }

  async query(sql) {
    for (const handler of this.handlers) {
      if (handler.match(sql)) {
        return handler.rows.map(row => ({ ...row }));
      }
    }
    throw new Error(`No fake query handler matched SQL: ${sql}`);
  }
}

function normalizeSql(sql) {
  return sql.replace(/\s+/g, ' ').trim().toLowerCase();
}

function sqlIncludes(fragment) {
  const normalizedFragment = normalizeSql(fragment);
  return {
    match(sql) {
      return normalizeSql(sql).includes(normalizedFragment);
    },
  };
}

module.exports = function runWebviewPanelTests(runTest, assert) {
  runTest('simple queries render step roadmap buttons in the webview HTML', async () => {
    const { sendResult } = loadPanelModule();
    const panel = { webview: { html: '' } };
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, name: 'Ada', active: 1 },
          { id: 2, name: 'Linus', active: 0 },
        ],
      },
      {
        ...sqlIncludes('select id, name from users'),
        rows: [
          { id: 1, name: 'Ada' },
          { id: 2, name: 'Linus' },
        ],
      },
    ]);

    const steps = await executeDebugSteps('SELECT id, name FROM users', runner);
    sendResult(panel, 'SELECT id, name FROM users', 'simple.sql', 'demo@localhost', steps, 'demo', ['demo']);

    assert.match(panel.webview.html, /MAIN QUERY/);
    assert.match(panel.webview.html, /flowNodeBtn/);
    assert.match(panel.webview.html, /data-step-index="\$\{idx\}"/);
    assert.match(panel.webview.html, /"name":"FROM"/);
    assert.match(panel.webview.html, /"name":"SELECT"/);
  });

  runTest('cte queries still render grouped roadmap buttons in the webview HTML', async () => {
    const { sendResult } = loadPanelModule();
    const compiledPanelSource = fs.readFileSync(require.resolve('../out/webview/panel'), 'utf8');
    const panel = { webview: { html: '' } };
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 5, team_id: 10, active: 0 },
          { id: 12, team_id: 10, active: 1 },
          { id: 20, team_id: 20, active: 1 },
        ],
      },
      {
        ...sqlIncludes('select `users`.`id`, `users`.`team_id`, `users`.`active` from users where active = 1'),
        rows: [
          { id: 12, team_id: 10, active: 1 },
          { id: 20, team_id: 20, active: 1 },
        ],
      },
      {
        ...sqlIncludes('select id, team_id from users where active = 1'),
        rows: [
          { id: 12, team_id: 10 },
          { id: 20, team_id: 20 },
        ],
      },
      {
        ...sqlIncludes('with `active_users` as (select id, team_id from users where active = 1) select `active_users`.* from active_users'),
        rows: [
          { id: 12, team_id: 10 },
          { id: 20, team_id: 20 },
        ],
      },
      {
        ...sqlIncludes('with `active_users` as (select id, team_id from users where active = 1) select id from active_users'),
        rows: [
          { id: 12 },
          { id: 20 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'WITH active_users AS (SELECT id, team_id FROM users WHERE active = 1) SELECT id FROM active_users',
      runner,
    );
    sendResult(panel, 'WITH active_users AS (SELECT id, team_id FROM users WHERE active = 1) SELECT id FROM active_users', 'cte.sql', 'demo@localhost', steps, 'demo', ['demo']);

    assert.match(panel.webview.html, /flowNodeBtn/);
    assert.match(panel.webview.html, /"blockType":"cte"/);
    assert.match(panel.webview.html, /"blockName":"active_users"/);
    assert.match(panel.webview.html, /"name":"WHERE"/);
    assert.match(compiledPanelSource, /flowBlock flowBlock\\\$\{group\.blockType === 'cte' \? 'Cte' : group\.blockType === 'subquery' \? 'Subquery' : 'Main'\}/);
    const renderBlockSummaryStart = compiledPanelSource.indexOf('function renderBlockSummary(step)');
    const renderFlowBlocksStart = compiledPanelSource.indexOf('function renderFlowBlocks()');
    const renderBlockSummarySource = compiledPanelSource.slice(renderBlockSummaryStart, renderFlowBlocksStart);
    assert.equal(renderBlockSummaryStart >= 0, true);
    assert.equal(renderFlowBlocksStart > renderBlockSummaryStart, true);
    assert.doesNotMatch(renderBlockSummarySource, /const countLabel = matching\.length > breakdownRows\.length/);
    assert.match(compiledPanelSource, /\.flowBlockCte/);
    assert.match(compiledPanelSource, /\.flowBlockSubquery/);
    assert.match(compiledPanelSource, /\.flowBlockMain/);
    assert.equal(compiledPanelSource.includes('align-items: flex-start;'), true);
    assert.equal(compiledPanelSource.includes('width: fit-content;'), true);
  });

  runTest('window detail rendering prefers visible intermediate rows and falls back to engine preview rows when needed', () => {
    const { sendResult } = loadPanelModule();
    const compiledPanelSource = fs.readFileSync(require.resolve('../out/webview/panel'), 'utf8');
    const panel = { webview: { html: '' } };
    sendResult(
      panel,
      'SELECT PlayerId, ROW_NUMBER() OVER (PARTITION BY TeamId ORDER BY Age DESC) AS rn FROM playerinfo',
      'window.sql',
      'demo@localhost',
      [
        {
          name: 'SELECT',
          title: 'SELECT',
          explanation: 'test',
          sqlFragment: 'SELECT PlayerId, ROW_NUMBER() OVER (PARTITION BY TeamId ORDER BY Age DESC) AS rn',
          rowsBefore: 2,
          rowsAfter: 2,
          data: [
            { PlayerId: 69, rn: 1 },
            { PlayerId: 74, rn: 2 },
          ],
          columns: ['PlayerId', 'rn'],
          windowColumns: [
            {
              outputColumn: 'rn',
              expression: 'ROW_NUMBER() OVER (PARTITION BY TeamId ORDER BY Age DESC) AS rn',
              functionName: 'ROW_NUMBER',
              partitionBy: ['TeamId'],
              orderBy: ['Age DESC'],
              orderByTerms: [{ column: 'Age', direction: 'DESC' }],
              explanation: 'test',
              howComputed: ['test'],
              previewColumns: ['TeamId', 'Age', 'rn'],
              previewRows: [
                { TeamId: 10, Age: 33, rn: 1 },
                { TeamId: 10, Age: 31, rn: 2 },
              ],
            },
          ],
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(compiledPanelSource, /const canUseVisibleRows = previewColumns\.every/);
    assert.match(compiledPanelSource, /const previewSourceRows = canUseVisibleRows/);
    assert.match(compiledPanelSource, /\? \(step\.data \|\| \[\]\)/);
    assert.match(compiledPanelSource, /: \(\(meta\.previewRows \|\| \[\]\)\.length > 0 \? meta\.previewRows : \(step\.data \|\| \[\]\)\)/);
    assert.match(compiledPanelSource, /Preview follows the \\\$\{formatNumber\(step\.data\.length\)\} visible row\(s\) shown above\./);
    assert.match(compiledPanelSource, /Preview includes supporting partition\/order columns that are not visible in the intermediate result\./);
    assert.match(compiledPanelSource, /Click a highlighted window-function column name to see how it was computed\./);
  });

  runTest('distinct detail groups rows with typed fingerprints so NULL and empty string stay distinct', () => {
    const { sendResult } = loadPanelModule();
    const compiledPanelSource = fs.readFileSync(require.resolve('../out/webview/panel'), 'utf8');
    const panel = { webview: { html: '' } };

    sendResult(
      panel,
      'SELECT DISTINCT code FROM qa_code_values ORDER BY code',
      'distinct-null.sql',
      'demo@localhost',
      [
        {
          name: 'SELECT',
          title: 'SELECT',
          explanation: 'test',
          sqlFragment: 'SELECT DISTINCT code',
          rowsBefore: 4,
          rowsAfter: 3,
          data: [
            { code: null },
            { code: '' },
            { code: 'A' },
          ],
          columns: ['code'],
          distinctMeta: {
            columns: ['code'],
            rows: [
              { code: null },
              { code: '' },
              { code: 'A' },
              { code: '' },
            ],
          },
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(compiledPanelSource, /function buildTypedRowFingerprint\(row, columns\)/);
    assert.match(compiledPanelSource, /if \(value === null\) return 'null';/);
    assert.match(compiledPanelSource, /if \(typeof value === 'string'\) return \\`string:\\\$\{value\}\\`;/);
    assert.match(compiledPanelSource, /const groupedRows = Array\.from\(groupMap\.values\(\)\);/);
    assert.doesNotMatch(compiledPanelSource, /const fingerprint = cols\.map\(c => String\(row\[c\] \?\? ''\)\)\.join/);
  });

  runTest('case detail rendering advertises the clickable CASE result cells', () => {
    const { sendResult } = loadPanelModule();
    const compiledPanelSource = fs.readFileSync(require.resolve('../out/webview/panel'), 'utf8');
    const panel = { webview: { html: '' } };
    sendResult(
      panel,
      "SELECT CASE WHEN score >= 10 THEN 'high' ELSE 'low' END AS band FROM scores",
      'case.sql',
      'demo@localhost',
      [
        {
          name: 'SELECT',
          title: 'SELECT',
          explanation: 'test',
          sqlFragment: "SELECT CASE WHEN score >= 10 THEN 'high' ELSE 'low' END AS band",
          rowsBefore: 2,
          rowsAfter: 2,
          data: [
            { band: 'high' },
            { band: 'low' },
          ],
          columns: ['band'],
          caseColumns: [
            {
              outputColumn: 'band',
              rowExplanations: [
                {
                  inputValues: [{ column: 'score', value: 12 }],
                  matchedRule: 'score >= 10',
                  returnedValue: 'high',
                },
                {
                  inputValues: [{ column: 'score', value: 6 }],
                  matchedRule: 'ELSE',
                  returnedValue: 'low',
                },
              ],
            },
          ],
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(compiledPanelSource, /Click a highlighted CASE WHEN result cell to see why that value was returned\./);
  });

  runTest('joined ORDER BY columns keep sort highlight classes in the rendered table logic', () => {
    const { sendResult } = loadPanelModule();
    const panel = { webview: { html: '' } };
    sendResult(
      panel,
      'SELECT p.name, tp.made, tp.attempted FROM playerinfo p RIGHT JOIN threes tp ON p.PlayerId = tp.PlayerId ORDER BY tp.attempted DESC, tp.made DESC',
      'orderby-join.sql',
      'demo@localhost',
      [
        {
          name: 'ORDER BY',
          title: 'ORDER BY',
          explanation: 'test',
          sqlFragment: 'ORDER BY tp.attempted DESC, tp.made DESC',
          rowsBefore: 2,
          rowsAfter: 2,
          data: [
            { PlayerName: 'A', Made: 2.4, Attempted: 8.6 },
            { PlayerName: 'B', Made: 2.0, Attempted: 7.2 },
          ],
          columns: ['PlayerName', 'Made', 'Attempted'],
          schemaContext: { joinIndicatorColumns: ['Made', 'Attempted'] },
          sortColumns: ['Made', 'Attempted'],
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(panel.webview.html, /isSort \? 'sortColHead' : ''/);
    assert.match(panel.webview.html, /!isDupe && isJoined \? 'joinedColHead' : ''/);
    assert.match(panel.webview.html, /isSort \? 'sortColCell' : ''/);
    assert.match(panel.webview.html, /!isDupe && isJoined \? 'joinedColCell' : ''/);
    assert.doesNotMatch(panel.webview.html, /const cls = isDupe\s+\? 'joinedColHead dupeHead'[\s\S]*?: isSort\s+\? 'sortColHead'/);
  });

  runTest('join UI uses the specific join type in roadmap and preview text', () => {
    const { sendResult } = loadPanelModule();
    const compiledPanelSource = fs.readFileSync(require.resolve('../out/webview/panel'), 'utf8');
    const panel = { webview: { html: '' } };
    sendResult(
      panel,
      'SELECT city.Name, countrylanguage.Language FROM city LEFT JOIN countrylanguage ON countrylanguage.CountryCode = city.CountryCode',
      'left-join.sql',
      'demo@localhost',
      [
        {
          name: 'JOIN',
          title: 'LEFT JOIN',
          explanation: 'test',
          sqlFragment: 'LEFT JOIN countrylanguage ON countrylanguage.CountryCode = city.CountryCode',
          rowsBefore: 2,
          rowsAfter: 3,
          data: [
            { Name: 'A', Language: 'X' },
            { Name: 'B', Language: null },
          ],
          columns: ['Name', 'Language'],
          joinMeta: {
            leftTable: 'city',
            rightTable: 'countrylanguage',
            leftKey: 'countrylanguage.CountryCode',
            rightKey: 'city.CountryCode',
            leftKeyCol: 'CountryCode',
            rightKeyCol: 'CountryCode',
            joinType: 'LEFT JOIN',
            leftRows: [{ CountryCode: 'A' }],
            rightRows: [{ CountryCode: 'A', Language: 'X' }],
            allLeftRows: [{ CountryCode: 'A' }, { CountryCode: 'B' }],
            allRightRows: [{ CountryCode: 'A', Language: 'X' }],
            leftColumns: ['CountryCode'],
            rightColumns: ['CountryCode', 'Language'],
            relationship: 'many-to-many',
            rowDelta: 1,
            joinedResultColumns: ['Name', 'Language'],
            joinIndicatorColumns: ['Language'],
          },
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(panel.webview.html, /const joinTypeLabel = jm\.joinType \|\| step\.title \|\| 'JOIN';/);
    assert.match(panel.webview.html, /describeJoinType\(jm\.joinType, jm\.leftTable, jm\.rightTable\)/);
    assert.match(panel.webview.html, /LEFT JOIN'/);
    assert.match(panel.webview.html, /function describeJoinType\(joinType, leftTable, rightTable\)/);
    assert.match(compiledPanelSource, /function getJoinComparableKey\(value\)/);
    assert.match(compiledPanelSource, /const matches  = joinIsNull \|\| joinComparableKey === null/);
    assert.match(compiledPanelSource, /getJoinComparableKey\(r\[joinKey\]\) === joinComparableKey/);
  });

  runTest('filtered view summary uses actual WHERE totals instead of the capped preview length', () => {
    const { sendResult } = loadPanelModule();
    const panel = { webview: { html: '' } };

    sendResult(
      panel,
      'SELECT * FROM city WHERE Population > 50000',
      'world.sql',
      'demo@localhost',
      [
        {
          name: 'WHERE',
          title: 'WHERE',
          explanation: 'test',
          impact: 'test',
          sqlFragment: 'WHERE Population > 50000',
          rowsBefore: 4079,
          rowsAfter: 4001,
          data: Array.from({ length: 200 }, (_, idx) => ({
            ID: idx + 1,
            Name: `City ${idx + 1}`,
            Population: 60000 + idx,
          })),
          columns: ['ID', 'Name', 'Population'],
          preFilterRows: Array.from({ length: 200 }, (_, idx) => ({
            ID: idx + 1,
            Name: `City ${idx + 1}`,
            Population: idx < 5 ? 1000 + idx : 60000 + idx,
          })),
          preFilterColumns: ['ID', 'Name', 'Population'],
          whereColumns: ['Population'],
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(
      panel.webview.html,
      /const removedCount = Math\.max\(0, \(step\.rowsBefore \|\| 0\) - \(step\.rowsAfter \|\| 0\)\);/,
    );
    assert.match(
      panel.webview.html,
      /const previewCount = step\.preFilterRows\.length;/,
    );
    assert.match(
      panel.webview.html,
      /const totalBefore = step\.rowsBefore \|\| previewCount;/,
    );
    assert.doesNotMatch(
      panel.webview.html,
      /const removedCount = \(step\.preFilterRows\.length\) - \(step\.rowsAfter \|\| 0\);/,
    );
    assert.match(
      panel.webview.html,
      /<div class="sectionTitle">Filtered \$\{noun\} preview <span class="subtle">/,
    );
    assert.match(
      panel.webview.html,
      /removed total|none removed/,
    );
    assert.match(
      panel.webview.html,
      /display limit reached/,
    );
  });

  runTest('group breakdown filters by group keys, caps rows, and auto-scrolls aggregation columns', () => {
    const { sendResult } = loadPanelModule();
    const compiledPanelSource = fs.readFileSync(require.resolve('../out/webview/panel'), 'utf8');
    const panel = { webview: { html: '' } };
    sendResult(
      panel,
      'SELECT CountryCode, AVG(Population) AS hopa FROM city GROUP BY CountryCode',
      'groupby.sql',
      'demo@localhost',
      [
        {
          name: 'GROUP BY',
          title: 'GROUP BY',
          explanation: 'test',
          sqlFragment: 'GROUP BY CountryCode',
          rowsBefore: 4079,
          rowsAfter: 232,
          data: [
            { CountryCode: 'ABW', hopa: 29034 },
            { CountryCode: 'AFG', hopa: 583025 },
          ],
          columns: ['CountryCode', 'hopa'],
          groupByColumns: ['CountryCode'],
          aggColumns: [{ col: 'hopa', fn: 'AVG', srcCol: 'Population' }],
          aggSummary: 'AVG(Population)',
          preGroupRows: Array.from({ length: 500 }, (_, idx) => ({
            ID: idx + 1,
            CountryCode: idx < 250 ? 'ABW' : 'AFG',
            Population: 1000 + idx,
          })),
          preGroupColumns: ['ID', 'CountryCode', 'Population'],
        },
      ],
      'demo',
      ['demo'],
    );

    assert.match(panel.webview.html, /breakdownLimit = 200/);
    assert.match(panel.webview.html, /const breakdownRows = matching\.slice\(0, breakdownLimit\);/);
    assert.match(panel.webview.html, /groupBreakdownTableWrap/);
    assert.match(panel.webview.html, /autoScrollGroupBreakdown\(step\)/);
    assert.ok(compiledPanelSource.includes('bdAggSrcHead" data-column-name="\\${escapeAttr(c)}"'));
    assert.ok(compiledPanelSource.includes('<th data-column-name="\\${escapeAttr(c)}">\\${escapeHtml(c)}</th>'));
    assert.ok(compiledPanelSource.includes('resolveColumnReference(preCols, a.srcCol)'));
    assert.ok(compiledPanelSource.includes("aggFns.map(fn => \\`<span class=\"aggBadge\">\\${escapeHtml(fn)}</span>\\`).join('')"));
    assert.match(panel.webview.html, /step\.name === 'GROUP BY' \? \[/);
    assert.match(panel.webview.html, /step\.aggColumns \|\| \[\]\)\.map\(\(agg\) => agg\.col\)/);
  });
};
