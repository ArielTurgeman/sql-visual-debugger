const Module = require('node:module');
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
  });
};
