const { executeDebugSteps } = require('../out/engine/stepEngine');

class FakeRunner {
  constructor(handlers) {
    this.handlers = handlers;
    this.queries = [];
  }

  async query(sql) {
    this.queries.push(sql);
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

module.exports = function runFromStepTests(runTest, assert) {
  runTest('FROM loads the base rows and columns for the query pipeline', async () => {
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
    const fromStep = steps.find(step => step.name === 'FROM');

    if (!fromStep) {
      throw new Error('Expected a FROM step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'SELECT']);
    assert.equal(fromStep.rowsBefore, 0);
    assert.equal(fromStep.rowsAfter, 2);
    assert.equal(fromStep.sqlFragment, 'FROM users');
    assert.deepEqual(fromStep.columns, ['id', 'name', 'active']);
    assert.deepEqual(fromStep.data, [
      { id: 1, name: 'Ada', active: 1 },
      { id: 2, name: 'Linus', active: 0 },
    ]);
    assert.match(fromStep.explanation, /Loaded 2 rows from `users`/);
    assert.match(fromStep.impact, /2 rows are available for subsequent steps/);
  });
};
