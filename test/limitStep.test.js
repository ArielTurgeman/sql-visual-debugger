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

module.exports = function runLimitStepTests(runTest, assert) {
  runTest('LIMIT keeps only the first N rows and reduces the final row count', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, name: 'Ada' },
          { id: 2, name: 'Linus' },
          { id: 3, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select id, name from users limit 2'),
        rows: [
          { id: 1, name: 'Ada' },
          { id: 2, name: 'Linus' },
        ],
      },
      {
        ...sqlIncludes('select id, name from users'),
        rows: [
          { id: 1, name: 'Ada' },
          { id: 2, name: 'Linus' },
          { id: 3, name: 'Grace' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT id, name FROM users LIMIT 2',
      runner,
    );

    const limitStep = steps.find(step => step.name === 'LIMIT');
    if (!limitStep) {
      throw new Error('Expected a LIMIT step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'SELECT', 'LIMIT']);
    assert.equal(limitStep.rowsBefore, 3);
    assert.equal(limitStep.rowsAfter, 2);
    assert.equal(limitStep.sqlFragment, 'LIMIT 2');
    assert.deepEqual(limitStep.columns, ['id', 'name']);
    assert.deepEqual(limitStep.data, [
      { id: 1, name: 'Ada' },
      { id: 2, name: 'Linus' },
    ]);
    assert.match(limitStep.explanation, /Kept only the first 2 rows/i);
    assert.match(limitStep.impact, /1 row beyond the limit was cut/i);
  });
};
