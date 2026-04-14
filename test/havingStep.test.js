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

module.exports = function runHavingStepTests(runTest, assert) {
  runTest('HAVING filters grouped rows and keeps pre-filter grouped data', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `orders`.* from orders'),
        rows: [
          { team_id: 10, amount: 50 },
          { team_id: 10, amount: 70 },
          { team_id: 20, amount: 30 },
        ],
      },
      {
        ...sqlIncludes('having total_amount >= 100'),
        rows: [
          { team_id: 10, total_amount: 120 },
        ],
      },
      {
        ...sqlIncludes('select team_id, sum(amount) as total_amount from orders group by team_id having total_amount >= 100'),
        rows: [
          { team_id: 10, total_amount: 120 },
        ],
      },
      {
        ...sqlIncludes('group by team_id'),
        rows: [
          { team_id: 10, total_amount: 120 },
          { team_id: 20, total_amount: 30 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT team_id, SUM(amount) AS total_amount FROM orders GROUP BY team_id HAVING total_amount >= 100',
      runner,
    );

    const havingStep = steps.find(step => step.name === 'HAVING');
    if (!havingStep) {
      throw new Error('Expected a HAVING step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'GROUP BY', 'HAVING', 'SELECT']);
    assert.equal(havingStep.rowsBefore, 2);
    assert.equal(havingStep.rowsAfter, 1);
    assert.equal(havingStep.sqlFragment, 'HAVING total_amount >= 100');
    assert.deepEqual(havingStep.columns, ['team_id', 'total_amount']);
    assert.deepEqual(havingStep.whereColumns, ['total_amount']);
    assert.deepEqual(havingStep.preFilterColumns, ['team_id', 'total_amount']);
    assert.deepEqual(havingStep.preFilterRows, [
      { team_id: 10, total_amount: 120 },
      { team_id: 20, total_amount: 30 },
    ]);
    assert.deepEqual(havingStep.data, [
      { team_id: 10, total_amount: 120 },
    ]);
  });

  runTest('HAVING detects aggregate expressions and highlights the matching SELECT alias', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `orders`.* from orders'),
        rows: [
          { team_id: 10, amount: 50 },
          { team_id: 10, amount: 70 },
          { team_id: 20, amount: 30 },
        ],
      },
      {
        ...sqlIncludes('having sum(amount) >= 100'),
        rows: [
          { team_id: 10, total_amount: 120 },
        ],
      },
      {
        ...sqlIncludes('select team_id, sum(amount) as total_amount from orders group by team_id having sum(amount) >= 100'),
        rows: [
          { team_id: 10, total_amount: 120 },
        ],
      },
      {
        ...sqlIncludes('group by team_id'),
        rows: [
          { team_id: 10, total_amount: 120 },
          { team_id: 20, total_amount: 30 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT team_id, SUM(amount) AS total_amount FROM orders GROUP BY team_id HAVING SUM(amount) >= 100',
      runner,
    );

    const havingStep = steps.find(step => step.name === 'HAVING');
    if (!havingStep) {
      throw new Error('Expected a HAVING step but none was produced.');
    }

    assert.deepEqual(havingStep.whereColumns, ['total_amount']);
    assert.deepEqual(havingStep.data, [
      { team_id: 10, total_amount: 120 },
    ]);
  });
};
