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

module.exports = function runGroupByStepTests(runTest, assert) {
  runTest('GROUP BY collapses rows into groups and exposes aggregation metadata', async () => {
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
        ...sqlIncludes('select team_id, sum(amount) as total_amount from orders group by team_id'),
        rows: [
          { team_id: 10, total_amount: 120 },
          { team_id: 20, total_amount: 30 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT team_id, SUM(amount) AS total_amount FROM orders GROUP BY team_id',
      runner,
    );

    const groupStep = steps.find(step => step.name === 'GROUP BY');
    if (!groupStep) {
      throw new Error('Expected a GROUP BY step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'GROUP BY', 'SELECT']);
    assert.equal(groupStep.rowsBefore, 3);
    assert.equal(groupStep.rowsAfter, 2);
    assert.equal(groupStep.sqlFragment, 'GROUP BY team_id');
    assert.deepEqual(groupStep.columns, ['team_id', 'total_amount']);
    assert.deepEqual(groupStep.groupByColumns, ['team_id']);
    assert.deepEqual(groupStep.aggColumns, [
      { col: 'total_amount', fn: 'SUM', srcCol: 'amount' },
    ]);
    assert.equal(groupStep.aggSummary, 'SUM(amount)');
    assert.deepEqual(groupStep.preGroupColumns, ['team_id', 'amount']);
    assert.deepEqual(groupStep.preGroupRows, [
      { team_id: 10, amount: 50 },
      { team_id: 10, amount: 70 },
      { team_id: 20, amount: 30 },
    ]);
    assert.deepEqual(groupStep.data, [
      { team_id: 10, total_amount: 120 },
      { team_id: 20, total_amount: 30 },
    ]);
    assert.match(groupStep.explanation, /Collapsed 3 rows into 2 groups by team_id/i);
  });

  runTest('GROUP BY keeps the full pre-group source rows for breakdown filtering', async () => {
    const largeRows = Array.from({ length: 650 }, (_, idx) => ({
      continent: idx < 600 ? 'Asia' : 'Europe',
      amount: idx + 1,
    }));

    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `countries`.* from countries'),
        rows: largeRows,
      },
      {
        ...sqlIncludes('select continent, count(*) as country_count from countries group by continent'),
        rows: [
          { continent: 'Asia', country_count: 600 },
          { continent: 'Europe', country_count: 50 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT continent, COUNT(*) AS country_count FROM countries GROUP BY continent',
      runner,
    );

    const groupStep = steps.find(step => step.name === 'GROUP BY');
    if (!groupStep) {
      throw new Error('Expected a GROUP BY step but none was produced.');
    }

    assert.equal(groupStep.preGroupRows.length, 650);
    assert.equal(
      groupStep.preGroupRows.filter(row => row.continent === 'Asia').length,
      600,
    );
    assert.equal(
      groupStep.preGroupRows.filter(row => row.continent === 'Europe').length,
      50,
    );
  });
};
