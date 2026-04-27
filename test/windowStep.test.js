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

module.exports = function runWindowStepTests(runTest, assert) {
  runTest('ranking window functions expose metadata for ROW_NUMBER, RANK, and DENSE_RANK', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `scores`.* from scores'),
        rows: [
          { player: 'Ada', team_id: 10, score: 95 },
          { player: 'Linus', team_id: 10, score: 95 },
          { player: 'Grace', team_id: 10, score: 80 },
          { player: 'Ken', team_id: 20, score: 88 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, row_number() over (partition by team_id order by score desc) as row_num from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, row_num: 1 },
          { team_id: 10, score: 95, row_num: 2 },
          { team_id: 10, score: 80, row_num: 3 },
          { team_id: 20, score: 88, row_num: 1 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, rank() over (partition by team_id order by score desc) as rank_num from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, rank_num: 1 },
          { team_id: 10, score: 95, rank_num: 1 },
          { team_id: 10, score: 80, rank_num: 3 },
          { team_id: 20, score: 88, rank_num: 1 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, dense_rank() over (partition by team_id order by score desc) as dense_rank_num from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, dense_rank_num: 1 },
          { team_id: 10, score: 95, dense_rank_num: 1 },
          { team_id: 10, score: 80, dense_rank_num: 2 },
          { team_id: 20, score: 88, dense_rank_num: 1 },
        ],
      },
      {
        ...sqlIncludes('row_number() over (partition by team_id order by score desc) as row_num'),
        rows: [
          { player: 'Ada', row_num: 1, rank_num: 1, dense_rank_num: 1 },
          { player: 'Linus', row_num: 2, rank_num: 1, dense_rank_num: 1 },
          { player: 'Grace', row_num: 3, rank_num: 3, dense_rank_num: 2 },
          { player: 'Ken', row_num: 1, rank_num: 1, dense_rank_num: 1 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT
        player,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY score DESC) AS row_num,
        RANK() OVER (PARTITION BY team_id ORDER BY score DESC) AS rank_num,
        DENSE_RANK() OVER (PARTITION BY team_id ORDER BY score DESC) AS dense_rank_num
      FROM scores
      `,
      runner,
    );

    const selectStep = steps.find(step => step.name === 'SELECT');
    if (!selectStep) throw new Error('Expected a SELECT step but none was produced.');

    assert.equal(selectStep.windowColumns.length, 3);
    assert.deepEqual(
      selectStep.windowColumns.map(col => ({
        outputColumn: col.outputColumn,
        functionName: col.functionName,
        partitionBy: col.partitionBy,
        orderBy: col.orderBy,
      })),
      [
        {
          outputColumn: 'row_num',
          functionName: 'ROW_NUMBER',
          partitionBy: ['team_id'],
          orderBy: ['score DESC'],
        },
        {
          outputColumn: 'rank_num',
          functionName: 'RANK',
          partitionBy: ['team_id'],
          orderBy: ['score DESC'],
        },
        {
          outputColumn: 'dense_rank_num',
          functionName: 'DENSE_RANK',
          partitionBy: ['team_id'],
          orderBy: ['score DESC'],
        },
      ],
    );
    assert.ok(selectStep.windowColumns.every(col => col.previewRows.length > 0));
    assert.deepEqual(
      selectStep.windowColumns[0].previewRows.map(row => ({
        team_id: row.team_id,
        score: row.score,
        row_num: row.row_num,
      })),
      [
        { team_id: 10, score: 95, row_num: 1 },
        { team_id: 10, score: 95, row_num: 2 },
        { team_id: 10, score: 80, row_num: 3 },
        { team_id: 20, score: 88, row_num: 1 },
      ],
    );
  });

  runTest('aggregate window functions expose metadata for SUM AVG COUNT MIN and MAX', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `scores`.* from scores'),
        rows: [
          { player: 'Ada', team_id: 10, score: 95 },
          { player: 'Linus', team_id: 10, score: 80 },
          { player: 'Grace', team_id: 20, score: 88 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, sum(score) over (partition by team_id order by score desc) as running_sum from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, running_sum: 95 },
          { team_id: 10, score: 80, running_sum: 175 },
          { team_id: 20, score: 88, running_sum: 88 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, avg(score) over (partition by team_id order by score desc) as running_avg from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, running_avg: 95 },
          { team_id: 10, score: 80, running_avg: 87.5 },
          { team_id: 20, score: 88, running_avg: 88 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, count(score) over (partition by team_id order by score desc) as running_count from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, running_count: 1 },
          { team_id: 10, score: 80, running_count: 2 },
          { team_id: 20, score: 88, running_count: 1 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, min(score) over (partition by team_id order by score desc) as running_min from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, running_min: 95 },
          { team_id: 10, score: 80, running_min: 80 },
          { team_id: 20, score: 88, running_min: 88 },
        ],
      },
      {
        ...sqlIncludes('select team_id, score, max(score) over (partition by team_id order by score desc) as running_max from scores order by team_id, score desc'),
        rows: [
          { team_id: 10, score: 95, running_max: 95 },
          { team_id: 10, score: 80, running_max: 95 },
          { team_id: 20, score: 88, running_max: 88 },
        ],
      },
      {
        ...sqlIncludes('sum(score) over (partition by team_id order by score desc) as running_sum'),
        rows: [
          {
            player: 'Ada',
            running_sum: 95,
            running_avg: 95,
            running_count: 1,
            running_min: 95,
            running_max: 95,
          },
          {
            player: 'Linus',
            running_sum: 175,
            running_avg: 87.5,
            running_count: 2,
            running_min: 80,
            running_max: 95,
          },
          {
            player: 'Grace',
            running_sum: 88,
            running_avg: 88,
            running_count: 1,
            running_min: 88,
            running_max: 88,
          },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT
        player,
        SUM(score) OVER (PARTITION BY team_id ORDER BY score DESC) AS running_sum,
        AVG(score) OVER (PARTITION BY team_id ORDER BY score DESC) AS running_avg,
        COUNT(score) OVER (PARTITION BY team_id ORDER BY score DESC) AS running_count,
        MIN(score) OVER (PARTITION BY team_id ORDER BY score DESC) AS running_min,
        MAX(score) OVER (PARTITION BY team_id ORDER BY score DESC) AS running_max
      FROM scores
      `,
      runner,
    );

    const selectStep = steps.find(step => step.name === 'SELECT');
    if (!selectStep) throw new Error('Expected a SELECT step but none was produced.');

    assert.equal(selectStep.windowColumns.length, 5);
    assert.deepEqual(
      selectStep.windowColumns.map(col => ({
        outputColumn: col.outputColumn,
        functionName: col.functionName,
        sourceColumn: col.sourceColumn,
        partitionBy: col.partitionBy,
        orderByTerms: col.orderByTerms,
      })),
      [
        {
          outputColumn: 'running_sum',
          functionName: 'SUM',
          sourceColumn: 'score',
          partitionBy: ['team_id'],
          orderByTerms: [{ column: 'score', direction: 'DESC' }],
        },
        {
          outputColumn: 'running_avg',
          functionName: 'AVG',
          sourceColumn: 'score',
          partitionBy: ['team_id'],
          orderByTerms: [{ column: 'score', direction: 'DESC' }],
        },
        {
          outputColumn: 'running_count',
          functionName: 'COUNT',
          sourceColumn: 'score',
          partitionBy: ['team_id'],
          orderByTerms: [{ column: 'score', direction: 'DESC' }],
        },
        {
          outputColumn: 'running_min',
          functionName: 'MIN',
          sourceColumn: 'score',
          partitionBy: ['team_id'],
          orderByTerms: [{ column: 'score', direction: 'DESC' }],
        },
        {
          outputColumn: 'running_max',
          functionName: 'MAX',
          sourceColumn: 'score',
          partitionBy: ['team_id'],
          orderByTerms: [{ column: 'score', direction: 'DESC' }],
        },
      ],
    );
  });

  runTest('window previews keep the full row set instead of truncating to the first 20 rows', async () => {
    const baseRows = Array.from({ length: 25 }, (_, index) => ({
      player: `P${index + 1}`,
      team_id: index < 13 ? 10 : 20,
      score: 100 - index,
    }));
    const selectedRows = baseRows.map((row, index) => ({
      player: row.player,
      row_num: index < 13 ? index + 1 : index - 12,
    }));

    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `scores`.* from scores'),
        rows: baseRows,
      },
      {
        ...sqlIncludes('select team_id, score, row_number() over (partition by team_id order by score desc) as row_num from scores order by team_id, score desc'),
        rows: baseRows
          .map((row, index) => ({
            team_id: row.team_id,
            score: row.score,
            row_num: index < 13 ? index + 1 : index - 12,
          }))
          .sort((left, right) => left.team_id - right.team_id || right.score - left.score),
      },
      {
        ...sqlIncludes('row_number() over (partition by team_id order by score desc) as row_num'),
        rows: selectedRows,
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT
        player,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY score DESC) AS row_num
      FROM scores
      `,
      runner,
    );

    const selectStep = steps.find(step => step.name === 'SELECT');
    if (!selectStep) throw new Error('Expected a SELECT step but none was produced.');

    assert.equal(selectStep.windowColumns[0].previewRows.length, 25);
    assert.deepEqual(
      selectStep.windowColumns[0].previewRows.at(-1),
      { team_id: 20, score: 76, row_num: 12 },
    );
  });

  runTest('window preview queries keep qualified source expressions after joins', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, name: 'Ada' },
          { id: 2, name: 'Ben' },
        ],
      },
      {
        ...sqlIncludes('select `o`.* from qa_orders o'),
        rows: [
          { id: 10, customer_id: 1, status: 'open', amount: 100, created_at: '2024-01-01' },
          { id: 11, customer_id: 1, status: 'open', amount: 50, created_at: '2024-01-02' },
          { id: 12, customer_id: 2, status: 'closed', amount: 25, created_at: '2024-01-03' },
        ],
      },
      {
        ...sqlIncludes('select c.id as `id`, o.created_at as `created_at`, o.amount as `amount`, sum(o.amount) over (partition by c.id order by o.created_at) as running_customer_amount'),
        rows: [
          { id: 1, created_at: '2024-01-01', amount: 100, running_customer_amount: 100 },
          { id: 1, created_at: '2024-01-02', amount: 50, running_customer_amount: 150 },
          { id: 2, created_at: '2024-01-03', amount: 25, running_customer_amount: 25 },
        ],
      },
      {
        ...sqlIncludes('sum(o.amount) over (partition by c.id order by o.created_at) as running_customer_amount'),
        rows: [
          { name: 'Ada', status: 'open', amount: 100, running_customer_amount: 100 },
          { name: 'Ada', status: 'open', amount: 50, running_customer_amount: 150 },
          { name: 'Ben', status: 'closed', amount: 25, running_customer_amount: 25 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT
        c.name,
        o.status,
        o.amount,
        SUM(o.amount) OVER (PARTITION BY c.id ORDER BY o.created_at) AS running_customer_amount
      FROM qa_customers c
      INNER JOIN qa_orders o ON c.id = o.customer_id
      ORDER BY c.id, o.created_at
      `,
      runner,
    );

    const selectStep = steps.find(step => step.name === 'SELECT');
    if (!selectStep) throw new Error('Expected a SELECT step but none was produced.');

    const windowMeta = selectStep.windowColumns[0];
    assert.deepEqual(windowMeta.partitionBy, ['id']);
    assert.deepEqual(windowMeta.partitionByExpressions, ['c.id']);
    assert.equal(windowMeta.sourceColumn, 'amount');
    assert.equal(windowMeta.sourceExpression, 'o.amount');
    assert.deepEqual(windowMeta.orderByTerms, [{ column: 'created_at', direction: 'ASC' }]);
    assert.deepEqual(windowMeta.orderBySourceTerms, [{ expression: 'o.created_at', direction: 'ASC' }]);
    assert.ok(runner.queries.some(sql => normalizeSql(sql).includes('select c.id as `id`, o.created_at as `created_at`, o.amount as `amount`')));
    assert.ok(runner.queries.every(sql => !normalizeSql(sql).includes('select id, created_at, amount,')));
  });
};
