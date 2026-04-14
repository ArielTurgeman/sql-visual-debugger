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
};
