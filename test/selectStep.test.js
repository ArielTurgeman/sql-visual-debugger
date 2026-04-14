const { executeDebugSteps } = require('../out/engine/stepEngine');

class FakeRunner {
  constructor(handlers) {
    this.handlers = handlers;
    this.queries = [];
    this.executes = [];
  }

  async query(sql) {
    this.queries.push(sql);
    for (const handler of this.handlers) {
      if (handler.match(sql)) {
        return cloneRows(handler.rows);
      }
    }
    throw new Error(`No fake query handler matched SQL: ${sql}`);
  }

  async execute(sql) {
    this.executes.push(sql);
  }
}

function cloneRows(rows) {
  return rows.map(row => ({ ...row }));
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

async function getSelectStep(sql, runner) {
  const steps = await executeDebugSteps(sql, runner);
  const step = steps.find(candidate => candidate.name === 'SELECT');
  if (!step) {
    throw new Error('Expected a SELECT step but none was produced.');
  }
  return { step, steps };
}

module.exports = function runSelectStepTests(runTest, assert) {
  runTest('SELECT projects only the requested output columns and keeps pre-select context', async () => {
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

    const { step, steps } = await getSelectStep('SELECT id, name FROM users', runner);

    assert.deepEqual(steps.map(candidate => candidate.name), ['FROM', 'SELECT']);
    assert.equal(step.rowsBefore, 2);
    assert.equal(step.rowsAfter, 2);
    assert.deepEqual(step.columns, ['id', 'name']);
    assert.deepEqual(step.preSelectColumns, ['id', 'name', 'active']);
    assert.deepEqual(step.data, [
      { id: 1, name: 'Ada' },
      { id: 2, name: 'Linus' },
    ]);
    assert.equal(step.distinctMeta, undefined);
    assert.deepEqual(step.caseColumns, []);
    assert.deepEqual(step.windowColumns, []);
  });

  runTest('SELECT DISTINCT stores the pre-distinct rows for explanation', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { status: 'open' },
          { status: 'open' },
          { status: 'closed' },
        ],
      },
      {
        ...sqlIncludes('select distinct status from users'),
        rows: [
          { status: 'open' },
          { status: 'closed' },
        ],
      },
      {
        ...sqlIncludes('select status from users'),
        rows: [
          { status: 'open' },
          { status: 'open' },
          { status: 'closed' },
        ],
      },
    ]);

    const { step } = await getSelectStep('SELECT DISTINCT status FROM users', runner);

    assert.equal(step.rowsBefore, 3);
    assert.equal(step.rowsAfter, 2);
    assert.deepEqual(step.columns, ['status']);
    assert.ok(step.distinctMeta);
    assert.deepEqual(step.distinctMeta.columns, ['status']);
    assert.deepEqual(step.distinctMeta.rows, [
      { status: 'open' },
      { status: 'open' },
      { status: 'closed' },
    ]);
  });

  runTest('SELECT CASE expressions produce row-level explanation metadata', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, score: 95 },
          { id: 2, score: 72 },
        ],
      },
      {
        ...sqlIncludes("select id, case when score >= 90 then 'a' else 'b' end as grade from users"),
        rows: [
          { id: 1, grade: 'A' },
          { id: 2, grade: 'B' },
        ],
      },
      {
        ...sqlIncludes('__sql_debug_case_0_branch'),
        rows: [
          {
            id: 1,
            grade: 'A',
            __sql_debug_case_0_branch: "WHEN score >= 90 THEN 'A'",
            __sql_debug_case_0_input_0: 95,
          },
          {
            id: 2,
            grade: 'B',
            __sql_debug_case_0_branch: "ELSE 'B'",
            __sql_debug_case_0_input_0: 72,
          },
        ],
      },
    ]);

    const { step } = await getSelectStep(
      "SELECT id, CASE WHEN score >= 90 THEN 'A' ELSE 'B' END AS grade FROM users",
      runner,
    );

    assert.equal(step.caseColumns.length, 1);
    assert.equal(step.caseColumns[0].outputColumn, 'grade');
    assert.deepEqual(step.caseColumns[0].inputColumns, ['score']);
    assert.equal(step.caseColumns[0].rowExplanations.length, 2);
    assert.equal(step.caseColumns[0].rowExplanations[0].matchedRule, "WHEN score >= 90 THEN 'A'");
    assert.equal(step.caseColumns[0].rowExplanations[0].returnedValue, 'A');
    assert.deepEqual(step.caseColumns[0].rowExplanations[0].inputValues, [
      { column: 'score', value: 95 },
    ]);
    assert.equal(step.caseColumns[0].rowExplanations[1].matchedRule, "ELSE 'B'");
    assert.equal(step.caseColumns[0].rowExplanations[1].returnedValue, 'B');
  });

  runTest('SELECT window functions expose partitioning and ordering metadata', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, team_id: 10, score: 90 },
          { id: 2, team_id: 10, score: 80 },
          { id: 3, team_id: 20, score: 88 },
        ],
      },
      {
        ...sqlIncludes('row_number() over (partition by team_id order by score desc) as row_num'),
        rows: [
          { id: 1, row_num: 1 },
          { id: 2, row_num: 2 },
          { id: 3, row_num: 1 },
        ],
      },
    ]);

    const { step } = await getSelectStep(
      'SELECT id, ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY score DESC) AS row_num FROM users',
      runner,
    );

    assert.equal(step.windowColumns.length, 1);
    assert.equal(step.windowColumns[0].outputColumn, 'row_num');
    assert.equal(step.windowColumns[0].functionName, 'ROW_NUMBER');
    assert.deepEqual(step.windowColumns[0].partitionBy, ['team_id']);
    assert.deepEqual(step.windowColumns[0].orderBy, ['score DESC']);
    assert.deepEqual(step.windowColumns[0].orderByTerms, [
      { column: 'score', direction: 'DESC' },
    ]);
    assert.ok(step.windowColumns[0].previewRows.length > 0);
  });
};
