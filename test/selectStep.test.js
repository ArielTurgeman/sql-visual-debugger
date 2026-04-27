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

  runTest('SELECT CASE explanations preserve qualified joined input labels', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, name: 'Ada', region_id: 1, status: 'active' },
          { id: 5, name: 'Nulla', region_id: null, status: 'active' },
        ],
      },
      {
        ...sqlIncludes('select `r`.* from qa_regions r'),
        rows: [
          { id: 1, name: 'North', status: 'active' },
        ],
      },
      {
        ...sqlIncludes("select c.id, c.name, c.status, r.name as region_name, case when r.id is null then 'no region' when r.status = 'inactive' then 'inactive region' else 'active region' end as region_state from qa_customers c left join qa_regions r on c.region_id = r.id"),
        rows: [
          { id: 1, name: 'Ada', status: 'active', region_name: 'North', region_state: 'active region' },
          { id: 5, name: 'Nulla', status: 'active', region_name: null, region_state: 'no region' },
        ],
      },
      {
        ...sqlIncludes('__sql_debug_case_0_branch'),
        rows: [
          {
            id: 1,
            name: 'Ada',
            status: 'active',
            region_name: 'North',
            region_state: 'active region',
            __sql_debug_case_0_branch: "ELSE 'active region'",
            __sql_debug_case_0_input_0: 1,
            __sql_debug_case_0_input_1: 'active',
          },
          {
            id: 5,
            name: 'Nulla',
            status: 'active',
            region_name: null,
            region_state: 'no region',
            __sql_debug_case_0_branch: "WHEN r.id IS NULL THEN 'no region'",
            __sql_debug_case_0_input_0: null,
            __sql_debug_case_0_input_1: null,
          },
        ],
      },
    ]);

    const { step } = await getSelectStep(
      `
      SELECT
        c.id,
        c.name,
        c.status,
        r.name AS region_name,
        CASE
          WHEN r.id IS NULL THEN 'no region'
          WHEN r.status = 'inactive' THEN 'inactive region'
          ELSE 'active region'
        END AS region_state
      FROM qa_customers c
      LEFT JOIN qa_regions r ON c.region_id = r.id
      `,
      runner,
    );

    assert.equal(step.caseColumns.length, 1);
    assert.deepEqual(step.caseColumns[0].inputColumns, ['r.id', 'r.status']);
    assert.deepEqual(step.caseColumns[0].rowExplanations[1].inputValues, [
      { column: 'r.id', value: null },
    ]);
  });

  runTest('grouped CASE outputs can still build CASE explanation metadata under ONLY_FULL_GROUP_BY', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, region_id: 1 },
          { id: 3, region_id: 2 },
        ],
      },
      {
        ...sqlIncludes('select `o`.* from qa_orders o'),
        rows: [
          { id: 10, customer_id: 1, amount: 100 },
          { id: 13, customer_id: 3, amount: 0 },
        ],
      },
      {
        ...sqlIncludes('select `p`.* from qa_payments p'),
        rows: [
          { id: 100, order_id: 10, status: 'paid', amount: 100 },
          { id: 103, order_id: 13, status: 'paid', amount: 0 },
        ],
      },
      {
        ...sqlIncludes('select `r`.* from qa_regions r'),
        rows: [
          { id: 1, name: 'North' },
          { id: 2, name: 'South' },
        ],
      },
      {
        ...sqlIncludes('select `c`.`id`, `c`.`region_id`, `o`.`id` as `o.id`, `o`.`customer_id`, `o`.`amount`, `p`.`id` as `p.id`, `p`.`order_id`, `p`.`status`, `p`.`amount` as `p.amount`, `r`.`id` as `r.id`, `r`.`name` from qa_customers c inner join qa_orders o on c.id = o.customer_id inner join qa_payments p on o.id = p.order_id left join qa_regions r on c.region_id = r.id where o.amount is not null'),
        rows: [
          { id: 1, region_id: 1, 'o.id': 10, customer_id: 1, amount: 100, 'p.id': 100, order_id: 10, status: 'paid', 'p.amount': 100, 'r.id': 1, name: 'North' },
          { id: 3, region_id: 2, 'o.id': 13, customer_id: 3, amount: 0, 'p.id': 103, order_id: 13, status: 'paid', 'p.amount': 0, 'r.id': 2, name: 'South' },
        ],
      },
      {
        ...sqlIncludes("select r.name as region_name, case when p.status = 'paid' then 'paid' when p.status = 'failed' then 'failed' else 'other' end as payment_group, count(*) as payment_count, sum(p.amount) as payment_total from qa_customers c inner join qa_orders o on c.id = o.customer_id inner join qa_payments p on o.id = p.order_id left join qa_regions r on c.region_id = r.id where o.amount is not null group by r.name, payment_group"),
        rows: [
          { region_name: 'North', payment_group: 'paid', payment_count: 1, payment_total: 100 },
          { region_name: 'South', payment_group: 'paid', payment_count: 1, payment_total: 0 },
        ],
      },
      {
        ...sqlIncludes('__sql_debug_case_0_branch'),
        rows: [
          {
            region_name: 'North',
            payment_group: 'paid',
            payment_count: 1,
            payment_total: 100,
            __sql_debug_case_0_branch: "WHEN p.status = 'paid' THEN 'paid'",
            __sql_debug_case_0_input_0: 'paid',
          },
          {
            region_name: 'South',
            payment_group: 'paid',
            payment_count: 1,
            payment_total: 0,
            __sql_debug_case_0_branch: "WHEN p.status = 'paid' THEN 'paid'",
            __sql_debug_case_0_input_0: 'paid',
          },
        ],
      },
    ]);

    const { step } = await getSelectStep(
      `
      SELECT
        r.name AS region_name,
        CASE
          WHEN p.status = 'paid' THEN 'paid'
          WHEN p.status = 'failed' THEN 'failed'
          ELSE 'other'
        END AS payment_group,
        COUNT(*) AS payment_count,
        SUM(p.amount) AS payment_total
      FROM qa_customers c
      INNER JOIN qa_orders o ON c.id = o.customer_id
      INNER JOIN qa_payments p ON o.id = p.order_id
      LEFT JOIN qa_regions r ON c.region_id = r.id
      WHERE o.amount IS NOT NULL
      GROUP BY r.name, payment_group
      HAVING COUNT(*) >= 1
      ORDER BY region_name, payment_total DESC
      `,
      runner,
    );

    assert.equal(step.caseColumns.length, 1);
    assert.equal(step.caseColumns[0].outputColumn, 'payment_group');
    assert.deepEqual(step.caseColumns[0].inputColumns, ['p.status']);
    assert.deepEqual(step.caseColumns[0].rowExplanations[0].inputValues, [
      { column: 'p.status', value: 'paid' },
    ]);
    const helperQuery = runner.queries.find(sql => normalizeSql(sql).includes('__sql_debug_case_0_branch'));
    assert.ok(helperQuery);
    assert.match(normalizeSql(helperQuery), /any_value\(case when p\.status = 'paid' then 'when p\.status = ''paid'' then ''paid'''/);
    assert.match(normalizeSql(helperQuery), /any_value\(p\.status\) as `__sql_debug_case_0_input_0`/);
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
