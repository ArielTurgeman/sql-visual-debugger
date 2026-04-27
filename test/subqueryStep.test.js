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
        return handler.rows.map(row => ({ ...row }));
      }
    }
    throw new Error(`No fake query handler matched SQL: ${sql}`);
  }

  async execute(sql) {
    this.executes.push(sql);
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

function sqlEquals(fragment) {
  const normalizedFragment = normalizeSql(fragment);
  return {
    match(sql) {
      return normalizeSql(sql) === normalizedFragment;
    },
  };
}

module.exports = function runSubqueryStepTests(runTest, assert) {
  runTest('FROM subqueries execute as a separate subquery block and feed the main query', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `sub`.`id`, `sub`.`team_id` from (select id, team_id from users where active = 1) sub where sub.id > 15'),
        rows: [
          { id: 20, team_id: 20 },
        ],
      },
      {
        ...sqlIncludes('select sub.id from (select id, team_id from users where active = 1) sub where sub.id > 15'),
        rows: [
          { id: 20 },
        ],
      },
      {
        ...sqlIncludes('select `sub`.* from (select id, team_id from users where active = 1) sub'),
        rows: [
          { id: 12, team_id: 10 },
          { id: 20, team_id: 20 },
        ],
      },
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 5, team_id: 10, active: 0 },
          { id: 12, team_id: 10, active: 1 },
          { id: 20, team_id: 20, active: 1 },
        ],
      },
      {
        ...sqlEquals('select `users`.`id`, `users`.`team_id`, `users`.`active` from users where active = 1'),
        rows: [
          { id: 12, team_id: 10, active: 1 },
          { id: 20, team_id: 20, active: 1 },
        ],
      },
      {
        ...sqlEquals('select id, team_id from users where active = 1'),
        rows: [
          { id: 12, team_id: 10 },
          { id: 20, team_id: 20 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT sub.id
      FROM (
        SELECT id, team_id
        FROM users
        WHERE active = 1
      ) AS sub
      WHERE sub.id > 15
      `,
      runner,
    );

    assert.deepEqual(
      steps.map(step => `${step.blockType}:${step.name}`),
      [
        'subquery:FROM',
        'subquery:WHERE',
        'subquery:SELECT',
        'main:FROM',
        'main:WHERE',
        'main:SELECT',
      ],
    );

    const subquerySelectStep = steps.find(step => step.blockType === 'subquery' && step.name === 'SELECT');
    const mainFromStep = steps.find(step => step.blockType === 'main' && step.name === 'FROM');
    const mainWhereStep = steps.find(step => step.blockType === 'main' && step.name === 'WHERE');
    const mainSelectStep = steps.find(step => step.blockType === 'main' && step.name === 'SELECT');

    if (!subquerySelectStep || !mainFromStep || !mainWhereStep || !mainSelectStep) {
      throw new Error('Expected subquery and main-query steps were not all produced.');
    }

    assert.deepEqual(subquerySelectStep.data, [
      { id: 12, team_id: 10 },
      { id: 20, team_id: 20 },
    ]);

    assert.equal(mainFromStep.sourceRows, 2);
    assert.equal(mainFromStep.sourceLabel, 'Loaded from subquery sub');
    assert.equal(mainFromStep.rowsAfter, 2);
    assert.deepEqual(mainFromStep.data, [
      { id: 12, team_id: 10 },
      { id: 20, team_id: 20 },
    ]);

    assert.equal(mainWhereStep.rowsBefore, 2);
    assert.equal(mainWhereStep.rowsAfter, 1);
    assert.deepEqual(mainWhereStep.data, [
      { id: 20, team_id: 20 },
    ]);

    assert.deepEqual(mainSelectStep.data, [
      { id: 20 },
    ]);

    assert.deepEqual(runner.executes, []);
    assert.equal(runner.queries.some(sql => /temporary\s+table/i.test(sql)), false);
  });

  runTest('FROM subqueries can be joined to outer tables without false unsupported-shape errors', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `sub`.*, `c`.* from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id left join qa_regions r on sub.region_id = r.id where c.status = \'active\''),
        rows: [
          { id: 10, customer_id: 1, region_id: 1, order_status: 'open', amount: 100, 'c.id': 1, code: 'C001', name: 'Ada', status: 'active', score: 90 },
          { id: 11, customer_id: 1, region_id: 1, order_status: 'open', amount: 100, 'c.id': 1, code: 'C001', name: 'Ada', status: 'active', score: 90 },
        ],
      },
      {
        ...sqlIncludes('select `sub`.`id`, `sub`.`customer_id`, `sub`.`region_id`, `sub`.`order_status`, `sub`.`amount`, `c`.`id` as `c.id`, `c`.`code`, `c`.`name`, `c`.`region_id` as `c.region_id`, `c`.`status`, `c`.`score`, `r`.`id` as `r.id`, `r`.`code` as `r.code`, `r`.`name` as `r.name`, `r`.`status` as `r.status` from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id left join qa_regions r on sub.region_id = r.id where c.status = \'active\''),
        rows: [
          {
            id: 10,
            customer_id: 1,
            region_id: 1,
            order_status: 'open',
            amount: 100,
            'c.id': 1,
            code: 'C001',
            name: 'Ada',
            'c.region_id': 1,
            status: 'active',
            score: 90,
            'r.id': 1,
            'r.code': 'N',
            'r.name': 'North',
            'r.status': 'active',
          },
          {
            id: 11,
            customer_id: 1,
            region_id: 1,
            order_status: 'open',
            amount: 100,
            'c.id': 1,
            code: 'C001',
            name: 'Ada',
            'c.region_id': 1,
            status: 'active',
            score: 90,
            'r.id': 1,
            'r.code': 'N',
            'r.name': 'North',
            'r.status': 'active',
          },
        ],
      },
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, code: 'C001', name: 'Ada', region_id: 1, status: 'active', score: 90 },
          { id: 2, code: 'C002', name: 'Ben', region_id: 1, status: 'active', score: null },
        ],
      },
      {
        ...sqlIncludes('select `r`.* from qa_regions r'),
        rows: [
          { id: 1, code: 'N', name: 'North', status: 'active' },
          { id: 2, code: 'S', name: 'South', status: 'active' },
        ],
      },
      {
        ...sqlIncludes('select `sub`.`customer_id`, `sub`.`order_status`, `r`.`name` as `region_name` from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id left join qa_regions r on sub.region_id = r.id where c.status = \'active\' order by sub.customer_id, sub.order_status, region_name'),
        rows: [
          { customer_id: 1, order_status: 'open', region_name: 'North' },
        ],
      },
      {
        ...sqlIncludes('select sub.customer_id, sub.order_status, r.name as region_name from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id left join qa_regions r on sub.region_id = r.id where c.status = \'active\''),
        rows: [
          { customer_id: 1, order_status: 'open', region_name: 'North' },
          { customer_id: 1, order_status: 'open', region_name: 'North' },
        ],
      },
      {
        ...sqlIncludes('select distinct sub.customer_id, sub.order_status, r.name as region_name from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id left join qa_regions r on sub.region_id = r.id where c.status = \'active\''),
        rows: [
          { customer_id: 1, order_status: 'open', region_name: 'North' },
        ],
      },
      {
        ...sqlIncludes('select distinct `sub`.`customer_id`, `sub`.`order_status`, `r`.`name` as `region_name` from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id left join qa_regions r on sub.region_id = r.id where c.status = \'active\' order by sub.customer_id, sub.order_status, region_name'),
        rows: [
          { customer_id: 1, order_status: 'open', region_name: 'North' },
        ],
      },
      {
        ...sqlIncludes('select `sub`.* from (select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub'),
        rows: [
          { id: 10, customer_id: 1, region_id: 1, order_status: 'open', amount: 100 },
          { id: 11, customer_id: 1, region_id: 1, order_status: 'open', amount: 100 },
        ],
      },
      {
        ...sqlIncludes('select `o`.* from qa_orders o'),
        rows: [
          { id: 10, customer_id: 1, region_id: 1, status: 'open', amount: 100 },
          { id: 11, customer_id: 1, region_id: 1, status: 'open', amount: 100 },
          { id: 12, customer_id: 2, region_id: 1, status: 'closed', amount: null },
        ],
      },
      {
        ...sqlEquals('select `o`.`id`, `o`.`customer_id`, `o`.`region_id`, `o`.`status`, `o`.`amount` from qa_orders o where o.amount is not null'),
        rows: [
          { id: 10, customer_id: 1, region_id: 1, status: 'open', amount: 100 },
          { id: 11, customer_id: 1, region_id: 1, status: 'open', amount: 100 },
        ],
      },
      {
        ...sqlEquals('select o.id, o.customer_id, o.region_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null'),
        rows: [
          { id: 10, customer_id: 1, region_id: 1, order_status: 'open', amount: 100 },
          { id: 11, customer_id: 1, region_id: 1, order_status: 'open', amount: 100 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT DISTINCT
        sub.customer_id,
        sub.order_status,
        r.name AS region_name
      FROM (
        SELECT
          o.id,
          o.customer_id,
          o.region_id,
          o.status AS order_status,
          o.amount
        FROM qa_orders o
        WHERE o.amount IS NOT NULL
      ) sub
      INNER JOIN qa_customers c ON sub.customer_id = c.id
      LEFT JOIN qa_regions r ON sub.region_id = r.id
      WHERE c.status = 'active'
      ORDER BY sub.customer_id, sub.order_status, region_name
      `,
      runner,
    );

    assert.ok(steps.some(step => step.blockType === 'main' && step.name === 'JOIN'));
    assert.ok(steps.some(step => step.blockType === 'main' && step.name === 'SELECT'));
    assert.equal(
      runner.queries.some(sql => sql.includes('__sql_debug_subquery_') && !/\b(?:from|join)\s+`?__sql_debug_subquery_/i.test(sql)),
      false,
    );
  });
};
