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
};
