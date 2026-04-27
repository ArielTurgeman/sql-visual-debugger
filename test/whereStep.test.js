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

module.exports = function runWhereStepTests(runTest, assert) {
  runTest('WHERE filters rows and keeps pre-filter data for comparison', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, age: 17, name: 'Ada' },
          { id: 2, age: 21, name: 'Linus' },
          { id: 3, age: 25, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select `users`.`id`, `users`.`age`, `users`.`name` from users where age >= 21'),
        rows: [
          { id: 2, age: 21, name: 'Linus' },
          { id: 3, age: 25, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select id, age, name from users where age >= 21'),
        rows: [
          { id: 2, age: 21, name: 'Linus' },
          { id: 3, age: 25, name: 'Grace' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT id, age, name FROM users WHERE age >= 21',
      runner,
    );

    const whereStep = steps.find(step => step.name === 'WHERE');
    if (!whereStep) {
      throw new Error('Expected a WHERE step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'WHERE', 'SELECT']);
    assert.equal(whereStep.rowsBefore, 3);
    assert.equal(whereStep.rowsAfter, 2);
    assert.equal(whereStep.sqlFragment, 'WHERE age >= 21');
    assert.deepEqual(whereStep.columns, ['id', 'age', 'name']);
    assert.deepEqual(whereStep.whereColumns, ['age']);
    assert.deepEqual(whereStep.preFilterColumns, ['id', 'age', 'name']);
    assert.deepEqual(whereStep.preFilterRows, [
      { id: 1, age: 17, name: 'Ada' },
      { id: 2, age: 21, name: 'Linus' },
      { id: 3, age: 25, name: 'Grace' },
    ]);
    assert.deepEqual(whereStep.data, [
      { id: 2, age: 21, name: 'Linus' },
      { id: 3, age: 25, name: 'Grace' },
    ]);
    assert.equal(whereStep.whereInSubquery, undefined);
    assert.equal(whereStep.whereScalarSubquery, undefined);
  });

  runTest('WHERE IN subqueries expose subquery preview metadata', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 2, team_id: 20, name: 'Linus' },
          { id: 3, team_id: 30, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select team_id from vip_teams'),
        rows: [
          { team_id: 10 },
          { team_id: 30 },
        ],
      },
      {
        ...sqlIncludes('where team_id in (select team_id from vip_teams)'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 3, team_id: 30, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select id, team_id, name from users where team_id in (select team_id from vip_teams)'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 3, team_id: 30, name: 'Grace' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT id, team_id, name FROM users WHERE team_id IN (SELECT team_id FROM vip_teams)',
      runner,
    );

    const whereStep = steps.find(step => step.name === 'WHERE');
    if (!whereStep) {
      throw new Error('Expected a WHERE step but none was produced.');
    }

    assert.ok(whereStep.whereInSubquery);
    assert.equal(
      whereStep.whereInSubquery.explanation,
      'Filters rows by checking whether team_id exists in the values returned by the subquery.',
    );
    assert.deepEqual(whereStep.whereInSubquery.columns, ['team_id']);
    assert.deepEqual(whereStep.whereInSubquery.rows, [
      { team_id: 10 },
      { team_id: 30 },
    ]);
    assert.equal(whereStep.whereInSubquery.totalRows, 2);
  });

  runTest('WHERE scalar subqueries expose comparison metadata', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 1, age: 18, name: 'Ada' },
          { id: 2, age: 24, name: 'Linus' },
          { id: 3, age: 30, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select avg(age) as avg_age from users'),
        rows: [
          { avg_age: 24 },
        ],
      },
      {
        ...sqlIncludes('where age > (select avg(age) as avg_age from users)'),
        rows: [
          { id: 3, age: 30, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select id, age, name from users where age > (select avg(age) as avg_age from users)'),
        rows: [
          { id: 3, age: 30, name: 'Grace' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT id, age, name FROM users WHERE age > (SELECT AVG(age) AS avg_age FROM users)',
      runner,
    );

    const whereStep = steps.find(step => step.name === 'WHERE');
    if (!whereStep) {
      throw new Error('Expected a WHERE step but none was produced.');
    }

    assert.ok(whereStep.whereScalarSubquery);
    assert.equal(
      whereStep.whereScalarSubquery.explanation,
      'Checks whether age is greater than the value returned by the subquery.',
    );
    assert.equal(whereStep.whereScalarSubquery.value, 24);
    assert.equal(whereStep.whereScalarSubquery.columnLabel, 'avg_age');
  });

  runTest('WHERE highlights only the qualified duplicate column that is actually referenced', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `co`.* from country co'),
        rows: [
          { Code: 'IND', Name: 'India', Population: 1000000 },
          { Code: 'PAK', Name: 'Pakistan', Population: 900000 },
        ],
      },
      {
        ...sqlIncludes('select `ci`.* from city ci'),
        rows: [
          { Name: 'Mumbai', Population: 900000, CountryCode: 'IND' },
          { Name: 'Karachi', Population: 800000, CountryCode: 'PAK' },
        ],
      },
      {
        ...sqlIncludes('where co.population >= 1000000'),
        rows: [
          { Code: 'IND', Name: 'India', Population: 1000000, 'ci.Name': 'Mumbai', 'ci.Population': 900000, CountryCode: 'IND' },
        ],
      },
      {
        ...sqlIncludes('select co.name as country_name, ci.name as city_name from country co inner join city ci on co.code = ci.countrycode where co.population >= 1000000'),
        rows: [
          { country_name: 'India', city_name: 'Mumbai' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT co.Name AS country_name, ci.Name AS city_name FROM country co INNER JOIN city ci ON co.Code = ci.CountryCode WHERE co.Population >= 1000000',
      runner,
    );

    const whereStep = steps.find(step => step.name === 'WHERE');
    if (!whereStep) {
      throw new Error('Expected a WHERE step but none was produced.');
    }

    assert.deepEqual(whereStep.preFilterColumns, ['Code', 'Name', 'Population', 'ci.Name', 'ci.Population', 'CountryCode']);
    assert.deepEqual(whereStep.whereColumns, ['Population']);
  });
};
