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

module.exports = function runOrderByStepTests(runTest, assert) {
  runTest('ORDER BY reorders rows and highlights every referenced sort column', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `users`.* from users'),
        rows: [
          { id: 2, score: 80, name: 'Linus' },
          { id: 1, score: 80, name: 'Ada' },
          { id: 3, score: 95, name: 'Grace' },
        ],
      },
      {
        ...sqlIncludes('select id, score, name from users order by score desc, id asc'),
        rows: [
          { id: 3, score: 95, name: 'Grace' },
          { id: 1, score: 80, name: 'Ada' },
          { id: 2, score: 80, name: 'Linus' },
        ],
      },
      {
        ...sqlIncludes('select id, score, name from users'),
        rows: [
          { id: 2, score: 80, name: 'Linus' },
          { id: 1, score: 80, name: 'Ada' },
          { id: 3, score: 95, name: 'Grace' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT id, score, name FROM users ORDER BY score DESC, id ASC',
      runner,
    );

    const orderStep = steps.find(step => step.name === 'ORDER BY');
    if (!orderStep) {
      throw new Error('Expected an ORDER BY step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'SELECT', 'ORDER BY']);
    assert.equal(orderStep.rowsBefore, 3);
    assert.equal(orderStep.rowsAfter, 3);
    assert.deepEqual(orderStep.sortColumns, ['id', 'score']);
    assert.deepEqual(orderStep.data, [
      { id: 3, score: 95, name: 'Grace' },
      { id: 1, score: 80, name: 'Ada' },
      { id: 2, score: 80, name: 'Linus' },
    ]);
    assert.equal(
      orderStep.sqlFragment,
      'ORDER BY score DESC, id ASC',
    );
  });

  runTest('ORDER BY highlights aliased output columns when sorting by their source column names', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `city`.* from city'),
        rows: [
          { Population: 503400, Name: 'Tjumen' },
          { Population: 506100, Name: 'Tula' },
          { Population: 504420, Name: 'Sale' },
        ],
      },
      {
        ...sqlIncludes('select population, name as hopa from city'),
        rows: [
          { Population: 503400, hopa: 'Tjumen' },
          { Population: 506100, hopa: 'Tula' },
          { Population: 504420, hopa: 'Sale' },
        ],
      },
      {
        ...sqlIncludes('select population, name as hopa from city where population > 500000'),
        rows: [
          { Population: 503400, hopa: 'Tjumen' },
          { Population: 506100, hopa: 'Tula' },
          { Population: 504420, hopa: 'Sale' },
        ],
      },
      {
        ...sqlIncludes('select `population`, `name` as hopa from city where `population` > 500000'),
        rows: [
          { Population: 503400, hopa: 'Tjumen' },
          { Population: 506100, hopa: 'Tula' },
          { Population: 504420, hopa: 'Sale' },
        ],
      },
      {
        ...sqlIncludes('select `city`.`population`, `city`.`name` from city where `population` > 500000'),
        rows: [
          { Population: 503400, Name: 'Tjumen' },
          { Population: 506100, Name: 'Tula' },
          { Population: 504420, Name: 'Sale' },
        ],
      },
      {
        ...sqlIncludes('select population, name as hopa from city where population > 500000 order by population, name'),
        rows: [
          { Population: 503400, hopa: 'Tjumen' },
          { Population: 504420, hopa: 'Sale' },
          { Population: 506100, hopa: 'Tula' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT `Population`, `Name` AS hopa FROM city WHERE `Population` > 500000 ORDER BY `Population`, `Name`',
      runner,
    );

    const orderStep = steps.find(step => step.name === 'ORDER BY');
    if (!orderStep) {
      throw new Error('Expected an ORDER BY step but none was produced.');
    }

    assert.deepEqual(orderStep.columns, ['Population', 'hopa']);
    assert.deepEqual(orderStep.sortColumns, ['Population', 'hopa']);
  });
};
