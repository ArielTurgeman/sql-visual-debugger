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
    assert.deepEqual(orderStep.sortColumns, ['score', 'id']);
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

  runTest('ORDER BY does not highlight same-named columns from the wrong table alias', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `ci`.* from city ci'),
        rows: [
          { Name: 'Mumbai', Population: 10500000, CountryCode: 'IND' },
          { Name: 'Karachi', Population: 9269265, CountryCode: 'PAK' },
        ],
      },
      {
        ...sqlIncludes('select `co`.* from country co'),
        rows: [
          { Code: 'IND', Name: 'India', Continent: 'Asia' },
          { Code: 'PAK', Name: 'Pakistan', Continent: 'Asia' },
        ],
      },
      {
        ...sqlIncludes("order by ci.population desc, ci.name"),
        rows: [
          { city_name: 'Mumbai', city_population: 10500000, country_name: 'India', Continent: 'Asia' },
          { city_name: 'Karachi', city_population: 9269265, country_name: 'Pakistan', Continent: 'Asia' },
        ],
      },
      {
        ...sqlIncludes("select ci.name as city_name, ci.population as city_population, co.name as country_name, co.continent from city ci inner join country co on ci.countrycode = co.code where co.continent = 'asia'"),
        rows: [
          { city_name: 'Mumbai', city_population: 10500000, country_name: 'India', Continent: 'Asia' },
          { city_name: 'Karachi', city_population: 9269265, country_name: 'Pakistan', Continent: 'Asia' },
        ],
      },
      {
        ...sqlIncludes("where co.continent = 'asia'"),
        rows: [
          { Name: 'Mumbai', Population: 10500000, CountryCode: 'IND', 'co.Name': 'India', Continent: 'Asia', Code: 'IND' },
          { Name: 'Karachi', Population: 9269265, CountryCode: 'PAK', 'co.Name': 'Pakistan', Continent: 'Asia', Code: 'PAK' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      "SELECT ci.Name AS city_name, ci.Population AS city_population, co.Name AS country_name, co.Continent FROM city ci INNER JOIN country co ON ci.CountryCode = co.Code WHERE co.Continent = 'Asia' ORDER BY ci.Population DESC, ci.Name",
      runner,
    );

    const orderStep = steps.find(step => step.name === 'ORDER BY');
    if (!orderStep) {
      throw new Error('Expected an ORDER BY step but none was produced.');
    }

    assert.deepEqual(orderStep.columns, ['city_name', 'city_population', 'country_name', 'Continent']);
    assert.deepEqual(orderStep.sortColumns, ['city_name', 'city_population']);
  });
};
