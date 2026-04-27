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

  runTest('GROUP BY preserves qualified aggregate source columns when different tables share the same bare name', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `co`.* from country co'),
        rows: [
          { Code: 'IND', Continent: 'Asia', Population: 1000000 },
          { Code: 'PAK', Continent: 'Asia', Population: 900000 },
        ],
      },
      {
        ...sqlIncludes('select `ci`.* from city ci'),
        rows: [
          { Id: 1, Population: 500000, CountryCode: 'IND' },
          { Id: 2, Population: 300000, CountryCode: 'IND' },
          { Id: 3, Population: 400000, CountryCode: 'PAK' },
        ],
      },
      {
        ...sqlIncludes('group by co.continent'),
        rows: [
          { Continent: 'Asia', avg_city_population: 400000, biggest_city_population: 500000 },
        ],
      },
      {
        ...sqlIncludes('select co.continent, avg(ci.population) as avg_city_population, max(ci.population) as biggest_city_population'),
        rows: [
          { Continent: 'Asia', avg_city_population: 400000, biggest_city_population: 500000 },
        ],
      },
      {
        ...sqlIncludes('where co.population >= 900000'),
        rows: [
          { Code: 'IND', Continent: 'Asia', Population: 1000000, Id: 1, 'ci.Population': 500000, CountryCode: 'IND' },
          { Code: 'IND', Continent: 'Asia', Population: 1000000, Id: 2, 'ci.Population': 300000, CountryCode: 'IND' },
          { Code: 'PAK', Continent: 'Asia', Population: 900000, Id: 3, 'ci.Population': 400000, CountryCode: 'PAK' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT co.Continent, AVG(ci.Population) AS avg_city_population, MAX(ci.Population) AS biggest_city_population FROM country co INNER JOIN city ci ON co.Code = ci.CountryCode WHERE co.Population >= 900000 GROUP BY co.Continent',
      runner,
    );

    const groupStep = steps.find(step => step.name === 'GROUP BY');
    if (!groupStep) {
      throw new Error('Expected a GROUP BY step but none was produced.');
    }

    assert.deepEqual(groupStep.groupByColumns, ['Continent']);
    assert.deepEqual(groupStep.aggColumns, [
      { col: 'avg_city_population', fn: 'AVG', srcCol: 'ci.Population' },
      { col: 'biggest_city_population', fn: 'MAX', srcCol: 'ci.Population' },
    ]);
    assert.deepEqual(groupStep.preGroupColumns, ['Code', 'Continent', 'Population', 'Id', 'ci.Population', 'CountryCode']);
  });

  runTest('GROUP BY keeps qualified source keys for breakdown when grouped output has a duplicate bare name', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, status: 'active' },
          { id: 2, status: 'active' },
        ],
      },
      {
        ...sqlIncludes('select `o`.* from qa_orders o'),
        rows: [
          { id: 10, customer_id: 1, status: 'open', amount: 100 },
          { id: 11, customer_id: 1, status: 'open', amount: 100 },
          { id: 12, customer_id: 2, status: 'closed', amount: 50 },
        ],
      },
      {
        ...sqlIncludes('group by o.status'),
        rows: [
          { status: 'open', order_count: 2, total_amount: 200 },
          { status: 'closed', order_count: 1, total_amount: 50 },
        ],
      },
      {
        ...sqlIncludes('select o.status, count(*) as order_count, sum(o.amount) as total_amount'),
        rows: [
          { status: 'open', order_count: 2, total_amount: 200 },
          { status: 'closed', order_count: 1, total_amount: 50 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT o.status, COUNT(*) AS order_count, SUM(o.amount) AS total_amount FROM qa_customers c INNER JOIN qa_orders o ON c.id = o.customer_id GROUP BY o.status ORDER BY order_count DESC, o.status',
      runner,
    );

    const groupStep = steps.find(step => step.name === 'GROUP BY');
    if (!groupStep) {
      throw new Error('Expected a GROUP BY step but none was produced.');
    }

    assert.deepEqual(groupStep.columns, ['status', 'order_count', 'total_amount']);
    assert.deepEqual(groupStep.groupByColumns, ['status']);
    assert.deepEqual(groupStep.groupBySourceColumns, ['o.status']);
    assert.deepEqual(groupStep.preGroupColumns, ['id', 'status', 'o.id', 'customer_id', 'o.status', 'amount']);
  });

  runTest('GROUP BY preserves output key order when mixing aliases and qualified subquery columns', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `sub`.*, `c`.* from (select o.customer_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id'),
        rows: [
          { customer_id: 1, order_status: 'open', amount: 100, id: 1, status: 'active' },
          { customer_id: 1, order_status: 'open', amount: 100, id: 1, status: 'active' },
          { customer_id: 2, order_status: 'closed', amount: null, id: 2, status: 'active' },
          { customer_id: 3, order_status: 'open', amount: 0, id: 3, status: 'inactive' },
        ],
      },
      {
        ...sqlIncludes('select c.status as customer_status, sub.order_status, count(*) as row_count, sum(sub.amount) as total_amount from (select o.customer_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub inner join qa_customers c on sub.customer_id = c.id group by c.status, sub.order_status'),
        rows: [
          { customer_status: 'active', order_status: 'open', row_count: 4, total_amount: 325 },
          { customer_status: 'inactive', order_status: 'open', row_count: 1, total_amount: 0 },
          { customer_status: 'active', order_status: 'closed', row_count: 1, total_amount: 100 },
        ],
      },
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, status: 'active' },
          { id: 2, status: 'active' },
          { id: 3, status: 'inactive' },
          { id: 4, status: 'active' },
          { id: 5, status: 'active' },
          { id: 6, status: 'active' },
        ],
      },
      {
        ...sqlIncludes('select `sub`.* from (select o.customer_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null) sub'),
        rows: [
          { customer_id: 1, order_status: 'open', amount: 100 },
          { customer_id: 1, order_status: 'open', amount: 100 },
          { customer_id: 3, order_status: 'open', amount: 0 },
          { customer_id: 4, order_status: 'open', amount: 50 },
          { customer_id: 5, order_status: 'open', amount: 75 },
          { customer_id: 6, order_status: 'closed', amount: 100 },
        ],
      },
      {
        ...sqlIncludes('select `o`.* from qa_orders o'),
        rows: [
          { customer_id: 1, status: 'open', amount: 100 },
          { customer_id: 1, status: 'open', amount: 100 },
          { customer_id: 3, status: 'open', amount: 0 },
          { customer_id: 4, status: 'open', amount: 50 },
          { customer_id: 5, status: 'open', amount: 75 },
          { customer_id: 6, status: 'closed', amount: 100 },
        ],
      },
      {
        ...sqlIncludes('select `o`.`customer_id`, `o`.`status`, `o`.`amount` from qa_orders o where o.amount is not null'),
        rows: [
          { customer_id: 1, status: 'open', amount: 100 },
          { customer_id: 1, status: 'open', amount: 100 },
          { customer_id: 3, status: 'open', amount: 0 },
          { customer_id: 4, status: 'open', amount: 50 },
          { customer_id: 5, status: 'open', amount: 75 },
          { customer_id: 6, status: 'closed', amount: 100 },
        ],
      },
      {
        ...sqlIncludes('select o.customer_id, o.status as order_status, o.amount from qa_orders o where o.amount is not null'),
        rows: [
          { customer_id: 1, order_status: 'open', amount: 100 },
          { customer_id: 1, order_status: 'open', amount: 100 },
          { customer_id: 3, order_status: 'open', amount: 0 },
          { customer_id: 4, order_status: 'open', amount: 50 },
          { customer_id: 5, order_status: 'open', amount: 75 },
          { customer_id: 6, order_status: 'closed', amount: 100 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      `
      SELECT
        c.status AS customer_status,
        sub.order_status,
        COUNT(*) AS row_count,
        SUM(sub.amount) AS total_amount
      FROM (
        SELECT
          o.customer_id,
          o.status AS order_status,
          o.amount
        FROM qa_orders o
        WHERE o.amount IS NOT NULL
      ) sub
      INNER JOIN qa_customers c ON sub.customer_id = c.id
      GROUP BY c.status, sub.order_status
      HAVING COUNT(*) >= 1
      ORDER BY customer_status, total_amount DESC
      `,
      runner,
    );

    const groupStep = steps.find(step => step.name === 'GROUP BY');
    if (!groupStep) {
      throw new Error('Expected a GROUP BY step but none was produced.');
    }

    assert.deepEqual(groupStep.groupByColumns, ['customer_status', 'order_status']);
    assert.deepEqual(groupStep.groupBySourceColumns, ['c.status', 'sub.order_status']);
  });
};
