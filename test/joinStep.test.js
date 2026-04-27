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

module.exports = function runJoinStepTests(runTest, assert) {
  runTest('JOIN creates joined rows, detects relationship type, and preserves duplicate column names', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 2, team_id: 20, name: 'Linus' },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 100, team_id: 10, label: 'Red' },
          { id: 101, team_id: 10, label: 'Crimson' },
          { id: 200, team_id: 20, label: 'Blue' },
        ],
      },
      {
        ...sqlIncludes('select u.id, u.name, t.label from users u inner join teams t on u.team_id = t.team_id'),
        rows: [
          { id: 1, name: 'Ada', label: 'Red' },
          { id: 1, name: 'Ada', label: 'Crimson' },
          { id: 2, name: 'Linus', label: 'Blue' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, u.name, t.label FROM users u INNER JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    const selectStep = steps.find(step => step.name === 'SELECT');
    if (!joinStep) {
      throw new Error('Expected a JOIN step but none was produced.');
    }
    if (!selectStep) {
      throw new Error('Expected a SELECT step but none was produced.');
    }

    assert.deepEqual(steps.map(step => step.name), ['FROM', 'JOIN', 'SELECT']);
    assert.equal(joinStep.rowsBefore, 2);
    assert.equal(joinStep.rowsAfter, 3);
    assert.equal(joinStep.title, 'INNER JOIN');
    assert.equal(joinStep.sqlFragment, 'INNER JOIN teams t ON u.team_id = t.team_id');
    assert.deepEqual(joinStep.columns, ['id', 'team_id', 'name', 't.id', 't.team_id', 'label']);
    assert.deepEqual(joinStep.data, [
      { id: 1, team_id: 10, name: 'Ada', 't.id': 100, 't.team_id': 10, label: 'Red' },
      { id: 1, team_id: 10, name: 'Ada', 't.id': 101, 't.team_id': 10, label: 'Crimson' },
      { id: 2, team_id: 20, name: 'Linus', 't.id': 200, 't.team_id': 20, label: 'Blue' },
    ]);

    assert.ok(joinStep.joinMeta);
    assert.equal(joinStep.joinMeta.relationship, 'one-to-many');
    assert.equal(joinStep.joinMeta.rowDelta, 1);
    assert.equal(joinStep.joinMeta.leftTable, 'u');
    assert.equal(joinStep.joinMeta.rightTable, 't');
    assert.equal(joinStep.joinMeta.leftKey, 'u.team_id');
    assert.equal(joinStep.joinMeta.rightKey, 't.team_id');
    assert.equal(joinStep.joinMeta.leftKeyCol, 'team_id');
    assert.equal(joinStep.joinMeta.rightKeyCol, 'team_id');
    assert.deepEqual(joinStep.joinMeta.joinIndicatorColumns, ['t.id', 't.team_id', 'label']);
    assert.deepEqual(joinStep.joinMeta.joinedResultColumns, ['id', 'team_id', 'name', 't.id', 't.team_id', 'label']);

    assert.deepEqual(selectStep.data, [
      { id: 1, name: 'Ada', label: 'Red' },
      { id: 1, name: 'Ada', label: 'Crimson' },
      { id: 2, name: 'Linus', label: 'Blue' },
    ]);
  });

  runTest('LEFT JOIN preserves unmatched left rows with null right-side values', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 2, team_id: 99, name: 'Linus' },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 100, team_id: 10, label: 'Red' },
        ],
      },
      {
        ...sqlIncludes('select u.id, u.name, t.label from users u left join teams t on u.team_id = t.team_id'),
        rows: [
          { id: 1, name: 'Ada', label: 'Red' },
          { id: 2, name: 'Linus', label: null },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, u.name, t.label FROM users u LEFT JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.title, 'LEFT JOIN');
    assert.equal(joinStep.rowsBefore, 2);
    assert.equal(joinStep.rowsAfter, 2);
    assert.deepEqual(joinStep.data, [
      { id: 1, team_id: 10, name: 'Ada', 't.id': 100, 't.team_id': 10, label: 'Red' },
      { id: 2, team_id: 99, name: 'Linus', 't.id': null, 't.team_id': null, label: null },
    ]);
  });

  runTest('RIGHT JOIN preserves unmatched right rows with null left-side values', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 100, team_id: 10, label: 'Red' },
          { id: 200, team_id: 20, label: 'Blue' },
        ],
      },
      {
        ...sqlIncludes('select u.id, u.name, t.label from users u right join teams t on u.team_id = t.team_id'),
        rows: [
          { id: 1, name: 'Ada', label: 'Red' },
          { id: null, name: null, label: 'Blue' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, u.name, t.label FROM users u RIGHT JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.title, 'RIGHT JOIN');
    assert.equal(joinStep.rowsBefore, 1);
    assert.equal(joinStep.rowsAfter, 2);
    assert.deepEqual(joinStep.data, [
      { id: 1, team_id: 10, name: 'Ada', 't.id': 100, 't.team_id': 10, label: 'Red' },
      { id: null, team_id: null, name: null, 't.id': 200, 't.team_id': 20, label: 'Blue' },
    ]);
  });

  runTest('FULL OUTER JOIN preserves unmatched rows from both sides', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 2, team_id: 99, name: 'Linus' },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 100, team_id: 10, label: 'Red' },
          { id: 200, team_id: 20, label: 'Blue' },
        ],
      },
      {
        ...sqlIncludes('select u.id, u.name, t.label from users u full outer join teams t on u.team_id = t.team_id'),
        rows: [
          { id: 1, name: 'Ada', label: 'Red' },
          { id: 2, name: 'Linus', label: null },
          { id: null, name: null, label: 'Blue' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, u.name, t.label FROM users u FULL OUTER JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.title, 'FULL OUTER JOIN');
    assert.equal(joinStep.rowsBefore, 2);
    assert.equal(joinStep.rowsAfter, 3);
    assert.deepEqual(joinStep.data, [
      { id: 1, team_id: 10, name: 'Ada', 't.id': 100, 't.team_id': 10, label: 'Red' },
      { id: 2, team_id: 99, name: 'Linus', 't.id': null, 't.team_id': null, label: null },
      { id: null, team_id: null, name: null, 't.id': 200, 't.team_id': 20, label: 'Blue' },
    ]);
  });

  runTest('JOIN detects many-to-one relationships', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
          { id: 2, team_id: 10, name: 'Linus' },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 100, team_id: 10, label: 'Red' },
        ],
      },
      {
        ...sqlIncludes('select u.id, t.label from users u inner join teams t on u.team_id = t.team_id'),
        rows: [
          { id: 1, label: 'Red' },
          { id: 2, label: 'Red' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, t.label FROM users u INNER JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.joinMeta.relationship, 'many-to-one');
  });

  runTest('JOIN detects many-to-many relationships', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10 },
          { id: 2, team_id: 10 },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 100, team_id: 10 },
          { id: 101, team_id: 10 },
        ],
      },
      {
        ...sqlIncludes('select u.id, t.id from users u inner join teams t on u.team_id = t.team_id'),
        rows: [
          { id: 1, 't.id': 100 },
          { id: 1, 't.id': 101 },
          { id: 2, 't.id': 100 },
          { id: 2, 't.id': 101 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, t.id FROM users u INNER JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.joinMeta.relationship, 'many-to-many');
    assert.equal(joinStep.rowsAfter, 4);
  });

  runTest('INNER JOIN can produce zero rows when keys do not match', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `u`.* from users u'),
        rows: [
          { id: 1, team_id: 10, name: 'Ada' },
        ],
      },
      {
        ...sqlIncludes('select `t`.* from teams t'),
        rows: [
          { id: 200, team_id: 20, label: 'Blue' },
        ],
      },
      {
        ...sqlIncludes('select u.id, t.label from users u inner join teams t on u.team_id = t.team_id'),
        rows: [],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT u.id, t.label FROM users u INNER JOIN teams t ON u.team_id = t.team_id',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.rowsAfter, 0);
    assert.deepEqual(joinStep.data, []);
    assert.deepEqual(joinStep.columns, ['id', 'team_id', 'name', 't.id', 't.team_id', 'label']);
  });

  runTest('INNER JOIN does not match NULL join keys to NULL or empty-string keys', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `l`.* from qa_join_left l'),
        rows: [
          { id: 1, ref_code: null },
          { id: 2, ref_code: '' },
          { id: 3, ref_code: 'A' },
        ],
      },
      {
        ...sqlIncludes('select `r`.* from qa_join_right r'),
        rows: [
          { id: 10, ref_code: null },
          { id: 11, ref_code: '' },
          { id: 12, ref_code: 'A' },
        ],
      },
      {
        ...sqlIncludes('select l.id as left_id, l.ref_code as left_ref_code, r.id as right_id, r.ref_code as right_ref_code from qa_join_left l inner join qa_join_right r on l.ref_code = r.ref_code'),
        rows: [
          { left_id: 2, left_ref_code: '', right_id: 11, right_ref_code: '' },
          { left_id: 3, left_ref_code: 'A', right_id: 12, right_ref_code: 'A' },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT l.id AS left_id, l.ref_code AS left_ref_code, r.id AS right_id, r.ref_code AS right_ref_code FROM qa_join_left l INNER JOIN qa_join_right r ON l.ref_code = r.ref_code',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.rowsBefore, 3);
    assert.equal(joinStep.rowsAfter, 2);
    assert.deepEqual(joinStep.data, [
      { id: 2, ref_code: '', 'r.id': 11, 'r.ref_code': '' },
      { id: 3, ref_code: 'A', 'r.id': 12, 'r.ref_code': 'A' },
    ]);
    assert.equal(joinStep.joinMeta.relationship, 'one-to-one');
  });

  runTest('JOIN relationship inference ignores NULL keys but still treats empty strings as real duplicates', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `l`.* from qa_join_left l'),
        rows: [
          { id: 1, ref_code: null },
          { id: 2, ref_code: '' },
          { id: 3, ref_code: '' },
        ],
      },
      {
        ...sqlIncludes('select `r`.* from qa_join_right r'),
        rows: [
          { id: 10, ref_code: '' },
        ],
      },
      {
        ...sqlIncludes('select l.id, r.id from qa_join_left l inner join qa_join_right r on l.ref_code = r.ref_code'),
        rows: [
          { id: 2, 'r.id': 10 },
          { id: 3, 'r.id': 10 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT l.id, r.id FROM qa_join_left l INNER JOIN qa_join_right r ON l.ref_code = r.ref_code',
      runner,
    );

    const joinStep = steps.find(step => step.name === 'JOIN');
    if (!joinStep) throw new Error('Expected a JOIN step but none was produced.');

    assert.equal(joinStep.joinMeta.relationship, 'many-to-one');
    assert.equal(joinStep.rowsAfter, 2);
  });

  runTest('chained JOIN steps use the qualified duplicate left key when later joins reference it', async () => {
    const runner = new FakeRunner([
      {
        ...sqlIncludes('select `c`.* from qa_customers c'),
        rows: [
          { id: 1, code: 'C001', region_id: 1, status: 'active' },
          { id: 3, code: 'C003', region_id: 2, status: 'inactive' },
        ],
      },
      {
        ...sqlIncludes('select `o`.* from qa_orders o'),
        rows: [
          { id: 10, customer_id: 1, status: 'open' },
          { id: 13, customer_id: 3, status: 'open' },
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
        ...sqlIncludes('select c.id, o.id as order_id, p.id as payment_id from qa_customers c inner join qa_orders o on c.id = o.customer_id inner join qa_payments p on o.id = p.order_id'),
        rows: [
          { id: 1, order_id: 10, payment_id: 100 },
          { id: 3, order_id: 13, payment_id: 103 },
        ],
      },
    ]);

    const steps = await executeDebugSteps(
      'SELECT c.id, o.id AS order_id, p.id AS payment_id FROM qa_customers c INNER JOIN qa_orders o ON c.id = o.customer_id INNER JOIN qa_payments p ON o.id = p.order_id',
      runner,
    );

    const joinSteps = steps.filter(step => step.name === 'JOIN');
    if (joinSteps.length !== 2) {
      throw new Error(`Expected 2 JOIN steps but found ${joinSteps.length}.`);
    }

    assert.equal(joinSteps[1].rowsBefore, 2);
    assert.equal(joinSteps[1].rowsAfter, 2);
    assert.equal(joinSteps[1].joinMeta.leftKey, 'o.id');
    assert.equal(joinSteps[1].joinMeta.leftKeyCol, 'o.id');
    assert.equal(joinSteps[1].joinMeta.rightKeyCol, 'order_id');
    assert.deepEqual(joinSteps[1].data, [
      { id: 1, code: 'C001', region_id: 1, status: 'active', 'o.id': 10, customer_id: 1, 'o.status': 'open', 'p.id': 100, order_id: 10, 'p.status': 'paid', amount: 100 },
      { id: 3, code: 'C003', region_id: 2, status: 'inactive', 'o.id': 13, customer_id: 3, 'o.status': 'open', 'p.id': 103, order_id: 13, 'p.status': 'paid', amount: 0 },
    ]);
  });

  runTest('unsupported non-equality JOIN conditions are rejected clearly', async () => {
    const runner = new FakeRunner([]);

    await assert.rejects(
      () => executeDebugSteps(
        'SELECT u.id FROM users u INNER JOIN teams t ON u.team_id > t.team_id',
        runner,
      ),
      /Only simple equality JOIN conditions are supported in this MVP\./,
    );
  });
};
