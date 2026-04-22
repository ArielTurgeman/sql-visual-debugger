const {
  MysqlRunner,
  MYSQL_DEBUG_QUERY_TIMEOUT_MS,
  normalizeMysqlConnectionError,
} = require('../out/mysql/mysqlRunner');

module.exports = function runMysqlRunnerTests(runTest, assert) {
  runTest('MysqlRunner applies the configured query timeout to executions', async () => {
    const runner = new MysqlRunner();
    const calls = [];
    runner.connection = {
      async execute(options) {
        calls.push(options);
        return [[{ id: 1 }]];
      },
    };

    const rows = await runner.query('SELECT 1');

    assert.deepEqual(rows, [{ id: 1 }]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].sql, 'SELECT 1');
    assert.equal(calls[0].timeout, MYSQL_DEBUG_QUERY_TIMEOUT_MS);
  });

  runTest('MysqlRunner fails clearly when a query times out', async () => {
    const runner = new MysqlRunner();
    runner.connection = {
      async execute() {
        const error = new Error('Query inactivity timeout');
        error.code = 'PROTOCOL_SEQUENCE_TIMEOUT';
        throw error;
      },
    };

    await assert.rejects(
      () => runner.query('SELECT SLEEP(15)'),
      /timed out after/i,
    );
  });

  runTest('MysqlRunner maps wrong host or port connection failures to a visible message', () => {
    const error = new Error('');
    error.code = 'ECONNREFUSED';

    const normalized = normalizeMysqlConnectionError(error, {
      host: 'localhost',
      port: 3307,
      user: 'root',
      database: 'world',
    });

    assert.match(normalized.message, /could not connect to mysql at localhost:3307/i);
    assert.match(normalized.message, /host and port are correct/i);
    assert.equal(normalized.code, 'ECONNREFUSED');
  });

  runTest('MysqlRunner falls back to a non-empty connection message when mysql returns none', () => {
    const normalized = normalizeMysqlConnectionError({ code: 'UNKNOWN' }, {
      host: 'localhost',
      port: 3306,
      user: 'root',
      database: 'world',
    });

    assert.match(normalized.message, /could not connect to mysql at localhost:3306/i);
    assert.match(normalized.message, /check the host, port, username, password, and database name/i);
  });

  runTest('MysqlRunner rejects non-read-only SQL before execution', async () => {
    const runner = new MysqlRunner();
    let executed = false;
    runner.connection = {
      async execute() {
        executed = true;
        return [[]];
      },
    };

    await assert.rejects(
      () => runner.query('DELETE FROM users'),
      /only runs read-only select\/with statements/i,
    );
    assert.equal(executed, false);
  });

  runTest('MysqlRunner disables the generic execute API', async () => {
    const runner = new MysqlRunner();

    await assert.rejects(
      () => runner.execute('DROP TABLE users'),
      /unsafe execution api disabled/i,
    );
  });

  runTest('MysqlRunner rolls back the read-only session before disconnecting', async () => {
    const runner = new MysqlRunner();
    const calls = [];
    runner.connection = {
      async query(sql) {
        calls.push(sql);
        return [];
      },
      async end() {
        calls.push('END');
      },
    };

    await runner.disconnect();

    assert.deepEqual(calls, ['ROLLBACK', 'END']);
    assert.equal(runner.isConnected(), false);
  });
};
