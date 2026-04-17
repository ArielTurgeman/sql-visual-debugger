const { MysqlRunner, MYSQL_DEBUG_QUERY_TIMEOUT_MS } = require('../out/mysql/mysqlRunner');

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
};
