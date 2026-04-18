const {
  isLocalMysqlHost,
  getLocalOnlyHostError,
  assertLocalOnlyServer,
} = require('../out/mysql/localHostPolicy');

module.exports = function runLocalHostPolicyTests(runTest, assert) {
  runTest('isLocalMysqlHost accepts supported local MySQL hosts', () => {
    assert.equal(isLocalMysqlHost('localhost'), true);
    assert.equal(isLocalMysqlHost('127.0.0.1'), true);
    assert.equal(isLocalMysqlHost('::1'), true);
    assert.equal(isLocalMysqlHost(' LOCALHOST '), true);
  });

  runTest('getLocalOnlyHostError rejects remote MySQL hosts clearly', () => {
    assert.match(
      getLocalOnlyHostError('db.internal'),
      /local mysql connections only/i,
    );
  });

  runTest('assertLocalOnlyServer throws for remote MySQL hosts', () => {
    assert.throws(
      () => assertLocalOnlyServer({ host: '192.168.1.50', port: 3306, user: 'root' }),
      /local mysql connections only/i,
    );
  });
};
