const { detectDatabaseNameInSql } = require('../out/editor/databaseDetection');

module.exports = function runDatabaseDetectionTests(runTest, assert) {
  runTest('detectDatabaseNameInSql reads USE statements from the exact SQL text', () => {
    assert.equal(
      detectDatabaseNameInSql('USE analytics; SELECT * FROM users'),
      'analytics',
    );
  });

  runTest('detectDatabaseNameInSql reads database annotations from the exact SQL text', () => {
    assert.equal(
      detectDatabaseNameInSql('-- @db: reporting\nSELECT * FROM users'),
      'reporting',
    );
  });

  runTest('detectDatabaseNameInSql ignores unrelated file content when the selected SQL has no database marker', () => {
    assert.equal(
      detectDatabaseNameInSql('SELECT * FROM users WHERE id = 1'),
      undefined,
    );
  });
};
