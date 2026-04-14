const { parseQueryBlocks } = require('../out/engine/queryBlocks');

module.exports = function runQueryBlockTests(runTest, assert) {
  runTest('parseQueryBlocks returns a single main block for a simple select', () => {
    const blocks = parseQueryBlocks('SELECT id, name FROM users WHERE active = 1;');

    assert.equal(blocks.length, 1);
    assert.deepEqual(
      blocks.map(block => ({
        type: block.type,
        name: block.name,
        sql: block.sql,
        dependencies: block.dependencies,
      })),
      [
        {
          type: 'main',
          name: 'Main Query',
          sql: 'SELECT id, name FROM users WHERE active = 1',
          dependencies: [],
        },
      ],
    );
  });

  runTest('parseQueryBlocks parses a non-recursive CTE and tracks the main-query dependency', () => {
    const sql = `
      WITH filtered_users AS (
        SELECT id, team_id
        FROM users
        WHERE active = 1
      )
      SELECT *
      FROM filtered_users
    `;

    const blocks = parseQueryBlocks(sql);

    assert.equal(blocks.length, 2);
    assert.equal(blocks[0].type, 'cte');
    assert.equal(blocks[0].name, 'filtered_users');
    assert.equal(blocks[0].materializedName, 'filtered_users');
    assert.equal(
      blocks[0].sql,
      'SELECT id, team_id FROM users WHERE active = 1',
    );

    assert.equal(blocks[1].type, 'main');
    assert.equal(blocks[1].name, 'Main Query');
    assert.deepEqual(blocks[1].dependencies, [
      {
        name: 'filtered_users',
        tableName: 'filtered_users',
        blockType: 'cte',
      },
    ]);
  });

  runTest('parseQueryBlocks expands a simple FROM subquery into a separate block', () => {
    const sql = `
      SELECT sub.id
      FROM (
        SELECT id
        FROM users
      ) AS sub
      WHERE sub.id > 10
    `;

    const blocks = parseQueryBlocks(sql);

    assert.equal(blocks.length, 2);

    assert.equal(blocks[0].type, 'subquery');
    assert.equal(blocks[0].name, 'sub');
    assert.match(blocks[0].materializedName, /^__sql_debug_subquery_/);
    assert.equal(blocks[0].sql, 'SELECT id FROM users');

    assert.equal(blocks[1].type, 'main');
    assert.equal(blocks[1].fromSource.kind, 'subquery');
    assert.equal(blocks[1].fromSource.name, 'sub');
    assert.equal(blocks[1].fromSource.alias, 'sub');
    assert.match(blocks[1].fromSource.tableName, /^__sql_debug_subquery_/);
    assert.deepEqual(blocks[1].dependencies, [
      {
        name: 'sub',
        tableName: blocks[0].materializedName,
        blockType: 'subquery',
      },
    ]);
  });

  runTest('parseQueryBlocks rejects recursive CTEs', () => {
    assert.throws(
      () => parseQueryBlocks('WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t'),
      /Recursive CTE is not supported yet\./,
    );
  });
};
