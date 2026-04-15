const { extractQuery, sanitizeSql } = require('../out/editor/queryExtractor');

function createEditor(text, options = {}) {
  const fileName = options.fileName ?? 'example.sql';
  const selectionStartOffset = options.selectionStartOffset ?? 0;
  const selectionEndOffset = options.selectionEndOffset ?? selectionStartOffset;
  const cursorOffset = options.cursorOffset ?? selectionEndOffset;

  const selectionStart = offsetToPosition(text, selectionStartOffset);
  const selectionEnd = offsetToPosition(text, selectionEndOffset);
  const active = offsetToPosition(text, cursorOffset);

  const document = {
    fileName,
    getText(range) {
      if (!range) {
        return text;
      }
      return text.slice(
        positionToOffset(text, range.start),
        positionToOffset(text, range.end),
      );
    },
    offsetAt(position) {
      return positionToOffset(text, position);
    },
    positionAt(offset) {
      return offsetToPosition(text, offset);
    },
  };

  return {
    document,
    selection: {
      start: selectionStart,
      end: selectionEnd,
      active,
      isEmpty:
        selectionStart.line === selectionEnd.line &&
        selectionStart.character === selectionEnd.character,
    },
  };
}

function offsetToPosition(text, offset) {
  const clamped = Math.max(0, Math.min(offset, text.length));
  const before = text.slice(0, clamped).split('\n');
  return {
    line: before.length - 1,
    character: before[before.length - 1].length,
  };
}

function positionToOffset(text, position) {
  const lines = text.split('\n');
  let offset = 0;

  for (let lineIndex = 0; lineIndex < position.line; lineIndex += 1) {
    offset += lines[lineIndex].length + 1;
  }

  return offset + position.character;
}

module.exports = function runQueryExtractorTests(runTest, assert) {
  runTest('extractQuery uses the selected text exactly when the user highlights a query', () => {
    const text = `
SELECT * FROM users;
SELECT * FROM orders WHERE status = 'open';
    `.trim();
    const selectedSql = "SELECT * FROM orders WHERE status = 'open';";
    const selectionStartOffset = text.indexOf(selectedSql);
    const selectionEndOffset = selectionStartOffset + selectedSql.length;
    const editor = createEditor(text, {
      selectionStartOffset,
      selectionEndOffset,
      cursorOffset: selectionEndOffset,
      fileName: 'dashboard.sql',
    });

    const result = extractQuery(editor);

    assert.ok(!('error' in result));
    assert.equal(result.sql, "SELECT * FROM orders WHERE status = 'open'");
    assert.equal(result.source, 'dashboard.sql (selection)');
  });

  runTest('extractQuery chooses the statement under the cursor when there is no selection', () => {
    const text = `
SELECT * FROM users;

SELECT id, total
FROM orders
WHERE total > 100;
    `.trim();
    const cursorOffset = text.indexOf('FROM orders');
    const editor = createEditor(text, { cursorOffset, fileName: 'orders.sql' });

    const result = extractQuery(editor);

    assert.ok(!('error' in result));
    assert.equal(
      result.sql,
      'SELECT id, total FROM orders WHERE total > 100',
    );
    assert.equal(result.source, 'orders.sql');
  });

  runTest('extractQuery reports a helpful error when the cursor is between queries with no active statement', () => {
    const text = `
SELECT * FROM users;

SELECT * FROM orders;
    `.trim();
    const cursorOffset = text.indexOf('\n\nSELECT * FROM orders');
    const editor = createEditor(text, { cursorOffset });

    const result = extractQuery(editor);

    assert.deepEqual(result, {
      error:
        'Could not determine which query to debug.\n' +
        'Place the cursor inside one query or select the query you want.',
    });
  });

  runTest('extractQuery rejects recursive CTE queries', () => {
    const editor = createEditor('WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t');

    const result = extractQuery(editor);

    assert.deepEqual(result, {
      error: 'SQL Debugger does not support recursive CTE yet.\n`WITH RECURSIVE` queries cannot be debugged.',
    });
  });

  runTest('extractQuery keeps a non-recursive CTE and its main SELECT as one statement under the cursor', () => {
    const sql = `
-- 9) CTE
WITH experienced_players AS (
    SELECT PlayerId, PlayerName, TeamId, Team, YearsInLeague
    FROM playerinfo
    WHERE YearsInLeague >= 5
)
SELECT e.PlayerId, e.PlayerName, e.Team, t.TeamId
FROM experienced_players e
INNER JOIN teaminfo t
  ON e.TeamId = t.TeamId
ORDER BY e.YearsInLeague DESC, e.PlayerName
LIMIT 20;
    `.trim();
    const cursorOffset = sql.indexOf('ORDER BY e.YearsInLeague DESC');

    const result = extractQuery(createEditor(sql, { cursorOffset }));

    assert.ok(!('error' in result));
    assert.equal(
      result.sql,
      'WITH experienced_players AS ( SELECT PlayerId, PlayerName, TeamId, Team, YearsInLeague FROM playerinfo WHERE YearsInLeague >= 5 ) SELECT e.PlayerId, e.PlayerName, e.Team, t.TeamId FROM experienced_players e INNER JOIN teaminfo t ON e.TeamId = t.TeamId ORDER BY e.YearsInLeague DESC, e.PlayerName LIMIT 20',
    );
  });

  runTest('extractQuery rejects non-SELECT statements such as INSERT, UPDATE, and DELETE', () => {
    for (const sql of [
      'INSERT INTO users (id) VALUES (1)',
      'UPDATE users SET active = 0',
      'DELETE FROM users WHERE id = 3',
    ]) {
      const result = extractQuery(createEditor(sql));
      assert.deepEqual(result, {
        error:
          'SQL Debugger only supports SELECT queries and non-recursive CTE queries.\n' +
          'INSERT, UPDATE, DELETE, DROP, and other statement types are not supported.',
      });
    }
  });

  runTest('extractQuery allows subqueries inside SELECT expressions', () => {
    const sql = `
      SELECT
        u.id,
        (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
      FROM users u;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok(!('error' in result));
    assert.equal(
      result.sql,
      'SELECT u.id, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u',
    );
  });

  runTest('sanitizeSql removes comments, collapses whitespace, and strips a trailing semicolon', () => {
    const sql = `
      /* dashboard query */
      SELECT
        id,
        name
      FROM users
      -- only active users
      WHERE active = 1;
    `;

    assert.equal(
      sanitizeSql(sql),
      'SELECT id, name FROM users WHERE active = 1',
    );
  });

  runTest('sanitizeSql returns an empty string when input only contains comments and whitespace', () => {
    const sql = `
      -- nothing here
      /* still nothing */
    `;

    assert.equal(sanitizeSql(sql), '');
  });

  runTest('sanitizeSql rejects multiple statements', () => {
    assert.throws(
      () => sanitizeSql('SELECT * FROM users; SELECT * FROM orders;'),
      /Only a single SQL statement is supported per debug run\./,
    );
  });
};
