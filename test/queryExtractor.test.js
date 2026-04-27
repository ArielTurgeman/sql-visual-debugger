const { extractQuery, sanitizeSql } = require('../out/editor/queryExtractor');
const { parseSelectQuery, normalizeSqlForParsing } = require('../out/engine/stepEngineParsing');

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

  runTest('extractQuery rejects an extra trailing semicolon even in cursor-based extraction mode', () => {
    const text = 'SELECT * FROM playerinfo;;';
    const cursorOffset = text.indexOf('playerinfo');
    const editor = createEditor(text, { cursorOffset, fileName: 'players.sql' });

    const result = extractQuery(editor);

    assert.deepEqual(result, {
      error:
        'Only a single SQL statement is supported per debug run.\n' +
        'Remove the extra semicolon or select just one query.',
    });
  });

  runTest('extractQuery reports a helpful error when the cursor is between queries with no active statement', () => {
    const text = `
SELECT * FROM users;

SELECT * FROM orders;
    `.trim();
    const cursorOffset = text.indexOf('\n\nSELECT * FROM orders') + 1;
    const editor = createEditor(text, { cursorOffset });

    const result = extractQuery(editor);

    assert.deepEqual(result, {
      error:
        'Could not determine which query to debug.\n' +
        'Place the cursor inside one query or select the query you want.',
    });
  });

  runTest('extractQuery keeps the cursor with the preceding query when it sits immediately after that query semicolon', () => {
    const text = `
SELECT * FROM users;
 SELECT * FROM orders;
    `.trim();
    const cursorOffset = text.indexOf(';') + 1;
    const editor = createEditor(text, { cursorOffset, fileName: 'two-queries.sql' });

    const result = extractQuery(editor);

    assert.ok(!('error' in result));
    assert.equal(result.sql, 'SELECT * FROM users');
    assert.equal(result.source, 'two-queries.sql');
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

  runTest('extractQuery rejects scalar subqueries inside SELECT expressions', () => {
    const sql = `
      SELECT
        u.id,
        (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
      FROM users u;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok('error' in result);
    assert.match(result.error, /scalar subqueries in the SELECT list/i);
  });

  runTest('extractQuery allows a simple uncorrelated aggregate scalar subquery in the SELECT list', () => {
    const sql = `
      SELECT
          p.PlayerId,
          p.PlayerName,
          (
              SELECT
                  AVG(Age)
              FROM playerinfo
          ) AS league_avg_age
      FROM playerinfo p
      ORDER BY p.PlayerName
      LIMIT 20;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok(!('error' in result));
    assert.equal(
      result.sql,
      'SELECT p.PlayerId, p.PlayerName, ( SELECT AVG(Age) FROM playerinfo ) AS league_avg_age FROM playerinfo p ORDER BY p.PlayerName LIMIT 20',
    );
  });

  runTest('extractQuery still rejects correlated scalar subqueries in the SELECT list', () => {
    const sql = `
      SELECT
        p.PlayerId,
        (
          SELECT AVG(Age)
          FROM playerinfo
          WHERE TeamId = p.TeamId
        ) AS team_avg_age
      FROM playerinfo p;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok('error' in result);
    assert.match(result.error, /uncorrelated scalar subqueries|Correlated projected subqueries/i);
  });

  runTest('extractQuery still rejects grouped scalar subqueries in the SELECT list', () => {
    const sql = `
      SELECT
        p.PlayerId,
        (
          SELECT AVG(Age)
          FROM playerinfo
          GROUP BY TeamId
        ) AS grouped_avg_age
      FROM playerinfo p;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok('error' in result);
    assert.match(result.error, /JOIN, GROUP BY, HAVING, ORDER BY, or LIMIT/i);
  });

  runTest('extractQuery allows a simple FROM subquery block when the projected columns are plain', () => {
    const sql = `
      SELECT
          *
      FROM (
          SELECT
              PlayerId,
              TeamId
          FROM playerinfo
      ) sub
      WHERE sub.PlayerId > 10;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok(!('error' in result));
    assert.equal(
      result.sql,
      'SELECT * FROM ( SELECT PlayerId, TeamId FROM playerinfo ) sub WHERE sub.PlayerId > 10',
    );
  });

  runTest('extractQuery allows simple qualified equality joins with and without backticks', () => {
    const cases = [
      'SELECT * FROM playerinfo JOIN teaminfo ON teaminfo.TeamId = playerinfo.TeamId',
      'SELECT * FROM playerinfo JOIN teaminfo ON teaminfo.`TeamId` = playerinfo.`TeamId`',
    ];

    for (const sql of cases) {
      const result = extractQuery(createEditor(sql));
      assert.ok(!('error' in result), `Expected query to be allowed: ${sql}`);
    }
  });

  runTest('extractQuery rejects unsafe SELECT write-like and locking modifiers', () => {
    const cases = [
      {
        sql: "SELECT * INTO OUTFILE '/tmp/users.csv' FROM users",
        pattern: /into outfile|select .* into .*not supported/i,
      },
      {
        sql: "SELECT * INTO DUMPFILE '/tmp/users.bin' FROM users",
        pattern: /into dumpfile|select .* into .*not supported/i,
      },
      {
        sql: 'SELECT id INTO @debug_user_id FROM users LIMIT 1',
        pattern: /select .* into .*not supported|variable assignment/i,
      },
      {
        sql: 'SELECT * FROM users FOR UPDATE',
        pattern: /FOR UPDATE/i,
      },
      {
        sql: 'SELECT * FROM users LOCK IN SHARE MODE',
        pattern: /LOCK IN SHARE MODE/i,
      },
      {
        sql: "SELECT 'FOR UPDATE' AS example FROM users",
        pattern: null,
      },
    ];

    for (const testCase of cases) {
      const result = extractQuery(createEditor(testCase.sql));
      if (testCase.pattern === null) {
        assert.ok(!('error' in result), `Expected query to be allowed: ${testCase.sql}`);
      } else {
        assert.ok('error' in result, `Expected query to be rejected: ${testCase.sql}`);
        assert.match(result.error, testCase.pattern);
      }
    }
  });

  runTest('extractQuery rejects policy-blocked query shapes explicitly', () => {
    const cases = [
      {
        sql: 'SELECT id FROM users INTERSECT SELECT id FROM admins',
        pattern: /INTERSECT/i,
      },
      {
        sql: 'SELECT id FROM users EXCEPT SELECT id FROM admins',
        pattern: /EXCEPT/i,
      },
      {
        sql: 'SELECT * FROM users NATURAL JOIN teams',
        pattern: /NATURAL JOIN/i,
      },
      {
        sql: 'SELECT * FROM users FULL OUTER JOIN teams ON users.team_id = teams.id',
        pattern: /FULL OUTER JOIN/i,
      },
      {
        sql: 'SELECT * FROM users u JOIN teams t ON u.team_id = t.id AND u.active = t.active',
        pattern: /simple equality JOIN conditions/i,
      },
      {
        sql: 'SELECT * FROM users u JOIN teams t ON LOWER(u.email) = LOWER(t.email)',
        pattern: /simple equality JOIN conditions/i,
      },
      {
        sql: 'SELECT id, SUM(score) OVER (ORDER BY score ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling_score FROM results',
        pattern: /window frame clauses/i,
      },
      {
        sql: 'SELECT id, SUM(score) OVER win FROM results WINDOW win AS (PARTITION BY team_id ORDER BY score)',
        pattern: /named windows/i,
      },
    ];

    for (const testCase of cases) {
      const result = extractQuery(createEditor(testCase.sql));
      assert.ok('error' in result, `Expected query to be rejected: ${testCase.sql}`);
      assert.match(result.error, testCase.pattern);
    }
  });

  runTest('extractQuery rejects read-only query shapes the debugger cannot visualize reliably yet', () => {
    const cases = [
      {
        sql: 'SELECT id FROM users UNION SELECT id FROM admins',
        pattern: /UNION|set-operation/i,
      },
      {
        sql: 'SELECT id FROM users UNION ALL SELECT id FROM admins',
        pattern: /UNION|set-operation/i,
      },
      {
        sql: 'SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)',
        pattern: /EXISTS/i,
      },
      {
        sql: 'SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)',
        pattern: /NOT EXISTS|EXISTS/i,
      },
      {
        sql: 'SELECT * FROM users JOIN teams USING (team_id)',
        pattern: /USING/i,
      },
      {
        sql: 'SELECT * FROM users CROSS JOIN teams',
        pattern: /CROSS JOIN/i,
      },
      {
        sql: 'SELECT * FROM users u JOIN (SELECT id FROM teams) t ON u.team_id = t.id',
        pattern: /derived tables/i,
      },
      {
        sql: 'SELECT id, LAG(score) OVER (ORDER BY score) AS prev_score FROM results',
        pattern: /LAG|window functions/i,
      },
      {
        sql: 'SELECT id, NTILE(4) OVER (ORDER BY score) AS quartile FROM results',
        pattern: /NTILE|window functions/i,
      },
      {
        sql: 'SELECT DATABASE()',
        pattern: /DATABASE\(\)|Function-only queries/i,
      },
      {
        sql: 'SELECT team_id, COUNT(*) FROM users GROUP BY team_id WITH ROLLUP',
        pattern: /WITH ROLLUP/i,
      },
    ];

    for (const testCase of cases) {
      const result = extractQuery(createEditor(testCase.sql));
      assert.ok('error' in result, `Expected query to be rejected: ${testCase.sql}`);
      assert.match(result.error, testCase.pattern);
    }
  });

  runTest('extractQuery rejects malformed select shapes before opening the debugger', () => {
    const cases = [
      {
        sql: 'SELECT FROM users',
        pattern: /could not understand|FROM clause/i,
      },
      {
        sql: 'SELECT id, FROM users',
        pattern: /could not understand|Unsupported or malformed SQL|FROM clause/i,
      },
      {
        sql: 'SELECT * FROM users LEFT JOIN teams',
        pattern: /could not understand|JOIN without ON/i,
      },
    ];

    for (const testCase of cases) {
      const result = extractQuery(createEditor(testCase.sql));
      assert.ok('error' in result, `Expected malformed query to be rejected: ${testCase.sql}`);
      assert.match(result.error, testCase.pattern);
    }
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

  runTest('sanitizeSql preserves semicolons double spaces and comment markers inside quoted strings', () => {
    const sql = `
      SELECT
        'A;B' AS code_value,
        'needs  review' AS spaced_value,
        'pre--launch' AS dashed_value,
        'keep /* text */ here' AS comment_like_value
      FROM city
      LIMIT 1;
    `;

    assert.equal(
      sanitizeSql(sql),
      "SELECT 'A;B' AS code_value, 'needs  review' AS spaced_value, 'pre--launch' AS dashed_value, 'keep /* text */ here' AS comment_like_value FROM city LIMIT 1",
    );
  });

  runTest('extractQuery allows quoted string contents that contain comment markers and repeated spaces', () => {
    const sql = `
      SELECT
        ID,
        Name,
        District
      FROM city
      WHERE District = 'Southeast Asia'
         OR District = 'needs  review'
         OR Name = 'pre--launch'
      ORDER BY Name;
    `;

    const result = extractQuery(createEditor(sql));

    assert.ok(!('error' in result));
    assert.equal(
      result.sql,
      "SELECT ID, Name, District FROM city WHERE District = 'Southeast Asia' OR District = 'needs  review' OR Name = 'pre--launch' ORDER BY Name",
    );
  });

  runTest('parseSelectQuery preserves quoted literal contents while normalizing outer whitespace', () => {
    const parsed = parseSelectQuery(`
      SELECT
        'A;B' AS code_value,
        'needs  review' AS spaced_value,
        'pre--launch' AS dashed_value
      FROM city
      WHERE Name = 'pre--launch'
      ORDER BY Name;
    `);

    assert.equal(
      parsed.selectClause,
      "SELECT 'A;B' AS code_value, 'needs  review' AS spaced_value, 'pre--launch' AS dashed_value",
    );
    assert.equal(parsed.whereClause, "WHERE Name = 'pre--launch'");
    assert.equal(parsed.orderByClause, 'ORDER BY Name');
  });

  runTest('normalizeSqlForParsing collapses whitespace only outside quoted strings', () => {
    assert.equal(
      normalizeSqlForParsing("SELECT  'needs  review'  AS value,  'pre--launch'  AS label  FROM city  "),
      "SELECT 'needs  review' AS value, 'pre--launch' AS label FROM city",
    );
  });
};
