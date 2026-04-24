# SQL Visual Debugger

Why did this query return these rows?

SQL Visual Debugger helps you step through supported MySQL `SELECT` queries in SQL execution order and inspect the intermediate result after each stage inside VS Code.

Instead of only showing the final output, it helps you see how rows were loaded, joined, filtered, grouped, deduplicated, sorted, limited, and otherwise transformed through the query.

It is built for read-only debugging of supported MySQL query flows in v1.

<img src="https://raw.githubusercontent.com/ArielTurgeman/sql-visual-debugger/main/images/marketplace/where-v2.png" alt="SQL Visual Debugger WHERE step" width="430" />

## How It Works

Open a `.sql` file, select a query or place your cursor inside one, then right-click and choose `SQL Debugger: Debug Query`.

The extension prompts for local MySQL connection details when needed, then opens a debugger panel and walks through the query step by step in SQL execution order.

That means you can inspect what happened at each stage instead of only seeing the final result.

<img src="https://raw.githubusercontent.com/ArielTurgeman/sql-visual-debugger/main/images/marketplace/rightclick-v2.png" alt="Right-click to debug a query" width="430" />

## See What The JOIN Did

When a query joins tables, the debugger shows both sides of the join, the join condition, the row-count change, and the joined result.

This makes it much easier to understand why rows matched, duplicated, or disappeared.

<img src="https://raw.githubusercontent.com/ArielTurgeman/sql-visual-debugger/main/images/marketplace/join-v2.png" alt="JOIN debugging example" width="430" />

## See How GROUP BY Changes The Data

For supported grouped queries, the debugger shows the grouped output and lets you inspect the rows that contributed to each group.

This helps explain aggregation instead of making it feel like a black box.

<img src="https://raw.githubusercontent.com/ArielTurgeman/sql-visual-debugger/main/images/marketplace/groupby-v2.png" alt="GROUP BY debugging example" width="430" />

SQL Visual Debugger also helps you inspect filtering, projection, deduplication, sorting, limits, supported CTE flows, supported subqueries, and more.

## What It Helps You Understand

- `FROM` - see the starting row set before later steps change it
- `JOIN` - understand how rows matched across tables and why row counts changed
- `WHERE` - see which rows were filtered out and why
- `GROUP BY` - see how rows were grouped and which rows contributed to each group
- `HAVING` - see which grouped rows were removed after aggregation
- `SELECT` - understand the final projected columns and derived values
- `DISTINCT` - see which duplicate rows were removed at the deduplication stage
- `ORDER BY` - see how the final result was sorted
- `LIMIT` - see where the result was truncated
- `CASE` - understand how conditional output values were chosen
- `Window Functions` - inspect supported ranking and windowed calculations with extra context
- `WITH` CTEs - follow supported non-recursive CTE flows as part of the execution path
- `FROM` subqueries - understand how supported derived tables feed the outer query
- `WHERE IN` subqueries - inspect supported subquery-based filtering
- scalar subqueries in `WHERE` - understand supported subquery comparisons in filters

## What V1 Supports

SQL Visual Debugger currently supports supported MySQL debugging flows built around:

- `FROM`
- `JOIN`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `SELECT`
- `DISTINCT`
- `ORDER BY`
- `LIMIT`
- non-recursive `WITH` CTEs
- simple `FROM (...) alias` subqueries
- supported `WHERE IN (...)` subqueries
- supported scalar subqueries in `WHERE`
- supported `CASE` expressions
- supported window functions in `SELECT`
- simple uncorrelated aggregate scalar subqueries in the `SELECT` list

## Not Supported Yet

V1 is intentionally narrow. It does not try to support all SQL.

Examples of unsupported or currently limited areas include:

- `UNION`
- recursive CTEs
- non-`SELECT` statements such as `INSERT`, `UPDATE`, and `DELETE`
- remote MySQL hosts
- non-equality join conditions
- many advanced subquery shapes
- correlated scalar subqueries in the `SELECT` list
- grouped scalar subqueries in the `SELECT` list
- some advanced window-function syntax

When a query is out of scope, the extension should stop with a clear message instead of pretending to debug it.

## Safety And Trust

SQL Visual Debugger is built for read-only debugging.

In v1 it:

- supports local MySQL connections only
- allows supported read-only `SELECT` and non-recursive `WITH` flows
- blocks non-read-only query shapes
- blocks unsupported query shapes instead of guessing
- keeps passwords only in session memory
- clears cached passwords after access-denied failures so the next attempt prompts again

This is not a general-purpose SQL runner. It is a focused debugger for supported MySQL query analysis.
