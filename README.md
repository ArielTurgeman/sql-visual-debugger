# SQL Visual Debugger

SQL Visual Debugger is a VS Code extension for stepping through supported MySQL `SELECT` queries clause by clause and inspecting the intermediate result at each stage.

It is designed for read-only debugging and learning flows. Instead of treating SQL as a black box, the extension shows how rows change as the query moves through `FROM`, `JOIN`, `WHERE`, `GROUP BY`, `HAVING`, `SELECT`, `ORDER BY`, and `LIMIT`.

## Why Use It

- Understand why rows were added, removed, grouped, sorted, or transformed
- Debug row-count surprises and unexpected join results
- Inspect intermediate result tables instead of only the final output
- Learn SQL execution flow visually inside VS Code

## Core Features

- Debug the selected query or the query under the cursor
- Step through the query one stage at a time
- Show intermediate result tables for each supported clause
- Highlight the active SQL clause in the editor
- Explain what changed between one step and the next
- Re-run the same query after switching databases
- Keep the debugger panel open beside the editor for side-by-side inspection

## Supported SQL In V1

V1 is intentionally narrow. The goal is to support a clear, trustworthy subset of MySQL query shapes rather than claim broad coverage.

Supported query patterns currently include:

- `SELECT`
- `FROM`
- `JOIN`
  - `INNER JOIN`
  - `LEFT JOIN`
  - `RIGHT JOIN`
  - `CROSS JOIN`
  - simple equality `ON` conditions
- `WHERE`
- `GROUP BY`
- `HAVING`
- `ORDER BY`
- `LIMIT`
- `DISTINCT`
- non-recursive `WITH` CTEs
- simple subqueries in `FROM (...) alias`
- simple uncorrelated aggregate scalar subqueries in the `SELECT` list
- `CASE` expressions in `SELECT`
- supported window functions in `SELECT`

## Current Limitations

This extension does not attempt to support every SQL feature. It is better to reject an unsupported query than to show misleading debug output.

Current limitations include:

- only `SELECT`-style debugging flows are supported
- support is focused on MySQL in v1
- v1 supports local MySQL connections only
- remote hosts are blocked in v1
- recursive CTEs are not supported
- `JOIN` conditions must currently be simple equality comparisons
- not every subquery shape is supported
- projected scalar subqueries are currently limited to simple uncorrelated single-value aggregate forms such as `AVG`, `SUM`, `COUNT`, `MIN`, and `MAX`
- some advanced window syntax is not yet supported
- some query shapes may be rejected if they cannot be handled safely in the extension's read-only execution model

When a query is outside the supported shape, the extension should fail with a clear message instead of silently producing misleading output.

## Safety And Trust Model

The extension is intended for read-only debugging flows.

In v1, SQL Visual Debugger:

- allows supported read-only `SELECT` and non-recursive `WITH` flows
- blocks non-read-only query shapes
- blocks unsupported query shapes instead of guessing
- enforces local-only MySQL connections in v1
- keeps safety checks in the execution path, not only in surface-level query parsing

This is not a general-purpose SQL runner. It is a bounded debugger for supported, read-only query analysis.

## Connection Behavior

- the extension is currently focused on MySQL only
- v1 allows only local MySQL hosts such as `localhost`, `127.0.0.1`, and `::1`
- server details and active database are stored separately
- the extension can detect the active database from:
  - `USE database_name;`
  - inline annotations such as `-- @db: my_database`
  - supported SQL extension APIs when available
- the debugger can re-run the same query after you switch databases

## Password Handling

- passwords are kept only in session memory
- passwords are not stored as durable extension configuration
- after access-denied failures, cached passwords are cleared so the next attempt prompts again

## How To Use

1. Open a `.sql` file in VS Code.
2. Select a query, or place the cursor inside a query.
3. Run `SQL Debugger: Debug Query` from the editor context menu or Command Palette.
4. Enter local MySQL connection details when prompted.
5. Step through the query in the debugger panel.

## Commands

- `SQL Debugger: Debug Query`
- `SQL Debugger: Configure Connection`
- `SQL Debugger: Switch Database`

## What The Debugger Shows

### JOIN

- preview both sides of the join
- trace matching records through join keys
- show relationship hints such as one-to-many or many-to-one
- preserve duplicate column names using qualified labels when needed

### WHERE and HAVING

- show which rows survived the filter
- show pre-filter rows for comparison
- highlight the columns involved in the condition
- show extra context for supported subquery-based filters

### GROUP BY

- show grouped output rows
- inspect which source rows formed each group
- highlight grouping columns
- surface aggregate columns and summaries

### SELECT and ORDER BY

- show projected output columns
- explain `DISTINCT` behavior when used
- surface `CASE` expression details
- surface supported window-function details and previews
- highlight sort columns in `ORDER BY`

## Development

### Requirements

- Node.js
- npm
- VS Code
- access to a local MySQL database for manual testing

### Install dependencies

```bash
npm install
```

### Compile

```bash
npm run compile
```

### Run tests

```bash
npm run test:unit
```
