# SQL Visual Debugger

SQL Visual Debugger is a VS Code extension that lets you step through a SQL query clause by clause and see how each stage changes the data.

Instead of treating SQL as a black box, the extension shows the intermediate result after each step so you can understand why rows were added, removed, grouped, sorted, or transformed.

## What It Does

- Runs selected SQL queries against a live MySQL database
- Breaks query execution into visual steps
- Shows intermediate result tables for each clause
- Highlights the active SQL clause in the editor
- Explains what changed between steps
- Helps debug row-count surprises, JOIN behavior, filters, aggregation, and derived columns

## Current Product Features

### Query Execution Flow

- Debug the selected query or the query under the cursor
- Step through the query one stage at a time
- Navigate through the execution flow in the webview
- Re-run the same query after switching databases
- Show the SQL fragment responsible for the current step

### Editor Integration

- Right-click a `.sql` file and run `SQL Debugger: Debug Query`
- Use Command Palette commands to debug queries or manage connections
- Highlight the active clause in the source editor as you move through steps
- Detect the active database from:
  - `USE database_name;`
  - inline annotations such as `-- @db: my_database`
  - supported SQL extension APIs when available

### MySQL Connection Flow

- Connect to a MySQL server from inside VS Code
- Store server details and active database separately
- Keep passwords only in session memory
- Switch databases from the extension UI
- Reconfigure server details without editing files

### Visual Debugging Experience

- Show row counts before and after each step
- Show row delta for each transformation
- Render intermediate result tables for every supported stage
- Surface step-specific explanations and impact summaries
- Keep the debugger panel open beside the editor for side-by-side inspection

## Supported Query Features

The extension currently supports debugging these query patterns:

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
- `CASE` expressions in `SELECT`
- supported window functions in `SELECT`

## Step-Specific Visualizations

### JOIN

- Preview both sides of the join
- Click join-key rows to trace matching records
- Show relationship hints such as one-to-many or many-to-one
- Preserve duplicate column names using qualified labels when needed

### WHERE and HAVING

- Show which rows survived the filter
- Show pre-filter rows for comparison
- Highlight the columns involved in the condition
- Show extra context for supported subquery-based filters

### GROUP BY

- Show grouped output rows
- Let you inspect which source rows formed each group
- Highlight grouping columns
- Surface aggregate columns and summaries

### SELECT

- Show the projected output columns
- Explain `DISTINCT` behavior when used
- Surface CASE expression details
- Surface window-function details and previews

### ORDER BY

- Highlight the sort columns
- Show the reordered result while preserving row count context

## Current Limitations

This extension is already productized, but SQL support is intentionally bounded. Some queries are still outside the supported scope.

Current known limitations include:

- only `SELECT`-style debugging flows are supported
- recursive CTEs are not supported
- JOIN conditions must currently be simple equality comparisons
- not every subquery shape is supported
- some advanced window syntax is not yet supported
- support is currently focused on MySQL

When a query is outside the supported shape, the extension should fail with a clear message instead of silently giving misleading output.

## Commands

- `SQL Debugger: Debug Query`
- `SQL Debugger: Configure Connection`
- `SQL Debugger: Switch Database`

## Development

### Requirements

- Node.js
- npm
- VS Code
- access to a MySQL database

### Install dependencies

```bash
npm install
```

### Compile

```bash
npm run compile
```

### Run the extension

Launch the extension in VS Code using the provided Extension Host launch configuration.

## Project Status

SQL Visual Debugger is an active product, not an MVP placeholder. The current focus is on expanding SQL coverage, improving resilience, refining the teaching experience, and hardening the extension for broader real-world usage.
