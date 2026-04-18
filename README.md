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
- Preserve VS Code's default `F5` behavior instead of overriding it inside SQL files
- Highlight the active clause in the source editor as you move through steps
- Detect the active database from:
  - `USE database_name;`
  - inline annotations such as `-- @db: my_database`
  - supported SQL extension APIs when available

### MySQL Connection Flow

- Connect to a MySQL server from inside VS Code
- Store server details and active database separately
- Keep passwords only in session memory
- Run the debugger using read-only `SELECT` / `WITH` execution only
- Enforce read-only execution in the MySQL runner itself, not only in the SQL extractor
- Start a read-only database transaction for debugger sessions and roll it back on disconnect
- Forget cached passwords after access-denied failures so the next retry prompts again
- Detect `USE database_name;` and `-- @db: ...` only from the exact SQL being debugged, not from unrelated text elsewhere in the file
- Allow only local MySQL connections in v1 (`localhost`, `127.0.0.1`, or `::1`)
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
- simple uncorrelated aggregate scalar subqueries in the `SELECT` list
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
- Allow supported simple scalar subqueries in projected columns even when they do not yet have dedicated explanation UI

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
- projected scalar subqueries are currently limited to simple uncorrelated single-value aggregate forms such as `AVG`, `SUM`, `COUNT`, `MIN`, and `MAX`
- some query shapes may be rejected if they cannot be inlined safely in read-only mode
- some advanced window syntax is not yet supported
- support is currently focused on MySQL
- v1 supports local MySQL connections only and blocks remote hosts

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

### Run tests

```bash
npm run test:unit
```

Current automated coverage includes:

- query extraction behavior from the editor selection or cursor position
- SQL sanitization and multi-statement rejection
- query block parsing for main queries, non-recursive CTEs, and simple `FROM` subqueries
- unsupported query-shape rejection such as recursive CTEs and non-equality joins
- read-only CTE and subquery inlining behavior for supported flows
- engine step execution for:
  - `FROM`
  - `JOIN`
  - `WHERE`
  - `GROUP BY`
  - `HAVING`
  - `SELECT`
  - `ORDER BY`
  - `LIMIT`
- `SELECT DISTINCT` explanation metadata
- `CASE` explanation metadata
- window-function metadata for ranking and aggregate window functions
- `WHERE IN` and scalar-subquery metadata
- extractor support for simple uncorrelated aggregate scalar subqueries in the `SELECT` list
- dependency flow from CTE blocks and read-only inlined `FROM` subquery blocks

Current test files:

- [test/queryExtractor.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/queryExtractor.test.js)
- [test/queryBlocks.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/queryBlocks.test.js)
- [test/fromStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/fromStep.test.js)
- [test/joinStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/joinStep.test.js)
- [test/whereStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/whereStep.test.js)
- [test/groupByStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/groupByStep.test.js)
- [test/havingStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/havingStep.test.js)
- [test/selectStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/selectStep.test.js)
- [test/windowStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/windowStep.test.js)
- [test/orderByStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/orderByStep.test.js)
- [test/limitStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/limitStep.test.js)
- [test/cteStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/cteStep.test.js)
- [test/subqueryStep.test.js](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/test/subqueryStep.test.js)

## Engine Architecture

The SQL debugging engine was refactored so `src/engine/stepEngine.ts` is now the orchestrator, and focused helpers live in separate modules.

### Module map

- [src/engine/stepEngine.ts](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/src/engine/stepEngine.ts)
  - top-level execution flow
  - per-block orchestration
  - row fetching helpers
  - read-only inlining for supported CTE/subquery blocks
- [src/engine/stepEngineTypes.ts](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/src/engine/stepEngineTypes.ts)
  - shared engine types
  - `DebugStep` and all step metadata types
  - parsed-query helper types
- [src/engine/stepEngineParsing.ts](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/src/engine/stepEngineParsing.ts)
  - SQL clause parsing
  - canonical query builders
  - select-item splitting
  - window and CASE parsing helpers
  - shared SQL token/identifier utilities
- [src/engine/stepEngineMetadata.ts](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/src/engine/stepEngineMetadata.ts)
  - metadata detection/building for `WHERE`, `HAVING`, `GROUP BY`, `ORDER BY`, `SELECT`
  - subquery metadata builders
  - aggregate, CASE, and window metadata
- [src/engine/stepEngineExplain.ts](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/src/engine/stepEngineExplain.ts)
  - join display row construction
  - join relationship inference
  - window explanation text and preview-row helpers
- [src/engine/stepEngineStepBuilders.ts](/C:/Users/׳׳©׳×׳׳©/Desktop/ג€ג€ג€ג€sqlclaudereservebackup/src/engine/stepEngineStepBuilders.ts)
  - construction of `DebugStep` objects for each clause
  - block-context attachment

### Where to add code

When changing the engine, use this routing guide:

- Add or change SQL parsing logic in `stepEngineParsing.ts`.
- Add or change derived metadata for UI panels in `stepEngineMetadata.ts`.
- Add or change human-readable explanations, preview ordering, or join display shaping in `stepEngineExplain.ts`.
- Add or change the shape/content of clause step objects in `stepEngineStepBuilders.ts`.
- Add or change orchestration order, query execution flow, or read-only block inlining in `stepEngine.ts`.
- Add or change shared step/meta types in `stepEngineTypes.ts`.

### Practical examples

- New supported clause parsing:
  start in `stepEngineParsing.ts`, then wire it into `stepEngine.ts`, then add tests.
- New panel metadata for an existing clause:
  start in `stepEngineMetadata.ts`, then expose it through the relevant step builder or `stepEngine.ts`, then add tests.
- Better explanation text without behavioral changes:
  start in `stepEngineExplain.ts` or `stepEngineStepBuilders.ts`, depending on whether the text is helper-derived or directly attached to a step.
- New `DebugStep` fields consumed by the webview:
  define the type in `stepEngineTypes.ts`, populate it in `stepEngine.ts` or `stepEngineStepBuilders.ts`, and cover it with tests.

### Workflow for future prompts

If a future task asks to extend SQL support or engine behavior, use this sequence:

1. Find the affected clause or helper in the engine module map above.
2. Update or add tests in the matching `test/*.test.js` file first when practical.
3. Make the engine change in the smallest correct module instead of growing `stepEngine.ts`.
4. Run `npm run compile` and `npm run test:unit`.
5. Update this README section if the architecture, coverage, or code-placement guidance changes.

### Run the extension

Launch the extension in VS Code using the provided Extension Host launch configuration.

## Project Status

SQL Visual Debugger is an active product, not an MVP placeholder. The current focus is on expanding SQL coverage, improving resilience, refining the teaching experience, and hardening the extension for broader real-world usage.

Recent hardening updates include:

- removal of the SQL-only `F5` keybinding so the extension no longer overrides VS Code's standard debug shortcut
- stronger Marketplace metadata in `package.json`, including clearer description, categories, and keywords
- a read-only debugger execution model for supported flows, removing reliance on temporary table creation for CTE and supported subquery handling
- runner-level read-only safety checks so non-read-only SQL is blocked before execution even if a future code path bypasses the extractor
- disabling the generic MySQL `execute()` path in the debugger runner so it cannot become an accidental write-capable escape hatch
- starting read-only MySQL sessions and rolling them back when the debugger disconnects
- clearing cached passwords after access-denied failures so users are re-prompted instead of silently retrying a bad password
- narrowing automatic database detection to the exact SQL being debugged so unrelated `USE ...;` statements elsewhere in the file do not silently switch the target database
- enforcing a local-only MySQL policy in v1 by blocking non-`localhost` hosts at configuration and execution time

## Remaining Pre-Launch Work

The core product is already useful. The current goal is not to keep adding features by default, but to finish the work around the product so the first public release is safe, clear, and stable.

Current remaining work:

- product boundaries
- safety and trust review
- release readiness
- first-run onboarding check
- error messages and empty states review
- VSIX packaging and install test
- publish metadata and assets
- marketplace page
- launch checklist
- post-launch feedback plan
