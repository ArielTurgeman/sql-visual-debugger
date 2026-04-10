import type { MysqlRunner } from '../mysql/mysqlRunner';
import { parseQueryBlocks, type QueryBlock, type QueryBlockType } from './queryBlocks';

export type DebugStep = {
  name: string;
  title: string;
  explanation: string;
  sqlFragment: string;
  rowsBefore: number;
  rowsAfter: number;
  data: Record<string, unknown>[];
  columns: string[];
  joinMeta?: JoinMeta;
  // Carries JOIN-resolved column indicators forward to non-JOIN steps
  schemaContext?: { joinIndicatorColumns: string[] };
  /** One-sentence "what happened" — specific to this step's clause and data. */
  impact?: string;
  /** All rows present *before* the WHERE filter (up to 200) — for the Filtered Rows View. */
  preFilterRows?: Record<string, unknown>[];
  /** Column names for preFilterRows (matches the post-JOIN canonical schema). */
  preFilterColumns?: string[];
  /** Columns referenced in the WHERE clause (used to highlight cells in Filtered Rows View). */
  whereColumns?: string[];
  whereInSubquery?: WhereInSubqueryMeta;
  whereScalarSubquery?: WhereScalarSubqueryMeta;
  /** Columns referenced in the ORDER BY clause (used to highlight sort columns in the result table). */
  sortColumns?: string[];
  /** Columns that define the GROUP BY keys (highlighted in the intermediate result table). */
  groupByColumns?: string[];
  /** Aggregated output columns with their function name and the raw source column inside the parens
   *  (e.g. { col: "total_players", fn: "COUNT", srcCol: "PlayerId" }). */
  aggColumns?: Array<{ col: string; fn: string; srcCol?: string }>;
  /** Compact summary of aggregate expressions for the "Aggregations:" hint line (e.g. "COUNT(PlayerId), AVG(score)"). */
  aggSummary?: string;
  /** All rows present *before* GROUP BY (up to 500) — used by the Group Breakdown panel. */
  preGroupRows?: Record<string, unknown>[];
  /** Column names for preGroupRows (the pre-aggregation schema). */
  preGroupColumns?: string[];
  blockType?: QueryBlockType;
  blockName?: string;
  blockIndex?: number;
  blockDependencies?: string[];
  blockSourceText?: string;
  blockSourceStart?: number;
  sourceRows?: number;
  sourceLabel?: string;
  windowColumns?: WindowColumnMeta[];
  caseColumns?: CaseColumnMeta[];
  preSelectRows?: Record<string, unknown>[];
  preSelectColumns?: string[];
  distinctMeta?: DistinctMeta;
};

export type WhereInSubqueryMeta = {
  explanation: string;
  rows: Record<string, unknown>[];
  columns: string[];
  totalRows: number;
};

export type WhereScalarSubqueryMeta = {
  explanation: string;
  value: unknown;
  columnLabel: string;
};

export type WindowColumnMeta = {
  outputColumn: string;
  expression: string;
  functionName: string;
  sourceColumn?: string;
  partitionBy: string[];
  orderBy: string[];
  orderByTerms: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
  explanation: string;
  howComputed: string[];
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
};

export type CaseColumnMeta = {
  outputColumn: string;
  expression: string;
  inputColumns: string[];
  rowExplanations: CaseRowExplanation[];
};

export type CaseRowExplanation = {
  matchedRule: string;
  returnedValue: unknown;
  inputValues: Array<{ column: string; value: unknown }>;
};

export type DistinctMeta = {
  columns: string[];
  rows: Record<string, unknown>[];
};

export type JoinMeta = {
  leftTable: string;
  rightTable: string;
  leftKey: string;      // fully-qualified, e.g. "playerinfo.TeamId" — for display only
  rightKey: string;     // fully-qualified, e.g. "teaminfo.TeamId"  — for display only
  leftKeyCol: string;   // plain column name, e.g. "TeamId" — for data access & matching
  rightKeyCol: string;  // plain column name, e.g. "TeamId" — for data access & matching
  joinType: string;
  leftRows: Record<string, unknown>[];    // capped at 200 — initial preview display
  rightRows: Record<string, unknown>[];   // capped at 200 — initial preview display
  allLeftRows: Record<string, unknown>[];  // full dataset — used for match computation
  allRightRows: Record<string, unknown>[]; // full dataset — used for match computation
  leftColumns: string[];
  rightColumns: string[];
  relationship: 'one-to-one' | 'one-to-many' | 'many-to-one' | 'many-to-many';
  rowDelta: number;
  joinedResultColumns: string[];
  joinIndicatorColumns: string[];  // plain column names exclusive to the right/joined table
};

/**
 * Describes one column in the canonical pipeline schema.
 * sqlExpr  — the table-qualified SQL expression (e.g. `playerinfo`.`PlayerId`)
 * sqlAlias — backtick alias emitted in the SELECT list when the display name
 *            cannot be inferred from the expression alone (e.g. `fdsro.PlayerId`)
 */
type ColumnDef = {
  displayName: string;   // JS row object key and display label
  sqlExpr: string;       // SQL expression used in SELECT list
  sqlAlias?: string;     // optional AS alias (backtick-quoted)
};

type ParsedQuery = {
  selectClause: string;
  fromClause: string;
  whereClause?: string;
  groupByClause?: string;
  havingClause?: string;
  orderByClause?: string;
  limitClause?: string;
  joins: ParsedJoin[];
};

type ParsedJoin = {
  joinType: string;
  tableName: string;
  tableAlias: string;
  rawClause: string;
  onClause: string;
  leftExpr: string;
  rightExpr: string;
};

/** Maximum number of rows shown in any intermediate-result table. */
const MAX_DISPLAY_ROWS = 200;

export async function executeDebugSteps(rawSql: string, runner: MysqlRunner): Promise<DebugStep[]> {
  const blocks = parseQueryBlocks(rawSql);
  const steps: DebugStep[] = [];

  for (const [blockIndex, block] of blocks.entries()) {
    const blockSteps = await executeSingleQueryBlockSteps(block, runner);
    steps.push(...blockSteps.map(step => attachBlockContext(step, block, blockIndex)));

    if (block.materializedName) {
      await materializeBlock(block, runner);
    }
  }

  return steps;
}

async function executeSingleQueryBlockSteps(block: QueryBlock, runner: MysqlRunner): Promise<DebugStep[]> {
  const parsed = parseSelectQuery(block.sql);
  const steps: DebugStep[] = [];

  const baseRows = await runAliasedSelect(runner, parsed.fromClause);
  let currentRows = baseRows;
  let currentFromSql = parsed.fromClause;

  const fromTableName = extractBaseTableName(parsed.fromClause);
  const fromDependency = block.dependencies.find(dep => dep.tableName.toLowerCase() === fromTableName.toLowerCase());
  const fromDependencyLabel = fromDependency
    ? fromDependency.blockType === 'cte'
      ? `CTE \`${fromDependency.name}\``
      : `subquery \`${fromDependency.name}\``
    : null;
  steps.push({
    name: 'FROM',
    title: 'FROM',
    explanation: fromDependencyLabel
      ? `Loaded ${currentRows.length.toLocaleString()} rows from ${fromDependencyLabel}. This block starts from rows produced earlier in the query.`
      : `Loaded ${currentRows.length.toLocaleString()} rows from \`${fromTableName}\`. This is the starting dataset before any joins or filters.`,
    impact: currentRows.length === 0
      ? 'The base table is empty — subsequent steps will have no rows to work with.'
      : `${currentRows.length.toLocaleString()} rows are available for subsequent steps.`,
    sqlFragment: block.fromSource?.originalClause ?? parsed.fromClause,
    rowsBefore: 0,
    rowsAfter: currentRows.length,
    data: currentRows.slice(0, MAX_DISPLAY_ROWS),
    columns: getColumns(currentRows),
  });
  if (fromDependency) {
    steps[steps.length - 1].impact =
      `${currentRows.length.toLocaleString()} rows were loaded from ${fromDependency.blockType === 'cte' ? 'CTE' : 'subquery'} \`${fromDependency.name}\` and are now available for subsequent steps in this block.`;
    steps[steps.length - 1].sourceRows = currentRows.length;
    steps[steps.length - 1].sourceLabel = `Loaded from ${fromDependency.blockType === 'cte' ? 'CTE' : 'subquery'} ${fromDependency.name}`;
  }

  // ── Canonical schema ────────────────────────────────────────────────────────
  // Tracks the authoritative column list through the pipeline.  After JOINs
  // introduce qualified duplicate names (e.g. "fdsro.PlayerId"), every subsequent
  // step builds its SQL SELECT from this schema so MySQL returns the right values
  // and the display never collapses duplicate columns back to one.
  const baseAlias = extractBaseAlias(parsed.fromClause);
  let canonicalSchema: ColumnDef[] = getColumns(baseRows).map(col => ({
    displayName: col,
    sqlExpr: `\`${baseAlias}\`.\`${col}\``,
  }));
  // Accumulated indicator columns (for visual badges in post-JOIN steps)
  let canonicalJoinIndicators: string[] = [];
  // Determined once; drives which path all tail steps take
  const isSelectStar = /^select\s+\*$/i.test(parsed.selectClause.trim());
  // ────────────────────────────────────────────────────────────────────────────

  for (const join of parsed.joins) {
    const leftBefore = currentRows.length;
    currentFromSql = `${currentFromSql} ${join.rawClause}`;

    // Fetch the right-side table rows independently (no column collision possible here)
    const rightRows = await runAliasedSelect(runner, `FROM ${join.tableName} ${join.tableAlias}`.trim());

    // Detect which ON expression belongs to the joined (right) table so we can assign
    // leftKeyCol / rightKeyCol correctly regardless of the condition's written order.
    const leftExprTableRaw = join.leftExpr.includes('.') ? join.leftExpr.split('.')[0] : '';
    const leftExprCol  = join.leftExpr.includes('.')  ? join.leftExpr.split('.').pop()!  : join.leftExpr;
    const rightExprCol = join.rightExpr.includes('.') ? join.rightExpr.split('.').pop()! : join.rightExpr;
    const leftExprBelongsToRight =
      leftExprTableRaw.toLowerCase() === join.tableAlias.toLowerCase() ||
      leftExprTableRaw.toLowerCase() === join.tableName.toLowerCase();
    const leftKeyCol  = leftExprBelongsToRight ? rightExprCol : leftExprCol;
    const rightKeyCol = leftExprBelongsToRight ? leftExprCol  : rightExprCol;

    // Build the intermediate result in JS so duplicate column names across both tables
    // are preserved as separate qualified keys (e.g. "PlayerId" vs "prfos.PlayerId")
    // instead of being silently overwritten by MySQL2's object merge.
    const { rows: joinedRows, columns: joinedColumns } = buildJoinDisplay(
      currentRows, rightRows, leftKeyCol, rightKeyCol, join.tableAlias, join.joinType,
    );

    // Update canonical schema with right-side columns for this JOIN.
    // Columns whose name already exists on the left get a qualified display name
    // (e.g. "fdsro.PlayerId") and an explicit SQL alias so MySQL returns them
    // correctly in all subsequent steps.
    const leftColDisplayNames = new Set(canonicalSchema.map(c => c.displayName));
    const rightCols  = getColumns(rightRows);
    const rightCanonical: ColumnDef[] = rightCols.map(col => {
      if (leftColDisplayNames.has(col)) {
        const displayName = `${join.tableAlias}.${col}`;
        return {
          displayName,
          sqlExpr: `\`${join.tableAlias}\`.\`${col}\``,
          sqlAlias: `\`${displayName}\``,
        };
      }
      return { displayName: col, sqlExpr: `\`${join.tableAlias}\`.\`${col}\`` };
    });
    canonicalSchema = [...canonicalSchema, ...rightCanonical];

    // joinIndicatorColumns: all right-side display keys (qualified when name collides)
    const joinIndicatorColumns = rightCols.map((c) =>
      leftColDisplayNames.has(c) ? `${join.tableAlias}.${c}` : c
    );
    canonicalJoinIndicators = [...canonicalJoinIndicators, ...joinIndicatorColumns];

    const relationship = inferRelationship(currentRows, rightRows, leftKeyCol, rightKeyCol);

    const joinDelta = joinedRows.length - leftBefore;
    const rightDisplay = join.tableAlias !== join.tableName
      ? `\`${join.tableName}\` (as \`${join.tableAlias}\`)`
      : `\`${join.tableName}\``;
    const joinImpactBase = joinDelta === 0
      ? `Row count stayed at ${joinedRows.length.toLocaleString()}.`
      : joinDelta > 0
        ? `Row count grew from ${leftBefore.toLocaleString()} to ${joinedRows.length.toLocaleString()} (+${joinDelta.toLocaleString()}).`
        : `Row count fell from ${leftBefore.toLocaleString()} to ${joinedRows.length.toLocaleString()} (${joinDelta.toLocaleString()}).`;
    const joinImpactReason =
      relationship === 'one-to-one'   ? 'Each row on both sides matched at most one row.' :
      relationship === 'one-to-many'  ? 'Some left-side keys matched multiple right-side rows.' :
      relationship === 'many-to-one'  ? 'Multiple left-side rows shared the same right-side key.' :
                                        'Multiple rows matched on both sides (many-to-many).';
    steps.push({
      name: 'JOIN',
      title: join.joinType,
      explanation: `Joined ${rightDisplay} to the current result on ${join.leftExpr} = ${join.rightExpr}.`,
      impact: `${joinImpactBase} Relationship: ${relationship} — ${joinImpactReason}`,
      sqlFragment: join.rawClause,
      rowsBefore: leftBefore,
      rowsAfter: joinedRows.length,
      data: joinedRows.slice(0, MAX_DISPLAY_ROWS),
      columns: joinedColumns,
      joinMeta: {
        leftTable: extractBaseAlias(parsed.fromClause),
        rightTable: join.tableAlias,
        leftKey: join.leftExpr,
        rightKey: join.rightExpr,
        leftKeyCol,
        rightKeyCol,
        joinType: join.joinType,
        leftRows: currentRows.slice(0, 200),
        rightRows: rightRows.slice(0, 200),
        allLeftRows: currentRows,
        allRightRows: rightRows,
        leftColumns: getColumns(currentRows),
        rightColumns: getColumns(rightRows),
        relationship,
        rowDelta: joinedRows.length - leftBefore,
        joinedResultColumns: joinedColumns,
        joinIndicatorColumns,
      },
    });

    currentRows = joinedRows;
  }

  // schemaContext is passed to every post-JOIN step so renderIntermediate can
  // display the same "joined" / "duplicate" column badges as the JOIN step itself.
  const schemaContext = canonicalJoinIndicators.length > 0
    ? { joinIndicatorColumns: canonicalJoinIndicators }
    : undefined;

  if (parsed.whereClause) {
    const before = currentRows.length;
    // Snapshot rows before filtering (capped at 200 for display purposes)
    const preFilterRows = currentRows.slice(0, 200);
    const preFilterColumns = getColumns(currentRows);
    const whereColumns = detectWhereColumns(parsed.whereClause, preFilterColumns);
    const whereInSubquery = await buildWhereInSubqueryMeta(parsed.whereClause, currentFromSql, runner);
    const whereScalarSubquery = await buildWhereScalarSubqueryMeta(parsed.whereClause, currentFromSql, runner);
    // Use canonical SELECT so qualified duplicate columns survive the WHERE step.
    const sql = buildCanonicalQuery(canonicalSchema, currentFromSql, parsed.whereClause);
    const rows = await runCustomSelect(runner, sql);
    const whereRemoved = before - rows.length;
    steps.push({
      name: 'WHERE',
      title: 'WHERE',
      explanation: whereRemoved === 0
        ? `Applied the WHERE condition. Every one of the ${before.toLocaleString()} rows satisfied it — none were removed.`
        : `Applied the WHERE condition. ${whereRemoved.toLocaleString()} row${whereRemoved === 1 ? '' : 's'} failed the check and ${whereRemoved === 1 ? 'was' : 'were'} discarded.`,
      impact: whereRemoved === 0
        ? `No rows removed. All ${rows.length.toLocaleString()} rows passed the filter.`
        : `${rows.length.toLocaleString()} of ${before.toLocaleString()} rows passed the filter (${whereRemoved.toLocaleString()} removed).`,
      sqlFragment: parsed.whereClause,
      rowsBefore: before,
      rowsAfter: rows.length,
      data: rows.slice(0, MAX_DISPLAY_ROWS),
      columns: getColumns(rows),
      schemaContext,
      preFilterRows,
      preFilterColumns,
      whereColumns,
      whereInSubquery,
      whereScalarSubquery,
    });
    currentRows = rows;
  }

  if (parsed.groupByClause) {
    const before = currentRows.length;
    const rows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'GROUP BY' }));
    // GROUP BY aggregates rows — the schema changes fundamentally.
    // Reset canonical schema to whatever columns the aggregation produced.
    const groupedCols = getColumns(rows);
    canonicalSchema = groupedCols.map(col => ({ displayName: col, sqlExpr: `\`${col}\`` }));
    canonicalJoinIndicators = [];
    // Snapshot rows before aggregation so the Group Breakdown panel can show
    // exactly which source rows collapsed into each group.  Cap at 500 rows.
    const preGroupRows    = currentRows.slice(0, 500);
    const preGroupColumns = getColumns(currentRows);
    const groupKeys = (parsed.groupByClause ?? '').replace(/^GROUP\s+BY\s+/i, '').trim();
    const groupByColumns = detectGroupByColumns(parsed.groupByClause ?? '', groupedCols);
    const aggColumns     = detectAggColumns(parsed.selectClause, groupedCols);
    const aggSummaryStr  = buildAggSummary(parsed.selectClause);
    steps.push({
      name: 'GROUP BY',
      title: 'GROUP BY',
      explanation: `Collapsed ${before.toLocaleString()} rows into ${rows.length.toLocaleString()} groups by ${groupKeys || 'the specified keys'}. Aggregate functions were applied within each group.`,
      impact: `Row count changed from ${before.toLocaleString()} to ${rows.length.toLocaleString()}. Each unique combination of group keys produced one output row.`,
      sqlFragment: parsed.groupByClause,
      rowsBefore: before,
      rowsAfter: rows.length,
      data: rows.slice(0, MAX_DISPLAY_ROWS),
      columns: groupedCols,
      groupByColumns,
      aggColumns,
      aggSummary: aggSummaryStr || undefined,
      preGroupRows,
      preGroupColumns,
    });
    currentRows = rows;
  }

  if (parsed.havingClause) {
    const before = currentRows.length;
    // Snapshot the post-GROUP BY rows before HAVING filters them out.
    // Same pattern as the WHERE step: capped at 200 rows for display.
    const preFilterRows = currentRows.slice(0, 200);
    const preFilterColumns = getColumns(currentRows);
    // detectHavingColumns first tries direct word-boundary matching (works when
    // HAVING references a column alias directly, e.g. "HAVING total_players > 5"),
    // then falls back to aggregate alias resolution so that a clause like
    // "HAVING COUNT(playerinfo.PlayerId) > 5" correctly highlights "total_players".
    const whereColumns = detectHavingColumns(
      parsed.havingClause,
      parsed.selectClause,
      preFilterColumns,
    );
    const rows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'HAVING' }));
    const havingCols = getColumns(rows);
    canonicalSchema = havingCols.map(col => ({ displayName: col, sqlExpr: `\`${col}\`` }));
    const havingRemoved = before - rows.length;
    steps.push({
      name: 'HAVING',
      title: 'HAVING',
      explanation: `Filtered aggregated groups using the HAVING condition. Unlike WHERE (which filters individual rows before grouping), HAVING filters after aggregation.`,
      impact: havingRemoved === 0
        ? `No groups removed. All ${rows.length.toLocaleString()} groups satisfied the condition.`
        : `${havingRemoved.toLocaleString()} group${havingRemoved === 1 ? '' : 's'} removed. ${rows.length.toLocaleString()} of ${before.toLocaleString()} groups passed the filter.`,
      sqlFragment: parsed.havingClause,
      rowsBefore: before,
      rowsAfter: rows.length,
      data: rows.slice(0, MAX_DISPLAY_ROWS),
      columns: havingCols,
      // Filtered-rows view — identical fields to the WHERE step so renderFilteredView
      // in panel.ts renders without any modification.
      preFilterRows,
      preFilterColumns,
      whereColumns,
    });
    currentRows = rows;
  }

  const finalBaseBefore = currentRows.length;
  const usesDistinct = isSelectDistinct(parsed.selectClause);
  // For SELECT *, use canonical schema so qualified duplicate columns are preserved.
  // For specific column projections or queries with aggregation, use the real SQL.
  const useCanonicalTail = isSelectStar && !parsed.groupByClause && !parsed.havingClause;
  let selectedRows: Record<string, unknown>[];
  let distinctMeta: DistinctMeta | undefined;
  if (useCanonicalTail) {
    const tail = parsed.whereClause ?? '';
    selectedRows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, tail, usesDistinct));
    if (usesDistinct) {
      const preDistinctRows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, tail));
      distinctMeta = {
        columns: getColumns(preDistinctRows),
        rows: preDistinctRows.slice(0, MAX_DISPLAY_ROWS),
      };
    }
  } else {
    selectedRows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'SELECT' }));
    if (usesDistinct) {
      const preDistinctRows = await runCustomSelect(
        runner,
        buildFinalQuery({ ...parsed, selectClause: removeDistinctFromSelectClause(parsed.selectClause) }, { upTo: 'SELECT' }),
      );
      distinctMeta = {
        columns: getColumns(preDistinctRows),
        rows: preDistinctRows.slice(0, MAX_DISPLAY_ROWS),
      };
    }
  }
  const preSelectRows = currentRows.slice(0, MAX_DISPLAY_ROWS);
  const preSelectColumns = getColumns(currentRows);
  const windowColumns = detectWindowColumns(parsed.selectClause, getColumns(selectedRows), currentRows, selectedRows);
  const caseColumns = await detectCaseColumns(
    parsed.selectClause,
    getColumns(selectedRows),
    currentRows,
    runner,
    parsed,
    useCanonicalTail,
    canonicalSchema,
    currentFromSql,
    usesDistinct,
  );
  steps.push({
    name: 'SELECT',
    title: 'SELECT',
    explanation: isSelectStar
      ? 'SELECT * was used — all available columns were kept as-is. No column projection occurred.'
      : 'Projected only the columns named in the SELECT clause. All other columns were excluded from the output.',
    impact: `Row count did not change (${selectedRows.length.toLocaleString()}). SELECT determines which columns appear in the output, not which rows are returned.`,
    sqlFragment: parsed.selectClause,
    rowsBefore: finalBaseBefore,
    rowsAfter: selectedRows.length,
    data: selectedRows.slice(0, MAX_DISPLAY_ROWS),
    columns: getColumns(selectedRows),
    schemaContext,
    windowColumns,
    caseColumns,
    preSelectRows,
    preSelectColumns,
    distinctMeta,
  });
  currentRows = selectedRows;

  if (parsed.orderByClause) {
    const before = currentRows.length;
    let orderRows: Record<string, unknown>[];
    if (useCanonicalTail) {
      const tail = [parsed.whereClause, parsed.orderByClause].filter(Boolean).join(' ');
      orderRows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, tail));
    } else {
      orderRows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'ORDER BY' }));
    }
    const orderKeys = (parsed.orderByClause ?? '').replace(/^ORDER\s+BY\s+/i, '').trim();
    const orderByCols = getColumns(orderRows);
    steps.push({
      name: 'ORDER BY',
      title: 'ORDER BY',
      explanation: `Reordered the result set by ${orderKeys || 'the specified expression'}.`,
      impact: `Row count is unchanged at ${orderRows.length.toLocaleString()}. ORDER BY only reorders rows — nothing is added or removed.`,
      sqlFragment: parsed.orderByClause,
      rowsBefore: before,
      rowsAfter: orderRows.length,
      data: orderRows.slice(0, MAX_DISPLAY_ROWS),
      columns: orderByCols,
      schemaContext,
      sortColumns: detectOrderByColumns(parsed.orderByClause ?? '', orderByCols),
    });
    currentRows = orderRows;
  }

  if (parsed.limitClause) {
    const before = currentRows.length;
    let limitRows: Record<string, unknown>[];
    if (useCanonicalTail) {
      const tail = [parsed.whereClause, parsed.orderByClause, parsed.limitClause].filter(Boolean).join(' ');
      limitRows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, tail));
    } else {
      limitRows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'LIMIT' }));
    }
    const limitN = (parsed.limitClause ?? '').replace(/^LIMIT\s+/i, '').trim() || 'N';
    const limitExcluded = before - limitRows.length;
    steps.push({
      name: 'LIMIT',
      title: 'LIMIT',
      explanation: `Kept only the first ${limitN} row${limitN === '1' ? '' : 's'} from the result. All rows beyond the limit were excluded.`,
      impact: limitExcluded > 0
        ? `${limitExcluded.toLocaleString()} row${limitExcluded === 1 ? '' : 's'} beyond the limit ${limitExcluded === 1 ? 'was' : 'were'} cut. Final output: ${limitRows.length.toLocaleString()} row${limitRows.length === 1 ? '' : 's'}.`
        : `All ${limitRows.length.toLocaleString()} rows fit within the limit — nothing was excluded.`,
      sqlFragment: parsed.limitClause,
      rowsBefore: before,
      rowsAfter: limitRows.length,
      data: limitRows.slice(0, MAX_DISPLAY_ROWS),
      columns: getColumns(limitRows),
      schemaContext,
    });
  }

  return steps;
}

async function runAliasedSelect(runner: MysqlRunner, fromAndJoinsSql: string): Promise<Record<string, unknown>[]> {
  const aliases = extractTableAliases(fromAndJoinsSql);
  const selectList = aliases.length > 0
    ? aliases.map((alias) => `\`${alias}\`.*`).join(', ')
    : '*';

  const sql = `SELECT ${selectList} ${fromAndJoinsSql}`;
  const rows = await runner.query(sql);
  return normalizeRows(rows);
}

async function runCustomSelect(runner: MysqlRunner, sql: string): Promise<Record<string, unknown>[]> {
  const rows = await runner.query(sql);
  return normalizeRows(rows);
}

function normalizeRows(rows: unknown): Record<string, unknown>[] {
  if (!Array.isArray(rows)) return [];
  return rows as Record<string, unknown>[];
}

function getColumns(rows: Record<string, unknown>[]): string[] {
  return rows.length > 0 ? Object.keys(rows[0]) : [];
}

/**
 * Returns the subset of `columns` that appear to be referenced in `whereClause`.
 * For qualified names like "teaminfo.TeamId", the bare column name is matched too
 * so that a WHERE clause writing "TeamId = 5" still highlights the qualified column.
 */
function detectWhereColumns(whereClause: string, columns: string[]): string[] {
  const clause = whereClause.replace(/^WHERE\s+/i, '');
  return columns.filter(col => {
    // For qualified names like "teaminfo.TeamId" check the plain part ("TeamId")
    const bare = col.includes('.') ? col.split('.').pop()! : col;
    const escaped = bare.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`\\b${escaped}\\b`, 'i').test(clause);
  });
}

async function buildWhereInSubqueryMeta(
  whereClause: string,
  currentFromSql: string,
  runner: MysqlRunner,
): Promise<WhereInSubqueryMeta | undefined> {
  const parsed = parseWhereInSubquery(whereClause);
  if (!parsed) {
    return undefined;
  }

  const outerAliases = extractTableAliases(currentFromSql);
  if (isCorrelatedSubquery(parsed.subquerySql, outerAliases)) {
    return undefined;
  }

  const rows = await runCustomSelect(runner, parsed.subquerySql);
  const columns = getColumns(rows);
  const displayColumn = parsed.outerColumn ? bareIdentifier(parsed.outerColumn) : 'value';

  return {
    explanation: `Filters rows by checking whether ${displayColumn} exists in the values returned by the subquery.`,
    rows: rows.slice(0, MAX_DISPLAY_ROWS),
    columns,
    totalRows: rows.length,
  };
}

async function buildWhereScalarSubqueryMeta(
  whereClause: string,
  currentFromSql: string,
  runner: MysqlRunner,
): Promise<WhereScalarSubqueryMeta | undefined> {
  const parsed = parseWhereScalarSubquery(whereClause);
  if (!parsed) {
    return undefined;
  }

  const outerAliases = extractTableAliases(currentFromSql);
  if (isCorrelatedSubquery(parsed.subquerySql, outerAliases)) {
    return undefined;
  }

  const rows = await runCustomSelect(runner, parsed.subquerySql);
  if (rows.length !== 1) {
    return undefined;
  }

  const columns = getColumns(rows);
  if (columns.length !== 1) {
    return undefined;
  }

  const columnLabel = columns[0];
  const value = rows[0][columnLabel];
  const displayColumn = parsed.outerColumn ? bareIdentifier(parsed.outerColumn) : 'value';

  return {
    explanation: `Checks whether ${displayColumn} is ${describeComparisonOperator(parsed.operator)} the value returned by the subquery.`,
    value,
    columnLabel,
  };
}

function parseWhereInSubquery(whereClause: string): { outerColumn?: string; subquerySql: string } | null {
  const clause = whereClause.replace(/^WHERE\s+/i, '').trim();
  let depth = 0;
  let i = 0;

  while (i < clause.length) {
    const ch = clause[i];
    if (ch === '\'' || ch === '"' || ch === '`') {
      i = skipQuotedSql(clause, i, ch);
      continue;
    }
    if (ch === '(') {
      depth += 1;
      i += 1;
      continue;
    }
    if (ch === ')') {
      depth = Math.max(0, depth - 1);
      i += 1;
      continue;
    }
    if (depth === 0 && matchesWordAt(clause, i, 'IN')) {
      const afterIn = skipSqlWhitespace(clause, i + 2);
      if (clause[afterIn] !== '(') {
        i += 1;
        continue;
      }
      const closeParen = findMatchingParenSql(clause, afterIn);
      const innerSql = clause.slice(afterIn + 1, closeParen).trim();
      if (!/^SELECT\b/i.test(innerSql)) {
        i += 1;
        continue;
      }
      const outerExpr = clause.slice(0, i).trim();
      const outerColumn = extractTrailingIdentifier(outerExpr) ?? undefined;
      return { outerColumn, subquerySql: innerSql };
    }
    i += 1;
  }

  return null;
}

function parseWhereScalarSubquery(whereClause: string): { outerColumn?: string; operator: string; subquerySql: string } | null {
  const clause = whereClause.replace(/^WHERE\s+/i, '').trim();
  let depth = 0;
  let i = 0;

  while (i < clause.length) {
    const ch = clause[i];
    if (ch === '\'' || ch === '"' || ch === '`') {
      i = skipQuotedSql(clause, i, ch);
      continue;
    }
    if (ch === '(') {
      depth += 1;
      i += 1;
      continue;
    }
    if (ch === ')') {
      depth = Math.max(0, depth - 1);
      i += 1;
      continue;
    }
    if (depth === 0) {
      const operator = readComparisonOperator(clause, i);
      if (!operator) {
        i += 1;
        continue;
      }
      const afterOperator = skipSqlWhitespace(clause, i + operator.length);
      if (clause[afterOperator] !== '(') {
        i += 1;
        continue;
      }
      const closeParen = findMatchingParenSql(clause, afterOperator);
      const innerSql = clause.slice(afterOperator + 1, closeParen).trim();
      if (!/^SELECT\b/i.test(innerSql)) {
        i += 1;
        continue;
      }
      const outerExpr = clause.slice(0, i).trim();
      if (!outerExpr || /\bIN\s*$/i.test(outerExpr)) {
        return null;
      }
      const outerColumn = extractTrailingIdentifier(outerExpr) ?? undefined;
      return { outerColumn, operator, subquerySql: innerSql };
    }
    i += 1;
  }

  return null;
}

function isCorrelatedSubquery(subquerySql: string, outerAliases: string[]): boolean {
  return outerAliases.some(alias => {
    const escaped = alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`(?:\\b|\\\`)${escaped}(?:\\\`)?\\.`, 'i').test(subquerySql);
  });
}

function describeComparisonOperator(operator: string): string {
  switch (operator) {
    case '>':
      return 'greater than';
    case '>=':
      return 'greater than or equal to';
    case '<':
      return 'less than';
    case '<=':
      return 'less than or equal to';
    case '=':
      return 'equal to';
    case '!=':
    case '<>':
      return 'not equal to';
    default:
      return 'compared with';
  }
}

function extractTrailingIdentifier(value: string): string | null {
  const match = /(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*(?:\.(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))?)\s*$/.exec(value);
  return match ? match[1] : null;
}

function readComparisonOperator(text: string, index: number): string | null {
  const operators = ['>=', '<=', '<>', '!=', '=', '>', '<'];
  for (const operator of operators) {
    if (text.slice(index, index + operator.length) === operator) {
      return operator;
    }
  }
  return null;
}

/**
 * Column detection for HAVING clauses. Two-pass strategy:
 *
 * Pass 1 — direct word-boundary match (covers `HAVING total_players > 5`).
 * Pass 2 — aggregate alias resolution.  HAVING commonly uses aggregate expressions
 *   like `COUNT(playerinfo.PlayerId) > 5` whose result is exposed to the user under
 *   an alias declared in the SELECT clause (`COUNT(...) AS total_players`).  This pass
 *   normalises both sides to `FN(BARE_COLUMN)` form and, when they match, returns the
 *   alias as the column to highlight — so `total_players` lights up in the header even
 *   though that exact string never appears in the HAVING clause.
 *
 * @param havingClause  full HAVING clause string (including the keyword)
 * @param selectClause  full SELECT clause string (used only in the aggregate pass)
 * @param columns       column names present in the post-GROUP BY result set
 */
function detectHavingColumns(
  havingClause: string,
  selectClause: string,
  columns: string[],
): string[] {
  const stripped = havingClause.replace(/^HAVING\s+/i, '');

  // --- Pass 1: direct word-boundary match ---
  const direct = columns.filter(col => {
    const bare = col.includes('.') ? col.split('.').pop()! : col;
    const escaped = bare.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`\\b${escaped}\\b`, 'i').test(stripped);
  });
  if (direct.length > 0) return direct;

  // --- Pass 2: aggregate alias resolution ---
  // Helper: given "COUNT(table.col)" produce the canonical key "COUNT(COL)".
  // Stripping the table qualifier lets "COUNT(playerinfo.PlayerId)" match
  // "COUNT(p.PlayerId)" or even "COUNT(PlayerId)" written in the SELECT.
  const aggKey = (fn: string, inner: string): string =>
    `${fn.toUpperCase()}(${inner.trim().split('.').pop()!.toUpperCase()})`;

  // Collect all aggregate keys mentioned in the HAVING condition.
  const aggRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)/gi;
  const havingAggKeys = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = aggRx.exec(stripped)) !== null) {
    havingAggKeys.add(aggKey(m[1], m[2]));
  }
  if (havingAggKeys.size === 0) return [];

  // Scan the SELECT clause for "AGG(...) AS alias" pairs and collect aliases
  // whose aggregate key matches one of the HAVING aggregate keys.
  const selectBody = selectClause.replace(/^SELECT\s+/i, '');
  const aliasRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?/gi;
  const matchedAliases = new Set<string>();
  while ((m = aliasRx.exec(selectBody)) !== null) {
    if (havingAggKeys.has(aggKey(m[1], m[2]))) {
      matchedAliases.add(m[3].toUpperCase());
    }
  }
  if (matchedAliases.size === 0) return [];

  return columns.filter(col => {
    const bare = (col.includes('.') ? col.split('.').pop()! : col).toUpperCase();
    return matchedAliases.has(bare);
  });
}

/**
 * Returns the subset of `columns` that appear in the ORDER BY clause as sort keys.
 *
 * Strategy: strip the `ORDER BY` keyword, split on commas, then for each term:
 *   1. Remove trailing ASC / DESC direction modifier
 *   2. Remove backtick quoting
 *   3. Extract the bare identifier (strip table qualifier if present)
 * A column is considered a sort key if its bare name matches any extracted term
 * (case-insensitive).  This correctly handles all common forms:
 *   "ORDER BY total_players DESC"          → ["total_players"]
 *   "ORDER BY teaminfo.Team, total_players" → ["Team", "total_players"]
 *   "ORDER BY `total_players` ASC"          → ["total_players"]
 */
function detectOrderByColumns(orderByClause: string, columns: string[]): string[] {
  const body = orderByClause.replace(/^ORDER\s+BY\s+/i, '');
  // Parse each comma-separated sort term down to a bare, unquoted identifier.
  const terms: string[] = body.split(',').map(t => {
    return t
      .trim()
      .replace(/\s+(ASC|DESC)\s*$/i, '') // drop direction
      .replace(/`/g, '')                  // drop backtick quoting
      .trim()
      .split('.')                         // drop table qualifier
      .pop()!
      .toLowerCase();
  });
  const termSet = new Set(terms);
  return columns.filter(col => {
    const bare = (col.includes('.') ? col.split('.').pop()! : col).toLowerCase();
    return termSet.has(bare);
  });
}

/**
 * Returns the subset of `columns` that are GROUP BY key columns.
 * Uses the same term-extraction logic as detectOrderByColumns: strip the keyword,
 * split on commas, remove backtick quoting and table qualifiers, compare lowercase.
 */
function detectGroupByColumns(groupByClause: string, columns: string[]): string[] {
  const body = groupByClause.replace(/^GROUP\s+BY\s+/i, '');
  const terms = body.split(',').map(t =>
    t.trim().replace(/`/g, '').split('.').pop()!.toLowerCase(),
  );
  const termSet = new Set(terms);
  return columns.filter(col => {
    const bare = (col.includes('.') ? col.split('.').pop()! : col).toLowerCase();
    return termSet.has(bare);
  });
}

/**
 * Returns one entry per aggregate output column, pairing the result column name
 * with the aggregate function that produced it (e.g. { col: "total_players", fn: "COUNT" }).
 *
 * Strategy: scan the SELECT clause for `AGG(...) AS alias` patterns.  If an alias is
 * found, match it against the result column list.  If no alias is present, try an exact
 * match of the raw expression against the column names (MySQL returns the expression
 * text verbatim as the column name when there is no alias).
 */
function detectAggColumns(
  selectClause: string,
  columns: string[],
): Array<{ col: string; fn: string; srcCol?: string }> {
  const body = selectClause.replace(/^SELECT\s+/i, '');
  const result: Array<{ col: string; fn: string; srcCol?: string }> = [];
  // Capture group 2 = argument inside parens; group 3 = optional AS alias.
  const aggRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)(?:\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?)?/gi;
  let m: RegExpExecArray | null;
  while ((m = aggRx.exec(body)) !== null) {
    const fn    = m[1].toUpperCase();
    const arg   = m[2].trim();
    const alias = m[3];
    // Strip table qualifier + backticks to get the bare source column name.
    // COUNT(*) and empty args produce no srcCol.
    const srcCol = (arg === '*' || arg === '') ? undefined
      : arg.replace(/`/g, '').split('.').pop()!;
    if (alias) {
      const col = columns.find(c => {
        const bare = (c.includes('.') ? c.split('.').pop()! : c).toLowerCase();
        return bare === alias.toLowerCase();
      });
      if (col) result.push({ col, fn, srcCol });
    } else {
      // No alias — MySQL exposes the raw expression as the column name.
      const rawExpr = m[0].trim();
      const col = columns.find(c => c.toLowerCase() === rawExpr.toLowerCase());
      if (col) result.push({ col, fn, srcCol });
    }
  }
  return result;
}

/**
 * Builds a compact human-readable summary of all aggregate expressions in the SELECT
 * clause, e.g. "COUNT(PlayerId), AVG(score)".  Used for the "Aggregations:" hint line.
 */
function buildAggSummary(selectClause: string): string {
  const body = selectClause.replace(/^SELECT\s+/i, '');
  const aggRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\([^)]*\)/gi;
  const found: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = aggRx.exec(body)) !== null) {
    found.push(m[0].replace(/\s+/g, ' ').trim());
  }
  return found.join(', ');
}

function detectWindowColumns(
  selectClause: string,
  resultColumns: string[],
  inputRows: Record<string, unknown>[],
  selectedRows: Record<string, unknown>[],
): WindowColumnMeta[] {
  const items = splitTopLevelSelectItems(selectClause.replace(/^SELECT\s+/i, ''));
  const windows: WindowColumnMeta[] = [];

  for (const item of items) {
    if (!/\bOVER\s*\(/i.test(item)) {
      continue;
    }

    const parsed = parseWindowExpression(item, resultColumns);
    if (!parsed) {
      throw new Error(`Unsupported window function expression: ${item}`);
    }

    windows.push({
      ...parsed,
      explanation: buildWindowExplanation(parsed),
      howComputed: buildWindowHowComputed(parsed),
      previewColumns: buildWindowPreviewColumns(parsed),
      previewRows: buildWindowPreviewRows(parsed, inputRows, selectedRows),
    });
  }

  return windows;
}

async function detectCaseColumns(
  selectClause: string,
  resultColumns: string[],
  inputRows: Record<string, unknown>[],
  runner: MysqlRunner,
  parsed: ParsedQuery,
  useCanonicalTail: boolean,
  canonicalSchema: ColumnDef[],
  currentFromSql: string,
  usesDistinct: boolean,
): Promise<CaseColumnMeta[]> {
  const items = splitTopLevelSelectItems(selectClause.replace(/^SELECT\s+/i, ''));
  const inputColumns = getColumns(inputRows);
  const parsedCases = items
    .map(item => parseCaseExpression(item, resultColumns, inputColumns))
    .filter((meta): meta is ParsedCaseExpression => meta !== null);

  if (parsedCases.length === 0) {
    return [];
  }

  const helperSql = buildCaseDebugQuery(
    selectClause,
    parsedCases,
    parsed,
    useCanonicalTail,
    canonicalSchema,
    currentFromSql,
    usesDistinct,
  );
  const helperRows = (await runCustomSelect(runner, helperSql)).slice(0, MAX_DISPLAY_ROWS);

  return parsedCases.map((meta, caseIndex) => ({
    outputColumn: meta.outputColumn,
    expression: meta.expression,
    inputColumns: meta.inputRefs.map(ref => ref.label),
    rowExplanations: helperRows.map((row) => ({
      matchedRule: String(row[`__sql_debug_case_${caseIndex}_branch`] ?? 'No matching branch'),
      returnedValue: row[meta.outputColumn],
      inputValues: meta.inputRefs.map((ref, inputIndex) => ({
        column: ref.label,
        value: row[`__sql_debug_case_${caseIndex}_input_${inputIndex}`],
      })),
    })),
  }));
}

function splitTopLevelSelectItems(selectBody: string): string[] {
  const items: string[] = [];
  let depth = 0;
  let current = '';
  let quote: string | null = null;

  for (let i = 0; i < selectBody.length; i++) {
    const ch = selectBody[i];

    if (quote) {
      current += ch;
      if (ch === quote && selectBody[i - 1] !== '\\') {
        quote = null;
      }
      continue;
    }

    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === '(') depth += 1;
    if (ch === ')') depth = Math.max(0, depth - 1);

    if (ch === ',' && depth === 0) {
      if (current.trim()) items.push(current.trim());
      current = '';
      continue;
    }

    current += ch;
  }

  if (current.trim()) items.push(current.trim());
  return items;
}

function parseWindowExpression(
  item: string,
  resultColumns: string[],
): Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'> | null {
  const aliasMatch = item.match(/^(.*?)(?:\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?)$/i);
  const expr = aliasMatch ? aliasMatch[1].trim() : item.trim();
  const alias = aliasMatch?.[2];

  const match = expr.match(/^(ROW_NUMBER|RANK|DENSE_RANK|AVG|SUM|COUNT|MAX|MIN)\s*\((.*?)\)\s*OVER\s*\(([\s\S]+)\)$/i);
  if (!match) {
    return null;
  }

  const functionName = match[1].toUpperCase();
  const sourceArg = match[2].trim();
  const overClause = match[3].trim();
  if (/\b(ROWS|RANGE)\b/i.test(overClause)) {
    throw new Error(`Window frame clauses are not supported yet: ${item}`);
  }

  const { partitionBy, orderBy, orderByTerms } = parseWindowOverClause(overClause);
  const outputColumn = resolveWindowOutputColumn(alias, expr, resultColumns);

  return {
    outputColumn,
    expression: item.trim(),
    functionName,
    sourceColumn: sourceArg && sourceArg !== '*' ? bareIdentifier(sourceArg) : undefined,
    partitionBy,
    orderBy,
    orderByTerms,
  };
}

function parseWindowOverClause(overClause: string): {
  partitionBy: string[];
  orderBy: string[];
  orderByTerms: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
} {
  let partitionBy: string[] = [];
  let orderBy: string[] = [];
  let orderByTerms: Array<{ column: string; direction: 'ASC' | 'DESC' }> = [];
  const normalized = overClause.trim();

  const orderIndex = indexOfTopLevelKeyword(normalized, 'ORDER BY');
  const partitionIndex = indexOfTopLevelKeyword(normalized, 'PARTITION BY');

  if (partitionIndex !== -1) {
    const partBody = normalized.slice(partitionIndex + 'PARTITION BY'.length, orderIndex === -1 ? normalized.length : orderIndex).trim();
    partitionBy = splitTopLevelSelectItems(partBody).map(bareIdentifier).filter(Boolean);
  }
  if (orderIndex !== -1) {
    const orderBody = normalized.slice(orderIndex + 'ORDER BY'.length).trim();
    orderByTerms = splitTopLevelSelectItems(orderBody)
      .map(term => {
        const directionMatch = term.match(/\s+(ASC|DESC)\s*$/i);
        const direction = (directionMatch?.[1]?.toUpperCase() as 'ASC' | 'DESC' | undefined) ?? 'ASC';
        const column = bareIdentifier(term.replace(/\s+(ASC|DESC)\s*$/i, ''));
        return column ? { column, direction } : null;
      })
      .filter((term): term is { column: string; direction: 'ASC' | 'DESC' } => term !== null);
    orderBy = orderByTerms.map(term => `${term.column} ${term.direction}`);
  }

  return { partitionBy, orderBy, orderByTerms };
}

function buildCaseDebugQuery(
  selectClause: string,
  cases: ParsedCaseExpression[],
  parsed: ParsedQuery,
  useCanonicalTail: boolean,
  canonicalSchema: ColumnDef[],
  currentFromSql: string,
  usesDistinct: boolean,
): string {
  if (useCanonicalTail) {
    const helperColumns = cases.flatMap((meta, caseIndex) => buildCaseHelperSelectParts(meta, caseIndex)).join(', ');
    const selectList = canonicalSchema
      .map(col => (col.sqlAlias ? `${col.sqlExpr} AS ${col.sqlAlias}` : col.sqlExpr))
      .concat(helperColumns ? [helperColumns] : [])
      .join(', ');
    const tail = parsed.whereClause ?? '';
    return `SELECT${usesDistinct ? ' DISTINCT' : ''} ${selectList} ${currentFromSql}${tail ? ` ${tail}` : ''}`;
  }

  const selectBody = selectClause.replace(/^SELECT\s+/i, '').trim();
  const helperColumns = cases.flatMap((meta, caseIndex) => buildCaseHelperSelectParts(meta, caseIndex));
  const helperSelectClause = `SELECT ${[selectBody, ...helperColumns].join(', ')}`;
  return buildFinalQuery({ ...parsed, selectClause: helperSelectClause }, { upTo: 'SELECT' });
}

function buildCaseHelperSelectParts(meta: ParsedCaseExpression, caseIndex: number): string[] {
  const branchClauses = meta.branches.map(branch =>
    `WHEN ${branch.condition} THEN ${quoteSqlString(branch.label)}`
  );
  const branchExpr = `CASE ${branchClauses.join(' ')} ELSE ${quoteSqlString(meta.elseLabel)} END AS \`__sql_debug_case_${caseIndex}_branch\``;
  const inputExprs = meta.inputRefs.map((ref, inputIndex) =>
    `${ref.expr} AS \`__sql_debug_case_${caseIndex}_input_${inputIndex}\``
  );
  return [branchExpr, ...inputExprs];
}

function resolveWindowOutputColumn(alias: string | undefined, expr: string, resultColumns: string[]): string {
  if (alias) {
    const matched = resultColumns.find(col => bareIdentifier(col).toLowerCase() === alias.toLowerCase());
    if (matched) return matched;
  }

  const normalizedExpr = normalizeSqlFragment(expr);
  return resultColumns.find(col => normalizeSqlFragment(col) === normalizedExpr) ?? alias ?? expr;
}

function resolveCaseOutputColumn(alias: string | undefined, expr: string, resultColumns: string[]): string {
  if (alias) {
    const matched = resultColumns.find(col => bareIdentifier(col).toLowerCase() === alias.toLowerCase());
    if (matched) return matched;
  }

  const normalizedExpr = normalizeSqlFragment(expr);
  return resultColumns.find(col => normalizeSqlFragment(col) === normalizedExpr) ?? alias ?? expr;
}

function buildWindowExplanation(meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>): string {
  if (meta.functionName === 'ROW_NUMBER') {
    return 'Shows each row’s position in its window after partitioning and ordering are applied.';
  }
  if (meta.functionName === 'RANK') {
    return 'Shows the rank of each row in its window, with gaps after ties.';
  }
  if (meta.functionName === 'DENSE_RANK') {
    return 'Shows the rank of each row in its window, without gaps after ties.';
  }
  return `Shows the ${meta.functionName} value computed over the rows in the same window.`;
}

function buildWindowHowComputed(meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>): string[] {
  const steps = [
    meta.partitionBy.length > 0
      ? `Rows were split into groups using ${meta.partitionBy.join(', ')}.`
      : 'All rows stayed in one group.',
    meta.orderBy.length > 0
      ? `Rows inside each group were ordered by ${meta.orderBy.join(', ')}.`
      : 'No window ordering was provided, so the existing row order was used.',
  ];

  if (meta.functionName === 'ROW_NUMBER') {
    steps.push('A running row number was assigned from top to bottom inside each group.');
  } else if (meta.functionName === 'RANK') {
    steps.push('Rows with the same ordering values received the same rank, and the next rank skipped ahead.');
  } else if (meta.functionName === 'DENSE_RANK') {
    steps.push('Rows with the same ordering values received the same rank, and the next distinct value received the next consecutive rank.');
  } else {
    steps.push(`The ${meta.functionName} result was calculated for the rows visible in each ordered group.`);
  }

  return steps;
}

function buildWindowPreviewColumns(meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>): string[] {
  return dedupeStrings([
    ...meta.partitionBy,
    ...meta.orderByTerms.map(term => term.column),
    ...(meta.sourceColumn ? [meta.sourceColumn] : []),
    meta.outputColumn,
  ]);
}

function buildWindowPreviewRows(
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
  inputRows: Record<string, unknown>[],
  selectedRows: Record<string, unknown>[],
): Record<string, unknown>[] {
  const previewColumns = buildWindowPreviewColumns(meta);
  const limitedInput = inputRows.slice(0, 20);
  const limitedSelected = selectedRows.slice(0, 20);

  const previewRows = limitedInput.map((row, index) => {
    const preview: Record<string, unknown> = {};
    for (const col of previewColumns) {
      if (col === meta.outputColumn) {
        preview[col] = limitedSelected[index]?.[meta.outputColumn];
      } else {
        preview[col] = readRowValue(row, col);
      }
    }
    return preview;
  });

  return previewRows.sort((left, right) => comparePreviewRows(left, right, meta));
}

function comparePreviewRows(
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
): number {
  for (const col of meta.partitionBy) {
    const result = compareUnknown(readRowValue(left, col), readRowValue(right, col));
    if (result !== 0) return result;
  }

  for (const term of meta.orderByTerms) {
    const result = compareUnknown(readRowValue(left, term.column), readRowValue(right, term.column));
    if (result !== 0) {
      return term.direction === 'DESC' ? -result : result;
    }
  }

  return compareUnknown(readRowValue(left, meta.outputColumn), readRowValue(right, meta.outputColumn));
}

function compareUnknown(left: unknown, right: unknown): number {
  const leftNum = Number(left);
  const rightNum = Number(right);
  if (!Number.isNaN(leftNum) && !Number.isNaN(rightNum)) {
    return leftNum - rightNum;
  }

  const leftStr = String(left ?? '');
  const rightStr = String(right ?? '');
  return leftStr.localeCompare(rightStr, undefined, { numeric: true, sensitivity: 'base' });
}

function readRowValue(row: Record<string, unknown>, targetCol: string): unknown {
  if (targetCol in row) return row[targetCol];
  const bareTarget = bareIdentifier(targetCol).toLowerCase();
  const matchedKey = Object.keys(row).find(key => bareIdentifier(key).toLowerCase() === bareTarget);
  return matchedKey ? row[matchedKey] : null;
}

function bareIdentifier(value: string): string {
  return value.replace(/`/g, '').trim().split('.').pop() ?? value.trim();
}

function normalizeSqlFragment(value: string): string {
  return value.replace(/`/g, '').replace(/\s+/g, ' ').trim().toLowerCase();
}

function dedupeStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(value);
  }
  return result;
}

function extractCaseInputRefs(
  conditions: string[],
  availableInputColumns: string[],
): Array<{ expr: string; label: string }> {
  const refs: Array<{ expr: string; label: string }> = [];
  const seen = new Set<string>();
  const availableBare = new Set(availableInputColumns.map(col => bareIdentifier(col).toLowerCase()));
  const keywordSet = new Set([
    'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'WHEN', 'THEN', 'ELSE', 'END',
    'CASE', 'TRUE', 'FALSE', 'AS', 'ON', 'OVER', 'PARTITION', 'BY', 'ORDER', 'ASC', 'DESC',
  ]);

  for (const condition of conditions) {
    const tokenRx = /(`[^`]+`(?:\.`[^`]+`)?|[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)/g;
    let match: RegExpExecArray | null;
    while ((match = tokenRx.exec(condition)) !== null) {
      const raw = match[1];
      const token = stripTicks(raw);
      const bare = bareIdentifier(token);
      const key = token.toLowerCase();
      const nextChar = condition.slice(match.index + raw.length).trimStart()[0];
      if (keywordSet.has(bare.toUpperCase())) continue;
      if (nextChar === '(') continue;
      if (!token.includes('.') && !availableBare.has(bare.toLowerCase())) continue;
      if (seen.has(key)) continue;
      seen.add(key);
      refs.push({ expr: raw, label: bare });
    }
  }

  return refs;
}

function findNextTopLevelKeyword(text: string, start: number, keywords: string[]): number {
  let depth = 0;
  let quote: string | null = null;

  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (quote) {
      if (ch === quote && text[i - 1] !== '\\') {
        quote = null;
      }
      continue;
    }
    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      continue;
    }
    if (ch === '(') {
      depth += 1;
      continue;
    }
    if (ch === ')') {
      depth = Math.max(0, depth - 1);
      continue;
    }
    if (depth === 0 && keywords.some(keyword => matchesWordAt(text, i, keyword))) {
      return i;
    }
  }

  return -1;
}

function quoteSqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function indexOfTopLevelKeyword(text: string, keyword: string): number {
  const upper = text.toUpperCase();
  const target = keyword.toUpperCase();
  let depth = 0;
  let quote: string | null = null;

  for (let i = 0; i <= upper.length - target.length; i++) {
    const ch = text[i];
    if (quote) {
      if (ch === quote && text[i - 1] !== '\\') quote = null;
      continue;
    }
    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      continue;
    }
    if (ch === '(') depth += 1;
    if (ch === ')') depth = Math.max(0, depth - 1);
    if (depth === 0 && upper.slice(i, i + target.length) === target) {
      return i;
    }
  }
  return -1;
}

/**
 * Performs the JOIN in JavaScript, producing rows with explicitly qualified column names.
 * Duplicate column names across left and right tables are preserved as separate keys:
 *   left side:  "PlayerId"          (plain — original name)
 *   right side: "prfos.PlayerId"    (alias-qualified — prevents collision)
 * This faithfully mirrors what SQL returns and makes the debugger display accurate.
 */
function buildJoinDisplay(
  leftRows: Record<string, unknown>[],
  rightRows: Record<string, unknown>[],
  leftKeyCol: string,
  rightKeyCol: string,
  rightAlias: string,
  joinType: string,
): { rows: Record<string, unknown>[]; columns: string[] } {
  const leftCols  = leftRows.length  > 0 ? Object.keys(leftRows[0])  : [];
  const rightCols = rightRows.length > 0 ? Object.keys(rightRows[0]) : [];
  const leftColSet = new Set(leftCols);

  // Right-side output keys: qualify any name that already exists on the left
  const rightOutputKeys = rightCols.map((col) =>
    leftColSet.has(col) ? `${rightAlias}.${col}` : col
  );
  const outputColumns = [...leftCols, ...rightOutputKeys];

  const isLeft  = /\bLEFT\b/i.test(joinType);
  const isRight = /\bRIGHT\b/i.test(joinType);
  const isCross = /\bCROSS\b/i.test(joinType);

  function mergeRow(
    lRow: Record<string, unknown> | null,
    rRow: Record<string, unknown> | null,
  ): Record<string, unknown> {
    const row: Record<string, unknown> = {};
    for (const col of leftCols)  row[col] = lRow?.[col] ?? null;
    rightCols.forEach((col, i) => { row[rightOutputKeys[i]] = rRow?.[col] ?? null; });
    return row;
  }

  if (isCross) {
    return {
      rows: leftRows.flatMap((l) => rightRows.map((r) => mergeRow(l, r))),
      columns: outputColumns,
    };
  }

  // Build a key-indexed bucket map on the right side for O(1) lookup
  const rightIndex = new Map<string, number[]>();
  rightRows.forEach((rRow, idx) => {
    const key = String(rRow[rightKeyCol] ?? '');
    const bucket = rightIndex.get(key);
    if (bucket) bucket.push(idx);
    else rightIndex.set(key, [idx]);
  });

  const result: Record<string, unknown>[] = [];
  const matchedRightSet = new Set<number>();

  for (const lRow of leftRows) {
    const lKey = String(lRow[leftKeyCol] ?? '');
    const matchIdxs = rightIndex.get(lKey) ?? [];
    if (matchIdxs.length > 0) {
      for (const idx of matchIdxs) {
        result.push(mergeRow(lRow, rightRows[idx]));
        matchedRightSet.add(idx);
      }
    } else if (isLeft) {
      // LEFT JOIN: keep unmatched left rows with nulls for the right side
      result.push(mergeRow(lRow, null));
    }
  }

  // RIGHT JOIN: append unmatched right rows with nulls for the left side
  if (isRight) {
    rightRows.forEach((rRow, idx) => {
      if (!matchedRightSet.has(idx)) result.push(mergeRow(null, rRow));
    });
  }

  return { rows: result, columns: outputColumns };
}

function inferRelationship(
  leftRows: Record<string, unknown>[],
  rightRows: Record<string, unknown>[],
  leftKey: string,
  rightKey: string,
): JoinMeta['relationship'] {
  const leftDup = hasDuplicates(leftRows, leftKey);
  const rightDup = hasDuplicates(rightRows, rightKey);
  if (!leftDup && !rightDup) return 'one-to-one';
  if (!leftDup && rightDup) return 'one-to-many';
  if (leftDup && !rightDup) return 'many-to-one';
  return 'many-to-many';
}

function hasDuplicates(rows: Record<string, unknown>[], key: string): boolean {
  const seen = new Set<string>();
  for (const row of rows) {
    const v = String(row[key] ?? '');
    if (seen.has(v)) return true;
    seen.add(v);
  }
  return false;
}

/**
 * Builds a SELECT query using the canonical schema so that every column is
 * explicitly addressed by table alias and duplicate names are preserved as
 * distinct backtick-aliased expressions (e.g. `fdsro`.`PlayerId` AS `fdsro.PlayerId`).
 * This ensures MySQL2 returns correctly keyed row objects for all post-JOIN steps.
 */
function buildCanonicalQuery(
  canonicalSchema: ColumnDef[],
  fromAndJoinsSql: string,
  tailClauses: string = '',
  distinct = false,
): string {
  if (canonicalSchema.length === 0) {
    return `SELECT${distinct ? ' DISTINCT' : ''} * ${fromAndJoinsSql}${tailClauses ? ' ' + tailClauses : ''}`;
  }
  const selectList = canonicalSchema
    .map(col => (col.sqlAlias ? `${col.sqlExpr} AS ${col.sqlAlias}` : col.sqlExpr))
    .join(', ');
  return `SELECT${distinct ? ' DISTINCT' : ''} ${selectList} ${fromAndJoinsSql}${tailClauses ? ' ' + tailClauses : ''}`;
}

type ParsedCaseExpression = {
  outputColumn: string;
  expression: string;
  inputRefs: Array<{ expr: string; label: string }>;
  branches: Array<{ condition: string; label: string }>;
  elseLabel: string;
};

function parseCaseExpression(
  item: string,
  resultColumns: string[],
  availableInputColumns: string[],
): ParsedCaseExpression | null {
  const aliasMatch = item.match(/^(CASE[\s\S]+?END)(?:\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?)?$/i);
  const expr = aliasMatch ? aliasMatch[1].trim() : item.trim();
  const alias = aliasMatch?.[2];
  if (!/^CASE\b/i.test(expr)) {
    return null;
  }

  const innerBody = expr.replace(/^CASE\b/i, '').replace(/\bEND$/i, '').trim();
  if (!innerBody) {
    return null;
  }

  let cursor = 0;
  const branches: Array<{ condition: string; label: string }> = [];
  let elseLabel = 'ELSE';

  while (cursor < innerBody.length) {
    cursor = skipSqlWhitespace(innerBody, cursor);
    if (matchesWordAt(innerBody, cursor, 'WHEN')) {
      const condStart = skipSqlWhitespace(innerBody, cursor + 4);
      const thenIndex = findNextTopLevelKeyword(innerBody, condStart, ['THEN']);
      if (thenIndex === -1) {
        return null;
      }
      const condition = innerBody.slice(condStart, thenIndex).trim();
      const resultStart = skipSqlWhitespace(innerBody, thenIndex + 4);
      const nextIndex = findNextTopLevelKeyword(innerBody, resultStart, ['WHEN', 'ELSE']);
      const resultExpr = innerBody.slice(resultStart, nextIndex === -1 ? innerBody.length : nextIndex).trim();
      branches.push({
        condition,
        label: `WHEN ${condition} THEN ${resultExpr}`,
      });
      cursor = nextIndex === -1 ? innerBody.length : nextIndex;
      continue;
    }
    if (matchesWordAt(innerBody, cursor, 'ELSE')) {
      const elseStart = skipSqlWhitespace(innerBody, cursor + 4);
      const elseExpr = innerBody.slice(elseStart).trim();
      elseLabel = `ELSE ${elseExpr}`;
      break;
    }
    cursor += 1;
  }

  if (branches.length === 0) {
    return null;
  }

  const outputColumn = resolveCaseOutputColumn(alias, expr, resultColumns);
  const inputRefs = extractCaseInputRefs(branches.map(branch => branch.condition), availableInputColumns);

  return {
    outputColumn,
    expression: item.trim(),
    inputRefs,
    branches,
    elseLabel,
  };
}

function buildFinalQuery(parsed: ParsedQuery, opts: { upTo: 'GROUP BY' | 'HAVING' | 'SELECT' | 'ORDER BY' | 'LIMIT' }): string {
  const segments = [
    parsed.selectClause,
    parsed.fromClause,
    ...parsed.joins.map((j) => j.rawClause),
    parsed.whereClause,
  ].filter(Boolean);

  if (opts.upTo === 'GROUP BY' || opts.upTo === 'HAVING' || opts.upTo === 'SELECT' || opts.upTo === 'ORDER BY' || opts.upTo === 'LIMIT') {
    if (parsed.groupByClause) segments.push(parsed.groupByClause);
  }
  if (opts.upTo === 'HAVING' || opts.upTo === 'SELECT' || opts.upTo === 'ORDER BY' || opts.upTo === 'LIMIT') {
    if (parsed.havingClause) segments.push(parsed.havingClause);
  }
  if (opts.upTo === 'ORDER BY' || opts.upTo === 'LIMIT') {
    if (parsed.orderByClause) segments.push(parsed.orderByClause);
  }
  if (opts.upTo === 'LIMIT') {
    if (parsed.limitClause) segments.push(parsed.limitClause);
  }
  return segments.join(' ');
}

function attachBlockContext(step: DebugStep, block: QueryBlock, blockIndex: number): DebugStep {
  return {
    ...step,
    blockType: block.type,
    blockName: block.name,
    blockIndex,
    blockDependencies: block.dependencies.map(dep => dep.name),
    blockSourceText: block.rawSql,
    blockSourceStart: block.sourceStart,
  };
}

async function materializeBlock(block: QueryBlock, runner: MysqlRunner): Promise<void> {
  if (!block.materializedName) {
    return;
  }
  const safeName = escapeIdentifier(block.materializedName);
  await runner.execute(`DROP TEMPORARY TABLE IF EXISTS ${safeName}`);
  await runner.execute(`CREATE TEMPORARY TABLE ${safeName} AS ${block.sql}`);
}

function escapeIdentifier(name: string): string {
  return `\`${name.replace(/`/g, '``')}\``;
}

function parseSelectQuery(sql: string): ParsedQuery {
  const cleaned = sql.replace(/\s+/g, ' ').trim().replace(/;$/, '');
  if (!/^SELECT\s+/i.test(cleaned)) {
    throw new Error('Only SELECT queries are supported in this MVP.');
  }

  const fromIndex = indexOfKeyword(cleaned, ' FROM ');
  if (fromIndex === -1) {
    throw new Error('Could not find FROM clause.');
  }

  const selectClause = cleaned.slice(0, fromIndex).trim();

  const wherePos = indexOfKeyword(cleaned, ' WHERE ');
  const groupByPos = indexOfKeyword(cleaned, ' GROUP BY ');
  const havingPos = indexOfKeyword(cleaned, ' HAVING ');
  const orderByPos = indexOfKeyword(cleaned, ' ORDER BY ');
  const limitPos = indexOfKeyword(cleaned, ' LIMIT ');

  const clauseStarts = [wherePos, groupByPos, havingPos, orderByPos, limitPos].filter((v) => v !== -1);
  const firstTailPos = clauseStarts.length > 0 ? Math.min(...clauseStarts) : cleaned.length;

  const fromAndJoins = cleaned.slice(fromIndex + 'FROM '.length, firstTailPos).trim();
  const parsedFromAndJoins = parseFromAndJoins(fromAndJoins);

  return {
    selectClause,
    fromClause: `FROM ${parsedFromAndJoins.baseClause}`,
    joins: parsedFromAndJoins.joins,
    whereClause: extractClause(cleaned, ' WHERE ', [' GROUP BY ', ' HAVING ', ' ORDER BY ', ' LIMIT ']),
    groupByClause: extractClause(cleaned, ' GROUP BY ', [' HAVING ', ' ORDER BY ', ' LIMIT ']),
    havingClause: extractClause(cleaned, ' HAVING ', [' ORDER BY ', ' LIMIT ']),
    orderByClause: extractClause(cleaned, ' ORDER BY ', [' LIMIT ']),
    limitClause: extractClause(cleaned, ' LIMIT ', []),
  };
}

function isSelectDistinct(selectClause: string): boolean {
  return /^SELECT\s+DISTINCT\b/i.test(selectClause.trim());
}

function removeDistinctFromSelectClause(selectClause: string): string {
  return selectClause.replace(/^(\s*SELECT)\s+DISTINCT\b\s*/i, '$1 ');
}

function parseFromAndJoins(fromAndJoins: string): { baseClause: string; joins: ParsedJoin[] } {
  const joinRegex = /\b((?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN)\b/ig;
  const matches = [...fromAndJoins.matchAll(joinRegex)];
  if (matches.length === 0) {
    return { baseClause: fromAndJoins.trim(), joins: [] };
  }

  const first = matches[0].index ?? -1;
  const baseClause = fromAndJoins.slice(0, first).trim();
  const joins: ParsedJoin[] = [];

  for (let i = 0; i < matches.length; i++) {
    const start = matches[i].index ?? 0;
    const end = i + 1 < matches.length ? (matches[i + 1].index ?? fromAndJoins.length) : fromAndJoins.length;
    const raw = fromAndJoins.slice(start, end).trim();

    const rawNormalized = raw.replace(/\s+/g, ' ').trim();
    const pieces = rawNormalized.split(/\s+ON\s+/i);
    if (pieces.length !== 2) {
      throw new Error('JOIN without ON is not supported in this MVP.');
    }

    const joinHeader = pieces[0].trim();
    const onClause = pieces[1].trim();
    const headerMatch = joinHeader.match(/^((?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN)\s+([\w`\.]+)(?:\s+(?:AS\s+)?([\w`]+))?$/i);
    if (!headerMatch) {
      throw new Error('Unsupported JOIN shape in this MVP.');
    }

    const joinType = headerMatch[1].replace(/\s+/g, ' ').trim().toUpperCase();
    const tableName = stripTicks(headerMatch[2]);
    const tableAlias = stripTicks(headerMatch[3] || tableName.split('.').pop() || tableName);
    const onMatch = onClause.match(/^([\w`]+\.[\w`]+)\s*=\s*([\w`]+\.[\w`]+)$/i);
    if (!onMatch) {
      throw new Error('Only simple equality JOIN conditions are supported in this MVP.');
    }

    joins.push({
      joinType,
      tableName,
      tableAlias,
      rawClause: rawNormalized,
      onClause,
      leftExpr: normalizeQualified(onMatch[1]),
      rightExpr: normalizeQualified(onMatch[2]),
    });
  }

  return { baseClause, joins };
}

function extractClause(source: string, keyword: string, nextKeywords: string[]): string | undefined {
  const start = indexOfKeyword(source, keyword);
  if (start === -1) return undefined;
  const tail = nextKeywords
    .map((k) => indexOfKeyword(source, k))
    .filter((v) => v !== -1 && v > start);
  const end = tail.length > 0 ? Math.min(...tail) : source.length;
  return source.slice(start, end).trim();
}

function indexOfKeyword(source: string, keyword: string): number {
  return indexOfTopLevelKeyword(source, keyword.trim());
}

const SQL_KEYWORDS = new Set([
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'OUTER',
  'ON', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'AS',
  'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BY', 'SET',
]);

function resolveAlias(rawAlias: string | undefined, tableName: string): string {
  if (rawAlias && !SQL_KEYWORDS.has(rawAlias.toUpperCase())) {
    return stripTicks(rawAlias);
  }
  return stripTicks(tableName.split('.').pop() || tableName);
}

function extractTableAliases(fromAndJoinsSql: string): string[] {
  const aliases: string[] = [];
  const fromMatch = fromAndJoinsSql.match(/FROM\s+([\w`\.]+)(?:\s+(?:AS\s+)?([\w`]+))?/i);
  if (fromMatch) {
    aliases.push(resolveAlias(fromMatch[2], fromMatch[1]));
  }

  for (const match of fromAndJoinsSql.matchAll(/JOIN\s+([\w`\.]+)(?:\s+(?:AS\s+)?([\w`]+))?/ig)) {
    aliases.push(resolveAlias(match[2], match[1]));
  }
  return aliases;
}

function extractBaseAlias(fromClause: string): string {
  const m = fromClause.match(/FROM\s+([\w`\.]+)(?:\s+(?:AS\s+)?([\w`]+))?/i);
  return stripTicks(m?.[2] || m?.[1]?.split('.').pop() || 'left');
}

function extractBaseTableName(fromClause: string): string {
  const m = fromClause.match(/FROM\s+([\w`\.]+)/i);
  return stripTicks(m?.[1] || 'table');
}

function stripTicks(v: string): string {
  return String(v).replace(/`/g, '');
}

function normalizeQualified(v: string): string {
  return stripTicks(v).replace(/\s+/g, '');
}

function skipSqlWhitespace(text: string, index: number): number {
  let i = index;
  while (i < text.length && /\s/.test(text[i])) {
    i += 1;
  }
  return i;
}

function matchesWordAt(text: string, index: number, word: string): boolean {
  const segment = text.slice(index, index + word.length);
  if (segment.toUpperCase() !== word.toUpperCase()) {
    return false;
  }
  const before = index > 0 ? text[index - 1] : ' ';
  const after = index + word.length < text.length ? text[index + word.length] : ' ';
  return !/[A-Za-z0-9_$]/.test(before) && !/[A-Za-z0-9_$]/.test(after);
}

function findMatchingParenSql(text: string, openIndex: number): number {
  let depth = 0;
  let i = openIndex;

  while (i < text.length) {
    const ch = text[i];
    if (ch === '\'' || ch === '"' || ch === '`') {
      i = skipQuotedSql(text, i, ch);
      continue;
    }
    if (ch === '(') {
      depth += 1;
    } else if (ch === ')') {
      depth -= 1;
      if (depth === 0) {
        return i;
      }
    }
    i += 1;
  }

  throw new Error('Unbalanced parentheses while parsing WHERE IN subquery.');
}

function skipQuotedSql(text: string, start: number, quote: string): number {
  let i = start + 1;
  while (i < text.length) {
    if (text[i] === '\\') {
      i += 2;
      continue;
    }
    if (text[i] === quote) {
      if ((quote === '\'' || quote === '"') && text[i + 1] === quote) {
        i += 2;
        continue;
      }
      return i + 1;
    }
    i += 1;
  }

  throw new Error('Unterminated quoted string while parsing WHERE IN subquery.');
}
