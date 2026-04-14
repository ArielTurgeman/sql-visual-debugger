import type { MysqlRunner } from '../mysql/mysqlRunner';
import { parseQueryBlocks, type QueryBlock } from './queryBlocks';
import { buildJoinDisplay, inferRelationship } from './stepEngineExplain';
import {
  buildAggSummary,
  buildWhereInSubqueryMeta,
  buildWhereScalarSubqueryMeta,
  detectAggColumns,
  detectCaseColumns,
  detectGroupByColumns,
  detectHavingColumns,
  detectOrderByColumns,
  detectWhereColumns,
  detectWindowColumns,
} from './stepEngineMetadata';
import {
  buildCanonicalQuery,
  buildFinalQuery,
  extractBaseAlias,
  extractBaseTableName,
  extractTableAliases,
  isSelectDistinct,
  parseSelectQuery,
  removeDistinctFromSelectClause,
  stripTicks,
} from './stepEngineParsing';
import {
  applyFromDependencyDetails,
  attachBlockContext,
  buildFromStep,
  buildGroupByStep,
  buildHavingStep,
  buildJoinStep,
  buildLimitStep,
  buildOrderByStep,
  buildSelectStep,
  buildWhereStep,
} from './stepEngineStepBuilders';
import type { CaseColumnMeta, ColumnDef, DebugStep, DistinctMeta, JoinMeta } from './stepEngineTypes';
import { MAX_DISPLAY_ROWS } from './stepEngineTypes';

export type {
  CaseColumnMeta,
  CaseRowExplanation,
  DebugStep,
  DistinctMeta,
  JoinMeta,
  WhereInSubqueryMeta,
  WhereScalarSubqueryMeta,
  WindowColumnMeta,
} from './stepEngineTypes';

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
  let fromStep = buildFromStep({
    fromDependencyLabel,
    fromTableName,
    currentRows,
    sqlFragment: block.fromSource?.originalClause ?? parsed.fromClause,
    columns: getColumns(currentRows),
  });
  if (fromDependency) {
    fromStep = applyFromDependencyDetails(fromStep, fromDependency, currentRows.length);
  }
  steps.push(fromStep);

  const baseAlias = extractBaseAlias(parsed.fromClause);
  let canonicalSchema: ColumnDef[] = getColumns(baseRows).map(col => ({
    displayName: col,
    sqlExpr: `\`${baseAlias}\`.\`${col}\``,
  }));
  let canonicalJoinIndicators: string[] = [];
  const isSelectStar = /^select\s+\*$/i.test(parsed.selectClause.trim());

  for (const join of parsed.joins) {
    const leftBefore = currentRows.length;
    currentFromSql = `${currentFromSql} ${join.rawClause}`;

    const rightRows = await runAliasedSelect(runner, `FROM ${join.tableName} ${join.tableAlias}`.trim());
    const leftExprTableRaw = join.leftExpr.includes('.') ? join.leftExpr.split('.')[0] : '';
    const leftExprCol = join.leftExpr.includes('.') ? join.leftExpr.split('.').pop()! : join.leftExpr;
    const rightExprCol = join.rightExpr.includes('.') ? join.rightExpr.split('.').pop()! : join.rightExpr;
    const leftExprBelongsToRight =
      leftExprTableRaw.toLowerCase() === join.tableAlias.toLowerCase() ||
      leftExprTableRaw.toLowerCase() === join.tableName.toLowerCase();
    const leftKeyCol = leftExprBelongsToRight ? rightExprCol : leftExprCol;
    const rightKeyCol = leftExprBelongsToRight ? leftExprCol : rightExprCol;

    const { rows: joinedRows, columns: joinedColumns } = buildJoinDisplay(
      currentRows,
      rightRows,
      leftKeyCol,
      rightKeyCol,
      join.tableAlias,
      join.joinType,
    );

    const leftColDisplayNames = new Set(canonicalSchema.map(col => col.displayName));
    const rightCols = getColumns(rightRows);
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

    const joinIndicatorColumns = rightCols.map(col => leftColDisplayNames.has(col) ? `${join.tableAlias}.${col}` : col);
    canonicalJoinIndicators = [...canonicalJoinIndicators, ...joinIndicatorColumns];
    const relationship = inferRelationship(currentRows, rightRows, leftKeyCol, rightKeyCol);
    const rightDisplay = join.tableAlias !== join.tableName
      ? `\`${join.tableName}\` (as \`${join.tableAlias}\`)`
      : `\`${join.tableName}\``;

    const joinMeta: JoinMeta = {
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
    };

    steps.push(buildJoinStep({
      joinType: join.joinType,
      rightDisplay,
      leftExpr: join.leftExpr,
      rightExpr: join.rightExpr,
      sqlFragment: join.rawClause,
      leftBefore,
      joinedRows,
      joinedColumns,
      relationship,
      joinMeta,
    }));
    currentRows = joinedRows;
  }

  const schemaContext = canonicalJoinIndicators.length > 0
    ? { joinIndicatorColumns: canonicalJoinIndicators }
    : undefined;

  if (parsed.whereClause) {
    const before = currentRows.length;
    const preFilterRows = currentRows.slice(0, 200);
    const preFilterColumns = getColumns(currentRows);
    const whereColumns = detectWhereColumns(parsed.whereClause, preFilterColumns);
    const whereInSubquery = await buildWhereInSubqueryMeta(parsed.whereClause, currentFromSql, runner, runCustomSelect, getColumns);
    const whereScalarSubquery = await buildWhereScalarSubqueryMeta(parsed.whereClause, currentFromSql, runner, runCustomSelect, getColumns);
    const rows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, parsed.whereClause));
    const removed = before - rows.length;
    steps.push(buildWhereStep({
      explanation: removed === 0
        ? `Applied the WHERE condition. Every one of the ${before.toLocaleString()} rows satisfied it ג€” none were removed.`
        : `Applied the WHERE condition. ${removed.toLocaleString()} row${removed === 1 ? '' : 's'} failed the check and ${removed === 1 ? 'was' : 'were'} discarded.`,
      impact: removed === 0
        ? `No rows removed. All ${rows.length.toLocaleString()} rows passed the filter.`
        : `${rows.length.toLocaleString()} of ${before.toLocaleString()} rows passed the filter (${removed.toLocaleString()} removed).`,
      sqlFragment: parsed.whereClause,
      rowsBefore: before,
      rows,
      columns: getColumns(rows),
      schemaContext,
      preFilterRows,
      preFilterColumns,
      whereColumns,
      whereInSubquery,
      whereScalarSubquery,
    }));
    currentRows = rows;
  }

  if (parsed.groupByClause) {
    const before = currentRows.length;
    const rows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'GROUP BY' }));
    const groupedCols = getColumns(rows);
    canonicalSchema = groupedCols.map(col => ({ displayName: col, sqlExpr: `\`${col}\`` }));
    canonicalJoinIndicators = [];
    steps.push(buildGroupByStep({
      before,
      rows,
      groupedCols,
      groupKeys: parsed.groupByClause.replace(/^GROUP\s+BY\s+/i, '').trim(),
      sqlFragment: parsed.groupByClause,
      groupByColumns: detectGroupByColumns(parsed.groupByClause, groupedCols),
      aggColumns: detectAggColumns(parsed.selectClause, groupedCols),
      aggSummary: buildAggSummary(parsed.selectClause) || undefined,
      preGroupRows: currentRows.slice(0, 500),
      preGroupColumns: getColumns(currentRows),
    }));
    currentRows = rows;
  }

  if (parsed.havingClause) {
    const before = currentRows.length;
    const preFilterRows = currentRows.slice(0, 200);
    const preFilterColumns = getColumns(currentRows);
    const rows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'HAVING' }));
    const havingCols = getColumns(rows);
    canonicalSchema = havingCols.map(col => ({ displayName: col, sqlExpr: `\`${col}\`` }));
    steps.push(buildHavingStep({
      before,
      rows,
      columns: havingCols,
      sqlFragment: parsed.havingClause,
      preFilterRows,
      preFilterColumns,
      whereColumns: detectHavingColumns(parsed.havingClause, parsed.selectClause, preFilterColumns),
    }));
    currentRows = rows;
  }

  const finalBaseBefore = currentRows.length;
  const usesDistinct = isSelectDistinct(parsed.selectClause);
  const useCanonicalTail = isSelectStar && !parsed.groupByClause && !parsed.havingClause;
  let selectedRows: Record<string, unknown>[];
  let distinctMeta: DistinctMeta | undefined;

  if (useCanonicalTail) {
    const tail = parsed.whereClause ?? '';
    selectedRows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, tail, usesDistinct));
    if (usesDistinct) {
      const preDistinctRows = await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, tail));
      distinctMeta = { columns: getColumns(preDistinctRows), rows: preDistinctRows.slice(0, MAX_DISPLAY_ROWS) };
    }
  } else {
    selectedRows = await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'SELECT' }));
    if (usesDistinct) {
      const preDistinctRows = await runCustomSelect(
        runner,
        buildFinalQuery({ ...parsed, selectClause: removeDistinctFromSelectClause(parsed.selectClause) }, { upTo: 'SELECT' }),
      );
      distinctMeta = { columns: getColumns(preDistinctRows), rows: preDistinctRows.slice(0, MAX_DISPLAY_ROWS) };
    }
  }

  const selectedColumns = getColumns(selectedRows);
  const caseColumns: CaseColumnMeta[] = await detectCaseColumns(
    parsed.selectClause,
    selectedColumns,
    currentRows,
    runner,
    parsed,
    useCanonicalTail,
    canonicalSchema,
    currentFromSql,
    usesDistinct,
    runCustomSelect,
    getColumns,
  );
  steps.push(buildSelectStep({
    isSelectStar,
    sqlFragment: parsed.selectClause,
    rowsBefore: finalBaseBefore,
    selectedRows,
    columns: selectedColumns,
    schemaContext,
    windowColumns: detectWindowColumns(parsed.selectClause, selectedColumns, currentRows, selectedRows),
    caseColumns,
    preSelectRows: currentRows.slice(0, MAX_DISPLAY_ROWS),
    preSelectColumns: getColumns(currentRows),
    distinctMeta,
  }));
  currentRows = selectedRows;

  if (parsed.orderByClause) {
    const before = currentRows.length;
    const rows = useCanonicalTail
      ? await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, [parsed.whereClause, parsed.orderByClause].filter(Boolean).join(' ')))
      : await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'ORDER BY' }));
    const columns = getColumns(rows);
    steps.push(buildOrderByStep({
      orderKeys: parsed.orderByClause.replace(/^ORDER\s+BY\s+/i, '').trim(),
      sqlFragment: parsed.orderByClause,
      rowsBefore: before,
      rows,
      columns,
      schemaContext,
      sortColumns: detectOrderByColumns(parsed.orderByClause, columns),
    }));
    currentRows = rows;
  }

  if (parsed.limitClause) {
    const before = currentRows.length;
    const rows = useCanonicalTail
      ? await runCustomSelect(runner, buildCanonicalQuery(canonicalSchema, currentFromSql, [parsed.whereClause, parsed.orderByClause, parsed.limitClause].filter(Boolean).join(' ')))
      : await runCustomSelect(runner, buildFinalQuery(parsed, { upTo: 'LIMIT' }));
    steps.push(buildLimitStep({
      limitN: parsed.limitClause.replace(/^LIMIT\s+/i, '').trim() || 'N',
      sqlFragment: parsed.limitClause,
      rowsBefore: before,
      rows,
      columns: getColumns(rows),
      schemaContext,
    }));
  }

  return steps;
}

async function runAliasedSelect(runner: MysqlRunner, fromAndJoinsSql: string): Promise<Record<string, unknown>[]> {
  const aliases = extractTableAliases(fromAndJoinsSql);
  const selectList = aliases.length > 0 ? aliases.map(alias => `\`${alias}\`.*`).join(', ') : '*';
  return normalizeRows(await runner.query(`SELECT ${selectList} ${fromAndJoinsSql}`));
}

async function runCustomSelect(runner: MysqlRunner, sql: string): Promise<Record<string, unknown>[]> {
  return normalizeRows(await runner.query(sql));
}

function normalizeRows(rows: unknown): Record<string, unknown>[] {
  if (!Array.isArray(rows)) return [];
  return rows as Record<string, unknown>[];
}

function getColumns(rows: Record<string, unknown>[]): string[] {
  return rows.length > 0 ? Object.keys(rows[0]) : [];
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
  return `\`${stripTicks(name).replace(/`/g, '``')}\``;
}
