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
  splitTopLevelSelectItems,
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
  const resolutionContext = buildBlockResolutionContext(blocks);

  for (const [blockIndex, block] of blocks.entries()) {
    const blockSteps = await executeSingleQueryBlockSteps(block, runner, resolutionContext);
    steps.push(...blockSteps.map(step => attachBlockContext(step, block, blockIndex)));
  }

  return steps;
}

type BlockResolutionContext = {
  byReferenceName: Map<string, QueryBlock>;
};

async function executeSingleQueryBlockSteps(
  block: QueryBlock,
  runner: MysqlRunner,
  resolutionContext: BlockResolutionContext,
): Promise<DebugStep[]> {
  const parsed = parseSelectQuery(block.sql);
  const steps: DebugStep[] = [];
  const runBlockSelect = (sql: string) =>
    runCustomSelect(runner, renderBlockSql(block, sql, resolutionContext));

  const baseRows = await runAliasedSelect(runBlockSelect, parsed.fromClause);
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

    const rightRows = await runAliasedSelect(runBlockSelect, `FROM ${join.tableName} ${join.tableAlias}`.trim());
    const leftExprTableRaw = join.leftExpr.includes('.') ? join.leftExpr.split('.')[0] : '';
    const leftExprCol = resolveJoinDisplayKey(currentRows, join.leftExpr);
    const rightExprCol = resolveJoinDisplayKey(rightRows, join.rightExpr);
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
    const whereColumns = detectWhereColumns(parsed.whereClause, preFilterColumns, canonicalSchema);
    const whereInSubquery = await buildWhereInSubqueryMeta(parsed.whereClause, currentFromSql, runner, runBlockSelect, getColumns);
    const whereScalarSubquery = await buildWhereScalarSubqueryMeta(parsed.whereClause, currentFromSql, runner, runBlockSelect, getColumns);
    const rows = await runBlockSelect(buildCanonicalQuery(canonicalSchema, currentFromSql, parsed.whereClause));
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
    const rows = await runBlockSelect(buildFinalQuery(parsed, { upTo: 'GROUP BY' }));
    const groupedCols = getColumns(rows);
    canonicalSchema = groupedCols.map(col => ({ displayName: col, sqlExpr: `\`${col}\`` }));
    canonicalJoinIndicators = [];
    steps.push(buildGroupByStep({
      before,
      rows,
      groupedCols,
      groupKeys: parsed.groupByClause.replace(/^GROUP\s+BY\s+/i, '').trim(),
      sqlFragment: parsed.groupByClause,
      groupByColumns: detectGroupByColumns(parsed.groupByClause, groupedCols, parsed.selectClause),
      groupBySourceColumns: splitTopLevelSelectItems(parsed.groupByClause.replace(/^GROUP\s+BY\s+/i, '').trim()),
      aggColumns: detectAggColumns(parsed.selectClause, groupedCols),
      aggSummary: buildAggSummary(parsed.selectClause) || undefined,
      preGroupRows: currentRows,
      preGroupColumns: getColumns(currentRows),
    }));
    currentRows = rows;
  }

  if (parsed.havingClause) {
    const before = currentRows.length;
    const preFilterRows = currentRows.slice(0, 200);
    const preFilterColumns = getColumns(currentRows);
    const rows = await runBlockSelect(buildFinalQuery(parsed, { upTo: 'HAVING' }));
    const havingCols = getColumns(rows);
    canonicalSchema = havingCols.map(col => ({ displayName: col, sqlExpr: `\`${col}\`` }));
    steps.push(buildHavingStep({
      before,
      rows,
      columns: havingCols,
      sqlFragment: parsed.havingClause,
      preFilterRows,
      preFilterColumns,
      whereColumns: detectHavingColumns(parsed.havingClause, parsed.selectClause, preFilterColumns, canonicalSchema),
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
    selectedRows = await runBlockSelect(buildCanonicalQuery(canonicalSchema, currentFromSql, tail, usesDistinct));
    if (usesDistinct) {
      const preDistinctRows = await runBlockSelect(buildCanonicalQuery(canonicalSchema, currentFromSql, tail));
      distinctMeta = { columns: getColumns(preDistinctRows), rows: preDistinctRows.slice(0, MAX_DISPLAY_ROWS) };
    }
  } else {
    selectedRows = await runBlockSelect(buildFinalQuery(parsed, { upTo: 'SELECT' }));
    if (usesDistinct) {
      const preDistinctRows = await runBlockSelect(
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
    runBlockSelect,
    getColumns,
  );
  steps.push(buildSelectStep({
    isSelectStar,
    sqlFragment: parsed.selectClause,
    rowsBefore: finalBaseBefore,
    selectedRows,
    columns: selectedColumns,
    schemaContext,
    windowColumns: await detectWindowColumns(parsed.selectClause, selectedColumns, parsed, runBlockSelect),
    caseColumns,
    preSelectRows: currentRows.slice(0, MAX_DISPLAY_ROWS),
    preSelectColumns: getColumns(currentRows),
    distinctMeta,
  }));
  currentRows = selectedRows;

  if (parsed.orderByClause) {
    const before = currentRows.length;
    const rows = useCanonicalTail
      ? await runBlockSelect(buildCanonicalQuery(canonicalSchema, currentFromSql, [parsed.whereClause, parsed.orderByClause].filter(Boolean).join(' ')))
      : await runBlockSelect(buildFinalQuery(parsed, { upTo: 'ORDER BY' }));
    const columns = getColumns(rows);
    steps.push(buildOrderByStep({
      orderKeys: parsed.orderByClause.replace(/^ORDER\s+BY\s+/i, '').trim(),
      sqlFragment: parsed.orderByClause,
      rowsBefore: before,
      rows,
      columns,
      schemaContext,
      sortColumns: detectOrderByColumns(parsed.orderByClause, columns, parsed.selectClause),
    }));
    currentRows = rows;
  }

  if (parsed.limitClause) {
    const before = currentRows.length;
    const rows = useCanonicalTail
      ? await runBlockSelect(buildCanonicalQuery(canonicalSchema, currentFromSql, [parsed.whereClause, parsed.orderByClause, parsed.limitClause].filter(Boolean).join(' ')))
      : await runBlockSelect(buildFinalQuery(parsed, { upTo: 'LIMIT' }));
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

async function runAliasedSelect(
  runBlockSelect: (sql: string) => Promise<Record<string, unknown>[]>,
  fromAndJoinsSql: string,
): Promise<Record<string, unknown>[]> {
  const aliases = extractTableAliases(fromAndJoinsSql);
  const selectList = aliases.length > 0 ? aliases.map(alias => `\`${alias}\`.*`).join(', ') : '*';
  return runBlockSelect(`SELECT ${selectList} ${fromAndJoinsSql}`);
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

function resolveJoinDisplayKey(rows: Record<string, unknown>[], expr: string): string {
  const columns = getColumns(rows);
  const exact = stripTicks(expr);
  if (columns.includes(exact)) {
    return exact;
  }

  return expr.includes('.') ? stripTicks(expr.split('.').pop() || expr) : stripTicks(expr);
}

function buildBlockResolutionContext(blocks: QueryBlock[]): BlockResolutionContext {
  const byReferenceName = new Map<string, QueryBlock>();
  for (const block of blocks) {
    byReferenceName.set(block.name.toLowerCase(), block);
    if (block.materializedName) {
      byReferenceName.set(block.materializedName.toLowerCase(), block);
    }
  }
  return { byReferenceName };
}

function renderBlockSql(
  block: QueryBlock,
  sql: string,
  resolutionContext: BlockResolutionContext,
): string {
  const cteDefinitions = collectCteDefinitions(block, resolutionContext);
  const inlineResolvedSql = inlineSubqueryDependencies(block, sql, resolutionContext);
  return cteDefinitions.length > 0 ? `WITH ${cteDefinitions.join(', ')} ${inlineResolvedSql}` : inlineResolvedSql;
}

function collectCteDefinitions(
  block: QueryBlock,
  resolutionContext: BlockResolutionContext,
  seen = new Set<string>(),
): string[] {
  const definitions: string[] = [];

  for (const dependency of block.dependencies) {
    const dependencyBlock = resolutionContext.byReferenceName.get(dependency.tableName.toLowerCase());
    if (!dependencyBlock) {
      continue;
    }

    definitions.push(...collectCteDefinitions(dependencyBlock, resolutionContext, seen));
    if (dependency.blockType !== 'cte') {
      continue;
    }

    const key = dependencyBlock.name.toLowerCase();
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    definitions.push(`${escapeIdentifier(dependencyBlock.name)} AS (${inlineSubqueryDependencies(dependencyBlock, dependencyBlock.sql, resolutionContext)})`);
  }

  return definitions;
}

function inlineSubqueryDependencies(
  block: QueryBlock,
  sql: string,
  resolutionContext: BlockResolutionContext,
): string {
  let rendered = sql;

  for (const dependency of block.dependencies) {
    if (dependency.blockType !== 'subquery') {
      continue;
    }

    const dependencyBlock = resolutionContext.byReferenceName.get(dependency.tableName.toLowerCase());
    if (!dependencyBlock) {
      throw new Error(`Unsupported read-only query shape: subquery dependency \`${dependency.name}\` could not be resolved.`);
    }

    const replacement = `(${inlineSubqueryDependencies(dependencyBlock, dependencyBlock.sql, resolutionContext)})`;
    rendered = replaceSourceReference(rendered, dependency.tableName, replacement);
  }

  return rendered;
}

function replaceSourceReference(sql: string, sourceName: string, replacement: string): string {
  const escapedSourceName = escapeRegex(stripTicks(sourceName));
  const anyReferencePattern = new RegExp(`(?:\`?${escapedSourceName}\`?)`, 'i');
  const sourcePattern = new RegExp(`\\b(FROM|JOIN)\\s+(?:\`${escapedSourceName}\`|${escapedSourceName})(?=\\s|$)`, 'gi');
  const nextSql = sql.replace(sourcePattern, (_match, clauseKeyword: string) => `${clauseKeyword} ${replacement}`);

  if (nextSql === sql && anyReferencePattern.test(sql)) {
    throw new Error(
      `Unsupported read-only query shape: expected to inline subquery source \`${sourceName}\`, but it was referenced in an unsupported way.`,
    );
  }

  return nextSql;
}

function escapeIdentifier(name: string): string {
  return `\`${stripTicks(name).replace(/`/g, '``')}\``;
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
