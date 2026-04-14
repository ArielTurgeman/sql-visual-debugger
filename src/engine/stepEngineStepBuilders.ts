import type { QueryBlock } from './queryBlocks';
import type { DebugStep, DistinctMeta, JoinMeta } from './stepEngineTypes';
import { MAX_DISPLAY_ROWS } from './stepEngineTypes';

export function buildFromStep(input: {
  fromDependencyLabel: string | null;
  fromTableName: string;
  currentRows: Record<string, unknown>[];
  sqlFragment: string;
  columns: string[];
}): DebugStep {
  return {
    name: 'FROM',
    title: 'FROM',
    explanation: input.fromDependencyLabel
      ? `Loaded ${input.currentRows.length.toLocaleString()} rows from ${input.fromDependencyLabel}. This block starts from rows produced earlier in the query.`
      : `Loaded ${input.currentRows.length.toLocaleString()} rows from \`${input.fromTableName}\`. This is the starting dataset before any joins or filters.`,
    impact: input.currentRows.length === 0
      ? 'The base table is empty ג€” subsequent steps will have no rows to work with.'
      : `${input.currentRows.length.toLocaleString()} rows are available for subsequent steps.`,
    sqlFragment: input.sqlFragment,
    rowsBefore: 0,
    rowsAfter: input.currentRows.length,
    data: input.currentRows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.columns,
  };
}

export function applyFromDependencyDetails(
  step: DebugStep,
  dependency: QueryBlock['dependencies'][number],
  rowCount: number,
): DebugStep {
  return {
    ...step,
    impact: `${rowCount.toLocaleString()} rows were loaded from ${dependency.blockType === 'cte' ? 'CTE' : 'subquery'} \`${dependency.name}\` and are now available for subsequent steps in this block.`,
    sourceRows: rowCount,
    sourceLabel: `Loaded from ${dependency.blockType === 'cte' ? 'CTE' : 'subquery'} ${dependency.name}`,
  };
}

export function buildJoinStep(input: {
  joinType: string;
  rightDisplay: string;
  leftExpr: string;
  rightExpr: string;
  sqlFragment: string;
  leftBefore: number;
  joinedRows: Record<string, unknown>[];
  joinedColumns: string[];
  relationship: JoinMeta['relationship'];
  joinMeta: JoinMeta;
}): DebugStep {
  const joinDelta = input.joinedRows.length - input.leftBefore;
  const joinImpactBase = joinDelta === 0
    ? `Row count stayed at ${input.joinedRows.length.toLocaleString()}.`
    : joinDelta > 0
      ? `Row count grew from ${input.leftBefore.toLocaleString()} to ${input.joinedRows.length.toLocaleString()} (+${joinDelta.toLocaleString()}).`
      : `Row count fell from ${input.leftBefore.toLocaleString()} to ${input.joinedRows.length.toLocaleString()} (${joinDelta.toLocaleString()}).`;
  const joinImpactReason =
    input.relationship === 'one-to-one' ? 'Each row on both sides matched at most one row.' :
    input.relationship === 'one-to-many' ? 'Some left-side keys matched multiple right-side rows.' :
    input.relationship === 'many-to-one' ? 'Multiple left-side rows shared the same right-side key.' :
    'Multiple rows matched on both sides (many-to-many).';

  return {
    name: 'JOIN',
    title: input.joinType,
    explanation: `Joined ${input.rightDisplay} to the current result on ${input.leftExpr} = ${input.rightExpr}.`,
    impact: `${joinImpactBase} Relationship: ${input.relationship} ג€” ${joinImpactReason}`,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.leftBefore,
    rowsAfter: input.joinedRows.length,
    data: input.joinedRows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.joinedColumns,
    joinMeta: input.joinMeta,
  };
}

export function buildWhereStep(input: {
  explanation: string;
  impact: string;
  sqlFragment: string;
  rowsBefore: number;
  rows: Record<string, unknown>[];
  columns: string[];
  schemaContext?: { joinIndicatorColumns: string[] };
  preFilterRows: Record<string, unknown>[];
  preFilterColumns: string[];
  whereColumns: string[];
  whereInSubquery?: DebugStep['whereInSubquery'];
  whereScalarSubquery?: DebugStep['whereScalarSubquery'];
}): DebugStep {
  return {
    name: 'WHERE',
    title: 'WHERE',
    explanation: input.explanation,
    impact: input.impact,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.rowsBefore,
    rowsAfter: input.rows.length,
    data: input.rows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.columns,
    schemaContext: input.schemaContext,
    preFilterRows: input.preFilterRows,
    preFilterColumns: input.preFilterColumns,
    whereColumns: input.whereColumns,
    whereInSubquery: input.whereInSubquery,
    whereScalarSubquery: input.whereScalarSubquery,
  };
}

export function buildGroupByStep(input: {
  before: number;
  rows: Record<string, unknown>[];
  groupedCols: string[];
  groupKeys: string;
  sqlFragment: string;
  groupByColumns: string[];
  aggColumns: Array<{ col: string; fn: string; srcCol?: string }>;
  aggSummary?: string;
  preGroupRows: Record<string, unknown>[];
  preGroupColumns: string[];
}): DebugStep {
  return {
    name: 'GROUP BY',
    title: 'GROUP BY',
    explanation: `Collapsed ${input.before.toLocaleString()} rows into ${input.rows.length.toLocaleString()} groups by ${input.groupKeys || 'the specified keys'}. Aggregate functions were applied within each group.`,
    impact: `Row count changed from ${input.before.toLocaleString()} to ${input.rows.length.toLocaleString()}. Each unique combination of group keys produced one output row.`,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.before,
    rowsAfter: input.rows.length,
    data: input.rows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.groupedCols,
    groupByColumns: input.groupByColumns,
    aggColumns: input.aggColumns,
    aggSummary: input.aggSummary,
    preGroupRows: input.preGroupRows,
    preGroupColumns: input.preGroupColumns,
  };
}

export function buildHavingStep(input: {
  before: number;
  rows: Record<string, unknown>[];
  columns: string[];
  sqlFragment: string;
  preFilterRows: Record<string, unknown>[];
  preFilterColumns: string[];
  whereColumns: string[];
}): DebugStep {
  const removed = input.before - input.rows.length;
  return {
    name: 'HAVING',
    title: 'HAVING',
    explanation: 'Filtered aggregated groups using the HAVING condition. Unlike WHERE (which filters individual rows before grouping), HAVING filters after aggregation.',
    impact: removed === 0
      ? `No groups removed. All ${input.rows.length.toLocaleString()} groups satisfied the condition.`
      : `${removed.toLocaleString()} group${removed === 1 ? '' : 's'} removed. ${input.rows.length.toLocaleString()} of ${input.before.toLocaleString()} groups passed the filter.`,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.before,
    rowsAfter: input.rows.length,
    data: input.rows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.columns,
    preFilterRows: input.preFilterRows,
    preFilterColumns: input.preFilterColumns,
    whereColumns: input.whereColumns,
  };
}

export function buildSelectStep(input: {
  isSelectStar: boolean;
  sqlFragment: string;
  rowsBefore: number;
  selectedRows: Record<string, unknown>[];
  columns: string[];
  schemaContext?: { joinIndicatorColumns: string[] };
  windowColumns: DebugStep['windowColumns'];
  caseColumns: DebugStep['caseColumns'];
  preSelectRows: Record<string, unknown>[];
  preSelectColumns: string[];
  distinctMeta?: DistinctMeta;
}): DebugStep {
  return {
    name: 'SELECT',
    title: 'SELECT',
    explanation: input.isSelectStar
      ? 'SELECT * was used ג€” all available columns were kept as-is. No column projection occurred.'
      : 'Projected only the columns named in the SELECT clause. All other columns were excluded from the output.',
    impact: `Row count did not change (${input.selectedRows.length.toLocaleString()}). SELECT determines which columns appear in the output, not which rows are returned.`,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.rowsBefore,
    rowsAfter: input.selectedRows.length,
    data: input.selectedRows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.columns,
    schemaContext: input.schemaContext,
    windowColumns: input.windowColumns,
    caseColumns: input.caseColumns,
    preSelectRows: input.preSelectRows,
    preSelectColumns: input.preSelectColumns,
    distinctMeta: input.distinctMeta,
  };
}

export function buildOrderByStep(input: {
  orderKeys: string;
  sqlFragment: string;
  rowsBefore: number;
  rows: Record<string, unknown>[];
  columns: string[];
  schemaContext?: { joinIndicatorColumns: string[] };
  sortColumns: string[];
}): DebugStep {
  return {
    name: 'ORDER BY',
    title: 'ORDER BY',
    explanation: `Reordered the result set by ${input.orderKeys || 'the specified expression'}.`,
    impact: `Row count is unchanged at ${input.rows.length.toLocaleString()}. ORDER BY only reorders rows ג€” nothing is added or removed.`,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.rowsBefore,
    rowsAfter: input.rows.length,
    data: input.rows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.columns,
    schemaContext: input.schemaContext,
    sortColumns: input.sortColumns,
  };
}

export function buildLimitStep(input: {
  limitN: string;
  sqlFragment: string;
  rowsBefore: number;
  rows: Record<string, unknown>[];
  columns: string[];
  schemaContext?: { joinIndicatorColumns: string[] };
}): DebugStep {
  const excluded = input.rowsBefore - input.rows.length;
  return {
    name: 'LIMIT',
    title: 'LIMIT',
    explanation: `Kept only the first ${input.limitN} row${input.limitN === '1' ? '' : 's'} from the result. All rows beyond the limit were excluded.`,
    impact: excluded > 0
      ? `${excluded.toLocaleString()} row${excluded === 1 ? '' : 's'} beyond the limit ${excluded === 1 ? 'was' : 'were'} cut. Final output: ${input.rows.length.toLocaleString()} row${input.rows.length === 1 ? '' : 's'}.`
      : `All ${input.rows.length.toLocaleString()} rows fit within the limit ג€” nothing was excluded.`,
    sqlFragment: input.sqlFragment,
    rowsBefore: input.rowsBefore,
    rowsAfter: input.rows.length,
    data: input.rows.slice(0, MAX_DISPLAY_ROWS),
    columns: input.columns,
    schemaContext: input.schemaContext,
  };
}

export function attachBlockContext(step: DebugStep, block: QueryBlock, blockIndex: number): DebugStep {
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
