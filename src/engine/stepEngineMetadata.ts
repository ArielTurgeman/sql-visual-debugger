import type { MysqlRunner } from '../mysql/mysqlRunner';
import type {
  CaseColumnMeta,
  ColumnDef,
  ParsedCaseExpression,
  ParsedQuery,
  WhereInSubqueryMeta,
  WhereScalarSubqueryMeta,
  WindowColumnMeta,
} from './stepEngineTypes';
import { MAX_DISPLAY_ROWS } from './stepEngineTypes';
import {
  bareIdentifier,
  buildFinalQuery,
  extractTableAliases,
  findMatchingParenSql,
  findNextTopLevelKeyword,
  matchesWordAt,
  parseCaseExpression,
  parseWindowExpression,
  quoteSqlString,
  skipQuotedSql,
  skipSqlWhitespace,
  splitTopLevelSelectItems,
} from './stepEngineParsing';
import {
  buildWindowExplanation,
  buildWindowHowComputed,
  buildWindowPreviewColumns,
  buildWindowPreviewRows,
} from './stepEngineExplain';

type RunCustomSelect = (sql: string) => Promise<Record<string, unknown>[]>;
type GetColumns = (rows: Record<string, unknown>[]) => string[];

export function detectWhereColumns(whereClause: string, columns: string[]): string[] {
  const clause = whereClause.replace(/^WHERE\s+/i, '');
  return columns.filter(col => {
    const bare = col.includes('.') ? col.split('.').pop()! : col;
    const escaped = bare.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`\\b${escaped}\\b`, 'i').test(clause);
  });
}

export async function buildWhereInSubqueryMeta(
  whereClause: string,
  currentFromSql: string,
  runner: MysqlRunner,
  runCustomSelect: RunCustomSelect,
  getColumns: GetColumns,
): Promise<WhereInSubqueryMeta | undefined> {
  const parsed = parseWhereInSubquery(whereClause);
  if (!parsed) return undefined;

  const outerAliases = extractTableAliases(currentFromSql);
  if (isCorrelatedSubquery(parsed.subquerySql, outerAliases)) {
    return undefined;
  }

  const rows = await runCustomSelect(parsed.subquerySql);
  return {
    explanation: `Filters rows by checking whether ${parsed.outerColumn ? bareIdentifier(parsed.outerColumn) : 'value'} exists in the values returned by the subquery.`,
    rows: rows.slice(0, MAX_DISPLAY_ROWS),
    columns: getColumns(rows),
    totalRows: rows.length,
  };
}

export async function buildWhereScalarSubqueryMeta(
  whereClause: string,
  currentFromSql: string,
  runner: MysqlRunner,
  runCustomSelect: RunCustomSelect,
  getColumns: GetColumns,
): Promise<WhereScalarSubqueryMeta | undefined> {
  const parsed = parseWhereScalarSubquery(whereClause);
  if (!parsed) return undefined;

  const outerAliases = extractTableAliases(currentFromSql);
  if (isCorrelatedSubquery(parsed.subquerySql, outerAliases)) {
    return undefined;
  }

  const rows = await runCustomSelect(parsed.subquerySql);
  if (rows.length !== 1) return undefined;

  const columns = getColumns(rows);
  if (columns.length !== 1) return undefined;

  return {
    explanation: `Checks whether ${parsed.outerColumn ? bareIdentifier(parsed.outerColumn) : 'value'} is ${describeComparisonOperator(parsed.operator)} the value returned by the subquery.`,
    value: rows[0][columns[0]],
    columnLabel: columns[0],
  };
}

export function detectHavingColumns(
  havingClause: string,
  selectClause: string,
  columns: string[],
): string[] {
  const stripped = havingClause.replace(/^HAVING\s+/i, '');
  const direct = columns.filter(col => {
    const bare = col.includes('.') ? col.split('.').pop()! : col;
    const escaped = bare.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`\\b${escaped}\\b`, 'i').test(stripped);
  });
  if (direct.length > 0) return direct;

  const aggKey = (fn: string, inner: string): string =>
    `${fn.toUpperCase()}(${inner.trim().split('.').pop()!.toUpperCase()})`;

  const aggRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)/gi;
  const havingAggKeys = new Set<string>();
  let match: RegExpExecArray | null;
  while ((match = aggRx.exec(stripped)) !== null) {
    havingAggKeys.add(aggKey(match[1], match[2]));
  }
  if (havingAggKeys.size === 0) return [];

  const selectBody = selectClause.replace(/^SELECT\s+/i, '');
  const aliasRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?/gi;
  const matchedAliases = new Set<string>();
  while ((match = aliasRx.exec(selectBody)) !== null) {
    if (havingAggKeys.has(aggKey(match[1], match[2]))) {
      matchedAliases.add(match[3].toUpperCase());
    }
  }
  if (matchedAliases.size === 0) return [];

  return columns.filter(col => matchedAliases.has((col.includes('.') ? col.split('.').pop()! : col).toUpperCase()));
}

export function detectOrderByColumns(orderByClause: string, columns: string[], selectClause?: string): string[] {
  const body = orderByClause.replace(/^ORDER\s+BY\s+/i, '');
  const terms = splitTopLevelSelectItems(body).map(term => term
    .trim()
    .replace(/\s+(ASC|DESC)\s*$/i, '')
    .trim());
  const matched = new Set<string>();

  const directTerms = new Set(terms.map(term => bareIdentifier(term).toLowerCase()));
  columns.forEach(col => {
    if (directTerms.has(bareIdentifier(col).toLowerCase())) {
      matched.add(col);
    }
  });

  if (!selectClause) {
    return Array.from(matched);
  }

  const selectItems = splitTopLevelSelectItems(selectClause.replace(/^SELECT\s+/i, ''));
  for (const item of selectItems) {
    const aliasMatch = item.match(/^(.*?)(?:\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?)$/i);
    const expr = (aliasMatch ? aliasMatch[1] : item).trim();
    const alias = aliasMatch?.[2];
    if (!alias) continue;

    const outputColumn = columns.find(col => bareIdentifier(col).toLowerCase() === alias.toLowerCase());
    if (!outputColumn) continue;

    const simpleColumnExpr = /^(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))?$/.test(expr);
    if (!simpleColumnExpr) continue;

    const sourceBare = bareIdentifier(expr).toLowerCase();
    if (directTerms.has(sourceBare)) {
      matched.add(outputColumn);
    }
  }

  return Array.from(matched);
}

export function detectGroupByColumns(groupByClause: string, columns: string[]): string[] {
  const body = groupByClause.replace(/^GROUP\s+BY\s+/i, '');
  const terms = body.split(',').map(term => term.trim().replace(/`/g, '').split('.').pop()!.toLowerCase());
  const termSet = new Set(terms);
  return columns.filter(col => termSet.has((col.includes('.') ? col.split('.').pop()! : col).toLowerCase()));
}

export function detectAggColumns(
  selectClause: string,
  columns: string[],
): Array<{ col: string; fn: string; srcCol?: string }> {
  const body = selectClause.replace(/^SELECT\s+/i, '');
  const result: Array<{ col: string; fn: string; srcCol?: string }> = [];
  const aggRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(([^)]*)\)(?:\s+AS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?)?/gi;
  let match: RegExpExecArray | null;
  while ((match = aggRx.exec(body)) !== null) {
    const fn = match[1].toUpperCase();
    const arg = match[2].trim();
    const alias = match[3];
    const srcCol = (arg === '*' || arg === '') ? undefined : arg.replace(/`/g, '').split('.').pop()!;
    if (alias) {
      const col = columns.find(column => {
        const bare = (column.includes('.') ? column.split('.').pop()! : column).toLowerCase();
        return bare === alias.toLowerCase();
      });
      if (col) result.push({ col, fn, srcCol });
    } else {
      const rawExpr = match[0].trim();
      const col = columns.find(column => column.toLowerCase() === rawExpr.toLowerCase());
      if (col) result.push({ col, fn, srcCol });
    }
  }
  return result;
}

export function buildAggSummary(selectClause: string): string {
  const body = selectClause.replace(/^SELECT\s+/i, '');
  const aggRx = /\b(COUNT|SUM|AVG|MIN|MAX)\s*\([^)]*\)/gi;
  const found: string[] = [];
  let match: RegExpExecArray | null;
  while ((match = aggRx.exec(body)) !== null) {
    found.push(match[0].replace(/\s+/g, ' ').trim());
  }
  return found.join(', ');
}

export async function detectWindowColumns(
  selectClause: string,
  resultColumns: string[],
  parsedQuery: ParsedQuery,
  runCustomSelect: RunCustomSelect,
): Promise<WindowColumnMeta[]> {
  const items = splitTopLevelSelectItems(selectClause.replace(/^SELECT\s+/i, ''));
  const windows: WindowColumnMeta[] = [];

  for (const item of items) {
    if (!/\bOVER\s*\(/i.test(item)) continue;
    const parsed = parseWindowExpression(item, resultColumns);
    if (!parsed) {
      throw new Error(`Unsupported window function expression: ${item}`);
    }
    const previewRows = await runCustomSelect(buildWindowPreviewQuery(parsed, parsedQuery));
    windows.push({
      ...parsed,
      explanation: buildWindowExplanation(parsed),
      howComputed: buildWindowHowComputed(parsed),
      previewColumns: buildWindowPreviewColumns(parsed),
      previewRows: buildWindowPreviewRows(parsed, previewRows),
    });
  }

  return windows;
}

export async function detectCaseColumns(
  selectClause: string,
  resultColumns: string[],
  inputRows: Record<string, unknown>[],
  runner: MysqlRunner,
  parsed: ParsedQuery,
  useCanonicalTail: boolean,
  canonicalSchema: ColumnDef[],
  currentFromSql: string,
  usesDistinct: boolean,
  runCustomSelect: RunCustomSelect,
  getColumns: GetColumns,
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
  const helperRows = (await runCustomSelect(helperSql)).slice(0, MAX_DISPLAY_ROWS);

  return parsedCases.map((meta, caseIndex) => ({
    outputColumn: meta.outputColumn,
    expression: meta.expression,
    inputColumns: meta.inputRefs.map(ref => ref.label),
    rowExplanations: helperRows.map(row => ({
      matchedRule: String(row[`__sql_debug_case_${caseIndex}_branch`] ?? 'No matching branch'),
      returnedValue: row[meta.outputColumn],
      inputValues: meta.inputRefs.map((ref, inputIndex) => ({
        column: ref.label,
        value: row[`__sql_debug_case_${caseIndex}_input_${inputIndex}`],
      })),
    })),
  }));
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
      return {
        outerColumn: extractTrailingIdentifier(clause.slice(0, i).trim()) ?? undefined,
        subquerySql: innerSql,
      };
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
      return {
        outerColumn: extractTrailingIdentifier(outerExpr) ?? undefined,
        operator,
        subquerySql: innerSql,
      };
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
    `WHEN ${branch.condition} THEN ${quoteSqlString(branch.label)}`,
  );
  const branchExpr = `CASE ${branchClauses.join(' ')} ELSE ${quoteSqlString(meta.elseLabel)} END AS \`__sql_debug_case_${caseIndex}_branch\``;
  const inputExprs = meta.inputRefs.map((ref, inputIndex) =>
    `${ref.expr} AS \`__sql_debug_case_${caseIndex}_input_${inputIndex}\``,
  );
  return [branchExpr, ...inputExprs];
}

function buildWindowPreviewQuery(
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
  parsedQuery: ParsedQuery,
): string {
  const selectParts = buildWindowPreviewColumns(meta).map(column => {
    if (column === meta.outputColumn) {
      return `${meta.expression}`;
    }
    return column;
  });

  const orderTerms = [
    ...meta.partitionBy,
    ...(meta.orderByTerms ?? []).map(term => `${term.column} ${term.direction}`),
  ];

  const baseSql = buildFinalQuery(
    {
      ...parsedQuery,
      selectClause: `SELECT ${selectParts.join(', ')}`,
      orderByClause: undefined,
      limitClause: undefined,
    },
    { upTo: 'SELECT' },
  );

  return `${baseSql}${orderTerms.length > 0 ? ` ORDER BY ${orderTerms.join(', ')}` : ''}`;
}
