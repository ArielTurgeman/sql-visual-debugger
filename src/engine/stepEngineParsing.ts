import type { ColumnDef, ParsedCaseExpression, ParsedJoin, ParsedQuery } from './stepEngineTypes';

const SQL_KEYWORDS = new Set([
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'OUTER',
  'ON', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'AS',
  'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BY', 'SET',
]);

export function buildCanonicalQuery(
  canonicalSchema: ColumnDef[],
  fromAndJoinsSql: string,
  tailClauses: string = '',
  distinct = false,
): string {
  if (canonicalSchema.length === 0) {
    return `SELECT${distinct ? ' DISTINCT' : ''} * ${fromAndJoinsSql}${tailClauses ? ` ${tailClauses}` : ''}`;
  }
  const selectList = canonicalSchema
    .map(col => (col.sqlAlias ? `${col.sqlExpr} AS ${col.sqlAlias}` : col.sqlExpr))
    .join(', ');
  return `SELECT${distinct ? ' DISTINCT' : ''} ${selectList} ${fromAndJoinsSql}${tailClauses ? ` ${tailClauses}` : ''}`;
}

export function buildFinalQuery(
  parsed: ParsedQuery,
  opts: { upTo: 'GROUP BY' | 'HAVING' | 'SELECT' | 'ORDER BY' | 'LIMIT' },
): string {
  const segments = [
    parsed.selectClause,
    parsed.fromClause,
    ...parsed.joins.map(join => join.rawClause),
    parsed.whereClause,
  ].filter(Boolean);

  if (parsed.groupByClause) segments.push(parsed.groupByClause);
  if (opts.upTo === 'GROUP BY') return segments.join(' ');
  if (parsed.havingClause) segments.push(parsed.havingClause);
  if (opts.upTo === 'HAVING' || opts.upTo === 'SELECT') return segments.join(' ');
  if (parsed.orderByClause) segments.push(parsed.orderByClause);
  if (opts.upTo === 'ORDER BY') return segments.join(' ');
  if (parsed.limitClause) segments.push(parsed.limitClause);
  return segments.join(' ');
}

export function parseSelectQuery(sql: string): ParsedQuery {
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
  const clauseStarts = [wherePos, groupByPos, havingPos, orderByPos, limitPos].filter(v => v !== -1);
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

export function isSelectDistinct(selectClause: string): boolean {
  return /^SELECT\s+DISTINCT\b/i.test(selectClause.trim());
}

export function removeDistinctFromSelectClause(selectClause: string): string {
  return selectClause.replace(/^(\s*SELECT)\s+DISTINCT\b\s*/i, '$1 ');
}

export function bareIdentifier(value: string): string {
  return value.replace(/`/g, '').trim().split('.').pop() ?? value.trim();
}

export function normalizeSqlFragment(value: string): string {
  return value.replace(/`/g, '').replace(/\s+/g, ' ').trim().toLowerCase();
}

export function quoteSqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

export function stripTicks(value: string): string {
  return String(value).replace(/`/g, '');
}

export function normalizeQualified(value: string): string {
  return stripTicks(value).replace(/\s+/g, '');
}

export function extractTableAliases(fromAndJoinsSql: string): string[] {
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

export function extractBaseAlias(fromClause: string): string {
  const match = fromClause.match(/FROM\s+([\w`\.]+)(?:\s+(?:AS\s+)?([\w`]+))?/i);
  return stripTicks(match?.[2] || match?.[1]?.split('.').pop() || 'left');
}

export function extractBaseTableName(fromClause: string): string {
  const match = fromClause.match(/FROM\s+([\w`\.]+)/i);
  return stripTicks(match?.[1] || 'table');
}

export function skipSqlWhitespace(text: string, index: number): number {
  let cursor = index;
  while (cursor < text.length && /\s/.test(text[cursor])) {
    cursor += 1;
  }
  return cursor;
}

export function matchesWordAt(text: string, index: number, word: string): boolean {
  const segment = text.slice(index, index + word.length);
  if (segment.toUpperCase() !== word.toUpperCase()) {
    return false;
  }
  const before = index > 0 ? text[index - 1] : ' ';
  const after = index + word.length < text.length ? text[index + word.length] : ' ';
  return !/[A-Za-z0-9_$]/.test(before) && !/[A-Za-z0-9_$]/.test(after);
}

export function findNextTopLevelKeyword(text: string, start: number, keywords: string[]): number {
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

export function indexOfTopLevelKeyword(text: string, keyword: string): number {
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

export function findMatchingParenSql(text: string, openIndex: number): number {
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

export function skipQuotedSql(text: string, start: number, quote: string): number {
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

export function splitTopLevelSelectItems(selectBody: string): string[] {
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

export function parseWindowExpression(
  item: string,
  resultColumns: string[],
): {
  outputColumn: string;
  expression: string;
  functionName: string;
  sourceColumn?: string;
  partitionBy: string[];
  orderBy: string[];
  orderByTerms: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
} | null {
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
  return {
    outputColumn: resolveOutputColumn(alias, expr, resultColumns),
    expression: item.trim(),
    functionName,
    sourceColumn: sourceArg && sourceArg !== '*' ? bareIdentifier(sourceArg) : undefined,
    partitionBy,
    orderBy,
    orderByTerms,
  };
}

export function parseWindowOverClause(overClause: string): {
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

export function parseCaseExpression(
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
      if (thenIndex === -1) return null;
      const condition = innerBody.slice(condStart, thenIndex).trim();
      const resultStart = skipSqlWhitespace(innerBody, thenIndex + 4);
      const nextIndex = findNextTopLevelKeyword(innerBody, resultStart, ['WHEN', 'ELSE']);
      const resultExpr = innerBody.slice(resultStart, nextIndex === -1 ? innerBody.length : nextIndex).trim();
      branches.push({ condition, label: `WHEN ${condition} THEN ${resultExpr}` });
      cursor = nextIndex === -1 ? innerBody.length : nextIndex;
      continue;
    }
    if (matchesWordAt(innerBody, cursor, 'ELSE')) {
      const elseStart = skipSqlWhitespace(innerBody, cursor + 4);
      elseLabel = `ELSE ${innerBody.slice(elseStart).trim()}`;
      break;
    }
    cursor += 1;
  }

  if (branches.length === 0) {
    return null;
  }

  return {
    outputColumn: resolveOutputColumn(alias, expr, resultColumns),
    expression: item.trim(),
    inputRefs: extractCaseInputRefs(branches.map(branch => branch.condition), availableInputColumns),
    branches,
    elseLabel,
  };
}

function parseFromAndJoins(fromAndJoins: string): { baseClause: string; joins: ParsedJoin[] } {
  const joinRegex = /\b((?:(?:INNER|LEFT|RIGHT|FULL)(?:\s+OUTER)?|CROSS)?\s*JOIN)\b/ig;
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
    const headerMatch = joinHeader.match(/^((?:(?:INNER|LEFT|RIGHT|FULL)(?:\s+OUTER)?|CROSS)?\s*JOIN)\s+([\w`\.]+)(?:\s+(?:AS\s+)?([\w`]+))?$/i);
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
  const tail = nextKeywords.map(nextKeyword => indexOfKeyword(source, nextKeyword)).filter(v => v !== -1 && v > start);
  const end = tail.length > 0 ? Math.min(...tail) : source.length;
  return source.slice(start, end).trim();
}

function indexOfKeyword(source: string, keyword: string): number {
  return indexOfTopLevelKeyword(source, keyword.trim());
}

function resolveAlias(rawAlias: string | undefined, tableName: string): string {
  if (rawAlias && !SQL_KEYWORDS.has(rawAlias.toUpperCase())) {
    return stripTicks(rawAlias);
  }
  return stripTicks(tableName.split('.').pop() || tableName);
}

function resolveOutputColumn(alias: string | undefined, expr: string, resultColumns: string[]): string {
  if (alias) {
    const matched = resultColumns.find(col => bareIdentifier(col).toLowerCase() === alias.toLowerCase());
    if (matched) return matched;
  }

  const normalizedExpr = normalizeSqlFragment(expr);
  return resultColumns.find(col => normalizeSqlFragment(col) === normalizedExpr) ?? alias ?? expr;
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
