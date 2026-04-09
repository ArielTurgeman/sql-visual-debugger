import { sanitizeSql } from '../editor/queryExtractor';

export type QueryBlockType = 'cte' | 'subquery' | 'main';

export type QueryBlockDependency = {
  name: string;
  tableName: string;
  blockType: Exclude<QueryBlockType, 'main'>;
};

export type QueryBlockSource = {
  kind: 'table' | 'cte' | 'subquery';
  name: string;
  alias?: string;
  tableName?: string;
  originalClause?: string;
};

export type QueryBlock = {
  type: QueryBlockType;
  name: string;
  materializedName?: string;
  sql: string;
  rawSql: string;
  sourceStart: number;
  sourceEnd: number;
  dependencies: QueryBlockDependency[];
  fromSource?: QueryBlockSource;
};

type FromSubqueryInfo = {
  innerSql: string;
  innerStart: number;
  innerEnd: number;
  alias: string;
  aliasSql: string;
  baseSourceStart: number;
  baseSourceEnd: number;
};

export function parseQueryBlocks(rawSql: string): QueryBlock[] {
  const queryStart = skipIgnorable(rawSql, 0);
  if (queryStart >= rawSql.length) {
    throw new Error('The SQL query is empty.');
  }

  if (matchesKeyword(rawSql, queryStart, 'WITH')) {
    return parseWithQuery(rawSql, queryStart);
  }

  if (!matchesKeyword(rawSql, queryStart, 'SELECT')) {
    throw new Error('Only SELECT queries and non-recursive CTE queries are supported.');
  }

  return finalizeBlocks([buildMainBlock(rawSql, queryStart)]);
}

function parseWithQuery(rawSql: string, withIndex: number): QueryBlock[] {
  let cursor = skipIgnorable(rawSql, withIndex + 4);
  if (matchesKeyword(rawSql, cursor, 'RECURSIVE')) {
    throw new Error('Recursive CTE is not supported yet. `WITH RECURSIVE` cannot be debugged.');
  }

  const cteBlocks: QueryBlock[] = [];

  while (cursor < rawSql.length) {
    cursor = skipIgnorable(rawSql, cursor);
    const nameResult = readIdentifier(rawSql, cursor);
    if (!nameResult) {
      throw new Error('Could not parse CTE name after WITH.');
    }

    const cteName = stripTicks(nameResult.value);
    cursor = skipIgnorable(rawSql, nameResult.next);

    if (rawSql[cursor] === '(') {
      cursor = skipIgnorable(rawSql, findMatchingParen(rawSql, cursor) + 1);
    }

    if (!matchesKeyword(rawSql, cursor, 'AS')) {
      throw new Error(`CTE \`${cteName}\` must use the form \`${cteName} AS (SELECT ...)\`.`);
    }

    cursor = skipIgnorable(rawSql, cursor + 2);
    if (rawSql[cursor] !== '(') {
      throw new Error(`CTE \`${cteName}\` must wrap its query in parentheses.`);
    }

    const openParen = cursor;
    const closeParen = findMatchingParen(rawSql, openParen);
    const rawInnerSql = rawSql.slice(openParen + 1, closeParen);

    cteBlocks.push({
      type: 'cte',
      name: cteName,
      materializedName: cteName,
      sql: sanitizeSql(rawInnerSql),
      rawSql: rawInnerSql,
      sourceStart: openParen + 1,
      sourceEnd: closeParen,
      dependencies: [],
    });

    cursor = skipIgnorable(rawSql, closeParen + 1);
    if (rawSql[cursor] === ',') {
      cursor += 1;
      continue;
    }
    break;
  }

  const mainStart = skipIgnorable(rawSql, cursor);
  if (!matchesKeyword(rawSql, mainStart, 'SELECT')) {
    throw new Error('The main query after WITH must be a SELECT statement.');
  }

  return finalizeBlocks([...cteBlocks, buildMainBlock(rawSql, mainStart)]);
}

function finalizeBlocks(blocks: QueryBlock[]): QueryBlock[] {
  const expandedBlocks = expandFromSubqueryBlocks(blocks);
  assignDependencies(expandedBlocks);
  return expandedBlocks;
}

function expandFromSubqueryBlocks(blocks: QueryBlock[]): QueryBlock[] {
  const counter = { value: 0 };
  return blocks.flatMap((block) => expandSingleBlockWithFromSubquery(block, counter));
}

function expandSingleBlockWithFromSubquery(block: QueryBlock, counter: { value: number }): QueryBlock[] {
  const fromSubquery = extractSimpleFromSubquery(block.rawSql);
  if (!fromSubquery) {
    return [block];
  }

  counter.value += 1;
  const alias = stripTicks(fromSubquery.alias);
  const materializedName = `__sql_debug_subquery_${counter.value}`;
  const replacement = `\`${materializedName}\`${fromSubquery.aliasSql ? ` ${fromSubquery.aliasSql}` : ''}`;
  const rewrittenRawSql =
    block.rawSql.slice(0, fromSubquery.baseSourceStart) +
    replacement +
    block.rawSql.slice(fromSubquery.baseSourceEnd);

  const subqueryBlock: QueryBlock = {
    type: 'subquery',
    name: alias,
    materializedName,
    sql: sanitizeSql(fromSubquery.innerSql),
    rawSql: fromSubquery.innerSql,
    sourceStart: block.sourceStart + fromSubquery.innerStart,
    sourceEnd: block.sourceStart + fromSubquery.innerEnd,
    dependencies: [],
  };

  const outerBlock: QueryBlock = {
    ...block,
    sql: sanitizeSql(rewrittenRawSql),
    dependencies: [
      ...block.dependencies,
      { name: alias, tableName: materializedName, blockType: 'subquery' },
    ],
    fromSource: {
      kind: 'subquery',
      name: alias,
      alias,
      tableName: materializedName,
      originalClause: `FROM ${block.rawSql.slice(fromSubquery.baseSourceStart, fromSubquery.baseSourceEnd).trim()}`,
    },
  };

  return [...expandSingleBlockWithFromSubquery(subqueryBlock, counter), outerBlock];
}

function extractSimpleFromSubquery(sql: string): FromSubqueryInfo | null {
  const queryStart = skipIgnorable(sql, 0);
  if (!matchesKeyword(sql, queryStart, 'SELECT')) {
    return null;
  }

  const fromIndex = indexOfTopLevelKeyword(sql, 'FROM', queryStart);
  if (fromIndex === -1) {
    return null;
  }

  const sourceStart = skipIgnorable(sql, fromIndex + 4);
  if (sql[sourceStart] !== '(') {
    return null;
  }

  const closeParen = findMatchingParen(sql, sourceStart);
  const innerStart = sourceStart + 1;
  const innerEnd = closeParen;

  let cursor = skipIgnorable(sql, closeParen + 1);
  if (matchesKeyword(sql, cursor, 'AS')) {
    cursor = skipIgnorable(sql, cursor + 2);
  }

  const aliasResult = readIdentifier(sql, cursor);
  if (!aliasResult) {
    throw new Error('FROM subqueries must use the form `FROM (SELECT ...) alias`.');
  }

  return {
    innerSql: sql.slice(innerStart, innerEnd),
    innerStart,
    innerEnd,
    alias: stripTicks(aliasResult.value),
    aliasSql: sql.slice(cursor, aliasResult.next).trim(),
    baseSourceStart: sourceStart,
    baseSourceEnd: aliasResult.next,
  };
}

function buildMainBlock(rawSql: string, start: number): QueryBlock {
  const trimmedEnd = trimStatementEnd(rawSql);
  const rawMainSql = rawSql.slice(start, trimmedEnd);
  return {
    type: 'main',
    name: 'Main Query',
    sql: sanitizeSql(rawMainSql),
    rawSql: rawMainSql,
    sourceStart: start,
    sourceEnd: trimmedEnd,
    dependencies: [],
  };
}

function assignDependencies(blocks: QueryBlock[]): void {
  const materializedBlocks = blocks.filter((block): block is QueryBlock & { materializedName: string } => Boolean(block.materializedName));
  const tableToBlock = new Map<string, { index: number; block: QueryBlock & { materializedName: string } }>();
  materializedBlocks.forEach((block) => {
    tableToBlock.set(block.materializedName.toLowerCase(), { index: blocks.indexOf(block), block });
  });

  for (const [index, block] of blocks.entries()) {
    const refs = extractReferencedNames(block.sql);
    const dependencyMap = new Map(block.dependencies.map(dep => [dep.tableName.toLowerCase(), dep] as const));
    for (const ref of refs) {
      const resolved = tableToBlock.get(ref.toLowerCase());
      if (!resolved) {
        continue;
      }
      dependencyMap.set(ref.toLowerCase(), {
        name: resolved.block.name,
        tableName: resolved.block.materializedName,
        blockType: resolved.block.type as Exclude<QueryBlockType, 'main'>,
      });
    }
    block.dependencies = [...dependencyMap.values()];

    for (const dep of block.dependencies) {
      const resolved = tableToBlock.get(dep.tableName.toLowerCase());
      if (!resolved) {
        continue;
      }
      if (resolved.index === index) {
        throw new Error(`Recursive block dependency is not supported yet. \`${block.name}\` references itself.`);
      }
      if (resolved.index > index) {
        throw new Error(`\`${block.name}\` references \`${dep.name}\` before it is defined.`);
      }
    }
  }
}

function extractReferencedNames(sql: string): string[] {
  const refs: string[] = [];
  const rx = /\b(?:FROM|JOIN)\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)/gi;
  let match: RegExpExecArray | null;
  while ((match = rx.exec(sql)) !== null) {
    refs.push(stripTicks(match[1]));
  }
  return refs;
}

function trimStatementEnd(text: string): number {
  let end = text.length;
  while (end > 0 && /\s/.test(text[end - 1])) {
    end -= 1;
  }
  if (text[end - 1] === ';') {
    end -= 1;
  }
  while (end > 0 && /\s/.test(text[end - 1])) {
    end -= 1;
  }
  return end;
}

function skipIgnorable(text: string, start: number): number {
  let i = start;
  while (i < text.length) {
    if (/\s/.test(text[i])) {
      i += 1;
      continue;
    }
    if (text[i] === '-' && text[i + 1] === '-') {
      i += 2;
      while (i < text.length && text[i] !== '\n') {
        i += 1;
      }
      continue;
    }
    if (text[i] === '/' && text[i + 1] === '*') {
      i += 2;
      while (i < text.length && !(text[i] === '*' && text[i + 1] === '/')) {
        i += 1;
      }
      i = Math.min(text.length, i + 2);
      continue;
    }
    break;
  }
  return i;
}

function matchesKeyword(text: string, index: number, keyword: string): boolean {
  const segment = text.slice(index, index + keyword.length);
  if (segment.localeCompare(keyword, undefined, { sensitivity: 'accent' }) !== 0 &&
      segment.toUpperCase() !== keyword.toUpperCase()) {
    return false;
  }

  const before = index > 0 ? text[index - 1] : ' ';
  const after = index + keyword.length < text.length ? text[index + keyword.length] : ' ';
  return !isIdentifierChar(before) && !isIdentifierChar(after);
}

function indexOfTopLevelKeyword(text: string, keyword: string, start = 0): number {
  let depth = 0;
  let i = start;

  while (i < text.length) {
    const ch = text[i];

    if (ch === '\'' || ch === '"' || ch === '`') {
      i = skipQuoted(text, i, ch);
      continue;
    }

    if (ch === '-' && text[i + 1] === '-') {
      i = skipIgnorable(text, i);
      continue;
    }

    if (ch === '/' && text[i + 1] === '*') {
      i = skipIgnorable(text, i);
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

    if (depth === 0 && matchesKeyword(text, i, keyword)) {
      return i;
    }

    i += 1;
  }

  return -1;
}

function readIdentifier(text: string, index: number): { value: string; next: number } | null {
  if (text[index] === '`') {
    const end = text.indexOf('`', index + 1);
    if (end === -1) {
      throw new Error('Unterminated backtick-quoted identifier.');
    }
    return { value: text.slice(index, end + 1), next: end + 1 };
  }

  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(text.slice(index));
  if (!match) {
    return null;
  }
  return { value: match[0], next: index + match[0].length };
}

function findMatchingParen(text: string, openIndex: number): number {
  let depth = 0;
  let i = openIndex;

  while (i < text.length) {
    const ch = text[i];

    if (ch === '\'' || ch === '"' || ch === '`') {
      i = skipQuoted(text, i, ch);
      continue;
    }

    if (ch === '-' && text[i + 1] === '-') {
      i = skipIgnorable(text, i);
      continue;
    }

    if (ch === '/' && text[i + 1] === '*') {
      i = skipIgnorable(text, i);
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

  throw new Error('Unbalanced parentheses while parsing SQL.');
}

function skipQuoted(text: string, start: number, quote: string): number {
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
  throw new Error('Unterminated quoted string while parsing SQL.');
}

function stripTicks(value: string): string {
  return value.replace(/`/g, '');
}

function isIdentifierChar(char: string): boolean {
  return /[A-Za-z0-9_$]/.test(char);
}
