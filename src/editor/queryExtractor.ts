import * as vscode from 'vscode';
import type { ExtractResult } from '../types';
import { parseQueryBlocks } from '../engine/queryBlocks';
import { extractTableAliases, parseSelectQuery, splitTopLevelSelectItems } from '../engine/stepEngineParsing';

type StatementSlice = {
    start: number;
    end: number;
    contentStart: number;
    contentEnd: number;
    rawText: string;
    trimmedText: string;
};

/**
 * Extract a SQL query from the active editor.
 *
 * Priority:
 *  1. If the user has a non-empty selection, use that text exactly.
 *  2. Otherwise, use the SQL statement containing the active cursor.
 *  3. Otherwise, show a helpful error.
 *
 * Returns the raw SQL plus the exact document position it came from so
 * downstream code can keep editor highlights aligned with the chosen statement.
 */
export function extractQuery(editor: vscode.TextEditor): ExtractResult {
    const hasSelection = !editor.selection.isEmpty;
    const fileName = editor.document.fileName.split(/[\\/]/).pop() ?? 'unknown';

    const extraction = hasSelection
        ? {
            raw: editor.document.getText(editor.selection),
            selectionStart: editor.selection.start,
            source: `${fileName} (selection)`,
        }
        : getQueryUnderCursor(editor, fileName);

    if ('error' in extraction) {
        return extraction;
    }

    const { raw, selectionStart, source } = extraction;
    const sql = sanitizeSql(raw);

    if (!sql) {
        return {
            error: hasSelection
                ? 'The selected text is empty. Select a SQL query and try again.'
                : 'The file is empty. Write a SQL query and try again.'
        };
    }

    if (/^\s*WITH\s+RECURSIVE\b/i.test(sql)) {
        return {
            error: 'SQL Debugger does not support recursive CTE yet.\n`WITH RECURSIVE` queries cannot be debugged.'
        };
    }

    if (!/^\s*(SELECT|WITH)\b/i.test(sql)) {
        return {
            error:
                'SQL Debugger only supports SELECT queries and non-recursive CTE queries.\n' +
                'INSERT, UPDATE, DELETE, DROP, and other statement types are not supported.'
        };
    }

    const unsafeReason = findUnsafeReadOnlyShape(sql);
    if (unsafeReason) {
        return { error: unsafeReason };
    }

    const unsupportedReason = findUnsupportedReadOnlyShape(sql);
    if (unsupportedReason) {
        return { error: unsupportedReason };
    }

    const malformedReason = findMalformedShapeReason(sql);
    if (malformedReason) {
        return { error: malformedReason };
    }

    return { sql, source, rawText: raw, selectionStart };
}

function getQueryUnderCursor(
    editor: vscode.TextEditor,
    fileName: string,
): { raw: string; selectionStart: vscode.Position; source: string } | { error: string } {
    const fullText = editor.document.getText();
    const cursorOffset = editor.document.offsetAt(editor.selection.active);
    if (hasConsecutiveSemicolonTerminators(fullText)) {
        return {
            error:
                'Only a single SQL statement is supported per debug run.\n' +
                'Remove the extra semicolon or select just one query.'
        };
    }

    const statements = splitStatements(fullText);
    const nonEmptyStatements = statements.filter(statement => statement.trimmedText.length > 0);

    if (nonEmptyStatements.length === 0) {
        return { error: 'The file is empty. Write a SQL query and try again.' };
    }

    if (nonEmptyStatements.length === 1) {
        const statement = nonEmptyStatements[0];
        return {
            raw: statement.rawText,
            selectionStart: editor.document.positionAt(statement.start),
            source: fileName,
        };
    }

    const activeStatement = nonEmptyStatements.find(statement =>
        cursorOffset >= statement.contentStart && cursorOffset <= statement.contentEnd
    );

    if (!activeStatement) {
        return {
            error:
                'Could not determine which query to debug.\n' +
                'Place the cursor inside one query or select the query you want.'
        };
    }

    return {
        raw: activeStatement.rawText,
        selectionStart: editor.document.positionAt(activeStatement.start),
        source: fileName,
    };
}

function splitStatements(text: string): StatementSlice[] {
    const boundaries: Array<{ start: number; end: number }> = [];
    let statementStart = 0;
    let i = 0;
    let quote: '\'' | '"' | '`' | null = null;
    let lineComment = false;
    let blockComment = false;
    let parenDepth = 0;
    let currentStatementRoot: 'SELECT' | 'WITH' | null = null;
    let lineHasCode = false;

    while (i < text.length) {
        const char = text[i];
        const next = text[i + 1];

        if (lineComment) {
            if (char === '\n') {
                lineComment = false;
                lineHasCode = false;
            }
            i += 1;
            continue;
        }

        if (blockComment) {
            if (char === '*' && next === '/') {
                blockComment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if (quote) {
            if (char === quote) {
                if ((quote === '\'' || quote === '"') && next === quote) {
                    i += 2;
                    continue;
                }

                let backslashCount = 0;
                for (let j = i - 1; j >= 0 && text[j] === '\\'; j -= 1) {
                    backslashCount += 1;
                }

                if (backslashCount % 2 === 0) {
                    quote = null;
                }
            }

            i += 1;
            continue;
        }

        if (char === '-' && next === '-') {
            lineComment = true;
            i += 2;
            continue;
        }

        if (char === '/' && next === '*') {
            blockComment = true;
            i += 2;
            continue;
        }

        if (char === '\'' || char === '"' || char === '`') {
            quote = char;
            lineHasCode = true;
            i += 1;
            continue;
        }

        if (char === '\r') {
            i += 1;
            continue;
        }

        if (char === '\n') {
            lineHasCode = false;
            i += 1;
            continue;
        }

        if (char === '(') {
            parenDepth += 1;
            lineHasCode = true;
            i += 1;
            continue;
        }

        if (char === ')') {
            parenDepth = Math.max(0, parenDepth - 1);
            lineHasCode = true;
            i += 1;
            continue;
        }

        if (char === ';') {
            boundaries.push({ start: statementStart, end: i });
            statementStart = i + 1;
            currentStatementRoot = null;
            lineHasCode = false;
            i += 1;
            continue;
        }

        if (!lineHasCode && parenDepth === 0) {
            const rootMatch = text.slice(i).match(/^(SELECT|WITH)\b/i);
            if (rootMatch) {
                const rootKeyword = rootMatch[1].toUpperCase() as 'SELECT' | 'WITH';
                const startsMainSelectForWith =
                    currentStatementRoot === 'WITH' && rootKeyword === 'SELECT';

                if (currentStatementRoot && !startsMainSelectForWith) {
                    boundaries.push({ start: statementStart, end: i });
                    statementStart = i;
                }
                if (!currentStatementRoot) {
                    currentStatementRoot = rootKeyword;
                }
            }
        }

        if (!/\s/.test(char)) {
            lineHasCode = true;
        }

        i += 1;
    }

    boundaries.push({ start: statementStart, end: text.length });

    return boundaries.map(({ start, end }) => {
        const rawText = text.slice(start, end);
        const leadingWhitespace = rawText.match(/^\s*/)?.[0].length ?? 0;
        const trailingWhitespace = rawText.match(/\s*$/)?.[0].length ?? 0;
        const trimmedText = rawText.trim();
        const contentStart = start + leadingWhitespace;
        const contentEnd = trimmedText.length === 0
            ? contentStart
            : end - trailingWhitespace;

        return {
            start,
            end,
            contentStart,
            contentEnd,
            rawText,
            trimmedText,
        };
    });
}

function hasConsecutiveSemicolonTerminators(text: string): boolean {
    const withoutComments = text
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/--[^\n]*/g, '');
    const masked = maskQuotedContent(withoutComments);
    return /;\s*;/.test(masked);
}

/**
 * Minimal sanitisation before the query is handed to MySQL or the parser.
 *  - Strips a trailing semicolon (MySQL accepts it, but it confuses clause detection)
 *  - Collapses internal runs of whitespace to a single space
 *  - Rejects multiple statements (semicolon in the middle)
 *
 * Throws on multi-statement input so the caller gets a clear error.
 */
export function sanitizeSql(raw: string): string {
    // Strip comments before any other processing so they don't interfere with
    // the SELECT check or the semicolon guard.
    //   - Block comments  /* ... */  may span multiple lines.
    //   - Line comments   -- ...     run to the end of the line.
    const withoutComments = raw
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/--[^\n]*/g, '');

    // Strip trailing semicolon first, then check for embedded ones.
    const withoutTrailing = withoutComments.trim().replace(/;\s*$/, '').trim();

    if (withoutTrailing.includes(';')) {
        throw new Error(
            'Only a single SQL statement is supported per debug run.\n' +
            'Remove the extra semicolon or select just one query.'
        );
    }

    return withoutTrailing.replace(/\s+/g, ' ').trim();
}

function findUnsafeReadOnlyShape(sql: string): string | null {
    const masked = maskQuotedContent(sql);

    if (/\bINTO\b/i.test(masked)) {
        return (
            'SQL Debugger only runs strictly read-only SELECT/WITH queries.\n' +
            '`SELECT ... INTO ...` is not supported, including `INTO OUTFILE`, `INTO DUMPFILE`, and variable assignment forms.'
        );
    }

    if (/\bFOR\s+UPDATE\b/i.test(masked)) {
        return (
            'SQL Debugger only runs strictly read-only SELECT/WITH queries.\n' +
            '`FOR UPDATE` is not supported because it requests row locks.'
        );
    }

    if (/\bLOCK\s+IN\s+SHARE\s+MODE\b/i.test(masked)) {
        return (
            'SQL Debugger only runs strictly read-only SELECT/WITH queries.\n' +
            '`LOCK IN SHARE MODE` is not supported because it requests row locks.'
        );
    }

    if (/\bINTERSECT\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `INTERSECT` yet.\n' +
            '`INTERSECT` is currently outside the supported query shapes.'
        );
    }

    if (/\bEXCEPT\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `EXCEPT` yet.\n' +
            '`EXCEPT` is currently outside the supported query shapes.'
        );
    }

    if (/\bNATURAL\s+JOIN\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `NATURAL JOIN` yet.\n' +
            '`NATURAL JOIN` is currently outside the supported query shapes.'
        );
    }

    if (/\bFULL\s+OUTER\s+JOIN\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `FULL OUTER JOIN` yet.\n' +
            '`FULL OUTER JOIN` is currently outside the supported query shapes.'
        );
    }

    if (/\bOVER\s+[A-Za-z_][A-Za-z0-9_]*\b/i.test(masked)) {
        return (
            'SQL Debugger does not support named windows yet.\n' +
            'Only inline `OVER (...)` window definitions are currently supported.'
        );
    }

    if (/\bOVER\s*\([^)]*\b(?:ROWS|RANGE)\b/i.test(masked)) {
        return (
            'SQL Debugger does not support window frame clauses yet.\n' +
            '`ROWS ...` and `RANGE ...` window frames are currently outside the supported query shapes.'
        );
    }

    const unsupportedJoinReason = findUnsupportedJoinReason(sql);
    if (unsupportedJoinReason) {
        return unsupportedJoinReason;
    }

    return null;
}

function findUnsupportedReadOnlyShape(sql: string): string | null {
    const masked = maskQuotedContent(sql);

    if (/\bUNION(?:\s+ALL)?\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `UNION` or `UNION ALL` yet.\n' +
            'Set-operation queries are currently outside the supported query shapes.'
        );
    }

    if (/\bJOIN\s+[\w`\.]+\s+USING\s*\(/i.test(masked)) {
        return (
            'SQL Debugger does not support `JOIN ... USING (...)` yet.\n' +
            'Use an explicit `JOIN ... ON left_col = right_col` query shape instead.'
        );
    }

    if (/\bCROSS\s+JOIN\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `CROSS JOIN` yet.\n' +
            '`CROSS JOIN` is currently outside the supported query shapes.'
        );
    }

    if (/\b(?:NOT\s+)?EXISTS\s*\(/i.test(masked)) {
        return (
            'SQL Debugger does not support `EXISTS` / `NOT EXISTS` subqueries yet.\n' +
            'These subquery predicates are currently outside the supported query shapes.'
        );
    }

    if (/^\s*SELECT\s+DATABASE\s*\(\s*\)\s*$/i.test(masked)) {
        return (
            'SQL Debugger does not support `SELECT DATABASE()` yet.\n' +
            'Function-only queries are currently outside the supported query shapes.'
        );
    }

    if (/\bWITH\s+ROLLUP\b/i.test(masked)) {
        return (
            'SQL Debugger does not support `WITH ROLLUP` yet.\n' +
            '`WITH ROLLUP` is currently outside the supported query shapes.'
        );
    }

    if (/\bJOIN\s*\(\s*SELECT\b/i.test(masked)) {
        return (
            'SQL Debugger does not support joins against derived tables yet.\n' +
            'Only direct table joins are currently supported.'
        );
    }

    const projectedScalarReason = findUnsupportedProjectedScalarSubqueryReason(sql);
    if (projectedScalarReason) {
        return projectedScalarReason;
    }

    const unsupportedWindowReason = findUnsupportedWindowFunctionReason(masked);
    if (unsupportedWindowReason) {
        return unsupportedWindowReason;
    }

    return null;
}

function findUnsupportedWindowFunctionReason(sql: string): string | null {
    const allowed = new Set([
        'ROW_NUMBER',
        'RANK',
        'DENSE_RANK',
        'AVG',
        'SUM',
        'COUNT',
        'MAX',
        'MIN',
    ]);
    const windowFnPattern = /\b([A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)\s*OVER\s*\(/gi;
    let match: RegExpExecArray | null;

    while ((match = windowFnPattern.exec(sql)) !== null) {
        const functionName = match[1].toUpperCase();
        if (!allowed.has(functionName)) {
            return (
                `SQL Debugger does not support \`${functionName}\` window functions yet.\n` +
                'Only ROW_NUMBER, RANK, DENSE_RANK, AVG, SUM, COUNT, MAX, and MIN window functions are currently supported.'
            );
        }
    }

    return null;
}

function findUnsupportedJoinReason(sql: string): string | null {
    const joinOnPattern =
        /\b(?:INNER|LEFT|RIGHT|FULL(?:\s+OUTER)?|CROSS)?\s*JOIN\b[\s\S]+?\bON\b\s+([\s\S]+?)(?=\b(?:INNER|LEFT|RIGHT|FULL(?:\s+OUTER)?|CROSS|NATURAL)\s+JOIN\b|\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b|$)/gi;
    let match: RegExpExecArray | null;

    while ((match = joinOnPattern.exec(sql)) !== null) {
        const onClause = match[1].trim();
        if (!isSimpleEqualityJoin(onClause)) {
            return (
                'SQL Debugger supports only simple equality JOIN conditions right now.\n' +
                'Complex JOIN predicates, multi-condition JOINs, and function-based JOINs are currently rejected.'
            );
        }
    }

    return null;
}

function findMalformedShapeReason(sql: string): string | null {
    if (/^\s*WITH\b/i.test(sql)) {
        return null;
    }

    try {
        const parsed = parseSelectQuery(sql);
        const selectBody = parsed.selectClause.replace(/^SELECT\b/i, '').trim();
        if (!selectBody) {
            return (
                'SQL Debugger could not understand this query shape.\n' +
                'The SELECT list is empty.'
            );
        }
        if (/,\s*$/.test(selectBody)) {
            return (
                'SQL Debugger could not understand this query shape.\n' +
                'The SELECT list ends with a trailing comma.'
            );
        }
        return null;
    } catch (error) {
        const message = error instanceof Error ? error.message : 'Unsupported or malformed SQL.';
        return (
            'SQL Debugger could not understand this query shape.\n' +
            `${message}`
        );
    }
}

function findUnsupportedProjectedScalarSubqueryReason(sql: string): string | null {
    try {
        const blocks = parseQueryBlocks(sql);
        for (const block of blocks) {
            const parsed = parseSelectQuery(block.sql);
            const currentFromSql = [parsed.fromClause, ...parsed.joins.map(join => join.rawClause)].join(' ');
            const outerAliases = extractTableAliases(currentFromSql);
            const selectItems = splitTopLevelSelectItems(parsed.selectClause.replace(/^SELECT\s+/i, ''));

            for (const item of selectItems) {
                const subqueries = extractProjectedScalarSubqueries(item);
                for (const subquerySql of subqueries) {
                    const unsupportedReason = getUnsupportedProjectedScalarSubqueryReason(subquerySql, outerAliases);
                    if (unsupportedReason) {
                        return unsupportedReason;
                    }
                }
            }
        }
        return null;
    } catch {
        // Let the normal malformed-shape path surface parser failures.
        return null;
    }
}

function extractProjectedScalarSubqueries(item: string): string[] {
    const subqueries: string[] = [];
    let i = 0;

    while (i < item.length) {
        const char = item[i];
        if (char === '\'' || char === '"' || char === '`') {
            i = skipQuotedSqlLike(item, i, char);
            continue;
        }
        if (char !== '(') {
            i += 1;
            continue;
        }

        const innerStart = skipWhitespace(item, i + 1);
        if (!/^SELECT\b/i.test(item.slice(innerStart))) {
            i += 1;
            continue;
        }

        const closeIndex = findMatchingParenInText(item, i);
        const innerSql = item.slice(innerStart, closeIndex).trim();
        subqueries.push(innerSql);
        i = closeIndex + 1;
    }

    return subqueries;
}

function getUnsupportedProjectedScalarSubqueryReason(subquerySql: string, outerAliases: string[]): string | null {
    if (isCorrelatedProjectedSubquery(subquerySql, outerAliases)) {
        return (
            'SQL Debugger supports only simple uncorrelated scalar subqueries in the SELECT list right now.\n' +
            'Correlated projected subqueries are still outside the supported query shapes.'
        );
    }

    let parsed;
    try {
        parsed = parseSelectQuery(subquerySql);
    } catch {
        return (
            'SQL Debugger supports only simple scalar subqueries in the SELECT list right now.\n' +
            'This projected subquery shape is currently outside the supported query shapes.'
        );
    }

    if (parsed.joins.length > 0 || parsed.groupByClause || parsed.havingClause || parsed.orderByClause || parsed.limitClause) {
        return (
            'SQL Debugger supports only simple scalar subqueries in the SELECT list right now.\n' +
            'Projected subqueries with JOIN, GROUP BY, HAVING, ORDER BY, or LIMIT are currently outside the supported query shapes.'
        );
    }

    const selectItems = splitTopLevelSelectItems(parsed.selectClause.replace(/^SELECT\s+/i, ''));
    if (selectItems.length !== 1) {
        return (
            'SQL Debugger supports only single-value scalar subqueries in the SELECT list right now.\n' +
            'Projected subqueries must return exactly one selected value.'
        );
    }

    const selectExpr = selectItems[0]
        .replace(/\s+AS\s+`?[A-Za-z_][A-Za-z0-9_]*`?\s*$/i, '')
        .trim();

    if (!/^(AVG|SUM|COUNT|MIN|MAX)\s*\(/i.test(selectExpr)) {
        return (
            'SQL Debugger supports only simple aggregate scalar subqueries in the SELECT list right now.\n' +
            'Use a single AVG, SUM, COUNT, MIN, or MAX expression in the projected subquery.'
        );
    }

    return null;
}

function isCorrelatedProjectedSubquery(subquerySql: string, outerAliases: string[]): boolean {
    return outerAliases.some(alias => {
        const escaped = alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        return new RegExp(`(?:\\b|\\\`)${escaped}(?:\\\`)?\\.`, 'i').test(subquerySql);
    });
}

function skipWhitespace(text: string, index: number): number {
    let cursor = index;
    while (cursor < text.length && /\s/.test(text[cursor])) {
        cursor += 1;
    }
    return cursor;
}

function findMatchingParenInText(text: string, openIndex: number): number {
    let depth = 0;
    let i = openIndex;

    while (i < text.length) {
        const char = text[i];
        if (char === '\'' || char === '"' || char === '`') {
            i = skipQuotedSqlLike(text, i, char);
            continue;
        }
        if (char === '(') {
            depth += 1;
        } else if (char === ')') {
            depth -= 1;
            if (depth === 0) {
                return i;
            }
        }
        i += 1;
    }

    throw new Error('Unbalanced parentheses while parsing projected scalar subquery.');
}

function skipQuotedSqlLike(text: string, start: number, quote: string): number {
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

    throw new Error('Unterminated quoted string while parsing projected scalar subquery.');
}

function isSimpleEqualityJoin(onClause: string): boolean {
    const identifier = '(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)(?:\\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*))?';
    const simpleEquality = new RegExp(`^${identifier}\\s*=\\s*${identifier}$`, 'i');
    return simpleEquality.test(onClause);
}

function maskQuotedContent(sql: string): string {
    let result = '';
    let i = 0;
    let quote: '\'' | '"' | '`' | null = null;

    while (i < sql.length) {
        const char = sql[i];
        const next = sql[i + 1];

        if (!quote && (char === '\'' || char === '"' || char === '`')) {
            quote = char;
            result += ' ';
            i += 1;
            continue;
        }

        if (quote) {
            if (char === quote) {
                if ((quote === '\'' || quote === '"') && next === quote) {
                    result += '  ';
                    i += 2;
                    continue;
                }

                let backslashCount = 0;
                for (let j = i - 1; j >= 0 && sql[j] === '\\'; j -= 1) {
                    backslashCount += 1;
                }
                if (backslashCount % 2 === 0) {
                    quote = null;
                }
            }
            result += ' ';
            i += 1;
            continue;
        }

        result += char;
        i += 1;
    }

    return result;
}
