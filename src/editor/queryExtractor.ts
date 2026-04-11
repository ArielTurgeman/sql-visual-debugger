import * as vscode from 'vscode';
import type { ExtractResult } from '../types';

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

    return { sql, source, rawText: raw, selectionStart };
}

function getQueryUnderCursor(
    editor: vscode.TextEditor,
    fileName: string,
): { raw: string; selectionStart: vscode.Position; source: string } | { error: string } {
    const fullText = editor.document.getText();
    const cursorOffset = editor.document.offsetAt(editor.selection.active);
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
    let currentStatementHasRoot = false;
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
            currentStatementHasRoot = false;
            lineHasCode = false;
            i += 1;
            continue;
        }

        if (!lineHasCode && parenDepth === 0) {
            const rootMatch = text.slice(i).match(/^(SELECT|WITH)\b/i);
            if (rootMatch) {
                if (currentStatementHasRoot) {
                    boundaries.push({ start: statementStart, end: i });
                    statementStart = i;
                }
                currentStatementHasRoot = true;
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
