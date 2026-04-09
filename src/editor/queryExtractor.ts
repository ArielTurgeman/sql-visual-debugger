import * as vscode from 'vscode';
import type { ExtractResult } from '../types';

/**
 * Extract a SQL query from the active editor.
 *
 * Priority:
 *  1. If the user has a non-empty selection → use that text exactly.
 *  2. Otherwise → use the entire file content.
 *
 * Returns { sql, source } on success or { error } on failure.
 */
export function extractQuery(editor: vscode.TextEditor): ExtractResult {
    const hasSelection = !editor.selection.isEmpty;

    const raw = hasSelection
        ? editor.document.getText(editor.selection)
        : editor.document.getText();

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

    const fileName = editor.document.fileName.split(/[\\/]/).pop() ?? 'unknown';
    const source = hasSelection ? `${fileName} (selection)` : fileName;

    return { sql, source };
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
    //   • Block comments  /* … */  may span multiple lines.
    //   • Line comments   -- …     run to the end of the line.
    const withoutComments = raw
        .replace(/\/\*[\s\S]*?\*\//g, ' ')  // block comments → single space
        .replace(/--[^\n]*/g, '');           // line comments  → remove entirely

    // Strip trailing semicolon first, then check for embedded ones
    const withoutTrailing = withoutComments.trim().replace(/;\s*$/, '').trim();

    if (withoutTrailing.includes(';')) {
        throw new Error(
            'Only a single SQL statement is supported per debug run.\n' +
            'Remove the extra semicolon or select just one query.'
        );
    }

    return withoutTrailing.replace(/\s+/g, ' ').trim();
}
