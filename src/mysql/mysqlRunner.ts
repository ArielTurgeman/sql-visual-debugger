// eslint-disable-next-line @typescript-eslint/no-var-requires
const mysql = require('mysql2/promise') as typeof import('mysql2/promise');

import type { ConnectionConfig, Row } from '../types';

export const MYSQL_DEBUG_QUERY_TIMEOUT_MS = 10_000;

export class MysqlRunner {
    private connection: import('mysql2/promise').Connection | null = null;

    async connect(config: ConnectionConfig, password: string): Promise<void> {
        this.connection = await mysql.createConnection({
            host: config.host,
            port: config.port,
            user: config.user,
            password,
            database: config.database,
            connectTimeout: 10_000,
            // Prevent native bindings from being required in VS Code's bundled env
            supportBigNumbers: true,
            bigNumberStrings: true,
        });

        await this.connection.query('SET SESSION TRANSACTION READ ONLY');
        await this.connection.query('START TRANSACTION READ ONLY');

        // The debugger only issues read-only SELECT/WITH statements. Query
        // safety is still enforced at the application layer: queryExtractor
        // accepts only SELECT / WITH queries, and recursive CTEs are rejected
        // before execution.
    }

    async query(sql: string): Promise<Row[]> {
        if (!this.connection) {
            throw new Error('Not connected to database. Call connect() first.');
        }
        assertReadOnlySql(sql);
        try {
            const [rows] = await this.connection.execute({
                sql,
                timeout: MYSQL_DEBUG_QUERY_TIMEOUT_MS,
            });
            return rows as Row[];
        } catch (error) {
            throw normalizeMysqlQueryError(error);
        }
    }

    async execute(sql: string): Promise<void> {
        throw new Error(
            'Unsafe execution API disabled.\n' +
            'SQL Debugger only allows read-only query execution through MysqlRunner.query().'
        );
    }

    async disconnect(): Promise<void> {
        if (this.connection) {
            await this.connection.query('ROLLBACK').catch(() => { /* ignore rollback errors */ });
            await this.connection.end().catch(() => { /* ignore close errors */ });
            this.connection = null;
        }
    }

    isConnected(): boolean {
        return this.connection !== null;
    }
}

function normalizeMysqlQueryError(error: unknown): Error {
    if (error instanceof Error) {
        const mysqlError = error as Error & { code?: string; errno?: number };
        if (mysqlError.code === 'PROTOCOL_SEQUENCE_TIMEOUT' || /timeout/i.test(mysqlError.message)) {
            return new Error(
                `SQL Debugger timed out after ${Math.round(MYSQL_DEBUG_QUERY_TIMEOUT_MS / 1000)} seconds.\n` +
                'Try narrowing the query with WHERE or LIMIT and run it again.'
            );
        }
        return mysqlError;
    }

    return new Error(String(error));
}

function assertReadOnlySql(sql: string): void {
    const normalized = sql.trim().replace(/;\s*$/, '').trim();
    if (!/^(SELECT|WITH)\b/i.test(normalized)) {
        throw new Error(
            'SQL Debugger only runs read-only SELECT/WITH statements.\n' +
            'This SQL was blocked before execution.'
        );
    }

    const masked = maskQuotedContent(normalized);
    if (masked.includes(';')) {
        throw new Error(
            'SQL Debugger only runs a single read-only statement per execution.\n' +
            'This SQL was blocked before execution.'
        );
    }

    const blockedPatterns = [
        /\bINTO\b/i,
        /\bFOR\s+UPDATE\b/i,
        /\bLOCK\s+IN\s+SHARE\s+MODE\b/i,
    ];
    if (blockedPatterns.some(pattern => pattern.test(masked))) {
        throw new Error(
            'SQL Debugger blocked a non-read-only SQL shape before execution.'
        );
    }
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
