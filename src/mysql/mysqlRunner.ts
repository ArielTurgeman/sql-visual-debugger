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

        // The debugger only issues read-only SELECT/WITH statements. Query
        // safety is still enforced at the application layer: queryExtractor
        // accepts only SELECT / WITH queries, and recursive CTEs are rejected
        // before execution.
    }

    async query(sql: string): Promise<Row[]> {
        if (!this.connection) {
            throw new Error('Not connected to database. Call connect() first.');
        }
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
        if (!this.connection) {
            throw new Error('Not connected to database. Call connect() first.');
        }
        await this.connection.execute(sql);
    }

    async disconnect(): Promise<void> {
        if (this.connection) {
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
