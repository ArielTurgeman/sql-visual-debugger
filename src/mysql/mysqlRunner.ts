// eslint-disable-next-line @typescript-eslint/no-var-requires
const mysql = require('mysql2/promise') as typeof import('mysql2/promise');

import type { ConnectionConfig, Row } from '../types';

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

        // CTE debugging materializes intermediate blocks into temporary tables.
        // That internal implementation detail is rejected by MySQL when the
        // session is forced into READ ONLY mode, so safety is enforced at the
        // application layer instead: queryExtractor only accepts SELECT / WITH
        // queries, and recursive CTEs are rejected before execution.
    }

    async query(sql: string): Promise<Row[]> {
        if (!this.connection) {
            throw new Error('Not connected to database. Call connect() first.');
        }
        const [rows] = await this.connection.execute(sql);
        return rows as Row[];
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
