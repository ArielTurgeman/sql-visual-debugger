// ─── Shared types used across all modules ────────────────────────────────────

export type Row = Record<string, unknown>;

// ── SQL structure ──────────────────────────────────────────────────────────

export type JoinInfo = {
    joinType: string;    // "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN"
    joinTable: string;   // e.g. "orders"
    joinAlias: string;   // e.g. "o"
    joinLeft: string;    // e.g. "c.customer_id"
    joinRight: string;   // e.g. "o.customer_id"
};

/** Lightweight structural representation of the query — enough to build step SQLs.
 *  MySQL handles all validation; we only parse to understand clause boundaries. */
export type ParsedQuery = {
    original: string;
    selectClause: string;        // everything between SELECT and FROM
    fromTable: string;
    fromAlias: string;
    joins: JoinInfo[];
    whereText: string | null;    // raw WHERE condition text (without the keyword)
    orderByText: string | null;  // raw ORDER BY text (without the keyword)
    limitValue: number | null;
};

// ── Debug steps ────────────────────────────────────────────────────────────

export type JoinMeta = {
    conditionLabel: string;
    leftTable: string;
    rightTable: string;
    leftJoinField: string;           // plain column name, e.g. "customer_id"
    rightJoinField: string;          // plain column name, e.g. "customer_id"
    leftRows: number;
    rightRows: number;
    relationship: string;            // "one-to-many" | "many-to-one" | ...
    relationshipNote: string;
    resultRows: number;
    rowDelta: number;
    growthText: string;
    previewLeftColumns: string[];    // plain column names
    previewRightColumns: string[];
    previewLeftRows: Row[];
    previewRightRows: Row[];
    leftMatchCounts: Record<string, number>;
    rightMatchCounts: Record<string, number>;
};

export type DebugStep = {
    name: 'FROM' | 'JOIN' | 'WHERE' | 'SELECT' | 'ORDER BY' | 'LIMIT';
    sqlFragment: string;
    explanation: string;
    rowsBefore: number;
    rowsAfter: number;
    rows: Row[];
    summaryBadge?: string;
    rowsRemoved?: number;
    joinedColumns?: string[];
    joinMeta?: JoinMeta;
    joinType?: string;
};

// ── Connections & extraction ───────────────────────────────────────────────

/**
 * Server-level connection details — no database name.
 * Stored once and reused every time the user switches databases on the same server.
 */
export type ServerConnection = {
    host: string;
    port: number;
    user: string;
};

/**
 * Full connection config passed to MysqlRunner — server + the active database.
 * Composed at runtime from ServerConnection + the selected activeDatabase string.
 */
export type ConnectionConfig = {
    host: string;
    port: number;
    user: string;
    database: string;
};

export type ExtractResult =
    | { sql: string; source: string; rawText: string; selectionStart: import('vscode').Position }
    | { error: string };
