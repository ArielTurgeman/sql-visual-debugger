export function detectDatabaseNameInSql(sql: string): string | undefined {
    const useMatch = sql.match(/\bUSE\s+`?(\w+)`?\s*;/i);
    if (useMatch?.[1]) {
        return useMatch[1];
    }

    const annotationMatch = sql.match(/--\s*@?(?:db|database)\s*:\s*(\w+)/i);
    if (annotationMatch?.[1]) {
        return annotationMatch[1];
    }

    return undefined;
}
