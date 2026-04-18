import type { JoinMeta, WindowColumnMeta } from './stepEngineTypes';
import { bareIdentifier } from './stepEngineParsing';

export function buildWindowExplanation(
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
): string {
  if (meta.functionName === 'ROW_NUMBER') {
    return 'Shows each rowג€™s position in its window after partitioning and ordering are applied.';
  }
  if (meta.functionName === 'RANK') {
    return 'Shows the rank of each row in its window, with gaps after ties.';
  }
  if (meta.functionName === 'DENSE_RANK') {
    return 'Shows the rank of each row in its window, without gaps after ties.';
  }
  return `Shows the ${meta.functionName} value computed over the rows in the same window.`;
}

export function buildWindowHowComputed(
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
): string[] {
  const steps = [
    meta.partitionBy.length > 0
      ? `Rows were split into groups using ${meta.partitionBy.join(', ')}.`
      : 'All rows stayed in one group.',
    meta.orderBy.length > 0
      ? `Rows inside each group were ordered by ${meta.orderBy.join(', ')}.`
      : 'No window ordering was provided, so the existing row order was used.',
  ];

  if (meta.functionName === 'ROW_NUMBER') {
    steps.push('A running row number was assigned from top to bottom inside each group.');
  } else if (meta.functionName === 'RANK') {
    steps.push('Rows with the same ordering values received the same rank, and the next rank skipped ahead.');
  } else if (meta.functionName === 'DENSE_RANK') {
    steps.push('Rows with the same ordering values received the same rank, and the next distinct value received the next consecutive rank.');
  } else {
    steps.push(`The ${meta.functionName} result was calculated for the rows visible in each ordered group.`);
  }

  return steps;
}

export function buildWindowPreviewColumns(
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
): string[] {
  return dedupeStrings([
    ...meta.partitionBy,
    ...meta.orderByTerms.map(term => term.column),
    ...(meta.sourceColumn ? [meta.sourceColumn] : []),
    meta.outputColumn,
  ]);
}

export function buildWindowPreviewRows(
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
  previewRows: Record<string, unknown>[],
): Record<string, unknown>[] {
  return [...previewRows].sort((left, right) => comparePreviewRows(left, right, meta));
}

export function buildJoinDisplay(
  leftRows: Record<string, unknown>[],
  rightRows: Record<string, unknown>[],
  leftKeyCol: string,
  rightKeyCol: string,
  rightAlias: string,
  joinType: string,
): { rows: Record<string, unknown>[]; columns: string[] } {
  const leftCols = leftRows.length > 0 ? Object.keys(leftRows[0]) : [];
  const rightCols = rightRows.length > 0 ? Object.keys(rightRows[0]) : [];
  const leftColSet = new Set(leftCols);
  const rightOutputKeys = rightCols.map(col => (leftColSet.has(col) ? `${rightAlias}.${col}` : col));
  const outputColumns = [...leftCols, ...rightOutputKeys];

  const isFull = /\bFULL\b/i.test(joinType);
  const isLeft = /\bLEFT\b/i.test(joinType) || isFull;
  const isRight = /\bRIGHT\b/i.test(joinType) || isFull;
  const isCross = /\bCROSS\b/i.test(joinType);

  function mergeRow(
    leftRow: Record<string, unknown> | null,
    rightRow: Record<string, unknown> | null,
  ): Record<string, unknown> {
    const row: Record<string, unknown> = {};
    for (const col of leftCols) row[col] = leftRow?.[col] ?? null;
    rightCols.forEach((col, index) => {
      row[rightOutputKeys[index]] = rightRow?.[col] ?? null;
    });
    return row;
  }

  if (isCross) {
    return {
      rows: leftRows.flatMap(leftRow => rightRows.map(rightRow => mergeRow(leftRow, rightRow))),
      columns: outputColumns,
    };
  }

  const rightIndex = new Map<string, number[]>();
  rightRows.forEach((row, index) => {
    const key = String(row[rightKeyCol] ?? '');
    const bucket = rightIndex.get(key);
    if (bucket) bucket.push(index);
    else rightIndex.set(key, [index]);
  });

  const result: Record<string, unknown>[] = [];
  const matchedRightSet = new Set<number>();

  for (const leftRow of leftRows) {
    const leftKey = String(leftRow[leftKeyCol] ?? '');
    const matchIndexes = rightIndex.get(leftKey) ?? [];
    if (matchIndexes.length > 0) {
      for (const index of matchIndexes) {
        result.push(mergeRow(leftRow, rightRows[index]));
        matchedRightSet.add(index);
      }
    } else if (isLeft) {
      result.push(mergeRow(leftRow, null));
    }
  }

  if (isRight) {
    rightRows.forEach((rightRow, index) => {
      if (!matchedRightSet.has(index)) result.push(mergeRow(null, rightRow));
    });
  }

  return { rows: result, columns: outputColumns };
}

export function inferRelationship(
  leftRows: Record<string, unknown>[],
  rightRows: Record<string, unknown>[],
  leftKey: string,
  rightKey: string,
): JoinMeta['relationship'] {
  const leftDup = hasDuplicates(leftRows, leftKey);
  const rightDup = hasDuplicates(rightRows, rightKey);
  if (!leftDup && !rightDup) return 'one-to-one';
  if (!leftDup && rightDup) return 'one-to-many';
  if (leftDup && !rightDup) return 'many-to-one';
  return 'many-to-many';
}

export function readRowValue(row: Record<string, unknown>, targetCol: string): unknown {
  if (targetCol in row) return row[targetCol];
  const bareTarget = bareIdentifier(targetCol).toLowerCase();
  const matchedKey = Object.keys(row).find(key => bareIdentifier(key).toLowerCase() === bareTarget);
  return matchedKey ? row[matchedKey] : null;
}

export function dedupeStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(value);
  }
  return result;
}

function comparePreviewRows(
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  meta: Omit<WindowColumnMeta, 'explanation' | 'howComputed' | 'previewColumns' | 'previewRows'>,
): number {
  for (const col of meta.partitionBy) {
    const result = compareUnknown(readRowValue(left, col), readRowValue(right, col));
    if (result !== 0) return result;
  }

  for (const term of meta.orderByTerms) {
    const result = compareUnknown(readRowValue(left, term.column), readRowValue(right, term.column));
    if (result !== 0) {
      return term.direction === 'DESC' ? -result : result;
    }
  }

  return compareUnknown(readRowValue(left, meta.outputColumn), readRowValue(right, meta.outputColumn));
}

function compareUnknown(left: unknown, right: unknown): number {
  const leftNum = Number(left);
  const rightNum = Number(right);
  if (!Number.isNaN(leftNum) && !Number.isNaN(rightNum)) {
    return leftNum - rightNum;
  }

  const leftStr = String(left ?? '');
  const rightStr = String(right ?? '');
  return leftStr.localeCompare(rightStr, undefined, { numeric: true, sensitivity: 'base' });
}

function hasDuplicates(rows: Record<string, unknown>[], key: string): boolean {
  const seen = new Set<string>();
  for (const row of rows) {
    const value = String(row[key] ?? '');
    if (seen.has(value)) return true;
    seen.add(value);
  }
  return false;
}
