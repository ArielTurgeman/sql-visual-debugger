import type { QueryBlockType } from './queryBlocks';

export type DebugStep = {
  name: string;
  title: string;
  explanation: string;
  sqlFragment: string;
  rowsBefore: number;
  rowsAfter: number;
  data: Record<string, unknown>[];
  columns: string[];
  joinMeta?: JoinMeta;
  schemaContext?: { joinIndicatorColumns: string[] };
  impact?: string;
  preFilterRows?: Record<string, unknown>[];
  preFilterColumns?: string[];
  whereColumns?: string[];
  whereInSubquery?: WhereInSubqueryMeta;
  whereScalarSubquery?: WhereScalarSubqueryMeta;
  sortColumns?: string[];
  groupByColumns?: string[];
  groupBySourceColumns?: string[];
  aggColumns?: Array<{ col: string; fn: string; srcCol?: string }>;
  aggSummary?: string;
  preGroupRows?: Record<string, unknown>[];
  preGroupColumns?: string[];
  blockType?: QueryBlockType;
  blockName?: string;
  blockIndex?: number;
  blockDependencies?: string[];
  blockSourceText?: string;
  blockSourceStart?: number;
  sourceRows?: number;
  sourceLabel?: string;
  windowColumns?: WindowColumnMeta[];
  caseColumns?: CaseColumnMeta[];
  preSelectRows?: Record<string, unknown>[];
  preSelectColumns?: string[];
  distinctMeta?: DistinctMeta;
};

export type WhereInSubqueryMeta = {
  explanation: string;
  rows: Record<string, unknown>[];
  columns: string[];
  totalRows: number;
};

export type WhereScalarSubqueryMeta = {
  explanation: string;
  value: unknown;
  columnLabel: string;
};

export type WindowColumnMeta = {
  outputColumn: string;
  expression: string;
  functionName: string;
  sourceColumn?: string;
  sourceExpression?: string;
  partitionBy: string[];
  partitionByExpressions?: string[];
  orderBy: string[];
  orderByTerms: Array<{ column: string; direction: 'ASC' | 'DESC' }>;
  orderBySourceTerms?: Array<{ expression: string; direction: 'ASC' | 'DESC' }>;
  explanation: string;
  howComputed: string[];
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
};

export type CaseColumnMeta = {
  outputColumn: string;
  expression: string;
  inputColumns: string[];
  rowExplanations: CaseRowExplanation[];
};

export type CaseRowExplanation = {
  matchedRule: string;
  returnedValue: unknown;
  inputValues: Array<{ column: string; value: unknown }>;
};

export type DistinctMeta = {
  columns: string[];
  rows: Record<string, unknown>[];
};

export type JoinMeta = {
  leftTable: string;
  rightTable: string;
  leftKey: string;
  rightKey: string;
  leftKeyCol: string;
  rightKeyCol: string;
  joinType: string;
  leftRows: Record<string, unknown>[];
  rightRows: Record<string, unknown>[];
  allLeftRows: Record<string, unknown>[];
  allRightRows: Record<string, unknown>[];
  leftColumns: string[];
  rightColumns: string[];
  relationship: 'one-to-one' | 'one-to-many' | 'many-to-one' | 'many-to-many';
  rowDelta: number;
  joinedResultColumns: string[];
  joinIndicatorColumns: string[];
};

export type ColumnDef = {
  displayName: string;
  sqlExpr: string;
  sqlAlias?: string;
};

export type ParsedQuery = {
  selectClause: string;
  fromClause: string;
  whereClause?: string;
  groupByClause?: string;
  havingClause?: string;
  orderByClause?: string;
  limitClause?: string;
  joins: ParsedJoin[];
};

export type ParsedJoin = {
  joinType: string;
  tableName: string;
  tableAlias: string;
  rawClause: string;
  onClause: string;
  leftExpr: string;
  rightExpr: string;
};

export type ParsedCaseExpression = {
  outputColumn: string;
  expression: string;
  inputRefs: Array<{ expr: string; label: string }>;
  branches: Array<{ condition: string; label: string; inputRefs: Array<{ expr: string; label: string }> }>;
  elseLabel: string;
};

export const MAX_DISPLAY_ROWS = 200;
