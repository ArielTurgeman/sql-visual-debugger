import * as vscode from 'vscode';
import type { DebugStep } from '../engine/stepEngine';

/**
 * Given the raw (unsanitized) editor document text and the debug steps produced
 * by executeDebugSteps, returns a parallel array where each entry is an array of
 * one or two vscode.Ranges covering the exact SQL text for that step.
 *
 * • Non-JOIN steps → one Range covering the whole clause text.
 * • JOIN steps     → two Ranges: one for "JOIN <table> <alias>", one for
 *   "ON <left> = <right>", so both fragments are highlighted independently
 *   when they fall on separate lines.
 *
 * Returns an empty array for a step if no match is found in the source text
 * (e.g. the document was edited after the run was started).
 */
export function computeStepRanges(
  originalText: string,
  steps: DebugStep[],
  basePosition: vscode.Position = new vscode.Position(0, 0),
): vscode.Range[][] {
  return steps.map(step => {
    const searchText = step.blockSourceText ?? originalText;
    const blockBase = step.blockSourceStart !== undefined
      ? offsetToPosition(originalText, step.blockSourceStart)
      : new vscode.Position(0, 0);
    const absoluteBase = new vscode.Position(
      basePosition.line + blockBase.line,
      blockBase.line === 0 ? basePosition.character + blockBase.character : blockBase.character,
    );

    return findRangesForStep(searchText, step).map(range => offsetRange(range, absoluteBase));
  });
}

// ─── Per-step dispatch ────────────────────────────────────────────────────────

function findRangesForStep(text: string, step: DebugStep): vscode.Range[] {
  const fragment = step.sqlFragment?.trim();
  if (!fragment) { return []; }

  if (step.name === 'JOIN') {
    return findJoinRanges(text, fragment);
  }

  const r = findFragment(text, fragment);
  return r ? [r] : [];
}

/**
 * For JOIN steps the raw clause is "INNER JOIN orders o ON o.id = p.id".
 * We split at the first " ON " boundary to produce two independent ranges so
 * that both the JOIN line and the ON line can be highlighted even when they
 * sit on different source lines.
 */
function findJoinRanges(text: string, rawClause: string): vscode.Range[] {
  // Find the first "ON" surrounded by whitespace — that is the actual ON keyword,
  // not an identifier fragment (e.g. "ONLINE").
  const onMatch = /\s+(ON)\s+/i.exec(rawClause);
  if (!onMatch) {
    // No ON found (shouldn't happen in valid SQL) — fall back to a single range.
    const r = findFragment(text, rawClause);
    return r ? [r] : [];
  }

  // joinPart: everything before the whitespace-ON-whitespace boundary.
  const joinPart = rawClause.slice(0, onMatch.index).trim();

  // onPart: "ON <condition>" — skip the single leading space captured by \s+
  // so the slice starts exactly at the "O" of "ON".
  const onPart = rawClause.slice(onMatch.index + 1).trim();

  const r1 = findFragment(text, joinPart);
  const r2 = findFragment(text, onPart);
  return [r1, r2].filter((r): r is vscode.Range => r !== null);
}

// ─── Core match + range helper ─────────────────────────────────────────────

/**
 * Builds a whitespace-flexible, case-insensitive regex from `fragment`
 * (spaces → \\s+ so the pattern matches across newlines and indentation),
 * searches `text`, and returns a vscode.Range or null.
 *
 * Special-regex characters in each token are escaped before joining with \\s+,
 * so fragments containing dots, parentheses, asterisks, etc. match literally.
 */
function findFragment(text: string, fragment: string): vscode.Range | null {
  const tokens = fragment.trim().split(/\s+/);
  if (tokens.length === 0) { return null; }

  const pattern = tokens
    .map(tok => tok.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('\\s+');

  let regex: RegExp;
  try {
    regex = new RegExp(pattern, 'i');
  } catch {
    return null;  // malformed pattern — skip silently
  }

  const match = regex.exec(text);
  if (!match) { return null; }

  const start = match.index;
  const end   = start + match[0].length;

  return new vscode.Range(
    offsetToPosition(text, start),
    offsetToPosition(text, end),
  );
}

// ─── Utility ─────────────────────────────────────────────────────────────────

/** Converts a character offset in `text` into a vscode line/character Position. */
function offsetToPosition(text: string, offset: number): vscode.Position {
  const before = text.slice(0, offset);
  const lines  = before.split('\n');
  return new vscode.Position(
    lines.length - 1,
    lines[lines.length - 1].length,
  );
}

/**
 * Translates a range computed against the start of `originalText` so it points
 * at the correct document position when the debug run came from a selection.
 */
function offsetRange(range: vscode.Range, base: vscode.Position): vscode.Range {
  const translate = (pos: vscode.Position): vscode.Position =>
    new vscode.Position(
      pos.line + base.line,
      pos.line === 0 ? pos.character + base.character : pos.character,
    );

  return new vscode.Range(
    translate(range.start),
    translate(range.end),
  );
}
