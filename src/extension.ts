import * as vscode from 'vscode';
import { detectDatabaseNameInSql } from './editor/databaseDetection';
import { extractQuery } from './editor/queryExtractor';
import { computeStepRanges } from './editor/rangeMapper';
import { assertLocalOnlyServer, getLocalOnlyHostError } from './mysql/localHostPolicy';
import { MysqlRunner } from './mysql/mysqlRunner';
import { executeDebugSteps } from './engine/stepEngine';
import { getOrCreatePanel, recreatePanel, sendLoading, sendResult, sendError } from './webview/panel';
import type { ConnectionConfig, ServerConnection } from './types';

// ─── Trial ─────────────────────────────────────────────────────────────────

function checkTrial(): boolean {
    return true;
}

// ─── Storage keys ──────────────────────────────────────────────────────────
const KEY_SERVER   = 'sqlDebugger.server';
const KEY_ACTIVE   = 'sqlDebugger.activeDatabase';
const KEY_KNOWN    = 'sqlDebugger.knownDatabases';
const KEY_LEGACY   = 'sqlDebugger.connection';
// Retained ONLY to delete any password that a previous version may have stored.
// Passwords are no longer persisted — they live in session RAM only.
const KEY_PASSWORD_LEGACY = 'sqlDebugger.mysqlPassword';

// ─── Module-level state ────────────────────────────────────────────────────

// Replaced on each run to avoid accumulating duplicate webview listeners.
let panelMessageHandler: vscode.Disposable | undefined;

// The SQL extracted during the most recent successful editor read.
// Stored here so the in-panel DB-switch flow can re-run without needing
// the active text editor (which would be `undefined` while the webview has focus).
// rawText and documentUri are used to compute editor decorations.
let lastRun: {
  sql: string;
  source: string;
  rawText: string;
  documentUri: vscode.Uri;
  selectionStart: vscode.Position;
} | undefined;

// Password held in session RAM only — never written to any durable storage.
// Set to undefined on activate, on server reconfiguration, and on every
// database switch so the user must re-enter it for each new DB context.
let cachedPassword: string | undefined;

// ─── Editor decoration state ───────────────────────────────────────────────

// Created once on activate; disposed on deactivate.
// Uses the built-in "find match" highlight colour so it looks consistent
// across light and dark themes without hardcoding RGBA values.
let stepDecorationType: vscode.TextEditorDecorationType | undefined;

// Parallel to the last run's steps — one Range[] per step index.
// Populated after a successful debug run, cleared when a new run starts or
// when the source document is edited.
let lastRunRanges: vscode.Range[][] | undefined;

// URI of the document that produced lastRunRanges.
let lastRunUri: vscode.Uri | undefined;

// ─── Extension lifecycle ───────────────────────────────────────────────────

export function activate(context: vscode.ExtensionContext): void {
    // One-time cleanup: delete any password a previous version may have stored
    // in VS Code's secrets API.  Passwords are no longer persisted anywhere.
    void Promise.resolve(context.secrets.delete(KEY_PASSWORD_LEGACY));

    // Create the decoration type used to highlight the active step in the editor.
    stepDecorationType = vscode.window.createTextEditorDecorationType({
        backgroundColor: new vscode.ThemeColor('editor.findMatchHighlightBackground'),
        borderRadius: '2px',
        overviewRulerColor: new vscode.ThemeColor('editorOverviewRuler.findMatchForeground'),
        overviewRulerLane: vscode.OverviewRulerLane.Center,
    });
    context.subscriptions.push(stepDecorationType);

    // Clear decorations whenever the user edits the SQL source document so stale
    // highlights from a previous run never persist on modified text.
    context.subscriptions.push(
        vscode.workspace.onDidChangeTextDocument(event => {
            if (lastRunUri &&
                event.document.uri.toString() === lastRunUri.toString()) {
                clearDecorations();
            }
        })
    );

    // Primary command — reads SQL from the active editor, then runs the debugger.
    context.subscriptions.push(
        vscode.commands.registerCommand('sqlDebugger.debugQuery', () => {
            return runDebugger(context);
        })
    );

    // Utility: reconfigure the MySQL server (host / port / user only).
    context.subscriptions.push(
        vscode.commands.registerCommand('sqlDebugger.configureConnection', async () => {
            const current = getServer(context);
            const updated = await promptForServer(context, current);
            if (updated) {
                // Server details changed — discard the in-memory password so the
                // next run prompts for fresh credentials against the new server.
                cachedPassword = undefined;
                vscode.window.showInformationMessage(
                    `SQL Debugger: server saved (${updated.user}@${updated.host})`
                );
            }
        })
    );

    // Switch database from the command palette — asks for a DB name and then
    // re-prompts for the password (different DB = different auth context).
    context.subscriptions.push(
        vscode.commands.registerCommand('sqlDebugger.changeConnection', async () => {
            const db = await promptForDatabaseName(context);
            if (!db) { return; }
            await activateDatabase(context, db);

            // Require the user to re-enter the password for the new database.
            cachedPassword = undefined;

            // If the panel is already open and we have SQL from a previous run,
            // re-run immediately without going through the editor check.
            const panel = recreatePanel(context);
            if (lastRun) {
                await rerunLastQueryInPanel(context, panel);
            } else {
                // No previous run — fall back to the normal command (editor must be focused).
                await vscode.commands.executeCommand('sqlDebugger.debugQuery');
            }
        })
    );
}

export function deactivate(): void {
    // stepDecorationType is in context.subscriptions so VS Code disposes it
    // automatically, but being explicit also clears any lingering highlights.
    stepDecorationType?.dispose();
    stepDecorationType = undefined;
}

// ─── Core debugger flow ────────────────────────────────────────────────────

/**
 * Entry point called by the `sqlDebugger.debugQuery` command.
 * Reads SQL from the active text editor, then delegates to executeDebugSession.
 */
async function runDebugger(context: vscode.ExtensionContext): Promise<void> {
    // 1. Require an active SQL editor
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showErrorMessage('SQL Debugger: Open a .sql file first.');
        return;
    }

    // 2. Extract and validate the exact SQL input for this run.
    let sql: string;
    let source: string;
    let rawText: string;
    let selectionStart: vscode.Position;
    try {
        const result = extractQuery(editor);
        if ('error' in result) {
            vscode.window.showErrorMessage(`SQL Debugger: ${result.error}`);
            return;
        }
        sql = result.sql;
        source = result.source;
        rawText = result.rawText;
        selectionStart = result.selectionStart;
    } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        vscode.window.showErrorMessage(`SQL Debugger: ${msg}`);
        return;
    }

    // 3. Persist so in-panel switches can re-run without re-reading the editor.
    const documentUri = editor.document.uri;
    lastRun = { sql, source, rawText, documentUri, selectionStart };

    // 4b. Try to detect the active database from the editor context (USE statement,
    //     annotation comment, or a safe API probe of any installed SQL extension).
    //     If detected and different from the stored DB, auto-switch before running
    //     so the debugger stays in sync when the user changes DB in their SQL client.
    const detectedDb = await detectDatabaseFromContext(rawText, editor);
    if (detectedDb) {
        await addKnownDatabase(context, detectedDb);
        if (detectedDb !== getActiveDatabase(context)) {
            await activateDatabase(context, detectedDb);
        }
    }

    // 5. Open (or focus) the panel; clear any decorations from the previous run
    const panel = getOrCreatePanel(context);
    await preparePanelForExecution(context, panel);

    // 6. Register the webview → extension message handler (replaces previous run's handler)

    // 7. One-time migration of the legacy combined config key
    migrateLegacyConfig(context);

    // 8. Execute
    await executeDebugSession(context, panel, rawText, source);
}

async function preparePanelForExecution(
    context: vscode.ExtensionContext,
    panel: vscode.WebviewPanel,
): Promise<void> {
    clearDecorations();
    sendLoading(panel);
    registerMessageHandler(context, panel);
}

async function rerunLastQueryInPanel(
    context: vscode.ExtensionContext,
    panel: vscode.WebviewPanel,
): Promise<void> {
    if (!lastRun) {
        return;
    }

    // Do NOT call sendLoading here — replacing the webview HTML twice in quick
    // succession on a visible panel (loading → result) triggers a VS Code webview
    // rendering bug that leaves the panel black until it is closed and reopened.
    // Skipping the loading state means the old result stays visible while the new
    // query runs, then gets replaced exactly once by sendResult / sendError.
    clearDecorations();
    registerMessageHandler(context, panel);
    await executeDebugSession(context, panel, lastRun.rawText, lastRun.source);
}

/**
 * Runs the actual connection + query execution + render cycle.
 * Called both by runDebugger (fresh run from editor) and directly by the
 * in-panel message handler (DB switch without re-reading the editor).
 */
async function executeDebugSession(
    context: vscode.ExtensionContext,
    panel: vscode.WebviewPanel,
    rawSql: string,
    source: string,
): Promise<void> {
    // Ensure we have a saved server; prompt on first-ever run
    let server = getServer(context);
    if (!server) {
        server = await promptForServer(context) ?? undefined;
        if (!server) {
            sendError(panel, 'Connection setup cancelled. Run the debugger again to configure MySQL.');
            return;
        }
    }
    try {
        assertLocalOnlyServer(server);
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        sendError(panel, message);
        return;
    }

    // Ensure there is an active database
    let activeDb = getActiveDatabase(context);
    if (!activeDb) {
        activeDb = await promptForDatabaseName(context) ?? undefined;
        if (!activeDb) {
            sendError(panel, 'No database selected. Run the debugger again and enter a database name.', getKnownDatabases(context));
            return;
        }
        await activateDatabase(context, activeDb);
    }

    // Password lives only in session RAM. Prompt whenever it is not yet available
    // (first run after VS Code launch, after a server reconfigure, after a DB switch).
    if (cachedPassword === undefined) {
        const pw = await promptForPassword(server.user);
        if (pw === null) {
            sendError(panel, 'Password entry cancelled. Run the debugger again to connect.');
            return;
        }
        cachedPassword = pw;
    }

    const config: ConnectionConfig = { ...server, database: activeDb };

    const runner = new MysqlRunner();
    try {
        await runner.connect(config, cachedPassword);
        const steps = await executeDebugSteps(rawSql, runner);

        // Map each step's clause text back to exact editor ranges using the
        // original (pre-sanitization) source.  The ranges stay in extension
        // memory; the webview requests highlights by step index via postMessage.
        if (lastRun?.rawText) {
            lastRunRanges = computeStepRanges(lastRun.rawText, steps, lastRun.selectionStart);
            lastRunUri    = lastRun.documentUri;
        }

        await addKnownDatabase(context, activeDb);

        sendResult(
            panel,
            rawSql,
            source,
            `${activeDb}@${server.host}`,
            steps,
            activeDb,
            getKnownDatabases(context),
        );
    } catch (err) {
        if (shouldForgetCachedPassword(err)) {
            cachedPassword = undefined;
        }
        const message = err instanceof Error ? err.message : String(err);
        sendError(panel, message, getKnownDatabases(context), activeDb);
    } finally {
        await runner.disconnect();
    }
}

/**
 * Registers the webview → extension message handler on the given panel.
 * Disposes any handler registered during a previous run first.
 *
 * All DB-switch paths call executeDebugSession directly with the stored lastRun SQL
 * so they never touch vscode.window.activeTextEditor (which is undefined while
 * the webview panel has focus).
 */
function registerMessageHandler(
    context: vscode.ExtensionContext,
    panel: vscode.WebviewPanel,
): void {
    panelMessageHandler?.dispose();
    panelMessageHandler = panel.webview.onDidReceiveMessage(async (msg) => {
        if (msg.command === 'switchDatabase' && msg.database) {
            // Known-DB selected — switching databases requires re-authentication.
            cachedPassword = undefined;
            await activateDatabase(context, msg.database as string);
            if (lastRun) {
                await rerunLastQueryInPanel(context, panel);
            }

        } else if (msg.command === 'promptDatabase') {
            // "Enter new database name" — ask for DB name, then re-authenticate.
            const db = await promptForDatabaseName(context);
            if (db) {
                cachedPassword = undefined;
                await activateDatabase(context, db);
                if (lastRun) {
                    await rerunLastQueryInPanel(context, panel);
                }
            }

        } else if (msg.command === 'changeConnection') {
            // "Change server / credentials" link — full server reconfigure only, no re-run.
            // cachedPassword is cleared inside the configureConnection command handler.
            await vscode.commands.executeCommand('sqlDebugger.configureConnection');

        } else if (msg.command === 'activeStep' && typeof msg.index === 'number') {
            // Webview navigated to a new step — highlight the matching SQL text
            // in the editor that produced the last run.
            const ranges = lastRunRanges?.[msg.index as number];
            applyDecorations(ranges ?? []);
        }
    }, undefined, context.subscriptions);
}

// ─── Decoration helpers ────────────────────────────────────────────────────

/**
 * Removes all step-highlight decorations from every currently visible editor
 * and discards the stored range data so stale highlights cannot reappear.
 * Called before every new debug run and whenever the source document changes.
 */
function clearDecorations(): void {
    lastRunRanges = undefined;
    if (!stepDecorationType) { return; }
    for (const editor of vscode.window.visibleTextEditors) {
        editor.setDecorations(stepDecorationType, []);
    }
}

/**
 * Applies `ranges` as step-highlight decorations on the visible editor that
 * shows `lastRunUri`.  Silently does nothing if that editor is not currently
 * visible (e.g. the user closed or moved the SQL tab).
 */
function applyDecorations(ranges: vscode.Range[]): void {
    if (!stepDecorationType || !lastRunUri) { return; }
    const editor = vscode.window.visibleTextEditors.find(
        e => e.document.uri.toString() === lastRunUri!.toString(),
    );
    if (!editor) { return; }
    editor.setDecorations(stepDecorationType, ranges);
}

// ─── State accessors ───────────────────────────────────────────────────────

function getServer(context: vscode.ExtensionContext): ServerConnection | undefined {
    return context.globalState.get<ServerConnection>(KEY_SERVER);
}

function getActiveDatabase(context: vscode.ExtensionContext): string | undefined {
    const db = context.globalState.get<string>(KEY_ACTIVE);
    return db && db.trim() ? db : undefined;
}

function getKnownDatabases(context: vscode.ExtensionContext): string[] {
    return context.globalState.get<string[]>(KEY_KNOWN) ?? [];
}

async function activateDatabase(context: vscode.ExtensionContext, db: string): Promise<void> {
    await context.globalState.update(KEY_ACTIVE, db);
}

async function addKnownDatabase(context: vscode.ExtensionContext, db: string): Promise<void> {
    const known = getKnownDatabases(context);
    if (!known.includes(db)) {
        await context.globalState.update(KEY_KNOWN, [...known, db]);
    }
}

// ─── Legacy migration (runs once, then the old key is cleared) ─────────────

function migrateLegacyConfig(context: vscode.ExtensionContext): void {
    const legacy = context.globalState.get<ConnectionConfig>(KEY_LEGACY);
    if (!legacy) { return; }

    if (!getServer(context)) {
        context.globalState.update(KEY_SERVER, { host: legacy.host, port: legacy.port, user: legacy.user });
    }
    if (legacy.database) {
        if (!getActiveDatabase(context)) {
            context.globalState.update(KEY_ACTIVE, legacy.database);
        }
        addKnownDatabase(context, legacy.database);
    }
    context.globalState.update(KEY_LEGACY, undefined);
}

// ─── Connection prompts ─────────────────────────────────────────────────────

/**
 * Prompt for server details only — host, port, and username.
 * The password is intentionally NOT collected here; it is requested separately
 * at connection time and kept only in session RAM (never persisted).
 * Pre-fills with stored values so the user edits only what changed.
 */
async function promptForServer(
    context: vscode.ExtensionContext,
    defaults?: ServerConnection,
): Promise<ServerConnection | null> {
    const host = await vscode.window.showInputBox({
        title: 'SQL Debugger — MySQL Server (1/3)',
        prompt: 'Host (local only in v1)',
        value: defaults?.host ?? '',
        ignoreFocusOut: true,
        placeHolder: 'Example: localhost',
        validateInput: value => getLocalOnlyHostError(value.trim()),
    });
    if (host === undefined) { return null; }

    const portStr = await vscode.window.showInputBox({
        title: 'SQL Debugger — MySQL Server (2/3)',
        prompt: 'Port',
        value: defaults?.port !== undefined ? String(defaults.port) : '',
        ignoreFocusOut: true,
        placeHolder: 'Example: 3306',
        validateInput: v => (/^\d+$/.test(v) ? null : 'Enter a valid port number'),
    });
    if (portStr === undefined) { return null; }

    const user = await vscode.window.showInputBox({
        title: 'SQL Debugger — MySQL Server (3/3)',
        prompt: 'Username',
        value: defaults?.user ?? '',
        ignoreFocusOut: true,
        placeHolder: 'Example: root',
    });
    if (user === undefined) { return null; }

    const server: ServerConnection = {
        host: host.trim(),
        port: parseInt(portStr, 10),
        user: user.trim(),
    };
    assertLocalOnlyServer(server);

    // Persist only non-sensitive server details.
    await context.globalState.update(KEY_SERVER, server);

    return server;
}

/**
 * Prompt for the MySQL password.
 * The returned string is held only in the `cachedPassword` module variable
 * for the duration of the VS Code session — it is never written to any
 * durable storage (globalState, secrets API, settings, disk).
 *
 * An empty string is a valid return value (passwordless MySQL accounts).
 * Returns null only when the user explicitly cancels (presses Escape).
 */
async function promptForPassword(username?: string): Promise<string | null> {
    const promptLabel = username ? `Password for ${username}` : 'MySQL password';
    const pw = await vscode.window.showInputBox({
        title: 'SQL Debugger — MySQL Password',
        prompt: promptLabel,
        password: true,
        ignoreFocusOut: true,
        placeHolder: 'Leave blank for passwordless accounts',
    });
    // undefined means the user cancelled; empty string means no password — both are valid.
    if (pw === undefined) { return null; }
    return pw;
}

// ─── Editor database detection ─────────────────────────────────────────────

/**
 * Best-effort detection of the active database from the current editor context.
 * Returns undefined if nothing can be determined — caller keeps the stored DB.
 *
 * Detection order (first match wins):
 *  1. USE statement anywhere in the open file  (standard SQL)
 *  2. Annotation comment:  -- @db: world   or  -- database: world
 *  3. Safe API probe of any installed SQL extension
 *     — probes are wrapped in try/catch so a missing API shape is silently skipped
 */
async function detectDatabaseFromContext(
    sqlText: string,
    editor: vscode.TextEditor,
): Promise<string | undefined> {
    const detectedInSql = detectDatabaseNameInSql(sqlText);
    if (detectedInSql) { return detectedInSql; }
    const content = '';

    // 1. Standard SQL USE statement — e.g.  USE world;  or  USE `world`;
    const useMatch = content.match(/\bUSE\s+`?(\w+)`?\s*;/i);
    if (useMatch?.[1]) { return useMatch[1]; }

    // 2. Annotation comment — e.g.  -- @db: world   or  -- database: world
    const annotationMatch = content.match(/--\s*@?(?:db|database)\s*:\s*(\w+)/i);
    if (annotationMatch?.[1]) { return annotationMatch[1]; }

    // 3. Safe probe of known SQL extensions — each ext may expose a different surface
    const SQL_EXTENSION_IDS = [
        'cweijan.vscode-database-client2', // Database Client (Weijan Chen) — very common
        'cweijan.dbclient-jdbc',
        'mtxr.sqltools',                   // SQLTools
        'formulahendry.vscode-mysql',
    ];
    for (const extId of SQL_EXTENSION_IDS) {
        const ext = vscode.extensions.getExtension(extId);
        if (!ext?.isActive) { continue; }
        try {
            const api = ext.exports as Record<string, unknown>;
            // Try common API shapes across different extensions
            const db: string | undefined =
                (typeof api['activeDatabase']  === 'string' ? api['activeDatabase']  : undefined) ??
                (typeof api['currentDatabase'] === 'string' ? api['currentDatabase'] : undefined) ??
                ((api['activeConnection']  as Record<string,string> | undefined)?.['database']) ??
                ((api['currentConnection'] as Record<string,string> | undefined)?.['database']) ??
                (typeof api['getActiveDatabase'] === 'function'
                    ? String(await (api['getActiveDatabase'] as () => Promise<unknown>)())
                    : undefined);
            if (db && db.trim() && db !== 'undefined') { return db.trim(); }
        } catch { /* extension doesn't expose this shape — try next */ }
    }

    return undefined;
}

function shouldForgetCachedPassword(error: unknown): boolean {
    if (!(error instanceof Error)) {
        return false;
    }

    const mysqlError = error as Error & { code?: string };
    return mysqlError.code === 'ER_ACCESS_DENIED_ERROR' || /access denied/i.test(mysqlError.message);
}

/**
 * Prompt for a database name only — no server details asked.
 * Pre-fills with the currently active database name.
 */
async function promptForDatabaseName(
    context: vscode.ExtensionContext,
): Promise<string | null> {
    const current = getActiveDatabase(context);
    const db = await vscode.window.showInputBox({
        title: 'SQL Debugger — Switch Database',
        prompt: 'Database name',
        value: current ?? '',
        ignoreFocusOut: true,
        validateInput: v => (v.trim() ? null : 'Database name cannot be empty'),
    });
    if (db === undefined) { return null; }
    const trimmed = db.trim();
    return trimmed || null;
}
