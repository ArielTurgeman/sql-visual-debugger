import type { ServerConnection } from '../types';

export function isLocalMysqlHost(host: string): boolean {
    const normalized = host.trim().toLowerCase();
    return normalized === 'localhost' || normalized === '127.0.0.1' || normalized === '::1';
}

export function getLocalOnlyHostError(host: string): string | null {
    if (isLocalMysqlHost(host)) {
        return null;
    }

    return (
        'SQL Debugger v1 supports local MySQL connections only.\n' +
        'Use localhost, 127.0.0.1, or ::1.'
    );
}

export function assertLocalOnlyServer(server: ServerConnection): void {
    const error = getLocalOnlyHostError(server.host);
    if (error) {
        throw new Error(error);
    }
}
