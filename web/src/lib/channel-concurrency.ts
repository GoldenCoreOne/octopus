export type ChannelConcurrencyMode = 'unlimited' | 'limited';

export type ChannelConcurrencyValidationCode = 'required' | 'positive_integer';

export function normalizeChannelMaxConcurrency(value: number | null | undefined): number {
    if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
        return 0;
    }

    return Math.trunc(value);
}

export function getChannelConcurrencyMode(
    maxConcurrency: number | null | undefined,
): ChannelConcurrencyMode {
    return normalizeChannelMaxConcurrency(maxConcurrency) > 0 ? 'limited' : 'unlimited';
}

export function validateLimitedChannelConcurrencyInput(
    rawValue: string,
): ChannelConcurrencyValidationCode | null {
    const normalized = rawValue.trim();
    if (normalized === '') {
        return 'required';
    }
    if (!/^[1-9]\d*$/.test(normalized)) {
        return 'positive_integer';
    }
    return null;
}

export function resolveChannelMaxConcurrency(
    mode: ChannelConcurrencyMode,
    rawValue: string,
): { ok: true; value: number } | { ok: false; code: ChannelConcurrencyValidationCode } {
    if (mode === 'unlimited') {
        return { ok: true, value: 0 };
    }

    const errorCode = validateLimitedChannelConcurrencyInput(rawValue);
    if (errorCode) {
        return { ok: false, code: errorCode };
    }

    return { ok: true, value: Number.parseInt(rawValue.trim(), 10) };
}
