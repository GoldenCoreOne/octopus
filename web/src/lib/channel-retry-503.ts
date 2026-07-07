// 503 自动重试的前端纯函数校验 / 解析。镜像 channel-concurrency.ts 的风格。
// 后端语义：retry_on_503 ∈ {0,1}（开关）；max_503_retries nil=默认12, 0=无限, >0=自定义上限。

export type ChannelRetryValidationCode = 'required' | 'non_negative_integer';

/**
 * 将后端的 max_503_retries 规范化为前端展示用的非负整数。
 * null/缺失/非有限/负数 → 0（语义为"无限"，与后端 0=无限一致）；
 * 正小数 → 截断为整数；0 → 0（无限）。
 */
export function normalizeChannelMax503Retries(value: number | null | undefined): number {
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
        return 0;
    }
    return Math.trunc(value);
}

/**
 * 校验 max_503_retries 的原始输入字符串。
 * 允许空串（表示"沿用默认"，提交时映射为 undefined）、0（无限）、正整数。
 */
export function validateChannelMax503RetriesInput(
    rawValue: string,
): ChannelRetryValidationCode | null {
    const normalized = rawValue.trim();
    if (normalized === '') {
        // 空串 = 沿用后端默认（12），合法。
        return null;
    }
    if (!/^\d+$/.test(normalized)) {
        return 'non_negative_integer';
    }
    return null;
}

/**
 * 解析提交时的 max_503_retries：
 * - retry 关闭时返回 undefined（不发送该字段，保留后端原值）；
 * - 空串 → undefined（沿用后端默认 12，不携带该 key）；
 * - 非负整数 → 数字值（0=无限，>0=自定义）；
 * - 非法输入 → 返回错误码。
 */
export function resolveChannelMax503Retries(
    retryEnabled: boolean,
    rawValue: string,
): { ok: true; value: number | undefined } | { ok: false; code: ChannelRetryValidationCode } {
    if (!retryEnabled) {
        return { ok: true, value: undefined };
    }

    const trimmed = rawValue.trim();
    if (trimmed === '') {
        // 沿用后端默认 12，不发送字段。
        return { ok: true, value: undefined };
    }

    const errorCode = validateChannelMax503RetriesInput(rawValue);
    if (errorCode) {
        return { ok: false, code: errorCode };
    }

    return { ok: true, value: Number.parseInt(trimmed, 10) };
}

/**
 * 把后端返回的 retry_on_503（0/1 或 null）映射为前端开关布尔值。
 * null/缺失 → false（未配置）。
 */
export function getChannelRetryOn503(retryOn503: number | null | undefined): boolean {
    return retryOn503 === 1;
}

/**
 * 把开关布尔值映射为提交给后端的 retry_on_503（0 或 1）。
 */
export function resolveChannelRetryOn503(retryEnabled: boolean): number {
    return retryEnabled ? 1 : 0;
}