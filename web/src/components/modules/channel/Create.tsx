import { useState } from 'react';
import {
    MorphingDialogClose,
    MorphingDialogTitle,
    MorphingDialogDescription,
    useMorphingDialog,
} from '@/components/ui/morphing-dialog';
import { useCreateChannel, ChannelType, AutoGroupType } from '@/api/endpoints/channel';
import { useTranslations } from 'next-intl';
import { toast } from '@/components/common/Toast';
import { resolveChannelMaxConcurrency } from '@/lib/channel-concurrency';
import { resolveChannelMax503Retries, resolveChannelRetryOn503 } from '@/lib/channel-retry-503';
import { ChannelForm, type ChannelFormData } from './Form';

const DEFAULT_FORM_DATA: ChannelFormData = {
    name: '',
    type: ChannelType.OpenAIChat,
    base_urls: [{ url: '', delay: 0 }],
    custom_header: [],
    channel_proxy: '',
    param_override: '',
    keys: [{ enabled: true, channel_key: '', remark: '' }],
    model: '',
    custom_model: '',
    auto_sync: false,
    auto_group: AutoGroupType.None,
    enabled: true,
    proxy: false,
    match_regex: '',
    concurrency_mode: 'unlimited',
    max_concurrency: '',
    retry_on_503: false,
    max_503_retries: '',
};

export function CreateDialogContent() {
    const { setIsOpen } = useMorphingDialog();
    const createChannel = useCreateChannel();
    const [formData, setFormData] = useState<ChannelFormData>(DEFAULT_FORM_DATA);
    const t = useTranslations('channel.create');

    const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const normalizedBaseUrls = (formData.base_urls ?? []).filter((u) => u.url.trim()).map((u) => ({
            url: u.url.trim(),
            delay: Number(u.delay || 0),
        }));
        const normalizedKeys = formData.keys
            .filter((k) => k.channel_key.trim())
            .map((k) => ({ enabled: k.enabled, channel_key: k.channel_key, remark: k.remark ?? '' }));
        const normalizedHeaders = (formData.custom_header ?? [])
            .map((h) => ({ header_key: h.header_key.trim(), header_value: h.header_value }))
            .filter((h) => h.header_key && h.header_value !== '');

        const channelProxy = formData.channel_proxy.trim();
        const paramOverride = formData.param_override.trim();

        const resolvedConcurrency = resolveChannelMaxConcurrency(formData.concurrency_mode, formData.max_concurrency);
        if (!resolvedConcurrency.ok) {
            toast.error(t('maxConcurrencyValidationTitle'), {
                description: t(`concurrencyErrors.${resolvedConcurrency.code}`),
            });
            return;
        }

        const resolvedMax503 = resolveChannelMax503Retries(formData.retry_on_503, formData.max_503_retries);
        if (!resolvedMax503.ok) {
            toast.error(t('max503RetriesValidationTitle'), {
                description: t(`retryErrors.${resolvedMax503.code}`),
            });
            return;
        }

        createChannel.mutate(
            {
                name: formData.name,
                type: formData.type,
                enabled: formData.enabled,
                base_urls: normalizedBaseUrls,
                keys: normalizedKeys,
                model: formData.model,
                custom_model: formData.custom_model,
                proxy: formData.proxy,
                auto_sync: formData.auto_sync,
                auto_group: formData.auto_group,
                custom_header: normalizedHeaders,
                channel_proxy: channelProxy,
                param_override: paramOverride,
                match_regex: formData.match_regex.trim(),
                max_concurrency: resolvedConcurrency.value,
                retry_on_503: resolveChannelRetryOn503(formData.retry_on_503),
                ...(resolvedMax503.value !== undefined ? { max_503_retries: resolvedMax503.value } : {}),
            },
            {
                onSuccess: () => {
                    setFormData(DEFAULT_FORM_DATA);
                    setIsOpen(false);
                }
            });
    };

    return (
        <div className="w-screen max-w-full md:max-w-xl h-full min-h-0 flex flex-col">
            <MorphingDialogTitle className="shrink-0">
                <header className="mb-6 flex items-center justify-between">
                    <h2 className="text-2xl font-bold text-card-foreground">{t('dialogTitle')}</h2>
                    <MorphingDialogClose
                        className="relative right-0 top-0"
                        variants={{
                            initial: { opacity: 0, scale: 0.8 },
                            animate: { opacity: 1, scale: 1 },
                            exit: { opacity: 0, scale: 0.8 }
                        }}
                    />
                </header>
            </MorphingDialogTitle>
            <MorphingDialogDescription disableLayoutAnimation className="flex-1 min-h-0 overflow-auto">
                <ChannelForm
                    formData={formData}
                    onFormDataChange={setFormData}
                    onSubmit={handleSubmit}
                    isPending={createChannel.isPending}
                    submitText={t('submit')}
                    pendingText={t('submitting')}
                    idPrefix="new-channel"
                />
            </MorphingDialogDescription>
        </div>
    );
}
