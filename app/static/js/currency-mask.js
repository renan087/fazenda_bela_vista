(function () {
    const centsDigitsFromRaw = (rawValue) => {
        const normalized = (rawValue || '').toString().replace(/[^\d]/g, '');
        return normalized ? normalized.replace(/^0+(?=\d)/, '') || '0' : '0';
    };

    const isNegativeRawCurrency = (rawValue) => String(rawValue || '').trim().startsWith('-');

    const formatCurrencyDisplayFromDigits = (digits, isNegative = false) => {
        const normalizedDigits = (digits || '0').replace(/[^\d]/g, '') || '0';
        const padded = normalizedDigits.padStart(3, '0');
        const cents = padded.slice(-2);
        const integerDigits = padded.slice(0, -2) || '0';
        const integerFormatted = Number(integerDigits).toLocaleString('pt-BR');
        return `${isNegative ? '-' : ''}R$ ${integerFormatted},${cents}`;
    };

    const rawFromCurrencyDigits = (digits, isNegative = false) => {
        const normalizedDigits = (digits || '0').replace(/[^\d]/g, '') || '0';
        const padded = normalizedDigits.padStart(3, '0');
        return `${isNegative ? '-' : ''}${String(Number(padded.slice(0, -2) || '0'))}.${padded.slice(-2)}`;
    };

    const notifyRawInputChanged = (rawInput) => {
        rawInput.dispatchEvent(new Event('input', { bubbles: true }));
        rawInput.dispatchEvent(new Event('change', { bubbles: true }));
    };

    const attachCurrencyMask = (displayInput, rawInput, options = {}) => {
        if (!displayInput || !rawInput || displayInput.dataset.currencyMaskBound === 'true') return null;
        displayInput.dataset.currencyMaskBound = 'true';
        const allowNegative = options.allowNegative === true;
        const minCents = displayInput.hasAttribute('data-currency-min-cents')
            ? Number(displayInput.dataset.currencyMinCents || '0')
            : Number.NaN;
        let currencyDigits = centsDigitsFromRaw(rawInput.value || displayInput.value || '0.00');
        let currencyNegative = allowNegative && isNegativeRawCurrency(rawInput.value || displayInput.value || '0.00');

        const syncCurrencyFields = (notify = true) => {
            displayInput.value = formatCurrencyDisplayFromDigits(currencyDigits, currencyNegative);
            rawInput.value = rawFromCurrencyDigits(currencyDigits, currencyNegative);
            if (Number.isFinite(minCents) && Number(currencyDigits || 0) < minCents) {
                displayInput.setCustomValidity(`Informe um valor mínimo de ${formatCurrencyDisplayFromDigits(String(minCents), false)}.`);
            } else {
                displayInput.setCustomValidity('');
            }
            if (notify) notifyRawInputChanged(rawInput);
        };

        syncCurrencyFields(false);

        displayInput.addEventListener('beforeinput', (event) => {
            const inputType = event.inputType || '';
            if (inputType === 'deleteContentBackward') {
                event.preventDefault();
                currencyDigits = currencyDigits.length > 1 ? currencyDigits.slice(0, -1) : '0';
                if (currencyDigits === '0') currencyNegative = false;
                syncCurrencyFields();
                return;
            }
            if (inputType === 'deleteContentForward') {
                event.preventDefault();
                currencyDigits = '0';
                currencyNegative = false;
                syncCurrencyFields();
                return;
            }
            if (inputType.startsWith('insert')) {
                if (allowNegative && (event.data || '') === '-' && currencyDigits === '0') {
                    event.preventDefault();
                    currencyNegative = true;
                    syncCurrencyFields();
                    return;
                }
                const inserted = (event.data || '').replace(/[^\d]/g, '');
                if (!inserted) {
                    event.preventDefault();
                    return;
                }
                event.preventDefault();
                currencyDigits = `${currencyDigits === '0' ? '' : currencyDigits}${inserted}`.replace(/^0+(?=\d)/, '') || '0';
                syncCurrencyFields();
            }
        });

        displayInput.addEventListener('keydown', (event) => {
            const allowedKeys = ['Tab', 'ArrowLeft', 'ArrowRight', 'Home', 'End'];
            if (allowedKeys.includes(event.key) || event.key === 'Backspace') return;
            if (allowNegative && event.key === '-' && currencyDigits === '0') return;
            if (/^\d$/.test(event.key)) return;
            event.preventDefault();
        });

        displayInput.addEventListener('focus', () => {
            requestAnimationFrame(() => {
                displayInput.setSelectionRange(displayInput.value.length, displayInput.value.length);
            });
        });

        displayInput.addEventListener('paste', (event) => {
            event.preventDefault();
            const pastedText = event.clipboardData?.getData('text') || '';
            if (allowNegative && pastedText.includes('-') && currencyDigits === '0') {
                currencyNegative = true;
            }
            const pasted = pastedText.replace(/[^\d]/g, '');
            if (pasted) {
                currencyDigits = `${currencyDigits === '0' ? '' : currencyDigits}${pasted}`.replace(/^0+(?=\d)/, '') || '0';
            }
            syncCurrencyFields();
        });

        displayInput.form?.addEventListener('reset', () => {
            window.setTimeout(() => {
                currencyDigits = centsDigitsFromRaw(rawInput.value || '0.00');
                currencyNegative = allowNegative && isNegativeRawCurrency(rawInput.value || '0.00');
                syncCurrencyFields();
            }, 0);
        });

        return {
            reset(rawValue = '0.00') {
                currencyDigits = centsDigitsFromRaw(rawValue);
                currencyNegative = allowNegative && isNegativeRawCurrency(rawValue);
                syncCurrencyFields();
            },
        };
    };

    const bootCurrencyMasks = (root = document) => {
        root.querySelectorAll('[data-currency-display]').forEach((displayInput) => {
            const targetSelector = displayInput.getAttribute('data-currency-target');
            const rawInput = targetSelector
                ? root.querySelector(targetSelector) || document.querySelector(targetSelector)
                : displayInput.closest('label, form, div')?.querySelector('[data-currency-raw]');
            attachCurrencyMask(displayInput, rawInput, {
                allowNegative: displayInput.dataset.currencyAllowNegative === 'true',
            });
        });
    };

    window.SisfarmCurrencyMask = {
        attach: attachCurrencyMask,
        boot: bootCurrencyMasks,
        formatFromRaw(rawValue, allowNegative = false) {
            return formatCurrencyDisplayFromDigits(centsDigitsFromRaw(rawValue), allowNegative && isNegativeRawCurrency(rawValue));
        },
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => bootCurrencyMasks());
    } else {
        bootCurrencyMasks();
    }
})();
