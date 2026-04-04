/**
 * Busca + presets de período: futuro (próximos N dias, próximo mês) ou passado
 * (últimos N dias, mês passado), além de personalizado. Form: data-module-period-filter-root
 * e data-module-range-default (ex.: next_10_days | last_10_days).
 */
(function (global) {
    const stateByForm = new WeakMap();
    let docClickAttached = false;

    const formatIsoDate = (value) => value.toISOString().slice(0, 10);

    const formatShortDatePtBr = (value) => {
        const months = ['jan.', 'fev.', 'mar.', 'abr.', 'mai.', 'jun.', 'jul.', 'ago.', 'set.', 'out.', 'nov.', 'dez.'];
        return `${value.getDate()} ${months[value.getMonth()]}`;
    };

    const getPresetRange = (preset) => {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        if (preset === 'last_10_days') {
            const start = new Date(today);
            start.setDate(start.getDate() - 10);
            return { start: formatIsoDate(start), end: formatIsoDate(today) };
        }
        if (preset === 'last_20_days') {
            const start = new Date(today);
            start.setDate(start.getDate() - 20);
            return { start: formatIsoDate(start), end: formatIsoDate(today) };
        }
        if (preset === 'last_month') {
            const start = new Date(today.getFullYear(), today.getMonth() - 1, 1);
            const end = new Date(today.getFullYear(), today.getMonth(), 0);
            return { start: formatIsoDate(start), end: formatIsoDate(end) };
        }
        if (preset === 'next_20_days') {
            const end = new Date(today);
            end.setDate(end.getDate() + 20);
            return { start: formatIsoDate(today), end: formatIsoDate(end) };
        }
        if (preset === 'next_month') {
            const start = new Date(today.getFullYear(), today.getMonth() + 1, 1);
            const end = new Date(today.getFullYear(), today.getMonth() + 2, 0);
            return { start: formatIsoDate(start), end: formatIsoDate(end) };
        }
        if (preset === 'next_10_days') {
            const end = new Date(today);
            end.setDate(end.getDate() + 10);
            return { start: formatIsoDate(today), end: formatIsoDate(end) };
        }
        if (String(preset || '').startsWith('last_')) {
            const start = new Date(today);
            start.setDate(start.getDate() - 10);
            return { start: formatIsoDate(start), end: formatIsoDate(today) };
        }
        const end = new Date(today);
        end.setDate(end.getDate() + 10);
        return { start: formatIsoDate(today), end: formatIsoDate(end) };
    };

    const getPresetRangeLabel = (preset) => {
        if (preset === 'custom') return 'Selecione data inicial e final';
        const range = getPresetRange(preset);
        const start = new Date(`${range.start}T00:00:00`);
        const end = new Date(`${range.end}T00:00:00`);
        return `${formatShortDatePtBr(start)} a ${formatShortDatePtBr(end)}`;
    };

    const getPresetRangeTitle = (preset) => {
        if (preset === 'last_20_days') return 'Últimos 20 dias';
        if (preset === 'last_month') return 'Mês passado';
        if (preset === 'last_10_days') return 'Últimos 10 dias';
        if (preset === 'next_20_days') return 'Próximos 20 dias';
        if (preset === 'next_month') return 'Próximo mês';
        if (preset === 'custom') return 'Período personalizado';
        return 'Próximos 10 dias';
    };

    function clickHappenedInsidePicker(event, picker) {
        if (!(event.target instanceof Node)) return false;
        if (picker.contains(event.target)) return true;
        const eventPath = typeof event.composedPath === 'function' ? event.composedPath() : [];
        return Array.isArray(eventPath) && eventPath.includes(picker);
    }

    function attachDocumentClose() {
        if (docClickAttached) return;
        docClickAttached = true;
        document.addEventListener(
            'click',
            (event) => {
        document.querySelectorAll('[data-module-period-filter-root]').forEach((root) => {
                    const st = stateByForm.get(root);
                    if (!st) return;
                    if (clickHappenedInsidePicker(event, st.picker)) return;
                    st.handleOutsideClick();
                });
            },
            true,
        );
    }

    function bindModulePeriodFilter(root, options) {
        if (!(root instanceof HTMLElement)) return;
        if (!root.hasAttribute('data-module-period-filter-root')) return;
        if (root.dataset.modulePeriodFilterBound === '1') return;
        root.dataset.modulePeriodFilterBound = '1';
        const ownerForm = root instanceof HTMLFormElement ? root : root.closest('form');
        if (!(ownerForm instanceof HTMLFormElement)) return;

        const picker = root.querySelector('[data-module-range-picker]');
        const fadePanel = picker?.closest('[data-stock-fade-panel]');
        const trigger = picker?.querySelector('[data-module-range-trigger]');
        const menu = picker?.querySelector('[data-module-range-menu]');
        const label = picker?.querySelector('[data-module-range-label]');
        const customRange = root.querySelector('[data-module-custom-range]');
        const calendarTitle = root.querySelector('[data-module-calendar-title]');
        const calendarGrid = root.querySelector('[data-module-calendar-grid]');
        const calendarPrev = root.querySelector('[data-module-calendar-prev]');
        const calendarNext = root.querySelector('[data-module-calendar-next]');
        const rangeInput = root.querySelector('[data-module-range-input]');
        const filterStartInput = root.querySelector('[data-module-filter-start]');
        const filterEndInput = root.querySelector('[data-module-filter-end]');
        const filterClearButton = root.querySelector('[data-module-filter-clear]');

        if (
            !(picker instanceof HTMLElement) ||
            !(trigger instanceof HTMLButtonElement) ||
            !(menu instanceof HTMLElement) ||
            !(label instanceof HTMLElement) ||
            !(rangeInput instanceof HTMLInputElement)
        ) {
            return;
        }

        const defaultPreset = (root.dataset.moduleRangeDefault || 'next_10_days').trim() || 'next_10_days';
        const placeholderLabel = (root.dataset.moduleRangePlaceholder || 'Filtre por período').trim() || 'Filtre por período';
        const customAutoApply = root.dataset.moduleCustomAutoApply !== 'false';

        const monthNames = [
            'Janeiro',
            'Fevereiro',
            'Março',
            'Abril',
            'Maio',
            'Junho',
            'Julho',
            'Agosto',
            'Setembro',
            'Outubro',
            'Novembro',
            'Dezembro',
        ];

        let calendarMonth = (() => {
            const seed = (filterStartInput?.value || filterEndInput?.value || formatIsoDate(new Date()));
            const parsed = new Date(`${seed}T00:00:00`);
            return new Date(parsed.getFullYear(), parsed.getMonth(), 1);
        })();

        let pendingRangeStart = (filterStartInput?.value || '').trim();
        let customRangeOpen = false;
        let customRangeSubmitTimer = null;

        const compareIsoDate = (left, right) => {
            if (!left || !right) return 0;
            return left.localeCompare(right);
        };

        const syncRangeLabel = () => {
            const s = (filterStartInput?.value || '').trim();
            const e = (filterEndInput?.value || '').trim();
            const isCustom = rangeInput.value === 'custom';
            if (!s && !e && !isCustom && !(rangeInput.value || '').trim()) {
                label.textContent = placeholderLabel;
                return;
            }
            if (isCustom) {
                label.textContent = getPresetRangeTitle('custom');
                return;
            }
            label.textContent = getPresetRangeTitle(rangeInput.value || defaultPreset);
        };

        const syncFilterActions = () => {
            const hasDateFilter = Boolean((filterStartInput?.value || '').trim() || (filterEndInput?.value || '').trim());
            filterClearButton?.classList.toggle('hidden', !hasDateFilter);
            syncRangeLabel();
            if (typeof options.onSync === 'function') options.onSync();
        };

        const syncCustomVisibility = () => {
            const shouldShowCustomRange = rangeInput.value === 'custom' && customRangeOpen;
            customRange?.classList.toggle('hidden', !shouldShowCustomRange);
            syncFilterActions();
        };

        const renderCalendar = () => {
            if (!(calendarGrid instanceof HTMLElement) || !(calendarTitle instanceof HTMLElement)) return;
            calendarTitle.textContent = `${monthNames[calendarMonth.getMonth()]} ${calendarMonth.getFullYear()}`;
            const monthStart = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth(), 1);
            const monthEnd = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth() + 1, 0);
            const startOffset = monthStart.getDay();
            const totalCells = Math.ceil((startOffset + monthEnd.getDate()) / 7) * 7;
            const selectedStart = pendingRangeStart || (filterStartInput?.value || '').trim();
            const selectedEnd = (filterEndInput?.value || '').trim();
            calendarGrid.innerHTML = '';
            for (let index = 0; index < totalCells; index += 1) {
                const dayNumber = index - startOffset + 1;
                const cellDate = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth(), dayNumber);
                const iso = formatIsoDate(cellDate);
                const button = document.createElement('button');
                button.type = 'button';
                button.className = 'module-filter-calendar-day';
                button.textContent = String(cellDate.getDate());
                button.dataset.dateValue = iso;
                if (cellDate.getMonth() !== calendarMonth.getMonth()) button.classList.add('is-outside');
                const isStart = selectedStart && compareIsoDate(iso, selectedStart) === 0;
                const isEnd = selectedEnd && compareIsoDate(iso, selectedEnd) === 0;
                const inRange =
                    selectedStart && selectedEnd && compareIsoDate(iso, selectedStart) >= 0 && compareIsoDate(iso, selectedEnd) <= 0;
                if (inRange) button.classList.add('is-in-range');
                if (isStart) button.classList.add('is-range-start');
                if (isEnd) button.classList.add('is-range-end');
                if (isStart || isEnd) button.classList.add('is-selected');
                button.addEventListener('click', (event) => {
                    event.stopPropagation();
                    const currentStart = pendingRangeStart || (filterStartInput?.value || '').trim();
                    const currentEnd = (filterEndInput?.value || '').trim();
                    if (!currentStart || currentEnd) {
                        pendingRangeStart = iso;
                        if (filterStartInput instanceof HTMLInputElement) filterStartInput.value = iso;
                        if (filterEndInput instanceof HTMLInputElement) filterEndInput.value = '';
                        syncCustomVisibility();
                        renderCalendar();
                        return;
                    }
                    const nextStart = compareIsoDate(iso, currentStart) < 0 ? iso : currentStart;
                    const nextEnd = compareIsoDate(iso, currentStart) < 0 ? currentStart : iso;
                    pendingRangeStart = '';
                    if (filterStartInput instanceof HTMLInputElement) filterStartInput.value = nextStart;
                    if (filterEndInput instanceof HTMLInputElement) filterEndInput.value = nextEnd;
                    syncCustomVisibility();
                    renderCalendar();
                    if (customRangeSubmitTimer) {
                        window.clearTimeout(customRangeSubmitTimer);
                    }
                    customRangeSubmitTimer = window.setTimeout(() => {
                        customRangeOpen = false;
                        customRange?.classList.add('hidden');
                        closeMenu();
                        if (customAutoApply) {
                            ownerForm.requestSubmit();
                        }
                    }, 1000);
                });
                calendarGrid.appendChild(button);
            }
        };

        menu.querySelectorAll('[data-module-range-meta]').forEach((meta) => {
            const preset = meta.getAttribute('data-module-range-meta') || '';
            meta.textContent = getPresetRangeLabel(preset);
        });

        const closeMenu = () => {
            menu.classList.add('hidden');
            trigger.setAttribute('aria-expanded', 'false');
            fadePanel?.classList.remove('is-filter-menu-open');
        };

        const openMenu = () => {
            menu.classList.remove('hidden');
            trigger.setAttribute('aria-expanded', 'true');
            fadePanel?.classList.add('is-filter-menu-open');
        };

        const openCustomRange = () => {
            customRangeOpen = true;
            customRange?.classList.remove('hidden');
            trigger.setAttribute('aria-expanded', 'true');
            fadePanel?.classList.add('is-filter-menu-open');
            renderCalendar();
        };

        const handleOutsideClick = () => {
            if (customRangeSubmitTimer) {
                window.clearTimeout(customRangeSubmitTimer);
                customRangeSubmitTimer = null;
            }
            customRangeOpen = false;
            customRange?.classList.add('hidden');
            closeMenu();
        };

        trigger.addEventListener('click', (event) => {
            event.stopPropagation();
            const customOpen = customRangeOpen && customRange instanceof HTMLElement && !customRange.classList.contains('hidden');
            if (customOpen) {
                if (customRangeSubmitTimer) {
                    window.clearTimeout(customRangeSubmitTimer);
                    customRangeSubmitTimer = null;
                }
                customRangeOpen = false;
                customRange.classList.add('hidden');
                closeMenu();
                return;
            }
            if (rangeInput.value === 'custom') {
                openCustomRange();
                return;
            }
            if (menu.classList.contains('hidden')) openMenu();
            else closeMenu();
        });

        menu.querySelectorAll('[data-module-range-option]').forEach((option) => {
            option.addEventListener('click', (event) => {
                event.stopPropagation();
                const value = option.getAttribute('data-range-value') || defaultPreset;
                rangeInput.value = value;
                menu.querySelectorAll('.module-filter-preset-option').forEach((item) => item.classList.remove('is-active'));
                option.classList.add('is-active');
                if (value !== 'custom') {
                    const range = getPresetRange(value);
                    if (filterStartInput instanceof HTMLInputElement) filterStartInput.value = range.start;
                    if (filterEndInput instanceof HTMLInputElement) filterEndInput.value = range.end;
                } else {
                    pendingRangeStart = '';
                    if (filterStartInput instanceof HTMLInputElement) filterStartInput.value = '';
                    if (filterEndInput instanceof HTMLInputElement) filterEndInput.value = '';
                    calendarMonth = new Date();
                    calendarMonth = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth(), 1);
                }
                syncCustomVisibility();
                closeMenu();
                if (value !== 'custom') {
                    form.requestSubmit();
                } else {
                    openCustomRange();
                }
            });
        });

        ownerForm.addEventListener('submit', () => {
            if (!(rangeInput.value || '').trim()) return;
            if (rangeInput.value === 'custom') return;
            const range = getPresetRange(rangeInput.value || defaultPreset);
            if (filterStartInput instanceof HTMLInputElement) filterStartInput.value = range.start;
            if (filterEndInput instanceof HTMLInputElement) filterEndInput.value = range.end;
        });

        calendarPrev?.addEventListener('click', (event) => {
            event.stopPropagation();
            calendarMonth = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth() - 1, 1);
            renderCalendar();
        });

        calendarNext?.addEventListener('click', (event) => {
            event.stopPropagation();
            calendarMonth = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth() + 1, 1);
            renderCalendar();
        });

        stateByForm.set(root, {
            picker,
            handleOutsideClick,
        });

        attachDocumentClose();

        syncCustomVisibility();
        renderCalendar();
    }

    global.ModulePeriodFilters = {
        bind(form, options) {
            bindModulePeriodFilter(form, options || {});
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
