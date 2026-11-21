(() => {
    const root = document.querySelector('[data-ft-schedule-page]');
    const dataEl = document.getElementById('ft-schedule-data');
    if (!root || !dataEl) return;

    let DATA;
    try { DATA = JSON.parse(dataEl.textContent || '{}'); } catch { return; }

    const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
    const fmtNum = (n, dec = 1) => Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
    const fmtQty = (n) => {
        const v = Number(n || 0);
        return Number.isInteger(v) ? v.toLocaleString('pt-BR') : v.toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 2 });
    };
    const fmtBRL = (n) => 'R$ ' + Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const fmtDate = (iso) => {
        if (!iso || iso.length < 10) return '—';
        const [y, m, d] = iso.slice(0, 10).split('-');
        return `${d}/${m}/${y}`;
    };
    const fmtDateShort = (iso) => {
        if (!iso || iso.length < 10) return '—';
        const [, m, d] = iso.slice(0, 10).split('-');
        return `${d}/${m}`;
    };
    const addDays = (iso, delta) => {
        const dt = new Date(iso + 'T12:00:00');
        dt.setDate(dt.getDate() + delta);
        return dt.toISOString().slice(0, 10);
    };
    const daysDiff = (isoA, isoB) => {
        const a = new Date(isoA + 'T12:00:00');
        const b = new Date(isoB + 'T12:00:00');
        return Math.round((a - b) / 86400000);
    };
    const fmtDayHeader = (iso) => {
        const days = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado'];
        const months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
        const dt = new Date(iso + 'T12:00:00');
        return { main: `${dt.getDate()} ${months[dt.getMonth()]}`, sub: days[dt.getDay()] };
    };

    const TODAY = DATA.today || new Date().toISOString().slice(0, 10);
    const CAN_WRITE = Boolean(DATA.canWrite);
    const WEATHER = DATA.weather || {};

    const DAYS_PER_PAGE = 5;

    const state = {
        view: 'list',
        filter: 'all',
        methodFilter: 'all',
        search: '',
        selectedIds: new Set(),
        collapsedDates: {},
        compCollapsedDates: {},
        schedPage: 1,
        compPage: 1,
        openPopover: null, // { type: 'day'|'item'|'menu'|'comp_menu', key }
        calYear: Number(TODAY.slice(0, 4)),
        calMonth: Number(TODAY.slice(5, 7)) - 1, // 0-indexed
        reschedIds: [],
        reschedMode: 'date',
        reschedShiftDays: 2,
        execDraft: null,
    };

    function paginateDateGroups(groups, rawPage, getCount) {
        const totalGroups = groups.length;
        const totalItems = groups.reduce((acc, g) => acc + getCount(g), 0);
        const totalPages = Math.max(1, Math.ceil(totalGroups / DAYS_PER_PAGE));
        const page = Math.min(Math.max(1, Number(rawPage) || 1), totalPages);
        const startIdx = (page - 1) * DAYS_PER_PAGE;
        const slice = groups.slice(startIdx, startIdx + DAYS_PER_PAGE);
        const shownItems = slice.reduce((acc, g) => acc + getCount(g), 0);
        return { page, totalPages, totalGroups, totalItems, slice, startIdx, shownItems };
    }

    function renderPaginationBar(pg, attrName, itemLabel) {
        if (pg.totalPages <= 1) return '';
        const pages = [];
        for (let p = 1; p <= pg.totalPages; p++) {
            if (p === 1 || p === pg.totalPages || Math.abs(p - pg.page) <= 1) pages.push(p);
            else if (pages[pages.length - 1] !== '…') pages.push('…');
        }
        const fromDay = pg.startIdx + 1;
        const toDay = pg.startIdx + pg.slice.length;
        return `
            <nav class="ft-pag" aria-label="Paginação da listagem">
                <div class="ft-pag-info">
                    Mostrando <strong>${fromDay}–${toDay}</strong> de <strong>${pg.totalGroups} datas</strong>
                    <span class="ft-hide-xs"> · ${pg.shownItems} de ${pg.totalItems} ${esc(itemLabel)}</span>
                </div>
                <div class="ft-pag-nav">
                    <button type="button" class="ft-pag-btn" ${attrName}="${pg.page - 1}" ${pg.page <= 1 ? 'disabled' : ''}>← Anterior</button>
                    ${pages.map((p) => p === '…'
                        ? '<span class="ft-pag-dots" aria-hidden="true">…</span>'
                        : `<button type="button" class="ft-pag-btn ${p === pg.page ? 'is-active' : ''}" ${attrName}="${p}" ${p === pg.page ? 'aria-current="page"' : ''}>${p}</button>`
                    ).join('')}
                    <button type="button" class="ft-pag-btn" ${attrName}="${pg.page + 1}" ${pg.page >= pg.totalPages ? 'disabled' : ''}>Próxima →</button>
                </div>
            </nav>
        `;
    }

    const IC = {
        check: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" aria-hidden="true"><path d="M20 6 9 17l-5-5"/></svg>',
        cal: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><rect x="3" y="4.5" width="18" height="16.5" rx="2.5"/><path d="M3 9.5h18M8 2.5v4M16 2.5v4"/></svg>',
        dots: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="12" cy="5" r="1.7"/><circle cx="12" cy="12" r="1.7"/><circle cx="12" cy="19" r="1.7"/></svg>',
        grip: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="9" cy="6" r="1.4"/><circle cx="15" cy="6" r="1.4"/><circle cx="9" cy="12" r="1.4"/><circle cx="15" cy="12" r="1.4"/><circle cx="9" cy="18" r="1.4"/><circle cx="15" cy="18" r="1.4"/></svg>',
        chev: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>',
        warn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="m10.3 3.9-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.7-3.1l-8-14a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4M12 17h.01"/></svg>',
        rain: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M7 15a4.5 4.5 0 0 1-.6-9 6 6 0 0 1 11.3 1.5A3.8 3.8 0 0 1 17 15H7z"/><path d="M8 18.5 7 21M12.5 18.5l-1 2.5M17 18.5 16 21"/></svg>',
        drop: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 2.69l5.66 5.66a8 8 0 1 1-11.31 0z"/></svg>',
    };

    const daysWrap = root.querySelector('[data-ft-days-wrap]');
    const calWrap = root.querySelector('[data-ft-cal-wrap]');
    const completedWrap = root.querySelector('[data-ft-completed-wrap]');
    const bulkBar = root.querySelector('[data-ft-bulk-bar]');
    const bulkCount = root.querySelector('[data-ft-bulk-count]');
    const activeFiltersRow = root.querySelector('[data-ft-active-filters]');
    const listTools = root.querySelector('[data-ft-list-tools]');
    const toggleAllBtn = root.querySelector('[data-ft-toggle-all-days]');

    const execDialog = document.getElementById('ft-exec-dialog');
    const reschedDialog = document.getElementById('ft-resched-dialog');
    const buylistDialog = document.getElementById('ft-buylist-dialog');
    const detailDialog = document.getElementById('ft-detail-dialog');
    const confirmDialog = document.getElementById('ft-confirm-dialog');

    const getSchedule = (id) =>
        (DATA.activeItems || []).find((x) => x.id === Number(id)) ||
        (DATA.completedItems || []).find((x) => x.id === Number(id));

    function filteredActive() {
        const q = state.search.trim().toLowerCase();
        return (DATA.activeItems || []).filter((s) => {
            if (state.filter === 'late' && !(s.scheduled_date && s.scheduled_date < TODAY)) return false;
            if (state.filter === 'today' && s.scheduled_date !== TODAY) return false;
            if (state.filter === 'shortage' && s.cumulative_ok) return false;
            if (state.methodFilter !== 'all' && s.method !== state.methodFilter) return false;
            if (q) {
                const hay = [
                    s.plot_name, s.plot_variety, s.recipe_name, s.responsible_name, s.notes,
                    s.method_label, ...(s.items || []).map((i) => i.name),
                ].join(' ').toLowerCase();
                if (!hay.includes(q)) return false;
            }
            return true;
        });
    }

    function filteredCompleted() {
        const q = state.search.trim().toLowerCase();
        return (DATA.completedItems || []).filter((s) => {
            if (!q) return true;
            const hay = [
                s.plot_name, s.recipe_name, s.responsible_name, s.notes, s.method_label,
                ...(s.items || []).map((i) => i.name),
            ].join(' ').toLowerCase();
            return hay.includes(q);
        });
    }

    function groupByExactDate(items) {
        const map = new Map();
        items.forEach((s) => {
            const k = s.scheduled_date || '';
            if (!map.has(k)) map.set(k, []);
            map.get(k).push(s);
        });
        return Array.from(map.keys()).sort().map((iso) => {
            const groupItems = map.get(iso);
            const hdr = iso ? fmtDayHeader(iso) : { main: 'Sem data', sub: '' };
            const diff = iso ? daysDiff(iso, TODAY) : 0;
            const weather = WEATHER[iso];
            const hasRainSensitive = groupItems.some((i) => i.method === 'fertilizacao_foliar' || i.method === 'adubacao_solida');
            const rainAlert = Boolean(weather && weather.risk === 'high' && hasRainSensitive);
            let dryCand = null;
            if (rainAlert && iso) {
                for (let off = 1; off <= 6; off++) {
                    const c = addDays(iso, off);
                    if (!WEATHER[c] || WEATHER[c].risk !== 'high') { dryCand = { iso: c, short: fmtDateShort(c) }; break; }
                }
                if (!dryCand) { const c = addDays(iso, 2); dryCand = { iso: c, short: fmtDateShort(c) }; }
            }
            return {
                date: iso,
                hdr,
                diff,
                isLate: diff < 0,
                isToday: diff === 0,
                isTomorrow: diff === 1,
                totalArea: groupItems.reduce((a, i) => a + Number(i.plot_area || 0), 0),
                methodsSummary: [...new Set(groupItems.map((i) => i.method_short))].join(', '),
                weather,
                rainAlert,
                dryCand,
                items: groupItems,
            };
        });
    }

    async function postDateBatch({ ids, mode = 'date', scheduledDate = '', shiftDays = 0 }) {
        const fd = new FormData();
        fd.append('csrf_token', DATA.csrf);
        fd.append('mode', mode);
        if (mode === 'shift') fd.append('shift_days', String(shiftDays));
        else fd.append('scheduled_date', scheduledDate);
        ids.forEach((id) => fd.append('schedule_ids', String(id)));
        try {
            const res = await fetch('/fertilizacao/agendamentos/alterar-data-lote', {
                method: 'POST',
                headers: { 'X-Requested-With': 'fetch', 'Accept': 'application/json' },
                body: fd,
            });
            if (res.ok) {
                window.location.reload();
                return;
            }
        } catch {
            // fallback abaixo
        }
        // fallback via formulário padrão
        const form = document.createElement('form');
        form.method = 'POST';
        form.action = '/fertilizacao/agendamentos/alterar-data-lote';
        const fields = [
            ['csrf_token', DATA.csrf],
            ['mode', mode],
            ['scheduled_date', scheduledDate],
            ['shift_days', String(shiftDays)],
            ...ids.map((id) => ['schedule_ids', String(id)]),
        ];
        fields.forEach(([k, v]) => {
            const inp = document.createElement('input');
            inp.type = 'hidden'; inp.name = k; inp.value = v;
            form.appendChild(inp);
        });
        document.body.appendChild(form);
        form.submit();
    }

    function renderQuickReschedPopover(ids, currentDate, titleLabel) {
        const opts = [
            { label: 'Hoje', iso: TODAY },
            { label: 'Amanhã', iso: addDays(TODAY, 1) },
            { label: 'Em 3 dias', iso: addDays(TODAY, 3) },
            { label: 'Em 7 dias', iso: addDays(TODAY, 7) },
        ].filter((o) => o.iso !== currentDate);

        return `
            <div class="ft-pop" data-ft-pop-box>
                <div class="ft-pop-title">${esc(titleLabel)}</div>
                <div class="ft-pop-list">
                    ${opts.map((o) => {
                        const w = WEATHER[o.iso];
                        const tag = w && w.risk === 'high'
                            ? `<span class="ft-pop-rain">🌧 ${fmtNum(w.rainMm, 0)}mm</span>`
                            : (w && w.rainMm === 0 ? '<span class="ft-pop-sun">☀️ Firme</span>' : `<span class="ft-pop-muted">${fmtDateShort(o.iso)}</span>`);
                        return `
                            <button type="button" class="ft-pop-item" data-ft-quick-move="${esc(o.iso)}" data-ft-ids="${ids.join(',')}">
                                <span><strong>${o.label}</strong> <small>${fmtDateShort(o.iso)}</small></span>
                                ${tag}
                            </button>
                        `;
                    }).join('')}
                </div>
                <div class="ft-pop-custom">
                    <label>Outra data</label>
                    <div class="ft-pop-custom-row">
                        <input type="date" value="${esc(addDays(currentDate || TODAY, 2))}" data-ft-pop-date>
                        <button type="button" class="sf-btn sf-btn-primary sf-btn-sm" data-ft-pop-custom-go data-ft-ids="${ids.join(',')}">OK</button>
                    </div>
                </div>
            </div>
        `;
    }

    function renderListView() {
        if (!daysWrap) return;
        const items = filteredActive();
        const groups = groupByExactDate(items);

        if (toggleAllBtn) {
            const allCol = groups.length > 0 && groups.every((g) => state.collapsedDates[g.date]);
            toggleAllBtn.textContent = allCol ? 'Expandir todos os dias' : 'Recolher dias';
            toggleAllBtn.dataset.mode = allCol ? 'expand' : 'collapse';
            toggleAllBtn.hidden = groups.length <= 1;
        }

        if (!groups.length) {
            const hasAny = (DATA.activeItems || []).length > 0;
            daysWrap.innerHTML = hasAny
                ? `<div class="pd-empty"><strong>Nenhum agendamento neste filtro.</strong><span>Limpe os filtros ou ajuste a busca para ver sua agenda.</span><button type="button" class="sf-btn sf-btn-secondary" data-ft-reset-filters>Limpar filtros</button></div>`
                : `<div class="pd-empty"><strong>Nenhum agendamento ativo cadastrado.</strong><span>Planeje as próximas adubações para monitorar estoque e executar em campo sem retrabalho.</span>${CAN_WRITE ? '<a class="sf-btn sf-btn-primary" href="/fertilizacao/agendamentos/novo">Novo agendamento</a>' : ''}</div>`;
            return;
        }

        const pg = paginateDateGroups(groups, state.schedPage, (g) => g.items.length);
        state.schedPage = pg.page;

        if (toggleAllBtn) {
            const allCol = pg.slice.length > 0 && pg.slice.every((g) => state.collapsedDates[g.date]);
            toggleAllBtn.textContent = allCol ? 'Expandir todos os dias' : 'Recolher dias';
            toggleAllBtn.dataset.mode = allCol ? 'expand' : 'collapse';
            toggleAllBtn.hidden = groups.length <= 1;
        }

        daysWrap.innerHTML = pg.slice.map((g) => {
            const collapsed = Boolean(state.collapsedDates[g.date]);
            const dayIds = g.items.map((i) => i.id);
            const popOpen = state.openPopover?.type === 'day' && state.openPopover.key === g.date;
            let relBadge = `<span class="pd-chip is-muted">em ${g.diff}d</span>`;
            if (g.isLate) relBadge = `<span class="pd-chip is-err">${Math.abs(g.diff)}d atrasado</span>`;
            else if (g.isToday) relBadge = '<span class="pd-chip is-warn">Hoje</span>';
            else if (g.isTomorrow) relBadge = '<span class="pd-chip is-brand">Amanhã</span>';

            return `
                <section class="ft-day ${g.isLate ? 'is-late' : ''} ${collapsed ? 'is-collapsed' : ''}" data-ft-day-zone="${esc(g.date)}">
                    <div class="ft-day-head">
                        <button type="button" class="ft-day-toggle" data-ft-toggle-day="${esc(g.date)}" aria-expanded="${!collapsed}">
                            <span class="ft-day-chev ${collapsed ? 'is-rot' : ''}">${IC.chev}</span>
                            <strong class="ft-day-main">${esc(g.hdr.main)}</strong>
                            <span class="ft-day-sub">${esc(g.hdr.sub)}</span>
                            ${relBadge}
                            <span class="ft-day-meta">· ${g.items.length} talh${g.items.length > 1 ? 'ões' : 'ão'} · ${fmtNum(g.totalArea, 1)} ha${collapsed ? ` · ${esc(g.methodsSummary)}` : ''}</span>
                            ${g.rainAlert ? `<span class="pd-chip is-info" title="${esc(g.weather.label)}">${IC.rain} Chuva ${fmtNum(g.weather.rainMm, 0)} mm</span>` : ''}
                        </button>
                        <div class="ft-day-actions">
                            ${CAN_WRITE && g.rainAlert && g.dryCand ? `
                                <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm ft-btn-rain" data-ft-quick-move="${esc(g.dryCand.iso)}" data-ft-ids="${dayIds.join(',')}">
                                    Adiar p/ ${esc(g.dryCand.short)} →
                                </button>
                            ` : ''}
                            ${CAN_WRITE ? `
                                <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm" data-ft-open-day-pop="${esc(g.date)}" title="Reagendar todos os itens desta data">
                                    ${IC.cal}<span class="ft-hide-xs">Reagendar dia</span>
                                </button>
                            ` : ''}
                            ${popOpen ? renderQuickReschedPopover(dayIds, g.date, g.items.length > 1 ? `Mover ${g.items.length} itens para` : 'Mover este dia para') : ''}
                        </div>
                    </div>
                    ${collapsed ? '' : `
                        <div class="ft-day-rows">
                            ${g.items.map((s) => renderRow(s, g)).join('')}
                        </div>
                    `}
                </section>
            `;
        }).join('') + renderPaginationBar(pg, 'data-ft-pag-sched', 'agendamentos');
    }

    function renderRow(s, dayGroup) {
        const checked = state.selectedIds.has(s.id);
        const urgent = dayGroup.isLate || dayGroup.isToday;
        const firstShort = !s.direct_ok ? s.direct_shortages[0] : (!s.cumulative_ok ? s.cumulative_shortages[0] : null);
        const isReschedPop = state.openPopover?.type === 'item' && state.openPopover.key === s.id;
        const isMenuPop = state.openPopover?.type === 'menu' && state.openPopover.key === s.id;

        return `
            <div class="ft-row ${checked ? 'is-selected' : ''}" ${CAN_WRITE ? `draggable="true" data-ft-drag="${s.id}"` : ''}>
                <div class="ft-row-left">
                    ${CAN_WRITE ? `<span class="ft-grip" title="Arraste para outra data">${IC.grip}</span>` : ''}
                    ${CAN_WRITE ? `<label class="dt-chk"><input type="checkbox" class="dt-check" ${checked ? 'checked' : ''} data-ft-check="${s.id}" aria-label="Selecionar ${esc(s.plot_name)}"></label>` : ''}
                    <div class="ft-row-info">
                        <div class="ft-row-title-line">
                            <button type="button" class="ft-row-title" data-ft-detail="${s.id}">${esc(s.plot_name)}</button>
                            <span class="ft-row-area">${fmtNum(s.plot_area, 1)} ha</span>
                        </div>
                        <div class="ft-row-sub">
                            <span class="ft-row-method"><i style="background:${esc(s.method_dot)}"></i>${esc(s.method_short)}${s.duration_minutes ? ` · ${s.duration_minutes} min` : ''}</span>
                            <span class="ft-sep">·</span>
                            <span class="ft-row-inputs">${esc(s.inputs_summary)}</span>
                        </div>
                    </div>
                </div>

                ${firstShort ? `
                    <button type="button" class="pd-chip ${!s.direct_ok ? 'is-err' : 'is-warn'} ft-row-alert" data-ft-open-buylist title="Ver lista de compras">
                        ${IC.warn}
                        <span>${!s.direct_ok ? `Falta ${fmtQty(firstShort.missing)} ${esc(firstShort.unit)}` : 'Risco no acumulado'}</span>
                    </button>
                ` : ''}

                <div class="ft-row-right">
                    ${s.est_cost > 0 ? `<span class="ft-row-cost">${fmtBRL(s.est_cost)}</span>` : ''}
                    ${CAN_WRITE ? `
                        <button type="button" class="sf-btn ${urgent ? 'sf-btn-primary' : 'sf-btn-secondary'} sf-btn-sm" data-ft-exec="${s.id}">
                            ${IC.check}<span>Executar</span>
                        </button>
                        <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm ft-ic-btn" data-ft-open-item-pop="${s.id}" aria-label="Reagendar" title="Reagendar rapidamente">
                            ${IC.cal}
                        </button>
                    ` : ''}
                    <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm ft-ic-btn" data-ft-open-menu="${s.id}" aria-label="Mais ações" title="Mais ações">
                        ${IC.dots}
                    </button>
                    ${isReschedPop ? renderQuickReschedPopover([s.id], s.scheduled_date, 'Mover agendamento para') : ''}
                    ${isMenuPop ? `
                        <div class="ft-pop is-menu" data-ft-pop-box>
                            <button type="button" class="ft-pop-item" data-ft-detail="${s.id}">Ver ficha completa</button>
                            ${CAN_WRITE ? `<a class="ft-pop-item" href="/fertilizacao/agendamentos/${s.id}/editar">Editar agendamento</a>` : ''}
                            ${CAN_WRITE ? `
                                <form method="post" action="/fertilizacao/agendamentos/${s.id}/duplicar">
                                    <input type="hidden" name="csrf_token" value="${esc(DATA.csrf)}">
                                    <button type="submit" class="ft-pop-item">Duplicar para +7 dias</button>
                                </form>
                                <div class="ft-pop-div"></div>
                                <button type="button" class="ft-pop-item is-danger" data-ft-confirm-delete="${s.id}">Excluir</button>
                            ` : ''}
                        </div>
                    ` : ''}
                </div>
            </div>
        `;
    }

    function renderCalendarView() {
        if (!calWrap) return;
        const items = filteredActive();
        const year = state.calYear;
        const month = state.calMonth;
        const firstDay = new Date(year, month, 1);
        const startWeekday = firstDay.getDay(); // 0=Dom
        const daysInMonth = new Date(year, month + 1, 0).getDate();
        const monthNames = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'];
        const cells = [];
        for (let i = 0; i < startWeekday; i++) cells.push({ empty: true });
        for (let d = 1; d <= daysInMonth; d++) {
            const iso = `${year}-${String(month + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
            cells.push({
                day: d,
                iso,
                isToday: iso === TODAY,
                weather: WEATHER[iso],
                items: items.filter((s) => s.scheduled_date === iso),
            });
        }
        while (cells.length % 7 !== 0) cells.push({ empty: true });

        calWrap.innerHTML = `
            <div class="ft-cal-head">
                <div class="ft-cal-nav">
                    <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm" data-ft-cal-nav="-1" aria-label="Mês anterior">←</button>
                    <h3>${monthNames[month]} de ${year}</h3>
                    <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm" data-ft-cal-nav="1" aria-label="Próximo mês">→</button>
                    <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm" data-ft-cal-today>Hoje</button>
                </div>
                <div class="ft-cal-legend">
                    <span><i style="background:#1F5FA8"></i>Fertirrigação</span>
                    <span><i style="background:#5B6660"></i>Sólida</span>
                    <span><i style="background:#446A36"></i>Foliar</span>
                </div>
            </div>
            <div class="ft-cal-scroll">
                <div class="ft-cal-inner">
                    <div class="ft-cal-weekdays">
                        ${['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'].map((w) => `<div>${w}</div>`).join('')}
                    </div>
                    <div class="ft-cal-grid">
                        ${cells.map((c) => {
                            if (c.empty) return '<div class="ft-cal-cell is-empty"></div>';
                            return `
                                <div class="ft-cal-cell ${c.isToday ? 'is-today' : ''}">
                                    <div class="ft-cal-cell-top">
                                        <span class="ft-cal-num ${c.isToday ? 'is-today' : ''}">${c.day}</span>
                                        <div class="ft-cal-cell-tools">
                                            ${c.weather && c.weather.risk === 'high' ? `<span class="ft-cal-rain" title="${esc(c.weather.label)}">🌧 ${fmtNum(c.weather.rainMm, 0)}mm</span>` : ''}
                                            ${CAN_WRITE ? `<a class="ft-cal-add" href="/fertilizacao/agendamentos/novo?date=${esc(c.iso)}" title="Agendar em ${fmtDate(c.iso)}">+</a>` : ''}
                                        </div>
                                    </div>
                                    <div class="ft-cal-items">
                                        ${c.items.map((s) => `
                                            <button type="button" class="ft-cal-ev ${s.scheduled_date < TODAY ? 'is-late' : ''}" data-ft-exec="${s.id}">
                                                <span class="ft-cal-ev-top">
                                                    <i style="background:${esc(s.method_dot)}"></i>
                                                    <strong>${esc(s.plot_name)}</strong>
                                                    ${!s.cumulative_ok ? '<span title="Alerta de estoque">⚠️</span>' : ''}
                                                </span>
                                                <small>${esc(s.method_short)}</small>
                                            </button>
                                        `).join('')}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            </div>
        `;
    }

    function renderCompletedView() {
        if (!completedWrap) return;
        const items = filteredCompleted();
        if (!items.length) {
            completedWrap.innerHTML = '<div class="pd-empty"><strong>Nenhum agendamento concluído neste filtro.</strong><span>Quando você executar um agendamento, ele aparece aqui agrupado por data e vinculado à aplicação em Fertilização.</span></div>';
            return;
        }

        const byDate = new Map();
        items.forEach((s) => {
            const dt = s.completed_date || s.scheduled_date || TODAY;
            if (!byDate.has(dt)) byDate.set(dt, []);
            byDate.get(dt).push(s);
        });

        const groups = [...byDate.entries()]
            .sort((a, b) => b[0].localeCompare(a[0]))
            .map(([date, list]) => {
                const totalArea = list.reduce((a, s) => a + Number(s.plot_area || 0), 0);
                const totalCost = list.reduce((a, s) => a + Number(s.completed_cost ?? s.est_cost ?? 0), 0);
                const methodsSummary = [...new Set(list.map((s) => s.method_short))].filter(Boolean).join(', ');
                return { date, hdr: fmtDayHeader(date), items: list, totalArea, totalCost, methodsSummary };
            });

        const pg = paginateDateGroups(groups, state.compPage, (g) => g.items.length);
        state.compPage = pg.page;
        const allCol = pg.slice.length > 0 && pg.slice.every((g) => state.compCollapsedDates[g.date]);

        completedWrap.innerHTML = `
            ${groups.length > 1 ? `
                <div class="ft-comp-bar">
                    <span>${pg.totalItems} concluído${pg.totalItems !== 1 ? 's' : ''} em ${pg.totalGroups} datas</span>
                    <button type="button" class="ft-link-btn" data-ft-toggle-all-comp="${allCol ? 'expand' : 'collapse'}">
                        ${allCol ? 'Expandir dias' : 'Recolher dias'}
                    </button>
                </div>
            ` : ''}
            <div class="ft-days-wrap">
                ${pg.slice.map((g) => {
                    const collapsed = Boolean(state.compCollapsedDates[g.date]);
                    return `
                        <section class="ft-day ${collapsed ? 'is-collapsed' : ''}">
                            <div class="ft-day-head">
                                <button type="button" class="ft-day-toggle" data-ft-toggle-comp-day="${esc(g.date)}" aria-expanded="${!collapsed}">
                                    <span class="ft-day-chev ${collapsed ? 'is-rot' : ''}">${IC.chev}</span>
                                    <strong class="ft-day-main">${esc(g.hdr.main)}</strong>
                                    <span class="ft-day-sub">${esc(g.hdr.sub)}</span>
                                    <span class="pd-chip is-ok">Concluído</span>
                                    <span class="ft-day-meta">· ${g.items.length} talh${g.items.length > 1 ? 'ões' : 'ão'} · ${fmtNum(g.totalArea, 1)} ha${collapsed ? ` · ${esc(g.methodsSummary)}` : ''}</span>
                                </button>
                                ${g.totalCost > 0 ? `<span class="ft-day-total">${fmtBRL(g.totalCost)}</span>` : ''}
                            </div>
                            ${collapsed ? '' : `
                                <div class="ft-day-rows">
                                    ${g.items.map((s) => {
                                        const cost = Number(s.completed_cost ?? s.est_cost ?? 0);
                                        const costHa = s.plot_area > 0 ? cost / s.plot_area : 0;
                                        const dateDiffers = s.completed_date && s.scheduled_date && s.completed_date !== s.scheduled_date;
                                        const isMenuPop = state.openPopover?.type === 'comp_menu' && state.openPopover.key === s.id;
                                        return `
                                            <div class="ft-row">
                                                <div class="ft-row-left">
                                                    <div class="ft-row-info">
                                                        <div class="ft-row-title-line">
                                                            <button type="button" class="ft-row-title" data-ft-detail="${s.id}">${esc(s.plot_name)}</button>
                                                            <span class="ft-row-area">${fmtNum(s.plot_area, 1)} ha</span>
                                                            ${dateDiffers ? `<span class="ft-row-prev">· Previsto ${esc(s.scheduled_date_br)}</span>` : ''}
                                                        </div>
                                                        <div class="ft-row-sub">
                                                            <span class="ft-row-method"><i style="background:${esc(s.method_dot)}"></i>${esc(s.method_short)}${s.duration_minutes ? ` · ${s.duration_minutes} min` : ''}</span>
                                                            <span class="ft-sep">·</span>
                                                            <span class="ft-row-inputs">${esc(s.inputs_summary)}</span>
                                                        </div>
                                                    </div>
                                                </div>
                                                <div class="ft-row-right">
                                                    ${cost > 0 ? `
                                                        <div class="ft-row-cost-stack">
                                                            <strong>${fmtBRL(cost)}</strong>
                                                            <small>${fmtBRL(costHa)}/ha</small>
                                                        </div>
                                                    ` : ''}
                                                    <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm" data-ft-detail="${s.id}">Detalhes</button>
                                                    <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm ft-ic-btn" data-ft-open-comp-menu="${s.id}" aria-label="Mais ações" title="Mais ações">
                                                        ${IC.dots}
                                                    </button>
                                                    ${isMenuPop ? `
                                                        <div class="ft-pop is-menu" data-ft-pop-box>
                                                            <button type="button" class="ft-pop-item" data-ft-detail="${s.id}">Ver detalhes completos</button>
                                                            ${s.fertilization_record_id ? `<a class="ft-pop-item" href="/fertilizacao">Ver em Fertilização</a>` : ''}
                                                            ${CAN_WRITE && s.fertilization_record_id ? `<a class="ft-pop-item" href="/fertilizacao/agendamentos/novo?copy_fert_id=${s.fertilization_record_id}">Agendar novamente</a>` : ''}
                                                            ${CAN_WRITE ? `
                                                                <div class="ft-pop-div"></div>
                                                                <button type="button" class="ft-pop-item" data-ft-confirm-revert="${s.id}">Reabrir agendamento</button>
                                                            ` : ''}
                                                        </div>
                                                    ` : ''}
                                                </div>
                                            </div>
                                        `;
                                    }).join('')}
                                </div>
                            `}
                        </section>
                    `;
                }).join('')}
            </div>
            ${renderPaginationBar(pg, 'data-ft-pag-comp', 'concluídos')}
        `;
    }

    function syncBulkBar() {
        if (!bulkBar) return;
        const count = state.selectedIds.size;
        bulkBar.hidden = count === 0 || state.view !== 'list';
        if (bulkCount) bulkCount.textContent = String(count);
    }

    function renderAll() {
        root.querySelectorAll('[data-ft-view]').forEach((btn) => {
            const active = btn.dataset.ftView === state.view;
            btn.classList.toggle('is-active', active);
            btn.setAttribute('aria-selected', active ? 'true' : 'false');
        });
        root.querySelectorAll('[data-ft-panel]').forEach((p) => {
            p.hidden = p.dataset.ftPanel !== state.view;
        });
        if (activeFiltersRow) activeFiltersRow.hidden = state.view === 'completed';
        if (listTools) listTools.hidden = state.view !== 'list';
        root.querySelectorAll('[data-ft-filter]').forEach((b) => {
            b.setAttribute('aria-pressed', b.dataset.ftFilter === state.filter ? 'true' : 'false');
        });

        if (state.view === 'list') renderListView();
        else if (state.view === 'calendar') renderCalendarView();
        else renderCompletedView();
        syncBulkBar();
        bindDynamic();
    }

    // ---- Modal de Execução (Agenda -> Fertilização) ----
    function openExecuteDialog(scheduleId) {
        const sch = getSchedule(scheduleId);
        if (!sch || !execDialog) return;
        state.execDraft = {
            sch,
            applicationDate: TODAY,
            durationMinutes: sch.duration_minutes || 45,
            responsibleName: sch.responsible_name || '',
            notes: sch.notes || '',
            items: (sch.items || []).map((it) => ({
                input_id: it.input_id,
                name: it.name,
                unit: it.unit,
                planned: it.quantity,
                actual: it.quantity,
                available: it.available,
                avg_cost: it.avg_cost,
            })),
        };
        const form = execDialog.querySelector('[data-ft-exec-form]');
        form.action = `/fertilizacao/agendamentos/${sch.id}/concluir`;
        execDialog.querySelector('[data-ft-exec-subtitle]').textContent = `${sch.plot_name} (${fmtNum(sch.plot_area, 1)} ha) · ${sch.method_label}`;
        renderExecDialogBody();
        execDialog.showModal();
    }

    function renderExecDialogBody() {
        const d = state.execDraft;
        if (!d || !execDialog) return;
        const sch = d.sch;
        const body = execDialog.querySelector('[data-ft-exec-body]');
        const submitBtn = execDialog.querySelector('[data-ft-exec-submit]');
        const itemsJsonInput = execDialog.querySelector('[data-ft-exec-items-json]');

        let totalCost = 0;
        let hasShortage = false;
        d.items.forEach((it) => {
            const qty = Math.max(0, Number(it.actual || 0));
            totalCost += qty * Number(it.avg_cost || 0);
            if (qty > Number(it.available || 0) + 0.001) hasShortage = true;
        });
        const irrVol = sch.method === 'fertirrigacao' && sch.plot_irrigation === 'gotejo' && sch.plot_lpm > 0 && d.durationMinutes > 0
            ? Math.round(sch.plot_lpm * Number(d.durationMinutes))
            : 0;

        itemsJsonInput.value = JSON.stringify(d.items.map((it) => ({
            input_id: it.input_id,
            name: it.name,
            unit: it.unit,
            quantity: Number(it.actual || 0),
        })));
        submitBtn.disabled = hasShortage;
        submitBtn.textContent = hasShortage ? 'Estoque insuficiente' : 'Confirmar e registrar aplicação';

        body.innerHTML = `
            <div class="ft-exec-grid">
                <label class="ft-field">
                    <span>Data real da aplicação</span>
                    <input type="date" name="application_date" value="${esc(d.applicationDate)}" required data-exec-f="applicationDate">
                </label>
                ${sch.method === 'fertirrigacao' ? `
                    <label class="ft-field">
                        <span>Duração real (min)</span>
                        <input type="number" name="duration_minutes" min="1" value="${esc(d.durationMinutes)}" required data-exec-f="durationMinutes">
                    </label>
                ` : ''}
                <label class="ft-field">
                    <span>Quem aplicou</span>
                    <input type="text" name="responsible_name" value="${esc(d.responsibleName)}" placeholder="Ex.: Carlos (Operador)" data-exec-f="responsibleName">
                </label>
            </div>

            <div class="ft-exec-section">
                <div class="ft-exec-sec-head">
                    <strong>Quantidades realmente aplicadas</strong>
                    <small>Ajuste caso o aplicado em campo tenha diferido do planejado</small>
                </div>
                <div class="ft-exec-items">
                    ${d.items.map((it, idx) => {
                        const qty = Number(it.actual || 0);
                        const after = Number((Number(it.available || 0) - qty).toFixed(2));
                        const short = after < -0.001;
                        return `
                            <div class="ft-exec-item ${short ? 'is-short' : ''}">
                                <div class="min-w-0">
                                    <strong>${esc(it.name)}</strong>
                                    <small>Planejado: ${fmtQty(it.planned)} ${esc(it.unit)} · Estoque: ${fmtQty(it.available)} → <b class="${short ? 'is-err' : 'is-ok'}">${fmtQty(after)} ${esc(it.unit)}</b></small>
                                </div>
                                <div class="ft-exec-qty">
                                    <input type="number" step="0.01" min="0" value="${qty}" data-exec-qty="${idx}">
                                    <span>${esc(it.unit)}</span>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>

            <label class="ft-field">
                <span>Observações da execução</span>
                <textarea name="notes" rows="2" data-exec-f="notes">${esc(d.notes)}</textarea>
            </label>

            <div class="ft-impact-box">
                <p class="pd-eyebrow is-brand">O que o SiSFarm fará automaticamente ao confirmar</p>
                <ul>
                    <li>Registra em <strong>Fertilização</strong> sem duplicidade (vinculado à Agenda #${sch.id})</li>
                    <li>Dá baixa FIFO no <strong>Estoque</strong>${totalCost > 0 ? ` e lança <strong>${fmtBRL(totalCost)}</strong> em Custos por Setor` : ''}</li>
                    ${irrVol ? `<li class="is-info">Registra <strong>${fmtQty(irrVol)} L</strong> (${esc(d.durationMinutes)} min) automaticamente em <strong>Irrigação</strong></li>` : ''}
                </ul>
            </div>
        `;

        body.querySelectorAll('[data-exec-f]').forEach((el) => {
            el.addEventListener('change', () => {
                const k = el.dataset.execF;
                d[k] = k === 'durationMinutes' ? Number(el.value || 0) : el.value;
                if (k === 'durationMinutes') renderExecDialogBody();
            });
        });
        body.querySelectorAll('[data-exec-qty]').forEach((el) => {
            el.addEventListener('change', () => {
                d.items[Number(el.dataset.execQty)].actual = Math.max(0, Number(el.value || 0));
                renderExecDialogBody();
            });
        });
    }

    // ---- Modal de Reagendamento em Lote (Mesma data ou Empurrar +N dias) ----
    function openReschedDialog(ids) {
        if (!reschedDialog || !ids.length) return;
        state.reschedIds = ids;
        reschedDialog.querySelector('[data-ft-resched-ids-input]').value = ids.join(',');
        reschedDialog.querySelector('[data-ft-resched-title]').textContent = `Reagendar ${ids.length} agendamento${ids.length > 1 ? 's' : ''}`;
        renderReschedDialogBody();
        reschedDialog.showModal();
    }

    function renderReschedDialogBody() {
        if (!reschedDialog) return;
        const body = reschedDialog.querySelector('[data-ft-resched-body]');
        const modeInput = reschedDialog.querySelector('[data-ft-resched-mode-input]');
        const submitBtn = reschedDialog.querySelector('[data-ft-resched-submit]');
        modeInput.value = state.reschedMode;
        const items = state.reschedIds.map(getSchedule).filter(Boolean);
        const uniqueDates = [...new Set(items.map((i) => i.scheduled_date).filter(Boolean))].sort();

        submitBtn.textContent = state.reschedMode === 'date'
            ? 'Salvar nova data'
            : `Empurrar +${state.reschedShiftDays} dia${state.reschedShiftDays > 1 ? 's' : ''}`;

        body.innerHTML = `
            <div class="ft-seg">
                <button type="button" class="ft-seg-btn ${state.reschedMode === 'date' ? 'is-active' : ''}" data-r-mode="date">
                    <strong>Mesma data</strong><small>juntar tudo num dia</small>
                </button>
                <button type="button" class="ft-seg-btn ${state.reschedMode === 'shift' ? 'is-active' : ''}" data-r-mode="shift">
                    <strong>Empurrar (+dias)</strong><small>mantém intervalo</small>
                </button>
            </div>
            ${state.reschedMode === 'date' ? `
                <div class="ft-resched-pane">
                    <div class="sl-chips">
                        <button type="button" class="sl-chip" data-r-quick="${addDays(TODAY, 1)}">Amanhã (${fmtDateShort(addDays(TODAY, 1))})</button>
                        <button type="button" class="sl-chip" data-r-quick="${addDays(TODAY, 3)}">+3 dias (${fmtDateShort(addDays(TODAY, 3))})</button>
                        <button type="button" class="sl-chip" data-r-quick="${addDays(TODAY, 7)}">+7 dias (${fmtDateShort(addDays(TODAY, 7))})</button>
                    </div>
                    <label class="ft-field">
                        <span>Nova data para todos</span>
                        <input type="date" name="scheduled_date" value="${addDays(TODAY, 2)}" required data-r-date-input>
                    </label>
                </div>
            ` : `
                <div class="ft-resched-pane">
                    <div class="sl-chips">
                        ${[1, 2, 3, 5, 7].map((d) => `<button type="button" class="sl-chip" aria-pressed="${state.reschedShiftDays === d ? 'true' : 'false'}" data-r-shift="${d}">+${d} dia${d > 1 ? 's' : ''}</button>`).join('')}
                    </div>
                    <label class="ft-field">
                        <span>Ou informe quantos dias adiar</span>
                        <input type="number" name="shift_days" min="1" max="180" value="${state.reschedShiftDays}" required data-r-shift-input>
                    </label>
                    <div class="ft-shift-preview">
                        <strong>Prévia do deslocamento:</strong>
                        ${uniqueDates.slice(0, 5).map((d) => `<div>${fmtDate(d)} → <b>${fmtDate(addDays(d, state.reschedShiftDays))}</b></div>`).join('')}
                    </div>
                </div>
            `}
        `;

        body.querySelectorAll('[data-r-mode]').forEach((b) => {
            b.addEventListener('click', () => { state.reschedMode = b.dataset.rMode; renderReschedDialogBody(); });
        });
        body.querySelectorAll('[data-r-quick]').forEach((b) => {
            b.addEventListener('click', () => {
                const inp = body.querySelector('[data-r-date-input]');
                if (inp) inp.value = b.dataset.rQuick;
            });
        });
        body.querySelectorAll('[data-r-shift]').forEach((b) => {
            b.addEventListener('click', () => { state.reschedShiftDays = Number(b.dataset.rShift); renderReschedDialogBody(); });
        });
        body.querySelector('[data-r-shift-input]')?.addEventListener('change', (e) => {
            state.reschedShiftDays = Math.max(1, Number(e.target.value || 1));
            renderReschedDialogBody();
        });
    }

    // ---- Gaveta de Ficha Completa do Agendamento ----
    function openDetailDrawer(scheduleId) {
        const s = getSchedule(scheduleId);
        if (!s || !detailDialog) return;
        const shell = detailDialog.querySelector('[data-ft-detail-shell]');
        shell.innerHTML = `
            <div class="ft-dialog-head">
                <div>
                    <p class="pd-eyebrow">Agendamento #${s.id}</p>
                    <h2>${esc(s.plot_name)}</h2>
                    <p class="ft-dialog-sub">Previsto para ${esc(s.scheduled_date_br)} · ${esc(s.method_label)}${s.duration_minutes ? ` (${s.duration_minutes} min)` : ''}</p>
                </div>
                <button type="button" class="ft-x" data-ft-close-dialog aria-label="Fechar">✕</button>
            </div>
            <div class="ft-dialog-body">
                <div class="ft-detail-kpis">
                    <div><span>Custo estimado</span><strong>${fmtBRL(s.est_cost)}</strong></div>
                    <div><span>Responsável</span><strong>${esc(s.responsible_name || 'Não definido')}</strong></div>
                </div>
                ${s.recipe_name ? `<div class="ft-detail-note"><span>Receita</span><strong>${esc(s.recipe_name)}</strong></div>` : ''}
                <div class="ft-detail-sec">
                    <p class="pd-eyebrow">Insumos programados</p>
                    <div class="ft-detail-items">
                        ${(s.items || []).map((it) => `
                            <div class="ft-detail-item">
                                <div><strong>${esc(it.name)}</strong><small>${fmtQty(it.dose_per_ha)} ${esc(it.unit)}/ha</small></div>
                                <strong class="ft-num">${fmtQty(it.quantity)} ${esc(it.unit)}</strong>
                            </div>
                        `).join('')}
                    </div>
                </div>
                <div class="ft-detail-sec">
                    <p class="pd-eyebrow">Observações</p>
                    <p class="ft-detail-notes">${esc(s.notes || 'Sem observações registradas.')}</p>
                </div>
            </div>
            ${CAN_WRITE && s.status !== 'completed' ? `
                <div class="ft-dialog-foot">
                    <a class="sf-btn sf-btn-secondary" href="/fertilizacao/agendamentos/${s.id}/editar">Editar</a>
                    <button type="button" class="sf-btn sf-btn-primary" data-ft-drawer-exec="${s.id}">Executar e registrar</button>
                </div>
            ` : ''}
        `;
        shell.querySelector('[data-ft-close-dialog]')?.addEventListener('click', () => detailDialog.close());
        shell.querySelector('[data-ft-drawer-exec]')?.addEventListener('click', () => {
            detailDialog.close();
            openExecuteDialog(s.id);
        });
        detailDialog.showModal();
    }

    function openConfirmDialog({ action, badge, title, text, btnText, isDanger = true }) {
        if (!confirmDialog) return;
        const form = confirmDialog.querySelector('[data-ft-confirm-form]');
        form.action = action;
        const bEl = confirmDialog.querySelector('[data-ft-confirm-badge]');
        bEl.textContent = badge;
        bEl.className = `pd-chip ${isDanger ? 'is-err' : 'is-warn'}`;
        confirmDialog.querySelector('[data-ft-confirm-title]').textContent = title;
        confirmDialog.querySelector('[data-ft-confirm-text]').textContent = text;
        const btn = confirmDialog.querySelector('[data-ft-confirm-btn]');
        btn.textContent = btnText;
        btn.className = `sf-btn ${isDanger ? 'sf-btn-danger' : 'sf-btn-primary'}`;
        confirmDialog.showModal();
    }

    function bindDynamic() {
        root.querySelectorAll('[data-ft-reset-filters]').forEach((el) => {
            el.addEventListener('click', () => {
                state.filter = 'all'; state.methodFilter = 'all'; state.search = '';
                const sInp = root.querySelector('[data-ft-search]');
                const mSel = root.querySelector('[data-ft-method-filter]');
                if (sInp) sInp.value = '';
                if (mSel) mSel.value = 'all';
                renderAll();
            });
        });

        root.querySelectorAll('[data-ft-toggle-day]').forEach((el) => {
            el.addEventListener('click', () => {
                const d = el.dataset.ftToggleDay;
                state.collapsedDates[d] = !state.collapsedDates[d];
                state.openPopover = null;
                renderAll();
            });
        });

        root.querySelectorAll('[data-ft-open-day-pop]').forEach((el) => {
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                const d = el.dataset.ftOpenDayPop;
                state.openPopover = (state.openPopover?.type === 'day' && state.openPopover.key === d) ? null : { type: 'day', key: d };
                renderAll();
            });
        });
        root.querySelectorAll('[data-ft-open-item-pop]').forEach((el) => {
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = Number(el.dataset.ftOpenItemPop);
                state.openPopover = (state.openPopover?.type === 'item' && state.openPopover.key === id) ? null : { type: 'item', key: id };
                renderAll();
            });
        });
        root.querySelectorAll('[data-ft-open-menu]').forEach((el) => {
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = Number(el.dataset.ftOpenMenu);
                state.openPopover = (state.openPopover?.type === 'menu' && state.openPopover.key === id) ? null : { type: 'menu', key: id };
                renderAll();
            });
        });
        root.querySelectorAll('[data-ft-open-comp-menu]').forEach((el) => {
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = Number(el.dataset.ftOpenCompMenu);
                state.openPopover = (state.openPopover?.type === 'comp_menu' && state.openPopover.key === id) ? null : { type: 'comp_menu', key: id };
                renderAll();
            });
        });
        root.querySelectorAll('[data-ft-toggle-comp-day]').forEach((el) => {
            el.addEventListener('click', () => {
                const dt = el.dataset.ftToggleCompDay;
                state.compCollapsedDates[dt] = !state.compCollapsedDates[dt];
                state.openPopover = null;
                renderAll();
            });
        });
        root.querySelector('[data-ft-toggle-all-comp]')?.addEventListener('click', (e) => {
            const mode = e.currentTarget.dataset.ftToggleAllComp;
            const dates = [...new Set((DATA.completedItems || []).map((i) => i.completed_date || i.scheduled_date || TODAY))];
            if (mode === 'collapse') dates.forEach((d) => { state.compCollapsedDates[d] = true; });
            else state.compCollapsedDates = {};
            state.openPopover = null;
            renderAll();
        });
        root.querySelectorAll('[data-ft-pag-sched]').forEach((el) => {
            el.addEventListener('click', () => {
                state.schedPage = Number(el.dataset.ftPagSched);
                state.openPopover = null;
                renderAll();
                daysWrap?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            });
        });
        root.querySelectorAll('[data-ft-pag-comp]').forEach((el) => {
            el.addEventListener('click', () => {
                state.compPage = Number(el.dataset.ftPagComp);
                state.openPopover = null;
                renderAll();
                completedWrap?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            });
        });
        root.querySelectorAll('[data-ft-pop-box]').forEach((el) => el.addEventListener('click', (e) => e.stopPropagation()));

        root.querySelectorAll('[data-ft-quick-move]').forEach((el) => {
            el.addEventListener('click', () => {
                const ids = el.dataset.ftIds.split(',').map(Number).filter(Boolean);
                postDateBatch({ ids, mode: 'date', scheduledDate: el.dataset.ftQuickMove });
            });
        });
        root.querySelectorAll('[data-ft-pop-custom-go]').forEach((el) => {
            el.addEventListener('click', () => {
                const box = el.closest('[data-ft-pop-box]');
                const dt = box?.querySelector('[data-ft-pop-date]')?.value;
                const ids = el.dataset.ftIds.split(',').map(Number).filter(Boolean);
                if (dt) postDateBatch({ ids, mode: 'date', scheduledDate: dt });
            });
        });

        root.querySelectorAll('[data-ft-check]').forEach((el) => {
            el.addEventListener('change', () => {
                const id = Number(el.dataset.ftCheck);
                if (el.checked) state.selectedIds.add(id);
                else state.selectedIds.delete(id);
                el.closest('.ft-row')?.classList.toggle('is-selected', el.checked);
                syncBulkBar();
            });
        });

        root.querySelectorAll('[data-ft-exec]').forEach((el) => {
            el.addEventListener('click', () => {
                state.openPopover = null;
                openExecuteDialog(el.dataset.ftExec);
            });
        });
        root.querySelectorAll('[data-ft-detail]').forEach((el) => {
            el.addEventListener('click', () => {
                state.openPopover = null;
                openDetailDrawer(el.dataset.ftDetail);
            });
        });
        root.querySelectorAll('[data-ft-open-buylist]').forEach((el) => {
            el.addEventListener('click', () => buylistDialog?.showModal());
        });
        root.querySelectorAll('[data-ft-confirm-delete]').forEach((el) => {
            el.addEventListener('click', () => {
                state.openPopover = null;
                openConfirmDialog({
                    action: `/fertilizacao/agendamentos/${el.dataset.ftConfirmDelete}/excluir`,
                    badge: 'Remover da agenda',
                    title: 'Excluir este agendamento?',
                    text: 'Esta ação remove apenas o planejamento futuro. Nenhum saldo de estoque será alterado.',
                    btnText: 'Excluir agendamento',
                    isDanger: true,
                });
            });
        });
        root.querySelectorAll('[data-ft-confirm-revert]').forEach((el) => {
            el.addEventListener('click', () => {
                openConfirmDialog({
                    action: `/fertilizacao/agendamentos/${el.dataset.ftConfirmRevert}/alternar-status`,
                    badge: 'Reabrir agendamento',
                    title: 'Voltar para Programado?',
                    text: 'A aplicação gerada em Fertilização será removida, a irrigação automática (se houver) será desfeita e os insumos retornarão ao estoque.',
                    btnText: 'Sim, reabrir',
                    isDanger: false,
                });
            });
        });

        // Calendário nav
        root.querySelectorAll('[data-ft-cal-nav]').forEach((el) => {
            el.addEventListener('click', () => {
                state.calMonth += Number(el.dataset.ftCalNav);
                if (state.calMonth < 0) { state.calMonth = 11; state.calYear--; }
                if (state.calMonth > 11) { state.calMonth = 0; state.calYear++; }
                renderCalendarView();
                bindDynamic();
            });
        });
        root.querySelector('[data-ft-cal-today]')?.addEventListener('click', () => {
            state.calYear = Number(TODAY.slice(0, 4));
            state.calMonth = Number(TODAY.slice(5, 7)) - 1;
            renderCalendarView();
            bindDynamic();
        });

        // Drag & Drop entre datas
        if (CAN_WRITE) {
            root.querySelectorAll('[data-ft-drag]').forEach((row) => {
                row.addEventListener('dragstart', (e) => {
                    e.dataTransfer.setData('text/plain', row.dataset.ftDrag);
                    row.classList.add('is-dragging');
                });
                row.addEventListener('dragend', () => {
                    row.classList.remove('is-dragging');
                    root.querySelectorAll('.is-drop-over').forEach((z) => z.classList.remove('is-drop-over'));
                });
            });
            root.querySelectorAll('[data-ft-day-zone]').forEach((zone) => {
                zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('is-drop-over'); });
                zone.addEventListener('dragleave', (e) => { if (!zone.contains(e.relatedTarget)) zone.classList.remove('is-drop-over'); });
                zone.addEventListener('drop', (e) => {
                    e.preventDefault();
                    zone.classList.remove('is-drop-over');
                    const id = Number(e.dataTransfer.getData('text/plain'));
                    const targetDate = zone.dataset.ftDayZone;
                    const sch = getSchedule(id);
                    if (sch && targetDate && sch.scheduled_date !== targetDate) {
                        postDateBatch({ ids: [id], mode: 'date', scheduledDate: targetDate });
                    }
                });
            });
        }
    }

    // Bindings estáticos
    root.querySelectorAll('[data-ft-view]').forEach((btn) => {
        btn.addEventListener('click', () => { state.view = btn.dataset.ftView; state.openPopover = null; renderAll(); });
    });
    root.querySelectorAll('[data-ft-filter]').forEach((btn) => {
        btn.addEventListener('click', () => { state.view = 'list'; state.filter = btn.dataset.ftFilter; state.schedPage = 1; state.openPopover = null; renderAll(); });
    });
    root.querySelectorAll('[data-ft-filter-jump]').forEach((btn) => {
        btn.addEventListener('click', () => {
            state.view = 'list';
            state.filter = btn.dataset.ftFilterJump;
            state.schedPage = 1;
            state.openPopover = null;
            renderAll();
            root.querySelector('.ft-stage')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
    });
    root.querySelector('[data-ft-method-filter]')?.addEventListener('change', (e) => {
        state.methodFilter = e.target.value; state.schedPage = 1; state.openPopover = null; renderAll();
    });
    root.querySelector('[data-ft-search]')?.addEventListener('input', (e) => {
        state.search = e.target.value; state.schedPage = 1; state.compPage = 1; renderAll();
    });
    root.querySelector('[data-ft-demand-toggle]')?.addEventListener('click', (e) => {
        const body = root.querySelector('[data-ft-demand-body]');
        if (!body) return;
        const willOpen = body.hidden;
        body.hidden = !willOpen;
        e.currentTarget.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
        e.currentTarget.textContent = willOpen ? 'Ocultar ▲' : 'Ver saldo por insumo ▼';
    });
    toggleAllBtn?.addEventListener('click', () => {
        const mode = toggleAllBtn.dataset.mode || 'collapse';
        const dates = [...new Set((DATA.activeItems || []).map((i) => i.scheduled_date))];
        if (mode === 'collapse') dates.forEach((d) => { state.collapsedDates[d] = true; });
        else state.collapsedDates = {};
        state.openPopover = null;
        renderAll();
    });
    root.querySelector('[data-ft-bulk-clear]')?.addEventListener('click', () => { state.selectedIds.clear(); renderAll(); });
    root.querySelector('[data-ft-open-bulk-resched]')?.addEventListener('click', () => openReschedDialog([...state.selectedIds]));

    document.querySelectorAll('[data-ft-close-dialog]').forEach((btn) => {
        btn.addEventListener('click', () => btn.closest('dialog')?.close());
    });
    [execDialog, reschedDialog, buylistDialog, detailDialog, confirmDialog].forEach((dlg) => {
        dlg?.addEventListener('click', (e) => { if (e.target === dlg) dlg.close(); });
    });

    window.addEventListener('click', () => {
        if (state.openPopover) { state.openPopover = null; renderAll(); }
    });
    window.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && state.openPopover) { state.openPopover = null; renderAll(); }
    });

    renderAll();
})();
