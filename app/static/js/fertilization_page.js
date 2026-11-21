(() => {
    const root = document.querySelector('[data-ft-fert-page]');
    const dataEl = document.getElementById('ft-fert-data');
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
    const fmtDayHeader = (iso) => {
        if (!iso || iso.length < 10) return { main: 'Sem data', sub: '' };
        const days = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado'];
        const months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
        const dt = new Date(iso.slice(0, 10) + 'T12:00:00');
        return { main: `${dt.getDate()} ${months[dt.getMonth()]}`, sub: days[dt.getDay()] };
    };

    const IC = {
        chev: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>',
        dots: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="12" cy="5" r="1.7"/><circle cx="12" cy="12" r="1.7"/><circle cx="12" cy="19" r="1.7"/></svg>',
    };

    const CAN_WRITE = Boolean(DATA.canWrite);
    const ROWS = DATA.rows || [];
    const DAYS_PER_PAGE = 5;

    const state = {
        filter: 'all',
        plotId: null,
        search: '',
        page: 1,
        collapsedDates: {},
        openMenuId: null,
    };

    const listEl = root.querySelector('[data-ft-fert-list]');
    const clearPlotBtn = root.querySelector('[data-ft-plot-clear]');
    const toggleAllBtn = root.querySelector('[data-ft-fert-toggle-all]');
    const detailDialog = document.getElementById('ft-fert-detail-dialog');
    const deleteDialog = document.getElementById('ft-fert-delete-dialog');

    function filteredRows() {
        const q = state.search.trim().toLowerCase();
        return ROWS.filter((r) => {
            if (state.plotId !== null && r.plot_id !== state.plotId) return false;
            if (['fertirrigacao', 'adubacao_solida', 'fertilizacao_foliar'].includes(state.filter) && r.method !== state.filter) return false;
            if (state.filter === 'from_schedule' && !r.schedule_id) return false;
            if (state.filter === 'manual' && r.schedule_id) return false;
            if (q) {
                const hay = [
                    r.plot_name, r.recipe_name, r.responsible_name, r.notes, r.method_label, r.product,
                    ...(r.items || []).map((i) => i.name),
                ].join(' ').toLowerCase();
                if (!hay.includes(q)) return false;
            }
            return true;
        });
    }

    function summarizeInputs(f) {
        const items = f.items || [];
        if (items.length) {
            const names = items.map((it) => String(it.name || '').split('(')[0].trim()).filter(Boolean);
            if (names.length <= 3) return names.join(', ');
            return `${names.slice(0, 3).join(', ')} +${names.length - 3}`;
        }
        return f.product || 'Sem insumos detalhados';
    }

    function paginateDateGroups(groups, rawPage) {
        const totalGroups = groups.length;
        const totalItems = groups.reduce((acc, g) => acc + g.items.length, 0);
        const totalPages = Math.max(1, Math.ceil(totalGroups / DAYS_PER_PAGE));
        const page = Math.min(Math.max(1, Number(rawPage) || 1), totalPages);
        const startIdx = (page - 1) * DAYS_PER_PAGE;
        const slice = groups.slice(startIdx, startIdx + DAYS_PER_PAGE);
        const shownItems = slice.reduce((acc, g) => acc + g.items.length, 0);
        return { page, totalPages, totalGroups, totalItems, slice, startIdx, shownItems };
    }

    function renderPaginationBar(pg) {
        if (pg.totalPages <= 1) return '';
        const pages = [];
        for (let p = 1; p <= pg.totalPages; p++) {
            if (p === 1 || p === pg.totalPages || Math.abs(p - pg.page) <= 1) pages.push(p);
            else if (pages[pages.length - 1] !== '…') pages.push('…');
        }
        const fromDay = pg.startIdx + 1;
        const toDay = pg.startIdx + pg.slice.length;
        return `
            <nav class="ft-pag" aria-label="Paginação do histórico">
                <div class="ft-pag-info">
                    Mostrando <strong>${fromDay}–${toDay}</strong> de <strong>${pg.totalGroups} datas</strong>
                    <span class="ft-hide-xs"> · ${pg.shownItems} de ${pg.totalItems} aplicações</span>
                </div>
                <div class="ft-pag-nav">
                    <button type="button" class="ft-pag-btn" data-ft-fert-pag="${pg.page - 1}" ${pg.page <= 1 ? 'disabled' : ''}>← Anterior</button>
                    ${pages.map((p) => p === '…'
                        ? '<span class="ft-pag-dots" aria-hidden="true">…</span>'
                        : `<button type="button" class="ft-pag-btn ${p === pg.page ? 'is-active' : ''}" data-ft-fert-pag="${p}" ${p === pg.page ? 'aria-current="page"' : ''}>${p}</button>`
                    ).join('')}
                    <button type="button" class="ft-pag-btn" data-ft-fert-pag="${pg.page + 1}" ${pg.page >= pg.totalPages ? 'disabled' : ''}>Próxima →</button>
                </div>
            </nav>
        `;
    }

    function openDetailDrawer(id) {
        const f = ROWS.find((r) => r.id === Number(id));
        if (!f || !detailDialog) return;
        const shell = detailDialog.querySelector('[data-ft-fert-detail-shell]');
        shell.innerHTML = `
            <div class="ft-dialog-head">
                <div>
                    <p class="pd-eyebrow">Ficha da aplicação #${f.id}</p>
                    <h2>${esc(f.plot_name)}</h2>
                    <p class="ft-dialog-sub">${esc(f.application_date_br)} · ${esc(f.method_label)}${f.duration_minutes ? ` (${f.duration_minutes} min)` : ''}</p>
                </div>
                <button type="button" class="ft-x" data-ft-close-dialog aria-label="Fechar">✕</button>
            </div>
            <div class="ft-dialog-body">
                <div class="ft-detail-kpis">
                    <div>
                        <span>Custo total (FIFO)</span>
                        <strong>${fmtBRL(f.total_cost)}</strong>
                        <small>${fmtBRL(f.cost_per_ha)}/ha</small>
                    </div>
                    <div>
                        <span>Origem do lançamento</span>
                        <strong>${f.schedule_id ? `Agendamento #${f.schedule_id}` : 'Lançamento direto'}</strong>
                        <small>${esc(f.responsible_name || 'Responsável não informado')}</small>
                    </div>
                </div>
                ${f.irrigation_liters ? `
                    <div class="ft-detail-irr">
                        <strong>Irrigação automática vinculada:</strong> ${fmtQty(f.irrigation_liters)} L (${f.duration_minutes} min no gotejo de ${esc(f.plot_name)}).
                    </div>
                ` : ''}
                ${f.recipe_name ? `<div class="ft-detail-note"><span>Receita</span><strong>${esc(f.recipe_name)}</strong></div>` : ''}
                <div class="ft-detail-sec">
                    <p class="pd-eyebrow">Insumos aplicados &amp; Rastreabilidade FIFO</p>
                    <div class="ft-detail-items">
                        ${(f.items || []).length ? f.items.map((it) => `
                            <div class="ft-fifo-card">
                                <div class="ft-fifo-top">
                                    <div>
                                        <strong>${esc(it.name)}</strong>
                                        <small>${fmtQty(it.total_qty)} ${esc(it.unit)} total · ${fmtQty(it.dose_per_ha)} ${esc(it.unit)}/ha</small>
                                    </div>
                                    <div class="ft-fifo-cost">
                                        <strong>${fmtBRL(it.total_cost)}</strong>
                                        <small>${fmtBRL(it.unit_cost)}/${esc(it.unit)}</small>
                                    </div>
                                </div>
                                ${(it.lots || []).length ? `
                                    <div class="ft-fifo-lots">
                                        ${it.lots.map((l) => `<span>Lote ${esc(l.code)}${l.supplier ? ` (${esc(l.supplier)})` : ''}: ${fmtQty(l.qty)} ${esc(it.unit)} × ${fmtBRL(l.unit_cost)}</span>`).join('')}
                                    </div>
                                ` : ''}
                            </div>
                        `).join('') : `<div class="ft-detail-item"><span>${esc(f.product)}</span><strong>${esc(f.dose)}</strong></div>`}
                    </div>
                </div>
                <div class="ft-detail-sec">
                    <p class="pd-eyebrow">Observações</p>
                    <p class="ft-detail-notes">${esc(f.notes || 'Sem observações registradas.')}</p>
                </div>
            </div>
            ${CAN_WRITE ? `
                <div class="ft-dialog-foot">
                    <button type="button" class="sf-btn sf-btn-danger-soft sf-btn-sm" data-ft-drawer-del="${f.id}">Excluir e estornar</button>
                    <div class="ft-drawer-foot-r">
                        <a class="sf-btn sf-btn-secondary sf-btn-sm" href="/fertilizacao/agendamentos/novo?copy_fert_id=${f.id}">Agendar novamente</a>
                        <a class="sf-btn sf-btn-primary sf-btn-sm" href="/fertilizacao/${f.id}/editar">Editar</a>
                    </div>
                </div>
            ` : ''}
        `;
        shell.querySelector('[data-ft-close-dialog]')?.addEventListener('click', () => detailDialog.close());
        shell.querySelector('[data-ft-drawer-del]')?.addEventListener('click', () => {
            detailDialog.close();
            openDeleteDialog(f);
        });
        detailDialog.showModal();
    }

    function openDeleteDialog(f) {
        if (!deleteDialog || !f) return;
        const form = deleteDialog.querySelector('[data-ft-fert-delete-form]');
        form.action = `/fertilizacao/${f.id}/excluir`;
        const parts = ['Os insumos consumidos serão devolvidos automaticamente ao estoque'];
        if (f.irrigation_liters) parts.push(`a irrigação automática de ${fmtQty(f.irrigation_liters)} L será removida`);
        if (f.schedule_id) parts.push(`o agendamento #${f.schedule_id} voltará para Programado`);
        deleteDialog.querySelector('[data-ft-fert-delete-text]').textContent = parts.join(', ') + '.';
        deleteDialog.showModal();
    }

    function render() {
        root.querySelectorAll('[data-ft-fert-filter]').forEach((b) => {
            b.setAttribute('aria-pressed', b.dataset.ftFertFilter === state.filter ? 'true' : 'false');
        });
        root.querySelectorAll('[data-ft-plot-card]').forEach((card) => {
            card.classList.toggle('is-selected', state.plotId === Number(card.dataset.ftPlotCard));
        });
        if (clearPlotBtn) clearPlotBtn.hidden = state.plotId === null;

        if (!listEl) return;
        const rows = filteredRows();
        if (!rows.length) {
            if (toggleAllBtn) toggleAllBtn.hidden = true;
            listEl.innerHTML = ROWS.length
                ? '<div class="pd-empty"><strong>Nenhuma aplicação neste filtro.</strong><span>Ajuste os filtros ou a busca para ver o histórico completo.</span><button type="button" class="sf-btn sf-btn-secondary" data-ft-fert-reset>Limpar filtros</button></div>'
                : `<div class="pd-empty"><strong>Nenhuma aplicação registrada nesta safra.</strong><span>Registre diretamente por aqui ou conclua um agendamento planejado na aba Agendamentos.</span>${CAN_WRITE ? '<a class="sf-btn sf-btn-primary" href="/fertilizacao/nova">Registrar primeira aplicação</a>' : ''}</div>`;
            listEl.querySelector('[data-ft-fert-reset]')?.addEventListener('click', () => {
                state.filter = 'all'; state.plotId = null; state.search = ''; state.page = 1; state.openMenuId = null;
                const sInp = root.querySelector('[data-ft-fert-search]');
                if (sInp) sInp.value = '';
                render();
            });
            return;
        }

        const byDate = new Map();
        rows.forEach((f) => {
            const dt = f.application_date || '0000-00-00';
            if (!byDate.has(dt)) byDate.set(dt, []);
            byDate.get(dt).push(f);
        });

        const groups = [...byDate.entries()]
            .sort((a, b) => b[0].localeCompare(a[0]))
            .map(([date, items]) => {
                const totalArea = items.reduce((a, f) => a + Number(f.plot_area || 0), 0);
                const totalCost = items.reduce((a, f) => a + Number(f.total_cost || 0), 0);
                const methodsSummary = [...new Set(items.map((f) => f.method_short))].filter(Boolean).join(', ');
                return { date, hdr: fmtDayHeader(date), items, totalArea, totalCost, methodsSummary };
            });

        const pg = paginateDateGroups(groups, state.page);
        state.page = pg.page;

        if (toggleAllBtn) {
            const allCol = pg.slice.length > 0 && pg.slice.every((g) => state.collapsedDates[g.date]);
            toggleAllBtn.textContent = allCol ? 'Expandir dias' : 'Recolher dias';
            toggleAllBtn.dataset.mode = allCol ? 'expand' : 'collapse';
            toggleAllBtn.hidden = groups.length <= 1;
        }

        listEl.innerHTML = `
            <div class="ft-days-wrap">
                ${pg.slice.map((g) => {
                    const collapsed = Boolean(state.collapsedDates[g.date]);
                    return `
                        <section class="ft-day ${collapsed ? 'is-collapsed' : ''}">
                            <div class="ft-day-head">
                                <button type="button" class="ft-day-toggle" data-ft-fert-toggle-day="${esc(g.date)}" aria-expanded="${!collapsed}">
                                    <span class="ft-day-chev ${collapsed ? 'is-rot' : ''}">${IC.chev}</span>
                                    <strong class="ft-day-main">${esc(g.hdr.main)}</strong>
                                    ${g.hdr.sub ? `<span class="ft-day-sub">${esc(g.hdr.sub)}</span>` : ''}
                                    <span class="ft-day-meta">· ${g.items.length} talh${g.items.length > 1 ? 'ões' : 'ão'} · ${fmtNum(g.totalArea, 1)} ha${collapsed ? ` · ${esc(g.methodsSummary)}` : ''}</span>
                                </button>
                                ${g.totalCost > 0 ? `<span class="ft-day-total">${fmtBRL(g.totalCost)}</span>` : ''}
                            </div>
                            ${collapsed ? '' : `
                                <div class="ft-day-rows">
                                    ${g.items.map((f) => {
                                        const isMenuOpen = state.openMenuId === f.id;
                                        return `
                                            <div class="ft-row">
                                                <div class="ft-row-left">
                                                    <div class="ft-row-info">
                                                        <div class="ft-row-title-line">
                                                            <button type="button" class="ft-row-title" data-ft-fert-open="${f.id}">${esc(f.plot_name)}</button>
                                                            <span class="ft-row-area">${fmtNum(f.plot_area, 1)} ha</span>
                                                        </div>
                                                        <div class="ft-row-sub">
                                                            <span class="ft-row-method"><i style="background:${esc(f.method_dot)}"></i>${esc(f.method_short)}${f.duration_minutes ? ` · ${f.duration_minutes} min` : ''}</span>
                                                            <span class="ft-sep">·</span>
                                                            <span class="ft-row-inputs">${esc(summarizeInputs(f))}</span>
                                                            ${f.schedule_id ? '<span class="ft-sep ft-hide-xs">·</span><span class="ft-row-origin ft-hide-xs">Agenda</span>' : ''}
                                                        </div>
                                                    </div>
                                                </div>
                                                <div class="ft-row-right">
                                                    <div class="ft-row-cost-stack">
                                                        <strong>${fmtBRL(f.total_cost)}</strong>
                                                        <small>${fmtBRL(f.cost_per_ha)}/ha</small>
                                                    </div>
                                                    <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm" data-ft-fert-open="${f.id}">Detalhes</button>
                                                    <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm ft-ic-btn" data-ft-fert-menu="${f.id}" aria-label="Mais ações" title="Mais ações">
                                                        ${IC.dots}
                                                    </button>
                                                    ${isMenuOpen ? `
                                                        <div class="ft-pop is-menu" data-ft-pop-box>
                                                            <button type="button" class="ft-pop-item" data-ft-fert-open="${f.id}">Ver detalhes completos</button>
                                                            ${CAN_WRITE ? `<a class="ft-pop-item" href="/fertilizacao/agendamentos/novo?copy_fert_id=${f.id}">Agendar novamente</a>` : ''}
                                                            ${CAN_WRITE ? `<a class="ft-pop-item" href="/fertilizacao/${f.id}/editar">Editar aplicação</a>` : ''}
                                                            ${CAN_WRITE ? `
                                                                <div class="ft-pop-div"></div>
                                                                <button type="button" class="ft-pop-item is-danger" data-ft-fert-del="${f.id}">Excluir</button>
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
            ${renderPaginationBar(pg)}
        `;

        listEl.querySelectorAll('[data-ft-fert-open]').forEach((b) => {
            b.addEventListener('click', () => { state.openMenuId = null; openDetailDrawer(b.dataset.ftFertOpen); });
        });
        listEl.querySelectorAll('[data-ft-fert-toggle-day]').forEach((b) => {
            b.addEventListener('click', () => {
                const dt = b.dataset.ftFertToggleDay;
                state.collapsedDates[dt] = !state.collapsedDates[dt];
                state.openMenuId = null;
                render();
            });
        });
        listEl.querySelectorAll('[data-ft-fert-menu]').forEach((b) => {
            b.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = Number(b.dataset.ftFertMenu);
                state.openMenuId = state.openMenuId === id ? null : id;
                render();
            });
        });
        listEl.querySelectorAll('[data-ft-pop-box]').forEach((box) => box.addEventListener('click', (e) => e.stopPropagation()));
        listEl.querySelectorAll('[data-ft-fert-del]').forEach((b) => {
            b.addEventListener('click', () => {
                const f = ROWS.find((r) => r.id === Number(b.dataset.ftFertDel));
                state.openMenuId = null;
                render();
                if (f) openDeleteDialog(f);
            });
        });
        listEl.querySelectorAll('[data-ft-fert-pag]').forEach((b) => {
            b.addEventListener('click', () => {
                state.page = Number(b.dataset.ftFertPag);
                state.openMenuId = null;
                render();
                listEl.closest('.ft-stage')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            });
        });
    }

    root.querySelectorAll('[data-ft-fert-filter]').forEach((b) => {
        b.addEventListener('click', () => { state.filter = b.dataset.ftFertFilter; state.page = 1; state.openMenuId = null; render(); });
    });
    root.querySelectorAll('[data-ft-plot-select]').forEach((b) => {
        b.addEventListener('click', () => {
            const id = Number(b.dataset.ftPlotSelect);
            state.plotId = state.plotId === id ? null : id;
            state.page = 1;
            state.openMenuId = null;
            render();
        });
    });
    clearPlotBtn?.addEventListener('click', () => { state.plotId = null; state.page = 1; state.openMenuId = null; render(); });
    root.querySelector('[data-ft-fert-search]')?.addEventListener('input', (e) => {
        state.search = e.target.value; state.page = 1; state.openMenuId = null; render();
    });
    toggleAllBtn?.addEventListener('click', () => {
        const mode = toggleAllBtn.dataset.mode || 'collapse';
        const dates = [...new Set(ROWS.map((r) => r.application_date || '0000-00-00'))];
        if (mode === 'collapse') dates.forEach((d) => { state.collapsedDates[d] = true; });
        else state.collapsedDates = {};
        state.openMenuId = null;
        render();
    });
    document.querySelectorAll('[data-ft-close-dialog]').forEach((b) => {
        b.addEventListener('click', () => b.closest('dialog')?.close());
    });
    [detailDialog, deleteDialog].forEach((d) => {
        d?.addEventListener('click', (e) => { if (e.target === d) d.close(); });
    });
    window.addEventListener('click', () => {
        if (state.openMenuId !== null) { state.openMenuId = null; render(); }
    });
    window.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && state.openMenuId !== null) { state.openMenuId = null; render(); }
    });

    render();
})();
