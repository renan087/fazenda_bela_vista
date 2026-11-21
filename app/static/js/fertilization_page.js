(() => {
    const root = document.querySelector('[data-ft-fert-page]');
    const dataEl = document.getElementById('ft-fert-data');
    if (!root || !dataEl) return;

    let DATA;
    try { DATA = JSON.parse(dataEl.textContent || '{}'); } catch { return; }

    const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
    const fmtQty = (n) => {
        const v = Number(n || 0);
        return Number.isInteger(v) ? v.toLocaleString('pt-BR') : v.toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 2 });
    };
    const fmtBRL = (n) => 'R$ ' + Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    const CAN_WRITE = Boolean(DATA.canWrite);
    const ROWS = DATA.rows || [];

    const state = {
        filter: 'all',
        plotId: null,
        search: '',
    };

    const listEl = root.querySelector('[data-ft-fert-list]');
    const clearPlotBtn = root.querySelector('[data-ft-plot-clear]');
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
            listEl.innerHTML = ROWS.length
                ? '<div class="pd-empty"><strong>Nenhuma aplicação neste filtro.</strong><span>Ajuste os filtros ou a busca para ver o histórico completo.</span><button type="button" class="sf-btn sf-btn-secondary" data-ft-fert-reset>Limpar filtros</button></div>'
                : `<div class="pd-empty"><strong>Nenhuma aplicação registrada nesta safra.</strong><span>Registre diretamente por aqui ou conclua um agendamento planejado na aba Agendamentos.</span>${CAN_WRITE ? '<a class="sf-btn sf-btn-primary" href="/fertilizacao/nova">Registrar primeira aplicação</a>' : ''}</div>`;
            listEl.querySelector('[data-ft-fert-reset]')?.addEventListener('click', () => {
                state.filter = 'all'; state.plotId = null; state.search = '';
                const sInp = root.querySelector('[data-ft-fert-search]');
                if (sInp) sInp.value = '';
                render();
            });
            return;
        }

        listEl.innerHTML = rows.map((f) => `
            <article class="ft-fert-card">
                <div class="ft-fert-main">
                    <div class="ft-fert-top">
                        <span class="ft-date-pill">${esc(f.application_date_br)}</span>
                        <button type="button" class="ft-row-title" data-ft-fert-open="${f.id}">${esc(f.plot_name)}</button>
                        <span class="pd-chip is-${esc(f.method_tone)}">${esc(f.method_label)}${f.duration_minutes ? ` · ${f.duration_minutes} min` : ''}</span>
                        ${f.schedule_id ? `<span class="pd-chip is-ok">Da Agenda #${f.schedule_id}</span>` : '<span class="pd-chip is-muted">Avulsa</span>'}
                        ${f.irrigation_liters ? `<span class="pd-chip is-info">💧 ${fmtQty(f.irrigation_liters)} L</span>` : ''}
                    </div>
                    <div class="ft-fert-pills">
                        ${(f.items || []).length ? f.items.map((it) => `
                            <span class="ft-item-pill"><strong>${esc(it.name)}</strong> ${fmtQty(it.total_qty)} ${esc(it.unit)} <em>(${fmtQty(it.dose_per_ha)}/ha)</em></span>
                        `).join('') : `<span class="ft-item-pill"><strong>${esc(f.product)}</strong> ${esc(f.dose)}</span>`}
                    </div>
                    ${f.notes ? `<p class="ft-fert-notes">${esc(f.notes)}</p>` : ''}
                </div>
                <div class="ft-fert-side">
                    <div class="ft-fert-cost">
                        <strong>${fmtBRL(f.total_cost)}</strong>
                        <small>${fmtBRL(f.cost_per_ha)}/ha</small>
                    </div>
                    <div class="ft-fert-acts">
                        <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm" data-ft-fert-open="${f.id}">Detalhes</button>
                        ${CAN_WRITE ? `<a class="sf-btn sf-btn-secondary sf-btn-sm" href="/fertilizacao/agendamentos/novo?copy_fert_id=${f.id}" title="Agendar novamente">Repetir</a>` : ''}
                        ${CAN_WRITE ? `<a class="sf-btn sf-btn-ghost sf-btn-sm" href="/fertilizacao/${f.id}/editar">Editar</a>` : ''}
                    </div>
                </div>
            </article>
        `).join('');

        listEl.querySelectorAll('[data-ft-fert-open]').forEach((b) => {
            b.addEventListener('click', () => openDetailDrawer(b.dataset.ftFertOpen));
        });
    }

    root.querySelectorAll('[data-ft-fert-filter]').forEach((b) => {
        b.addEventListener('click', () => { state.filter = b.dataset.ftFertFilter; render(); });
    });
    root.querySelectorAll('[data-ft-plot-select]').forEach((b) => {
        b.addEventListener('click', () => {
            const id = Number(b.dataset.ftPlotSelect);
            state.plotId = state.plotId === id ? null : id;
            render();
        });
    });
    clearPlotBtn?.addEventListener('click', () => { state.plotId = null; render(); });
    root.querySelector('[data-ft-fert-search]')?.addEventListener('input', (e) => {
        state.search = e.target.value; render();
    });
    document.querySelectorAll('[data-ft-close-dialog]').forEach((b) => {
        b.addEventListener('click', () => b.closest('dialog')?.close());
    });
    [detailDialog, deleteDialog].forEach((d) => {
        d?.addEventListener('click', (e) => { if (e.target === d) d.close(); });
    });

    render();
})();
