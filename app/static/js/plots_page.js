(() => {
    const dataEl = document.getElementById('plots-overview-data');
    const root = document.querySelector('[data-pl-page]');
    if (!dataEl || !root) return;

    let payload = {};
    try {
        payload = JSON.parse(dataEl.textContent || '{}');
    } catch (err) {
        console.error('Erro ao ler dados de setores:', err);
        return;
    }

    const items = Array.isArray(payload.items) ? payload.items : [];
    const byId = new Map(items.map((item) => [Number(item.id), item]));
    const canWrite = Boolean(payload.canWrite);
    const activeSeasonName = payload.activeSeasonName || '';
    const urls = payload.urls || {};
    const perms = payload.perms || {};
    const showFarmName = !payload.farmScopeLocked;

    const state = {
        viewMode: 'cards',
        quickFilter: 'all',
        search: payload.initialFilters?.q || '',
        farmId: payload.initialFilters?.farmId || '',
        variety: payload.initialFilters?.variety || '',
        group: '',
        sort: payload.initialFilters?.sort || 'name',
    };

    const cardsContainer = document.querySelector('[data-pl-cards-view]');
    const tableContainer = document.querySelector('[data-pl-table-view]');
    const groupsContainer = document.querySelector('[data-pl-groups-view]');
    const emptyFilterEl = document.querySelector('[data-pl-empty-filter]');
    const filterSummaryBar = document.querySelector('[data-pl-filter-summary]');
    const filterSummaryText = document.querySelector('[data-pl-filter-summary-text]');
    const searchInput = document.querySelector('[data-pl-search]');
    const farmSelect = document.querySelector('[data-pl-filter-farm]');
    const varietySelect = document.querySelector('[data-pl-filter-variety]');
    const groupSelect = document.querySelector('[data-pl-filter-group]');
    const sortSelect = document.querySelector('[data-pl-sort]');
    const clearVarietyBtn = document.querySelector('[data-pl-clear-variety]');
    const viewButtons = Array.from(document.querySelectorAll('[data-pl-view-mode]'));
    const quickChipButtons = Array.from(document.querySelectorAll('[data-pl-quick-filter]'));
    const varietyTriggers = Array.from(document.querySelectorAll('[data-pl-pick-variety]'));
    const cardElsById = new Map(
        Array.from(document.querySelectorAll('[data-pl-card]')).map((el) => [Number(el.dataset.plCard), el]),
    );

    const drawerBackdrop = document.getElementById('pl-drawer-backdrop');
    const drawerColorDot = document.getElementById('pl-drawer-dot');
    const drawerEyebrow = document.getElementById('pl-drawer-eyebrow');
    const drawerTitle = document.getElementById('pl-drawer-title');
    const drawerSub = document.getElementById('pl-drawer-sub');
    const drawerBody = document.getElementById('pl-drawer-body');
    const drawerFoot = document.getElementById('pl-drawer-foot');

    const delDialog = document.getElementById('pl-delete-modal');
    const delTitle = document.getElementById('pl-del-title');
    const delSummary = document.getElementById('pl-del-summary');
    const delImpactList = document.getElementById('pl-del-impact-list');
    const delAltRenew = document.getElementById('pl-del-alt-renew');
    const delConfirmLabel = document.getElementById('pl-del-confirm-label');
    const delConfirmCheck = document.getElementById('pl-del-confirm-check');
    const delConfirmName = document.getElementById('pl-del-confirm-name');
    const delForm = document.getElementById('pl-del-form');
    const delSubmitBtn = document.getElementById('pl-del-submit');

    const mobileMq = window.matchMedia('(max-width: 639px)');

    const esc = (value) =>
        String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');

    const normalize = (value) =>
        String(value ?? '')
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .trim();

    const fmtDec = (val, dec = 1) =>
        Number(val || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });

    const fmtInt = (val) => Math.round(Number(val || 0)).toLocaleString('pt-BR');

    const fmtMoney = (val) =>
        Number(val || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 });

    const getFilteredItems = () => {
        const q = normalize(state.search);
        const filtered = items.filter((p) => {
            if (state.quickFilter === 'season' && !p.in_active_season) return false;
            if (state.quickFilter === 'irrigated' && !p.is_irrigated) return false;
            if (state.quickFilter === 'no_map' && p.has_map) return false;
            if (state.quickFilter === 'forming' && p.stage !== 'form') return false;
            if (state.quickFilter === 'attention' && !p.needs_attention) return false;
            if (state.farmId && String(p.farm_id || '') !== String(state.farmId)) return false;
            if (state.variety && p.variety_name !== state.variety) return false;
            if (state.group && (p.group_name || '') !== state.group) return false;
            if (q) {
                const haystack = normalize(
                    `${p.name} ${p.farm_name} ${p.variety_name} ${p.group_name} ${p.field_reference} ${p.soil_texture_label} ${p.notes} ${p.irrigation_label}`,
                );
                if (!haystack.includes(q)) return false;
            }
            return true;
        });

        const sorted = [...filtered];
        if (state.sort === 'name') {
            sorted.sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'));
        } else if (state.sort === 'area_desc') {
            sorted.sort((a, b) => (b.area || 0) - (a.area || 0));
        } else if (state.sort === 'area_asc') {
            sorted.sort((a, b) => (a.area || 0) - (b.area || 0));
        } else if (state.sort === 'yield_desc') {
            sorted.sort((a, b) => (b.estimated_sacks || 0) - (a.estimated_sacks || 0));
        } else if (state.sort === 'planting_desc') {
            sorted.sort((a, b) => (b.planting_date_iso || '').localeCompare(a.planting_date_iso || ''));
        } else if (state.sort === 'planting_asc') {
            sorted.sort((a, b) => {
                const da = a.planting_date_iso || '9999-12-31';
                const db = b.planting_date_iso || '9999-12-31';
                return da.localeCompare(db);
            });
        }
        return sorted;
    };

    const syncVarietyPanoramaUi = () => {
        if (clearVarietyBtn) clearVarietyBtn.hidden = !state.variety;
        varietyTriggers.forEach((btn) => {
            const name = btn.dataset.plPickVariety || '';
            const isActive = Boolean(state.variety && state.variety === name);
            const isDimmed = Boolean(state.variety && state.variety !== name);
            if (btn.classList.contains('pl-var-seg')) {
                btn.classList.toggle('is-dimmed', isDimmed);
            } else {
                btn.classList.toggle('is-active', isActive);
                btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
            }
        });
    };

    const renderTableHtml = (list) => {
        const rows = list
            .map((p) => {
                const spacing =
                    p.spacing_row !== null && p.spacing_row !== undefined
                        ? `${fmtDec(p.spacing_row, 1)} m × ${p.spacing_plant_cm !== null && p.spacing_plant_cm !== undefined ? `${p.spacing_plant_cm} cm` : '—'}`
                        : '—';
                const estHtml =
                    p.estimated_sacks > 0
                        ? `<strong>${fmtInt(p.estimated_sacks)} sc</strong><br><small style="color:#446A36;font-weight:600;">${p.sc_per_ha !== null ? `${fmtDec(p.sc_per_ha, 1)} sc/ha` : ''}</small>`
                        : `<span style="color:#7A857E;">—</span>`;
                const mapHtml = p.has_map
                    ? `<a href="/setores?edit_id=${p.id}&amp;plot_map=1" class="pd-chip is-ok" data-pl-stop-row style="text-decoration:none;">Ver mapa</a>`
                    : `<a href="/mapa?edit_plot_id=${p.id}" class="pd-chip is-warn" data-pl-stop-row style="text-decoration:none;">Desenhar</a>`;
                const editBtn = canWrite
                    ? `<a href="/setores?edit_id=${p.id}" class="sf-icon-btn" style="width:34px;height:34px;" aria-label="Editar ${esc(p.name)}" title="Editar" data-pl-stop-row>
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 1 1 3 3L7 19l-4 1 1-4Z"/></svg>
                       </a>`
                    : '';

                return `
                    <tr data-pl-open-drawer="${p.id}">
                        <td>
                            <div style="display:flex;align-items:center;gap:10px;">
                                <span class="pl-dot" style="background:${esc(p.map_color)};" aria-hidden="true"></span>
                                <div>
                                    <strong style="color:#16201A;">${esc(p.name)}</strong>
                                    <div style="font-size:12px;color:#5B6660;">${showFarmName ? `${esc(p.farm_name)} · ` : ''}${p.group_name ? `Grupo ${esc(p.group_name)}` : 'Sem grupo'}</div>
                                </div>
                            </div>
                        </td>
                        <td>
                            <div style="font-weight:500;color:#16201A;">${esc(p.variety_name)}</div>
                            <div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:3px;">
                                ${p.in_active_season && activeSeasonName ? `<span class="pd-chip is-ok" style="padding:1px 7px;font-size:11px;">${esc(activeSeasonName)}</span>` : ''}
                                ${p.stage === 'form' ? `<span class="pd-chip is-info" style="padding:1px 7px;font-size:11px;">Formação</span>` : ''}
                            </div>
                        </td>
                        <td class="is-num"><strong>${fmtDec(p.area, 1)} ha</strong></td>
                        <td class="is-num">
                            ${p.plant_count > 0 ? `<strong>${fmtInt(p.plant_count)}</strong><br><small style="color:#5B6660;">${fmtInt(p.density)}/ha</small>` : '<span style="color:#7A857E;">—</span>'}
                        </td>
                        <td style="color:#5B6660;">${spacing}</td>
                        <td class="is-num">${estHtml}</td>
                        <td><span class="pd-chip ${p.is_irrigated ? 'is-info' : 'is-muted'}">${esc(p.irrigation_label)}</span></td>
                        <td>${mapHtml}</td>
                        <td class="is-num" data-pl-stop-row>
                            <div style="display:inline-flex;align-items:center;gap:6px;">
                                ${editBtn}
                                <button type="button" class="sf-icon-btn" style="width:34px;height:34px;" aria-label="Abrir ficha de ${esc(p.name)}" title="Ficha rápida" data-pl-open-drawer="${p.id}">
                                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M5 12h14m-6-6 6 6-6 6"/></svg>
                                </button>
                            </div>
                        </td>
                    </tr>
                `;
            })
            .join('');

        return `
            <div class="pd-card pl-table-card">
                <div class="pl-table-wrap">
                    <table class="pl-table">
                        <thead>
                            <tr>
                                <th>Setor</th>
                                <th>Variedade e fase</th>
                                <th class="is-num">Área</th>
                                <th class="is-num">Plantas (pés/ha)</th>
                                <th>Espaçamento</th>
                                <th class="is-num">Estimativa</th>
                                <th>Irrigação</th>
                                <th>Mapa</th>
                                <th class="is-num">Ações</th>
                            </tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
            </div>
        `;
    };

    const renderGroupsView = (list) => {
        if (!groupsContainer) return;
        groupsContainer.innerHTML = '';
        const grouped = new Map();
        list.forEach((p) => {
            const key = p.group_name || 'Sem grupo definido';
            if (!grouped.has(key)) grouped.set(key, []);
            grouped.get(key).push(p);
        });

        grouped.forEach((groupItems, groupName) => {
            const gArea = groupItems.reduce((sum, item) => sum + (item.area || 0), 0);
            const gSacks = groupItems.reduce((sum, item) => sum + (item.estimated_sacks || 0), 0);
            const block = document.createElement('div');
            block.className = 'pl-group-block';

            const head = document.createElement('div');
            head.className = 'pl-group-head';
            head.innerHTML = `
                <div style="display:flex;align-items:center;gap:8px;">
                    <h3>${groupName === 'Sem grupo definido' ? esc(groupName) : `Grupo ${esc(groupName)}`}</h3>
                    <span class="pd-chip is-muted">${groupItems.length} ${groupItems.length === 1 ? 'setor' : 'setores'}</span>
                </div>
                <span class="pl-group-totals">${fmtDec(gArea, 1)} ha${gSacks > 0 ? ` · ${fmtInt(gSacks)} sacas estimadas` : ''}</span>
            `;

            const listEl = document.createElement('div');
            listEl.className = 'pl-list';
            groupItems.forEach((item) => {
                const origCard = cardElsById.get(item.id);
                if (origCard) {
                    const clone = origCard.cloneNode(true);
                    clone.hidden = false;
                    listEl.appendChild(clone);
                }
            });

            block.appendChild(head);
            block.appendChild(listEl);
            groupsContainer.appendChild(block);
        });
    };

    const render = () => {
        const filtered = getFilteredItems();
        const filteredIds = new Set(filtered.map((item) => item.id));
        const hasFilters =
            state.quickFilter !== 'all' ||
            Boolean(state.search.trim()) ||
            Boolean(state.farmId) ||
            Boolean(state.variety) ||
            Boolean(state.group);

        // Atualiza estados de chips, botões e selects
        quickChipButtons.forEach((btn) => {
            btn.setAttribute('aria-pressed', btn.dataset.plQuickFilter === state.quickFilter ? 'true' : 'false');
        });
        viewButtons.forEach((btn) => {
            btn.setAttribute('aria-pressed', btn.dataset.plViewMode === state.viewMode ? 'true' : 'false');
        });
        if (varietySelect && varietySelect.value !== state.variety) varietySelect.value = state.variety;
        if (groupSelect && groupSelect.value !== state.group) groupSelect.value = state.group;
        if (farmSelect && farmSelect.value !== state.farmId) farmSelect.value = state.farmId;
        if (sortSelect && sortSelect.value !== state.sort) sortSelect.value = state.sort;

        syncVarietyPanoramaUi();

        // Resumo de filtro ativo
        if (filterSummaryBar && filterSummaryText) {
            filterSummaryBar.hidden = !hasFilters;
            if (hasFilters) {
                const areaSum = filtered.reduce((sum, item) => sum + (item.area || 0), 0);
                filterSummaryText.textContent = `Exibindo ${filtered.length} de ${items.length} setores (${fmtDec(areaSum, 1)} ha)`;
            }
        }

        // Estado vazio filtrado
        if (items.length > 0 && filtered.length === 0) {
            if (cardsContainer) cardsContainer.hidden = true;
            if (tableContainer) tableContainer.hidden = true;
            if (groupsContainer) groupsContainer.hidden = true;
            if (emptyFilterEl) emptyFilterEl.hidden = false;
            return;
        }
        if (emptyFilterEl) emptyFilterEl.hidden = true;

        const effectiveMode = mobileMq.matches && state.viewMode === 'table' ? 'cards' : state.viewMode;

        if (effectiveMode === 'cards') {
            if (cardsContainer) {
                cardsContainer.hidden = false;
                cardElsById.forEach((cardEl, id) => {
                    cardEl.hidden = !filteredIds.has(id);
                });
                filtered.forEach((item) => {
                    const cardEl = cardElsById.get(item.id);
                    if (cardEl) cardsContainer.appendChild(cardEl);
                });
            }
            if (tableContainer) tableContainer.hidden = true;
            if (groupsContainer) groupsContainer.hidden = true;
        } else if (effectiveMode === 'table') {
            if (cardsContainer) cardsContainer.hidden = true;
            if (groupsContainer) groupsContainer.hidden = true;
            if (tableContainer) {
                tableContainer.innerHTML = renderTableHtml(filtered);
                tableContainer.hidden = false;
            }
        } else {
            if (cardsContainer) cardsContainer.hidden = true;
            if (tableContainer) tableContainer.hidden = true;
            if (groupsContainer) {
                renderGroupsView(filtered);
                groupsContainer.hidden = false;
            }
        }
    };

    // Gaveta Lateral — Ficha Rápida do Setor
    const openDrawer = (plotId) => {
        const p = byId.get(Number(plotId));
        if (!p || !drawerBackdrop) return;

        if (drawerColorDot) drawerColorDot.style.background = p.map_color;
        if (drawerEyebrow) {
            drawerEyebrow.textContent = `${p.farm_name}${p.group_name ? ` · Grupo ${p.group_name}` : ''}`;
        }
        if (drawerTitle) drawerTitle.textContent = p.name;
        if (drawerSub) drawerSub.textContent = `${p.culture} · ${p.variety_name} · ${fmtDec(p.area, 1)} ha`;

        const mapBlock = p.preview_ready
            ? `<div style="position:relative;overflow:hidden;border-radius:14px;border:1px solid #D6DDD5;background:#1D2B22;aspect-ratio:16/9;">
                    <img src="${esc(p.full_url)}" alt="Mapa de ${esc(p.name)}" style="width:100%;height:100%;object-fit:cover;display:block;">
                    <a href="/setores?edit_id=${p.id}&amp;plot_map=1" style="position:absolute;right:10px;bottom:10px;background:rgba(15,21,19,.78);color:#fff;font-size:12px;font-weight:600;padding:6px 12px;border-radius:10px;text-decoration:none;">
                        Ampliar mapa →
                    </a>
               </div>`
            : p.has_map
              ? `<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;border-radius:14px;border:1px solid #BFD6B6;background:#EEF3EB;padding:13px 14px;">
                    <div>
                        <strong style="font-size:13.5px;color:#16201A;">Perímetro georreferenciado salvo</strong>
                        <p style="margin:2px 0 0;font-size:12px;color:#294127;">A imagem de prévia está sendo gerada. Você já pode abrir no mapa.</p>
                    </div>
                    <a href="/mapa?edit_plot_id=${p.id}" class="sf-btn sf-btn-secondary" style="min-height:36px;font-size:12.5px;">Abrir mapa</a>
                 </div>`
              : `<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;border-radius:14px;border:1.5px dashed #F0D9A6;background:#FFFBF2;padding:13px 14px;">
                    <div>
                        <strong style="font-size:13.5px;color:#16201A;">Perímetro ainda não desenhado</strong>
                        <p style="margin:2px 0 0;font-size:12px;color:#7A4F00;">Desenhe no mapa para medir a área real e visualizar no Mapa da Fazenda.</p>
                    </div>
                    ${canWrite ? `<a href="/mapa?edit_plot_id=${p.id}" class="sf-btn sf-btn-primary" style="min-height:36px;font-size:12.5px;">Desenhar</a>` : ''}
                 </div>`;

        const spacingText =
            p.spacing_row !== null && p.spacing_row !== undefined
                ? `${fmtDec(p.spacing_row, 1)} m × ${p.spacing_plant_cm !== null && p.spacing_plant_cm !== undefined ? `${p.spacing_plant_cm} cm` : '—'}`
                : '—';

        const estText =
            p.estimated_sacks > 0
                ? `${fmtInt(p.estimated_sacks)} sc${p.sc_per_ha !== null ? ` (${fmtDec(p.sc_per_ha, 1)}/ha)` : ''}`
                : p.stage === 'form'
                  ? 'Em formação'
                  : 'Não informada';

        const pastPlantingsHtml =
            p.past_plantings && p.past_plantings.length
                ? `<div style="border-top:1px solid #EEF1ED;padding-top:10px;font-size:12px;color:#5B6660;">
                        <strong style="color:#16201A;">Histórico de plantio:</strong>
                        ${p.past_plantings.map((pp) => `${esc(pp.variety)} (${esc(pp.period)} · ${esc(pp.reason)})`).join(' · ')}
                   </div>`
                : '';

        const irrigBlock = p.is_irrigated
            ? `<section class="pl-drawer-box">
                    <h3>Sistema de irrigação (${esc(p.irrigation_label)})</h3>
                    <dl class="pl-drawer-kv" style="grid-template-columns:repeat(3,minmax(0,1fr));">
                        <div><dt>Linhas</dt><dd>${p.irrigation_line_count || '—'}</dd></div>
                        <div><dt>Comp. médio</dt><dd>${p.irrigation_line_length_meters ? `${fmtDec(p.irrigation_line_length_meters, 0)} m` : '—'}</dd></div>
                        <div><dt>${p.irrigation_type === 'gotejo' ? 'Vazão' : 'Aspersores'}</dt><dd>${
                            p.irrigation_type === 'gotejo'
                                ? p.drip_liters_per_hour
                                    ? `${fmtDec(p.drip_liters_per_hour, 1)} L/h`
                                    : '—'
                                : p.sprinkler_count
                                  ? `${p.sprinkler_count} un.`
                                  : '—'
                        }</dd></div>
                    </dl>
               </section>`
            : '';

        const modCards = [];
        if (perms.finance) {
            modCards.push(`
                <a class="pl-mod-card" href="${esc(urls.sectorCosts || '/custos-por-setor')}">
                    <span>Custos${activeSeasonName ? ' na safra' : ''}</span>
                    <strong>${p.season_cost > 0 ? fmtMoney(p.season_cost) : 'Sem lançamentos'}</strong>
                    <small>${p.cost_per_ha > 0 ? `${fmtMoney(p.cost_per_ha)}/ha` : 'Ver custos por setor →'}</small>
                </a>
            `);
        }
        modCards.push(`
            <a class="pl-mod-card" href="${esc(urls.fertilization || '/fertilizacao')}">
                <span>Adubação</span>
                <strong style="${p.pending_fert_count > 0 ? 'color:#7A4F00;' : ''}">${
                    p.pending_fert_count > 0
                        ? `${p.pending_fert_count} agendamento(s) pendente(s)`
                        : p.last_fert_summary
                          ? esc(p.last_fert_summary)
                          : 'Nenhum registro'
                }</strong>
                <small>Abrir adubações →</small>
            </a>
        `);
        modCards.push(`
            <a class="pl-mod-card" href="${esc(urls.soil || '/analise-solo')}">
                <span>Análise de solo</span>
                <strong>${p.last_soil_summary ? esc(p.last_soil_summary) : 'Sem análise cadastrada'}</strong>
                <small>${p.attachments_count} anexo(s) no setor</small>
            </a>
        `);
        modCards.push(`
            <a class="pl-mod-card" href="${esc(urls.production || '/producao')}">
                <span>Produção</span>
                <strong style="color:#446A36;">Colheitas e lotes →</strong>
                <small>${p.last_harvest_avg ? `Média histórica: ${fmtDec(p.last_harvest_avg, 1)} sc/ha` : 'Acompanhar produção'}</small>
            </a>
        `);

        const notesBlock =
            p.field_reference || p.notes
                ? `<section class="pl-drawer-box" style="background:#F6F8F5;">
                        ${p.field_reference ? `<p style="margin:0;font-size:12.5px;color:#5B6660;"><strong style="color:#16201A;">Referência no campo:</strong> ${esc(p.field_reference)}</p>` : ''}
                        ${p.notes ? `<p style="margin:0;font-size:12.5px;color:#5B6660;white-space:pre-line;"><strong style="color:#16201A;">Observações:</strong> ${esc(p.notes)}</p>` : ''}
                   </section>`
                : '';

        if (drawerBody) {
            drawerBody.innerHTML = `
                ${mapBlock}
                <div style="display:flex;flex-wrap:wrap;gap:6px;">
                    ${activeSeasonName ? `<span class="pd-chip ${p.in_active_season ? 'is-ok' : 'is-muted'}">${p.in_active_season ? `Na ${esc(activeSeasonName)}` : `Fora da ${esc(activeSeasonName)}`}</span>` : ''}
                    <span class="pd-chip ${p.stage === 'form' ? 'is-info' : 'is-done'}">${p.stage === 'form' ? 'Lavoura em formação' : p.stage === 'prod' ? 'Em produção' : 'Plantio não informado'}</span>
                    <span class="pd-chip is-muted">${esc(p.irrigation_label)}</span>
                    ${p.soil_texture_label ? `<span class="pd-chip is-muted">Solo ${esc(p.soil_texture_label)}</span>` : ''}
                </div>
                <section class="pl-drawer-box">
                    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
                        <h3>Plantio e estande</h3>
                        ${canWrite ? `<a href="/setores?edit_id=${p.id}&amp;renovar=1" class="pl-link-btn" style="text-decoration:none;">Renovar plantio</a>` : ''}
                    </div>
                    <dl class="pl-drawer-kv">
                        <div><dt>Data / Idade</dt><dd>${esc(p.planting_month_year)}${p.age_label ? ` · ${esc(p.age_label)}` : ''}</dd></div>
                        <div><dt>Total de plantas</dt><dd>${p.plant_count > 0 ? `${fmtInt(p.plant_count)} (${fmtInt(p.density)}/ha)` : '—'}</dd></div>
                        <div><dt>Espaçamento</dt><dd>${spacingText}</dd></div>
                        <div><dt>Estimativa</dt><dd>${estText}${p.last_harvest_avg ? `<span style="display:block;font-size:11px;font-weight:500;color:#5B6660;">Média 3 safras: ${fmtDec(p.last_harvest_avg, 1)} sc/ha</span>` : ''}</dd></div>
                    </dl>
                    ${pastPlantingsHtml}
                </section>
                ${irrigBlock}
                <section class="pl-drawer-box">
                    <div>
                        <h3>Acompanhamento nos módulos</h3>
                        <p style="margin:2px 0 0;font-size:12px;color:#5B6660;">Atalhos integrados para o histórico deste setor</p>
                    </div>
                    <div class="pl-mod-grid">${modCards.join('')}</div>
                </section>
                ${notesBlock}
            `;
        }

        if (drawerFoot) {
            drawerFoot.innerHTML = `
                <a href="/mapa?edit_plot_id=${p.id}" class="sf-btn sf-btn-secondary" style="min-height:38px;">${p.has_map ? 'Editar no mapa' : 'Desenhar no mapa'}</a>
                ${
                    canWrite
                        ? `<a href="/setores?edit_id=${p.id}" class="sf-btn sf-btn-primary" style="min-height:38px;">Editar setor</a>`
                        : `<button type="button" class="sf-btn sf-btn-primary" style="min-height:38px;" data-pl-drawer-close>Fechar</button>`
                }
            `;
        }

        drawerBackdrop.classList.add('is-open');
        drawerBackdrop.setAttribute('aria-hidden', 'false');
    };

    const closeDrawer = () => {
        if (!drawerBackdrop) return;
        drawerBackdrop.classList.remove('is-open');
        drawerBackdrop.setAttribute('aria-hidden', 'true');
    };

    // Modal de Exclusão Redesenhado
    const openDeleteModal = (plotId) => {
        const p = byId.get(Number(plotId));
        if (!p || !delDialog) return;

        // Fecha menus <details> abertos
        document.querySelectorAll('details.sf-menu[open]').forEach((el) => el.removeAttribute('open'));

        if (delTitle) delTitle.textContent = `Excluir ${p.name}?`;
        if (delConfirmName) delConfirmName.textContent = p.name;

        if (delSummary) {
            const mediaHtml = p.preview_ready
                ? `<div class="pl-del-summary-thumb"><img src="${esc(p.thumb_url)}" alt=""></div>`
                : `<div class="pl-del-summary-swatch"><span class="pl-dot" style="width:16px;height:16px;background:${esc(p.map_color)};"></span></div>`;

            delSummary.innerHTML = `
                ${mediaHtml}
                <div style="min-width:0;flex:1;">
                    <div style="display:flex;align-items:center;gap:7px;">
                        <span class="pl-dot" style="background:${esc(p.map_color)};"></span>
                        <strong style="font-size:14.5px;color:#16201A;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(p.name)}</strong>
                    </div>
                    <p style="margin:2px 0 0;font-size:12px;color:#5B6660;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
                        ${esc(p.farm_name)}${p.group_name ? ` · Grupo ${esc(p.group_name)}` : ''} · ${esc(p.variety_name)}
                    </p>
                    <div class="pl-del-summary-pills">
                        <span>${fmtDec(p.area, 1)} ha</span>
                        ${p.plant_count > 0 ? `<span>${fmtInt(p.plant_count)} plantas</span>` : ''}
                        ${p.in_active_season && activeSeasonName ? `<span class="is-season">${esc(activeSeasonName)}</span>` : ''}
                    </div>
                </div>
            `;
        }

        if (delImpactList) {
            const impacts = [];
            if (p.in_active_season && activeSeasonName) {
                impacts.push(
                    `Desvincula o setor da <strong>${esc(activeSeasonName)}</strong>${p.estimated_sacks > 0 ? ` e remove a estimativa de <strong>${fmtInt(p.estimated_sacks)} sacas</strong>` : ''}.`,
                );
            }
            if (p.has_map) {
                impacts.push(
                    `Apaga o <strong>perímetro desenhado no mapa</strong>${p.attachments_count > 0 ? ` e ${p.attachments_count} anexo(s) vinculados` : ''}.`,
                );
            } else if (p.attachments_count > 0) {
                impacts.push(`Remove <strong>${p.attachments_count} anexo(s)</strong> (laudos e documentos) vinculados ao setor.`);
            }
            impacts.push(
                'Exclui o histórico de plantio e os registros de campo ligados diretamente a este setor (adubações, irrigações, análises e aplicações).',
            );
            delImpactList.innerHTML = impacts.map((html) => `<li><span>${html}</span></li>`).join('');
        }

        if (delAltRenew) {
            delAltRenew.setAttribute('href', `/setores?edit_id=${p.id}&renovar=1`);
        }

        if (delForm) {
            delForm.setAttribute('action', `/setores/${p.id}/excluir`);
        }

        if (delConfirmCheck && delConfirmLabel && delSubmitBtn) {
            delConfirmCheck.checked = false;
            delConfirmLabel.classList.remove('is-checked');
            delSubmitBtn.disabled = true;
        }

        delDialog.showModal();
    };

    const closeDeleteModal = () => {
        if (delDialog?.open) delDialog.close();
    };

    // Eventos
    searchInput?.addEventListener('input', () => {
        state.search = searchInput.value || '';
        render();
    });
    farmSelect?.addEventListener('change', () => {
        state.farmId = farmSelect.value || '';
        render();
    });
    varietySelect?.addEventListener('change', () => {
        state.variety = varietySelect.value || '';
        render();
    });
    groupSelect?.addEventListener('change', () => {
        state.group = groupSelect.value || '';
        render();
    });
    sortSelect?.addEventListener('change', () => {
        state.sort = sortSelect.value || 'name';
        render();
    });

    clearVarietyBtn?.addEventListener('click', () => {
        state.variety = '';
        render();
    });

    delConfirmCheck?.addEventListener('change', () => {
        const checked = Boolean(delConfirmCheck.checked);
        delConfirmLabel?.classList.toggle('is-checked', checked);
        if (delSubmitBtn) delSubmitBtn.disabled = !checked;
    });

    delForm?.addEventListener('submit', () => {
        window.showPageLoader?.('Excluindo setor', 'Removendo o setor e atualizando os dados da fazenda.');
    });

    delDialog?.addEventListener('click', (event) => {
        if (event.target === delDialog) closeDeleteModal();
    });

    document.addEventListener('click', (event) => {
        const quickChip = event.target.closest('[data-pl-quick-filter]');
        if (quickChip) {
            state.quickFilter = quickChip.dataset.plQuickFilter || 'all';
            render();
            return;
        }

        const viewBtn = event.target.closest('[data-pl-view-mode]');
        if (viewBtn) {
            state.viewMode = viewBtn.dataset.plViewMode || 'cards';
            render();
            return;
        }

        const pickVar = event.target.closest('[data-pl-pick-variety]');
        if (pickVar) {
            const v = pickVar.dataset.plPickVariety || '';
            state.variety = state.variety === v ? '' : v;
            render();
            return;
        }

        if (event.target.closest('[data-pl-jump-attention]')) {
            state.quickFilter = 'attention';
            render();
            document.getElementById('pl-list-anchor')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            return;
        }

        if (event.target.closest('[data-pl-reset-filters]')) {
            state.quickFilter = 'all';
            state.search = '';
            state.farmId = '';
            state.variety = '';
            state.group = '';
            if (searchInput) searchInput.value = '';
            render();
            return;
        }

        const delOpen = event.target.closest('[data-pl-delete-open]');
        if (delOpen) {
            event.preventDefault();
            event.stopPropagation();
            openDeleteModal(delOpen.dataset.plDeleteOpen);
            return;
        }

        if (event.target.closest('[data-pl-delete-close]')) {
            closeDeleteModal();
            return;
        }

        if (event.target.closest('[data-pl-drawer-close]') || event.target === drawerBackdrop) {
            closeDrawer();
            return;
        }

        if (event.target.closest('[data-pl-stop-row]')) return;

        const drawerOpen = event.target.closest('[data-pl-open-drawer]');
        if (drawerOpen) {
            event.preventDefault();
            openDrawer(drawerOpen.dataset.plOpenDrawer);
        }
    });

    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && drawerBackdrop?.classList.contains('is-open')) {
            closeDrawer();
        }
    });

    mobileMq.addEventListener('change', () => {
        if (mobileMq.matches && state.viewMode === 'table') {
            state.viewMode = 'cards';
        }
        render();
    });

    render();
})();
