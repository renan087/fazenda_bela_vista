(() => {
    const root = document.querySelector('[data-ft-wizard-page]');
    const dataEl = document.getElementById('ft-wizard-data');
    const form = root?.querySelector('[data-ftw-form]');
    if (!root || !dataEl || !form) return;

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
    const fmtDateWeekday = (iso) => {
        if (!iso || iso.length < 10) return '—';
        const days = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
        const dt = new Date(iso + 'T12:00:00');
        return `${days[dt.getDay()]}, ${fmtDate(iso)}`;
    };
    const addDays = (iso, d) => {
        const dt = new Date(iso + 'T12:00:00');
        dt.setDate(dt.getDate() + d);
        return dt.toISOString().slice(0, 10);
    };
    const addMonths = (iso, m) => {
        const dt = new Date(iso + 'T12:00:00');
        dt.setMonth(dt.getMonth() + m);
        return dt.toISOString().slice(0, 10);
    };

    const MODE = DATA.mode || 'new_schedule';
    const IS_SCHED = MODE.includes('schedule');
    const IS_EDIT = MODE.startsWith('edit_');
    const PLOTS = DATA.plots || [];
    const INPUTS = DATA.inputs || [];
    const RECS = DATA.recommendations || [];
    const WEATHER = DATA.weather || {};

    const METHOD_META = {
        fertirrigacao: { label: 'Fertirrigação', short: 'Fertirrigação', tone: 'info', dot: '#1F5FA8', desc: 'Via sistema de irrigação (lança água automaticamente no gotejo)' },
        adubacao_solida: { label: 'Adubação sólida', short: 'Sólida', tone: 'muted', dot: '#5B6660', desc: 'Granulado no solo (lanço ou projeção de copa)' },
        fertilizacao_foliar: { label: 'Fertilização foliar', short: 'Foliar', tone: 'brand', dot: '#446A36', desc: 'Pulverização nas folhas (sensível à chuva)' },
    };

    const init = DATA.initial || {};
    const wiz = {
        step: 1,
        plotIds: (init.plotIds || []).map(Number).filter(Boolean),
        date: init.date || DATA.today,
        method: init.method || 'fertirrigacao',
        durationMinutes: Number(init.durationMinutes || 45),
        responsible: init.responsible || '',
        recipeName: init.recipeName || '',
        notes: init.notes || '',
        doseMode: init.doseMode || 'per_ha',
        repeatPreset: 'none',
        repeatEvery: 7,
        repeatUnit: 'days',
        repeatCount: 2,
        skippedDates: [],
        quickQuery: '',
        catalogSearch: '',
        catalogCat: 'all',
        catalogOnlyInStock: true,
        catalogSort: 'popular',
        catalogLimit: 20,
        saveAsRecipe: false,
        saveRecipeName: '',
        items: (init.items || []).map((it) => ({ inputId: Number(it.inputId), value: Number(it.value || 0) })),
    };

    function checkMixCompatibility() {
        const names = wiz.items
            .filter((it) => it.inputId && Number(it.value) > 0)
            .map((it) => (getInput(it.inputId)?.name || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase());
        const alerts = [];
        const hasCalciumNitrate = names.some((n) => n.includes('nitrato') && n.includes('calcio'));
        const hasPhosOrSulf = names.some((n) => n.includes('map') || n.includes('mkp') || n.includes('fosfat') || n.includes('sulfato') || n.includes('superfosf'));
        if (hasCalciumNitrate && hasPhosOrSulf) {
            alerts.push({ level: 'err', text: 'Incompatibilidade no tanque: Nitrato de Cálcio não deve ser misturado com fosfatos (MAP/MKP) ou sulfatos — forma precipitado e entope gotejadores. Aplique em tanques ou dias separados.' });
        }
        const hasUrea = names.some((n) => n.includes('ureia'));
        const hasSsp = names.some((n) => n.includes('superfosfato simples') || /\bssp\b/.test(n));
        if (hasUrea && hasSsp) {
            alerts.push({ level: 'warn', text: 'Atenção à mistura: Ureia com Superfosfato Simples absorve umidade e empedra se misturada com antecedência. Misture apenas na hora da aplicação.' });
        }
        const hasLime = names.some((n) => n.includes('calcario'));
        const hasAmmoniacal = names.some((n) => n.includes('ureia') || n.includes('sulfato de amon') || n.includes('map'));
        if (hasLime && hasAmmoniacal) {
            alerts.push({ level: 'warn', text: 'Atenção agronômica: evite aplicar calcário junto com adubos nitrogenados amoniacais ou fosfatados na mesma operação — prefira intervalo mínimo de 60 dias para evitar perda de nitrogênio.' });
        }
        return alerts;
    }

    const stageEl = root.querySelector('[data-ftw-step-stage]');
    const summaryEl = root.querySelector('[data-ftw-summary]');
    const errorEl = root.querySelector('[data-ftw-error]');
    const backBtn = root.querySelector('[data-ftw-back]');
    const nextBtn = root.querySelector('[data-ftw-next]');
    const submitBtn = root.querySelector('[data-ftw-submit]');
    const payloadInput = root.querySelector('[data-ftw-payload-input]');
    const catalogDialog = document.getElementById('ftw-catalog-dialog');
    const catalogShell = catalogDialog?.querySelector('[data-ftw-catalog-shell]');

    const getPlot = (id) => PLOTS.find((p) => p.id === Number(id));
    const getInput = (id) => INPUTS.find((i) => i.id === Number(id));

    function getDates() {
        if (!wiz.date) return [];
        if (MODE !== 'new_schedule' || wiz.repeatPreset === 'none') return [wiz.date];
        let stepVal = wiz.repeatEvery || 7;
        let stepUnit = wiz.repeatUnit || 'days';
        const count = Math.min(36, Math.max(1, Number(wiz.repeatCount || 1)));
        if (wiz.repeatPreset === 'weekly') { stepVal = 7; stepUnit = 'days'; }
        else if (wiz.repeatPreset === 'biweekly') { stepVal = 15; stepUnit = 'days'; }
        else if (wiz.repeatPreset === 'monthly') { stepVal = 1; stepUnit = 'months'; }
        const out = [wiz.date];
        for (let i = 1; i <= count; i++) {
            out.push(stepUnit === 'months' ? addMonths(wiz.date, stepVal * i) : addDays(wiz.date, stepVal * i));
        }
        return out.filter((d) => !wiz.skippedDates.includes(d));
    }

    function calcQtyForPlot(item, plot) {
        const val = Number(item.value || 0);
        if (!plot || val <= 0) return 0;
        if (wiz.doseMode === 'per_ha') return Number(((plot.area > 0 ? val * plot.area : val)).toFixed(2));
        if (wiz.doseMode === 'per_plant') return Number(((plot.plants > 0 ? (val * plot.plants) / 1000 : val)).toFixed(2));
        return Number(val.toFixed(2));
    }

    function calcTotals() {
        const plots = wiz.plotIds.map(getPlot).filter(Boolean);
        const dates = getDates();
        const datesCount = Math.max(1, dates.length);
        const area = plots.reduce((a, p) => a + Number(p.area || 0), 0);
        const plants = plots.reduce((a, p) => a + Number(p.plants || 0), 0);
        const ops = plots.length * datesCount;
        const byInputMap = {};

        wiz.items.forEach((it) => {
            const inp = getInput(it.inputId);
            if (!inp) return;
            let sumQty = 0;
            plots.forEach((p) => { sumQty += calcQtyForPlot(it, p) * datesCount; });
            sumQty = Number(sumQty.toFixed(2));
            if (!byInputMap[inp.id]) byInputMap[inp.id] = { input: inp, qty: 0, cost: 0 };
            byInputMap[inp.id].qty = Number((byInputMap[inp.id].qty + sumQty).toFixed(2));
            byInputMap[inp.id].cost = Number((byInputMap[inp.id].qty * Number(inp.avgCost || 0)).toFixed(2));
        });

        let estCost = 0;
        const byInput = Object.values(byInputMap).map((row) => {
            estCost += row.cost;
            const after = Number((Number(row.input.available || 0) - row.qty).toFixed(2));
            return { ...row, after, short: after < -0.001 };
        });

        let irrigationLiters = 0;
        const nonIrrigatedPlots = [];
        if (wiz.method === 'fertirrigacao' && wiz.durationMinutes > 0) {
            plots.forEach((p) => {
                if (p.irrigation === 'gotejo' && p.litersPerMin > 0) {
                    irrigationLiters += p.litersPerMin * Number(wiz.durationMinutes) * datesCount;
                } else if (p.irrigation === 'none' || p.irrigation === 'sequeiro') {
                    nonIrrigatedPlots.push(p.name);
                }
            });
        }
        return { plots, dates, datesCount, area, plants, ops, estCost: Number(estCost.toFixed(2)), byInput, irrigationLiters: Math.round(irrigationLiters), nonIrrigatedPlots };
    }

    function showError(msg) {
        if (!errorEl) return;
        errorEl.textContent = msg || '';
        errorEl.hidden = !msg;
    }

    function validateStep(stepNum) {
        if (stepNum === 1) {
            if (!wiz.plotIds.length) { showError('Selecione ao menos um talhão para continuar.'); return false; }
            if (!wiz.date) { showError('Informe a data prevista ou da aplicação.'); return false; }
            if (wiz.method === 'fertirrigacao' && (!wiz.durationMinutes || wiz.durationMinutes < 1)) {
                showError('Informe a duração em minutos para o modo Fertirrigação.'); return false;
            }
        }
        if (stepNum === 2) {
            const valid = wiz.items.filter((it) => it.inputId && Number(it.value) > 0);
            if (!valid.length) { showError('Adicione ao menos um insumo com dose maior que zero.'); return false; }
        }
        if (stepNum === 3 && !IS_SCHED) {
            const totals = calcTotals();
            const short = totals.byInput.find((b) => b.short);
            if (short) {
                showError(`Estoque insuficiente de ${short.input.name} (falta ${fmtQty(Math.abs(short.after))} ${short.input.unit}). Ajuste a dose ou lance uma compra antes de registrar.`);
                return false;
            }
        }
        showError('');
        return true;
    }

    function doseUnitLabel(inp) {
        if (wiz.doseMode === 'per_ha') return `${inp?.unit || 'kg'}/ha`;
        if (wiz.doseMode === 'per_plant') return 'g/pé';
        return inp?.unit || 'kg';
    }

    // ---- PASSO 1 ----
    function renderStep1() {
        const dates = getDates();
        const hasGotejo = PLOTS.some((p) => p.irrigation === 'gotejo');
        return `
            <div class="ftw-sec">
                <div class="ftw-sec-head">
                    <div>
                        <h2>${IS_EDIT ? 'Talhão da operação' : 'Em quais talhões?'}</h2>
                        <p>${IS_EDIT ? 'Talhão vinculado a este registro.' : 'Selecione um ou mais talhões da safra ativa. A dose por hectare se ajusta sozinha à área de cada um.'}</p>
                    </div>
                    ${!IS_EDIT ? `
                        <div class="ftw-quick-picks">
                            ${hasGotejo ? '<button type="button" class="sf-btn sf-btn-ghost sf-btn-sm" data-ftw-plots-quick="gotejo">Só gotejo</button>' : ''}
                            <button type="button" class="sf-btn sf-btn-ghost sf-btn-sm" data-ftw-plots-quick="all">Todos</button>
                        </div>
                    ` : ''}
                </div>
                <div class="ftw-plot-grid">
                    ${PLOTS.map((p) => {
                        const sel = wiz.plotIds.includes(p.id);
                        return `
                            <button type="button" class="ftw-pick ${sel ? 'is-selected' : ''}" data-ftw-plot="${p.id}">
                                <div class="ftw-pick-top">
                                    <strong>${esc(p.name)}</strong>
                                    <span class="ftw-pick-chk">${sel ? '✓' : ''}</span>
                                </div>
                                <div class="ftw-pick-sub">${fmtNum(p.area, 1)} ha${p.plants ? ` · ${fmtQty(p.plants)} pés` : ''}${p.variety ? ` · ${esc(p.variety)}` : ''}</div>
                                <div class="ftw-pick-tags">
                                    <span class="pd-chip ${p.irrigation === 'gotejo' ? 'is-info' : 'is-muted'}">${esc(p.irrigationLabel)}</span>
                                </div>
                            </button>
                        `;
                    }).join('')}
                </div>
            </div>

            <div class="ftw-sec">
                <h2 class="ftw-label">Modo de aplicação</h2>
                <div class="ftw-method-grid">
                    ${Object.entries(METHOD_META).map(([code, m]) => `
                        <button type="button" class="ftw-pick ${wiz.method === code ? 'is-selected' : ''}" data-ftw-method="${code}">
                            <div class="ftw-method-title"><i style="background:${m.dot}"></i><strong>${m.label}</strong></div>
                            <p>${m.desc}</p>
                        </button>
                    `).join('')}
                </div>
            </div>

            <div class="ftw-fields-3">
                <label class="ft-field">
                    <span>${IS_SCHED ? 'Data prevista' : 'Data da aplicação'}</span>
                    <input type="date" value="${esc(wiz.date)}" data-ftw-field="date" required>
                </label>
                ${wiz.method === 'fertirrigacao' ? `
                    <label class="ft-field">
                        <span>Duração (minutos) *</span>
                        <input type="number" min="1" value="${esc(wiz.durationMinutes)}" data-ftw-field="durationMinutes" placeholder="Ex.: 45" required>
                    </label>
                ` : ''}
                <label class="ft-field">
                    <span>Responsável / Equipe</span>
                    <input type="text" value="${esc(wiz.responsible)}" data-ftw-field="responsible" placeholder="Ex.: Carlos (Operador)" maxlength="160">
                </label>
            </div>

            ${MODE === 'new_schedule' ? `
                <div class="ftw-repeat-box">
                    <div class="ftw-repeat-head">
                        <strong>Repetir agendamento</strong>
                        <small>Ideal para parcelamento semanal ou quinzenal de fertirrigação.</small>
                    </div>
                    <div class="sl-chips">
                        ${[
                            ['none', 'Sem repetição'],
                            ['weekly', 'Semanal (7d)'],
                            ['biweekly', 'Quinzenal (15d)'],
                            ['monthly', 'Mensal'],
                            ['custom', 'Personalizado'],
                        ].map(([k, lbl]) => `
                            <button type="button" class="sl-chip" aria-pressed="${wiz.repeatPreset === k ? 'true' : 'false'}" data-ftw-repeat="${k}">${lbl}</button>
                        `).join('')}
                    </div>
                    ${wiz.repeatPreset !== 'none' ? `
                        <div class="ftw-fields-3 ftw-repeat-inputs">
                            ${wiz.repeatPreset === 'custom' ? `
                                <label class="ft-field">
                                    <span>A cada</span>
                                    <input type="number" min="1" value="${wiz.repeatEvery}" data-ftw-field="repeatEvery">
                                </label>
                                <label class="ft-field">
                                    <span>Unidade</span>
                                    <select data-ftw-field="repeatUnit">
                                        <option value="days" ${wiz.repeatUnit === 'days' ? 'selected' : ''}>Dias</option>
                                        <option value="months" ${wiz.repeatUnit === 'months' ? 'selected' : ''}>Meses</option>
                                    </select>
                                </label>
                            ` : ''}
                            <label class="ft-field">
                                <span>Repetições adicionais</span>
                                <input type="number" min="1" max="36" value="${wiz.repeatCount}" data-ftw-field="repeatCount">
                            </label>
                        </div>
                        <div class="ftw-repeat-dates">
                            <small>Datas geradas (${dates.length}) — clique no "×" para pular uma data:</small>
                            <div class="ftw-date-chips">
                                ${dates.map((d, i) => `
                                    <span class="ftw-date-chip">
                                        ${fmtDate(d)}
                                        ${i > 0 ? `<button type="button" data-ftw-skip-date="${d}" aria-label="Pular data ${fmtDate(d)}">×</button>` : ''}
                                    </span>
                                `).join('')}
                            </div>
                        </div>
                    ` : ''}
                </div>
            ` : ''}
        `;
    }

    // ---- PASSO 2 (Escalável para 200+ insumos!) ----
    function renderStep2(totals) {
        const addedIds = new Set(wiz.items.map((it) => Number(it.inputId)));
        const prefCat = wiz.method === 'fertirrigacao' ? 'soluvel' : (wiz.method === 'fertilizacao_foliar' ? 'foliar' : 'solido');
        const qq = (wiz.quickQuery || '').trim().toLowerCase();

        const quickMatchesAll = qq
            ? INPUTS.filter((inp) => `${inp.name} ${inp.categoryLabel}`.toLowerCase().includes(qq))
                .sort((a, b) => {
                    const sa = a.available > 0 ? 1 : 0; const sb = b.available > 0 ? 1 : 0;
                    if (sb !== sa) return sb - sa;
                    const ma = a.category === prefCat ? 1 : 0; const mb = b.category === prefCat ? 1 : 0;
                    if (mb !== ma) return mb - ma;
                    return (b.usedCount || 0) - (a.usedCount || 0);
                })
            : [];
        const quickTop = quickMatchesAll.slice(0, 6);

        const suggested = INPUTS.filter((inp) => inp.available > 0)
            .sort((a, b) => {
                const ma = a.category === prefCat ? 1 : 0; const mb = b.category === prefCat ? 1 : 0;
                if (mb !== ma) return mb - ma;
                return (b.usedCount || 0) - (a.usedCount || 0);
            })
            .slice(0, 5);
        const inStockCount = INPUTS.filter((i) => i.available > 0).length;

        return `
            ${RECS.length ? `
                <div class="ftw-rec-box">
                    <div class="ftw-rec-head">
                        <div>
                            <strong>Começar de uma recomendação pronta</strong>
                            <small>Opcional — preenche os insumos automaticamente e você ajusta como quiser.</small>
                        </div>
                        ${wiz.recipeName ? `
                            <div class="ftw-rec-active">
                                <span class="pd-chip is-ok">Ativa: ${esc(wiz.recipeName)}</span>
                                <button type="button" class="ft-link-btn" data-ftw-clear-rec>Limpar</button>
                            </div>
                        ` : ''}
                    </div>
                    <div class="ftw-rec-grid">
                        ${RECS.slice(0, 6).map((r) => {
                            const active = wiz.recipeName === r.name;
                            const preview = r.items.map((x) => getInput(x.inputId)?.name.split(' ')[0]).filter(Boolean).join(' + ');
                            return `
                                <button type="button" class="ftw-rec-card ${active ? 'is-active' : ''}" data-ftw-rec="${r.id}">
                                    <strong>${esc(r.name)}</strong>
                                    <span class="ftw-rec-prev">${esc(preview)}</span>
                                    <small>${esc(r.source)}</small>
                                </button>
                            `;
                        }).join('')}
                    </div>
                </div>
            ` : ''}

            <div class="ftw-sec">
                <h2 class="ftw-label">Unidade de dosagem</h2>
                <p class="ftw-sub">Escolha como prefere informar — o SiSFarm calcula o consumo total de cada talhão automaticamente.</p>
                <div class="ft-seg is-3">
                    <button type="button" class="ft-seg-btn ${wiz.doseMode === 'per_ha' ? 'is-active' : ''}" data-ftw-dosemode="per_ha">
                        <strong>Por hectare</strong><small>kg/ha ou L/ha</small>
                    </button>
                    <button type="button" class="ft-seg-btn ${wiz.doseMode === 'per_plant' ? 'is-active' : ''}" data-ftw-dosemode="per_plant">
                        <strong>Por pé de café</strong><small>g/planta</small>
                    </button>
                    <button type="button" class="ft-seg-btn ${wiz.doseMode === 'fixed_plot' ? 'is-active' : ''}" data-ftw-dosemode="fixed_plot">
                        <strong>Total por talhão</strong><small>quantidade fixa</small>
                    </button>
                </div>
            </div>

            <!-- Adição escalável para 200+ produtos -->
            <div class="ftw-adder-box">
                <div class="ftw-adder-head">
                    <div>
                        <h3 class="ftw-adder-title">Adicionar insumo do estoque</h3>
                        <small>${INPUTS.length} cadastrados · ${inStockCount} com saldo disponível</small>
                    </div>
                    <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm ftw-catalog-btn" data-ftw-open-catalog>
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/><path d="m3.3 7 8.7 5 8.7-5"/><path d="M12 22V12"/></svg>
                        <span>Explorar catálogo (${INPUTS.length})</span>
                    </button>
                </div>

                <div class="ftw-quick-wrap">
                    <label class="ftw-search-field">
                        <span class="sr-only">Buscar insumo do estoque</span>
                        <svg class="ftw-search-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
                        <input type="search" class="ftw-search-input" placeholder="Digite nome ou nutriente (ex.: Ureia, Boro, NPK)..." value="${esc(wiz.quickQuery)}" data-ftw-quick-input autocomplete="off">
                    </label>
                    ${qq ? `
                        <div class="ftw-quick-drop">
                            <div class="ftw-quick-drop-head">
                                <span>Mostrando <strong>${quickTop.length}</strong> de <strong>${quickMatchesAll.length}</strong> (priorizando com saldo)</span>
                                <button type="button" class="ft-link-btn is-brand" data-ftw-clear-quick>Fechar</button>
                            </div>
                            ${quickTop.length ? `
                                <div class="ftw-quick-list">
                                    ${quickTop.map((inp) => {
                                        const added = addedIds.has(inp.id);
                                        const zero = inp.available <= 0;
                                        return `
                                            <button type="button" class="ftw-quick-row ${added ? 'is-added' : ''}" data-ftw-toggle-input="${inp.id}" data-keep-quick="1">
                                                <div class="min-w-0">
                                                    <strong>${esc(inp.name)}</strong>
                                                    <small>${esc(inp.categoryLabel)} · Estoque: <b class="${zero ? 'is-err' : 'is-ok'}">${fmtQty(inp.available)} ${esc(inp.unit)}</b>${inp.avgCost > 0 ? ` · ${fmtBRL(inp.avgCost)}/${esc(inp.unit)}` : ''}</small>
                                                </div>
                                                <span class="ftw-add-pill ${added ? 'is-added' : ''}">${added ? '✓ Na lista' : '+ Adicionar'}</span>
                                            </button>
                                        `;
                                    }).join('')}
                                </div>
                                ${quickMatchesAll.length > 6 ? `
                                    <div class="ftw-quick-more">
                                        <button type="button" class="ft-link-btn is-brand" data-ftw-open-catalog="from_quick">
                                            Ver todos os ${quickMatchesAll.length} resultados no catálogo completo →
                                        </button>
                                    </div>
                                ` : ''}
                            ` : `<p class="ftw-quick-empty">Nenhum insumo encontrado para "${esc(wiz.quickQuery)}".</p>`}
                        </div>
                    ` : ''}
                </div>

                ${suggested.length ? `
                    <div class="ftw-sug-row">
                        <span>Mais usados (${METHOD_META[wiz.method].short}):</span>
                        ${suggested.map((inp) => {
                            const added = addedIds.has(inp.id);
                            return `
                                <button type="button" class="ftw-sug-chip ${added ? 'is-added' : ''}" data-ftw-toggle-input="${inp.id}">
                                    ${added ? '✓' : '+'} ${esc(inp.name.split('(')[0].trim())}
                                </button>
                            `;
                        }).join('')}
                    </div>
                ` : ''}
            </div>

            <!-- Composição dos insumos selecionados (stepper + impacto de estoque) -->
            ${(() => {
                const compatAlerts = checkMixCompatibility();
                if (!compatAlerts.length) return '';
                return compatAlerts.map((a) => `
                    <div class="ftw-stock-notice ${a.level === 'err' ? 'is-block' : ''}" role="alert" style="margin-bottom: .75rem;">
                        <div class="ftw-stock-notice-body">
                            <strong>${a.level === 'err' ? 'Alerta de mistura incompatível' : 'Cuidado na mistura'}</strong>
                            <p>${esc(a.text)}</p>
                        </div>
                    </div>
                `).join('');
            })()}
            <div class="ftw-sec">
                <div class="ftw-sec-head">
                    <div class="ftw-sec-title-inline">
                        <h2>Insumos na operação</h2>
                        <span class="pd-chip is-brand">${wiz.items.length} selecionado${wiz.items.length === 1 ? '' : 's'}</span>
                    </div>
                </div>
                ${wiz.items.length ? `
                    <div class="ftw-items-list">
                        ${wiz.items.map((it, idx) => {
                            const inp = getInput(it.inputId);
                            if (!inp) return '';
                            const perDate = totals.plots.reduce((a, p) => a + calcQtyForPlot(it, p), 0);
                            const totalQty = Number((perDate * totals.datesCount).toFixed(2));
                            const rowCost = totalQty * Number(inp.avgCost || 0);
                            const after = Number((Number(inp.available || 0) - totalQty).toFixed(2));
                            const isShort = after < -0.001;
                            const pct = inp.available > 0 ? Math.min(100, Math.round((totalQty / inp.available) * 100)) : 100;
                            return `
                                <div class="ftw-item-card ${isShort ? 'is-short' : ''}">
                                    <div class="ftw-item-top">
                                        <div class="ftw-item-id">
                                            <strong>${esc(inp.name)}</strong>
                                            <small>${esc(inp.categoryLabel)}${inp.avgCost > 0 ? ` · Custo médio: ${fmtBRL(inp.avgCost)}/${esc(inp.unit)}` : ''}</small>
                                        </div>
                                        <div class="ftw-item-controls">
                                            <div class="ftw-dose-box">
                                                <button type="button" class="ftw-dose-btn" data-ftw-step-dose="${idx}" data-dir="-1" aria-label="Diminuir dose">−</button>
                                                <input type="number" step="0.1" min="0" value="${it.value}" data-ftw-item-val="${idx}" aria-label="Dose de ${esc(inp.name)}">
                                                <span class="ftw-dose-unit">${esc(doseUnitLabel(inp))}</span>
                                                <button type="button" class="ftw-dose-btn" data-ftw-step-dose="${idx}" data-dir="1" aria-label="Aumentar dose">+</button>
                                            </div>
                                            <div class="ftw-item-tot">
                                                <strong class="${isShort ? 'is-err' : ''}">${fmtQty(totalQty)} ${esc(inp.unit)}</strong>
                                                ${rowCost > 0 ? `<small>${fmtBRL(rowCost)}</small>` : '<small>—</small>'}
                                            </div>
                                            <button type="button" class="ftw-del-btn" data-ftw-del-item="${idx}" aria-label="Remover ${esc(inp.name)}" title="Remover">✕</button>
                                        </div>
                                    </div>
                                    <div class="ftw-item-foot">
                                        <div class="ftw-item-stock">
                                            <div class="ft-meter is-mini"><i class="${isShort ? 'is-err' : (pct > 80 ? 'is-warn' : 'is-brand')}" style="width:${pct}%"></i></div>
                                            <span>Estoque: <b>${fmtQty(inp.available)} ${esc(inp.unit)}</b> → <b class="${isShort ? 'is-err' : 'is-ok'}">${fmtQty(after)} ${esc(inp.unit)}</b></span>
                                        </div>
                                        ${isShort
                                            ? `<span class="pd-chip is-err">Faltam ${fmtQty(Math.abs(after))} ${esc(inp.unit)}</span>`
                                            : (totals.datesCount > 1
                                                ? `<small>${fmtQty(perDate)} ${esc(inp.unit)}/data × ${totals.datesCount} datas</small>`
                                                : '<span class="ftw-ok-txt">Saldo disponível OK</span>')}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                ` : `
                    <div class="pd-empty">
                        <strong>Nenhum insumo adicionado ainda.</strong>
                        <span>Use a busca acima, clique em um dos sugeridos ou abra o catálogo completo.</span>
                    </div>
                `}
            </div>

            <label class="ft-field">
                <span>Observações de campo (opcional)</span>
                <textarea rows="2" placeholder="Ex.: aplicar pela manhã, pré-molhamento de 10 min, pressão da bomba..." data-ftw-field="notes">${esc(wiz.notes)}</textarea>
            </label>
        `;
    }

    // ---- PASSO 3 (Conferência limpa, tabular e sem embolamento!) ----
    function renderStep3(totals) {
        const hasShort = totals.byInput.some((b) => b.short);
        const costPerHaSingle = totals.area > 0 ? totals.estCost / (totals.area * totals.datesCount) : 0;
        const fmtBaseDose = (it, inp) => {
            if (wiz.doseMode === 'per_ha') return `${fmtQty(it.value)} ${inp.unit}/ha`;
            if (wiz.doseMode === 'per_plant') return `${fmtQty(it.value)} g/pé`;
            return `${fmtQty(it.value)} ${inp.unit}/talhão`;
        };

        return `
            <div class="ftw-rev-head">
                <div>
                    <h2>Confira os dados antes de salvar</h2>
                    <p>Revise o cronograma, o consumo consolidado de estoque e a divisão exata por talhão.</p>
                </div>
                <span class="pd-chip is-${METHOD_META[wiz.method].tone}">${METHOD_META[wiz.method].label}${wiz.method === 'fertirrigacao' ? ` · ${wiz.durationMinutes} min` : ''}</span>
            </div>

            ${hasShort ? (IS_SCHED ? `
                <div class="ftw-stock-notice" role="status">
                    <span class="ftw-stock-notice-ico" aria-hidden="true">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8h.01"/><path d="M11 12h1v4h1"/></svg>
                    </span>
                    <div class="ftw-stock-notice-body">
                        <strong>Alguns insumos estão com estoque insuficiente</strong>
                        <p>Você pode salvar o agendamento normalmente e realizar a reposição depois, pela Agenda.</p>
                    </div>
                </div>
            ` : `
                <div class="ftw-stock-notice is-block" role="alert">
                    <span class="ftw-stock-notice-ico" aria-hidden="true">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m10.3 3.9-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.7-3.1l-8-14a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>
                    </span>
                    <div class="ftw-stock-notice-body">
                        <strong>Estoque insuficiente para registrar agora</strong>
                        <p>Volte ao passo 2 para ajustar as doses ou registre primeiro a entrada em Compras.</p>
                    </div>
                </div>
            `) : ''}

            <div class="ftw-rev-kpis">
                <div><span class="pd-eyebrow">Operação</span><strong>${totals.ops} ${IS_SCHED ? (totals.ops > 1 ? 'agendamentos' : 'agendamento') : (totals.ops > 1 ? 'aplicações' : 'aplicação')}</strong><small>${totals.plots.length} talh${totals.plots.length > 1 ? 'ões' : 'ão'} × ${totals.datesCount} data${totals.datesCount > 1 ? 's' : ''}</small></div>
                <div><span class="pd-eyebrow">Área atendida</span><strong>${fmtNum(totals.area, 1)} ha</strong><small>${totals.plants ? `${fmtQty(totals.plants)} pés` : 'Área da safra'}</small></div>
                <div><span class="pd-eyebrow">${IS_SCHED ? 'Custo estimado' : 'Custo total'}</span><strong>${fmtBRL(totals.estCost)}</strong><small>${fmtBRL(costPerHaSingle)}/ha por aplic.</small></div>
                <div><span class="pd-eyebrow">Responsável</span><strong>${esc(wiz.responsible || 'Não informado')}</strong><small>${esc(wiz.recipeName || 'Receita personalizada')}</small></div>
            </div>

            <div class="ftw-rev-box">
                <div class="ftw-rev-box-head">
                    <span class="pd-eyebrow">${totals.datesCount > 1 ? `Cronograma programado (${totals.datesCount} datas)` : 'Data da operação'}</span>
                    <button type="button" class="ft-link-btn is-brand" data-ftw-jump="1">Alterar datas ou talhões</button>
                </div>
                <div class="ftw-rev-dates">
                    ${totals.dates.map((dt) => {
                        const w = WEATHER[dt];
                        const rainRisk = w && w.risk === 'high' && (wiz.method === 'fertilizacao_foliar' || wiz.method === 'adubacao_solida');
                        return `<span class="ftw-rev-date-pill ${rainRisk ? 'is-rain' : ''}">${fmtDateWeekday(dt)}${rainRisk ? ` <em>🌧 ${fmtNum(w.rainMm, 0)} mm</em>` : ''}</span>`;
                    }).join('')}
                </div>
            </div>

            <div class="ftw-rev-table-card">
                <div class="ftw-rev-table-head">
                    <div>
                        <h3>Resumo consolidado de insumos</h3>
                        <p>Soma de todos os talhões${totals.datesCount > 1 ? ` nas ${totals.datesCount} datas` : ''} comparada ao saldo atual do estoque.</p>
                    </div>
                    <button type="button" class="ft-link-btn is-brand" data-ftw-jump="2">Editar insumos</button>
                </div>
                <div class="ftw-table-scroll">
                    <table class="ft-table">
                        <thead>
                            <tr>
                                <th>Insumo</th>
                                <th class="is-r">Dose base</th>
                                <th class="is-r">Consumo total</th>
                                <th>Saldo no estoque</th>
                                <th class="is-r">Custo (FIFO)</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${totals.byInput.map((row) => {
                                const it = wiz.items.find((x) => Number(x.inputId) === row.input.id);
                                return `
                                    <tr>
                                        <td><strong>${esc(row.input.name)}</strong><small>${esc(row.input.categoryLabel)}${row.input.avgCost > 0 ? ` · ${fmtBRL(row.input.avgCost)}/${esc(row.input.unit)}` : ''}</small></td>
                                        <td class="is-r">${it ? esc(fmtBaseDose(it, row.input)) : '—'}</td>
                                        <td class="is-r"><strong>${fmtQty(row.qty)} ${esc(row.input.unit)}</strong></td>
                                        <td>
                                            <span>${fmtQty(row.input.available)} → <b class="${row.short ? 'is-err' : 'is-ok'}">${fmtQty(row.after)} ${esc(row.input.unit)}</b></span>
                                            ${row.short ? `<small class="is-err">Falta ${fmtQty(Math.abs(row.after))} ${esc(row.input.unit)}</small>` : ''}
                                        </td>
                                        <td class="is-r"><strong>${fmtBRL(row.cost)}</strong></td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                        <tfoot>
                            <tr>
                                <td colspan="4"><strong>Total consolidado da operação</strong></td>
                                <td class="is-r"><strong>${fmtBRL(totals.estCost)}</strong></td>
                            </tr>
                        </tfoot>
                    </table>
                </div>
            </div>

            <div class="ftw-sec">
                <h3>Detalhamento por talhão (${totals.plots.length})</h3>
                <p class="ftw-sub">Quantidade exata calculada para a área e população de cada talhão.</p>
                <div class="ftw-rev-plots">
                    ${totals.plots.map((p) => {
                        const rows = wiz.items.map((it) => {
                            const inp = getInput(it.inputId);
                            if (!inp) return null;
                            const qtyPerApp = calcQtyForPlot(it, p);
                            const qtyAll = Number((qtyPerApp * totals.datesCount).toFixed(2));
                            const subCost = qtyAll * Number(inp.avgCost || 0);
                            const doseHa = p.area > 0 ? Number((qtyPerApp / p.area).toFixed(2)) : qtyPerApp;
                            return { inp, qtyPerApp, qtyAll, subCost, doseHa };
                        }).filter(Boolean);
                        const plotCost = rows.reduce((a, r) => a + r.subCost, 0);
                        const plotCostHa = p.area > 0 ? plotCost / (p.area * totals.datesCount) : 0;
                        const irrPerApp = wiz.method === 'fertirrigacao' && p.irrigation === 'gotejo' && p.litersPerMin > 0 && wiz.durationMinutes > 0
                            ? Math.round(p.litersPerMin * Number(wiz.durationMinutes))
                            : 0;
                        return `
                            <article class="ftw-rev-plot-card">
                                <div class="ftw-rev-plot-head">
                                    <div>
                                        <div class="ftw-rev-plot-title">
                                            <strong>${esc(p.name)}</strong>
                                            <span class="pd-chip is-muted">${fmtNum(p.area, 1)} ha</span>
                                            <span class="pd-chip ${p.irrigation === 'gotejo' ? 'is-info' : 'is-muted'}">${esc(p.irrigationLabel)}</span>
                                            ${irrPerApp ? `<span class="pd-chip is-info">💧 ${fmtQty(irrPerApp)} L/aplic.</span>` : ''}
                                        </div>
                                        <small>${esc(p.variety || 'Sem variedade informada')}${p.plants ? ` · ${fmtQty(p.plants)} pés` : ''}</small>
                                    </div>
                                    <div class="ftw-rev-plot-cost">
                                        <strong>${fmtBRL(plotCost)}</strong>
                                        <small>${fmtBRL(plotCostHa)}/ha por aplicação</small>
                                    </div>
                                </div>
                                <div class="ftw-table-scroll">
                                    <table class="ft-table is-white">
                                        <thead>
                                            <tr>
                                                <th>Insumo</th>
                                                <th class="is-r">Dose/ha</th>
                                                <th class="is-r">Por aplicação</th>
                                                ${totals.datesCount > 1 ? `<th class="is-r">Total (${totals.datesCount} datas)</th>` : ''}
                                                <th class="is-r">Subtotal</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            ${rows.map((r) => `
                                                <tr>
                                                    <td>${esc(r.inp.name)}</td>
                                                    <td class="is-r">${fmtQty(r.doseHa)} ${esc(r.inp.unit)}/ha</td>
                                                    <td class="is-r"><strong>${fmtQty(r.qtyPerApp)} ${esc(r.inp.unit)}</strong></td>
                                                    ${totals.datesCount > 1 ? `<td class="is-r"><strong class="is-brand">${fmtQty(r.qtyAll)} ${esc(r.inp.unit)}</strong></td>` : ''}
                                                    <td class="is-r"><strong>${fmtBRL(r.subCost)}</strong></td>
                                                </tr>
                                            `).join('')}
                                        </tbody>
                                    </table>
                                </div>
                            </article>
                        `;
                    }).join('')}
                </div>
            </div>

            ${wiz.notes.trim() ? `
                <div class="ftw-rev-notes">
                    <span class="pd-eyebrow">Observações de campo</span>
                    <p>${esc(wiz.notes)}</p>
                </div>
            ` : ''}

            ${!IS_EDIT ? `
                <div class="ftw-rev-box" style="margin-top: .75rem;">
                    <label style="display:flex; align-items:center; gap:.6rem; cursor:pointer; font-size:.9rem; font-weight:600; color:#16201A;">
                        <input type="checkbox" ${wiz.saveAsRecipe ? 'checked' : ''} data-ftw-save-recipe-toggle style="width:1.1rem; height:1.1rem; accent-color:#446A36;">
                        <span>Salvar esta mistura como receita reutilizável</span>
                    </label>
                    ${wiz.saveAsRecipe ? `
                        <label class="ft-field" style="margin-top:.65rem;">
                            <span>Nome da nova receita</span>
                            <input type="text" placeholder="Ex.: Fertirrigação de Pegamento" value="${esc(wiz.saveRecipeName || wiz.recipeName || '')}" data-ftw-save-recipe-name>
                        </label>
                    ` : ''}
                </div>
            ` : ''}
        `;
    }

    function renderSummary(totals) {
        if (!summaryEl) return;
        const submitLabel = IS_SCHED
            ? (IS_EDIT ? 'Salvar alterações' : `Confirmar ${totals.ops} agendamento${totals.ops > 1 ? 's' : ''}`)
            : (IS_EDIT ? 'Salvar alterações' : 'Confirmar e baixar estoque');

        if (wiz.step === 3) {
            summaryEl.innerHTML = `
                <span class="pd-eyebrow is-brand">Ao confirmar</span>
                <h3>${IS_SCHED ? 'O que entra no planejamento' : 'Impactos automáticos no sistema'}</h3>
                <ul class="ftw-check-list">
                    <li><strong>${IS_SCHED ? `${totals.ops} agendamento${totals.ops > 1 ? 's' : ''} na Agenda` : `${totals.plots.length} registro${totals.plots.length > 1 ? 's' : ''} em Fertilização`}</strong><span>${IS_SCHED ? 'Pronto para executar em campo sem redigitar nada.' : 'Vinculado à safra ativa e aos talhões selecionados.'}</span></li>
                    <li><strong>${IS_SCHED ? 'Reserva de estoque (sem baixar ainda)' : 'Baixa automática FIFO no Estoque'}</strong><span>${IS_SCHED ? 'O sistema monitora o saldo acumulado até a execução.' : `${totals.byInput.length} insumo${totals.byInput.length > 1 ? 's' : ''} deduzido${totals.byInput.length > 1 ? 's' : ''} dos lotes mais antigos.`}</span></li>
                    <li><strong>${IS_SCHED ? 'Previsão em Custos por Setor' : 'Lançamento em Custos por Setor'}</strong><span>${fmtBRL(totals.estCost)} (${totals.area ? fmtBRL(totals.estCost / (totals.area * totals.datesCount)) : 'R$ 0,00'}/ha por aplic.)</span></li>
                    ${totals.irrigationLiters > 0 ? `<li class="is-info"><strong>Irrigação automática (${wiz.durationMinutes} min)</strong><span>${fmtQty(totals.irrigationLiters)} L ${IS_SCHED ? 'serão lançados ao concluir.' : 'lançados agora no módulo Irrigação.'}</span></li>` : ''}
                </ul>
                <button type="submit" class="sf-btn sf-btn-primary ftw-full-btn">${submitLabel}</button>
            `;
            return;
        }

        summaryEl.innerHTML = `
            <div>
                <span class="pd-eyebrow">Resumo da operação</span>
                <div class="ftw-sum-tags">
                    <span class="pd-chip is-${METHOD_META[wiz.method].tone}">${METHOD_META[wiz.method].label}</span>
                    ${IS_SCHED && totals.datesCount > 1 ? `<span class="pd-chip is-brand">${totals.datesCount} datas</span>` : ''}
                </div>
            </div>
            <div class="ftw-sum-grid">
                <div><small>Talhões</small><strong>${totals.plots.length} (${fmtNum(totals.area, 1)} ha)</strong></div>
                <div><small>${IS_SCHED ? 'Custo estimado' : 'Custo real FIFO'}</small><strong>${fmtBRL(totals.estCost)}</strong></div>
            </div>
            ${totals.irrigationLiters > 0 ? `
                <div class="ftw-sum-info">
                    <strong>Irrigação automática</strong>
                    <span>Serão lançados <b>${fmtQty(totals.irrigationLiters)} L</b> (${wiz.durationMinutes} min no gotejo) ao concluir.</span>
                </div>
            ` : ''}
            ${totals.nonIrrigatedPlots.length ? `
                <div class="ftw-sum-warn">
                    <strong>Atenção:</strong> ${esc(totals.nonIrrigatedPlots.join(', '))} é de sequeiro — não gerará irrigação automática.
                </div>
            ` : ''}
            <div class="ftw-sum-stock">
                <span class="pd-eyebrow">Impacto no estoque</span>
                ${totals.byInput.length ? totals.byInput.map((row) => `
                    <div class="ftw-sum-stock-row">
                        <div class="min-w-0">
                            <strong>${esc(row.input.name)}</strong>
                            <small>${fmtQty(row.input.available)} → <b class="${row.short ? 'is-err' : 'is-ok'}">${fmtQty(row.after)} ${esc(row.input.unit)}</b></small>
                        </div>
                        <span class="${row.short ? 'is-err' : ''}">−${fmtQty(row.qty)} ${esc(row.input.unit)}</span>
                    </div>
                `).join('') : '<p class="ftw-sum-empty">Adicione insumos no passo 2 para ver a previsão de baixa.</p>'}
            </div>
        `;
    }

    // ---- Modal dedicado de Catálogo (200+ itens paginados) ----
    function renderCatalogModal() {
        if (!catalogShell) return;
        const addedIds = new Set(wiz.items.map((x) => Number(x.inputId)));
        const q = (wiz.catalogSearch || '').trim().toLowerCase();
        const filtered = INPUTS.filter((inp) => {
            if (wiz.catalogOnlyInStock && inp.available <= 0) return false;
            if (wiz.catalogCat !== 'all' && inp.category !== wiz.catalogCat) return false;
            if (q && !`${inp.name} ${inp.categoryLabel}`.toLowerCase().includes(q)) return false;
            return true;
        }).sort((a, b) => {
            if (wiz.catalogSort === 'stock') return b.available - a.available;
            if (wiz.catalogSort === 'az') return a.name.localeCompare(b.name, 'pt-BR');
            return (b.usedCount || 0) - (a.usedCount || 0) || b.available - a.available;
        });
        const visible = filtered.slice(0, wiz.catalogLimit);
        const remaining = Math.max(0, filtered.length - visible.length);

        catalogShell.innerHTML = `
            <div class="ft-dialog-head">
                <div>
                    <p class="pd-eyebrow">Catálogo do estoque</p>
                    <h2>Selecionar insumos para a operação</h2>
                    <p class="ft-dialog-sub">${INPUTS.length} cadastrados · mostrando ${visible.length} de ${filtered.length} no filtro atual</p>
                </div>
                <button type="button" class="ft-x" data-ftw-cat-close aria-label="Fechar">✕</button>
            </div>
            <div class="ftw-cat-tools">
                <div class="ftw-cat-bar">
                    <label class="ftw-search-field ftw-cat-search">
                        <span class="sr-only">Buscar insumo no catálogo</span>
                        <svg class="ftw-search-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
                        <input type="search" class="ftw-search-input" placeholder="Buscar entre ${INPUTS.length} insumos..." value="${esc(wiz.catalogSearch)}" data-ftw-cat-search autocomplete="off">
                    </label>
                    <select class="ft-method-select" data-ftw-cat-sort aria-label="Ordenar">
                        <option value="popular" ${wiz.catalogSort === 'popular' ? 'selected' : ''}>Mais usados</option>
                        <option value="stock" ${wiz.catalogSort === 'stock' ? 'selected' : ''}>Maior saldo</option>
                        <option value="az" ${wiz.catalogSort === 'az' ? 'selected' : ''}>Ordem A–Z</option>
                    </select>
                </div>
                <div class="ftw-cat-subbar">
                    <div class="sl-chips">
                        ${[
                            ['all', 'Todos'],
                            ['soluvel', 'Solúveis'],
                            ['solido', 'Granulados'],
                            ['foliar', 'Foliares'],
                            ['corretivo', 'Corretivos'],
                        ].map(([k, l]) => `<button type="button" class="sl-chip" aria-pressed="${wiz.catalogCat === k ? 'true' : 'false'}" data-ftw-cat-filter="${k}">${l}</button>`).join('')}
                    </div>
                    <label class="ftw-instock-toggle">
                        <input type="checkbox" ${wiz.catalogOnlyInStock ? 'checked' : ''} data-ftw-cat-instock>
                        <span>Apenas com saldo em estoque</span>
                    </label>
                </div>
            </div>
            <div class="ft-dialog-body">
                ${visible.length ? `
                    <div class="ftw-cat-grid">
                        ${visible.map((inp) => {
                            const added = addedIds.has(inp.id);
                            const zero = inp.available <= 0;
                            return `
                                <button type="button" class="ftw-cat-card ${added ? 'is-added' : ''} ${zero ? 'is-zero' : ''}" data-ftw-cat-pick="${inp.id}">
                                    <div class="ftw-cat-card-top">
                                        <div class="min-w-0">
                                            <strong>${esc(inp.name)}</strong>
                                            <small>${esc(inp.categoryLabel)}</small>
                                        </div>
                                        <span class="ftw-add-pill ${added ? 'is-added' : ''}">${added ? '✓ Na lista' : '+ Selecionar'}</span>
                                    </div>
                                    <div class="ftw-cat-card-foot">
                                        <span class="${zero ? 'is-err' : 'is-ok'}">${zero ? `Sem saldo (0 ${esc(inp.unit)})` : `${fmtQty(inp.available)} ${esc(inp.unit)} disponível`}</span>
                                        ${inp.avgCost > 0 ? `<small>${fmtBRL(inp.avgCost)}/${esc(inp.unit)}</small>` : ''}
                                    </div>
                                </button>
                            `;
                        }).join('')}
                    </div>
                    ${remaining > 0 ? `
                        <div class="ftw-cat-more">
                            <button type="button" class="sf-btn sf-btn-secondary sf-btn-sm" data-ftw-cat-more>Carregar mais ${Math.min(20, remaining)} (${remaining} restantes)</button>
                        </div>
                    ` : ''}
                ` : '<div class="pd-empty"><strong>Nenhum insumo encontrado.</strong><span>Tente outro termo ou desmarque "Apenas com saldo em estoque".</span></div>'}
            </div>
            <div class="ft-dialog-foot">
                <span class="ftw-cat-count"><strong>${wiz.items.length}</strong> insumo${wiz.items.length === 1 ? '' : 's'} na receita atual</span>
                <button type="button" class="sf-btn sf-btn-primary" data-ftw-cat-close>Concluir seleção</button>
            </div>
        `;

        catalogShell.querySelectorAll('[data-ftw-cat-close]').forEach((b) => b.addEventListener('click', () => { catalogDialog.close(); render(); }));
        catalogShell.querySelector('[data-ftw-cat-search]')?.addEventListener('input', (e) => {
            wiz.catalogSearch = e.target.value; wiz.catalogLimit = 20;
            const pos = e.target.selectionStart;
            renderCatalogModal();
            const inp = catalogShell.querySelector('[data-ftw-cat-search]');
            if (inp) { inp.focus(); inp.setSelectionRange(pos, pos); }
        });
        catalogShell.querySelector('[data-ftw-cat-sort]')?.addEventListener('change', (e) => { wiz.catalogSort = e.target.value; wiz.catalogLimit = 20; renderCatalogModal(); });
        catalogShell.querySelectorAll('[data-ftw-cat-filter]').forEach((b) => b.addEventListener('click', () => { wiz.catalogCat = b.dataset.ftwCatFilter; wiz.catalogLimit = 20; renderCatalogModal(); }));
        catalogShell.querySelector('[data-ftw-cat-instock]')?.addEventListener('change', (e) => { wiz.catalogOnlyInStock = e.target.checked; wiz.catalogLimit = 20; renderCatalogModal(); });
        catalogShell.querySelector('[data-ftw-cat-more]')?.addEventListener('click', () => { wiz.catalogLimit += 20; renderCatalogModal(); });
        catalogShell.querySelectorAll('[data-ftw-cat-pick]').forEach((b) => {
            b.addEventListener('click', () => {
                toggleInputInRecipe(Number(b.dataset.ftwCatPick));
                renderCatalogModal();
            });
        });
    }

    function toggleInputInRecipe(inputId) {
        const idx = wiz.items.findIndex((x) => Number(x.inputId) === inputId);
        if (idx >= 0) wiz.items.splice(idx, 1);
        else {
            const inp = getInput(inputId);
            wiz.items.push({ inputId, value: inp?.defaultDose || 10 });
        }
        wiz.recipeName = '';
    }

    function render() {
        const totals = calcTotals();
        form.classList.toggle('is-step-3', wiz.step === 3);

        root.querySelectorAll('[data-ftw-go]').forEach((btn) => {
            const n = Number(btn.dataset.ftwGo);
            btn.classList.toggle('is-current', wiz.step === n);
            btn.classList.toggle('is-done', wiz.step > n);
        });

        if (wiz.step === 1) stageEl.innerHTML = renderStep1();
        else if (wiz.step === 2) stageEl.innerHTML = renderStep2(totals);
        else stageEl.innerHTML = renderStep3(totals);

        renderSummary(totals);

        if (backBtn) backBtn.hidden = wiz.step === 1;
        if (nextBtn) nextBtn.hidden = wiz.step === 3;
        if (submitBtn) {
            submitBtn.hidden = wiz.step !== 3;
            submitBtn.textContent = IS_SCHED
                ? (IS_EDIT ? 'Salvar alterações' : `Confirmar ${totals.ops} agendamento${totals.ops > 1 ? 's' : ''}`)
                : (IS_EDIT ? 'Salvar alterações' : 'Confirmar e baixar estoque');
        }

        bindStage();
    }

    function bindStage() {
        stageEl.querySelectorAll('[data-ftw-plot]').forEach((b) => {
            b.addEventListener('click', () => {
                const id = Number(b.dataset.ftwPlot);
                if (IS_EDIT) wiz.plotIds = [id];
                else if (wiz.plotIds.includes(id)) {
                    if (wiz.plotIds.length > 1) wiz.plotIds = wiz.plotIds.filter((x) => x !== id);
                } else wiz.plotIds.push(id);
                render();
            });
        });
        stageEl.querySelectorAll('[data-ftw-plots-quick]').forEach((b) => {
            b.addEventListener('click', () => {
                if (b.dataset.ftwPlotsQuick === 'all') wiz.plotIds = PLOTS.map((p) => p.id);
                else wiz.plotIds = PLOTS.filter((p) => p.irrigation === 'gotejo').map((p) => p.id);
                render();
            });
        });
        stageEl.querySelectorAll('[data-ftw-method]').forEach((b) => {
            b.addEventListener('click', () => { wiz.method = b.dataset.ftwMethod; render(); });
        });
        stageEl.querySelectorAll('[data-ftw-field]').forEach((el) => {
            el.addEventListener('change', () => {
                const k = el.dataset.ftwField;
                wiz[k] = ['durationMinutes', 'repeatEvery', 'repeatCount'].includes(k) ? Number(el.value || 0) : el.value;
                if (k === 'date') wiz.skippedDates = [];
                render();
            });
        });
        stageEl.querySelectorAll('[data-ftw-repeat]').forEach((b) => {
            b.addEventListener('click', () => { wiz.repeatPreset = b.dataset.ftwRepeat; wiz.skippedDates = []; render(); });
        });
        stageEl.querySelectorAll('[data-ftw-skip-date]').forEach((b) => {
            b.addEventListener('click', () => { wiz.skippedDates.push(b.dataset.ftwSkipDate); render(); });
        });
        stageEl.querySelectorAll('[data-ftw-rec]').forEach((b) => {
            b.addEventListener('click', () => {
                const rec = RECS.find((r) => r.id === Number(b.dataset.ftwRec));
                if (!rec) return;
                wiz.recipeName = rec.name;
                wiz.doseMode = rec.doseMode || 'per_ha';
                if (rec.method && METHOD_META[rec.method]) wiz.method = rec.method;
                wiz.items = rec.items.map((x) => ({ inputId: Number(x.inputId), value: Number(x.value ?? x.dosePerHa ?? 0) }));
                render();
            });
        });
        stageEl.querySelector('[data-ftw-clear-rec]')?.addEventListener('click', () => {
            wiz.recipeName = ''; wiz.items = []; render();
        });
        stageEl.querySelectorAll('[data-ftw-dosemode]').forEach((b) => {
            b.addEventListener('click', () => { wiz.doseMode = b.dataset.ftwDosemode; render(); });
        });
        stageEl.querySelector('[data-ftw-quick-input]')?.addEventListener('input', (e) => {
            wiz.quickQuery = e.target.value;
            const pos = e.target.selectionStart;
            render();
            const inp = stageEl.querySelector('[data-ftw-quick-input]');
            if (inp) { inp.focus(); inp.setSelectionRange(pos, pos); }
        });
        stageEl.querySelector('[data-ftw-clear-quick]')?.addEventListener('click', () => { wiz.quickQuery = ''; render(); });
        stageEl.querySelectorAll('[data-ftw-open-catalog]').forEach((b) => {
            b.addEventListener('click', () => {
                if (b.dataset.ftwOpenCatalog === 'from_quick') {
                    wiz.catalogSearch = wiz.quickQuery;
                    wiz.quickQuery = '';
                }
                wiz.catalogLimit = 20;
                renderCatalogModal();
                catalogDialog?.showModal();
            });
        });
        stageEl.querySelectorAll('[data-ftw-toggle-input]').forEach((b) => {
            b.addEventListener('click', () => {
                toggleInputInRecipe(Number(b.dataset.ftwToggleInput));
                if (b.dataset.keepQuick !== '1') wiz.quickQuery = '';
                render();
            });
        });
        stageEl.querySelectorAll('[data-ftw-step-dose]').forEach((b) => {
            b.addEventListener('click', () => {
                const idx = Number(b.dataset.ftwStepDose);
                const dir = Number(b.dataset.dir);
                const item = wiz.items[idx];
                if (!item) return;
                const inp = getInput(item.inputId);
                const step = wiz.doseMode === 'per_plant' ? 5 : (inp?.step || 1);
                item.value = Math.max(0, Number((Number(item.value || 0) + dir * step).toFixed(2)));
                render();
            });
        });
        stageEl.querySelectorAll('[data-ftw-item-val]').forEach((el) => {
            el.addEventListener('change', () => {
                wiz.items[Number(el.dataset.ftwItemVal)].value = Math.max(0, Number(el.value || 0));
                render();
            });
        });
        stageEl.querySelectorAll('[data-ftw-del-item]').forEach((b) => {
            b.addEventListener('click', () => { wiz.items.splice(Number(b.dataset.ftwDelItem), 1); wiz.recipeName = ''; render(); });
        });
        stageEl.querySelectorAll('[data-ftw-jump]').forEach((b) => {
            b.addEventListener('click', () => { wiz.step = Number(b.dataset.ftwJump); render(); window.scrollTo({ top: 0, behavior: 'smooth' }); });
        });
        stageEl.querySelector('[data-ftw-save-recipe-toggle]')?.addEventListener('change', (e) => {
            wiz.saveAsRecipe = Boolean(e.target.checked);
            if (wiz.saveAsRecipe && !wiz.saveRecipeName) wiz.saveRecipeName = wiz.recipeName || '';
            render();
        });
        stageEl.querySelector('[data-ftw-save-recipe-name]')?.addEventListener('input', (e) => {
            wiz.saveRecipeName = e.target.value;
        });
    }

    // Navegação dos passos
    root.querySelectorAll('[data-ftw-go]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const target = Number(btn.dataset.ftwGo);
            if (target > wiz.step && !validateStep(wiz.step)) return;
            if (target === 3 && wiz.step === 1 && (!validateStep(1) || !validateStep(2))) return;
            wiz.step = target;
            showError('');
            render();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
    });
    backBtn?.addEventListener('click', () => {
        if (wiz.step > 1) { wiz.step--; showError(''); render(); window.scrollTo({ top: 0, behavior: 'smooth' }); }
    });
    nextBtn?.addEventListener('click', () => {
        if (!validateStep(wiz.step)) return;
        if (wiz.step < 3) { wiz.step++; render(); window.scrollTo({ top: 0, behavior: 'smooth' }); }
    });

    form.addEventListener('submit', (e) => {
        if (!validateStep(1) || !validateStep(2) || !validateStep(3)) {
            e.preventDefault();
            return;
        }
        const dates = getDates();
        payloadInput.value = JSON.stringify({
            plotIds: wiz.plotIds,
            date: wiz.date,
            dates,
            method: wiz.method,
            durationMinutes: wiz.method === 'fertirrigacao' ? wiz.durationMinutes : null,
            responsible: wiz.responsible,
            recipeName: wiz.recipeName,
            notes: wiz.notes,
            doseMode: wiz.doseMode,
            saveAsRecipe: Boolean(wiz.saveAsRecipe && !IS_EDIT),
            saveRecipeName: (wiz.saveRecipeName || wiz.recipeName || '').trim(),
            items: wiz.items.filter((x) => x.inputId && Number(x.value) > 0),
        });
    });

    catalogDialog?.addEventListener('click', (e) => { if (e.target === catalogDialog) { catalogDialog.close(); render(); } });

    render();
})();
