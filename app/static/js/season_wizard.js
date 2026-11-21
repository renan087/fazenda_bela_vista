/* Assistente de safra (5 passos). Lê os dados de #season-wizard-data e só envia o formulário no último passo. */
(() => {
    const form = document.querySelector('[data-season-wizard]');
    const dataEl = document.getElementById('season-wizard-data');
    if (!form || !dataEl) return;
    let data;
    try { data = JSON.parse(dataEl.textContent || '{}'); } catch (error) { return; }

    const $ = (sel) => form.querySelector(sel);
    const $$ = (sel) => Array.from(form.querySelectorAll(sel));
    const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const n1 = (v) => Number(v || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 });
    const n2 = (v) => Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const brl = (v) => 'R$ ' + Math.round(Number(v || 0)).toLocaleString('pt-BR');
    const digits = (v) => { const d = String(v ?? '').replace(/\D/g, ''); return d === '' ? null : Number(d); };
    const dateBr = (iso) => (iso ? iso.split('-').reverse().join('/') : '—');
    const STAGE = { prod: ['Em produção', 'is-ok'], form: ['Em formação', 'is-info'], sem: ['Sem plantio informado', 'is-muted'] };
    const STEPS = 5;
    const edit = data.edit || null;
    const plots = data.plots || [];
    const periods = data.periods || [];
    const categories = data.categories || [];
    const DRAFT_KEY = 'sisfarm.seasonWizardDraft';

    const state = {
        step: 0,
        period: periods[0] ? periods[0].key : 'custom',
        start: '', end: '', nameTouched: false,
        copyPrev: false,
        sel: new Set(),
        yields: {}, budget: {}, price: data.price ? Math.round(data.price) : null,
    };

    // ---------- estado inicial (edição ou nova) ----------
    if (edit) {
        const match = periods.find((p) => p.start === edit.start && p.end === edit.end);
        state.period = match ? match.key : 'custom';
        state.start = edit.start; state.end = edit.end;
        state.nameTouched = true;
        $('[data-szw-name]').value = edit.name || '';
        $('[data-szw-culture]').value = edit.culture || 'Café';
        $('[data-szw-notes]').value = edit.notes || '';
        (edit.plot_ids || []).forEach((id) => state.sel.add(id));
        applyGoals(edit.goals);
    } else {
        $('[data-szw-culture]').value = 'Café';
        plots.filter((p) => p.stage !== 'sem').forEach((p) => state.sel.add(p.id));
        plots.forEach((p) => { if (p.hist) state.yields[p.id] = p.hist; });
    }

    function applyGoals(goals) {
        if (!goals) return;
        Object.entries(goals.yields || {}).forEach(([id, v]) => { state.yields[Number(id)] = Number(v); });
        Object.entries(goals.budget || {}).forEach(([k, v]) => { state.budget[k] = Number(v); });
        if (goals.price) state.price = Math.round(Number(goals.price));
    }

    // ---------- rascunho (ida e volta ao cadastro de setor) ----------
    const saveDraft = () => {
        try {
            sessionStorage.setItem(DRAFT_KEY, JSON.stringify({
                editId: edit ? edit.id : null, period: state.period, start: state.start, end: state.end,
                name: $('[data-szw-name]').value, nameTouched: state.nameTouched, culture: $('[data-szw-culture]').value,
                notes: $('[data-szw-notes]').value, sel: [...state.sel], yields: state.yields, budget: state.budget, price: state.price,
            }));
        } catch (error) { /* sem armazenamento: segue sem rascunho */ }
    };
    window.saveSeasonDraft = saveDraft;
    const query = new URLSearchParams(window.location.search);
    const backFromPlot = query.get('novo') === '1' || query.get('rascunho') === '1';
    if (backFromPlot) {
        try {
            const draft = JSON.parse(sessionStorage.getItem(DRAFT_KEY) || 'null');
            if (draft && (draft.editId || null) === (edit ? edit.id : null)) {
                Object.assign(state, { period: draft.period, start: draft.start, end: draft.end, nameTouched: draft.nameTouched, yields: draft.yields || {}, budget: draft.budget || {}, price: draft.price });
                state.sel = new Set(draft.sel || []);
                $('[data-szw-name]').value = draft.name || '';
                $('[data-szw-culture]').value = draft.culture || 'Café';
                $('[data-szw-notes]').value = draft.notes || '';
            }
        } catch (error) { /* rascunho ilegível: ignora */ }
        const newPlot = Number(query.get('setor'));
        if (newPlot && plots.some((p) => p.id === newPlot)) state.sel.add(newPlot);
        state.step = 1;
        try { sessionStorage.removeItem(DRAFT_KEY); } catch (error) { /* ok */ }
    }
    $('[data-szw-new-plot]')?.addEventListener('click', () => {
        saveDraft();
        window.showPageLoader?.('Abrindo cadastro de setor', 'Os dados desta safra ficam guardados.');
    });

    // ---------- cálculos ----------
    const periodDates = () => {
        if (state.period === 'custom') return [state.start, state.end];
        const p = periods.find((item) => item.key === state.period);
        return p ? [p.start, p.end] : ['', ''];
    };
    const statusOf = () => {
        const [start, end] = periodDates();
        if (edit && edit.status === 'encerrada') return ['encerrada', 'Encerrada', 'is-muted', 'Safra encerrada.'];
        if (!start || !end) return ['', 'Defina as datas', 'is-muted', ''];
        const today = data.today;
        if (today < start) return ['planejada', 'Planejada', 'is-info', 'Vira “Em andamento” sozinha quando chegar o início.'];
        if (today > end) return ['encerrada', 'Encerrada', 'is-muted', 'O período já terminou.'];
        return ['ativa', 'Em andamento', 'is-ok', `Começou em ${dateBr(start)}, calculado pelas datas.`];
    };
    const calc = () => {
        const sel = plots.filter((p) => state.sel.has(p.id));
        const area = sel.reduce((a, p) => a + p.area, 0);
        const prodPlots = sel.filter((p) => p.stage === 'prod');
        const sacks = prodPlots.reduce((a, p) => a + p.area * (Number(state.yields[p.id]) || 0), 0);
        const productiveArea = prodPlots.filter((p) => Number(state.yields[p.id]) > 0).reduce((a, p) => a + p.area, 0);
        const budgetHa = categories.reduce((a, c) => a + (Number(state.budget[c.key]) || 0), 0);
        const cost = budgetHa * area;
        const price = Number(state.price) || 0;
        const varieties = [...new Set(sel.map((p) => p.variety).filter(Boolean))];
        return { sel, area, prodPlots, sacks, productiveArea, budgetHa, cost, price, varieties,
            revenue: price ? sacks * price : null, margin: price ? sacks * price - cost : null,
            breakeven: price && productiveArea ? cost / price / productiveArea : null };
    };
    const autoName = () => {
        if (state.period === 'custom') {
            const [start, end] = periodDates();
            if (!start || !end) return '';
            const a = start.slice(0, 4), b = end.slice(0, 4);
            return a === b ? `Safra ${a}` : `Safra ${a}/${b.slice(-2)}`;
        }
        const p = periods.find((item) => item.key === state.period);
        return p ? `Safra ${p.label}` : '';
    };

    // ---------- passo 1 ----------
    const periodBox = $('[data-szw-periods]');
    const renderPeriods = () => {
        const items = [...periods, { key: 'custom', label: 'Outras datas', tag: 'Personalizado', range: 'Escolha início e fim' }];
        periodBox.innerHTML = items.map((p) => `
            <button type="button" role="radio" class="szw-period${state.period === p.key ? ' is-on' : ''}" aria-checked="${state.period === p.key}" data-szw-period="${esc(p.key)}">
                <span class="szw-period-tag">${esc(p.tag)}<span class="szw-dot" aria-hidden="true"></span></span>
                <strong>${esc(p.label)}</strong><small>${esc(p.range)}</small>
            </button>`).join('');
        $('[data-szw-custom]').hidden = state.period !== 'custom';
    };
    periodBox.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-szw-period]');
        if (!btn) return;
        state.period = btn.dataset.szwPeriod;
        if (state.period === 'custom' && !state.start) {
            const p = periods[0];
            if (p) { state.start = p.start; state.end = p.end; }
        }
        update();
    });
    $('[data-szw-start-input]').addEventListener('change', (e) => { state.start = e.target.value; update(); });
    $('[data-szw-end-input]').addEventListener('change', (e) => { state.end = e.target.value; update(); });
    $('[data-szw-name]').addEventListener('input', () => { state.nameTouched = true; updateAside(); });
    const copyBtn = $('[data-szw-copy]');
    copyBtn?.addEventListener('click', () => {
        state.copyPrev = !state.copyPrev;
        copyBtn.setAttribute('aria-checked', String(state.copyPrev));
        const prev = data.previous;
        if (state.copyPrev && prev) {
            state.sel = new Set((prev.plot_ids || []).filter((id) => plots.some((p) => p.id === id)));
            applyGoals(prev.goals);
        }
        update();
    });

    // ---------- passo 2 ----------
    const plotsBox = $('[data-szw-plots]');
    const renderPlots = () => {
        $('[data-szw-plots-empty]').hidden = plots.length > 0;
        plotsBox.innerHTML = plots.map((p) => {
            const on = state.sel.has(p.id);
            const [stageTxt, stageCls] = STAGE[p.stage] || STAGE.sem;
            const age = p.year ? `${Math.max(0, new Date(data.today).getFullYear() - p.year)} anos` : '—';
            const foot = p.stage === 'prod' ? (p.hist ? `Média das últimas safras: ${n1(p.hist)} sc/ha` : 'Sem histórico de colheita ainda') : 'Entra nos custos, sem colheita prevista';
            return `<button type="button" class="szw-plot${on ? ' is-on' : ''}" aria-pressed="${on}" data-szw-plot="${p.id}">
                <span class="szw-plot-top"><span class="szw-box" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12l5 5L20 7"/></svg></span>
                <span class="szw-grow"><strong>${esc(p.name)}</strong><small>${esc(p.variety || 'Variedade não informada')}</small><span class="szw-chip ${stageCls}">${stageTxt}</span></span></span>
                <span class="szw-plot-stats"><span><small>Área</small><b>${n2(p.area)} ha</b></span><span><small>Plantas</small><b>${p.plants ? Number(p.plants).toLocaleString('pt-BR') : '—'}</b></span><span><small>Idade</small><b>${age}</b></span></span>
                <small class="szw-plot-foot">${foot}</small>
            </button>`;
        }).join('');
    };
    plotsBox.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-szw-plot]');
        if (!btn) return;
        const id = Number(btn.dataset.szwPlot);
        if (state.sel.has(id)) state.sel.delete(id); else state.sel.add(id);
        update();
    });
    $$('[data-szw-select]').forEach((btn) => btn.addEventListener('click', () => {
        const mode = btn.dataset.szwSelect;
        state.sel = new Set(plots.filter((p) => mode === 'all' || p.stage === 'prod').map((p) => p.id));
        update();
    }));

    // ---------- passo 3 ----------
    const prodBox = $('[data-szw-prod]');
    const budgetBox = $('[data-szw-budget]');
    const renderGoals = (c) => {
        prodBox.innerHTML = c.prodPlots.length ? c.prodPlots.map((p) => {
            const v = state.yields[p.id] ?? '';
            return `<div class="szw-row">
                <span class="szw-grow"><strong>${esc(p.name)}</strong><small>${n2(p.area)} ha${p.hist ? ` · média ${n1(p.hist)} sc/ha` : ''}</small></span>
                <span class="szw-stepper"><button type="button" data-szw-dec="${p.id}" aria-label="Diminuir produtividade de ${esc(p.name)}">−</button><input inputmode="decimal" value="${esc(v === '' ? '' : String(v).replace('.', ','))}" data-szw-yield="${p.id}" aria-label="Sacas por hectare em ${esc(p.name)}"><button type="button" data-szw-inc="${p.id}" aria-label="Aumentar produtividade de ${esc(p.name)}">+</button></span>
                <span class="szw-row-total" data-szw-yield-total="${p.id}">${n1(p.area * (Number(v) || 0))} sc</span>
            </div>`;
        }).join('') : '<p class="szw-muted">Nenhum setor em produção selecionado.</p>';
        budgetBox.innerHTML = categories.map((cat) => {
            const v = state.budget[cat.key];
            return `<div class="szw-row">
                <label class="szw-grow" for="szw-b-${cat.key}"><strong>${esc(cat.label)}</strong><small>${esc(cat.hint)}</small></label>
                <span class="szw-money-box"><em>R$</em><input id="szw-b-${cat.key}" inputmode="numeric" value="${v ? Number(v).toLocaleString('pt-BR') : ''}" data-szw-budget-key="${cat.key}"></span>
                <span class="szw-row-total" data-szw-budget-total-key="${cat.key}">${brl((Number(v) || 0) * c.area)}</span>
            </div>`;
        }).join('');
    };
    const setYield = (id, value) => { state.yields[id] = value; };
    prodBox.addEventListener('click', (event) => {
        const dec = event.target.closest('[data-szw-dec]');
        const inc = event.target.closest('[data-szw-inc]');
        const btn = dec || inc;
        if (!btn) return;
        const id = Number(btn.dataset.szwDec || btn.dataset.szwInc);
        const cur = Number(state.yields[id]) || 0;
        setYield(id, Math.max(0, Math.round(cur + (dec ? -1 : 1))));
        const input = prodBox.querySelector(`[data-szw-yield="${id}"]`);
        if (input) input.value = String(state.yields[id]);
        refreshNumbers();
    });
    prodBox.addEventListener('input', (event) => {
        const input = event.target.closest('[data-szw-yield]');
        if (!input) return;
        const value = parseFloat(input.value.replace(',', '.'));
        setYield(Number(input.dataset.szwYield), Number.isFinite(value) && value >= 0 ? value : '');
        refreshNumbers();
    });
    budgetBox.addEventListener('input', (event) => {
        const input = event.target.closest('[data-szw-budget-key]');
        if (!input) return;
        const value = digits(input.value);
        state.budget[input.dataset.szwBudgetKey] = value;
        input.value = value === null ? '' : value.toLocaleString('pt-BR');
        refreshNumbers();
    });
    const priceInput = $('[data-szw-price]');
    priceInput.addEventListener('input', () => {
        state.price = digits(priceInput.value);
        priceInput.value = state.price === null ? '' : state.price.toLocaleString('pt-BR');
        refreshNumbers();
    });
    if (data.price) $('[data-szw-price-note]').textContent = `Sugestão: cotação do café arábica de ${data.price_date} (Comercialização). Ajuste se quiser.`;

    const refreshNumbers = () => {
        const c = calc();
        c.prodPlots.forEach((p) => {
            const cell = prodBox.querySelector(`[data-szw-yield-total="${p.id}"]`);
            if (cell) cell.textContent = `${n1(p.area * (Number(state.yields[p.id]) || 0))} sc`;
        });
        categories.forEach((cat) => {
            const cell = budgetBox.querySelector(`[data-szw-budget-total-key="${cat.key}"]`);
            if (cell) cell.textContent = brl((Number(state.budget[cat.key]) || 0) * c.area);
        });
        const noProd = c.sel.filter((p) => p.stage !== 'prod');
        const note = $('[data-szw-noprod]');
        note.hidden = !noProd.length;
        note.textContent = noProd.length ? `Sem colheita nesta safra: ${noProd.map((p) => `${p.name} (${STAGE[p.stage][0].toLowerCase()})`).join(', ')}.` : '';
        $('[data-szw-sacks]').textContent = `${n1(c.sacks)} sacas`;
        $('[data-szw-area-txt]').textContent = `${n2(c.area)} ha`;
        $('[data-szw-budget-ha]').textContent = `${brl(c.budgetHa)}/ha`;
        $('[data-szw-budget-total]').textContent = brl(c.cost);
        $('[data-szw-revenue]').textContent = c.revenue === null ? '—' : brl(c.revenue);
        $('[data-szw-cost]').textContent = c.cost ? brl(c.cost) : '—';
        const margin = $('[data-szw-margin]');
        margin.textContent = c.margin === null ? '—' : brl(c.margin);
        margin.className = c.margin === null ? '' : c.margin >= 0 ? 'is-pos' : 'is-neg';
        $('[data-szw-breakeven]').textContent = c.breakeven === null ? '—' : `${n1(c.breakeven)} sc/ha`;
        updateAside(c);
    };

    // ---------- passo 5 e resumo ----------
    const goTo = (i) => { state.step = i; update(); };
    const renderReview = (c) => {
        const [start, end] = periodDates();
        const [, statusTxt] = statusOf();
        const active = $('[data-szw-active]');
        const cards = [
            ['Período', $('[data-szw-name]').value || autoName(), `${dateBr(start)} a ${dateBr(end)} · ${statusTxt}`, 0],
            ['Setores', `${c.sel.length} ${c.sel.length === 1 ? 'setor' : 'setores'} · ${n2(c.area)} ha`, c.varieties.join(', ') || 'Variedades não informadas', 1],
            ['Metas', c.sacks ? `${n1(c.sacks)} sacas` : 'Sem meta de produção', c.cost ? `Orçamento ${brl(c.cost)}${c.breakeven ? ` · equilíbrio ${n1(c.breakeven)} sc/ha` : ''}` : 'Sem orçamento', 2],
            ['Integrações', active && active.checked ? 'Safra ativa no topo' : 'Nenhuma ligada', 'As demais chegam em breve', 3],
        ];
        $('[data-szw-review]').innerHTML = cards.map(([t, v, d, step]) => `
            <div class="szw-review-card"><div class="szw-head-row"><span class="szw-overline">${t}</span><button type="button" class="szw-link" data-szw-go="${step}">Alterar</button></div>
            <strong>${esc(v)}</strong><small>${esc(d)}</small></div>`).join('');
        const impact = [
            `${edit ? 'Salvar' : 'Criar'} a ${esc($('[data-szw-name]').value || autoName())} (${dateBr(start)} a ${dateBr(end)}) como “${statusTxt}”.`,
            `Vincular ${c.sel.length} ${c.sel.length === 1 ? 'setor' : 'setores'} (${n2(c.area)} ha), guardando o plantio de cada um.`,
        ];
        if (c.sacks || c.cost) impact.push(`Guardar as metas: ${c.sacks ? `${n1(c.sacks)} sacas` : 'sem meta de sacas'}${c.cost ? ` e orçamento de ${brl(c.cost)}` : ''}.`);
        if (active && active.checked && !active.disabled) impact.push('Trocar a safra ativa do topo para esta safra.');
        $('[data-szw-impact]').innerHTML = impact.map((t) => `<li>${t}</li>`).join('');
    };
    const updateAside = (c = calc()) => {
        const [start, end] = periodDates();
        const [, statusTxt, statusCls] = statusOf();
        $('[data-szw-aside-name]').textContent = $('[data-szw-name]').value || autoName() || 'Nova safra';
        const chip = $('[data-szw-aside-status]');
        chip.textContent = statusTxt; chip.className = `szw-chip ${statusCls}`;
        const rows = [
            ['Período', start && end ? `${dateBr(start)} a ${dateBr(end)}` : '—'],
            ['Setores', c.sel.length ? `${c.sel.length} · ${n2(c.area)} ha` : '—'],
            ['Variedades', c.varieties.length ? String(c.varieties.length) : '—'],
            ['Produção prevista', c.sacks ? `${n1(c.sacks)} sc` : '—'],
            ['Orçamento', c.cost ? brl(c.cost) : '—'],
            ['Custo por hectare', c.budgetHa ? brl(c.budgetHa) : '—'],
            ['Ponto de equilíbrio', c.breakeven ? `${n1(c.breakeven)} sc/ha` : '—'],
        ];
        $('[data-szw-summary]').innerHTML = rows.map(([k, v]) => `<div><dt>${k}</dt><dd>${esc(v)}</dd></div>`).join('');
    };

    // ---------- navegação ----------
    const stepsEl = $('[data-szw-steps]');
    const canAdvance = () => {
        if (state.step === 0) {
            const [start, end] = periodDates();
            if (!start || !end) return [false, 'Escolha o período'];
            if (end < start) return [false, 'O fim precisa ser depois do início'];
            if (!$('[data-szw-culture]').value.trim()) return [false, 'Informe a cultura'];
        }
        if (state.step === 1 && !state.sel.size) return [false, 'Escolha pelo menos um setor'];
        return [true, ''];
    };
    const update = () => {
        const c = calc();
        const [start, end] = periodDates();
        if (!state.nameTouched) $('[data-szw-name]').value = autoName();
        $('[data-szw-start-input]').value = state.start || '';
        $('[data-szw-end-input]').value = state.end || '';
        const [, statusTxt, statusCls, statusHint] = statusOf();
        const status = $('[data-szw-status]');
        status.textContent = statusTxt; status.className = `szw-chip ${statusCls}`;
        $('[data-szw-status-hint]').textContent = statusHint;
        renderPeriods();
        const n = c.sel.length;
        $('[data-szw-plot-strip]').innerHTML = `<strong>${n} ${n === 1 ? 'setor selecionado' : 'setores selecionados'}</strong><span>${n2(c.area)} ha no total</span><span>${c.varieties.length} ${c.varieties.length === 1 ? 'variedade' : 'variedades'}</span>`;
        if (state.step === 1) renderPlots();
        if (state.step === 2) { renderGoals(c); }
        if (state.step === 2 && !priceInput.value && state.price) priceInput.value = Number(state.price).toLocaleString('pt-BR');
        if (state.step === 4) renderReview(c);
        refreshNumbers();
        $$('[data-szw-panel]').forEach((panel) => { panel.hidden = Number(panel.dataset.szwPanel) !== state.step; });
        $$('.szw-step').forEach((li, i) => {
            li.classList.toggle('is-current', i === state.step);
            li.classList.toggle('is-done', i < state.step);
            const btn = li.querySelector('button');
            if (i === state.step) btn.setAttribute('aria-current', 'step'); else btn.removeAttribute('aria-current');
        });
        const current = stepsEl.querySelector('.is-current');
        if (current) {
            const left = Math.max(0, current.offsetLeft - (stepsEl.clientWidth - current.offsetWidth) / 2);
            const calm = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
            stepsEl.scrollTo({ left, behavior: calm ? 'auto' : 'smooth' });
        }
        const [ok, why] = canAdvance();
        const last = state.step === STEPS - 1;
        $('[data-szw-back]').disabled = state.step === 0;
        $('[data-szw-next]').hidden = last;
        $('[data-szw-next]').disabled = !ok;
        $('[data-szw-submit]').hidden = !last;
        $('[data-szw-steptxt]').textContent = ok ? `Passo ${state.step + 1} de ${STEPS}` : why;
        // campos que vão para o servidor
        $('[data-szw-start]').value = start || '';
        $('[data-szw-end]').value = end || '';
        $('[data-szw-plot-inputs]').innerHTML = [...state.sel].map((id) => `<input type="hidden" name="plot_ids" value="${Number(id)}">`).join('');
        const yields = {};
        c.prodPlots.forEach((p) => { const v = Number(state.yields[p.id]); if (v > 0) yields[p.id] = v; });
        $('[data-szw-goals]').value = JSON.stringify({ yields, budget: state.budget, price: state.price });
        updateAside(c);
    };
    const canJumpTo = (target) => {
        for (let i = 0; i < target; i += 1) {
            const saved = state.step; state.step = i;
            const [ok] = canAdvance(); state.step = saved;
            if (!ok) return i;
        }
        return target;
    };
    form.addEventListener('click', (event) => {
        const go = event.target.closest('[data-szw-go]');
        if (go) { goTo(canJumpTo(Number(go.dataset.szwGo))); }
    });
    $('[data-szw-back]').addEventListener('click', () => goTo(Math.max(0, state.step - 1)));
    $('[data-szw-next]').addEventListener('click', () => {
        if (canAdvance()[0]) { goTo(Math.min(STEPS - 1, state.step + 1)); $('.szw-main')?.scrollIntoView({ block: 'nearest' }); }
    });
    $('[data-szw-culture]').addEventListener('input', () => update());
    $('[data-szw-active]')?.addEventListener('change', () => update());
    form.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' && event.target.tagName === 'INPUT' && state.step < STEPS - 1) event.preventDefault();
    });
    form.addEventListener('submit', (event) => {
        if (state.step !== STEPS - 1) { event.preventDefault(); return; }
        update();
        window.showPageLoader?.(edit ? 'Salvando safra' : 'Criando safra', 'Só um instante.');
        try { sessionStorage.removeItem(DRAFT_KEY); } catch (error) { /* ok */ }
    });

    update();
    const modal = document.getElementById('seasons-launch-modal');
    if (backFromPlot && modal && !modal.open) modal.showModal();
})();
