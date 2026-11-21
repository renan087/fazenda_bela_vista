/* Assistente de safra (canvas aprovado "Nova safra — assistente em 5 passos" + versão celular).
   Lê #season-wizard-data, guarda rascunho no aparelho e só envia no último passo. */
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
    const n2 = (v) => Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 2 });
    const brl = (v) => 'R$ ' + Math.round(Number(v || 0)).toLocaleString('pt-BR');
    const digits = (v) => { const d = String(v ?? '').replace(/\D/g, ''); return d === '' ? null : Number(d); };
    const dateBr = (iso) => (iso ? iso.split('-').reverse().join('/') : '—');
    const STAGE = { prod: ['Em produção', 'is-ok'], form: ['Em formação', 'is-info'], sem: ['Sem plantio informado', 'is-muted'] };
    const STEPS = 5;
    const STEP_NAMES = ['Período', 'Setores', 'Metas', 'Integrações', 'Conferir'];
    const edit = data.edit || null;
    const plots = data.plots || [];
    const periods = data.periods || [];
    const categories = data.categories || [];
    const thisYear = new Date(`${data.today}T12:00:00`).getFullYear();
    const DRAFT_KEY = `sisfarm.seasonDraft.${data.farm_name || 'fazenda'}.${edit ? edit.id : 'nova'}`;
    const nameInput = $('[data-szw-name]');
    const cultureInput = $('[data-szw-culture]');
    const notesInput = $('[data-szw-notes]');
    const priceInput = $('[data-szw-price]');

    const state = {
        step: 0,
        period: periods[0] ? periods[0].key : 'custom',
        start: '', end: '', nameTouched: false, copyPrev: false,
        sel: new Set(), yields: {}, budget: {}, price: data.price ? Math.round(data.price) : null,
    };

    function applyGoals(goals) {
        if (!goals) return;
        Object.entries(goals.yields || {}).forEach(([id, v]) => { state.yields[Number(id)] = Number(v); });
        Object.entries(goals.budget || {}).forEach(([k, v]) => { state.budget[k] = Number(v); });
        if (goals.price) state.price = Math.round(Number(goals.price));
    }
    const integInputs = () => $$('[data-szw-integ]');
    const setIntegrations = (values) => {
        integInputs().forEach((input) => { if (!input.disabled && values && input.dataset.szwInteg in values) input.checked = Boolean(values[input.dataset.szwInteg]); });
    };

    // ---------- estado inicial ----------
    if (edit) {
        const match = periods.find((p) => p.start === edit.start && p.end === edit.end);
        state.period = match ? match.key : 'custom';
        state.start = edit.start; state.end = edit.end; state.nameTouched = true;
        nameInput.value = edit.name || '';
        cultureInput.value = edit.culture || 'Café';
        notesInput.value = edit.notes || '';
        (edit.plot_ids || []).forEach((id) => state.sel.add(id));
        applyGoals(edit.goals);
        setIntegrations(edit.integrations);
    } else {
        cultureInput.value = 'Café';
        plots.filter((p) => p.stage !== 'sem').forEach((p) => state.sel.add(p.id));
        plots.forEach((p) => { if (p.hist) state.yields[p.id] = Math.round(p.hist); });
    }

    // ---------- rascunho no aparelho ----------
    const draftStatus = document.querySelector('[data-szw-draft-status]');
    const hhmm = (d) => d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
    const collect = () => ({
        period: state.period, start: state.start, end: state.end, name: nameInput.value, nameTouched: state.nameTouched,
        culture: cultureInput.value, notes: notesInput.value, sel: [...state.sel], yields: state.yields, budget: state.budget,
        price: state.price, step: state.step, integrations: Object.fromEntries(integInputs().map((i) => [i.dataset.szwInteg, i.checked])),
        savedAt: new Date().toISOString(),
    });
    let ready = false;
    let touched = false;
    const saveDraft = (manual) => {
        if (!manual && (!ready || !touched)) return;
        try {
            localStorage.setItem(DRAFT_KEY, JSON.stringify(collect()));
            if (draftStatus) draftStatus.textContent = `${manual ? 'rascunho salvo' : 'rascunho salvo automaticamente'} às ${hhmm(new Date())}`;
            if (manual) window.showToast?.('Rascunho salvo neste aparelho.');
        } catch (error) {
            if (manual) window.alert('Não foi possível salvar o rascunho neste aparelho.');
        }
    };
    const clearDraft = () => { try { localStorage.removeItem(DRAFT_KEY); } catch (error) { /* ok */ } };
    let draftTimer = null;
    const scheduleDraft = () => { window.clearTimeout(draftTimer); draftTimer = window.setTimeout(() => saveDraft(false), 800); };
    window.saveSeasonDraft = () => saveDraft(true);
    document.querySelectorAll('[data-szw-save-draft]').forEach((btn) => btn.addEventListener('click', () => saveDraft(true)));

    const query = new URLSearchParams(window.location.search);
    try {
        const draft = JSON.parse(localStorage.getItem(DRAFT_KEY) || 'null');
        if (draft) {
            Object.assign(state, { period: draft.period, start: draft.start, end: draft.end, nameTouched: draft.nameTouched, yields: draft.yields || {}, budget: draft.budget || {}, price: draft.price ?? state.price });
            state.sel = new Set((draft.sel || []).filter((id) => plots.some((p) => p.id === id)));
            nameInput.value = draft.name || '';
            cultureInput.value = draft.culture || cultureInput.value;
            notesInput.value = draft.notes || '';
            setIntegrations(draft.integrations);
            state.step = Math.min(Number(draft.step) || 0, 1);
            if (draftStatus && draft.savedAt) {
                const at = new Date(draft.savedAt);
                draftStatus.innerHTML = `rascunho recuperado de ${at.toLocaleDateString('pt-BR')} às ${hhmm(at)} · <button type="button" class="szw-link szw-link-inline" data-szw-discard>Descartar</button>`;
                draftStatus.querySelector('[data-szw-discard]')?.addEventListener('click', () => { clearDraft(); window.location.reload(); });
            }
        }
    } catch (error) { /* rascunho ilegível: ignora */ }
    const newPlot = Number(query.get('setor'));
    if (newPlot && plots.some((p) => p.id === newPlot)) { state.sel.add(newPlot); state.step = 1; }

    // ---------- cálculos ----------
    const periodDates = () => {
        if (state.period === 'custom') return [state.start, state.end];
        const p = periods.find((item) => item.key === state.period);
        return p ? [p.start, p.end] : ['', ''];
    };
    const statusOf = () => {
        const [start, end] = periodDates();
        if (edit && edit.status === 'encerrada') return ['Encerrada', 'is-muted', 'Safra encerrada.'];
        if (!start || !end) return ['Defina as datas', 'is-muted', ''];
        if (data.today < start) return ['Planejada', 'is-info', 'Vira “Em andamento” sozinha quando chegar o início.'];
        if (data.today > end) return ['Encerrada', 'is-muted', 'O período já terminou.'];
        return ['Em andamento', 'is-ok', `Começou em ${dateBr(start)}, calculado pelas datas.`];
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
    const seasonName = () => nameInput.value.trim() || autoName() || 'Nova safra';
    const harvestYear = () => { const [, end] = periodDates(); return end ? end.slice(0, 4) : ''; };
    const integrationValues = () => Object.fromEntries(integInputs().map((i) => [i.dataset.szwInteg, i.checked && !i.disabled]));

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
        if (state.period === 'custom' && !state.start && periods[0]) { state.start = periods[0].start; state.end = periods[0].end; }
        update();
    });
    $('[data-szw-start-input]').addEventListener('change', (e) => { state.start = e.target.value; update(); });
    $('[data-szw-end-input]').addEventListener('change', (e) => { state.end = e.target.value; update(); });
    nameInput.addEventListener('input', () => { state.nameTouched = true; updateChrome(calc()); scheduleDraft(); });
    const cultureField = $('[data-szw-culture-field]');
    $('[data-szw-culture-toggle]')?.addEventListener('click', () => { cultureField.hidden = !cultureField.hidden; if (!cultureField.hidden) cultureInput.focus(); });
    if ((cultureInput.value || 'Café').toLowerCase() !== 'café') cultureField.hidden = false;
    cultureInput.addEventListener('input', () => { $('[data-szw-culture-label]').textContent = cultureInput.value.trim() || '—'; update(); });
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
    const backTo = edit ? `safra:${edit.id}` : 'safra';
    const renderPlots = () => {
        const cards = plots.map((p) => {
            const on = state.sel.has(p.id);
            const [stageTxt, stageCls] = STAGE[p.stage] || STAGE.sem;
            const age = p.year ? `${Math.max(0, thisYear - p.year)} anos` : '—';
            const foot = p.stage === 'prod' ? (p.hist ? `Média das 3 últimas safras: ${n1(p.hist)} sc/ha` : 'Ainda sem colheita registrada') : 'Entra nos custos, sem colheita prevista';
            return `<button type="button" class="szw-plot${on ? ' is-on' : ''}" aria-pressed="${on}" data-szw-plot="${p.id}">
                <span class="szw-plot-top"><span class="szw-box" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12l5 5L20 7"/></svg></span>
                <span class="szw-grow"><span class="szw-plot-name"><strong>${esc(p.name)}</strong><b class="szw-plot-ha">${n2(p.area)} ha</b></span><small>${esc(p.variety || 'Variedade não informada')}<span class="szw-plot-age"> · ${age}</span></small><span class="szw-chip szw-chip-m ${stageCls}">${stageTxt}</span></span>
                <span class="szw-chip szw-chip-d ${stageCls}">${stageTxt}</span></span>
                <span class="szw-plot-stats"><span><small>Área</small><b>${n2(p.area)} ha</b></span><span><small>Plantas</small><b>${p.plants ? Number(p.plants).toLocaleString('pt-BR') : '—'}</b></span><span><small>Idade</small><b>${age}</b></span></span>
                <small class="szw-plot-foot">${foot}</small>
            </button>`;
        });
        cards.push(`<a class="szw-newcard" href="/setores?novo=1&amp;voltar=${backTo}" data-szw-new-plot>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>
            <span>Cadastrar novo setor</span><small>Volta para cá depois de salvar</small></a>`);
        plotsBox.innerHTML = cards.join('');
    };
    plotsBox.addEventListener('click', (event) => {
        if (event.target.closest('[data-szw-new-plot]')) {
            saveDraft(false);
            window.showPageLoader?.('Abrindo cadastro de setor', 'Os dados desta safra ficam guardados.');
            return;
        }
        const btn = event.target.closest('[data-szw-plot]');
        if (!btn) return;
        const id = Number(btn.dataset.szwPlot);
        if (state.sel.has(id)) state.sel.delete(id); else state.sel.add(id);
        update();
    });
    $$('[data-szw-select]').forEach((btn) => btn.addEventListener('click', () => {
        state.sel = new Set(plots.filter((p) => btn.dataset.szwSelect === 'all' || p.stage === 'prod').map((p) => p.id));
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
                <span class="szw-stepper"><button type="button" data-szw-dec="${p.id}" aria-label="Diminuir produtividade de ${esc(p.name)}">−</button><span class="szw-stepper-val"><input inputmode="decimal" value="${esc(v === '' ? '' : String(v).replace('.', ','))}" data-szw-yield="${p.id}" aria-label="Sacas por hectare em ${esc(p.name)}"><em>sc/ha</em></span><button type="button" data-szw-inc="${p.id}" aria-label="Aumentar produtividade de ${esc(p.name)}">+</button></span>
                <span class="szw-row-total" data-szw-yield-total="${p.id}">${n1(p.area * (Number(v) || 0))} sc</span>
            </div>`;
        }).join('') : '<p class="szw-muted szw-row">Nenhum setor em produção selecionado.</p>';
        budgetBox.innerHTML = categories.map((cat) => {
            const v = state.budget[cat.key];
            return `<div class="szw-row">
                <label class="szw-grow" for="szw-b-${cat.key}"><strong>${esc(cat.label)}</strong><small>${esc(cat.hint)}</small></label>
                <span class="szw-money-box"><em>R$</em><input id="szw-b-${cat.key}" inputmode="numeric" value="${v ? Number(v).toLocaleString('pt-BR') : ''}" data-szw-budget-key="${cat.key}"></span>
                <span class="szw-row-total" data-szw-budget-total-key="${cat.key}">${brl((Number(v) || 0) * c.area)}</span>
            </div>`;
        }).join('');
        if (state.price) priceInput.value = Number(state.price).toLocaleString('pt-BR');
    };
    prodBox.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-szw-dec], [data-szw-inc]');
        if (!btn) return;
        const dec = btn.hasAttribute('data-szw-dec');
        const id = Number(btn.dataset.szwDec || btn.dataset.szwInc);
        state.yields[id] = Math.max(0, Math.round((Number(state.yields[id]) || 0) + (dec ? -1 : 1)));
        const input = prodBox.querySelector(`[data-szw-yield="${id}"]`);
        if (input) input.value = String(state.yields[id]);
        refreshNumbers();
    });
    prodBox.addEventListener('input', (event) => {
        const input = event.target.closest('[data-szw-yield]');
        if (!input) return;
        const value = parseFloat(input.value.replace(',', '.'));
        state.yields[Number(input.dataset.szwYield)] = Number.isFinite(value) && value >= 0 ? value : '';
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
    priceInput.addEventListener('input', () => {
        state.price = digits(priceInput.value);
        priceInput.value = state.price === null ? '' : state.price.toLocaleString('pt-BR');
        refreshNumbers();
    });
    if (data.price) $('[data-szw-price-note]').textContent = `Preço de referência vem da cotação de ${data.price_date} em Comercialização.`;

    // ---------- passo 4 ----------
    integInputs().forEach((input) => input.addEventListener('change', () => update()));
    $('[data-szw-active]')?.addEventListener('change', () => update());

    // ---------- números, resumo, celular ----------
    const tipFor = (c) => {
        if (state.step === 0) return 'Começar da safra anterior economiza tempo: você só ajusta o que mudou.';
        if (state.step === 1) {
            const forming = c.sel.find((p) => p.stage !== 'prod');
            return forming ? `O setor ${forming.name} ${forming.stage === 'form' ? 'ainda está em formação' : 'está sem plantio informado'}: ele soma custo, mas não entra na meta de sacas.` : 'Marque só os setores que vão ter custo ou colheita nesta safra.';
        }
        if (state.step === 2) {
            const withHist = c.prodPlots.filter((p) => p.hist);
            const area = withHist.reduce((a, p) => a + p.area, 0);
            const avg = area ? withHist.reduce((a, p) => a + p.area * p.hist, 0) / area : 0;
            return avg ? `Sua média histórica é ${n1(avg)} sc/ha. Metas muito acima disso merecem uma conversa com o agrônomo.` : 'Sem histórico de colheita ainda? Use a estimativa do agrônomo e ajuste depois.';
        }
        if (state.step === 3) return 'Integrações criam rascunhos. Você revisa cada aplicação antes de qualquer baixa no estoque.';
        return 'Depois de criar, eu aviso quando o custo chegar a 80% do orçado.';
    };
    const canAdvance = (step = state.step) => {
        if (step === 0) {
            const [start, end] = periodDates();
            if (!start || !end) return [false, 'Escolha o período'];
            if (end < start) return [false, 'O fim precisa ser depois do início'];
            if (!cultureInput.value.trim()) return [false, 'Informe a cultura'];
        }
        if (step === 1 && !state.sel.size) return [false, 'Escolha pelo menos um setor'];
        return [true, ''];
    };
    const updateChrome = (c) => {
        const [start, end] = periodDates();
        const [statusTxt, statusCls] = statusOf();
        $('[data-szw-aside-name]').textContent = seasonName();
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
        $('[data-szw-tip]').textContent = tipFor(c);
        const titles = ['Período da safra', `Setores da ${seasonName()}`, 'Produção e orçamento', 'O que preparar junto', edit ? 'Confira antes de salvar' : 'Confira antes de criar'];
        $('[data-szw-mstep]').textContent = `${edit ? 'Editar safra' : 'Nova safra'} · Passo ${state.step + 1} de ${STEPS}`;
        $('[data-szw-mtitle]').textContent = titles[state.step];
        $$('[data-szw-bar]').forEach((bar, i) => bar.classList.toggle('is-on', i <= state.step));
        const n = c.sel.length;
        const integCount = Object.values(integrationValues()).filter(Boolean).length + ($('[data-szw-active]')?.checked ? 1 : 0);
        const sheet = [
            [start && end ? `${dateBr(start)} a ${dateBr(end)}` : 'Escolha o período', seasonName()],
            [`${n} ${n === 1 ? 'setor selecionado' : 'setores selecionados'}`, `${n2(c.area)} ha`],
            ['Total previsto', `${n1(c.sacks)} sacas`],
            ['Integrações ligadas', String(integCount)],
            ['Orçamento', c.cost ? brl(c.cost) : '—'],
        ][state.step];
        $('[data-szw-sheet-label]').textContent = sheet[0];
        $('[data-szw-sheet-value]').textContent = sheet[1];
        const [ok, why] = canAdvance();
        const sheetBtn = $('[data-szw-sheet-next]');
        sheetBtn.textContent = !ok ? why : state.step === STEPS - 1 ? (edit ? 'Salvar safra' : 'Criar safra') : `Continuar para ${STEP_NAMES[state.step + 1].toLowerCase()}`;
        sheetBtn.disabled = !ok;
        const year = harvestYear();
        $('[data-szw-harvest-title]').textContent = `Prever a colheita de maio a agosto${year ? `/${year}` : ''}`;
    };
    const syncHidden = (c) => {
        const [start, end] = periodDates();
        $('[data-szw-start]').value = start || '';
        $('[data-szw-end]').value = end || '';
        $('[data-szw-plot-inputs]').innerHTML = [...state.sel].map((id) => `<input type="hidden" name="plot_ids" value="${Number(id)}">`).join('');
        const yields = {};
        c.prodPlots.forEach((p) => { const v = Number(state.yields[p.id]); if (v > 0) yields[p.id] = v; });
        $('[data-szw-goals]').value = JSON.stringify({ yields, budget: state.budget, price: state.price });
        $('[data-szw-integrations]').value = JSON.stringify(integrationValues());
    };
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
        note.textContent = noProd.length ? `Sem colheita nesta safra: ${noProd.map((p) => `${p.name} (${STAGE[p.stage][0].toLowerCase()})`).join(', ')}` : '';
        $('[data-szw-sacks]').textContent = `${n1(c.sacks)} sacas`;
        $('[data-szw-area-txt]').textContent = `${n2(c.area)} ha`;
        $('[data-szw-budget-ha]').textContent = `${brl(c.budgetHa)}/ha`;
        $('[data-szw-budget-total]').textContent = brl(c.cost);
        $('[data-szw-revenue]').textContent = c.revenue === null ? '—' : brl(c.revenue);
        $('[data-szw-cost]').textContent = brl(c.cost);
        const margin = $('[data-szw-margin]');
        margin.textContent = c.margin === null ? '—' : brl(c.margin);
        margin.className = c.margin === null ? '' : c.margin >= 0 ? 'is-pos' : 'is-neg';
        $('[data-szw-breakeven]').textContent = c.breakeven === null ? '—' : `${n1(c.breakeven)} sc/ha`;
        updateChrome(c);
        syncHidden(c);
        scheduleDraft();
    };
    const renderReview = (c) => {
        const [start, end] = periodDates();
        const [statusTxt] = statusOf();
        const integ = integrationValues();
        const active = $('[data-szw-active]');
        const on = [active && active.checked ? 'Contexto' : null, integ.adub ? 'Adubação' : null, integ.fin ? 'Financeiro' : null, integ.colh ? 'Colheita' : null, integ.est ? 'Estoque' : null, integ.lemb ? 'Assistente' : null].filter(Boolean);
        const cards = [
            ['Período', seasonName(), `${dateBr(start)} a ${dateBr(end)} · ${statusTxt}`, 0],
            ['Setores', `${c.sel.length} ${c.sel.length === 1 ? 'setor' : 'setores'} · ${n2(c.area)} ha`, c.varieties.join(', ') || 'Variedades não informadas', 1],
            ['Metas', `${n1(c.sacks)} sacas`, `Orçamento ${brl(c.cost)}${c.breakeven ? ` · equilíbrio ${n1(c.breakeven)} sc/ha` : ''}`, 2],
            ['Integrações', `${on.length} ${on.length === 1 ? 'ligada' : 'ligadas'}`, on.join(', ') || 'Nenhuma', 3],
        ];
        $('[data-szw-review]').innerHTML = cards.map(([t, v, d, step]) => `
            <div class="szw-review-card"><div class="szw-review-top"><span class="szw-overline">${t}</span><button type="button" class="szw-link" data-szw-go="${step}">Alterar</button></div>
            <strong>${esc(v)}</strong><small>${esc(d)}</small></div>`).join('');
        const impact = [
            `${edit ? 'Salvar' : 'Criar'} a ${esc(seasonName())} (${dateBr(start)} a ${dateBr(end)}) como “${statusTxt}”.`,
            `Vincular ${c.sel.length} ${c.sel.length === 1 ? 'setor' : 'setores'} (${n2(c.area)} ha), guardando o plantio de cada um.`,
        ];
        if (active && active.checked && !active.disabled) impact.push('Trocar a safra ativa do topo para esta safra.');
        if (integ.adub && data.previous) impact.push(`Criar ${data.previous.schedules} ${data.previous.schedules === 1 ? 'aplicação' : 'aplicações'} de adubação em rascunho.`);
        if (integ.fin) impact.push(`Registrar orçamento de ${brl(c.cost)} para comparar com as despesas da safra.`);
        if (integ.colh) impact.push(`Marcar a colheita de maio a agosto/${harvestYear()} no painel da safra.`);
        if (integ.est) impact.push('Mostrar a lista de insumos a comprar (sem mexer no estoque).');
        if (integ.lemb) impact.push('Ligar os avisos do Pezinho para esta safra.');
        $('[data-szw-impact]').innerHTML = impact.map((t) => `<li>${t}</li>`).join('');
    };

    // ---------- navegação ----------
    const stepsEl = $('[data-szw-steps]');
    function update() {
        const c = calc();
        if (!state.nameTouched) nameInput.value = autoName();
        $('[data-szw-start-input]').value = state.start || '';
        $('[data-szw-end-input]').value = state.end || '';
        const [statusTxt, statusCls, statusHint] = statusOf();
        const status = $('[data-szw-status]');
        status.textContent = statusTxt; status.className = `szw-chip ${statusCls}`;
        $('[data-szw-status-hint]').textContent = statusHint;
        renderPeriods();
        const n = c.sel.length;
        $('[data-szw-plot-strip]').innerHTML = `<strong>${n} ${n === 1 ? 'setor selecionado' : 'setores selecionados'}</strong><span>${n2(c.area)} ha no total</span><span>${c.varieties.length} ${c.varieties.length === 1 ? 'variedade' : 'variedades'}</span>`;
        if (state.step === 1) renderPlots();
        if (state.step === 2) renderGoals(c);
        if (state.step === 4) renderReview(c);
        $$('[data-szw-panel]').forEach((panel) => { panel.hidden = Number(panel.dataset.szwPanel) !== state.step; });
        $$('.szw-step').forEach((li, i) => {
            li.classList.toggle('is-current', i === state.step);
            li.classList.toggle('is-done', i < state.step);
            const btn = li.querySelector('button');
            if (i === state.step) btn.setAttribute('aria-current', 'step'); else btn.removeAttribute('aria-current');
        });
        const current = stepsEl.querySelector('.is-current');
        if (current && stepsEl.scrollWidth > stepsEl.clientWidth) {
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
        refreshNumbers();
    }
    const goTo = (target) => {
        for (let i = 0; i < target; i += 1) {
            if (!canAdvance(i)[0]) { state.step = i; update(); return; }
        }
        state.step = target;
        update();
        if (window.matchMedia('(max-width: 639px)').matches) window.scrollTo({ top: 0 });
    };
    form.addEventListener('click', (event) => {
        const go = event.target.closest('[data-szw-go]');
        if (go) goTo(Number(go.dataset.szwGo));
    });
    $('[data-szw-back]').addEventListener('click', () => goTo(Math.max(0, state.step - 1)));
    $('[data-szw-mback]').addEventListener('click', () => { if (state.step === 0) window.location.href = '/safras'; else goTo(state.step - 1); });
    $('[data-szw-next]').addEventListener('click', () => { if (canAdvance()[0]) goTo(Math.min(STEPS - 1, state.step + 1)); });
    $('[data-szw-sheet-next]').addEventListener('click', () => {
        if (!canAdvance()[0]) return;
        if (state.step === STEPS - 1) form.requestSubmit();
        else goTo(state.step + 1);
    });
    form.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' && event.target.tagName === 'INPUT' && state.step < STEPS - 1) event.preventDefault();
    });
    form.addEventListener('submit', (event) => {
        if (state.step !== STEPS - 1) { event.preventDefault(); return; }
        syncHidden(calc());
        clearDraft();
        window.clearTimeout(draftTimer);
        ready = false;
        window.showPageLoader?.(edit ? 'Salvando safra' : 'Criando safra', 'Só um instante.');
    });

    $('[data-szw-culture-label]').textContent = cultureInput.value || 'Café';
    update();
    ready = true;
    ['input', 'change'].forEach((evt) => form.addEventListener(evt, () => { touched = true; }, true));
    form.addEventListener('click', (event) => { if (event.target.closest('button, a, input, label')) touched = true; }, true);
})();
