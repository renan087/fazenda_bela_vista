/* Assistente de setor (5 passos) sobre o formulário existente: navegação, cor, "deixar para depois",
   mapa em tela cheia no celular e conferência final. Só envia no último passo. */
(() => {
    const form = document.querySelector('[data-plot-wizard]');
    if (!form) return;
    const $ = (sel) => form.querySelector(sel);
    const $$ = (sel) => Array.from(form.querySelectorAll(sel));
    const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const STEPS = 5;
    const editing = form.dataset.plotsEditMode === '1';
    let step = 0;

    const field = (name) => form.elements.namedItem(name);
    const val = (name) => {
        const el = field(name);
        if (!el) return '';
        if (el instanceof RadioNodeList) {
            const items = Array.from(el);
            if (items.some((item) => item.type === 'radio')) return el.value || '';
            const live = items.find((item) => !item.disabled && item.value) || items.find((item) => item.value);
            return live ? live.value.trim() : '';
        }
        return (el.value || '').trim();
    };
    const selectText = (name) => {
        let el = field(name);
        if (el instanceof RadioNodeList) el = Array.from(el).find((item) => item.options) || null;
        if (!el || !el.options) return '';
        const opt = el.options[el.selectedIndex];
        return opt && opt.value ? opt.textContent.trim() : '';
    };

    // ---------- cor no mapa ----------
    const modeInput = $('[data-stw-color-mode]');
    const colorInput = $('[data-stw-color]');
    const swatches = $('[data-stw-swatches]');
    const renderColor = () => {
        const manual = modeInput.value === 'manual';
        $$('[data-stw-mode]').forEach((btn) => btn.setAttribute('aria-checked', String((btn.dataset.stwMode === 'manual') === manual)));
        swatches.hidden = !manual;
        $('[data-stw-auto-note]').hidden = manual;
        $$('[data-stw-swatch]').forEach((btn) => btn.setAttribute('aria-checked', String(btn.dataset.stwSwatch === colorInput.value)));
    };
    $$('[data-stw-mode]').forEach((btn) => btn.addEventListener('click', () => {
        modeInput.value = btn.dataset.stwMode;
        if (modeInput.value === 'manual' && !colorInput.value) colorInput.value = $$('[data-stw-swatch]')[0]?.dataset.stwSwatch || '';
        renderColor();
    }));
    swatches?.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-stw-swatch]');
        if (!btn) return;
        colorInput.value = btn.dataset.stwSwatch;
        renderColor();
    });
    $('[data-stw-custom]')?.addEventListener('input', (event) => { colorInput.value = event.target.value; modeInput.value = 'manual'; renderColor(); });

    // ---------- deixar para depois (plantio e manejo) ----------
    const later = { plant: false, man: false };
    const applySkip = (kind) => {
        const group = $(`[data-stw-group="${kind}"]`);
        if (group) {
            group.hidden = later[kind];
            group.querySelectorAll('input, select, textarea, button').forEach((el) => {
                if (later[kind]) { el.dataset.stwWasDisabled = el.disabled ? '1' : ''; el.disabled = true; } else if (el.dataset.stwWasDisabled !== '1') el.disabled = false;
            });
        }
        const note = $(`[data-stw-later="${kind}"]`);
        if (note) note.hidden = !later[kind];
        $$(`[data-stw-skip="${kind}"]`).forEach((btn) => btn.setAttribute('aria-checked', String((btn.dataset.value === 'later') === later[kind])));
    };
    $$('[data-stw-skip]').forEach((btn) => btn.addEventListener('click', () => {
        later[btn.dataset.stwSkip] = btn.dataset.value === 'later';
        applySkip(btn.dataset.stwSkip);
        update();
    }));

    // ---------- mapa em tela cheia (celular) ----------
    const fullbar = $('[data-stw-fullbar]');
    const fullMsg = $('[data-stw-full-msg]');
    const editor = () => window.__stwEditor || null;
    const refreshCount = () => {
        const n = editor()?.pointCount?.() || 0;
        const counter = $('[data-stw-full-count]');
        if (counter) counter.textContent = `${n} ${n === 1 ? 'ponto' : 'pontos'}`;
    };
    const setFull = (on) => {
        form.classList.toggle('stw-full', on);
        if (fullbar) fullbar.hidden = !on;
        document.documentElement.classList.toggle('stw-lock', on);
        window.setTimeout(() => editor()?.invalidate?.(), 60);
        window.setTimeout(() => editor()?.invalidate?.(), 400);
        if (on) window.setTimeout(() => editor()?.fit?.(), 120);
        refreshCount();
    };
    $('[data-stw-fullmap-open]')?.addEventListener('click', () => {
        if (!editor()) {
            const farm = field('farm_id');
            window.alert(farm && !farm.value ? 'Escolha a fazenda no passo 1 para carregar o mapa.' : 'O mapa ainda está carregando. Tente de novo em instantes.');
            return;
        }
        setFull(true);
    });
    $('[data-stw-full-add]')?.addEventListener('click', () => { editor()?.addPointAtCenter?.(); refreshCount(); });
    $('[data-stw-full-undo]')?.addEventListener('click', () => { editor()?.undo?.(); refreshCount(); });
    $('[data-stw-full-done]')?.addEventListener('click', () => setFull(false));
    let msgTimer = null;
    window.addEventListener('plot-boundary-clamped', () => {
        const text = 'Esse ponto ficava fora da fazenda e foi colocado na divisa.';
        if (fullMsg && form.classList.contains('stw-full')) {
            fullMsg.textContent = text;
            fullMsg.hidden = false;
            window.clearTimeout(msgTimer);
            msgTimer = window.setTimeout(() => { fullMsg.hidden = true; }, 3500);
        } else {
            window.showToast?.(text);
        }
    });

    // ---------- conferência ----------
    const renderReview = () => {
        const area = parseFloat(val('area_hectares').replace(',', '.')) || 0;
        const areaTxt = `${area.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ha`;
        const boundary = $$('[data-plots-boundary-input]').some((el) => !el.disabled && el.value.trim());
        const hasFile = $$('input[type="file"][name="boundary_geojson_file"]').some((el) => el.files && el.files.length);
        const mapTxt = boundary || hasFile ? 'Contorno no mapa' : editing ? 'Contorno atual mantido' : 'Sem contorno';
        const variety = selectText('variety_id') || 'Variedade não informada';
        const plantDate = val('planting_date');
        const plants = val('plant_count');
        const irrigation = selectText('irrigation_type') || 'Nenhum';
        const soilEl = form.querySelector('input[name="soil_texture"]:checked');
        const soil = soilEl ? soilEl.nextElementSibling?.textContent : 'Solo não informado';
        const cards = [
            ['Identificação', val('name') || 'Sem nome', `${areaTxt} · ${selectText('farm_id') || 'Fazenda'}${val('group_name') ? ` · ${val('group_name')}` : ''}`, 0],
            ['Mapa', mapTxt, boundary || hasFile ? 'Dentro do perímetro da fazenda' : 'Dá para desenhar depois', 1],
            ['Plantio', later.plant ? 'Para completar depois' : variety, later.plant ? 'Fica fora das metas de sacas' : `${plantDate ? plantDate.split('-').reverse().join('/') : 'Data não informada'}${plants ? ` · ${Number(plants).toLocaleString('pt-BR')} plantas` : ''}`, 2],
            ['Manejo', later.man ? 'Para completar depois' : `Irrigação: ${irrigation}`, later.man ? 'Recomendação de adubação fica pendente' : soil, 3],
        ];
        $('[data-stw-review]').innerHTML = cards.map(([t, v, d, s]) => `
            <div class="szw-review-card"><div class="szw-head-row"><span class="szw-overline">${t}</span><button type="button" class="szw-link" data-stw-go="${s}">Alterar</button></div>
            <strong>${esc(v)}</strong><small>${esc(d)}</small></div>`).join('');
        const season = $('[data-stw-season]');
        const impact = [
            `${editing ? 'Salvar' : 'Criar'} o setor ${esc(val('name') || '')} com ${areaTxt}${boundary || hasFile ? ' e o contorno no mapa' : ''}.`,
            later.plant ? 'Deixar o plantio para depois.' : `Registrar o plantio: ${esc(variety)}.`,
            modeInput.value === 'manual' ? 'Usar a cor escolhida no mapa da fazenda.' : 'Dar uma cor automática, diferente dos vizinhos.',
        ];
        if (season && season.checked && !season.disabled) impact.push(`Incluir o setor na safra ativa.`);
        $('[data-stw-impact]').innerHTML = impact.map((t) => `<li>${t}</li>`).join('');
    };

    // ---------- navegação ----------
    const stepsEl = $('[data-stw-steps]');
    const problem = (target) => {
        if (target === 0) {
            if (!val('name')) return 'Dê um nome ao setor';
            const area = parseFloat(val('area_hectares').replace(',', '.'));
            if (!(area > 0)) return 'Informe a área em hectares';
            if (!val('farm_id')) return 'Escolha a fazenda';
        }
        return '';
    };
    const update = () => {
        $$('[data-stw-panel]').forEach((panel) => { panel.hidden = Number(panel.dataset.stwPanel) !== step; });
        $$('.szw-step').forEach((li, i) => {
            li.classList.toggle('is-current', i === step);
            li.classList.toggle('is-done', i < step);
            const btn = li.querySelector('button');
            if (i === step) btn.setAttribute('aria-current', 'step'); else btn.removeAttribute('aria-current');
        });
        const current = stepsEl?.querySelector('.is-current');
        if (current) {
            const left = Math.max(0, current.offsetLeft - (stepsEl.clientWidth - current.offsetWidth) / 2);
            const calm = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
            stepsEl.scrollTo({ left, behavior: calm ? 'auto' : 'smooth' });
        }
        const why = problem(step);
        const last = step === STEPS - 1;
        $('[data-stw-back]').disabled = step === 0;
        $('[data-stw-next]').hidden = last;
        $('[data-stw-next]').disabled = Boolean(why);
        $('[data-stw-submit]').hidden = !last;
        $('[data-stw-steptxt]').textContent = why || `Passo ${step + 1} de ${STEPS}`;
        if (last) renderReview();
        if (step === 1) window.setTimeout(() => { editor()?.invalidate?.(); if (!editor()?.pointCount?.()) editor()?.fit?.(); }, 80);
    };
    const goTo = (target) => {
        for (let i = 0; i < target; i += 1) {
            if (problem(i)) { step = i; update(); return; }
        }
        step = target;
        update();
        form.closest('[data-modal-scroll]')?.scrollTo?.({ top: 0 });
    };
    form.addEventListener('click', (event) => {
        const go = event.target.closest('[data-stw-go]');
        if (go) goTo(Number(go.dataset.stwGo));
    });
    $('[data-stw-back]').addEventListener('click', () => goTo(Math.max(0, step - 1)));
    $('[data-stw-next]').addEventListener('click', () => { if (!problem(step)) goTo(Math.min(STEPS - 1, step + 1)); });
    ['name', 'area_hectares', 'farm_id'].forEach((name) => {
        const el = field(name);
        const list = el instanceof RadioNodeList ? Array.from(el) : [el];
        list.forEach((item) => item?.addEventListener?.('input', update));
        list.forEach((item) => item?.addEventListener?.('change', update));
    });
    form.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' && event.target.tagName === 'INPUT' && step < STEPS - 1) event.preventDefault();
    });
    form.addEventListener('submit', (event) => {
        if (step !== STEPS - 1) { event.preventDefault(); event.stopImmediatePropagation(); goTo(Math.min(STEPS - 1, step + 1)); return; }
        const why = problem(0);
        if (why) { event.preventDefault(); event.stopImmediatePropagation(); goTo(0); }
    }, true);

    renderColor();
    update();
})();
