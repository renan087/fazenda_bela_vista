/* Assistente de setor fiel ao canvas "Novo setor — assistente em 5 passos".
   Identificação → Mapa → Plantio → Manejo → Conferir. Só envia no último passo. */
(() => {
    const form = document.querySelector('[data-plot-wizard]');
    if (!form) return;
    const dialog = document.getElementById('plots-launch-modal');
    const $ = (sel) => form.querySelector(sel);
    const $$ = (sel) => Array.from(form.querySelectorAll(sel));
    const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const num = (value) => {
        let text = String(value ?? '').trim().replace(/\s/g, '');
        if (text.includes(',')) text = text.replace(/\./g, '').replace(',', '.');
        const n = parseFloat(text);
        return Number.isFinite(n) ? n : 0;
    };
    const n1 = (v) => Number(v || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 });
    const n2 = (v) => Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const int = (v) => Math.round(Number(v || 0)).toLocaleString('pt-BR');
    let data = {};
    try { data = JSON.parse(document.getElementById('plot-wizard-data')?.textContent || '{}'); } catch (e) { data = {}; }
    let farmBounds = {};
    try { farmBounds = JSON.parse(document.getElementById('plot-farm-boundaries-data')?.textContent || '{}') || {}; } catch (e) { farmBounds = {}; }
    const editing = form.dataset.plotsEditMode === '1';
    const editId = data.edit ? data.edit.id : null;
    const STEPS = 5;
    const today = new Date(`${data.today || new Date().toISOString().slice(0, 10)}T12:00:00`);
    const IRR = { none: 'Sequeiro', gotejo: 'Gotejamento', aspersor: 'Aspersão' };
    const SOIL = { argiloso: 'Argiloso', medio: 'Médio', arenoso: 'Arenoso', nao_sei: 'Não sei' };
    let step = 0;
    const later = { plant: false, man: false };
    const map = { vertices: 0, area: 0, pointOnly: false, started: false, areaChoice: null };

    const field = (name) => form.elements.namedItem(name);
    const val = (name) => {
        const el = field(name);
        if (!el) return '';
        if (el instanceof RadioNodeList) {
            const items = Array.from(el);
            if (items.some((item) => item.type === 'radio')) return el.value || '';
            const live = items.find((item) => !item.disabled && item.value);
            return live ? live.value.trim() : '';
        }
        return (el.value || '').trim();
    };
    const farmId = () => val('farm_id');
    const farmName = () => (data.farms || {})[farmId()] || '';
    const declared = () => num(val('area_hectares'));
    const boundaryInput = () => $('[data-plots-boundary-input]');
    const editor = () => window.__stwEditor || null;
    const geometryPoints = () => {
        try { return window.extractFarmBoundaryPoints?.(JSON.parse(boundaryInput()?.value || 'null')) || []; } catch (e) { return []; }
    };
    const effectiveArea = () => (map.vertices >= 3 && map.areaChoice !== 'declared' && map.area > 0 && (map.areaChoice === 'map' || !declared()) ? map.area : declared());

    // ---------- cor no mapa ----------
    const modeInput = $('[data-stw-color-mode]');
    const colorInput = $('[data-stw-color]');
    const fills = (data.colors || []).map((c) => c[2]);
    const strokeOf = (fill) => ((data.colors || []).find((c) => c[2] === fill) || [null, fill])[1];
    const box = (pts) => (pts.length ? [Math.min(...pts.map((p) => p.lng)), Math.min(...pts.map((p) => p.lat)), Math.max(...pts.map((p) => p.lng)), Math.max(...pts.map((p) => p.lat))] : null);
    const autoColor = () => {
        const others = (data.used || []).filter((p) => String(p.farm_id) === farmId() && p.id !== editId);
        const used = Object.fromEntries(fills.map((f) => [f, 0]));
        others.forEach((p) => { if (p.fill in used) used[p.fill] += 1; });
        const own = box(geometryPoints());
        const pad = 0.002;
        const neighbors = own ? others.filter((p) => p.box && p.box[0] - pad <= own[2] && own[0] <= p.box[2] + pad && p.box[1] - pad <= own[3] && own[1] <= p.box[3] + pad) : [];
        const blocked = new Set(neighbors.map((p) => p.fill));
        const candidates = fills.filter((f) => !blocked.has(f));
        const pool = candidates.length ? candidates : fills;
        const pick = pool.slice().sort((a, b) => (used[a] - used[b]) || (fills.indexOf(a) - fills.indexOf(b)))[0] || '#fbbf24';
        return { fill: pick, neighbors: neighbors.map((p) => p.name) };
    };
    const currentColor = () => (modeInput.value === 'manual' && colorInput.value ? colorInput.value : autoColor().fill);
    const renderColor = () => {
        const manual = modeInput.value === 'manual';
        $$('[data-stw-mode]').forEach((btn) => btn.setAttribute('aria-checked', String((btn.dataset.stwMode === 'manual') === manual)));
        $('[data-stw-swatches]').hidden = !manual;
        $('[data-stw-auto-note]').hidden = manual;
        const auto = autoColor();
        const sw = $('[data-stw-auto-swatch]');
        sw.style.background = auto.fill;
        sw.style.borderColor = strokeOf(auto.fill);
        $('[data-stw-auto-neighbors]').textContent = auto.neighbors.length ? ` (${auto.neighbors.slice(0, 3).join(', ').replace(/, ([^,]*)$/, ' e $1')})` : '';
        if (!manual) colorInput.value = auto.fill;
        $$('[data-stw-swatch]').forEach((btn) => btn.setAttribute('aria-checked', String(manual && btn.dataset.stwSwatch === colorInput.value)));
    };
    $$('[data-stw-mode]').forEach((btn) => btn.addEventListener('click', () => {
        modeInput.value = btn.dataset.stwMode;
        if (modeInput.value === 'manual' && !fills.includes(colorInput.value) && !/^#[0-9a-f]{6}$/i.test(colorInput.value)) colorInput.value = fills[0] || '';
        renderColor(); refresh();
    }));
    $('[data-stw-swatches]').addEventListener('click', (event) => {
        const btn = event.target.closest('[data-stw-swatch]');
        if (!btn) return;
        colorInput.value = btn.dataset.stwSwatch;
        renderColor(); refresh();
    });
    $('[data-stw-custom]')?.addEventListener('input', (event) => { colorInput.value = event.target.value; modeInput.value = 'manual'; renderColor(); refresh(); });

    // ---------- deixar para depois ----------
    const applySkip = (kind) => {
        const group = $(`[data-stw-group="${kind}"]`);
        if (group) {
            group.hidden = later[kind];
            group.querySelectorAll('input, select, textarea').forEach((el) => {
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
        refresh();
    }));

    // ---------- mapa ----------
    const prompt = $('[data-stw-prompt]');
    const syncPrompt = () => { if (prompt) prompt.hidden = !editor() || map.started || map.vertices > 0 || map.pointOnly || editing; };
    window.addEventListener('plot-boundary-stats', (event) => {
        map.vertices = event.detail?.vertices || 0;
        map.area = event.detail?.area || 0;
        if (map.vertices > 0) map.started = true;
        $('[data-stw-vertices]').textContent = String(map.vertices);
        $('[data-stw-map-area]').textContent = `${n2(map.area)} ha`;
        const counter = $('[data-stw-full-count]');
        if (counter) counter.textContent = `${map.vertices} ${map.vertices === 1 ? 'ponto' : 'pontos'}`;
        syncPrompt();
        renderColor();
        refresh();
    });
    window.addEventListener('plot-boundary-ready', () => { syncPrompt(); renderColor(); refresh(); });
    $('[data-stw-start]')?.addEventListener('click', () => { map.started = true; syncPrompt(); });
    $('[data-stw-reset]')?.addEventListener('click', () => editor()?.resetToInitial?.());
    let snapTimer = null;
    window.addEventListener('plot-boundary-clamped', () => {
        const fullMsg = $('[data-stw-full-msg]');
        if (form.classList.contains('stw-full') && fullMsg) {
            fullMsg.textContent = 'Esse ponto ficava fora da fazenda e foi colocado na divisa.';
            fullMsg.hidden = false;
            window.clearTimeout(snapTimer);
            snapTimer = window.setTimeout(() => { fullMsg.hidden = true; }, 3500);
            return;
        }
        const snap = $('[data-stw-snap]');
        snap.hidden = false;
        window.clearTimeout(snapTimer);
        snapTimer = window.setTimeout(() => { snap.hidden = true; }, 6000);
    });
    const pointBox = $('[data-stw-point-box]');
    $('[data-stw-point-toggle]').addEventListener('click', () => {
        map.pointOnly = !map.pointOnly;
        pointBox.hidden = !map.pointOnly;
        $('[data-stw-point-toggle]').textContent = map.pointOnly ? 'Voltar a desenhar o contorno' : 'Não tenho o contorno agora: marcar só a localização';
        syncPrompt(); refresh();
    });
    $('[data-stw-point-mark]').addEventListener('click', () => {
        const ed = editor();
        if (!ed?.map) return;
        const c = ed.map.getCenter();
        const lat = typeof c.lat === 'function' ? c.lat() : c.lat;
        const lng = typeof c.lng === 'function' ? c.lng() : c.lng;
        $('[data-stw-lat]').value = lat.toFixed(6);
        $('[data-stw-lng]').value = lng.toFixed(6);
        $('[data-stw-point-mark]').textContent = 'Localização marcada · marcar de novo';
        refresh();
    });

    // celular: desenho sempre em tela cheia
    const fullbar = $('[data-stw-fullbar]');
    const setFull = (on) => {
        form.classList.toggle('stw-full', on);
        if (fullbar) fullbar.hidden = !on;
        document.documentElement.classList.toggle('stw-lock', on);
        window.setTimeout(() => editor()?.invalidate?.(), 60);
        if (on) window.setTimeout(() => { editor()?.invalidate?.(); if (!editor()?.pointCount?.()) editor()?.fit?.(); }, 300);
    };
    $('[data-stw-fullmap-open]')?.addEventListener('click', () => {
        if (!editor()) { window.alert(farmId() ? 'O mapa ainda está carregando. Tente de novo em instantes.' : 'Escolha a fazenda no passo 1 para carregar o mapa.'); return; }
        map.started = true; setFull(true);
    });
    $('[data-stw-full-add]')?.addEventListener('click', () => editor()?.addPointAtCenter?.());
    $('[data-stw-full-undo]')?.addEventListener('click', () => editor()?.undo?.());
    $('[data-stw-full-done]')?.addEventListener('click', () => setFull(false));

    // confirmação quando as áreas não batem (>5%)
    const areaDialog = document.querySelector('[data-stw-area-dialog]');
    const divergence = () => (map.vertices >= 3 && declared() > 0 ? Math.abs(map.area - declared()) / declared() * 100 : 0);
    const askArea = () => {
        const d = divergence();
        areaDialog.querySelector('[data-stw-div-text]').innerHTML = `O desenho tem <strong>${n2(map.area)} ha</strong> e você informou <strong>${n2(declared())} ha</strong> (${n1(d)}% de diferença). Qual área vale para custos e produtividade?`;
        areaDialog.querySelector('[data-stw-use-map]').textContent = `Usar a área do mapa (${n2(map.area)} ha)`;
        areaDialog.querySelector('[data-stw-keep-declared]').textContent = `Manter a informada (${n2(declared())} ha)`;
        areaDialog.showModal();
    };
    areaDialog.querySelector('[data-stw-use-map]').addEventListener('click', () => {
        map.areaChoice = 'map';
        field('area_hectares').value = n2(map.area).replace(/\./g, '');
        areaDialog.close(); goTo(2);
    });
    areaDialog.querySelector('[data-stw-keep-declared]').addEventListener('click', () => { map.areaChoice = 'declared'; areaDialog.close(); goTo(2); });
    areaDialog.querySelector('[data-stw-adjust]').addEventListener('click', () => areaDialog.close());

    // ---------- plantio ----------
    const newVarInput = $('[data-stw-new-variety-input]');
    $('[data-stw-new-variety]').addEventListener('click', () => {
        newVarInput.hidden = !newVarInput.hidden;
        if (!newVarInput.hidden) { $$('input[name="variety_id"]').forEach((r) => { r.checked = false; }); newVarInput.focus(); } else newVarInput.value = '';
        refresh();
    });
    $$('input[name="variety_id"]').forEach((r) => r.addEventListener('change', () => { newVarInput.hidden = true; newVarInput.value = ''; }));
    const planting = () => {
        const month = $('[data-stw-month]').value;
        const [y, m] = month ? month.split('-').map(Number) : [0, 1];
        const row = num($('[data-stw-row]').value);
        const plant = num($('[data-stw-plant]').value);
        const area = effectiveArea();
        const estimate = row && plant && area ? area * 10000 / (row * plant) : 0;
        const counted = Math.round(num($('[data-stw-counted]').value));
        const age = y ? (today.getFullYear() + today.getMonth() / 12) - (y + (m - 1) / 12) : 0;
        const forming = y && age < 2.5;
        const yieldHa = num($('[data-stw-yield]').value);
        const varietyEl = form.querySelector('input[name="variety_id"]:checked');
        const variety = newVarInput.value.trim() || (varietyEl ? varietyEl.nextElementSibling.textContent : '');
        return { y, m, row, plant, estimate, counted, plants: counted || estimate, age, forming, yieldHa, variety, density: row && plant ? 10000 / (row * plant) : 0 };
    };
    const stageOf = (p) => (later.plant || !p.y ? ['Sem plantio informado', 'is-muted'] : p.forming ? ['Em formação', 'is-info'] : ['Em produção', 'is-ok']);

    // ---------- manejo ----------
    const irrigation = () => (later.man ? 'none' : val('irrigation_type') || 'none');
    const laudoInput = $('[data-stw-laudo-input]');
    laudoInput.addEventListener('change', () => {
        const file = laudoInput.files?.[0];
        $('[data-stw-laudo-file]').hidden = !file;
        $('[data-stw-laudo-pick]').hidden = Boolean(file);
        $('[data-stw-laudo-name]').textContent = file ? file.name : '';
        $('[data-stw-laudo-hint]').hidden = Boolean(file);
        refresh();
    });
    $('[data-stw-laudo-clear]').addEventListener('click', () => { laudoInput.value = ''; laudoInput.dispatchEvent(new Event('change')); });

    // ---------- miniatura do mapa ----------
    const miniMap = () => {
        const farmPts = window.extractFarmBoundaryPoints?.(farmBounds[farmId()]) || [];
        const pts = geometryPoints();
        const all = [...farmPts, ...pts];
        if (!all.length) return '<div class="stw-mini-empty">Sem desenho ainda</div>';
        const b = box(all);
        const w = 300, h = 190, padX = (b[2] - b[0]) * 0.08 || 0.001, padY = (b[3] - b[1]) * 0.08 || 0.001;
        const sx = w / ((b[2] - b[0]) + padX * 2), sy = h / ((b[3] - b[1]) + padY * 2), sc = Math.min(sx, sy);
        const px = (p) => `${((p.lng - b[0] + padX) * sc).toFixed(1)},${(h - (p.lat - b[1] + padY) * sc).toFixed(1)}`;
        const fill = currentColor();
        return `<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="Miniatura do setor no mapa da fazenda">
            <rect width="${w}" height="${h}" fill="#4B5532"/>
            ${farmPts.length >= 3 ? `<polygon points="${farmPts.map(px).join(' ')}" fill="#5BB34A" fill-opacity="0.2" stroke="#418436" stroke-width="2.5"/>` : ''}
            ${pts.length >= 3 ? `<polygon points="${pts.map(px).join(' ')}" fill="${fill}" fill-opacity="0.45" stroke="${strokeOf(fill)}" stroke-width="3"/>` : ''}
        </svg>`;
    };

    // ---------- atualização geral ----------
    const TIPS = [
        'Nomes curtos e conhecidos facilitam quando você fala com o Pezinho: “aplicar adubo na Represa”.',
        'Tem o arquivo KML do CAR ou do agrônomo? Importar é mais rápido e preciso que desenhar.',
        'Para café, plantas a cada 0,5 a 0,8 m e linhas de 3,5 a 4 m são comuns. Confira com o que está no campo.',
        'Anexe a análise de solo: ela fica nos arquivos do setor e ajuda na recomendação de adubação.',
        'Depois de criar, o setor já aparece para escolher em aplicações, colheitas e despesas.',
    ];
    const problem = (target) => {
        if (target === 0) {
            if (!val('name')) return 'Dê um nome ao setor';
            if (!(declared() > 0)) return 'Informe a área em hectares';
            if (!farmId()) return 'Escolha a fazenda';
        }
        return '';
    };
    function refresh() {
        const p = planting();
        const area = effectiveArea();
        // hidden fields enviados ao servidor
        $('[data-stw-planting-date]').value = p.y ? `${p.y}-${String(p.m).padStart(2, '0')}-01` : '';
        $('[data-stw-plant-cm]').value = p.plant ? String(Math.round(p.plant * 10000) / 100) : '';
        $('[data-stw-plant-count]').value = p.plants ? String(Math.round(p.plants)) : '';
        $('[data-stw-yield-total]').value = p.yieldHa && area ? String(Math.round(p.yieldHa * area * 100) / 100) : '';
        const dripM = num($('[data-stw-drip-m]').value);
        $('[data-stw-drip-cm]').value = dripM ? String(Math.round(dripM * 10000) / 100) : '';
        // plantio
        $('[data-stw-plants-est]').textContent = p.estimate ? int(p.estimate) : '—';
        $('[data-stw-density]').textContent = p.density ? `${int(p.density)} plantas/ha` : 'Informe o espaçamento';
        $('[data-stw-age]').textContent = p.y ? `${n1(Math.max(0, p.age))} anos` : '—';
        $('[data-stw-first-harvest]').textContent = p.y ? (p.forming ? `Primeira colheita prevista em ${Math.floor(p.y + 2.5 + (p.m - 1) / 12)}` : 'Já em idade de produção') : '';
        const [stageTxt, stageCls] = stageOf(p);
        $('[data-stw-stage]').textContent = stageTxt; $('[data-stw-stage]').className = `szw-chip ${stageCls}`;
        // irrigação
        const irr = irrigation();
        $$('[data-stw-irrig-box]').forEach((boxEl) => { boxEl.hidden = boxEl.dataset.stwIrrigBox !== irr; });
        const lines = num($('[data-stw-lines]').value), len = num($('[data-stw-line-len]').value), lh = num($('[data-stw-drip-lh]').value);
        const drippers = dripM ? lines * len / dripM : 0;
        const flow = drippers * lh;
        $('[data-stw-drippers]').textContent = drippers ? int(drippers) : '—';
        $('[data-stw-flow]').textContent = flow ? `${int(flow)} L/h` : '—';
        $('[data-stw-lamina]').textContent = flow && area ? `${n2(flow / (area * 10000))} mm/h` : '—';
        const spr = num($('[data-stw-spr-count]').value) * num($('[data-stw-spr-lh]').value);
        $('[data-stw-spr-lamina]').textContent = spr && area ? `${n2(spr / (area * 10000))} mm/h` : '—';
        // mapa: selo e áreas
        const chip = $('[data-stw-poly-chip]');
        const located = $('[data-stw-lat]').value && $('[data-stw-lng]').value;
        if (map.vertices >= 3) { chip.textContent = 'Perímetro pronto'; chip.className = 'szw-chip is-ok'; }
        else if (map.pointOnly && located) { chip.textContent = 'Só localização'; chip.className = 'szw-chip is-info'; }
        else { chip.textContent = 'Sem perímetro'; chip.className = 'szw-chip is-warn'; }
        const d = divergence();
        $('[data-stw-calc-map]').textContent = map.vertices >= 3 ? `${n2(map.area)} ha` : '—';
        $('[data-stw-calc-declared]').textContent = declared() ? `${n2(declared())} ha` : '—';
        $('[data-stw-calc-div]').textContent = map.vertices >= 3 && declared() ? `${n1(d)}%` : '—';
        $('[data-stw-calc-div-hint]').textContent = map.vertices < 3 ? 'Desenhe para comparar' : d > 5 ? 'Acima de 5%: vamos pedir para confirmar' : 'Dentro da margem de 5%';
        $('[data-stw-calc-div-box]').className = `stw-calc${map.vertices >= 3 ? (d > 5 ? ' is-warn' : ' is-ok') : ''}`;
        $('[data-stw-limit-text]').textContent = farmBounds[farmId()] ? `Limite: perímetro da ${farmName() || 'fazenda'}. Se a fazenda não tiver perímetro cadastrado, o desenho fica livre.` : 'Esta fazenda ainda não tem perímetro cadastrado: o desenho fica livre.';
        $('[data-stw-rain-farm]').textContent = farmName() || 'fazenda';
        const farmLabel = dialog?.querySelector('[data-stw-farm-label]');
        if (farmLabel) farmLabel.textContent = farmName() || 'Escolha a fazenda';
        // resumo à direita
        const color = currentColor();
        $('[data-stw-mini]').innerHTML = miniMap();
        $('[data-stw-aside-sw]').style.background = color;
        $('[data-stw-aside-sw]').style.borderColor = strokeOf(color);
        $('[data-stw-aside-name]').textContent = val('name') || 'Sem nome';
        const asideStage = $('[data-stw-aside-stage]');
        asideStage.textContent = stageTxt; asideStage.className = `szw-chip ${stageCls}`;
        const row = (k, v) => `<div><dt>${k}</dt><dd>${esc(v)}</dd></div>`;
        $('[data-stw-summary]').innerHTML = [
            row('Área', area ? `${n2(area)} ha` : '—'),
            row('Variedade', later.plant ? '—' : (p.variety || '—')),
            row('Plantio', later.plant || !p.y ? '—' : `${String(p.m).padStart(2, '0')}/${p.y}`),
            row('Plantas', later.plant || !p.plants ? '—' : int(p.plants)),
            row('Espaçamento', later.plant || !(p.row && p.plant) ? '—' : `${n2(p.row)} × ${n2(p.plant)} m`),
            row('Irrigação', later.man ? 'Não informado' : IRR[irr]),
            row('Meta', later.plant || !p.yieldHa ? '—' : `${n1(p.yieldHa)} sc/ha`),
        ].join('');
        $('[data-stw-tip]').textContent = TIPS[step];
        const seasonDesc = $('[data-stw-season-desc]');
        if (seasonDesc) seasonDesc.textContent = !later.plant && p.yieldHa ? `Entra com a meta de ${n1(p.yieldHa)} sc/ha. Sem isso, o setor fica fora dos custos e da produção desta safra.` : 'Sem meta de produção, entra só nos custos. A meta fica para quando completar o plantio.';
        if (step === STEPS - 1) renderReview(p, area, irr);
        const why = problem(step);
        $('[data-stw-steptxt]').textContent = why || `Passo ${step + 1} de ${STEPS}`;
        $('[data-stw-next]').disabled = Boolean(why);
        scheduleDraft();
    }
    const renderReview = (p, area, irr) => {
        const soilEl = form.querySelector('input[name="soil_texture"]:checked');
        const laudo = laudoInput.files?.[0];
        const located = $('[data-stw-lat]').value;
        const cards = [
            ['Identificação', val('name') || 'Sem nome', `${n2(area)} ha · ${farmName()}${val('group_name') ? ` · ${val('group_name')}` : ''}`, 0],
            ['Mapa', map.vertices >= 3 ? 'Contorno desenhado' : map.pointOnly && located ? 'Só a localização' : editing ? 'Contorno atual mantido' : 'Sem mapa',
                map.vertices >= 3 ? `${n2(map.area)} ha pelo desenho · diferença ${n1(divergence())}%` : 'Dá para desenhar depois', 1],
            ['Plantio', later.plant ? 'Para completar depois' : (p.variety || 'Variedade não informada'),
                later.plant ? 'Fica fora das metas de sacas' : `${p.y ? `${String(p.m).padStart(2, '0')}/${p.y}` : 'Mês não informado'}${p.row && p.plant ? ` · ${n2(p.row)} × ${n2(p.plant)} m` : ''}${p.plants ? ` · ${int(p.plants)} plantas` : ''}`, 2],
            ['Manejo', later.man ? 'Para completar depois' : IRR[irr],
                later.man ? 'Recomendação de adubação fica pendente' : `${soilEl ? SOIL[soilEl.value] : 'Solo não informado'} · ${laudo ? 'análise anexada' : 'sem análise de solo'}`, 3],
        ];
        $('[data-stw-review]').innerHTML = cards.map(([t, v, d, s]) => `
            <div class="szw-review-card"><div class="szw-review-top"><span class="szw-overline">${t}</span><button type="button" class="szw-link" data-stw-go="${s}">Alterar</button></div>
            <strong>${esc(v)}</strong><small>${esc(d)}</small></div>`).join('');
        const season = $('[data-stw-season]');
        const impact = [
            `${editing ? 'Salvar' : 'Criar'} o setor ${esc(val('name'))} com ${n2(area)} ha${map.vertices >= 3 ? ' e o contorno no mapa' : ''}.`,
            later.plant ? 'Deixar o plantio para depois: o setor fica fora das metas de produção até ser completado.'
                : `Registrar o plantio${p.y ? ` de ${String(p.m).padStart(2, '0')}/${p.y}` : ''}: ${esc(p.variety || 'variedade não informada')}${p.plants ? `, cerca de ${int(p.plants)} plantas` : ''}.`,
        ];
        if (newVarInput.value.trim()) impact.push(`Cadastrar a variedade ${esc(newVarInput.value.trim())}.`);
        if (season && season.checked && !season.disabled) impact.push(`Incluir na ${esc(data.season || 'safra ativa')}${!later.plant && p.yieldHa ? ` com meta de ${n1(p.yieldHa)} sc/ha` : ''}.`);
        impact.push(later.man ? 'Deixar irrigação e solo para depois. A recomendação de adubação fica pendente.'
            : laudo ? 'Guardar o laudo da análise de solo nos arquivos do setor.' : 'Deixar a recomendação de adubação pendente até anexar a análise de solo.');
        impact.push(`Usar ${modeInput.value === 'manual' ? 'a cor escolhida' : 'uma cor automática, diferente dos vizinhos,'} no mapa da fazenda.`);
        $('[data-stw-impact]').innerHTML = impact.map((t) => `<li>${t}</li>`).join('');
    };

    // ---------- navegação ----------
    const stepsEl = $('[data-stw-steps]');
    const attachments = dialog?.querySelector('[data-stw-attachments]');
    const show = () => {
        $$('[data-stw-panel]').forEach((panel) => { panel.hidden = Number(panel.dataset.stwPanel) !== step; });
        $$('.szw-step').forEach((li, i) => {
            li.classList.toggle('is-current', i === step);
            li.classList.toggle('is-done', i < step);
            const btn = li.querySelector('button');
            if (i === step) btn.setAttribute('aria-current', 'step'); else btn.removeAttribute('aria-current');
        });
        const current = stepsEl?.querySelector('.is-current');
        if (current && stepsEl.scrollWidth > stepsEl.clientWidth) {
            const left = Math.max(0, current.offsetLeft - (stepsEl.clientWidth - current.offsetWidth) / 2);
            const calm = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
            stepsEl.scrollTo({ left, behavior: calm ? 'auto' : 'smooth' });
        }
        const last = step === STEPS - 1;
        $('[data-stw-back]').disabled = step === 0;
        $('[data-stw-next]').hidden = last;
        $('[data-stw-submit]').hidden = !last;
        if (attachments) attachments.hidden = step !== 3;
        if (step === 1) window.setTimeout(() => { editor()?.invalidate?.(); if (!editor()?.pointCount?.()) editor()?.fit?.(); syncPrompt(); }, 80);
        refresh();
    };
    function goTo(target) {
        for (let i = 0; i < target; i += 1) {
            if (problem(i)) { step = i; show(); return; }
        }
        if (step === 1 && target > 1 && divergence() > 5 && !map.areaChoice) { askArea(); return; }
        step = target;
        show();
        dialog?.querySelector('[data-modal-scroll]')?.scrollTo?.({ top: 0 });
    }
    form.addEventListener('click', (event) => {
        const go = event.target.closest('[data-stw-go]');
        if (go) goTo(Number(go.dataset.stwGo));
    });
    $('[data-stw-back]').addEventListener('click', () => goTo(Math.max(0, step - 1)));
    $('[data-stw-next]').addEventListener('click', () => { if (!problem(step)) goTo(Math.min(STEPS - 1, step + 1)); });
    form.addEventListener('input', (event) => {
        if (event.target.matches('[data-stw-area]')) map.areaChoice = null;
        refresh();
    });
    form.addEventListener('change', () => refresh());
    form.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' && event.target.tagName === 'INPUT' && step < STEPS - 1) event.preventDefault();
    });
    form.addEventListener('submit', (event) => {
        if (step !== STEPS - 1) { event.preventDefault(); event.stopImmediatePropagation(); goTo(Math.min(STEPS - 1, step + 1)); return; }
        if (problem(0)) { event.preventDefault(); event.stopImmediatePropagation(); goTo(0); return; }
        refresh();
        clearDraft();
    }, true);

    // ---------- rascunho no aparelho ----------
    const DRAFT_KEY = `sisfarm.plotDraft.${editId || 'novo'}`;
    const draftStatus = dialog?.querySelector('[data-stw-draft-status]');
    const hhmm = (d) => d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
    let touched = false;
    let draftTimer = null;
    const collect = () => {
        const values = {};
        Array.from(form.elements).forEach((el) => {
            if (!el.name || el.type === 'file' || el.name === 'csrf_token' || el.disabled) return;
            if ((el.type === 'radio' || el.type === 'checkbox') && !el.checked) return;
            values[el.name] = el.value;
        });
        $$('[data-stw-month], [data-stw-plant], [data-stw-counted], [data-stw-yield], [data-stw-drip-m]').forEach((el, i) => { values[`__ui${i}`] = el.value; });
        return { values, later, step, savedAt: new Date().toISOString() };
    };
    const saveDraft = (manual) => {
        if (!manual && !touched) return;
        try {
            localStorage.setItem(DRAFT_KEY, JSON.stringify(collect()));
            if (draftStatus) draftStatus.textContent = `${manual ? 'rascunho salvo' : 'rascunho salvo automaticamente'} às ${hhmm(new Date())}`;
            if (manual) window.showToast?.('Rascunho salvo neste aparelho.');
        } catch (e) { if (manual) window.alert('Não foi possível salvar o rascunho neste aparelho.'); }
    };
    function scheduleDraft() { window.clearTimeout(draftTimer); draftTimer = window.setTimeout(() => saveDraft(false), 800); }
    function clearDraft() { try { localStorage.removeItem(DRAFT_KEY); } catch (e) { /* ok */ } }
    dialog?.querySelector('[data-stw-save-draft]')?.addEventListener('click', () => saveDraft(true));
    ['input', 'change'].forEach((evt) => form.addEventListener(evt, () => { touched = true; }, true));
    const restoreDraft = () => {
        let draft = null;
        try { draft = JSON.parse(localStorage.getItem(DRAFT_KEY) || 'null'); } catch (e) { draft = null; }
        if (!draft || !draft.values) return;
        Object.entries(draft.values).forEach(([name, value]) => {
            if (name.startsWith('__ui')) return;
            const el = field(name);
            if (!el) return;
            if (el instanceof RadioNodeList) {
                Array.from(el).forEach((item) => { if (item.type === 'radio' || item.type === 'checkbox') item.checked = item.value === value; else if (!item.disabled && item.type !== 'hidden') item.value = value; });
            } else if (el.type === 'checkbox') el.checked = true;
            else el.value = value;
        });
        $$('[data-stw-month], [data-stw-plant], [data-stw-counted], [data-stw-yield], [data-stw-drip-m]').forEach((el, i) => { if (draft.values[`__ui${i}`] !== undefined) el.value = draft.values[`__ui${i}`]; });
        Object.assign(later, draft.later || {});
        applySkip('plant'); applySkip('man');
        if (draftStatus && draft.savedAt) {
            const at = new Date(draft.savedAt);
            draftStatus.innerHTML = `rascunho recuperado de ${at.toLocaleDateString('pt-BR')} às ${hhmm(at)} · <button type="button" class="szw-link szw-link-inline" data-stw-discard>Descartar</button>`;
            draftStatus.querySelector('[data-stw-discard]')?.addEventListener('click', () => { clearDraft(); window.location.href = editId ? `/setores?edit_id=${editId}` : '/setores?novo=1'; });
        }
    };
    if (!new URLSearchParams(window.location.search).get('voltar')) restoreDraft();

    renderColor();
    show();
})();
