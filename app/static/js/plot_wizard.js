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
    const intNum = (value) => {
        const digits = String(value ?? '').replace(/\D/g, '');
        if (!digits) return 0;
        const n = parseInt(digits, 10);
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
    const farmMaxArea = () => Number((data.farm_areas || {})[farmId()] || 0);
    const declared = () => num(val('area_hectares'));
    const areaExceedsFarm = () => {
        const max = farmMaxArea();
        return max > 0 && declared() > max + 1e-6;
    };
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
    const latInput = $('[data-stw-lat]');
    const lngInput = $('[data-stw-lng]');
    const mapSubtitle = $('[data-stw-map-subtitle]');
    const mapbarDraw = $('[data-stw-mapbar-draw]');
    const mapbarPin = $('[data-stw-mapbar-pin]');
    const pinStatusEl = $('[data-stw-pin-status]');
    const pinPillEl = $('[data-stw-pin-pill]');
    const toolsDraw = $('[data-stw-tools-draw]');
    const toolsPin = $('[data-stw-tools-pin]');
    const pinHintEl = $('[data-stw-pin-hint]');
    const pinClearBtn = $('[data-stw-pin-clear]');
    const hasPinCoords = () => Boolean(latInput?.value && lngInput?.value && Number.isFinite(parseFloat(latInput.value)) && Number.isFinite(parseFloat(lngInput.value)));

    let mapExpandTimer1 = null;
    let mapExpandTimer2 = null;
    const syncMapEditingHeight = () => {
        const isEditingMap = Boolean(map.started || map.pointOnly || map.vertices > 0 || editing);
        const changed = form.classList.contains('is-map-editing') !== isEditingMap;
        form.classList.toggle('is-map-editing', isEditingMap);
        if (changed) {
            window.clearTimeout(mapExpandTimer1);
            window.clearTimeout(mapExpandTimer2);
            mapExpandTimer1 = window.setTimeout(() => editor()?.invalidate?.(), 160);
            mapExpandTimer2 = window.setTimeout(() => editor()?.invalidate?.(), 350);
        }
    };
    const syncPrompt = () => {
        if (prompt) prompt.hidden = !editor() || map.started || map.vertices > 0 || map.pointOnly || editing;
        syncMapEditingHeight();
    };
    $('#plots-form-boundary-map')?.addEventListener('pointerdown', () => {
        if (!map.started) {
            map.started = true;
            syncPrompt();
        }
    });
    const syncMapToolUi = () => {
        const isPin = map.pointOnly;
        const hasPin = hasPinCoords();
        $$('[data-stw-map-tool]').forEach((btn) => {
            btn.setAttribute('aria-checked', String((btn.dataset.stwMapTool === 'pin') === isPin));
        });
        if (mapSubtitle) {
            mapSubtitle.textContent = isPin
                ? 'Clique no mapa para posicionar o marcador do setor. Para alterar, clique em outro ponto.'
                : 'Toque nos cantos do setor por dentro do contorno da fazenda, ou importe o arquivo. A área é calculada pelo desenho.';
        }
        if (mapbarDraw) mapbarDraw.hidden = isPin;
        if (mapbarPin) mapbarPin.hidden = !isPin;
        if (toolsDraw) toolsDraw.hidden = isPin;
        if (toolsPin) toolsPin.hidden = !isPin;
        if (pinPillEl) pinPillEl.hidden = !isPin || hasPin || !editor();
        if (pinClearBtn) pinClearBtn.disabled = !hasPin;
        if (pinStatusEl) {
            if (hasPin) {
                const latStr = parseFloat(latInput.value).toFixed(4).replace('.', ',');
                const lngStr = parseFloat(lngInput.value).toFixed(4).replace('.', ',');
                pinStatusEl.innerHTML = `<strong style="color:#16201a">Ponto salvo</strong> · ${latStr}, ${lngStr}`;
            } else {
                pinStatusEl.textContent = 'Clique no mapa para marcar a localização do setor';
            }
        }
        if (pinHintEl) {
            pinHintEl.textContent = hasPin
                ? 'Clique em qualquer outro ponto do mapa para alterar a localização.'
                : 'Nenhum ponto marcado ainda. Clique no mapa acima.';
        }
        syncPrompt();
    };
    const applyEditorMapTool = () => {
        const ed = editor();
        if (!ed) return;
        if (hasPinCoords()) {
            ed.setPin?.(parseFloat(latInput.value), parseFloat(lngInput.value), true);
        } else {
            ed.setPin?.(NaN, NaN, true);
        }
        ed.setInteractionMode?.(map.pointOnly ? 'pin' : 'draw');
    };
    const setMapTool = (tool) => {
        map.pointOnly = tool === 'pin';
        applyEditorMapTool();
        syncMapToolUi();
        refresh();
    };
    $$('[data-stw-map-tool]').forEach((btn) => {
        btn.addEventListener('click', () => setMapTool(btn.dataset.stwMapTool));
    });
    pinClearBtn?.addEventListener('click', () => {
        if (latInput) latInput.value = '';
        if (lngInput) lngInput.value = '';
        editor()?.clearPin?.();
        syncMapToolUi();
        refresh();
    });
    window.addEventListener('plot-pin-changed', (event) => {
        const pt = event.detail;
        if (pt && Number.isFinite(pt.lat) && Number.isFinite(pt.lng)) {
            if (latInput) latInput.value = pt.lat.toFixed(6);
            if (lngInput) lngInput.value = pt.lng.toFixed(6);
        } else {
            if (latInput) latInput.value = '';
            if (lngInput) lngInput.value = '';
        }
        syncMapToolUi();
        refresh();
    });
    window.addEventListener('plot-boundary-stats', (event) => {
        map.vertices = event.detail?.vertices || 0;
        map.area = event.detail?.area || 0;
        if (map.vertices > 0) map.started = true;
        $('[data-stw-vertices]').textContent = String(map.vertices);
        $('[data-stw-map-area]').textContent = `${n2(map.area)} ha`;
        const counter = $('[data-stw-full-count]');
        if (counter) counter.textContent = `${map.vertices} ${map.vertices === 1 ? 'ponto' : 'pontos'}`;
        syncMapToolUi();
        renderColor();
        refresh();
    });
    window.addEventListener('plot-boundary-ready', () => {
        applyEditorMapTool();
        syncMapToolUi();
        renderColor();
        refresh();
    });
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
        field('area_hectares').value = formatDecimalInitial(map.area);
        areaDialog.close(); goTo(2);
    });
    areaDialog.querySelector('[data-stw-keep-declared]').addEventListener('click', () => { map.areaChoice = 'declared'; areaDialog.close(); goTo(2); });
    areaDialog.querySelector('[data-stw-adjust]').addEventListener('click', () => areaDialog.close());

    // ---------- máscara decimal fixa (0,00 com milhar, da direita para a esquerda) e inteiros com milhar ----------
    const formatDecimalCents = (raw) => {
        const clean = String(raw ?? '').replace(/\D/g, '').slice(0, 10);
        if (!clean) return '';
        const cents = parseInt(clean, 10);
        if (!cents) return '';
        const str = String(cents).padStart(3, '0');
        const intPart = str.slice(0, -2).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
        const decPart = str.slice(-2);
        return `${intPart},${decPart}`;
    };
    const formatDecimalInitial = (raw) => {
        const txt = String(raw ?? '').trim();
        if (!txt) return '';
        const v = num(txt);
        if (!(v > 0)) return '';
        return formatDecimalCents(String(Math.round(v * 100)));
    };
    const formatIntThousands = (raw) => {
        const clean = String(raw ?? '').replace(/\D/g, '').slice(0, 9);
        if (!clean) return '';
        const n = parseInt(clean, 10);
        if (!n) return '';
        return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    };
    const bindDecimalMask = (input) => {
        if (!input) return;
        if (input.value) input.value = formatDecimalInitial(input.value);
        input.addEventListener('input', () => {
            const formatted = formatDecimalCents(input.value);
            if (input.value !== formatted) input.value = formatted;
            try { input.setSelectionRange(formatted.length, formatted.length); } catch (e) { /* ignore */ }
        });
        input.addEventListener('paste', (event) => {
            const pasted = (event.clipboardData || window.clipboardData)?.getData('text') || '';
            if (/[.,]/.test(pasted)) {
                event.preventDefault();
                input.value = formatDecimalInitial(pasted);
                input.dispatchEvent(new Event('input', { bubbles: true }));
            }
        });
    };
    const bindIntMask = (input) => {
        if (!input) return;
        if (input.value) input.value = formatIntThousands(input.value);
        input.addEventListener('input', () => {
            const formatted = formatIntThousands(input.value);
            if (input.value !== formatted) input.value = formatted;
            try { input.setSelectionRange(formatted.length, formatted.length); } catch (e) { /* ignore */ }
        });
    };
    $$('[data-stw-decimal]').forEach(bindDecimalMask);
    $$('[data-stw-int]').forEach(bindIntMask);

    // ---------- plantio ----------
    const newVarInput = $('[data-stw-new-variety-input]');
    const monthInput = $('[data-stw-month]');
    const monthHint = $('[data-stw-month-hint]');
    const formatPlantingMonth = (raw) => {
        const txt = String(raw ?? '').trim();
        const iso = /^(\d{4})-(\d{2})$/.exec(txt);
        if (iso) return `${iso[2]}/${iso[1]}`;
        const digits = txt.replace(/\D/g, '').slice(0, 6);
        if (digits.length <= 2) return digits;
        return `${digits.slice(0, 2)}/${digits.slice(2)}`;
    };
    const parsePlantingMonth = (raw) => {
        const txt = String(raw ?? '').trim();
        if (!txt) return { y: 0, m: 0, error: '' };
        let m = 0, y = 0;
        const br = /^(\d{2})\/(\d{4})$/.exec(txt);
        const iso = /^(\d{4})-(\d{2})$/.exec(txt);
        if (br) { m = Number(br[1]); y = Number(br[2]); }
        else if (iso) { y = Number(iso[1]); m = Number(iso[2]); }
        else return { y: 0, m: 0, error: txt.length >= 3 ? 'Digite no formato MM/AAAA (ex.: 03/2021).' : '' };
        const maxYear = today.getFullYear() + 2;
        if (m < 1 || m > 12) return { y: 0, m: 0, error: 'Mês inválido — use de 01 a 12 (ex.: 03/2021).' };
        if (y < 1900 || y > maxYear) return { y: 0, m: 0, error: `Informe um ano entre 1900 e ${maxYear}.` };
        return { y, m, error: '' };
    };
    const formatAgeLabel = (totalMonths) => {
        if (totalMonths < 0) return 'Plantio futuro';
        if (totalMonths === 0) return 'Menos de 1 mês';
        const yrs = Math.floor(totalMonths / 12);
        const mos = totalMonths % 12;
        if (yrs === 0) return `${mos} ${mos === 1 ? 'mês' : 'meses'}`;
        if (mos === 0) return `${yrs} ${yrs === 1 ? 'ano' : 'anos'}`;
        return `${yrs} ${yrs === 1 ? 'ano' : 'anos'} e ${mos} ${mos === 1 ? 'mês' : 'meses'}`;
    };
    if (monthInput) {
        if (monthInput.value) monthInput.value = formatPlantingMonth(monthInput.value);
        monthInput.addEventListener('input', () => {
            const formatted = formatPlantingMonth(monthInput.value);
            if (monthInput.value !== formatted) monthInput.value = formatted;
        });
    }
    $('[data-stw-new-variety]').addEventListener('click', () => {
        newVarInput.hidden = !newVarInput.hidden;
        if (!newVarInput.hidden) { $$('input[name="variety_id"]').forEach((r) => { r.checked = false; }); newVarInput.focus(); } else newVarInput.value = '';
        refresh();
    });
    $$('input[name="variety_id"]').forEach((r) => r.addEventListener('change', () => { newVarInput.hidden = true; newVarInput.value = ''; }));
    const planting = () => {
        const parsed = parsePlantingMonth(monthInput?.value || '');
        const y = parsed.y;
        const m = parsed.m;
        const row = num($('[data-stw-row]').value);
        const plant = num($('[data-stw-plant]').value);
        const area = effectiveArea();
        const estimate = row && plant && area ? area * 10000 / (row * plant) : 0;
        const counted = intNum($('[data-stw-counted]').value);
        const totalMonths = y ? (today.getFullYear() - y) * 12 + (today.getMonth() + 1 - m) : 0;
        const age = y ? totalMonths / 12 : 0;
        const forming = Boolean(y && totalMonths < 30);
        const yieldHa = num($('[data-stw-yield]').value);
        const varietyEl = form.querySelector('input[name="variety_id"]:checked');
        const variety = newVarInput.value.trim() || (varietyEl ? varietyEl.nextElementSibling.textContent : '');
        return { y, m, monthError: parsed.error, totalMonths, row, plant, estimate, counted, plants: counted || estimate, age, forming, yieldHa, variety, density: row && plant ? 10000 / (row * plant) : 0 };
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
        const pts = map.pointOnly ? [] : geometryPoints();
        const pinPt = map.pointOnly && hasPinCoords() ? { lat: parseFloat(latInput.value), lng: parseFloat(lngInput.value) } : null;
        const all = [...farmPts, ...pts, ...(pinPt ? [pinPt] : [])];
        if (!all.length) return '<div class="stw-mini-empty">Sem desenho ainda</div>';
        const b = box(all);
        const w = 300, h = 190, padX = (b[2] - b[0]) * 0.08 || 0.001, padY = (b[3] - b[1]) * 0.08 || 0.001;
        const sx = w / ((b[2] - b[0]) + padX * 2), sy = h / ((b[3] - b[1]) + padY * 2), sc = Math.min(sx, sy);
        const pxObj = (p) => ({ x: (p.lng - b[0] + padX) * sc, y: h - (p.lat - b[1] + padY) * sc });
        const px = (p) => { const c = pxObj(p); return `${c.x.toFixed(1)},${c.y.toFixed(1)}`; };
        const fill = currentColor();
        const pinSvg = pinPt ? (() => {
            const c = pxObj(pinPt);
            return `<g transform="translate(${c.x.toFixed(1)}, ${c.y.toFixed(1)})"><path d="M0 -24C-6.6 -24 -12 -18.6 -12 -12c0 8.2 10.4 18.2 11.4 19.2a.9.9 0 0 0 1.2 0C1.6 6.2 12 -3.8 12 -12c0-6.6-5.4-12-12-12Z" fill="#446A36" stroke="#FFFFFF" stroke-width="2.4"/><circle cx="0" cy="-12.2" r="4" fill="#FFFFFF"/></g>`;
        })() : '';
        return `<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="Miniatura do setor no mapa da fazenda">
            <rect width="${w}" height="${h}" fill="#4B5532"/>
            ${farmPts.length >= 3 ? `<polygon points="${farmPts.map(px).join(' ')}" fill="#5BB34A" fill-opacity="0.2" stroke="#418436" stroke-width="2.5"/>` : ''}
            ${pts.length >= 3 ? `<polygon points="${pts.map(px).join(' ')}" fill="${fill}" fill-opacity="0.45" stroke="${strokeOf(fill)}" stroke-width="3"/>` : ''}
            ${pinSvg}
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
            if (areaExceedsFarm()) return `Área maior que a da fazenda (${n2(farmMaxArea())} ha)`;
        }
        return '';
    };
    const areaInputEl = $('[data-stw-area]');
    const areaHintEl = $('[data-stw-area-hint]');
    const areaErrEl = $('[data-stw-area-err]');
    function refresh() {
        const p = planting();
        const area = effectiveArea();
        const tooBig = areaExceedsFarm();
        if (areaInputEl) areaInputEl.setAttribute('aria-invalid', tooBig ? 'true' : 'false');
        if (areaErrEl) {
            areaErrEl.textContent = tooBig ? `A área do setor não pode ser maior que a área da fazenda ${farmName() || 'selecionada'} (${n2(farmMaxArea())} ha).` : '';
            areaErrEl.hidden = !tooBig;
        }
        if (areaHintEl) areaHintEl.hidden = tooBig;
        // hidden fields enviados ao servidor
        $('[data-stw-planting-date]').value = p.y ? `${p.y}-${String(p.m).padStart(2, '0')}-01` : '';
        $('[data-stw-plant-cm]').value = p.plant ? String(Math.round(p.plant * 10000) / 100) : '';
        $('[data-stw-plant-count]').value = p.plants ? String(Math.round(p.plants)) : '';
        $('[data-stw-yield-total]').value = p.yieldHa && area ? String(Math.round(p.yieldHa * area * 100) / 100) : '';
        const dripM = num($('[data-stw-drip-m]').value);
        $('[data-stw-drip-cm]').value = dripM ? String(Math.round(dripM * 10000) / 100) : '';
        // plantio
        if (monthHint) {
            monthHint.textContent = p.monthError || '';
            monthHint.hidden = !p.monthError;
            monthHint.classList.toggle('is-err', Boolean(p.monthError));
        }
        $('[data-stw-plants-est]').textContent = p.estimate ? int(p.estimate) : '—';
        $('[data-stw-density]').textContent = p.density ? `${int(p.density)} plantas/ha` : 'Informe o espaçamento';
        $('[data-stw-age]').textContent = p.y ? formatAgeLabel(p.totalMonths) : '—';
        $('[data-stw-first-harvest]').textContent = p.y
            ? (p.totalMonths < 0
                ? `Previsto para ${String(p.m).padStart(2, '0')}/${p.y}`
                : p.forming
                    ? `1ª colheita prevista em ${Math.floor(p.y + (p.m - 1 + 30) / 12)}`
                    : 'Já em idade de produção (≥ 2,5 anos)')
            : 'Calculada pelo mês do plantio';
        const [stageTxt, stageCls] = stageOf(p);
        $('[data-stw-stage]').textContent = stageTxt; $('[data-stw-stage]').className = `szw-chip ${stageCls}`;
        // irrigação
        const irr = irrigation();
        $$('[data-stw-irrig-box]').forEach((boxEl) => { boxEl.hidden = boxEl.dataset.stwIrrigBox !== irr; });
        const lines = intNum($('[data-stw-lines]').value), len = num($('[data-stw-line-len]').value), lh = num($('[data-stw-drip-lh]').value);
        const drippers = dripM ? lines * len / dripM : 0;
        const flow = drippers * lh;
        $('[data-stw-drippers]').textContent = drippers ? int(drippers) : '—';
        $('[data-stw-flow]').textContent = flow ? `${int(flow)} L/h` : '—';
        $('[data-stw-lamina]').textContent = flow && area ? `${n2(flow / (area * 10000))} mm/h` : '—';
        const spr = intNum($('[data-stw-spr-count]').value) * num($('[data-stw-spr-lh]').value);
        $('[data-stw-spr-lamina]').textContent = spr && area ? `${n2(spr / (area * 10000))} mm/h` : '—';
        // mapa: selo e áreas
        const chip = $('[data-stw-poly-chip]');
        const located = hasPinCoords();
        if (map.pointOnly) {
            if (located) { chip.textContent = 'Localização marcada'; chip.className = 'szw-chip is-ok'; }
            else { chip.textContent = 'Sem localização'; chip.className = 'szw-chip is-warn'; }
        } else if (map.vertices >= 3) {
            chip.textContent = 'Perímetro pronto'; chip.className = 'szw-chip is-ok';
        } else if (located) {
            chip.textContent = 'Só localização'; chip.className = 'szw-chip is-info';
        } else {
            chip.textContent = 'Sem perímetro'; chip.className = 'szw-chip is-warn';
        }
        const d = divergence();
        const showDrawStats = !map.pointOnly && map.vertices >= 3;
        $('[data-stw-calc-map]').textContent = showDrawStats ? `${n2(map.area)} ha` : '—';
        $('[data-stw-calc-declared]').textContent = declared() ? `${n2(declared())} ha` : '—';
        $('[data-stw-calc-div]').textContent = showDrawStats && declared() ? `${n1(d)}%` : '—';
        $('[data-stw-calc-div-hint]').textContent = map.pointOnly
            ? (located ? 'Usando apenas localização (sem contorno)' : 'Desenhe a área para comparar')
            : map.vertices < 3 ? 'Desenhe para comparar' : d > 5 ? 'Acima de 5%: vamos pedir para confirmar' : 'Dentro da margem de 5%';
        $('[data-stw-calc-div-box]').className = `stw-calc${showDrawStats ? (d > 5 ? ' is-warn' : ' is-ok') : ''}`;
        $('[data-stw-limit-text]').textContent = farmBounds[farmId()] ? `Limite: perímetro da ${farmName() || 'fazenda'}. Se a fazenda não tiver perímetro cadastrado, o mapa fica livre.` : 'Esta fazenda ainda não tem perímetro cadastrado: o mapa fica livre.';
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
        $$('[data-stw-month], [data-stw-plant], [data-stw-counted], [data-stw-yield], [data-stw-drip-m]').forEach((el, i) => {
            if (draft.values[`__ui${i}`] !== undefined) {
                el.value = el.matches('[data-stw-month]') ? formatPlantingMonth(draft.values[`__ui${i}`]) : draft.values[`__ui${i}`];
            }
        });
        $$('[data-stw-decimal]').forEach((el) => {
            if (el.value) el.value = formatDecimalInitial(el.value);
        });
        $$('[data-stw-int]').forEach((el) => {
            if (el.value) el.value = formatIntThousands(el.value);
        });
        Object.assign(later, draft.later || {});
        applySkip('plant'); applySkip('man');
        if (draftStatus && draft.savedAt) {
            const at = new Date(draft.savedAt);
            draftStatus.innerHTML = `rascunho recuperado de ${at.toLocaleDateString('pt-BR')} às ${hhmm(at)} · <button type="button" class="szw-link szw-link-inline" data-stw-discard>Descartar</button>`;
            draftStatus.querySelector('[data-stw-discard]')?.addEventListener('click', () => { clearDraft(); window.location.href = editId ? `/setores?edit_id=${editId}` : '/setores?novo=1'; });
        }
    };
    if (!new URLSearchParams(window.location.search).get('voltar')) restoreDraft();
    if (hasPinCoords() && geometryPoints().length < 3) map.pointOnly = true;

    // ---------- modal de anexos do setor ----------
    const attDialog = dialog?.querySelector('[data-stw-att-dialog]');
    if (attDialog) {
        const attList = attDialog.querySelector('[data-stw-att-list]');
        const attEmpty = attDialog.querySelector('[data-stw-att-empty]');
        const attSubtitle = attDialog.querySelector('[data-stw-att-subtitle]');
        const clipBtn = dialog.querySelector('.stw-clip-btn');
        const clipBadge = dialog.querySelector('[data-stw-att-count]');
        const inlineLink = form.querySelector('[data-stw-att-inline]');
        const inlineText = form.querySelector('[data-stw-att-inline-text]');

        const closeAllAttConfirms = () => {
            attDialog.querySelectorAll('[data-stw-att-item]').forEach((item) => {
                item.classList.remove('is-confirming');
                const bar = item.querySelector('[data-stw-att-confirm]');
                if (bar) bar.hidden = true;
            });
        };

        const syncAttCounters = () => {
            const count = attList ? attList.querySelectorAll('[data-stw-att-item]').length : 0;
            if (clipBadge) clipBadge.textContent = String(count);
            if (clipBtn) clipBtn.classList.toggle('has-files', count > 0);
            if (attEmpty) attEmpty.hidden = count > 0;
            if (attSubtitle) {
                const plotName = attSubtitle.dataset.plotName || 'Setor';
                const label = count === 0
                    ? 'Nenhum arquivo salvo'
                    : (count === 1 ? '1 arquivo salvo' : `${count} arquivos salvos`);
                attSubtitle.textContent = `${plotName} · ${label}`;
            }
            if (inlineLink) {
                inlineLink.hidden = count === 0;
                if (inlineText && count > 0) {
                    inlineText.textContent = count === 1 ? 'Ver 1 anexo salvo' : `Ver ${count} anexos salvos`;
                }
            }
        };

        dialog.querySelectorAll('[data-stw-att-open]').forEach((btn) => {
            btn.addEventListener('click', () => {
                closeAllAttConfirms();
                syncAttCounters();
                if (!attDialog.open) attDialog.showModal();
            });
        });

        attDialog.querySelectorAll('[data-stw-att-close]').forEach((btn) => {
            btn.addEventListener('click', () => attDialog.close());
        });

        attDialog.addEventListener('click', (event) => {
            if (event.target === attDialog) { attDialog.close(); return; }
            const askBtn = event.target.closest('[data-stw-att-ask-del]');
            if (askBtn) {
                const item = askBtn.closest('[data-stw-att-item]');
                const bar = item?.querySelector('[data-stw-att-confirm]');
                if (!item || !bar) return;
                const willOpen = bar.hidden;
                closeAllAttConfirms();
                if (willOpen) {
                    item.classList.add('is-confirming');
                    bar.hidden = false;
                }
                return;
            }
            if (event.target.closest('[data-stw-att-cancel-del]')) {
                closeAllAttConfirms();
            }
        });

        attDialog.addEventListener('submit', async (event) => {
            const delForm = event.target.closest('[data-stw-att-del-form]');
            if (!delForm) return;
            event.preventDefault();
            const item = delForm.closest('[data-stw-att-item]');
            const submitBtn = delForm.querySelector('[data-stw-att-confirm-submit]');
            const msgEl = item?.querySelector('[data-stw-att-confirm-msg]');
            if (submitBtn) {
                submitBtn.disabled = true;
                submitBtn.textContent = 'Excluindo...';
            }
            try {
                const resp = await fetch(delForm.action, {
                    method: 'POST',
                    body: new FormData(delForm),
                    credentials: 'same-origin',
                });
                if (!resp.ok) throw new Error('Falha ao excluir anexo');
                item?.remove();
                syncAttCounters();
            } catch (_err) {
                if (msgEl) msgEl.textContent = 'Não foi possível excluir agora. Tente novamente.';
                if (submitBtn) {
                    submitBtn.disabled = false;
                    submitBtn.textContent = 'Excluir';
                }
            }
        });

        syncAttCounters();
    }

    syncMapToolUi();
    renderColor();
    show();
})();
