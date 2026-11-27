/* Fazendas — lista, ficha (abas e painéis) e cadastro em 3 etapas. Prefixo fz-. */
(function () {
    'use strict';

    const $ = (sel, root) => (root || document).querySelector(sel);
    const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));
    const ESRI = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}';
    const reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    const fmtBr = (n, d = 2) => Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d });
    const parseBr = (v) => {
        const s = String(v || '').trim().replace(/\s/g, '');
        if (!s) return NaN;
        return parseFloat(s.includes(',') ? s.replace(/\./g, '').replace(',', '.') : s);
    };
    const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const norm = (s) => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
    const readJson = (id, fallback) => {
        const el = document.getElementById(id);
        if (!el) return fallback;
        try { return JSON.parse(el.textContent || 'null') ?? fallback; } catch (e) { return fallback; }
    };

    /* ---------- Aviso (toast) ---------- */
    let toastTimer = null;
    function toast(message, isError) {
        let el = $('[data-fz-toast]');
        if (!el) {
            el = document.createElement('div');
            el.setAttribute('role', 'status');
            el.setAttribute('data-fz-toast', '');
            document.body.appendChild(el);
        }
        el.className = 'fz-toast' + (isError ? ' err' : '');
        el.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
            + (isError ? '<path d="M12 9v4M12 17h.01"></path>' : '<path d="M5 12l5 5 9-11"></path>') + '</svg>';
        el.appendChild(document.createTextNode(message));
        el.hidden = false;
        clearTimeout(toastTimer);
        toastTimer = setTimeout(() => { el.hidden = true; }, 3200);
    }
    const initialToast = $('[data-fz-toast]');
    if (initialToast) toastTimer = setTimeout(() => { initialToast.hidden = true; }, 4200);

    /* ---------- Máscara decimal (100 → 1,00) ---------- */
    function bindDecimal(input) {
        input.addEventListener('input', () => {
            const digits = input.value.replace(/\D/g, '').replace(/^0+(?=\d)/, '');
            if (!digits) { input.value = ''; return; }
            input.value = fmtBr(parseInt(digits, 10) / 100, 2);
        });
    }
    $$('[data-fz-decimal]').forEach(bindDecimal);

    /* ---------- Mapas de satélite (somente leitura) ---------- */
    function toLatLngs(geom) {
        if (!geom) return [];
        if (typeof window.extractFarmBoundaryPoints === 'function') {
            return (window.extractFarmBoundaryPoints(geom) || []).map((p) => [p.lat, p.lng]);
        }
        const ring = geom.type === 'Polygon' ? geom.coordinates[0] : geom.type === 'MultiPolygon' ? geom.coordinates[0][0] : [];
        return (ring || []).map((c) => [c[1], c[0]]);
    }
    function satelliteMap(el, opts) {
        if (!el || typeof L === 'undefined') return null;
        const map = L.map(el, { zoomControl: !opts.compact, attributionControl: false, scrollWheelZoom: false, dragging: !opts.compact });
        L.tileLayer(ESRI, { maxZoom: 19 }).addTo(map);
        const bounds = [];
        const plots = (opts.plots || []).map((p) => ({ name: p.name, ll: toLatLngs(p.geometry) })).filter((p) => p.ll.length >= 3);
        const ll = toLatLngs(opts.geometry);
        plots.forEach((p) => p.ll.forEach((x) => bounds.push(x)));
        if (ll.length >= 3) ll.forEach((x) => bounds.push(x));
        /* A vista precisa existir antes de desenhar (os rótulos fixos dependem dela). */
        if (bounds.length) map.fitBounds(bounds, { padding: [24, 24] });
        else map.setView(opts.center || [-19.0, -46.5], opts.center ? 12 : 6);
        const plotLayer = L.layerGroup().addTo(map);
        plots.forEach((p) => {
            const poly = L.polygon(p.ll, { color: '#D6EDBE', weight: 1.5, fillColor: '#9BCB6B', fillOpacity: 0.28 }).addTo(plotLayer);
            if (!opts.compact && p.name) poly.bindTooltip(p.name, { permanent: true, direction: 'center', className: 'fz-maplabel' });
        });
        if (ll.length >= 3) L.polygon(ll, { color: '#9BCB6B', weight: 3, fill: false, lineJoin: 'round' }).addTo(map);
        return {
            map,
            setPlots(on) { if (on) plotLayer.addTo(map); else map.removeLayer(plotLayer); },
            refresh() { map.invalidateSize(); if (bounds.length) map.fitBounds(bounds, { padding: [24, 24] }); },
        };
    }
    async function geocode(query) {
        if (!query) return null;
        try {
            const res = await fetch('/fazendas/geocodificar?q=' + encodeURIComponent(query));
            const data = await res.json();
            return data && data.ok ? [data.lat, data.lng] : null;
        } catch (e) { return null; }
    }

    /* ---------- Menus (⋮): fecham ao clicar fora e com Esc ---------- */
    function closeMenus(except) {
        $$('[data-fz-menu]').forEach((m) => {
            if (m === except) return;
            const list = $('[data-fz-menu-list]', m);
            const btn = $('[data-fz-menu-btn]', m);
            if (list) list.hidden = true;
            if (btn) btn.setAttribute('aria-expanded', 'false');
        });
    }
    document.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-fz-menu-btn]');
        if (btn) {
            const wrap = btn.closest('[data-fz-menu]');
            const list = $('[data-fz-menu-list]', wrap);
            const open = list.hidden;
            closeMenus(wrap);
            list.hidden = !open;
            btn.setAttribute('aria-expanded', open ? 'true' : 'false');
            return;
        }
        if (!e.target.closest('[data-fz-menu-list]')) closeMenus();
    });

    /* ---------- Combo de município ---------- */
    function bindCityCombo(root, ufSelect, initialList) {
        if (!root || !ufSelect) return { valid: () => true };
        const input = $('[data-fz-city-input]', root);
        const list = $('[data-fz-city-list]', root);
        const cache = new Map();
        let cities = initialList || [];
        if (ufSelect.value && cities.length) cache.set(ufSelect.value, cities);
        let active = -1;
        const render = () => {
            const q = norm(input.value);
            const found = (q ? cities.filter((c) => norm(c).includes(q)) : cities).slice(0, 60);
            list.innerHTML = '';
            if (!ufSelect.value) { list.innerHTML = '<li class="none">Selecione o estado primeiro.</li>'; }
            else if (!found.length) { list.innerHTML = '<li class="none">Nenhum município encontrado.</li>'; }
            found.forEach((c, i) => {
                const li = document.createElement('li');
                const b = document.createElement('button');
                b.type = 'button';
                b.textContent = c;
                if (i === active) b.classList.add('is-active');
                b.addEventListener('mousedown', (ev) => { ev.preventDefault(); input.value = c; list.hidden = true; input.dispatchEvent(new Event('change', { bubbles: true })); });
                li.appendChild(b);
                list.appendChild(li);
            });
            list.hidden = false;
        };
        const load = async (uf) => {
            if (!uf) { cities = []; return; }
            if (cache.has(uf)) { cities = cache.get(uf); return; }
            try {
                const res = await fetch('/fazendas/municipios?uf=' + encodeURIComponent(uf));
                const data = await res.json();
                cities = data.municipalities || [];
                cache.set(uf, cities);
            } catch (e) { cities = []; }
        };
        ufSelect.addEventListener('change', async () => {
            input.value = '';
            input.placeholder = ufSelect.value ? 'Digite para buscar' : 'Selecione o estado primeiro';
            await load(ufSelect.value);
        });
        input.addEventListener('focus', async () => { await load(ufSelect.value); active = -1; render(); });
        input.addEventListener('input', () => { active = -1; render(); });
        input.addEventListener('blur', () => setTimeout(() => { list.hidden = true; }, 120));
        input.addEventListener('keydown', (e) => {
            const items = $$('button', list);
            if (e.key === 'ArrowDown') { e.preventDefault(); active = Math.min(items.length - 1, active + 1); render(); }
            else if (e.key === 'ArrowUp') { e.preventDefault(); active = Math.max(0, active - 1); render(); }
            else if (e.key === 'Enter' && active >= 0 && items[active]) { e.preventDefault(); input.value = items[active].textContent; list.hidden = true; }
            else if (e.key === 'Escape') { list.hidden = true; }
        });
        return {
            valid: () => !!ufSelect.value && cities.some((c) => norm(c) === norm(input.value)),
            canon: () => cities.find((c) => norm(c) === norm(input.value)) || input.value.trim(),
            async set(uf, city) { ufSelect.value = uf; await load(uf); input.value = city; },
            load,
        };
    }

    /* =================== LISTA =================== */
    const listPage = $('[data-fz-list]');
    if (listPage) {
        let filter = 'todas';
        const search = $('[data-fz-search]');
        const apply = () => {
            const q = norm(search ? search.value : '');
            let shown = 0;
            $$('[data-fz-row]').forEach((row) => {
                let ok = true;
                if (filter === 'pend' && row.dataset.pend !== '1') ok = false;
                if (filter === 'semmapa' && row.dataset.map !== '0') ok = false;
                if (q && !norm(row.dataset.name).includes(q)) ok = false;
                row.hidden = !ok;
                if (ok) shown += 1;
            });
            const empty = $('[data-fz-empty]');
            if (empty) empty.hidden = shown > 0;
        };
        $$('[data-fz-filter]').forEach((chip) => chip.addEventListener('click', () => {
            filter = chip.dataset.fzFilter;
            $$('[data-fz-filter]').forEach((c) => { const on = c === chip; c.classList.toggle('on', on); c.setAttribute('aria-pressed', on ? 'true' : 'false'); });
            apply();
        }));
        search?.addEventListener('input', apply);
        $('[data-fz-clear]')?.addEventListener('click', () => {
            if (search) search.value = '';
            $('[data-fz-filter="todas"]')?.click();
        });
        /* Miniatura enquanto a imagem de satélite ainda está sendo gerada: desenha o contorno. */
        const pinData = readJson('fz-pins-data', []);
        $$('[data-fz-thumb]').forEach((box) => {
            const p = pinData.find((x) => String(x.id) === box.dataset.fzThumb);
            const ll = toLatLngs(p && p.geometry);
            if (ll.length < 3) return;
            const lats = ll.map((x) => x[0]);
            const lngs = ll.map((x) => x[1]);
            const minLa = Math.min(...lats), maxLa = Math.max(...lats), minLn = Math.min(...lngs), maxLn = Math.max(...lngs);
            const span = Math.max(maxLa - minLa, (maxLn - minLn) * 0.66) || 1;
            const pts = ll.map(([la, ln]) => (48 + ((ln - (minLn + maxLn) / 2) / span) * 40).toFixed(1) + ',' + (32 - ((la - (minLa + maxLa) / 2) / span) * 40).toFixed(1)).join(' ');
            box.innerHTML = '<svg viewBox="0 0 96 64" width="100%" height="100%" preserveAspectRatio="xMidYMid slice" aria-hidden="true"><rect width="96" height="64" fill="#3A5232"></rect><polygon points="0,0 40,0 30,22 0,26" fill="#41593A"></polygon><polygon points="60,64 96,40 96,64" fill="#6E6B48"></polygon><polygon points="' + pts + '" fill="rgba(155,203,107,.3)" stroke="#9BCB6B" stroke-width="2.5" stroke-linejoin="round"></polygon></svg>';
        });
        let region = null;
        const buildRegion = async () => {
            if (region || typeof L === 'undefined') { region?.invalidateSize(); return; }
            const el = document.getElementById('fz-region-map');
            region = L.map(el, { attributionControl: false, scrollWheelZoom: false });
            L.tileLayer(ESRI, { maxZoom: 19 }).addTo(region);
            const pins = readJson('fz-pins-data', []);
            const bounds = [];
            for (const p of pins) {
                let at = null;
                const ll = toLatLngs(p.geometry);
                if (ll.length >= 3) {
                    L.polygon(ll, { color: '#CFE8B4', weight: 2.5, fillColor: '#9BCB6B', fillOpacity: 0.32 }).addTo(region);
                    ll.forEach((x) => bounds.push(x));
                    const top = ll.reduce((a, b) => (b[0] > a[0] ? b : a), ll[0]);
                    at = [top[0], ll.reduce((s, x) => s + x[1], 0) / ll.length];
                } else if (p.lat != null) {
                    at = [p.lat, p.lng];
                    bounds.push(at);
                }
                if (!at) continue;
                const html = '<a class="fz-pin' + (ll.length >= 3 ? '' : ' nomap') + '" href="' + esc(p.href) + '"><b>' + esc(p.name) + '<small>' + esc(p.sub) + '</small></b><i></i></a>';
                L.marker(at, { icon: L.divIcon({ className: 'fz-pin-icon', html, iconSize: [0, 0] }) }).addTo(region);
            }
            if (bounds.length) region.fitBounds(bounds, { padding: [70, 70], maxZoom: 14 });
            else region.setView([-19.0, -46.5], 6);
        };
        $$('[data-fz-view]').forEach((btn) => btn.addEventListener('click', () => {
            const view = btn.dataset.fzView;
            $$('[data-fz-view]').forEach((b) => { const on = b === btn; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); });
            $$('[data-fz-panel]').forEach((p) => { p.hidden = p.dataset.fzPanel !== view; });
            if (view === 'mapa') setTimeout(buildRegion, 0);
        }));
    }

    /* =================== FICHA =================== */
    const farmPage = $('[data-fz-farm]');
    if (farmPage) {
        const farmId = farmPage.dataset.farmId;
        const geometries = readJson('fm-boundary-geometries-data', {});
        const geometry = geometries[String(farmId)] || null;
        const plotShapes = readJson('fz-plot-shapes', []);
        const location = readJson('fz-location', { query: '' });
        const maps = {};
        let center = null;

        const ensureMap = async (name) => {
            const el = $('[data-fz-map="' + name + '"]');
            if (!el) return;
            if (maps[name]) { maps[name].refresh(); return; }
            if (!geometry && !center) center = await geocode(location.query);
            maps[name] = satelliteMap(el, { geometry, plots: plotShapes, center });
        };

        /* Abas */
        const setTab = (tab, push) => {
            $$('[data-fz-tab]').forEach((b) => { const on = b.dataset.fzTab === tab; b.classList.toggle('on', on); b.setAttribute('aria-selected', on ? 'true' : 'false'); });
            $$('[data-fz-tabpanel]').forEach((p) => { p.hidden = p.dataset.fzTabpanel !== tab; });
            if (push) {
                const url = new URL(window.location.href);
                if (tab === 'resumo') url.searchParams.delete('aba'); else url.searchParams.set('aba', tab);
                url.searchParams.delete('painel');
                history.replaceState(null, '', url.toString());
            }
            if (tab === 'resumo' || tab === 'territorio') setTimeout(() => ensureMap(tab), 0);
        };
        $$('[data-fz-tab]').forEach((b) => b.addEventListener('click', () => setTab(b.dataset.fzTab, true)));
        setTab(farmPage.dataset.activeTab || 'resumo', false);

        /* Agronomia: menu lateral */
        $$('[data-fz-agro]').forEach((b) => b.addEventListener('click', () => {
            const id = b.dataset.fzAgro;
            $$('[data-fz-agro]').forEach((x) => { const on = x === b; x.classList.toggle('on', on); x.setAttribute('aria-selected', on ? 'true' : 'false'); });
            $$('[data-fz-agbody]').forEach((p) => { p.hidden = p.dataset.fzAgbody !== id; });
        }));

        /* Camada de setores */
        $$('[data-fz-layer]').forEach((chip) => chip.addEventListener('click', () => {
            const on = !chip.classList.contains('on');
            chip.classList.toggle('on', on);
            chip.setAttribute('aria-pressed', on ? 'true' : 'false');
            maps.territorio?.setPlots(on);
        }));

        /* Copiar CAR */
        $$('[data-fz-copy]').forEach((b) => b.addEventListener('click', async () => {
            try { await navigator.clipboard.writeText(b.dataset.fzCopy); toast('Número do CAR copiado.'); }
            catch (e) { toast('Não foi possível copiar agora.', true); }
        }));

        /* Pezinho */
        $$('[data-fz-pezinho]').forEach((b) => b.addEventListener('click', () => {
            const bubble = $('[data-pzw-bubble]');
            if (bubble) bubble.click(); else toast('O Pezinho não está disponível nesta conta.', true);
        }));

        /* ---------- Painéis ---------- */
        const scrim = $('.fz-scrim');
        let openDrawer = null;
        let lastFocus = null;
        const drawerOpeners = {};
        const closeDrawer = () => {
            if (!openDrawer) return;
            openDrawer.hidden = true;
            if (scrim) scrim.hidden = true;
            document.body.style.overflow = '';
            openDrawer = null;
            lastFocus?.focus?.();
        };
        const showDrawer = (name, trigger) => {
            const dr = $('[data-fz-drawer="' + name + '"]');
            if (!dr) return;
            closeMenus();
            if (openDrawer && openDrawer !== dr) openDrawer.hidden = true;
            lastFocus = trigger || document.activeElement;
            dr.hidden = false;
            if (scrim) scrim.hidden = false;
            document.body.style.overflow = 'hidden';
            openDrawer = dr;
            if (drawerOpeners[name]) drawerOpeners[name](trigger);
            const first = $('input:not([type=hidden]):not(.fz-sr), select, textarea', dr);
            if (first && !(trigger && trigger.dataset.fzImport)) setTimeout(() => first.focus({ preventScroll: true }), reduceMotion ? 0 : 60);
        };
        document.addEventListener('click', (e) => {
            const goto = e.target.closest('[data-fz-goto]');
            if (goto) setTab(goto.dataset.fzGoto, true);
            const opener = e.target.closest('[data-fz-open]');
            if (opener) { e.preventDefault(); showDrawer(opener.dataset.fzOpen, opener); return; }
            if (goto && openDrawer) closeDrawer();
            if (e.target.closest('[data-fz-close]')) closeDrawer();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape') return;
            if (openDrawer) closeDrawer(); else closeMenus();
        });

        /* Anexo: tipo, dica e arquivo */
        const anexo = $('[data-fz-anexo]');
        if (anexo) {
            const hint = $('[data-fz-doc-hint]', anexo);
            const ref = $('[data-fz-ref]', anexo);
            const fileInput = $('[data-fz-file-input]', anexo);
            const drop = $('[data-fz-drop]', anexo);
            const dropLabel = $('[data-fz-drop-label]', anexo);
            const setType = (value) => {
                $$('[data-fz-choice]', anexo).forEach((lab) => {
                    const input = $('input', lab);
                    const on = input.value === value;
                    input.checked = on;
                    lab.classList.toggle('on', on);
                });
                const linked = ['CCIR', 'ITR', 'Matrícula'].includes(value);
                const expiring = ['Licença ambiental', 'Outorga de água', 'Contrato de arrendamento'].includes(value);
                hint.textContent = linked ? 'O arquivo fica ligado ao registro ' + value + ' em Registros do imóvel.'
                    : expiring ? 'Com a validade preenchida, o documento entra nas pendências 60 dias antes de vencer.'
                        : 'O arquivo fica guardado em Arquivos desta fazenda.';
                ref.placeholder = linked || value === 'Recibo do CAR' ? 'Ex.: exercício ' + new Date().getFullYear() : 'Ex.: número do processo';
            };
            $$('[data-fz-choice]', anexo).forEach((lab) => $('input', lab).addEventListener('change', (e) => setType(e.target.value)));
            const showFile = () => { dropLabel.textContent = fileInput.files && fileInput.files[0] ? fileInput.files[0].name : 'Arraste o arquivo aqui'; };
            fileInput.addEventListener('change', showFile);
            ['dragenter', 'dragover'].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add('is-over'); }));
            ['dragleave', 'drop'].forEach((ev) => drop.addEventListener(ev, () => drop.classList.remove('is-over')));
            drop.addEventListener('drop', (e) => { e.preventDefault(); if (e.dataTransfer.files.length) { fileInput.files = e.dataTransfer.files; showFile(); } });
            drawerOpeners.anexo = (trigger) => {
                anexo.reset();
                showFile();
                setType((trigger && trigger.dataset.fzDoc) || 'CCIR');
            };
        }

        /* Editar e excluir arquivo */
        const editFile = $('[data-fz-edit-file]');
        if (editFile) drawerOpeners.editarquivo = (t) => {
            editFile.action = '/fazendas/' + farmId + '/anexos/' + t.dataset.fzFile + '/editar';
            $('[data-fz-ef-name]', editFile).textContent = t.dataset.fzFileName;
            $('[data-fz-ef-type]', editFile).value = t.dataset.fzFileType || 'Outro';
            $('[data-fz-ef-ref]', editFile).value = t.dataset.fzFileRef || '';
            $('[data-fz-ef-valid]', editFile).value = t.dataset.fzFileValid || '';
        };
        const delFile = $('[data-fz-del-file]');
        if (delFile) drawerOpeners.excluirarquivo = (t) => {
            delFile.action = '/fazendas/' + farmId + '/anexos/' + t.dataset.fzFile + '/excluir';
            const type = t.dataset.fzFileType;
            $('[data-fz-df-name]', delFile).textContent = t.dataset.fzFileName;
            $('[data-fz-df-meta]', delFile).textContent = type + ' · ' + (t.dataset.fzFileRef || '');
            $('[data-fz-df-impact]', delFile).textContent = ['CCIR', 'ITR', 'Matrícula'].includes(type)
                ? 'O registro ' + type + ' continua com o número, mas fica sem arquivo e volta a aparecer em “Para completar o cadastro”.'
                : 'Ele sai dos arquivos desta fazenda e não poderá ser recuperado.';
        };

        /* Escolhas (pragas) */
        $$('[data-fz-drawer="historico"] [data-fz-choice] input').forEach((input) => input.addEventListener('change', () => input.closest('[data-fz-choice]').classList.toggle('on', input.checked)));

        /* Excluir fazenda */
        const delOk = $('[data-fz-del-ok]');
        if (delOk) {
            const btn = $('[data-fz-del-btn]');
            delOk.addEventListener('change', () => { btn.disabled = !delOk.checked; });
            drawerOpeners.excluir = () => { delOk.checked = false; btn.disabled = true; };
        }

        /* Identificação: município */
        const identity = $('[data-fz-drawer="identificacao"]');
        if (identity) {
            const combo = bindCityCombo($('[data-fz-city]', identity), $('[data-fz-uf]', identity), readJson('fz-municipalities', []));
            identity.addEventListener('submit', (e) => {
                if (!combo.valid()) { e.preventDefault(); toast('Escolha o município na lista.', true); return; }
                $('[data-fz-city-input]', identity).value = combo.canon();
            });
        }

        /* CAR: consultar e comparar antes de aplicar */
        const carForm = $('[data-fz-car]');
        if (carForm) {
            const current = readJson('fz-car-current', {});
            const btn = $('[data-fz-car-check]', carForm);
            const msg = $('[data-fz-car-msg]', carForm);
            const apply = $('[data-fz-car-apply]', carForm);
            const reset = () => {
                ['[data-fz-car-result]', '[data-fz-car-perim]', '[data-fz-car-areas]'].forEach((s) => { $(s, carForm).hidden = true; });
                $('[data-fz-car-hint]', carForm).hidden = false;
                apply.disabled = true;
                msg.className = 'fz-help';
                msg.textContent = 'Consulta a base pública do CAR. Nada muda na fazenda até você aplicar.';
            };
            drawerOpeners.car = reset;
            $('[data-fz-car-input]', carForm).addEventListener('input', reset);
            btn.addEventListener('click', async () => {
                const raw = $('[data-fz-car-input]', carForm).value.trim();
                if (!raw) { msg.className = 'fz-help err'; msg.textContent = 'Informe o número do CAR.'; return; }
                btn.disabled = true;
                btn.textContent = 'Consultando…';
                try {
                    const res = await fetch('/fazendas/consultar-car?car=' + encodeURIComponent(raw));
                    const payload = await res.json();
                    if (!res.ok || !payload.ok) throw new Error(payload.message || 'Não foi possível consultar o CAR agora.');
                    const d = payload.data || {};
                    const area = d.total_area != null ? fmtBr(d.total_area) + ' ha' : '—';
                    const rows = [
                        ['Área declarada', current.area, area],
                        ['Situação', current.status, d.status || '—'],
                        ['Condição', current.condition, d.condition || '—'],
                        ['Perímetro', current.map, d.geometry ? area : 'Sem perímetro público'],
                    ];
                    const tbody = $('[data-fz-car-rows]', carForm);
                    tbody.innerHTML = '';
                    rows.forEach(([k, a, b]) => {
                        const tr = document.createElement('tr');
                        [k, a, b].forEach((v, i) => {
                            const td = document.createElement('td');
                            td.textContent = v;
                            if (i === 1) td.style.color = '#5B6660';
                            if (i === 2 && a !== b) td.className = 'chg';
                            tr.appendChild(td);
                        });
                        tbody.appendChild(tr);
                    });
                    if (d.car_number) $('[data-fz-car-input]', carForm).value = d.car_number;
                    $('[data-fz-car-result]', carForm).hidden = false;
                    $('[data-fz-car-perim]', carForm).hidden = !d.geometry;
                    $('[data-fz-car-areas]', carForm).hidden = false;
                    $('[data-fz-car-hint]', carForm).hidden = true;
                    msg.className = 'fz-help';
                    msg.textContent = 'Encontrado na base pública do CAR. Confira e aplique.';
                    apply.disabled = false;
                } catch (err) {
                    msg.className = 'fz-help err';
                    msg.textContent = err.message + ' Confira o número e tente de novo.';
                } finally {
                    btn.disabled = false;
                    btn.textContent = 'Consultar';
                }
            });
        }

        /* Perímetro: editor no mapa */
        const perim = $('[data-fz-perim]');
        if (perim) {
            const input = $('[data-fz-perim-input]', perim);
            const applyInput = $('[data-fz-perim-apply]', perim);
            const save = $('[data-fz-perim-save]', perim);
            const hint = $('[data-fz-perim-hint]', perim);
            const areaEl = $('[data-fz-perim-area]', perim);
            const choice = $('[data-fz-perim-choice]', perim);
            const err = $('[data-fz-perim-err]', perim);
            const undoBtn = $('[data-fz-perim-undo]', perim);
            const removeBtn = $('[data-fz-perim-remove]', perim);
            const fileInput = $('[data-fz-perim-file]', perim);
            let editor = null;
            let startGeom = geometry;
            const mount = async (geom) => {
                if (typeof window.initFarmBoundaryEditor !== 'function') return;
                const host = document.getElementById('fz-edit-map');
                if (editor && editor.map && editor.map.remove) { try { editor.map.remove(); } catch (e) { /* noop */ } }
                host.innerHTML = '';
                host.className = 'fz-mapcanvas';
                if (!geom && !center) center = await geocode(location.query);
                editor = await window.initFarmBoundaryEditor({
                    containerId: 'fz-edit-map',
                    satellite: true,
                    hiddenInput: input,
                    initialGeometry: geom,
                    contextPlotGeometries: plotShapes.map((p) => p.geometry),
                    removePointButton: removeBtn,
                    undoButton: undoBtn,
                    initialCenter: !geom && center ? { lat: center[0], lng: center[1] } : undefined,
                    initialZoom: !geom && center ? 14 : undefined,
                    onStats: ({ vertices, area, selectedIndex, canUndo }) => {
                        const ok = vertices >= 3;
                        save.disabled = !ok;
                        undoBtn.disabled = !canUndo;
                        removeBtn.disabled = selectedIndex === null || vertices <= 3;
                        areaEl.textContent = fmtBr(area);
                        if (geometry) choice.hidden = !ok;
                        hint.textContent = ok ? 'Arraste os cantos para ajustar · ' + fmtBr(area) + ' ha'
                            : vertices === 0 ? 'Toque no mapa para marcar o primeiro canto' : 'Marque mais ' + (3 - vertices) + ' ponto(s) para fechar o perímetro';
                    },
                });
                setTimeout(() => editor?.invalidate?.() || editor?.fit?.(), 80);
            };
            drawerOpeners.perimetro = (t) => {
                err.hidden = true;
                mount(startGeom);
                if (t && t.dataset.fzImport) setTimeout(() => fileInput.click(), 120);
            };
            $('[data-fz-perim-start]', perim).addEventListener('click', () => { hint.textContent = geometry ? 'Arraste os cantos para ajustar' : 'Toque no mapa para marcar o primeiro canto'; });
            $('[data-fz-perim-clear]', perim).addEventListener('click', () => editor?.clearPoints?.());
            undoBtn.addEventListener('click', () => editor?.undo?.());
            $$('[data-fz-ac]', perim).forEach((b) => b.addEventListener('click', () => {
                applyInput.value = b.dataset.fzAc;
                $$('[data-fz-ac]', perim).forEach((x) => { const on = x === b; x.classList.toggle('on', on); x.setAttribute('aria-pressed', on ? 'true' : 'false'); });
            }));
            fileInput.addEventListener('change', async () => {
                const f = fileInput.files && fileInput.files[0];
                if (!f) return;
                const geom = window.parseGeodesicText ? window.parseGeodesicText(await f.text()) : null;
                if (!geom) { err.textContent = 'Não encontramos um perímetro neste arquivo. Envie um .kml, .geojson ou .json com o contorno da fazenda.'; err.hidden = false; return; }
                err.hidden = true;
                startGeom = geom;
                await mount(geom);
                toast('Perímetro importado. Confira e salve.');
            });
            perim.addEventListener('submit', (e) => { if (!input.value) { e.preventDefault(); toast('Marque pelo menos 3 pontos no mapa.', true); } });
        }

        if (farmPage.dataset.openPanel) setTimeout(() => showDrawer(farmPage.dataset.openPanel), 0);
    }

    /* =================== CADASTRO (3 ETAPAS) =================== */
    const wizard = $('[data-fz-wizard]');
    if (wizard) {
        const form = $('[data-fz-wizard-form]', wizard);
        const el = {
            car: $('[data-fz-w-car]', form), carBtn: $('[data-fz-w-car-btn]', form), carErr: $('[data-fz-w-car-err]', form),
            carOk: $('[data-fz-w-car-ok]', form), carSum: $('[data-fz-w-car-sum]', form),
            name: $('[data-fz-w-name]', form), uf: $('[data-fz-w-uf]', form), city: $('[data-fz-w-city]', form), area: $('[data-fz-w-area]', form),
            loc: $('[data-fz-w-location]', form), geo: $('[data-fz-w-geo]', form),
            back: $('[data-fz-w-back]', wizard), next: $('[data-fz-w-next]', wizard), submit: $('[data-fz-w-submit]', wizard),
        };
        const combo = bindCityCombo($('[data-fz-city]', form), el.uf, []);
        const state = { step: 1, car: null, perim: 'draw', drawn: null, file: null };
        let drawEditor = null;
        const viewMaps = {};

        const setFromCar = (on) => $$('[data-fz-fromcar]', form).forEach((s) => { s.hidden = !on; });
        const currentGeom = () => (state.perim === 'car' && state.car ? state.car.geometry : state.perim === 'draw' ? state.drawn : state.perim === 'file' ? state.file : null);

        const renderStep = () => {
            $$('[data-fz-wstep]', form).forEach((s) => { s.hidden = String(state.step) !== s.dataset.fzWstep; });
            $$('[data-fz-step]', wizard).forEach((li) => {
                const n = Number(li.dataset.fzStep);
                li.classList.toggle('on', n === state.step);
                li.classList.toggle('done', n < state.step);
                $('i', li).textContent = n < state.step ? '✓' : String(n);
            });
            el.back.textContent = state.step === 1 ? 'Cancelar' : 'Voltar';
            el.next.hidden = state.step === 3;
            el.submit.hidden = state.step !== 3;
            if (state.step === 2) renderPerim();
            if (state.step === 3) renderSummary();
            window.scrollTo({ top: 0, behavior: reduceMotion ? 'auto' : 'smooth' });
        };

        const showErr = (key, on) => { const e = $('[data-fz-err="' + key + '"]', form); if (e) e.hidden = !on; };
        const validate1 = () => {
            const okName = !!el.name.value.trim();
            const okUf = !!el.uf.value;
            const okCity = combo.valid();
            const area = parseBr(el.area.value);
            const okArea = area > 0;
            showErr('name', !okName); showErr('uf', !okUf); showErr('city', okUf && !okCity); showErr('area', !okArea);
            if (!okName) el.name.focus(); else if (!okUf) el.uf.focus(); else if (!okCity) el.city.focus(); else if (!okArea) el.area.focus();
            return okName && okUf && okCity && okArea;
        };

        /* CAR no passo 1: máscara UF-1234567-32 letras/números, busca sozinha ao completar */
        const carClean = (v) => String(v || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
        const carMask = (v) => {
            const c = carClean(v);
            let uf = '', mun = '', i = 0;
            while (i < c.length && uf.length < 2) { if (/[A-Z]/.test(c[i])) uf += c[i]; i++; }
            while (i < c.length && mun.length < 7) { if (/[0-9]/.test(c[i])) mun += c[i]; i++; }
            const cod = c.slice(i, i + 32);
            let out = uf;
            if (uf.length === 2 && mun) out += '-' + mun;
            if (mun.length === 7 && cod) out += '-' + cod;
            return out;
        };
        const CAR_FULL = 41;
        const carBox = el.car.closest('.fz-carbox') || el.car.parentElement;
        const carSignal = (status) => {
            if (carBox) carBox.dataset.carStatus = status;
            form.dispatchEvent(new CustomEvent('fz:car', { bubbles: true, detail: { status } }));
        };
        let carBusy = false, carLast = '';
        el.car.addEventListener('keydown', (e) => {
            if (e.key.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey && el.car.selectionStart === el.car.selectionEnd && carClean(el.car.value).length >= CAR_FULL) e.preventDefault();
        });
        el.car.addEventListener('input', () => {
            const masked = carMask(el.car.value);
            if (masked !== el.car.value) el.car.value = masked;
            el.carErr.hidden = true;
            if (carLast && masked !== carLast && !el.carOk.hidden) { el.carOk.hidden = true; carSignal('edited'); }
            else if (carLast && masked !== carLast && carBox?.dataset.carStatus === 'edited') carSignal('edited');
            else if (carLast && masked === carLast && state.car && el.carOk.hidden) { el.carOk.hidden = false; carSignal('restored'); }
            if (carClean(masked).length === CAR_FULL && !carBusy && masked !== carLast) el.carBtn.click();
        });
        el.carBtn.addEventListener('click', async () => {
            if (carBusy) return;
            const raw = carMask(el.car.value.trim());
            el.carErr.hidden = true;
            if (!raw) { el.carErr.textContent = 'Digite o número do CAR ou preencha os campos abaixo.'; el.carErr.hidden = false; return; }
            if (carClean(raw).length !== CAR_FULL) { el.carErr.textContent = 'Número incompleto. O CAR tem a sigla do estado, 7 números do município e 32 letras e números. Ex.: BA-2925006-E805CD5G28DB477C962F527B4F664CAA'; el.carErr.hidden = false; return; }
            el.car.value = raw;
            carBusy = true;
            el.carBtn.disabled = true;
            el.car.readOnly = true;
            el.car.setAttribute('aria-busy', 'true');
            el.carBtn.textContent = 'Consultando…';
            carSignal('busy');
            let found = false;
            try {
                const res = await fetch('/fazendas/consultar-car?car=' + encodeURIComponent(raw));
                const payload = await res.json();
                if (!res.ok || !payload.ok) throw new Error(payload.message || 'Não foi possível consultar o CAR agora.');
                const d = payload.data || {};
                state.car = d;
                el.car.value = carMask(d.car_number || raw);
                carLast = el.car.value;
                if (carBox) carBox.dataset.carLast = carLast;
                const parts = String(d.location || '').split(',');
                const city = (parts[0] || '').trim();
                const uf = (parts[1] || '').trim().toUpperCase();
                if (uf) await combo.set(uf, city);
                if (!el.name.value.trim() && d.name) el.name.value = d.name;
                if (d.total_area) el.area.value = fmtBr(d.total_area);
                const pts = d.geometry && window.extractFarmBoundaryPoints ? window.extractFarmBoundaryPoints(d.geometry).length : 0;
                el.carSum.textContent = [d.location ? d.location.replace(',', ' ·') : '', d.total_area ? fmtBr(d.total_area) + ' ha' : '', pts ? 'perímetro com ' + pts + ' pontos' : ''].filter(Boolean).join(' · ');
                el.carOk.hidden = false;
                setFromCar(true);
                state.perim = d.geometry ? 'car' : 'draw';
                ['name', 'uf', 'city', 'area'].forEach((k) => showErr(k, false));
                found = true;
            } catch (err) {
                el.carErr.textContent = err.message + ' Você pode preencher os dados manualmente.';
                el.carErr.hidden = false;
            } finally {
                carBusy = false;
                el.carBtn.disabled = false;
                el.car.readOnly = false;
                el.car.removeAttribute('aria-busy');
                el.carBtn.textContent = found ? 'Buscar de novo' : 'Buscar dados';
                carSignal(found ? 'found' : 'error');
            }
        });
        $('[data-fz-w-car-clear]', form).addEventListener('click', () => {
            state.car = null;
            el.car.value = '';
            carLast = '';
            el.carOk.hidden = true;
            carSignal('');
            setFromCar(false);
            if (state.perim === 'car') state.perim = 'draw';
        });

        /* Passo 2: perímetro */
        const showView = async (key, el2, geom) => {
            if (typeof L === 'undefined' || !el2) return;
            if (viewMaps[key]) { viewMaps[key].map.remove(); viewMaps[key] = null; el2.innerHTML = ''; el2.className = el2.className; }
            const c = geom ? null : await geocode(locText());
            viewMaps[key] = satelliteMap(el2, { geometry: geom, center: c });
            setTimeout(() => viewMaps[key]?.refresh(), 60);
        };
        const locText = () => (combo.canon() && el.uf.value ? combo.canon() + ', ' + el.uf.value : '');
        const polyArea = (geom) => {
            const pts = geom && window.extractFarmBoundaryPoints ? window.extractFarmBoundaryPoints(geom) : [];
            if (pts.length < 3) return 0;
            const R = 6378137;
            let sum = 0;
            for (let i = 0; i < pts.length; i++) {
                const a = pts[i]; const b = pts[(i + 1) % pts.length];
                sum += ((b.lng - a.lng) * Math.PI / 180) * (2 + Math.sin(a.lat * Math.PI / 180) + Math.sin(b.lat * Math.PI / 180));
            }
            return Math.abs(sum * R * R / 2) / 10000;
        };
        const mountDraw = async () => {
            if (drawEditor || typeof window.initFarmBoundaryEditor !== 'function') { drawEditor?.fit?.(); return; }
            const c = await geocode(locText());
            const hidden = document.createElement('input');
            drawEditor = await window.initFarmBoundaryEditor({
                containerId: 'fz-wizard-map',
                satellite: true,
                hiddenInput: hidden,
                initialGeometry: state.drawn,
                initialCenter: c ? { lat: c[0], lng: c[1] } : undefined,
                initialZoom: c ? 14 : undefined,
                undoButton: $('[data-fz-w-undo]', form),
                onStats: ({ vertices, area, canUndo }) => {
                    $('[data-fz-w-undo]', form).disabled = !canUndo;
                    try { state.drawn = hidden.value ? JSON.parse(hidden.value) : null; } catch (e) { state.drawn = null; }
                    if (vertices < 3) state.drawn = null;
                    $('[data-fz-w-hint]', form).textContent = vertices >= 3 ? vertices + ' pontos · ' + fmtBr(area) + ' ha'
                        : vertices === 0 ? 'Mapa centralizado em ' + (combo.canon() || 'sua região') + ' · toque para marcar o primeiro canto' : 'Marque mais ' + (3 - vertices) + ' ponto(s) para fechar o perímetro';
                },
            });
            setTimeout(() => drawEditor?.fit?.(), 80);
        };
        $('[data-fz-w-undo]', form).addEventListener('click', () => drawEditor?.undo?.());
        $('[data-fz-w-clear]', form).addEventListener('click', () => drawEditor?.clearPoints?.());
        $('[data-fz-w-start]', form).addEventListener('click', () => { $('[data-fz-w-hint]', form).textContent = 'Toque no mapa para marcar o primeiro canto'; });

        const renderPerim = () => {
            const carBtn = $('[data-fz-popt="car"]', form);
            const hasCarGeo = !!(state.car && state.car.geometry);
            carBtn.disabled = !hasCarGeo;
            $('[data-fz-popt-tag]', form).hidden = !hasCarGeo;
            const pts = hasCarGeo && window.extractFarmBoundaryPoints ? window.extractFarmBoundaryPoints(state.car.geometry).length : 0;
            $('[data-fz-popt-car-sub]', form).textContent = hasCarGeo ? pts + ' pontos · ' + fmtBr(polyArea(state.car.geometry)) + ' ha' : 'Busque o CAR no passo 1';
            if (state.perim === 'car' && !hasCarGeo) state.perim = 'draw';
            $$('[data-fz-popt]', form).forEach((b) => { const on = b.dataset.fzPopt === state.perim; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); });
            $$('[data-fz-pview]', form).forEach((v) => { v.hidden = v.dataset.fzPview !== state.perim; });
            if (state.perim === 'car') {
                showView('car', $('[data-fz-w-carmap]', form), state.car.geometry);
                const pa = polyArea(state.car.geometry);
                const declared = parseBr(el.area.value);
                const same = declared > 0 && Math.abs(pa - declared) / declared <= 0.05;
                const box = $('[data-fz-w-carcmp]', form);
                box.className = 'fz-box ' + (same ? 'ok' : 'warn');
                $('[data-fz-w-carcmp-text]', form).textContent = same
                    ? 'A área do perímetro (' + fmtBr(pa) + ' ha) é igual à área informada.'
                    : 'A área do perímetro (' + fmtBr(pa) + ' ha) é diferente da informada (' + fmtBr(declared) + ' ha). Você poderá escolher qual usar na ficha.';
            }
            if (state.perim === 'draw') setTimeout(mountDraw, 0);
        };
        $$('[data-fz-popt]', form).forEach((b) => b.addEventListener('click', () => { if (!b.disabled) { state.perim = b.dataset.fzPopt; renderPerim(); } }));
        const fileIn = $('[data-fz-w-file]', form);
        fileIn.addEventListener('change', async () => {
            const f = fileIn.files && fileIn.files[0];
            const errEl = $('[data-fz-w-file-err]', form);
            if (!f) return;
            const geom = window.parseGeodesicText ? window.parseGeodesicText(await f.text()) : null;
            if (!geom) { errEl.textContent = 'Não encontramos um perímetro neste arquivo. Envie um .kml, .geojson ou .json com o contorno da fazenda.'; errEl.hidden = false; state.file = null; return; }
            errEl.hidden = true;
            state.file = geom;
            $('[data-fz-w-drop-label]', form).textContent = f.name + ' · ' + fmtBr(polyArea(geom)) + ' ha';
            const m = $('[data-fz-w-filemap]', form);
            m.hidden = false;
            showView('file', m, geom);
        });

        /* Passo 3: conferir */
        const perimLabel = () => {
            const g = currentGeom();
            if (state.perim === 'car') return 'Do CAR · ' + fmtBr(polyArea(g)) + ' ha';
            if (state.perim === 'draw') return g ? 'Desenhado no mapa · ' + fmtBr(polyArea(g)) + ' ha' : 'Ainda não desenhado (fica para depois)';
            if (state.perim === 'file') return g ? 'Importado de arquivo · ' + fmtBr(polyArea(g)) + ' ha' : 'Nenhum arquivo (fica para depois)';
            return 'Fazer depois';
        };
        const renderSummary = () => {
            const set = (k, v) => { $('[data-fz-sum="' + k + '"]', form).textContent = v; };
            set('name', el.name.value.trim());
            set('location', combo.canon() + ' · ' + el.uf.value);
            set('area', fmtBr(parseBr(el.area.value)) + ' ha');
            set('car', el.car.value.trim() || 'Não informado');
            set('perim', perimLabel());
            const g = currentGeom();
            $('[data-fz-w-sumempty]', form).hidden = !!g;
            showView('sum', $('[data-fz-w-summap]', form), g);
        };
        $$('[data-fz-goto-step]', form).forEach((b) => b.addEventListener('click', () => { state.step = Number(b.dataset.fzGotoStep); renderStep(); }));

        el.next.addEventListener('click', () => {
            if (state.step === 1 && !validate1()) return;
            state.step += 1;
            renderStep();
        });
        el.back.addEventListener('click', () => {
            if (state.step === 1) { window.location.href = '/fazendas'; return; }
            state.step -= 1;
            renderStep();
        });
        form.addEventListener('submit', (e) => {
            if (!validate1()) { e.preventDefault(); state.step = 1; renderStep(); return; }
            el.loc.value = combo.canon() + ', ' + el.uf.value;
            const g = currentGeom();
            el.geo.value = g ? JSON.stringify(g) : '';
            el.area.value = String(parseBr(el.area.value));
            el.submit.disabled = true;
            el.submit.textContent = 'Salvando…';
        });
        renderStep();
    }
})();
