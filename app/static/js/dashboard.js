// Dashboard: painel de comando da safra.
(() => {
    const dataEl = document.getElementById('db-data');
    if (!dataEl) return;
    const D = JSON.parse(dataEl.textContent || '{}');
    const $ = (sel, root = document) => root.querySelector(sel);
    const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
    const brl = (v, dec = 2) => 'R$ ' + Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
    const compact = (v) => {
        const sign = v < 0 ? '− ' : '';
        const a = Math.abs(v);
        if (a >= 1e6) return sign + 'R$ ' + (a / 1e6).toLocaleString('pt-BR', { maximumFractionDigits: 2 }) + ' mi';
        if (a >= 1e3) return sign + 'R$ ' + Math.round(a / 1e3).toLocaleString('pt-BR') + ' mil';
        return sign + 'R$ ' + Math.round(a).toLocaleString('pt-BR');
    };
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

    // Saudação conforme a hora
    const greet = $('[data-db-greeting]');
    if (greet) { const h = new Date().getHours(); greet.textContent = h < 12 ? 'Bom dia' : h < 18 ? 'Boa tarde' : 'Boa noite'; }

    // Simulador de preço
    const hero = $('[data-db-hero]');
    const priceInput = $('[data-db-price]');
    if (hero && priceInput) {
        const sacks = Number(hero.dataset.sacks) || 0, cost = Number(hero.dataset.cost) || 0;
        priceInput.addEventListener('input', () => {
            const p = Number(priceInput.value) || 0, revenue = sacks * p, margin = revenue - cost;
            $$('[data-db-price-label]', hero).forEach((el) => { el.textContent = brl(p); });
            $$('[data-db-revenue]', hero).forEach((el) => { el.textContent = compact(revenue); });
            $$('[data-db-margin]', hero).forEach((el) => { el.textContent = compact(margin); });
            $$('[data-db-margin-pct]', hero).forEach((el) => { el.textContent = revenue ? Math.round(margin / revenue * 100) + '%' : '—'; });
        });
    }

    // Mapa vivo dos setores
    const tilesRoot = $('[data-db-tiles]');
    if (tilesRoot) {
        const tiles = $$('[data-db-tile]', tilesRoot);
        const green = ['#E3F1DF', '#BFDDB3', '#8FC27C', '#5E9A4C', '#3A6E2E', '#22461B'];
        const warm = ['#FFF4E0', '#FBE0B4', '#F4C27E', '#E59A4C', '#C8692E', '#9B3D1C'];
        const darkSet = new Set(['#5E9A4C', '#3A6E2E', '#22461B', '#C8692E', '#9B3D1C', '#E59A4C', '#B42318']);
        const scale = (arr, t) => arr[Math.max(0, Math.min(arr.length - 1, Math.round(t * (arr.length - 1))))];
        const labels = { prod: 'produtividade (sc/ha)', cost: 'custo por hectare', health: 'sanidade' };
        const range = (key) => { const vals = tiles.map((t) => Number(t.dataset[key]) || 0); const lo = Math.min(...vals), hi = Math.max(...vals); return [lo, hi === lo ? lo + 1 : hi]; };
        let metric = 'prod';
        const paint = () => {
            const [lo, hi] = metric === 'health' ? [0, 1] : range(metric);
            tiles.forEach((t) => {
                let bg;
                if (metric === 'prod') bg = scale(green, ((Number(t.dataset.prod) || 0) - lo) / (hi - lo));
                else if (metric === 'cost') bg = scale(warm, ((Number(t.dataset.cost) || 0) - lo) / (hi - lo));
                else bg = t.dataset.health === 'critico' ? '#B42318' : t.dataset.health === 'atencao' ? '#E59A4C' : '#5E9A4C';
                t.style.background = bg;
                t.style.color = darkSet.has(bg) ? '#fff' : '#16201A';
                const value = $('[data-db-tile-value]', t);
                if (value) value.textContent = metric === 'prod' ? (Number(t.dataset.prod) || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + ' sc/ha' : metric === 'cost' ? t.dataset.costLabel : t.dataset.healthLabel;
            });
            const lbl = $('[data-db-metric-label]', tilesRoot);
            if (lbl) lbl.textContent = labels[metric];
        };
        const select = (t) => {
            tiles.forEach((x) => x.setAttribute('aria-pressed', x === t ? 'true' : 'false'));
            $('[data-db-sel-name]', tilesRoot).textContent = `${t.dataset.name} · ${t.dataset.variety}`;
            $('[data-db-sel-detail]', tilesRoot).textContent = `${(Number(t.dataset.prod) || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 })} sc/ha estimadas · ${t.dataset.costLabel} de custo · ${t.dataset.healthDetail} · ${t.dataset.area} ha`;
            $('[data-db-sel-link]', tilesRoot).href = t.dataset.link;
        };
        $$('[data-db-metric]', tilesRoot).forEach((b) => b.addEventListener('click', () => {
            metric = b.dataset.dbMetric;
            $$('[data-db-metric]', tilesRoot).forEach((x) => x.setAttribute('aria-pressed', x === b ? 'true' : 'false'));
            paint();
        }));
        tiles.forEach((t) => t.addEventListener('click', () => select(t)));
        paint();
        if (tiles[0]) select(tiles[0]);
    }

    // Gráfico de linha reutilizável (cartão e janela)
    const drawLine = ({ root, svgW, svgH, series, labels, lineEls, guide, tip, empty, axis, costLine, costValue, nameOf }) => {
        const all = series.flatMap((s) => s.values.filter((v) => v != null));
        const show = all.length >= 2;
        if (empty) empty.hidden = show;
        lineEls.forEach((el) => el && el.setAttribute('points', ''));
        if (!show) { if (axis) axis.innerHTML = ''; return; }
        if (costValue) all.push(costValue);
        const lo = Math.min(...all), hi = Math.max(...all), pad = (hi - lo) * 0.1 || hi * 0.05 || 1;
        const n = labels.length;
        const X = (i) => (n <= 1 ? 0 : i / (n - 1) * svgW);
        const Y = (v) => svgH - (v - lo + pad) / (hi - lo + 2 * pad) * svgH;
        series.forEach((s, k) => {
            const pts = s.values.map((v, i) => (v == null ? null : `${X(i).toFixed(1)},${Y(v).toFixed(1)}`)).filter(Boolean).join(' ');
            if (lineEls[k]) lineEls[k].setAttribute('points', pts);
            if (s.area) s.area.setAttribute('points', pts ? `${pts} ${svgW},${svgH} 0,${svgH}` : '');
        });
        if (costLine) {
            if (costValue) { costLine.setAttribute('x1', 0); costLine.setAttribute('x2', svgW); costLine.setAttribute('y1', Y(costValue)); costLine.setAttribute('y2', Y(costValue)); costLine.style.display = ''; } else costLine.style.display = 'none';
        }
        if (axis) {
            const idx = n > 5 ? [0, Math.round((n - 1) * 0.25), Math.round((n - 1) * 0.5), Math.round((n - 1) * 0.75), n - 1] : labels.map((_, i) => i);
            axis.innerHTML = idx.map((i) => `<span>${esc(labels[i])}</span>`).join('');
        }
        const move = (clientX) => {
            const r = root.getBoundingClientRect();
            const i = Math.max(0, Math.min(n - 1, Math.round((clientX - r.left) / r.width * (n - 1))));
            const pct = n <= 1 ? 0 : i / (n - 1) * 100;
            guide.hidden = false; tip.hidden = false;
            guide.style.left = pct + '%';
            tip.style.left = Math.max(12, Math.min(88, pct)) + '%';
            tip.innerHTML = `<span class="db-tip-date">${esc(labels[i])}</span>` + series.map((s) => `<span>${series.length > 1 ? esc(nameOf(s)) + ': ' : ''}<strong>${s.values[i] == null ? '—' : brl(s.values[i])}</strong></span>`).join('');
        };
        root.onmousemove = (e) => move(e.clientX);
        root.ontouchstart = root.ontouchmove = (e) => { if (e.touches[0]) move(e.touches[0].clientX); };
        root.onmouseleave = root.ontouchend = () => { guide.hidden = true; tip.hidden = true; };
    };
    const history = D.history || {};
    const names = { arabica: 'Arábica', robusta: 'Robusta' };

    // Cartão Mercado do café
    const market = $('[data-db-market]');
    if (market) {
        let type = 'arabica';
        const render = () => {
            const h = history['12m'] || { labels: [], arabica: [], robusta: [] };
            const vals = h[type] || [];
            drawLine({ root: $('[data-db-spark]', market), svgW: 300, svgH: 96, labels: h.labels || [],
                series: [{ values: vals, area: $('[data-db-spark-area]', market), key: type }],
                lineEls: [$('[data-db-spark-line]', market)], guide: $('[data-db-spark-guide]', market), tip: $('[data-db-spark-tip]', market),
                empty: $('[data-db-spark-empty]', market), axis: $('[data-db-spark-axis]', market), nameOf: (s) => names[s.key] });
            const valid = vals.map((v, i) => [v, i]).filter(([v]) => v != null);
            const q = (D.quotes || {})[type];
            $('[data-db-coffee-name]', market).textContent = names[type];
            $('[data-db-coffee-price]', market).textContent = q ? brl(q.price) : '—';
            $('[data-db-coffee-date]', market).textContent = q ? 'em ' + q.date : '';
            if (valid.length >= 2) {
                const min = valid.reduce((a, b) => (b[0] < a[0] ? b : a)), max = valid.reduce((a, b) => (b[0] > a[0] ? b : a));
                const change = (valid[valid.length - 1][0] / valid[0][0] - 1) * 100;
                $('[data-db-min]', market).textContent = brl(min[0]); $('[data-db-min-when]', market).textContent = h.labels[min[1]];
                $('[data-db-max]', market).textContent = brl(max[0]); $('[data-db-max-when]', market).textContent = h.labels[max[1]];
                const v = $('[data-db-var]', market);
                v.textContent = (change >= 0 ? '+' : '') + change.toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + '%';
                v.style.color = change >= 0 ? '#1f7a3a' : '#b42318';
            }
        };
        $$('[data-db-coffee]', market).forEach((b) => b.addEventListener('click', () => {
            type = b.dataset.dbCoffee;
            $$('[data-db-coffee]', market).forEach((x) => x.setAttribute('aria-pressed', x === b ? 'true' : 'false'));
            render();
        }));
        render();
    }

    // Janela de histórico de preços
    const modal = document.getElementById('db-history-modal');
    if (modal) {
        let type = 'arabica', range = '12m', showCost = true;
        const render = () => {
            const h = history[range] || { labels: [], arabica: [], robusta: [] };
            const types = type === 'both' ? ['arabica', 'robusta'] : [type];
            const main = h[types[0]] || [];
            drawLine({ root: $('[data-db-h-chart]', modal), svgW: 1000, svgH: 300, labels: h.labels || [],
                series: types.map((k) => ({ values: h[k] || [], key: k })),
                lineEls: type === 'robusta' ? [$('[data-db-h-line-b]', modal)] : [$('[data-db-h-line-a]', modal), $('[data-db-h-line-b]', modal)],
                guide: $('[data-db-h-guide]', modal), tip: $('[data-db-h-tip]', modal), empty: $('[data-db-h-empty]', modal), axis: $('[data-db-h-axis]', modal),
                costLine: $('[data-db-h-costline]', modal), costValue: showCost && type !== 'robusta' ? D.costPerSack : 0, nameOf: (s) => names[s.key] });
            if (type === 'robusta') $('[data-db-h-line-a]', modal).setAttribute('points', '');
            const valid = main.filter((v) => v != null);
            const q = (D.quotes || {})[types[0]];
            const stat = (l, v, s = '', style = '') => `<div><dt>${l}</dt><dd style="${style}">${v}</dd><dd class="db-muted">${s}</dd></div>`;
            let html = stat('Preço atual', q ? brl(q.price) : '—', names[types[0]]);
            if (valid.length) {
                const min = Math.min(...valid), max = Math.max(...valid), avg = valid.reduce((a, b) => a + b, 0) / valid.length;
                const ch = valid.length >= 2 ? (valid[valid.length - 1] / valid[0] - 1) * 100 : 0;
                html += stat('Mínima', brl(min), h.labels[main.indexOf(min)]) + stat('Máxima', brl(max), h.labels[main.indexOf(max)]) + stat('Média', brl(avg), 'no período')
                    + stat('Variação', (ch >= 0 ? '+' : '') + ch.toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + '%', 'no período', 'color:' + (ch >= 0 ? '#1f7a3a' : '#b42318'));
            }
            $('[data-db-h-stats]', modal).innerHTML = html;
            const tableType = type === 'robusta' ? 'robusta' : 'arabica';
            const t = history['12m'] || { labels: [], arabica: [], robusta: [] };
            const tv = t[tableType] || [];
            $('[data-db-h-table-name]', modal).textContent = names[tableType];
            $('[data-db-h-table]', modal).innerHTML = (t.labels || []).map((label, i) => {
                const v = tv[i], prev = i ? tv[i - 1] : null;
                const d = v != null && prev ? (v / prev - 1) * 100 : null;
                return `<div><span>${esc(label)}</span><strong>${v == null ? '—' : brl(v)}</strong><span style="color:${d == null ? '#5b6660' : d >= 0 ? '#1f7a3a' : '#b42318'}">${d == null ? '—' : (d >= 0 ? '+' : '') + d.toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + '%'}</span></div>`;
            }).reverse().join('') || '<p class="db-muted">Sem cotações salvas.</p>';
        };
        $$('[data-db-h-type]', modal).forEach((b) => b.addEventListener('click', () => { type = b.dataset.dbHType; $$('[data-db-h-type]', modal).forEach((x) => x.setAttribute('aria-pressed', x === b ? 'true' : 'false')); render(); }));
        $$('[data-db-h-range]', modal).forEach((b) => b.addEventListener('click', () => { range = b.dataset.dbHRange; $$('[data-db-h-range]', modal).forEach((x) => x.setAttribute('aria-pressed', x === b ? 'true' : 'false')); render(); }));
        $('[data-db-h-cost]', modal)?.addEventListener('change', (e) => { showCost = e.target.checked; render(); });
        $$('[data-db-history-open]').forEach((b) => b.addEventListener('click', () => { modal.showModal(); render(); }));
        $$('[data-db-history-close]', modal).forEach((b) => b.addEventListener('click', () => modal.close()));
        modal.addEventListener('click', (e) => { if (e.target === modal) modal.close(); });
    }

    // Calendário de aplicações
    const cal = $('[data-db-calendar]');
    if (cal) {
        const items = D.calendar || [];
        const today = D.today ? new Date(D.today + 'T12:00:00') : new Date();
        const iso = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
        const monthsLong = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'];
        const weekdays = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado'];
        const colors = { plan: '#446a36', late: '#d9480f', done: '#a3ada6' };
        const statusLabel = { plan: 'Programada', late: 'Atrasada', done: 'Concluída' };
        const firstPending = items.find((i) => i.status !== 'done' && i.date >= iso(today));
        let view = new Date(today.getFullYear(), today.getMonth(), 1);
        let selected = firstPending ? firstPending.date : iso(today);
        const render = () => {
            const y = view.getFullYear(), m = view.getMonth();
            $('[data-db-cal-title]', cal).textContent = `${monthsLong[m][0].toUpperCase()}${monthsLong[m].slice(1)} de ${y}`;
            const monthItems = items.filter((i) => i.date.startsWith(`${y}-${String(m + 1).padStart(2, '0')}`));
            const late = monthItems.filter((i) => i.status === 'late').length, plan = monthItems.filter((i) => i.status === 'plan').length;
            $('[data-db-cal-summary]', cal).textContent = monthItems.length ? `${monthItems.length} aplicação(ões) no mês · ${late} atrasada(s) · ${plan} programada(s)` : 'Nenhuma aplicação neste mês';
            const first = new Date(y, m, 1).getDay(), days = new Date(y, m + 1, 0).getDate();
            let html = '';
            for (let i = 0; i < first; i++) html += '<span></span>';
            for (let d = 1; d <= days; d++) {
                const key = `${y}-${String(m + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
                const evs = items.filter((i) => i.date === key);
                const cls = ['db-cal-day-btn', key === selected ? 'is-selected' : '', key === iso(today) ? 'is-today' : '', key < iso(today) ? 'is-past' : ''].join(' ');
                html += `<button type="button" class="${cls}" data-day="${key}" aria-label="${d} de ${monthsLong[m]}, ${evs.length} aplicação(ões)"><span>${d}</span><span class="db-cal-dots">${evs.slice(0, 3).map((e) => `<i style="background:${colors[e.status]}"></i>`).join('')}</span></button>`;
            }
            const grid = $('[data-db-cal-grid]', cal);
            grid.innerHTML = html;
            $$('[data-day]', grid).forEach((b) => b.addEventListener('click', () => { selected = b.dataset.day; render(); }));
            const sel = new Date(selected + 'T12:00:00');
            $('[data-db-cal-day-title]', cal).textContent = `${selected === iso(today) ? 'Hoje' : weekdays[sel.getDay()]}, ${sel.getDate()} de ${monthsLong[sel.getMonth()]}`;
            const dayItems = items.filter((i) => i.date === selected);
            $('[data-db-cal-day-list]', cal).innerHTML = dayItems.length ? dayItems.map((e) => `
                <article class="db-cal-item is-${e.status}">
                    <div class="db-cal-item-head"><strong>${esc(e.method)} — ${esc(e.plot)}</strong><span class="db-cal-pill is-${e.status}">${statusLabel[e.status]}</span></div>
                    <p class="db-muted">${esc(e.products)}</p>
                    <div class="db-cal-actions">
                        ${e.status !== 'done' ? `<form method="post" action="/fertilizacao/agendamentos/${e.id}/concluir"><input type="hidden" name="csrf_token" value="${esc(D.csrf)}"><input type="hidden" name="redirect_to" value="${esc(D.redirectTo)}"><button class="sf-btn sf-btn-primary db-btn-sm">Concluir</button></form>` : ''}
                        <a class="sf-btn sf-btn-secondary db-btn-sm" href="${esc(e.link)}">${e.status === 'done' ? 'Ver' : 'Reagendar'}</a>
                    </div>
                </article>`).join('') : '<p class="db-cal-empty">Nenhuma aplicação neste dia.</p>';
        };
        $('[data-db-cal-prev]', cal).addEventListener('click', () => { view = new Date(view.getFullYear(), view.getMonth() - 1, 1); render(); });
        $('[data-db-cal-next]', cal).addEventListener('click', () => { view = new Date(view.getFullYear(), view.getMonth() + 1, 1); render(); });
        render();
    }

    // Janela de pluviometria
    const rainModal = document.getElementById('db-rain-modal');
    if (rainModal && D.rain) {
        const R = D.rain, S = R.settings || {};
        const heavy = Number(S.heavy_rain_mm) || 30, rainyMin = Number(S.rainy_day_mm) || 0, since = S.average_since_year ? Number(S.average_since_year) : null;
        const today = new Date(D.today + 'T12:00:00');
        const iso = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
        const mShort = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
        const mLong = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'];
        const mmFmt = (v) => Number(v || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + ' mm';
        let gauge = '', period = '12m', cmp = 'avg', hov = null;
        const gaugeSel = $('[data-db-r-gauge]', rainModal);
        [...new Set(R.records.map((r) => r.src).filter(Boolean))].sort().forEach((src) => { const o = document.createElement('option'); o.value = src; o.textContent = src; gaugeSel.appendChild(o); });
        const sinceSel = $('[data-db-r-since]', rainModal);
        [...new Set(R.records.map((r) => Number(r.d.slice(0, 4))))].sort().forEach((y) => { const o = document.createElement('option'); o.value = y; o.textContent = y; if (since === y) o.selected = true; sinceSel.appendChild(o); });
        const fromInput = $('[data-db-r-from]', rainModal), toInput = $('[data-db-r-to]', rainModal);
        fromInput.value = `${today.getFullYear()}-01-01`; toInput.value = iso(today);
        const recs = () => R.records.filter((r) => !gauge || r.src === gauge);
        const byDay = () => { const m = {}; recs().forEach((r) => { m[r.d] = (m[r.d] || 0) + r.mm; }); return m; };
        const sumRange = (days, a, b) => Object.entries(days).reduce((t, [d, v]) => (d >= a && d <= b ? t + v : t), 0);
        const monthKey = (y, m) => `${y}-${String(m + 1).padStart(2, '0')}`;
        const monthTotals = (days) => { const m = {}; Object.entries(days).forEach(([d, v]) => { const k = d.slice(0, 7); m[k] = (m[k] || 0) + v; }); return m; };
        const histAvg = (totalsByKey, matchFn, untilYear) => {
            const vals = Object.entries(totalsByKey).filter(([k]) => { const y = Number(k.slice(0, 4)); return matchFn(k) && y < untilYear && (!since || y >= since); }).map(([, v]) => v);
            return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
        };
        const build = () => {
            const days = byDay(), months = monthTotals(days);
            const bars = [];
            const addMonth = (y, m, partial) => {
                const k = monthKey(y, m);
                bars.push({ x: mShort[m] + ((period === '12m' || period === 'custom') && window.innerWidth >= 640 ? `/${String(y).slice(2)}` : ''), label: `${mLong[m]} de ${y}${partial ? ' (até hoje)' : ''}`, v: months[k] || 0,
                    avg: histAvg(months, (kk) => kk.slice(5) === k.slice(5), y), prev: months[monthKey(y - 1, m)] || 0, day: false });
            };
            let a, b;
            if (period === 'month') {
                const y = today.getFullYear(), m = today.getMonth();
                a = iso(new Date(y, m, 1)); b = iso(today);
                for (let d = 1; d <= today.getDate(); d++) {
                    const k = `${monthKey(y, m)}-${String(d).padStart(2, '0')}`;
                    const sameDay = Object.entries(days).filter(([dd]) => dd.slice(5) === k.slice(5) && Number(dd.slice(0, 4)) < y && (!since || Number(dd.slice(0, 4)) >= since));
                    const yearsWithData = new Set(Object.keys(days).map((dd) => dd.slice(0, 4)).filter((yy) => Number(yy) < y && (!since || Number(yy) >= since))).size;
                    bars.push({ x: String(d), label: `${d} de ${mLong[m]}`, v: days[k] || 0, avg: yearsWithData ? sameDay.reduce((t, [, v]) => t + v, 0) / yearsWithData : null, prev: days[`${y - 1}${k.slice(4)}`] || 0, day: true });
                }
            } else if (period === '5y') {
                const y0 = today.getFullYear() - 4; a = `${y0}-01-01`; b = iso(today);
                const years = {}; Object.entries(days).forEach(([d, v]) => { const y = Number(d.slice(0, 4)); years[y] = (years[y] || 0) + v; });
                for (let y = y0; y <= today.getFullYear(); y++) {
                    const prevYears = Object.entries(years).filter(([yy]) => Number(yy) < y && Number(yy) < today.getFullYear() && (!since || Number(yy) >= since)).map(([, v]) => v);
                    bars.push({ x: String(y), label: y === today.getFullYear() ? `${y} (até hoje)` : String(y), v: years[y] || 0, avg: prevYears.length ? prevYears.reduce((p, q) => p + q, 0) / prevYears.length : null, prev: years[y - 1] || 0, day: false });
                }
            } else {
                let start, end;
                if (period === '12m') { start = new Date(today.getFullYear(), today.getMonth() - 11, 1); end = today; }
                else if (period === 'season') { start = R.season.start ? new Date(R.season.start + 'T12:00:00') : new Date(today.getFullYear(), 0, 1); end = R.season.end ? new Date(Math.min(new Date(R.season.end + 'T12:00:00'), today)) : today; }
                else { start = fromInput.value ? new Date(fromInput.value + 'T12:00:00') : new Date(today.getFullYear(), 0, 1); end = toInput.value ? new Date(toInput.value + 'T12:00:00') : today; }
                if (end < start) [start, end] = [end, start];
                a = iso(start); b = iso(end);
                const spanDays = (end - start) / 864e5;
                if (period === 'custom' && spanDays <= 62) {
                    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) { const k = iso(d); bars.push({ x: String(d.getDate()), label: `${d.getDate()} de ${mLong[d.getMonth()]}`, v: days[k] || 0, avg: null, prev: days[`${d.getFullYear() - 1}${k.slice(4)}`] || 0, day: true }); }
                } else {
                    for (let d = new Date(start.getFullYear(), start.getMonth(), 1); d <= end; d.setMonth(d.getMonth() + 1)) addMonth(d.getFullYear(), d.getMonth(), d.getFullYear() === today.getFullYear() && d.getMonth() === today.getMonth());
                }
            }
            return { bars, a, b, days };
        };
        const render = () => {
            const { bars, a, b, days } = build();
            const cmpVals = cmp === 'none' ? null : bars.map((x) => (cmp === 'avg' ? x.avg : x.prev));
            const top = Math.max(1, ...bars.map((x) => x.v), ...(cmpVals || []).filter((v) => v != null)) * 1.15;
            const chart = $('[data-db-r-chart]', rainModal);
            chart.style.setProperty('--n', bars.length);
            chart.innerHTML = `<div class="db-rain-yaxis">${[1, .75, .5, .25, 0].map((t) => `<span>${Math.round(top * t)}</span>`).join('')}</div><div class="db-rain-plot">${bars.map((x, i) => {
                const c = cmpVals ? cmpVals[i] : null;
                const isHeavy = x.day && x.v >= heavy;
                return `<div class="db-rain-col${hov === i ? ' is-hov' : ''}" data-i="${i}"><span class="db-rain-bar${isHeavy ? ' is-heavy' : ''}" style="height:${Math.max(0.5, x.v / top * 100)}%"></span>${c != null ? `<span class="db-rain-cmp" style="bottom:${c / top * 100}%"></span>` : ''}${hov === i ? `<span class="db-rain-tip"><span class="db-tip-date">${esc(x.label)}</span><strong>${mmFmt(x.v)}</strong>${c != null ? `<span>${cmp === 'avg' ? 'Média' : 'Ano anterior'}: ${mmFmt(c)}</span>` : ''}</span>` : ''}<span class="db-rain-x">${esc(x.x)}</span></div>`;
            }).join('')}</div>`;
            $$('.db-rain-col', chart).forEach((col) => {
                col.addEventListener('mouseenter', () => { hov = Number(col.dataset.i); render(); });
                col.addEventListener('click', () => { hov = Number(col.dataset.i); render(); });
            });
            chart.onmouseleave = () => { if (hov !== null) { hov = null; render(); } };
            const total = bars.reduce((t, x) => t + x.v, 0);
            const cmpTotal = cmpVals && cmpVals.some((v) => v != null) ? cmpVals.reduce((t, v) => t + (v || 0), 0) : null;
            const diff = cmpTotal ? (total / cmpTotal - 1) * 100 : null;
            const inRange = Object.entries(days).filter(([d]) => d >= a && d <= b);
            const rainy = inRange.filter(([, v]) => v >= Math.max(rainyMin, 0.01)).length;
            const maxDay = inRange.reduce((m, e) => (e[1] > m[1] ? e : m), ['', 0]);
            const lastRain = Object.entries(days).filter(([d, v]) => v >= Math.max(rainyMin, 0.01) && d <= iso(today)).map(([d]) => d).sort().pop();
            const sinceDays = lastRain ? Math.round((today - new Date(lastRain + 'T12:00:00')) / 864e5) : null;
            const fmtDate = (d) => (d ? `${d.slice(8, 10)}/${d.slice(5, 7)}/${d.slice(0, 4)}` : '—');
            const stat = (l, v, s = '', style = '') => `<div><dt>${l}</dt><dd style="${style}">${v}</dd><dd class="db-muted">${s}</dd></div>`;
            $('[data-db-r-stats]', rainModal).innerHTML = stat('Total no período', mmFmt(total), '', 'color:#1f4e86')
                + stat(cmp === 'prev' ? 'Ano anterior' : 'Média histórica', cmpTotal == null ? '—' : mmFmt(cmpTotal), cmp === 'none' ? 'sem comparação' : 'mesmo período')
                + stat('Diferença', diff == null ? '—' : (diff >= 0 ? '+' : '') + Math.round(diff) + '%', diff == null ? '' : diff >= 0 ? 'acima' : 'abaixo', diff == null ? '' : 'color:' + (diff >= 0 ? '#1f7a3a' : '#b43c0b'))
                + stat('Dias com chuva', String(rainy), `≥ ${rainyMin.toLocaleString('pt-BR')} mm`)
                + stat('Maior chuva', maxDay[1] ? mmFmt(maxDay[1]) : '—', fmtDate(maxDay[0]))
                + stat('Sem chuva há', sinceDays == null ? '—' : sinceDays === 0 ? 'hoje choveu' : `${sinceDays} dia(s)`, lastRain ? 'último: ' + fmtDate(lastRain) : '');
            $('[data-db-r-cmp-legend]', rainModal).hidden = cmp === 'none';
            $('[data-db-r-cmp-name]', rainModal).textContent = cmp === 'avg' ? 'Média histórica' : 'Ano anterior';
            const list = recs().slice().sort((x, y) => (x.d < y.d ? 1 : -1)).slice(0, 8);
            $('[data-db-r-records]', rainModal).innerHTML = list.length ? list.map((r) => `<div class="db-rain-rec"><span class="db-rain-rec-mm"><strong>${Number(r.mm).toLocaleString('pt-BR', { maximumFractionDigits: 1 })}</strong><small>mm</small></span><span class="min-w-0"><span class="db-inbox-title">${fmtDate(r.d)}</span><span class="db-muted block">${esc([r.src, r.notes].filter(Boolean).join(' · ') || 'Registro manual')}</span></span></div>`).join('') : '<p class="db-muted">Nenhum registro de chuva.</p>';
        };
        const pick = (attr, setter) => $$(`[${attr}]`, rainModal).forEach((btn) => btn.addEventListener('click', () => {
            $$(`[${attr}]`, rainModal).forEach((x) => x.setAttribute('aria-pressed', x === btn ? 'true' : 'false'));
            setter(btn.getAttribute(attr)); hov = null; render();
        }));
        pick('data-db-r-period', (v) => { period = v; $('[data-db-r-custom]', rainModal).hidden = v !== 'custom'; });
        pick('data-db-r-cmp', (v) => { cmp = v; });
        gaugeSel.addEventListener('change', () => { gauge = gaugeSel.value; render(); });
        [fromInput, toInput].forEach((el) => el.addEventListener('change', render));
        const settingsForm = $('[data-db-r-settings]', rainModal), mainSide = $('[data-db-r-main-side]', rainModal);
        $('[data-db-r-settings-open]', rainModal)?.addEventListener('click', () => { settingsForm.hidden = false; mainSide.hidden = true; });
        $$('[data-db-r-settings-close]', rainModal).forEach((b2) => b2.addEventListener('click', () => { settingsForm.hidden = true; mainSide.hidden = false; settingsForm.reset(); }));
        $$('[data-db-rain-open]').forEach((b2) => b2.addEventListener('click', () => { rainModal.showModal(); render(); }));
        $$('[data-db-rain-close]', rainModal).forEach((b2) => b2.addEventListener('click', () => rainModal.close()));
        rainModal.addEventListener('click', (e) => { if (e.target === rainModal) rainModal.close(); });
    }

    // Janela da agenda completa
    const agendaModal = document.getElementById('db-agenda-modal');
    if (agendaModal) {
        const allowed = new Set(D.agendaTypes || []);
        const events = (D.agenda || []).filter((e) => allowed.has(e.type));
        const T = { app: ['Aplicações', 'Aplicação', '#446a36', '#eef7ed', '#294127'], pay: ['Pagamentos', 'Pagamento', '#b42318', '#fef3f2', '#912018'], rec: ['Recebimentos', 'Recebimento', '#1f7a3a', '#e3f1df', '#1f5a22'], maint: ['Manutenção', 'Manutenção', '#5b6660', '#eef1ed', '#3c4741'] };
        const statusLabel = { plan: 'Programada', late: 'Atrasada', done: 'Concluída' };
        const today = new Date(D.today + 'T12:00:00');
        const iso = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
        const mLong = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'];
        const wd = ['Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado'];
        let view = new Date(today.getFullYear(), today.getMonth(), 1), sel = iso(today), type = 'all';
        const filtered = () => events.filter((e) => type === 'all' || e.type === type);
        const render = () => {
            const y = view.getFullYear(), m = view.getMonth();
            $('[data-db-a-title]', agendaModal).textContent = `${mLong[m][0].toUpperCase()}${mLong[m].slice(1)} de ${y}`;
            const types = ['all', ...Object.keys(T).filter((k) => allowed.has(k))];
            $('[data-db-a-filters]', agendaModal).innerHTML = types.map((k) => {
                const n = events.filter((e) => k === 'all' || e.type === k).length;
                const color = k === 'all' ? '#16201a' : T[k][2];
                return `<button type="button" class="db-a-chip" aria-pressed="${type === k}" data-type="${k}"><i style="background:${type === k ? '#fff' : color}"></i>${k === 'all' ? 'Todas' : T[k][0]} <span>${n}</span></button>`;
            }).join('');
            $$('[data-type]', agendaModal).forEach((b2) => b2.addEventListener('click', () => { type = b2.dataset.type; render(); }));
            const first = new Date(y, m, 1).getDay(), days = new Date(y, m + 1, 0).getDate();
            const list = filtered();
            let html = '';
            for (let i = 0; i < first; i++) html += '<span class="db-a-cell is-empty"></span>';
            for (let d = 1; d <= days; d++) {
                const key = `${y}-${String(m + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
                const evs = list.filter((e) => e.date === key);
                const chips = evs.slice(0, 3).map((e) => { const t = T[e.type]; const late = e.status === 'late'; return `<span class="db-a-ev${e.status === 'done' ? ' is-done' : ''}" style="background:${late ? '#fff0e8' : t[3]};color:${late ? '#b43c0b' : t[4]};border-left-color:${late ? '#d9480f' : t[2]}">${esc(e.title)}</span>`; }).join('');
                html += `<button type="button" class="db-a-cell${key === sel ? ' is-selected' : ''}${key < iso(today) ? ' is-past' : ''}" data-day="${key}" aria-label="${d} de ${mLong[m]}, ${evs.length} atividade(s)"><span class="db-a-num${key === iso(today) ? ' is-today' : ''}">${d}</span>${chips}${evs.length > 3 ? `<span class="db-a-more">+${evs.length - 3} mais</span>` : ''}</button>`;
            }
            const grid = $('[data-db-a-grid]', agendaModal);
            grid.innerHTML = html;
            $$('[data-day]', grid).forEach((b2) => b2.addEventListener('click', () => { sel = b2.dataset.day; render(); }));
            const sd = new Date(sel + 'T12:00:00');
            $('[data-db-a-weekday]', agendaModal).textContent = wd[sd.getDay()] + (sel === iso(today) ? ' · hoje' : '');
            $('[data-db-a-day-title]', agendaModal).textContent = `${sd.getDate()} de ${mLong[sd.getMonth()]}`;
            const dayList = list.filter((e) => e.date === sel);
            $('[data-db-a-day-list]', agendaModal).innerHTML = dayList.length ? dayList.map((e) => {
                const t = T[e.type];
                const action = e.type === 'app' && e.status !== 'done'
                    ? `<form method="post" action="/fertilizacao/agendamentos/${e.schedule_id}/concluir"><input type="hidden" name="csrf_token" value="${esc(D.csrf)}"><input type="hidden" name="redirect_to" value="${esc(D.redirectTo)}"><button class="sf-btn sf-btn-primary db-btn-sm">Concluir</button></form>`
                    : (e.type === 'pay' || e.type === 'rec') && e.status !== 'done' ? `<a class="sf-btn sf-btn-primary db-btn-sm" href="${esc(e.link)}">${e.type === 'pay' ? 'Pagar' : 'Confirmar'}</a>` : '';
                return `<article class="db-a-item${e.status === 'late' ? ' is-late' : ''}"><div class="db-a-item-type"><i style="background:${t[2]}"></i>${t[1]}<span class="db-cal-pill is-${e.status}">${statusLabel[e.status]}</span></div><p class="db-inbox-title">${esc(e.title)}</p><p class="db-muted">${esc(e.meta)}</p><div class="db-cal-actions">${action}<a class="sf-btn sf-btn-secondary db-btn-sm" href="${esc(e.link)}">Abrir</a></div></article>`;
            }).join('') : '<p class="db-cal-empty">Nada programado neste dia.</p>';
        };
        $('[data-db-a-prev]', agendaModal).addEventListener('click', () => { view = new Date(view.getFullYear(), view.getMonth() - 1, 1); render(); });
        $('[data-db-a-next]', agendaModal).addEventListener('click', () => { view = new Date(view.getFullYear(), view.getMonth() + 1, 1); render(); });
        $('[data-db-a-today]', agendaModal).addEventListener('click', () => { view = new Date(today.getFullYear(), today.getMonth(), 1); sel = iso(today); render(); });
        $$('[data-db-agenda-open]').forEach((b2) => b2.addEventListener('click', () => { agendaModal.showModal(); render(); }));
        $$('[data-db-agenda-close]', agendaModal).forEach((b2) => b2.addEventListener('click', () => agendaModal.close()));
        agendaModal.addEventListener('click', (e) => { if (e.target === agendaModal) agendaModal.close(); });
    }

    // Sincronização diária da cotação do CEPEA pelo navegador (mantida do Dashboard anterior)
    (() => {
        if (!market || !D.csrf) return;
        const syncKey = `sisfarm:cepea-browser-sync:${new Date().toISOString().slice(0, 10)}`;
        try { if (sessionStorage.getItem(syncKey) === 'done') return; } catch (e) { return; }
        const parseNumber = (value) => { const n = Number.parseFloat(String(value || '').replace(/[^\d,.-]/g, '').replace(/\./g, '').replace(',', '.')); return Number.isFinite(n) ? n : null; };
        const normalizeDate = (value) => { const p = String(value || '').trim().split('/'); return p.length === 3 ? `${p[2].padStart(4, '0')}-${p[1].padStart(2, '0')}-${p[0].padStart(2, '0')}` : ''; };
        const currentMax = () => Object.values(D.quoteDates || {}).filter(Boolean).sort().pop() || '';
        const parseWidget = (doc) => Array.from(doc?.querySelectorAll('tr') || []).map((row) => {
            const cells = Array.from(row.querySelectorAll('td')).map((c) => c.textContent.trim());
            if (cells.length < 3) return null;
            const quote_date = normalizeDate(cells[0]), price_brl = parseNumber(cells[2]);
            const product = cells[1].normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
            return quote_date && price_brl ? { quote_type: product.includes('robusta') || product.includes('conillon') ? 'robusta' : 'arabica', quote_date, price_brl } : null;
        }).filter(Boolean);
        const submit = async (quotes) => {
            if (!quotes.length || (quotes.map((q) => q.quote_date).sort().pop() || '') <= currentMax()) return;
            const response = await fetch('/api/dashboard/cotacao-cafe/sincronizar-navegador', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ csrf_token: D.csrf, quotes }) });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok || !payload.ok) return;
            try { sessionStorage.setItem(syncKey, 'done'); } catch (e) { /* segue */ }
            location.reload();
        };
        const frame = document.createElement('iframe');
        frame.className = 'hidden';
        frame.setAttribute('aria-hidden', 'true');
        frame.srcdoc = `<!doctype html><html><body><script src="https://www.cepea.org.br/br/widgetproduto.js.php?fonte=arial&tamanho=10&largura=400px&corfundo=dbd6b2&cortexto=333333&corlinha=ede7bf&id_indicador%5B%5D=23&id_indicador%5B%5D=24"><\/script></body></html>`;
        frame.addEventListener('load', () => {
            window.setTimeout(() => {
                try { submit(parseWidget(frame.contentDocument || frame.contentWindow?.document)).finally(() => frame.remove()); } catch (e) { frame.remove(); }
            }, 900);
        }, { once: true });
        document.body.appendChild(frame);
    })();
})();
