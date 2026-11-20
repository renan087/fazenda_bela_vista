/* Nova análise em 4 passos: lote e origem → física → prova de xícara → conferir. */
(() => {
    const form = document.querySelector('[data-cf-form]');
    if (!form) return;
    const $ = (s, r = form) => r.querySelector(s);
    const $$ = (s, r = form) => [...r.querySelectorAll(s)];
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const num = (v) => { const n = parseFloat(String(v ?? '').replace(',', '.')); return Number.isFinite(n) ? n : null; };
    const fmt = (n, d = 1) => n.toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: 2 });
    const scale = Number(form.dataset.scale) || 10;
    const cup = $('[data-cf-cup]');
    const checkedVal = (name) => $(`[name="${name}"]:checked`)?.value || '';
    let step = 1;
    let skipped = false;

    const kind = () => checkedVal('analysis_kind') || 'ambos';
    const usesCup = () => kind() !== 'fisica' && !skipped;
    const usesPhys = () => kind() !== 'xicara';
    const steps = () => [1, ...(usesPhys() ? [2] : []), ...(usesCup() || kind() === 'xicara' ? [3] : []), 4];

    function syncCup() {
        const off = !usesCup();
        $$('input, textarea', cup).forEach((el) => { if (el.dataset.cfFlavorNew === undefined) el.disabled = off; });
        $('[data-cf-cup-tab]', document).hidden = kind() === 'fisica';
    }

    function band(score) {
        if (score == null) return ['', 'none'];
        return score >= 84 ? ['Especial · excelente', 'top'] : score >= 80 ? ['Especial', 'ok'] : ['Comercial', 'std'];
    }

    function radar(svg, small) {
        if (!svg) return;
        const inputs = $$('[data-cf-attr]');
        const n = inputs.length;
        if (n < 3) { svg.innerHTML = ''; return; }
        const C = 120; const R = 86;
        const pt = (i, k) => { const a = -Math.PI / 2 + i * 2 * Math.PI / n; return `${(C + k * Math.cos(a)).toFixed(1)},${(C + k * Math.sin(a)).toFixed(1)}`; };
        const ring = (f) => inputs.map((_, i) => pt(i, R * f)).join(' ');
        const shape = usesCup() ? inputs.map((el, i) => pt(i, R * Math.min(Math.max(num(el.value) || 0, 0), scale) / scale)).join(' ') : '';
        const labels = small ? '' : inputs.map((el, i) => { const a = -Math.PI / 2 + i * 2 * Math.PI / n; const l = el.dataset.label.length > 12 ? `${el.dataset.label.slice(0, 11)}…` : el.dataset.label;
            return `<text x="${(C + (R + 20) * Math.cos(a)).toFixed(0)}" y="${(C + (R + 20) * Math.sin(a) + 4).toFixed(0)}" text-anchor="middle" font-size="11" fill="#5B6660">${esc(l)}</text>`; }).join('');
        svg.innerHTML = `${small ? '' : `<polygon points="${ring(0.33)}" fill="none" stroke="#EEF1ED"/><polygon points="${ring(0.66)}" fill="none" stroke="#EEF1ED"/>`}<polygon points="${ring(1)}" fill="none" stroke="#E3E8E2"/>${shape ? `<polygon points="${shape}" fill="#9BCB6B" fill-opacity=".45" stroke="#446A36" stroke-width="2"/>` : ''}${labels}`;
    }

    function lotCode() {
        const radio = $('[name="harvest_id"]:checked');
        if (radio) return radio.dataset.code;
        return $('.cf-fixed-lot strong')?.textContent || '';
    }

    function physText() {
        const parts = [checkedVal('beverage'), checkedVal('coffee_type'), checkedVal('sieve') && `peneira ${checkedVal('sieve')}`].filter(Boolean);
        return parts.join(' · ') || '—';
    }

    function refresh() {
        const score = usesCup() ? num($('[data-cf-score-input]').value) : null;
        const [label, tone] = band(score);
        const bandEl = $('[data-cf-band]');
        bandEl.textContent = label;
        bandEl.className = `cf-band is-${tone}`;
        const prev = $('[data-cf-prev-score]', document);
        prev.textContent = score != null ? fmt(score) : (checkedVal('coffee_type').replace('Tipo ', 'T') || '—');
        prev.className = `cl-score is-${tone}`;
        $('[data-cf-prev-lot]', document).textContent = lotCode() || 'Escolha o lote';
        $('[data-cf-prev-phys]', document).textContent = physText();
        $('[data-cf-prev-flavors]', document).innerHTML = usesCup() ? $$('[name="flavor"]:checked').map((el) => `<span>${esc(el.value)}</span>`).join('') : '';
        radar($('[data-cf-radar]'), false);
        radar($('[data-cf-prev-radar]', document), true);
    }

    function validate(n) {
        if (n === 1) {
            if (!$('[name="harvest_id"]:checked') && !$('input[type=hidden][name="harvest_id"]')) return 'Escolha o lote analisado.';
            const date = $('[name="analysis_date"]');
            if (!date.value) return 'Informe a data da análise.';
            if (date.max && date.value > date.max) return 'A data da análise não pode ser no futuro.';
        }
        if (n === 2) {
            const m = $('[name="moisture"]').value;
            if (m && (num(m) == null || num(m) < 0 || num(m) > 100)) return 'Umidade deve ficar entre 0 e 100%.';
            const b = $('[name="broca_percentage"]').value;
            if (b && (num(b) == null || num(b) < 0 || num(b) > 100)) return 'Broca deve ficar entre 0 e 100%.';
        }
        if (n === 3 && usesCup()) {
            const s = $('[data-cf-score-input]').value;
            if (s && (num(s) == null || num(s) < 0 || num(s) > 100)) return 'A nota final deve ficar entre 0 e 100.';
            const bad = $$('[data-cf-attr]').find((el) => el.value && (num(el.value) == null || num(el.value) < 0 || num(el.value) > scale));
            if (bad) return `A nota de ${bad.dataset.label.toLowerCase()} deve ficar entre 0 e ${scale}.`;
        }
        return '';
    }

    function review() {
        const files = [...$('[data-cf-report]').files].map((f) => f.name);
        const score = usesCup() ? num($('[data-cf-score-input]').value) : null;
        const date = $('[name="analysis_date"]').value.split('-').reverse().join('/');
        const kindLabel = $('[name="analysis_kind"]:checked')?.parentElement.querySelector('strong').textContent || '';
        const attrs = usesCup() ? $$('[data-cf-attr]').filter((el) => el.value) : [];
        $('[data-cf-review]').innerHTML = `
            <dl class="hf-review">
                <div><dt>Lote</dt><dd>${esc(lotCode())}</dd></div>
                <div><dt>Análise</dt><dd>${esc(date)} · ${esc(kindLabel)}</dd></div>
                <div><dt>Física</dt><dd>${esc(usesPhys() ? physText() : 'Não se aplica')}</dd></div>
                <div><dt>Prova de xícara</dt><dd>${score != null ? `${fmt(score)} pts · ${band(score)[0]}` : (usesCup() ? 'Sem nota final' : 'Não registrada')}</dd></div>
            </dl>
            ${attrs.length ? `<div class="vw-rev-card"><p class="vw-rev-head">Atributos (0 a ${scale})</p><div class="cf-rev-attrs">${attrs.map((el) => `<span>${esc(el.dataset.label)} <b>${esc(el.value)}</b></span>`).join('')}</div></div>` : ''}
            <div class="vw-rev-card"><p class="vw-rev-head">O que vai mudar</p>
                <div>• A qualidade passa a aparecer na ficha do lote, na lista de classificação e na hora de vender.</div>
                ${files.length ? `<div>• ${files.length} laudo${files.length > 1 ? 's' : ''} anexado${files.length > 1 ? 's' : ''}: ${files.map(esc).join(', ')}</div>` : ''}
            </div>`;
    }

    function error(msg) {
        const box = $('[data-vw-error]');
        box.hidden = !msg;
        box.textContent = msg || '';
    }

    function show(n) {
        step = n;
        const order = steps();
        $$('[data-vw-step]').forEach((el) => { el.hidden = Number(el.dataset.vwStep) !== step; });
        $$('[data-vw-step-tab]', document).forEach((el) => {
            const k = Number(el.dataset.vwStepTab);
            el.classList.toggle('is-done', order.indexOf(k) > -1 && order.indexOf(k) < order.indexOf(step));
            if (k === step) el.setAttribute('aria-current', 'step'); else el.removeAttribute('aria-current');
        });
        $('[data-vw-back]').hidden = step === 1;
        $('[data-vw-cancel]').hidden = step !== 1;
        $('[data-vw-next]').hidden = step === 4;
        $('[data-vw-submit]').hidden = step !== 4;
        $('[data-cf-skip]').hidden = !(step === 3 && kind() === 'ambos');
        if (step === 4) review();
        const heading = $(`[data-vw-step="${step}"] h2`);
        if (heading) { heading.setAttribute('tabindex', '-1'); heading.focus(); }
    }

    function next() {
        const msg = validate(step);
        if (msg) { error(msg); return; }
        error('');
        const order = steps();
        show(order[order.indexOf(step) + 1] || 4);
    }
    function back() {
        const order = steps();
        error('');
        show(order[order.indexOf(step) - 1] || 1);
    }

    $('[data-vw-next]').addEventListener('click', next);
    $('[data-vw-back]').addEventListener('click', back);
    $('[data-cf-skip]').addEventListener('click', () => { skipped = true; syncCup(); refresh(); show(4); });
    form.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-cf-step], [data-cf-score]');
        if (!btn) return;
        const input = btn.dataset.cfScore ? $('[data-cf-score-input]') : btn.parentElement.querySelector('[data-cf-attr]');
        const max = btn.dataset.cfScore ? 100 : scale;
        const delta = Number(btn.dataset.cfScore || btn.dataset.cfStep);
        const base = num(input.value) ?? (btn.dataset.cfScore ? 80 : scale * 0.7);
        input.value = String(Math.min(max, Math.max(0, Math.round((base + (input.value ? delta : 0)) * 100) / 100))).replace('.', ',');
        refresh();
    });
    form.addEventListener('change', (e) => {
        if (e.target.dataset.cfKind !== undefined) { skipped = false; syncCup(); }
        refresh();
    });
    form.addEventListener('input', refresh);
    $('[data-cf-flavor-new]').addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        const value = e.target.value.trim().replace(/\s+/g, ' ');
        if (!value) return;
        const exists = $$('[name="flavor"]').find((el) => el.value.toLowerCase() === value.toLowerCase());
        if (exists) { exists.checked = true; } else {
            const label = document.createElement('label');
            label.innerHTML = `<input type="checkbox" name="flavor" value="${esc(value)}" checked><span>${esc(value)}</span>`;
            e.target.parentElement.before(label);
        }
        e.target.value = '';
        refresh();
    });
    $('[data-cf-report]').addEventListener('change', (e) => {
        $('[data-cf-files]').innerHTML = [...e.target.files].map((f) => `<li>${esc(f.name)}</li>`).join('');
    });
    form.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && e.target.tagName === 'INPUT' && step < 4 && e.target.dataset.cfFlavorNew === undefined) { e.preventDefault(); next(); }
    });
    form.addEventListener('submit', (e) => {
        for (const n of [1, 2, 3]) { const msg = validate(n); if (msg) { e.preventDefault(); error(msg); return; } }
        const attrs = $$('[data-cf-attr]');
        if (attrs.every((el) => !el.value.trim())) attrs.forEach((el) => { el.disabled = true; });
        $('[data-vw-submit]').disabled = true;
    });
    syncCup();
    refresh();
})();
