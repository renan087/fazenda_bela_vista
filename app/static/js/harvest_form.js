/* Registrar colheita em 3 passos: origem → quantidade → conferir. Estimativa de sacas ao vivo. */
(() => {
    const form = document.querySelector('[data-harvest-wizard]');
    if (!form) return;
    const $ = (s, r = form) => r.querySelector(s);
    const $$ = (s, r = form) => [...r.querySelectorAll(s)];
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const parse = (v) => {
        let t = String(v ?? '').replace(/[^\d.,-]/g, '');
        if (t.includes(',')) t = t.replace(/\./g, '').replace(',', '.');
        else if (/^-?\d{1,3}(\.\d{3})+$/.test(t)) t = t.replace(/\./g, '');
        const n = parseFloat(t);
        return Number.isFinite(n) ? n : 0;
    };
    const fmt = (n, d = 0) => n.toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d });
    const checked = (name) => $(`[name=${name}]:checked`);
    const yieldInput = $('[data-hf-yield]');
    let step = 1;
    let lastUnit = checked('measure_unit')?.value;

    function data() {
        const unitEl = checked('measure_unit');
        const unit = unitEl?.value || 'sc_60';
        const qty = parse($('[data-hf-qty]').value);
        const factor = unit === 'sc_60' ? 1 : (parse(yieldInput.value) || parse(unitEl.dataset.yield));
        const sacks = factor ? Math.round((qty / factor) * 100) / 100 : 0;
        const plot = checked('plot_id');
        const area = plot ? parse(plot.dataset.area.replace('.', ',')) : 0;
        return { unit, unitEl, qty, factor, sacks, plot, area, perHa: area ? sacks / area : 0 };
    }

    function syncUnit() {
        const d = data();
        $('[data-hf-unit]').textContent = d.unitEl?.dataset.short || 'sc';
        $('[data-hf-yield-box]').hidden = d.unit === 'sc_60';
        $('[data-hf-yield-label]').textContent = d.unitEl?.dataset.yieldLabel || '';
        if (d.unit !== lastUnit || !yieldInput.value) {
            if (d.unit !== 'sc_60') yieldInput.value = fmt(parse(d.unitEl.dataset.yield));
            lastUnit = d.unit;
        }
        refresh();
    }

    function refresh() {
        const d = data();
        const est = $('[data-hf-estimate]');
        if (!d.qty) {
            est.textContent = '';
            est.hidden = true;
        } else {
            est.hidden = false;
            est.textContent = d.unit === 'sc_60'
                ? `Quantidade real: ${fmt(d.sacks, 2)} sacas de 60 kg. O lote já nasce pronto para vender.`
                : `Estimativa: ≈ ${fmt(d.sacks)} sacas de 60 kg beneficiadas${d.perHa ? ` · ${fmt(d.perHa, 1)} sc/ha` : ''}`;
        }
        const set = (k, v) => { const el = $(`[data-hf-sum="${k}"]`, document); if (el) el.textContent = v || '—'; };
        set('plot', d.plot?.dataset.name);
        set('type', checked('harvest_type')?.nextElementSibling.textContent);
        set('qty', d.qty ? `${fmt(d.qty)} ${d.unitEl?.dataset.short || 'sc'}` : '');
        set('sacks', d.qty ? `${d.unit === 'sc_60' ? '' : '≈ '}${fmt(d.sacks)} sc` : '');
        set('dest', checked('initial_destination')?.nextElementSibling.textContent);
    }

    function validate(n) {
        const d = data();
        if (n === 1) {
            if (!d.plot) return 'Escolha o talhão colhido.';
            const date = $('[name=harvest_date]');
            if (!date.value) return 'Informe a data da colheita.';
            if ((date.min && date.value < date.min) || (date.max && date.value > date.max)) return 'A data precisa estar dentro do período da safra ativa.';
        }
        if (n === 2) {
            if (d.qty <= 0) return 'Informe quanto foi colhido.';
            if (d.unit !== 'sc_60' && d.factor <= 0) return 'Informe o rendimento esperado.';
            for (const name of ['maturation_percentage', 'impurity_percentage', 'input_moisture_percentage']) {
                const el = $(`[name=${name}]`);
                if (el.value && (parse(el.value) < 0 || parse(el.value) > 100)) return 'Os percentuais devem ficar entre 0 e 100.';
            }
        }
        return '';
    }

    function review() {
        const d = data();
        const total = parse(form.dataset.total.replace('.', ','));
        const current = parse(form.dataset.current.replace('.', ','));
        const after = total - current + d.sacks;
        const date = $('[name=harvest_date]').value.split('-').reverse().join('/');
        const prefix = d.unit === 'sc_60' ? '' : '≈ ';
        $('[data-hf-review]').innerHTML = `
            <dl class="hf-review">
                <div><dt>Talhão</dt><dd>${esc(d.plot.dataset.name)}</dd></div>
                <div><dt>Data e tipo</dt><dd>${esc(date)} · ${esc(checked('harvest_type')?.nextElementSibling.textContent || '')}</dd></div>
                <div><dt>Quantidade</dt><dd>${fmt(d.qty)} ${esc(d.unitEl.dataset.short)}${d.unit === 'sc_60' ? '' : ` · ${prefix}${fmt(d.sacks)} sc`}</dd></div>
                <div><dt>Destino</dt><dd>${esc(checked('initial_destination')?.nextElementSibling.textContent || '')}</dd></div>
            </dl>
            <div class="vw-rev-card"><p class="vw-rev-head">O que vai mudar</p>
                ${form.dataset.current === '0' ? '<div class="vw-rev-row"><span>Novo lote</span><strong>criado ao confirmar</strong></div>' : ''}
                ${d.perHa ? `<div class="vw-rev-row"><span>Produtividade do lote</span><strong>${fmt(d.perHa, 1)} sc/ha</strong></div>` : ''}
                <div class="vw-rev-row"><span>Total colhido na safra</span><span>${fmt(total)} → <strong>${prefix}${fmt(after)} sc</strong></span></div>
            </div>
            ${d.unit === 'sc_60' ? '' : '<div class="pd-alert is-info">O volume fica como estimativa até você informar o benefício na ficha do lote.</div>'}`;
    }

    function error(msg) {
        const box = $('[data-vw-error]');
        box.hidden = !msg;
        box.textContent = msg || '';
    }

    function go(n) {
        if (n > step) {
            for (let s = step; s < n; s += 1) { const msg = validate(s); if (msg) { error(msg); return; } }
        }
        error('');
        step = n;
        $$('[data-vw-step]').forEach((el) => { el.hidden = Number(el.dataset.vwStep) !== step; });
        $$('[data-vw-step-tab]', document).forEach((el) => {
            const k = Number(el.dataset.vwStepTab);
            el.classList.toggle('is-done', k < step);
            if (k === step) el.setAttribute('aria-current', 'step'); else el.removeAttribute('aria-current');
        });
        $('[data-vw-back]').hidden = step === 1;
        $('[data-vw-cancel]').hidden = step !== 1;
        $('[data-vw-next]').hidden = step === 3;
        const submit = $('[data-vw-submit]');
        if (submit) submit.hidden = step !== 3;
        if (step === 3) review();
        const heading = $(`[data-vw-step="${step}"] h2, [data-vw-step="${step}"] legend`);
        if (heading) { heading.setAttribute('tabindex', '-1'); heading.focus(); }
    }

    $('[data-vw-next]').addEventListener('click', () => go(step + 1));
    $('[data-vw-back]').addEventListener('click', () => go(step - 1));
    form.addEventListener('change', (e) => { if (e.target.name === 'measure_unit') syncUnit(); else refresh(); });
    form.addEventListener('input', refresh);
    form.addEventListener('keydown', (e) => { if (e.key === 'Enter' && e.target.tagName === 'INPUT' && step < 3) { e.preventDefault(); go(step + 1); } });
    form.addEventListener('submit', (e) => {
        const msg = validate(1) || validate(2);
        if (msg) { e.preventDefault(); error(msg); return; }
        $('[data-vw-submit]').disabled = true;
    });
    syncUnit();
})();

/* Talhão fora da safra: pergunta ali mesmo se deve entrar na safra ativa. */
(function () {
  'use strict';
  const ask = document.querySelector('[data-hf-ask]');
  const flag = document.querySelector('[data-hf-include-flag]');
  if (!ask || !flag) return;
  const radios = document.querySelectorAll('input[name="plot_id"]');
  let pending = null;
  const toast = (text) => {
    const t = document.createElement('div');
    t.className = 'sv-toast'; t.setAttribute('role', 'status'); t.textContent = text;
    document.body.appendChild(t);
    setTimeout(() => { t.classList.add('is-out'); setTimeout(() => t.remove(), 260); }, 4000);
  };
  radios.forEach((r) => r.addEventListener('change', () => {
    if (r.dataset.out === '1' && flag.value !== r.value) {
      pending = r; flag.dataset.ok = '';
      ask.querySelector('[data-hf-plotname]').textContent = r.dataset.name;
      ask.hidden = false;
    } else if (r.dataset.out !== '1') {
      ask.hidden = true;
    }
  }));
  ask.querySelector('[data-hf-no]').addEventListener('click', () => {
    if (pending) pending.checked = false;
    pending = null; ask.hidden = true;
  });
  ask.querySelector('[data-hf-yes]').addEventListener('click', () => {
    if (!pending) return;
    flag.value = '1'; flag.dataset.plot = pending.value; ask.hidden = true;
    pending.closest('.hf-plot').classList.remove('is-out');
    const tag = pending.closest('.hf-plot').querySelector('.hf-out'); if (tag) tag.textContent = 'Entra na safra';
    toast(pending.dataset.name + ' será incluído na safra ao salvar');
    pending = null;
  });
  // Não deixa seguir com um talhão fora da safra sem a resposta.
  document.querySelectorAll('[data-vw-next], [data-vw-submit]').forEach((b) => b.addEventListener('click', (e) => {
    const chosen = document.querySelector('input[name="plot_id"]:checked');
    if (chosen && chosen.dataset.out === '1' && flag.dataset.plot !== chosen.value) {
      e.preventDefault(); e.stopImmediatePropagation(); ask.hidden = false;
      ask.querySelector('[data-hf-plotname]').textContent = chosen.dataset.name; pending = chosen;
    }
  }, true));
})();
