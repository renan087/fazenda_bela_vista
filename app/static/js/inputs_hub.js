/* Insumos e máquinas — painéis "Registrar uso" e "Abastecer máquina".
   Fluxo: preencher (um ou vários) → conferir → confirmar. Nada é gravado antes do "Confirmar". */
(function () {
  'use strict';
  const dataEl = document.querySelector('[data-io-data]');
  if (!dataEl) return;
  const DATA = JSON.parse(dataEl.textContent || '{}');
  const ITEMS = DATA.items || [];
  const PLOTS = DATA.plots || [];
  const MACHINES = DATA.machines || [];
  const byId = (list) => Object.fromEntries(list.map((x) => [String(x.id), x]));
  const ITEM = byId(ITEMS);
  const PLOT = byId(PLOTS);
  const MACH = byId(MACHINES);
  const TODAY = DATA.today || new Date().toISOString().slice(0, 10);
  const INT_UNITS = ['un', 'un.', 'unid', 'unidade', 'par', 'pares', 'peça', 'peca', 'kit', 'cx', 'caixa', 'rolo'];
  const isIntUnit = (u) => INT_UNITS.includes(String(u || '').trim().toLowerCase());

  const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const num = (raw) => {
    let t = String(raw == null ? '' : raw).trim().replace(/\s/g, '');
    if (!t) return null;
    if (t.includes(',')) t = t.replace(/\./g, '').replace(',', '.');
    else if (/^\d{1,3}(\.\d{3})+$/.test(t)) t = t.replace(/\./g, '');
    const n = Number(t);
    return Number.isFinite(n) ? n : NaN;
  };
  const fmt = (n, places) => {
    const p = places != null ? places : (Math.abs(n - Math.round(n)) < 0.005 ? 0 : (Math.abs(n * 10 - Math.round(n * 10)) < 0.05 ? 1 : 2));
    return Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: p, maximumFractionDigits: p });
  };
  const qty = (n, unit) => `${fmt(n)}${unit ? ' ' + unit : ''}`;
  const dateBr = (iso) => (iso ? iso.split('-').reverse().slice(0, 2).join('/') : '');
  const options = (list, label, empty) =>
    (empty ? `<option value="">${esc(empty)}</option>` : '') + list.map((x) => `<option value="${x.id}">${esc(label(x))}</option>`).join('');
  const X_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>';

  function setupDrawer(dlg, kind) {
    const form = dlg.querySelector('[data-io-form]');
    const errorEl = dlg.querySelector('[data-io-error]');
    const rowsEl = dlg.querySelector('[data-io-rows]');
    const summaryEl = dlg.querySelector('[data-io-summary]');
    const one = (f) => dlg.querySelector(`[data-io-pane="one"] [data-f="${f}"]`);
    const draftKey = `sisfarm.io-draft.${kind}`;
    let mode = 'one';
    let rows = [];
    let fixBonus = null;
    if (!form || !rowsEl) return null;

    // ---------- fonte do combustível (abastecimento)
    const sourceEl = dlg.querySelector('[data-io-source]');
    const priceWrap = dlg.querySelector('[data-io-price-wrap]');
    const priceEl = dlg.querySelector('[data-io-price]');
    const whoEl = dlg.querySelector('[data-io-who]');
    const source = () => (sourceEl ? sourceEl.value : '');
    const tank = () => (source() && source() !== 'posto' ? ITEM[source()] : null);

    // ---------- campos do modo "um"
    if (kind === 'use') {
      one('input_id').innerHTML = options(ITEMS, (i) => `${i.name} · tem ${i.label}`, 'Escolha o item');
      one('plot_id').innerHTML = options(PLOTS, (p) => p.name, 'Sem setor (uso geral)');
    } else {
      one('asset_id').innerHTML = options(MACHINES, (m) => (m.meter ? `${m.name} · ${fmt(m.meter, 0)} ${m.unit}` : m.name), 'Escolha a máquina');
    }

    // ---------- cálculo
    function useLines() {
      if (mode === 'one') {
        return [{ input_id: one('input_id').value, plot_id: one('plot_id').value, date: one('date').value, quantity: one('quantity').value, notes: one('notes').value }];
      }
      return rows.filter((r) => r.input_id || r.quantity);
    }
    function fuelLines() {
      const base = { source: source() === 'posto' ? 'posto' : 'estoque', input_id: source() === 'posto' ? '' : source(), price: priceEl ? priceEl.value : '', responsible: whoEl ? whoEl.value : '' };
      if (mode === 'one') return [{ ...base, asset_id: one('asset_id').value, liters: one('liters').value, meter: one('meter').value, date: one('date').value }];
      return rows.filter((r) => r.asset_id || r.liters).map((r) => ({ ...base, ...r }));
    }
    function useBalances(lines) {
      const left = {};
      return lines.map((l) => {
        const item = ITEM[l.input_id];
        if (!item) return null;
        if (!(l.input_id in left)) left[l.input_id] = item.available;
        const q = num(l.quantity);
        if (q && q > 0) left[l.input_id] -= q;
        return { item, after: left[l.input_id] };
      });
    }
    function fuelRates(lines) {
      const last = {};
      return lines.map((l) => {
        const m = MACH[l.asset_id];
        if (!m) return null;
        const prev = l.asset_id in last ? last[l.asset_id] : m.meter;
        const meter = num(l.meter);
        const liters = num(l.liters);
        let rate = null;
        let back = false;
        if (meter != null && !Number.isNaN(meter)) {
          if (prev && meter < prev) back = true;
          else if (prev && meter > prev && liters > 0) rate = m.unit === 'km' ? `${fmt((meter - prev) / liters, 1)} km/L` : `${fmt(liters / (meter - prev), 1)} L/h`;
          last[l.asset_id] = Math.max(prev || 0, meter);
        }
        return { m, prev, rate, back, worked: meter != null && prev ? meter - prev : null };
      });
    }
    function itemSummary(lines) {
      const out = {};
      lines.forEach((l) => {
        const item = ITEM[l.input_id];
        const q = num(l.quantity);
        if (!item || !(q > 0)) return;
        out[l.input_id] = out[l.input_id] || { item, used: 0 };
        out[l.input_id].used += q;
      });
      return Object.values(out);
    }
    const toneFor = (item, after) => (after < 0 ? 'bad' : item.min && after < item.min ? 'warn' : '');

    // ---------- renderização
    function renderImpact() {
      const box = dlg.querySelector('[data-io-pane="one"] [data-io-impact]');
      if (!box) return;
      if (kind === 'use') {
        const item = ITEM[one('input_id').value];
        const hint = dlg.querySelector('[data-io-unit-hint]');
        if (hint) hint.textContent = item ? `(${item.unit})` : '';
        if (!item) { box.className = 'io-impact'; box.innerHTML = '<small>Escolha o item para ver como o estoque fica.</small>'; return; }
        const q = num(one('quantity').value) || 0;
        const after = item.available - q;
        const tone = toneFor(item, after);
        const plot = PLOT[one('plot_id').value];
        box.className = 'io-impact' + (tone ? ' is-' + tone : '');
        box.innerHTML = `<small>Depois de registrar</small><span>${esc(item.name)}: <span class="io-txt-muted">${qty(item.available, item.unit)}</span> → <b class="${tone ? 'is-' + tone : ''}">${qty(after, item.unit)}</b></span>` +
          (tone === 'bad' ? `<small class="io-txt-red">Falta ${qty(-after, item.unit)} no estoque. Lance a compra antes.</small>` :
            tone === 'warn' ? `<small class="io-txt-amber">Vai ficar abaixo do mínimo (${qty(item.min, item.unit)}). O aviso de compra aparece na Visão geral.</small>` :
              `<small>${plot ? 'Custo lançado no ' + esc(plot.name) + ' nesta safra.' : 'Sem setor: o custo fica como uso geral da fazenda.'}</small>`);
      } else {
        const m = MACH[one('asset_id').value];
        const label = dlg.querySelector('[data-io-meter-label]');
        const hint = dlg.querySelector('[data-io-meter-hint]');
        if (label) label.textContent = m && m.unit === 'km' ? 'Hodômetro agora (km)' : 'Horímetro agora (h)';
        if (hint) hint.textContent = m && m.meter ? `Último registro: ${fmt(m.meter, 0)} ${m.unit}` : 'Opcional, mas ajuda a calcular o consumo e avisar a revisão.';
        const lines = fuelLines();
        const [r] = fuelRates(lines);
        const liters = num(one('liters').value) || 0;
        const t = tank();
        const parts = [];
        if (r && r.back) parts.push(`<small class="io-txt-red">O ${m.unit === 'km' ? 'hodômetro' : 'horímetro'} está menor que o último registro (${fmt(r.prev, 0)} ${m.unit}).</small>`);
        if (r && r.rate) parts.push(`<span>Consumo: <b>${r.rate}</b> <small>(trabalhou ${fmt(r.worked)} ${m.unit})</small></span>`);
        if (t) {
          const after = t.available - liters;
          const tone = toneFor(t, after);
          parts.push(`<span>Tanque: <span class="io-txt-muted">${qty(t.available, t.unit)}</span> → <b class="${tone ? 'is-' + tone : ''}">${qty(after, t.unit)}</b></span>`);
          if (tone === 'bad') parts.push('<small class="io-txt-red">Não há combustível suficiente no estoque.</small>');
        }
        if (m && m.next && m.next.overdue) parts.push(`<small class="io-txt-red">${esc(m.next.description)}: ${esc(m.next.when)}.</small>`);
        box.className = 'io-impact' + (r && r.back ? ' is-bad' : '');
        box.innerHTML = parts.length ? parts.join('') : '<small>Escolha a máquina e informe os litros.</small>';
      }
    }

    function rowHtml(r, i) {
      if (kind === 'use') {
        return `<div class="io-trow is-use" data-row="${i}">
          <input class="io-cell" type="date" data-r="date" value="${esc(r.date)}" aria-label="Data da linha ${i + 1}">
          <select class="io-cell" data-r="input_id" aria-label="Item da linha ${i + 1}">${options(ITEMS, (x) => x.name, 'Item')}</select>
          <select class="io-cell" data-r="plot_id" aria-label="Setor da linha ${i + 1}">${options(PLOTS, (x) => x.name, 'Sem setor')}</select>
          <input class="io-cell" type="text" inputmode="decimal" data-r="quantity" value="${esc(r.quantity)}" placeholder="0" aria-label="Quantidade da linha ${i + 1}">
          <span class="io-after" data-after></span>
          <button type="button" class="io-x" data-remove aria-label="Remover linha ${i + 1}">${X_ICON}</button></div>`;
      }
      return `<div class="io-trow is-fuel" data-row="${i}">
        <input class="io-cell" type="date" data-r="date" value="${esc(r.date)}" aria-label="Data da linha ${i + 1}">
        <select class="io-cell" data-r="asset_id" aria-label="Máquina da linha ${i + 1}">${options(MACHINES, (x) => x.name, 'Máquina')}</select>
        <input class="io-cell" type="text" inputmode="decimal" data-r="liters" value="${esc(r.liters)}" placeholder="L" aria-label="Litros da linha ${i + 1}">
        <input class="io-cell" type="text" inputmode="decimal" data-r="meter" value="${esc(r.meter)}" placeholder="h ou km" aria-label="Horímetro da linha ${i + 1}">
        <span class="io-after" data-after></span>
        <button type="button" class="io-x" data-remove aria-label="Remover linha ${i + 1}">${X_ICON}</button></div>`;
    }
    function renderRows() {
      rowsEl.innerHTML = rows.map(rowHtml).join('');
      rows.forEach((r, i) => {
        const el = rowsEl.querySelector(`[data-row="${i}"]`);
        el.querySelectorAll('select[data-r]').forEach((s) => { s.value = r[s.dataset.r] || ''; });
      });
      updateRows();
    }
    function updateRows() {
      if (kind === 'use') {
        const bal = useBalances(rows);
        rows.forEach((r, i) => {
          const cell = rowsEl.querySelector(`[data-row="${i}"] [data-after]`);
          const b = bal[i];
          if (!cell) return;
          if (!b) { cell.innerHTML = ''; return; }
          const tone = toneFor(b.item, b.after);
          cell.className = 'io-after ' + (tone === 'bad' ? 'io-txt-red' : tone === 'warn' ? 'io-txt-amber' : '');
          cell.innerHTML = `${qty(b.after, b.item.unit)}${tone === 'bad' ? '<small>falta estoque</small>' : tone === 'warn' ? '<small>abaixo do mínimo</small>' : ''}`;
        });
        const sum = itemSummary(rows);
        summaryEl.innerHTML = sum.length ? '<div>Resumo por item</div>' + sum.map((s) => {
          const after = s.item.available - s.used;
          const tone = toneFor(s.item, after);
          return `<div><strong>${esc(s.item.name)}</strong><span><span class="io-txt-muted">${qty(s.item.available, s.item.unit)}</span> → <b class="${tone === 'bad' ? 'io-txt-red' : tone === 'warn' ? 'io-txt-amber' : ''}">${qty(after, s.item.unit)}</b></span></div>`;
        }).join('') : '';
      } else {
        const rates = fuelRates(rows);
        rows.forEach((r, i) => {
          const cell = rowsEl.querySelector(`[data-row="${i}"] [data-after]`);
          const x = rates[i];
          if (!cell) return;
          cell.className = 'io-after ' + (x && x.back ? 'io-txt-red' : '');
          cell.innerHTML = !x ? '' : x.back ? `menor que ${fmt(x.prev, 0)}<small>confira o número</small>` : (x.rate || '<small class="io-txt-muted">sem horímetro anterior</small>');
        });
        const total = rows.reduce((s, r) => s + (num(r.liters) || 0), 0);
        const t = tank();
        summaryEl.innerHTML = total ? `<div>Resumo</div><div><strong>Total</strong><span>${qty(total, 'L')}</span></div>` +
          (t ? `<div><strong>${esc(t.name)}</strong><span><span class="io-txt-muted">${qty(t.available, t.unit)}</span> → <b class="${toneFor(t, t.available - total) === 'bad' ? 'io-txt-red' : ''}">${qty(t.available - total, t.unit)}</b></span></div>` : '') : '';
      }
    }
    function blankRow() {
      const last = rows[rows.length - 1];
      return kind === 'use' ? { date: last ? last.date : TODAY, input_id: '', plot_id: last ? last.plot_id : '', quantity: '' } : { date: last ? last.date : TODAY, asset_id: '', liters: '', meter: '' };
    }

    function setMode(next) {
      mode = next;
      dlg.classList.toggle('is-wide', mode === 'many');
      dlg.querySelectorAll('[data-io-mode]').forEach((b) => {
        const on = b.dataset.ioMode === mode;
        b.classList.toggle('is-on', on);
        b.setAttribute('aria-selected', on ? 'true' : 'false');
      });
      dlg.querySelectorAll('[data-io-pane]').forEach((p) => p.classList.toggle('io-hidden', p.dataset.ioPane !== mode));
      if (mode === 'many' && !rows.length) {
        const first = blankRow();
        if (kind === 'use') { first.input_id = one('input_id').value; first.plot_id = one('plot_id').value; first.quantity = one('quantity').value; first.date = one('date').value || TODAY; }
        else { first.asset_id = one('asset_id').value; first.liters = one('liters').value; first.meter = one('meter').value; first.date = one('date').value || TODAY; }
        rows = [first, blankRow(), blankRow()];
        renderRows();
      }
      showError('');
    }
    function setStep(step) {
      dlg.querySelectorAll('[data-io-step]').forEach((el) => el.classList.toggle('io-hidden', el.dataset.ioStep !== step));
      if (step === 'edit') dlg.querySelectorAll('[data-io-pane]').forEach((p) => p.classList.toggle('io-hidden', p.dataset.ioPane !== mode));
      dlg.classList.toggle('is-wide', step === 'edit' && mode === 'many');
    }
    function showError(msg) {
      if (!errorEl) return;
      errorEl.textContent = msg;
      errorEl.classList.toggle('io-hidden', !msg);
    }

    // ---------- conferir
    function check() {
      const lines = kind === 'use' ? useLines() : fuelLines();
      if (!lines.length) return showError('Preencha pelo menos uma linha.');
      for (let i = 0; i < lines.length; i += 1) {
        const l = lines[i];
        const tag = lines.length > 1 ? `Linha ${i + 1}: ` : '';
        if (kind === 'use') {
          const q = num(l.quantity);
          if (!ITEM[l.input_id]) return showError(tag + 'escolha o item.');
          if (!(q > 0)) return showError(tag + 'informe uma quantidade maior que zero.');
          if (isIntUnit(ITEM[l.input_id].unit) && Math.abs(q - Math.round(q)) > 1e-9) return showError(tag + `${ITEM[l.input_id].unit} é contado em números inteiros.`);
        } else {
          const lt = num(l.liters);
          if (!MACH[l.asset_id]) return showError(tag + 'escolha a máquina.');
          if (!(lt > 0)) return showError(tag + 'informe quantos litros.');
          if (Number.isNaN(num(l.meter))) return showError(tag + 'o horímetro precisa ser um número.');
        }
      }
      if (kind === 'use') {
        const bad = useBalances(lines).findIndex((b) => b && b.after < 0);
        if (bad >= 0) return showError(`${lines.length > 1 ? 'Linha ' + (bad + 1) + ': ' : ''}não há estoque suficiente de ${lines[bad] && ITEM[lines[bad].input_id].name}.`);
      } else {
        const back = fuelRates(lines).findIndex((r) => r && r.back);
        if (back >= 0) return showError(`${lines.length > 1 ? 'Linha ' + (back + 1) + ': ' : ''}o horímetro está menor que o último registro.`);
        if (source() === '') return showError('Escolha de onde saiu o combustível.');
        const t = tank();
        const total = lines.reduce((s, l) => s + (num(l.liters) || 0), 0);
        if (t && total > t.available) return showError(`Não há combustível suficiente: tem ${qty(t.available, t.unit)} e os abastecimentos somam ${qty(total, 'L')}.`);
      }
      showError('');
      renderReview(lines);
      form.querySelector('[data-io-lines]').value = JSON.stringify(lines);
      setStep('review');
      return null;
    }
    function renderReview(lines) {
      const review = dlg.querySelector('[data-io-review]');
      const sumEl = dlg.querySelector('[data-io-review-summary]');
      if (kind === 'use') {
        review.innerHTML = lines.map((l) => {
          const item = ITEM[l.input_id];
          const plot = PLOT[l.plot_id];
          return `<div><strong>${qty(num(l.quantity), item.unit)} de ${esc(item.name)}</strong><span>${esc(plot ? plot.name : 'Sem setor')} · ${dateBr(l.date || TODAY)}</span></div>`;
        }).join('');
        sumEl.innerHTML = '<div>Como o estoque fica</div>' + itemSummary(lines).map((s) => {
          const after = s.item.available - s.used;
          const tone = toneFor(s.item, after);
          return `<div><strong>${esc(s.item.name)}</strong><span><span class="io-txt-muted">${qty(s.item.available, s.item.unit)}</span> → <b class="${tone === 'warn' ? 'io-txt-amber' : ''}">${qty(after, s.item.unit)}</b></span></div>`;
        }).join('');
      } else {
        const rates = fuelRates(lines);
        review.innerHTML = lines.map((l, i) => {
          const m = MACH[l.asset_id];
          const extra = [num(l.meter) != null ? `${fmt(num(l.meter), 0)} ${m.unit}` : 'sem horímetro', rates[i] && rates[i].rate].filter(Boolean).join(' · ');
          return `<div><strong>${qty(num(l.liters), 'L')} · ${esc(m.name)}</strong><span>${esc(extra)} · ${dateBr(l.date || TODAY)}</span></div>`;
        }).join('');
        const total = lines.reduce((s, l) => s + (num(l.liters) || 0), 0);
        const t = tank();
        const price = num(priceEl && priceEl.value);
        sumEl.innerHTML = `<div>Resumo</div><div><strong>Total abastecido</strong><span>${qty(total, 'L')}</span></div>` +
          (t ? `<div><strong>${esc(t.name)}</strong><span><span class="io-txt-muted">${qty(t.available, t.unit)}</span> → <b>${qty(t.available - total, t.unit)}</b></span></div>`
            : `<div><strong>Pago no posto</strong><span>${price > 0 ? 'R$ ' + fmt(price * total, 2) : 'sem valor informado'}</span></div>`);
      }
    }

    // ---------- rascunho
    function saveDraft() {
      const payload = { mode, rows, one: kind === 'use' ? useLines()[0] : fuelLines()[0], source: source(), price: priceEl ? priceEl.value : '', who: whoEl ? whoEl.value : '' };
      try { localStorage.setItem(draftKey, JSON.stringify(payload)); } catch (e) { /* sem armazenamento: segue sem rascunho */ }
      showError('');
      const btn = dlg.querySelector('[data-io-draft]');
      if (btn) { btn.textContent = 'Rascunho salvo'; setTimeout(() => { btn.textContent = 'Salvar rascunho'; }, 1800); }
    }
    function loadDraft() {
      let d = null;
      try { d = JSON.parse(localStorage.getItem(draftKey) || 'null'); } catch (e) { d = null; }
      if (!d) return false;
      if (sourceEl && d.source && [...sourceEl.options].some((o) => o.value === d.source)) sourceEl.value = d.source;
      if (priceEl) priceEl.value = d.price || '';
      if (whoEl) whoEl.value = d.who || '';
      Object.entries(d.one || {}).forEach(([k, v]) => { const el = one(k); if (el && v != null) el.value = v; });
      rows = Array.isArray(d.rows) ? d.rows : [];
      if (d.mode === 'many' && rows.length) { setMode('many'); renderRows(); }
      return true;
    }

    // ---------- eventos
    dlg.querySelectorAll('[data-io-mode]').forEach((b) => b.addEventListener('click', () => setMode(b.dataset.ioMode)));
    dlg.querySelectorAll('[data-io-close]').forEach((b) => b.addEventListener('click', () => dlg.close()));
    dlg.addEventListener('close', () => { if (fixBonus) { fixBonus.item.available -= fixBonus.qty; fixBonus = null; } });
    dlg.addEventListener('click', (e) => { if (e.target === dlg) dlg.close(); });
    const intOnly = (input, unit) => { if (isIntUnit(unit) && /[^\d]/.test(input.value)) input.value = input.value.replace(/[^\d]/g, ''); };
    dlg.addEventListener('input', (e) => {
      if (kind !== 'use') return;
      if (e.target.matches('[data-io-pane="one"] [data-f="quantity"]')) { const it = ITEM[one('input_id').value]; if (it) intOnly(e.target, it.unit); }
      if (e.target.matches('[data-r="quantity"]')) { const r = rows[Number(e.target.closest('[data-row]').dataset.row)]; const it = r && ITEM[r.input_id]; if (it) intOnly(e.target, it.unit); }
    }, true);
    dlg.querySelector('[data-io-pane="one"]') && dlg.querySelector('[data-io-pane="one"]').addEventListener('input', renderImpact);
    dlg.querySelector('[data-io-pane="one"]') && dlg.querySelector('[data-io-pane="one"]').addEventListener('change', renderImpact);
    const onRowEdit = (e) => {
      const rowEl = e.target.closest('[data-row]');
      if (!rowEl || !e.target.dataset.r) return;
      rows[Number(rowEl.dataset.row)][e.target.dataset.r] = e.target.value;
      updateRows();
    };
    rowsEl.addEventListener('input', onRowEdit);
    rowsEl.addEventListener('change', onRowEdit);
    rowsEl.addEventListener('keydown', (e) => {
      const rowEl = e.target.closest('[data-row]');
      if (e.key === 'Enter' && rowEl && Number(rowEl.dataset.row) === rows.length - 1 && e.target.tagName === 'INPUT') {
        e.preventDefault();
        rows.push(blankRow());
        renderRows();
        const first = rowsEl.querySelector(`[data-row="${rows.length - 1}"] select`);
        if (first) first.focus();
      }
    });
    rowsEl.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-remove]');
      if (!btn) return;
      rows.splice(Number(btn.closest('[data-row]').dataset.row), 1);
      if (!rows.length) rows.push(blankRow());
      renderRows();
    });
    const addBtn = dlg.querySelector('[data-io-add]');
    if (addBtn) addBtn.addEventListener('click', () => { rows.push(blankRow()); renderRows(); });
    const checkBtn = dlg.querySelector('[data-io-check]');
    if (checkBtn) checkBtn.addEventListener('click', check);
    const backBtn = dlg.querySelector('[data-io-back]');
    if (backBtn) backBtn.addEventListener('click', () => setStep('edit'));
    const draftBtn = dlg.querySelector('[data-io-draft]');
    if (draftBtn) draftBtn.addEventListener('click', saveDraft);
    if (sourceEl) sourceEl.addEventListener('change', () => { if (priceWrap) priceWrap.classList.toggle('io-hidden', source() !== 'posto'); renderImpact(); updateRows(); });
    form.addEventListener('submit', (e) => {
      if (!form.querySelector('[data-io-lines]').value) { e.preventDefault(); check(); return; }
      try { localStorage.removeItem(draftKey); } catch (err) { /* ignora */ }
      const btn = form.querySelector('[data-io-confirm]');
      if (btn) { btn.disabled = true; btn.textContent = 'Gravando…'; }
    });

    return {
      open(opts) {
        form.querySelector('[data-io-lines]').value = '';
        const replace = form.querySelector('[data-io-replace]');
        if (fixBonus) { fixBonus.item.available -= fixBonus.qty; fixBonus = null; }
        if (replace) replace.value = opts.fix ? String(opts.fix.output) : '';
        const title = dlg.querySelector('#io-use-title');
        if (title) title.textContent = opts.fix ? 'Corrigir uso' : 'Registrar uso';
        dlg.querySelectorAll('[data-io-mode="many"]').forEach((b) => { b.disabled = !!opts.fix; });
        setStep('edit');
        showError('');
        rows = [];
        if (one('date')) one('date').value = TODAY;
        const restored = !opts.item && !opts.machine && !opts.fix && loadDraft();
        if (opts.fix && kind === 'use') {
          const item = ITEM[String(opts.fix.item)];
          const q = num(opts.fix.qty) || 0;
          if (item) { item.available += q; fixBonus = { item, qty: q }; }
          setMode('one');
          one('input_id').value = String(opts.fix.item);
          one('plot_id').value = opts.fix.plot ? String(opts.fix.plot) : '';
          one('date').value = opts.fix.date || TODAY;
          one('quantity').value = opts.fix.qty || '';
          one('notes').value = opts.fix.notes || '';
        }
        if (!restored) setMode(opts.mode === 'many' ? 'many' : 'one');
        if (opts.item && one('input_id')) one('input_id').value = opts.item;
        if (opts.machine && one('asset_id')) one('asset_id').value = opts.machine;
        if (priceWrap) priceWrap.classList.toggle('io-hidden', source() !== 'posto');
        renderImpact();
        if (typeof dlg.showModal === 'function') dlg.showModal(); else dlg.setAttribute('open', '');
        const focus = mode === 'many' ? rowsEl.querySelector('select') : dlg.querySelector('[data-io-pane="one"] select');
        if (focus) focus.focus();
      },
    };
  }

  // ---------- filtros (Estoque e Máquinas): chips por tipo + busca
  document.querySelectorAll('[data-io-filter-root]').forEach((root) => {
    const chips = Array.from(root.querySelectorAll('[data-io-chip]'));
    const search = root.querySelector('[data-io-search]');
    const items = Array.from(root.querySelectorAll('[data-io-fitem]'));
    const empty = root.querySelector('[data-io-fempty]');
    let group = 'todos';
    const apply = () => {
      const q = (search ? search.value : '').trim().toLowerCase();
      let shown = 0;
      items.forEach((el) => {
        const ok = (group === 'todos' || el.dataset.group === group) && (!q || (el.dataset.search || '').includes(q));
        el.classList.toggle('io-hidden', !ok);
        if (ok) shown += 1;
      });
      if (empty) empty.classList.toggle('io-hidden', shown > 0);
    };
    chips.forEach((chip) => chip.addEventListener('click', () => {
      group = chip.dataset.ioChip;
      chips.forEach((c) => { c.classList.toggle('is-on', c === chip); c.setAttribute('aria-pressed', c === chip ? 'true' : 'false'); });
      apply();
    }));
    if (search) search.addEventListener('input', apply);
  });

  // ---------- Estoque: escolher o item mostra o painel ao lado (embaixo no celular)
  document.addEventListener('click', (e) => {
    const row = e.target.closest('[data-io-stock-row]');
    if (!row) return;
    const id = row.dataset.ioStockRow;
    document.querySelectorAll('[data-io-stock-row]').forEach((r) => { const on = r === row; r.classList.toggle('is-on', on); r.setAttribute('aria-pressed', on ? 'true' : 'false'); });
    let panel = null;
    document.querySelectorAll('[data-io-stock-panel]').forEach((p) => { const on = p.dataset.ioStockPanel === id; p.classList.toggle('io-hidden', !on); if (on) panel = p; });
    try { const u = new URL(window.location.href); u.searchParams.set('item', id); window.history.replaceState(null, '', u); } catch (err) { /* ignora */ }
    if (panel && window.matchMedia('(max-width: 1023px)').matches) panel.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' });
  });

  // ---------- Ficha da máquina: filtro do histórico
  document.querySelectorAll('[data-io-filter-scope]').forEach((scope) => {
    const buttons = Array.from(scope.querySelectorAll('[data-io-filter]'));
    buttons.forEach((b) => b.addEventListener('click', () => {
      const kind = b.dataset.ioFilter;
      buttons.forEach((x) => { x.classList.toggle('is-on', x === b); x.setAttribute('aria-pressed', x === b ? 'true' : 'false'); });
      scope.querySelectorAll('[data-io-kind]').forEach((el) => el.classList.toggle('io-hidden', kind !== 'all' && el.dataset.ioKind !== kind));
    }));
  });

  // ---------- Movimentações: detalhe num painel ao lado, com desfazer e corrigir
  const movDialog = document.querySelector('[data-io-mov-dialog]');
  if (movDialog) {
    const body = movDialog.querySelector('[data-io-mov-body]');
    const openMov = (id) => {
      const tpl = document.getElementById(`io-mov-${id}`);
      if (!tpl) return;
      body.innerHTML = '';
      body.appendChild(tpl.content.cloneNode(true));
      if (typeof movDialog.showModal === 'function') movDialog.showModal(); else movDialog.setAttribute('open', '');
      const close = body.querySelector('[data-io-mov-close]');
      if (close) close.focus();
    };
    document.addEventListener('click', (e) => {
      const row = e.target.closest('[data-io-mov]');
      if (row) { openMov(row.dataset.ioMov); return; }
      if (e.target.closest('[data-io-mov-close]')) { movDialog.close(); return; }
      if (e.target === movDialog) { movDialog.close(); return; }
      if (e.target.closest('[data-io-undo-ask]')) {
        const box = body.querySelector('[data-io-undo-box]');
        if (box) { box.classList.remove('io-hidden'); box.scrollIntoView({ block: 'nearest' }); }
        e.target.closest('[data-io-undo-ask]').classList.add('io-hidden');
      }
      if (e.target.closest('[data-io-undo-cancel]')) {
        const box = body.querySelector('[data-io-undo-box]');
        if (box) box.classList.add('io-hidden');
        const ask = body.querySelector('[data-io-undo-ask]');
        if (ask) ask.classList.remove('io-hidden');
      }
    });
    const auto = document.querySelector('[data-io-mov-autoopen]');
    if (auto) openMov(auto.dataset.ioMovAutoopen);
  }

  // ---------- Contagem do galpão: diferença na hora, filtros e conferência
  const countForm = document.querySelector('[data-io-count]');
  if (countForm) {
    const rowsC = Array.from(countForm.querySelectorAll('[data-io-crow]'));
    const money = (n) => `R$ ${fmt(Math.abs(n), 2)}`;
    let filter = 'all';
    const calc = () => {
      let done = 0; let ok = 0; let diff = 0; let missing = 0; let extra = 0;
      rowsC.forEach((row) => {
        const input = row.querySelector('input');
        const cell = row.querySelector('[data-io-cdiff]');
        const counted = num(input.value);
        const avail = Number(row.dataset.available) || 0;
        const cost = Number(row.dataset.cost) || 0;
        const unit = row.dataset.unit;
        input.classList.toggle('is-done', input.value.trim() !== '');
        let status = 'todo';
        if (input.value.trim() === '') { cell.textContent = 'falta contar'; cell.className = 'io-count-diff io-txt-muted'; }
        else if (Number.isNaN(counted) || counted < 0) { cell.textContent = 'número inválido'; cell.className = 'io-count-diff io-txt-red'; status = 'diff'; done += 1; diff += 1; }
        else {
          done += 1;
          const d = Math.round((counted - avail) * 100) / 100;
          if (Math.abs(d) < 0.005) { status = 'ok'; ok += 1; cell.textContent = 'bateu'; cell.className = 'io-count-diff io-txt-green'; }
          else {
            status = 'diff'; diff += 1;
            if (d < 0) missing += -d * cost; else extra += d * cost;
            cell.textContent = `${d > 0 ? '+' : '−'} ${qty(Math.abs(d), unit)} · ${d > 0 ? '+' : '−'} ${money(d * cost)}`;
            cell.className = 'io-count-diff io-txt-red';
          }
        }
        row.dataset.status = status;
        row.classList.toggle('io-hidden', filter !== 'all' && status !== filter);
      });
      const total = rowsC.length;
      const pct = total ? Math.round((done / total) * 100) : 0;
      const set = (sel, text) => { const el = document.querySelector(sel); if (el) el.textContent = text; };
      set('[data-io-count-done]', `${done} de ${total} itens contados`);
      set('[data-io-count-pct]', `${pct}%`);
      const bar = document.querySelector('[data-io-count-bar]');
      if (bar) bar.style.width = `${pct}%`;
      set('[data-io-sum-done]', `${done} de ${total}`);
      set('[data-io-sum-ok]', String(ok));
      set('[data-io-sum-diff]', String(diff));
      set('[data-io-sum-missing]', `− ${money(missing)}`);
      set('[data-io-sum-extra]', `+ ${money(extra)}`);
      return { done, diff };
    };
    // Máscara de centavos: 1 → 0,01 · 10 → 0,10 · 12345 → 123,45 (máximo 9.999.999,99)
    const MAX_DIGITS = 9;
    const maskCents = (input) => {
      const intUnit = isIntUnit(input.closest('[data-io-crow]').dataset.unit);
      const digits = input.value.replace(/\D/g, '').replace(/^0+/, '').slice(0, intUnit ? MAX_DIGITS - 2 : MAX_DIGITS);
      if (!digits) { input.value = ''; return; }
      if (intUnit) { input.value = Number(digits).toLocaleString('pt-BR'); return; }
      const padded = digits.padStart(3, '0');
      const whole = Number(padded.slice(0, -2)).toLocaleString('pt-BR');
      input.value = `${whole},${padded.slice(-2)}`;
    };
    rowsC.forEach((row) => {
      const input = row.querySelector('input');
      input.setAttribute('inputmode', 'numeric');
      input.setAttribute('maxlength', '12');
      const start = num(input.value);
      const whole = isIntUnit(row.dataset.unit);
      if (input.value.trim() && !Number.isNaN(start)) {
        input.value = whole ? Math.round(Math.min(start, 9999999)).toLocaleString('pt-BR')
          : Math.min(start, 9999999.99).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      } else input.value = '';
    });
    countForm.addEventListener('input', (e) => {
      if (!e.target.closest('[data-io-crow]')) return;
      maskCents(e.target);
      calc();
    });
    countForm.querySelectorAll('[data-io-count-filter]').forEach((b) => b.addEventListener('click', () => {
      filter = b.dataset.ioCountFilter;
      countForm.querySelectorAll('[data-io-count-filter]').forEach((x) => { x.classList.toggle('is-on', x === b); x.setAttribute('aria-pressed', x === b ? 'true' : 'false'); });
      calc();
    }));
    const dialog = countForm.querySelector('[data-io-count-dialog]');
    const check = countForm.querySelector('[data-io-count-check]');
    if (check && dialog) check.addEventListener('click', () => {
      const r = calc();
      const review = dialog.querySelector('[data-io-count-review]');
      const bad = rowsC.some((row) => { const v = row.querySelector('input').value.trim(); return v !== '' && (Number.isNaN(num(v)) || num(v) < 0); });
      if (!r.done || bad) {
        review.innerHTML = `<div><strong>${bad ? 'Há quantidades inválidas. Use só números.' : 'Nenhum item foi contado ainda.'}</strong></div>`;
        dialog.querySelector('[value="aplicar"]').disabled = true;
      } else {
        dialog.querySelector('[value="aplicar"]').disabled = false;
        const diffs = rowsC.filter((row) => row.dataset.status === 'diff');
        review.innerHTML = `<div><span>Itens contados</span><strong>${r.done}</strong></div>` +
          (diffs.length ? diffs.map((row) => `<div><span>${row.querySelector('strong').textContent}</span><strong>${row.querySelector('[data-io-cdiff]').textContent}</strong></div>`).join('')
            : '<div><span>Diferenças</span><strong class="io-txt-green">tudo bateu</strong></div>');
      }
      dialog.showModal();
    });
    if (dialog) dialog.querySelector('[data-io-count-cancel]').addEventListener('click', () => dialog.close());
    calc();
  }
  document.querySelectorAll('[data-io-autosubmit]').forEach((el) => el.addEventListener('change', () => el.form && el.form.submit()));
  document.querySelectorAll('[data-io-print]').forEach((b) => b.addEventListener('click', () => window.print()));

  const drawers = {};
  document.querySelectorAll('[data-io-drawer]').forEach((dlg) => { drawers[dlg.dataset.ioDrawer] = setupDrawer(dlg, dlg.dataset.ioDrawer); });
  document.addEventListener('click', (e) => {
    const trigger = e.target.closest('[data-io-open]');
    if (!trigger) return;
    const drawer = drawers[trigger.dataset.ioOpen];
    if (!drawer) return;
    e.preventDefault();
    let fix = null;
    try { fix = trigger.dataset.ioFix ? JSON.parse(trigger.dataset.ioFix) : null; } catch (err) { fix = null; }
    const movDialog = trigger.closest('[data-io-mov-dialog]');
    if (movDialog) movDialog.close();
    drawer.open({ item: trigger.dataset.ioItem, machine: trigger.dataset.ioMachine, mode: trigger.dataset.ioModeOpen, fix });
  });
  const params = new URLSearchParams(window.location.search);
  if (params.get('contar') === '1') { const b = document.querySelector('[data-stock-inventory-open]'); if (b) window.setTimeout(() => b.click(), 50); }
  const auto = params.get('registrar');
  if (auto && drawers[auto === 'abastecimento' ? 'fuel' : auto === 'uso' ? 'use' : '']) {
    drawers[auto === 'abastecimento' ? 'fuel' : 'use'].open({ item: params.get('item'), machine: params.get('maquina') });
  }
}());
