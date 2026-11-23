/* Compra em 4 etapas: Nota → Itens → Pagamento → Conferir.
   O rascunho é salvo sozinho neste navegador a cada mudança; nada vai para o servidor antes do "Confirmar compra". */
(function () {
  'use strict';
  const page = document.querySelector('[data-cw-page]');
  const dataEl = document.querySelector('[data-cw-data]');
  const form = document.querySelector('[data-cw-form]');
  if (!page || !dataEl || !form) return;
  const D = JSON.parse(dataEl.textContent || '{}');
  const DRAFT_KEY = 'sisfarm.compra.rascunho';
  const CATALOG = D.catalog || [];
  const TYPES = D.types || [];
  const TYPE = Object.fromEntries(TYPES.map((t) => [t.key, t]));
  const byName = (name) => CATALOG.find((c) => c.name.toLowerCase() === String(name || '').trim().toLowerCase());
  const $ = (sel, root) => (root || page).querySelector(sel);
  const $$ = (sel, root) => Array.from((root || page).querySelectorAll(sel));

  const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const plain = (v) => String(v || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
  const num = (raw) => {
    let t = String(raw == null ? '' : raw).replace(/R\$/g, '').trim().replace(/\s/g, '');
    if (!t) return null;
    if (t.includes(',')) t = t.replace(/\./g, '').replace(',', '.');
    else if (/^\d{1,3}(\.\d{3})+$/.test(t)) t = t.replace(/\./g, '');
    const n = Number(t);
    return Number.isFinite(n) ? n : NaN;
  };
  const fmt = (n, p) => Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: p == null ? 0 : p, maximumFractionDigits: p == null ? 2 : p });
  const money = (n) => `R$ ${fmt(n, 2)}`;
  const dateBr = (iso) => (iso ? iso.split('-').reverse().join('/') : '');
  const addMonths = (iso, k) => {
    const [y, m, d] = iso.split('-').map(Number);
    const t = new Date(Date.UTC(y, m - 1 + k, 1));
    const last = new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth() + 1, 0)).getUTCDate();
    t.setUTCDate(Math.min(d, last));
    return t.toISOString().slice(0, 10);
  };
  const traceable = (it) => it.item_type === 'insumo_agricola' && it.category !== 'Outros Custos de Insumos';
  const groupLabel = (it) => {
    const known = byName(it.name);
    if (known) return known.group_label;
    const t = TYPE[it.type_key] || TYPES.find((x) => x.item_type === it.item_type && x.categories.includes(it.category));
    return t ? t.group_label : 'escolha o tipo';
  };

  // ------------------------------------------------------------------ estado
  const blank = () => ({
    step: 1,
    note: { supplier: '', invoice: '', date: D.today, notes: '' },
    items: [],
    payment: { mode: 'vista', account_id: String((D.accounts.find((a) => a.default) || D.accounts[0] || {}).id || ''), method: (D.methods || [])[0] || 'PIX', count: '3', first_due: addMonths(D.today, 1), card_id: String((D.cards[0] || {}).id || ''), card_count: '1', installments: null },
    open: null,
  });
  const EDIT = D.edit || null;
  let S = blank();
  let fileName = '';
  if (EDIT) {
    S = { ...blank(), ...EDIT.state, note: { ...blank().note, ...EDIT.state.note }, payment: { ...blank().payment, ...EDIT.state.payment } };
    S.items.forEach((it) => { it.old_total = (num(it.packs) || 0) * (num(it.size) || 0); });
    const title = page.querySelector('.io-title');
    if (title) title.textContent = 'Corrigir compra';
    const sub = page.querySelector('[data-cw-sub]');
    if (sub) sub.textContent = 'Mude o que estiver errado. O que já foi usado do estoque fica como está.';
    const submit = page.querySelector('[data-cw-submit]');
    if (submit) submit.textContent = 'Salvar correção';
  } else try {
    const saved = JSON.parse(localStorage.getItem(DRAFT_KEY) || 'null');
    if (saved && saved.items && (saved.items.length || saved.note.supplier)) {
      S = { ...blank(), ...saved, note: { ...blank().note, ...saved.note }, payment: { ...blank().payment, ...saved.payment } };
      const restored = $('[data-cw-restored]');
      if (restored) restored.classList.remove('io-hidden');
    }
  } catch (e) { /* sem armazenamento: começa do zero */ }
  const save = () => { if (EDIT) return; try { localStorage.setItem(DRAFT_KEY, JSON.stringify(S)); } catch (e) { /* ignora */ } };

  const itemTotal = (it) => (num(it.packs) || 0) * (num(it.price) || 0);
  const total = () => Math.round(S.items.reduce((s, it) => s + itemTotal(it), 0) * 100) / 100;

  // ------------------------------------------------------------------ parcelas
  function buildParcels() {
    const n = Math.max(1, parseInt(S.payment.count, 10) || 1);
    const cents = Math.round(total() * 100);
    const base = Math.floor(cents / n);
    const rest = cents % n;
    return Array.from({ length: n }, (_, i) => ({ due_date: addMonths(S.payment.first_due || D.today, i), amount: fmt((base + (i < rest ? 1 : 0)) / 100, 2) }));
  }
  function parcels() {
    const n = Math.max(1, parseInt(S.payment.count, 10) || 1);
    if (!S.payment.installments || S.payment.installments.length !== n) S.payment.installments = buildParcels();
    return S.payment.installments;
  }
  const parcelSum = () => parcels().reduce((s, p) => s + (num(p.amount) || 0), 0);

  // ------------------------------------------------------------------ validação por etapa
  function problem(step) {
    if (step >= 1) {
      if (!S.note.supplier.trim()) return [1, 'Informe de quem você comprou.'];
      if (!S.note.date) return [1, 'Informe a data da compra.'];
    }
    if (step >= 2) {
      if (!S.items.length) return [2, 'Adicione pelo menos um item da nota.'];
      for (let i = 0; i < S.items.length; i += 1) {
        const it = S.items[i];
        const tag = S.items.length > 1 ? `Item ${i + 1} (${it.name}): ` : '';
        if (!it.item_type || !it.category) return [2, tag + 'escolha o tipo e a categoria do item novo.', i];
        if (!it.unit) return [2, tag + 'escolha a unidade.', i];
        if (!(num(it.packs) > 0)) return [2, tag + 'informe quantas embalagens vieram.', i];
        if (!(num(it.size) > 0)) return [2, tag + 'informe quanto tem cada embalagem.', i];
        if (!(num(it.price) >= 0) || it.price === '') return [2, tag + 'informe o preço por embalagem.', i];
        if (it.manufacture_date && it.expiration_date && it.manufacture_date > it.expiration_date) return [2, tag + 'a fabricação não pode ser depois da validade.', i];
        if (it.used && (num(it.packs) || 0) * (num(it.size) || 0) + 1e-9 < it.used) return [2, tag + `já saíram ${it.used_label} desta compra; a quantidade não pode ficar menor.`, i];
      }
    }
    if (step >= 3) {
      const p = S.payment;
      if (p.mode === 'prazo') {
        if (!p.account_id) return [3, 'Escolha a conta que vai pagar as parcelas.'];
        if (!p.first_due) return [3, 'Informe o primeiro vencimento.'];
        if (parcels().some((x) => !x.due_date || !(num(x.amount) >= 0))) return [3, 'Confira datas e valores das parcelas.'];
        if (Math.abs(parcelSum() - total()) > 0.01) return [3, `A soma das parcelas (${money(parcelSum())}) não bate com a nota (${money(total())}).`];
      }
      if (p.mode === 'cartao' && !p.card_id) return [3, 'Escolha o cartão.'];
    }
    return null;
  }
  function showError(msg) {
    const el = $('[data-cw-error]');
    el.textContent = msg || '';
    el.classList.toggle('io-hidden', !msg);
  }

  // ------------------------------------------------------------------ render
  // máscaras: embalagens inteiras; tamanho, preço e mínimo em centavos (100 → 1,00)
  const maskWhole = (raw) => { const d = String(raw || '').replace(/\D/g, '').replace(/^0+/, '').slice(0, 6); return d ? Number(d).toLocaleString('pt-BR') : ''; };
  const maskCents = (raw) => {
    const d = String(raw || '').replace(/\D/g, '').replace(/^0+/, '').slice(0, 9);
    if (!d) return '';
    const p = d.padStart(3, '0');
    return `${Number(p.slice(0, -2)).toLocaleString('pt-BR')},${p.slice(-2)}`;
  };
  const asCents = (v) => { const n = num(v); return n && !Number.isNaN(n) ? n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : ''; };
  const asWhole = (v) => { const n = num(v); return n && !Number.isNaN(n) ? Math.round(n).toLocaleString('pt-BR') : ''; };
  const MASKS = { packs: maskWhole, size: maskCents, price: maskCents, minimum: maskCents };
  S.items.forEach((it) => {
    it.packs = asWhole(it.packs); it.size = asCents(it.size); it.price = asCents(it.price); it.minimum = asCents(it.minimum);
  });
  const missing = (it) => ({ packs: !(num(it.packs) > 0), size: !(num(it.size) > 0), price: !(num(it.price) > 0), type: !it.item_type || !it.category });
  const isDone = (it) => { const m = missing(it); return !m.packs && !m.size && !m.price && !m.type; };
  const calcLine = (it) => {
    const p = num(it.packs) || 0; const sz = num(it.size) || 0; const pr = num(it.price) || 0;
    return {
      qty: p && sz ? `${fmt(p * sz)} ${it.unit || ''}` : '—',
      detail: isDone(it) ? `${money(p * pr)} · ${money(pr / sz)} por ${it.unit}` : 'complete a linha',
    };
  };

  function itemHtml(it, i) {
    const known = byName(it.name);
    const units = known ? [known.unit] : (((TYPE[it.type_key] || TYPES.find((t) => t.item_type === it.item_type && t.categories.includes(it.category))) || {}).units || {})[it.category] || [];
    const m = missing(it);
    const done = isDone(it);
    const c = calcLine(it);
    const typeChips = known ? '' : `<div class="cw-newtype"><span class="cw-lab">Item novo: que tipo é?${m.type ? ' <i>falta</i>' : ''}</span><div class="cw-types">${TYPES.map((t) => `<button type="button" class="cw-type${it.type_key === t.key ? ' is-on' : ''}" data-cw-type="${t.key}">${esc(t.label)}</button>`).join('')}</div>
      ${it.type_key ? `<div class="cw-seg cw-seg-sm"><label class="cw-cell"><span class="cw-lab">Categoria</span><select data-cw-i="category">${TYPE[it.type_key].categories.map((x) => `<option${x === it.category ? ' selected' : ''}>${esc(x)}</option>`).join('')}</select></label>
        <label class="cw-cell"><span class="cw-lab">Unidade</span><select data-cw-i="unit">${(units.length ? units : ['—']).map((u) => `<option${u === it.unit ? ' selected' : ''}>${esc(u)}</option>`).join('')}</select></label></div>` : ''}</div>`;
    const cell = (key, label, input, extraCls) => `<label class="cw-cell${m[key] ? ' is-miss' : ''}${extraCls || ''}"><span class="cw-lab">${label}${m[key] ? ' <i>falta</i>' : ''}</span>${input}</label>`;
    const trace = traceable(it);
    const extraLabel = it.open_extra ? `− Ocultar ${trace ? 'fabricação e aviso de mínimo' : 'aviso de mínimo'}` : `+ ${trace ? 'Fabricação e aviso de mínimo' : 'Aviso de mínimo'}`;
    const removeBtn = it.used
      ? `<span class="io-pill io-tone-amber" title="Já usado: não pode sair da nota">já usado ${esc(it.used_label)}</span>`
      : `<button type="button" class="io-btn is-ghost is-sm cw-remove" data-cw-remove aria-label="Remover ${esc(it.name)}">Remover</button>`;
    return `<article class="cw-item${done ? '' : ' is-todo'}" data-cw-index="${i}" aria-label="Item ${i + 1}: ${esc(it.name)}">
      <div class="cw-ihead">
        <span class="cw-num${done ? ' is-done' : ''}" data-cw-num>${done ? '✓' : i + 1}</span>
        <span class="cw-iname"><strong>${esc(it.name)}</strong><span class="cw-imeta"><span class="io-pill io-tone-green">vai para: ${esc(groupLabel(it))}</span>${known ? `tem ${esc(known.stock)} no estoque` : '<span class="io-pill io-tone-blue">item novo</span>'}</span></span>
        ${removeBtn}
      </div>
      ${typeChips}
      <div class="cw-seg">
        ${cell('packs', 'Embalagens', `<span class="cw-val"><input type="text" inputmode="numeric" data-cw-i="packs" value="${esc(it.packs)}" placeholder="0" autocomplete="off"><b>un.</b></span>`)}
        ${cell('size', 'Cada uma tem', `<span class="cw-val"><input type="text" inputmode="numeric" data-cw-i="size" value="${esc(it.size)}" placeholder="0,00" autocomplete="off"><b data-cw-unit>${esc(it.unit || '')}</b></span>`)}
        ${cell('price', 'Preço de cada', `<span class="cw-val"><b>R$</b><input type="text" inputmode="numeric" data-cw-i="price" value="${esc(it.price)}" placeholder="0,00" autocomplete="off"></span>`)}
        <div class="cw-cell is-out"><span class="cw-lab">Entra no estoque</span><strong data-cw-qty>${esc(c.qty)}</strong><small data-cw-detail>${esc(c.detail)}</small></div>
      </div>
      <div class="cw-extra">
        ${trace ? `<div class="cw-seg cw-seg-sm">
          <label class="cw-cell"><span class="cw-lab">Lote <i class="is-opt">opcional</i></span><input type="text" maxlength="120" data-cw-i="batch" value="${esc(it.batch)}" placeholder="Como está na embalagem"></label>
          <label class="cw-cell"><span class="cw-lab">Validade <i class="is-opt">opcional</i></span><input type="date" data-cw-i="expiration_date" value="${esc(it.expiration_date)}"></label>
          ${it.open_extra ? `<label class="cw-cell"><span class="cw-lab">Fabricação <i class="is-opt">opcional</i></span><input type="date" data-cw-i="manufacture_date" value="${esc(it.manufacture_date)}"></label>` : ''}
        </div>` : ''}
        ${it.open_extra ? `<div class="cw-seg cw-seg-sm cw-seg-min">
          <label class="cw-cell"><span class="cw-lab">Avisar quando tiver menos de <i class="is-opt">opcional</i></span><span class="cw-val"><input type="text" inputmode="numeric" data-cw-i="minimum" value="${esc(it.minimum)}" placeholder="0,00" autocomplete="off"><b>${esc(it.unit || '')}</b></span></label>
          <div class="cw-cell is-hint"><span>O aviso aparece na Visão geral quando o estoque de ${esc(it.name)} chegar nesse ponto.</span></div>
        </div>` : ''}
        <button type="button" class="cw-link-btn" data-cw-extra aria-expanded="${it.open_extra ? 'true' : 'false'}">${extraLabel}</button>
      </div>
    </article>`;
  }

  function renderTodo() {
    const todo = S.items.filter((it) => !isDone(it)).length;
    const box = $('[data-cw-todo]');
    if (box) {
      box.innerHTML = !S.items.length ? '' : todo
        ? `<button type="button" class="io-btn is-sm cw-todo-btn" data-cw-goto-missing>${todo === 1 ? '1 item falta completar' : `${todo} itens faltam completar`} · Ir para o que falta</button>`
        : '<span class="io-pill io-tone-green cw-alldone">✓ Todos os itens completos</span>';
    }
    const count = $('[data-cw-count]');
    if (count) count.textContent = S.items.length ? `${S.items.length} ${S.items.length === 1 ? 'item' : 'itens'} na nota${todo ? ` · ${todo} por completar` : ' · todos completos'}` : '';
    $('[data-cw-total]').textContent = money(total());
  }

  function renderItems() {
    const box = $('[data-cw-items]');
    box.innerHTML = S.items.length ? S.items.map(itemHtml).join('') : '<div class="io-empty" style="padding:18px"><strong>Nenhum item ainda</strong><span>Use a busca acima para adicionar o primeiro.</span></div>';
    renderTodo();
  }
  function refreshItemNumbers(i) {
    const card = $(`[data-cw-index="${i}"]`);
    if (!card) return;
    const it = S.items[i];
    const m = missing(it);
    const done = isDone(it);
    card.classList.toggle('is-todo', !done);
    ['packs', 'size', 'price'].forEach((k) => {
      const el = $(`[data-cw-i="${k}"]`, card);
      const cellEl = el && el.closest('.cw-cell');
      if (!cellEl) return;
      cellEl.classList.toggle('is-miss', m[k]);
      const lab = $('.cw-lab', cellEl);
      const flag = $('i', lab);
      if (m[k] && !flag) lab.insertAdjacentHTML('beforeend', ' <i>falta</i>');
      if (!m[k] && flag) flag.remove();
    });
    const badge = $('[data-cw-num]', card);
    if (badge) { badge.textContent = done ? '✓' : String(i + 1); badge.classList.toggle('is-done', done); }
    const c = calcLine(it);
    $('[data-cw-qty]', card).textContent = c.qty;
    $('[data-cw-detail]', card).textContent = c.detail;
    renderTodo();
  }
  function focusItem(i, selector) {
    const card = $(`[data-cw-index="${i}"]`);
    if (!card) return;
    const target = $(selector || '.cw-cell.is-miss input, .cw-type, [data-cw-i="packs"]', card) || $('[data-cw-i="packs"]', card);
    card.scrollIntoView({ block: 'center', behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
    if (target) setTimeout(() => target.focus({ preventScroll: true }), 120);
  }

  function renderPayment() {
    const p = S.payment;
    if (EDIT && EDIT.paid) {
      $$('[data-cw-mode], [data-cw-p], [data-cw-parc], [data-vr-open]').forEach((el) => { el.disabled = true; });
      const nofin = $('[data-cw-nofin]');
      nofin.textContent = `Esta nota já tem ${EDIT.paid} ${EDIT.paid === 1 ? 'parcela paga' : 'parcelas pagas'}: o pagamento e o total não podem mudar aqui. Para isso, estorne o pagamento no Financeiro.`;
      nofin.classList.remove('io-hidden');
    }
    $$('[data-cw-mode]').forEach((b) => { const on = b.dataset.cwMode === p.mode; b.classList.toggle('is-on', on); b.setAttribute('aria-checked', on ? 'true' : 'false'); });
    $$('[data-cw-pay]').forEach((el) => el.classList.toggle('io-hidden', !el.dataset.cwPay.split(' ').includes(p.mode)));
    $$('[data-cw-p]').forEach((el) => { if (p[el.dataset.cwP] != null && el.value !== String(p[el.dataset.cwP])) el.value = p[el.dataset.cwP]; });
    if (!(EDIT && EDIT.paid)) $('[data-cw-nofin]').classList.toggle('io-hidden', !(p.mode === 'vista' && !p.account_id));
    if (p.mode === 'prazo') {
      const rows = parcels();
      const diff = Math.abs(parcelSum() - total()) <= 0.01;
      $('[data-cw-parcels]').innerHTML = `<div class="cw-parc is-head"><span>Parcela</span><span>Vencimento</span><span>Valor (R$)</span></div>` +
        rows.map((r, i) => `<div class="cw-parc"><strong>${i + 1}ª</strong><input type="date" data-cw-parc="${i}" data-k="due_date" value="${esc(r.due_date)}" aria-label="Vencimento da parcela ${i + 1}"><input type="text" inputmode="decimal" data-cw-parc="${i}" data-k="amount" value="${esc(r.amount)}" aria-label="Valor da parcela ${i + 1}"></div>`).join('') +
        `<div class="cw-parc-foot"><span class="${diff ? 'io-txt-green' : 'io-txt-red'}" style="font-weight:600" data-cw-parc-status>${diff ? '✓ Soma das parcelas bate com a nota' : `A soma (${money(parcelSum())}) não bate com a nota`}</span><button type="button" class="cw-link" data-cw-reparc>Dividir igual de novo</button></div>`;
    }
    if (p.mode === 'cartao') {
      const card = D.cards.find((c) => String(c.id) === String(p.card_id));
      $('[data-cw-card-info]').innerHTML = card
        ? `<strong>${esc(card.name)}</strong><br>Fecha dia ${card.closing_day} · vence dia ${card.due_day}. ${p.card_count > 1 ? `${p.card_count} parcelas nas próximas faturas.` : 'Entra na próxima fatura.'}`
        : (D.cards.length ? 'Escolha o cartão.' : 'Cadastre um cartão ao lado para lançar a compra direto na fatura.');
    }
  }

  function payLabel() {
    const p = S.payment;
    if (p.mode === 'prazo') { const r = parcels(); return `${r.length}x · 1ª em ${dateBr(r[0].due_date)}`; }
    if (p.mode === 'cartao') return `Cartão · ${p.card_count}x`;
    return p.account_id ? `À vista · ${p.method}` : 'À vista · fora do Financeiro';
  }
  function renderSummary() {
    const groups = {};
    S.items.forEach((it) => { const g = groupLabel(it); groups[g] = (groups[g] || 0) + itemTotal(it); });
    $('[data-cw-summary]').innerHTML = '<strong style="font-size:16px;margin-bottom:6px">Resumo da nota</strong>' +
      `<div class="cw-sum"><span>Fornecedor</span><strong>${esc(S.note.supplier) || '<span class="io-txt-muted">—</span>'}</strong></div>` +
      `<div class="cw-sum"><span>Nota</span><strong>${esc([S.note.invoice, dateBr(S.note.date)].filter(Boolean).join(' · ')) || '—'}</strong></div>` +
      (S.items.length ? Object.entries(groups).map(([g, v]) => `<div class="cw-sum"><span>${esc(g)}</span><strong>${money(v)}</strong></div>`).join('') : '<div class="cw-sum"><span>Itens</span><strong class="io-txt-muted">ainda nenhum</strong></div>') +
      `<div class="cw-sum"><span>Pagamento</span><strong>${S.step >= 3 ? esc(payLabel()) : '—'}</strong></div>` +
      (fileName ? `<div class="cw-sum"><span>Anexo</span><strong>${esc(fileName)}</strong></div>` : '') +
      `<div class="cw-sum is-total"><span>Total</span><strong>${money(total())}</strong></div>` +
      '<p style="margin:10px 0 0;font-size:12px;color:#7A857E">Nada é gravado antes da etapa 4. O rascunho fica salvo neste aparelho se você sair no meio.</p>';
    const sub = $('[data-cw-sub]');
    if (sub && S.step > 1 && S.note.supplier) sub.textContent = [S.note.invoice && `Nota ${S.note.invoice}`, S.note.supplier, dateBr(S.note.date), S.items.length && `${S.items.length} ${S.items.length === 1 ? 'item' : 'itens'}`].filter(Boolean).join(' · ');
  }

  function renderReview() {
    $('[data-cw-review]').innerHTML = '<div class="cw-rrow is-head"><span>Item</span><span>Estoque</span><span>Custo</span><span style="text-align:right">Subtotal</span></div>' +
      S.items.map((it) => {
        const known = byName(it.name);
        const added = (num(it.packs) || 0) * (num(it.size) || 0) - (it.old_total || 0);
        const before = known ? known.available : 0;
        const unit = it.unit;
        const lot = [it.batch && `lote ${it.batch}`, it.expiration_date && `vence ${dateBr(it.expiration_date)}`].filter(Boolean).join(' · ');
        return `<div class="cw-rrow"><span><strong>${esc(it.name)}</strong><small>${fmt(num(it.packs))} × ${fmt(num(it.size))} ${esc(unit)}${lot ? ' · ' + esc(lot) : ''}${known ? '' : ' · item novo'}</small></span>
          <span><span class="io-txt-muted">${fmt(before)} ${esc(unit)}</span> → <strong class="${added < 0 ? 'io-txt-amber' : 'io-txt-green'}">${fmt(before + added)} ${esc(unit)}</strong></span>
          <span class="io-txt-muted">${num(it.size) ? money((num(it.price) || 0) / num(it.size)) + ' por ' + esc(unit) : '—'}</span>
          <strong style="text-align:right">${money(itemTotal(it))}</strong></div>`;
      }).join('');
    const p = S.payment;
    let fin;
    if (p.mode === 'prazo') {
      const r = parcels();
      const acc = D.accounts.find((a) => String(a.id) === String(p.account_id));
      fin = `No Financeiro: <strong>${r.length} ${r.length === 1 ? 'parcela' : 'parcelas'}</strong> em contas a pagar (${r.map((x) => `${dateBr(x.due_date).slice(0, 5)}: ${money(num(x.amount))}`).join(', ')}), na conta ${esc(acc ? acc.name : '')}, por ${esc(p.method)}.`;
    } else if (p.mode === 'cartao') {
      const card = D.cards.find((c) => String(c.id) === String(p.card_id));
      fin = `No Financeiro: <strong>${money(total())}</strong> no ${esc(card ? card.name : 'cartão')}, em ${p.card_count}x.`;
    } else if (p.account_id) {
      const acc = D.accounts.find((a) => String(a.id) === String(p.account_id));
      fin = `No Financeiro: <strong>${money(total())}</strong> pago à vista em ${dateBr(S.note.date)}, na conta ${esc(acc ? acc.name : '')}, por ${esc(p.method)}.`;
    } else {
      fin = 'Sem conta escolhida: a compra entra no estoque, mas não vai para o Financeiro.';
    }
    $('[data-cw-review-fin]').innerHTML = fin;
    const nt = $('[data-cw-nav-total]');
    nt.innerHTML = `Total <strong>${money(total())}</strong>`;
  }

  function renderStep() {
    $$('[data-cw-step]').forEach((el) => el.classList.toggle('io-hidden', Number(el.dataset.cwStep) !== S.step));
    $$('[data-cw-goto]').forEach((b) => {
      const n = Number(b.dataset.cwGoto);
      b.classList.toggle('is-on', n === S.step);
      b.classList.toggle('is-done', n < S.step);
      if (n === S.step) b.setAttribute('aria-current', 'step'); else b.removeAttribute('aria-current');
      b.querySelector('b').textContent = `${n}. ${['Nota', 'Itens', 'Pagamento', 'Conferir'][n - 1]}${n < S.step ? ' ✓' : ''}`;
    });
    const last = S.step === 4;
    $('[data-cw-prev]').textContent = S.step === 1 ? 'Cancelar' : 'Voltar';
    $('[data-cw-next]').classList.toggle('io-hidden', last);
    $('[data-cw-next]').textContent = ['Continuar para itens', 'Continuar para pagamento', 'Continuar para conferir'][S.step - 1] || 'Continuar';
    $('[data-cw-submit]').classList.toggle('io-hidden', !last);
    $('[data-cw-nav-total]').classList.toggle('io-hidden', !last);
    if (S.step === 1) $$('[data-cw-note]').forEach((el) => { el.value = S.note[el.dataset.cwNote] || ''; });
    if (S.step === 2) renderItems();
    if (S.step === 3) renderPayment();
    if (S.step === 4) renderReview();
    renderSummary();
    checkDuplicate();
  }

  function go(step) {
    if (step > S.step) {
      const p = problem(step - 1);
      if (p) {
        showError(p[1]);
        if (p[0] !== S.step) { S.step = p[0]; renderStep(); }
        if (p[2] != null) { renderItems(); focusItem(p[2]); }
        return;
      }
    }
    showError('');
    S.step = Math.min(4, Math.max(1, step));
    save();
    renderStep();
    const main = $('.cw-main');
    if (main && main.getBoundingClientRect().top < 0) main.scrollIntoView({ block: 'start', behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
  }

  function checkDuplicate() {
    const box = $('[data-cw-dup]');
    if (!box) return;
    const inv = plain(S.note.invoice);
    const hit = inv && (D.notes || []).find((n) => plain(n.invoice) === inv && plain(n.supplier) === plain(S.note.supplier));
    box.textContent = hit ? `Já existe uma compra com a nota ${S.note.invoice} de ${hit.supplier}, lançada em ${hit.date}. Confira antes de continuar.` : '';
    box.classList.toggle('io-hidden', !hit);
  }

  // ------------------------------------------------------------------ busca de itens
  const search = $('[data-cw-search]');
  const sugg = $('[data-cw-sugg]');
  function renderSuggestions() {
    const q = plain(search.value);
    if (!q) { sugg.classList.add('io-hidden'); sugg.innerHTML = ''; return; }
    const inNote = new Set(S.items.map((it) => plain(it.name)));
    const hits = CATALOG.filter((c) => plain(c.name).includes(q) && !inNote.has(plain(c.name))).slice(0, 6);
    const exact = CATALOG.some((c) => plain(c.name) === q);
    sugg.innerHTML = hits.map((c) => `<button type="button" role="option" data-cw-pick="${esc(c.name)}">${esc(c.name)} <span>${esc(c.group_label)} · tem ${esc(c.stock)}</span></button>`).join('') +
      (exact ? '' : `<button type="button" role="option" class="is-new" data-cw-new>+ Cadastrar “${esc(search.value.trim())}” como item novo</button>`);
    sugg.classList.remove('io-hidden');
  }
  function addItem(name, isNew) {
    const known = isNew ? null : byName(name);
    const it = { name: known ? known.name : name.trim(), item_type: known ? known.item_type : '', category: known ? known.category : '', unit: known ? known.unit : '', packs: '', size: '', price: '', batch: '', manufacture_date: '', expiration_date: '', minimum: known && known.minimum ? asCents(known.minimum) : '', type_key: '', open_extra: false };
    S.items.unshift(it);
    search.value = '';
    renderSuggestions();
    save();
    renderItems();
    renderSummary();
    focusItem(0, known ? '[data-cw-i="packs"]' : '.cw-type');
  }
  search.addEventListener('input', renderSuggestions);
  search.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      const firstBtn = sugg.querySelector('button');
      if (firstBtn) firstBtn.click();
    } else if (e.key === 'Escape') { search.value = ''; renderSuggestions(); }
  });
  sugg.addEventListener('click', (e) => {
    const pick = e.target.closest('[data-cw-pick]');
    if (pick) addItem(pick.dataset.cwPick, false);
    else if (e.target.closest('[data-cw-new]')) addItem(search.value, true);
  });

  // ------------------------------------------------------------------ eventos
  $('[data-cw-items]').addEventListener('click', (e) => {
    const card = e.target.closest('[data-cw-index]');
    if (!card) return;
    const i = Number(card.dataset.cwIndex);
    const it = S.items[i];
    if (e.target.closest('[data-cw-remove]')) { S.items.splice(i, 1); save(); renderItems(); renderSummary(); return; }
    if (e.target.closest('[data-cw-extra]')) { it.open_extra = !it.open_extra; save(); renderItems(); if (it.open_extra) focusItem(i, '[data-cw-i="minimum"]'); return; }
    const typeBtn = e.target.closest('[data-cw-type]');
    if (typeBtn) {
      const t = TYPE[typeBtn.dataset.cwType];
      it.type_key = t.key;
      it.item_type = t.item_type;
      it.category = t.categories[0];
      it.unit = (t.units[it.category] || [])[0] || '';
      save(); renderItems(); renderSummary();
      focusItem(i, '[data-cw-i="packs"]');
    }
  });
  page.addEventListener('click', (e) => {
    if (!e.target.closest('[data-cw-goto-missing]')) return;
    const idx = S.items.findIndex((it) => !isDone(it));
    if (idx >= 0) focusItem(idx);
  });
  const onItemInput = (e) => {
    const card = e.target.closest('[data-cw-index]');
    const key = e.target.dataset.cwI;
    if (!card || !key) return;
    const i = Number(card.dataset.cwIndex);
    const it = S.items[i];
    if (MASKS[key]) {
      const masked = MASKS[key](e.target.value);
      if (masked !== e.target.value) e.target.value = masked;
    }
    it[key] = e.target.value;
    if (key === 'category') {
      const units = (TYPE[it.type_key] || { units: {} }).units[it.category] || [];
      if (!units.includes(it.unit)) it.unit = units[0] || '';
      S.payment.installments = null;
      renderItems();
    } else if (key === 'unit') {
      renderItems();
    } else {
      if (key === 'packs' || key === 'price') S.payment.installments = null;
      if (key !== 'minimum' && key !== 'batch') refreshItemNumbers(i);
    }
    save();
    renderSummary();
  };
  $('[data-cw-items]').addEventListener('input', onItemInput);
  $('[data-cw-items]').addEventListener('change', (e) => { if (e.target.tagName === 'SELECT') onItemInput(e); });

  $$('[data-cw-note]').forEach((el) => el.addEventListener('input', () => { S.note[el.dataset.cwNote] = el.value; save(); renderSummary(); checkDuplicate(); }));
  $$('[data-cw-supplier]').forEach((b) => b.addEventListener('click', () => { S.note.supplier = b.dataset.cwSupplier; $('[data-cw-note="supplier"]').value = S.note.supplier; save(); renderSummary(); checkDuplicate(); }));
  const file = $('[data-cw-file]');
  if (file) file.addEventListener('change', () => { fileName = file.files[0] ? file.files[0].name : ''; $('[data-cw-file-label]').textContent = fileName ? `Anexada: ${fileName}` : 'Opcional. A nota fica anexada a cada item da compra.'; renderSummary(); });

  // ------------------------------------------------------------------ cadastro rápido de conta / cartão (gaveta)
  const quickDrawers = $$('[data-vr-drawer]');
  let quickOpener = null;
  const quickSelectFor = (kind) => $(`[data-vr-pick="${kind}"]`);
  const openQuickDrawer = (kind, from) => {
    if (EDIT && EDIT.paid) return;
    const dw = $(`[data-vr-drawer="${kind}"]`);
    if (!dw) return;
    quickOpener = from;
    dw.hidden = false;
    document.body.style.overflow = 'hidden';
    const first = dw.querySelector('input:not([type=hidden]), select');
    if (first) first.focus();
  };
  const closeQuickDrawer = (dw) => {
    dw.hidden = true;
    document.body.style.overflow = '';
    if (quickOpener) quickOpener.focus();
  };
  const showQuickToast = (text) => {
    const t = document.createElement('div');
    t.className = 'sv-toast';
    t.setAttribute('role', 'status');
    t.textContent = text;
    document.body.appendChild(t);
    setTimeout(() => { t.classList.add('is-out'); setTimeout(() => t.remove(), 260); }, 4000);
  };
  $$('[data-vr-open]').forEach((b) => b.addEventListener('click', () => openQuickDrawer(b.dataset.vrOpen, b)));
  quickDrawers.forEach((dw) => {
    dw.querySelectorAll('[data-vr-close]').forEach((b) => b.addEventListener('click', () => closeQuickDrawer(dw)));
    dw.querySelectorAll('[data-mask="decimal"]').forEach((el) => el.addEventListener('input', () => {
      const d = el.value.replace(/\D/g, '').replace(/^0+(?=\d)/, '').slice(0, 13);
      el.value = d ? (parseInt(d, 10) / 100).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '';
    }));
    const qform = dw.querySelector('[data-vr-quick]');
    const qerr = qform.querySelector('[data-vr-error]');
    qform.addEventListener('submit', async (e) => {
      e.preventDefault();
      const missingField = Array.from(qform.querySelectorAll('[required]')).find((el) => !el.value.trim());
      if (missingField) {
        const lbl = missingField.closest('.vw-f') && missingField.closest('.vw-f').querySelector('.vr-t');
        qerr.textContent = 'Preencha ' + (lbl ? lbl.textContent.trim() : 'os campos') + '.';
        qerr.hidden = false;
        missingField.focus();
        return;
      }
      const btn = qform.querySelector('[type=submit]');
      btn.disabled = true;
      try {
        const res = await fetch(qform.action, { method: 'POST', body: new FormData(qform), headers: { Accept: 'application/json' } });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.id) {
          qerr.textContent = data.error || 'Não foi possível salvar. Tente de novo.';
          qerr.hidden = false;
          return;
        }
        const kind = qform.dataset.vrQuick;
        const sel = quickSelectFor(kind);
        if (sel) {
          const opt = new Option(data.name, String(data.id), true, true);
          sel.insertBefore(opt, sel.querySelector('option[value="__nova__"]'));
          sel.value = String(data.id);
          sel.hidden = false;
          const addBtn = sel.parentElement && sel.parentElement.querySelector('[data-vr-open]');
          if (addBtn) addBtn.remove();
        }
        if (kind === 'conta') {
          D.accounts.push({ id: data.id, name: data.name, default: !D.accounts.length });
          const cardAccSel = $('[data-vr-card-account]');
          if (cardAccSel) cardAccSel.add(new Option(data.name, String(data.id)));
          S.payment.account_id = String(data.id);
        } else if (kind === 'cartao') {
          D.cards.push({ id: data.id, name: data.name, closing_day: Number(data.closing_day || 0), due_day: Number(data.due_day || 0) });
          S.payment.card_id = String(data.id);
          const sub = $('[data-cw-card-mode-sub]');
          if (sub) sub.textContent = 'Entra na fatura do cartão';
        }
        save();
        renderPayment();
        renderSummary();
        showError('');
        qform.reset();
        qerr.hidden = true;
        closeQuickDrawer(dw);
        quickOpener = sel;
        if (sel) sel.focus();
        showQuickToast(`${kind === 'conta' ? 'Conta' : 'Cartão'} ${data.name} criado e escolhido`);
      } finally {
        btn.disabled = false;
      }
    });
  });
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    const dw = quickDrawers.find((d) => !d.hidden);
    if (dw) closeQuickDrawer(dw);
  });

  $$('[data-cw-mode]').forEach((b) => b.addEventListener('click', () => { if (b.disabled) return; S.payment.mode = b.dataset.cwMode; save(); renderPayment(); renderSummary(); showError(''); }));
  $$('[data-cw-p]').forEach((el) => {
    const handler = () => {
      if (el.value === '__nova__' && el.dataset.vrPick) {
        el.value = String(S.payment[el.dataset.cwP] || '');
        openQuickDrawer(el.dataset.vrPick, el);
        return;
      }
      S.payment[el.dataset.cwP] = el.value;
      if (el.dataset.cwP === 'count' || el.dataset.cwP === 'first_due') S.payment.installments = null;
      save(); renderPayment(); renderSummary();
    };
    el.addEventListener('change', handler);
    if (el.tagName === 'INPUT') el.addEventListener('input', handler);
  });
  page.addEventListener('input', (e) => {
    const p = e.target.closest('[data-cw-parc]');
    if (!p) return;
    parcels()[Number(p.dataset.cwParc)][p.dataset.k] = p.value;
    save();
    const ok = Math.abs(parcelSum() - total()) <= 0.01;
    const st = $('[data-cw-parc-status]');
    if (st) { st.className = ok ? 'io-txt-green' : 'io-txt-red'; st.style.fontWeight = '600'; st.textContent = ok ? '✓ Soma das parcelas bate com a nota' : `A soma (${money(parcelSum())}) não bate com a nota`; }
  });
  page.addEventListener('click', (e) => {
    if (e.target.closest('[data-cw-reparc]')) { S.payment.installments = null; save(); renderPayment(); }
  });

  $$('[data-cw-goto]').forEach((b) => b.addEventListener('click', () => go(Number(b.dataset.cwGoto))));
  $('[data-cw-next]').addEventListener('click', () => go(S.step + 1));
  $('[data-cw-prev]').addEventListener('click', () => { if (S.step === 1) window.location.href = EDIT ? `/insumos/estoque?aba=movimentacoes&tipo=compra&periodo=safra&mov=c-${EDIT.entry_id}` : '/insumos/estoque'; else go(S.step - 1); });
  $('[data-cw-draft]') && $('[data-cw-draft]').addEventListener('click', (e) => { save(); e.currentTarget.textContent = 'Rascunho salvo'; setTimeout(() => { e.currentTarget && (e.currentTarget.textContent = 'Salvar rascunho'); }, 1600); });
  const discard = $('[data-cw-discard]');
  if (discard) discard.addEventListener('click', () => { try { localStorage.removeItem(DRAFT_KEY); } catch (err) { /* ignora */ } S = blank(); fileName = ''; $('[data-cw-restored]').classList.add('io-hidden'); renderStep(); });

  form.addEventListener('submit', (e) => {
    const p = problem(3);
    if (p) { e.preventDefault(); go(p[0]); showError(p[1]); return; }
    const pay = { ...S.payment, count: S.payment.mode === 'cartao' ? S.payment.card_count : (S.payment.mode === 'prazo' ? S.payment.count : '1') };
    if (pay.mode !== 'prazo') delete pay.installments; else pay.installments = parcels().map((x) => ({ due_date: x.due_date, amount: x.amount }));
    $('[data-cw-payload]').value = JSON.stringify({
      edit_of: EDIT ? EDIT.entry_id : null,
      note: S.note,
      payment: pay,
      items: S.items.map((it) => ({ entry_id: it.entry_id || null, name: it.name, item_type: it.item_type, category: it.category, unit: it.unit, packs: it.packs, size: it.size, price: it.price, batch: it.batch, manufacture_date: it.manufacture_date, expiration_date: it.expiration_date, minimum: it.minimum })),
    });
    const btn = $('[data-cw-submit]');
    btn.disabled = true;
    btn.textContent = 'Gravando…';
  });

  if (!S.note.date) S.note.date = D.today;
  renderStep();
}());
