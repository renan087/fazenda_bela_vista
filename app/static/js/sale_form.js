/* Nova venda em 3 passos: lotes → negócio → conferir. Nada é enviado antes do passo 3. */
(() => {
    const form = document.querySelector('[data-sale-wizard]');
    if (!form) return;
    const $ = (s, r = form) => r.querySelector(s);
    const $$ = (s, r = form) => [...r.querySelectorAll(s)];
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const parse = (v) => {
        let t = String(v ?? '').replace(/[R$%\s]/g, '');
        if (t.includes(',')) t = t.replace(/\./g, '').replace(',', '.');
        else if (/^-?\d{1,3}(\.\d{3})+$/.test(t)) t = t.replace(/\./g, '');
        const n = parseFloat(t);
        return Number.isFinite(n) ? n : 0;
    };
    const fmt = (n, d = 0) => n.toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d === 0 ? 2 : d });
    const money = (n) => 'R$ ' + n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const colors = ['#294127', '#446A36', '#9BCB6B', '#C9D1C8', '#7A857E', '#3A5C2E'];
    const lots = $$('[data-vw-lot]');
    let step = 1;

    const selected = () => lots.filter((el) => $('.vw-lot-check', el).checked).map((el, i) => ({
        el, id: el.dataset.id, code: el.dataset.code, available: parse(el.dataset.available),
        sacks: parse($('[data-vw-sacks]', el).value), color: colors[i % colors.length],
    }));

    function values() {
        const lines = selected();
        const sacks = lines.reduce((a, l) => a + l.sacks, 0);
        const price = parse($('[data-vw-price]').value);
        const gross = Math.round(sacks * price * 100) / 100;
        const fun = Math.round(gross * parse($('[data-vw-funrural]').value)) / 100;
        const bro = Math.round(gross * parse($('[data-vw-brokerage]').value)) / 100;
        const freight = parse($('[data-vw-freight]').value);
        const discount = Math.round((fun + bro + freight) * 100) / 100;
        return { lines, sacks, price, gross, discount, net: Math.max(gross - discount, 0), freight, fun, bro };
    }

    function setQty(el, value) {
        const max = parse(el.dataset.available);
        const v = Math.max(0, Math.min(max, Math.round(value * 100) / 100));
        $('[data-vw-sacks]', el).value = fmt(v);
        refresh();
    }

    lots.forEach((el) => {
        const check = $('.vw-lot-check', el);
        check.addEventListener('change', () => {
            el.classList.toggle('is-on', check.checked);
            $('[data-vw-qty]', el).hidden = !check.checked;
            if (check.checked && !parse($('[data-vw-sacks]', el).value)) setQty(el, parse(el.dataset.available));
            refresh();
        });
        $('[data-vw-plus]', el).addEventListener('click', () => setQty(el, parse($('[data-vw-sacks]', el).value) + 10));
        $('[data-vw-minus]', el).addEventListener('click', () => setQty(el, Math.max(1, parse($('[data-vw-sacks]', el).value) - 10)));
        $('[data-vw-all]', el).addEventListener('click', () => setQty(el, parse(el.dataset.available)));
        $('[data-vw-sacks]', el).addEventListener('input', refresh);
        $('[data-vw-sacks]', el).addEventListener('blur', (e) => setQty(el, parse(e.target.value)));
    });

    function buyerName() { return ($('[data-bp-name]')?.value || '').trim(); }

    function refresh() {
        const v = values();
        $('[data-vw-total]', document).textContent = `${fmt(v.sacks)} sc`;
        $('[data-vw-lotcount]', document).textContent = v.lines.length ? ` de ${v.lines.length} lote${v.lines.length > 1 ? 's' : ''}` : ' nenhum lote';
        $('[data-vw-bar]', document).innerHTML = v.lines.map((l) => `<span style="flex:${l.sacks || 0.0001};background:${l.color}"></span>`).join('');
        $('[data-vw-sumlist]', document).innerHTML = v.lines.map((l) => `<li><span><i style="background:${l.color}"></i>${esc(l.code)}</span><strong>${fmt(l.sacks)} sc</strong></li>`).join('');
        $('[data-vw-sum-buyer]', document).textContent = buyerName() || '—';
        $('[data-vw-sum-gross]', document).textContent = v.gross ? money(v.gross) : '—';
        $('[data-vw-sum-net]', document).textContent = v.gross ? money(v.net) : '—';
        [['fun', v.fun], ['freight', v.freight], ['bro', v.bro]].forEach(([k, val]) => {
            const row = document.querySelector(`[data-vr-neg="${k}"]`);
            if (!row) return;
            row.hidden = !(val > 0);
            row.querySelector('dd').textContent = '− ' + money(val);
        });
        const r = receipt();
        const parcRow = document.querySelector('[data-vr-sum-parc]');
        if (parcRow) {
            parcRow.hidden = r.mode !== 'parc' || !v.net;
            parcRow.querySelector('dt').textContent = `${r.count} parcelas de`;
            parcRow.querySelector('dd').textContent = money(v.net / Math.max(r.count, 1));
        }
        const delivered = $('[data-vr-delivered]')?.checked;
        const put = (sel, txt) => { const el = document.querySelector(sel); if (el) el.textContent = txt; };
        put('[data-vr-sum-del]', delivered ? 'Entregue' : 'A entregar');
        put('[data-vr-sum-rec]', r.text);
        put('[data-vr-sum-fin]', r.fin);
        const range = document.querySelector('[data-vr-paid-range]');
        if (range) range.textContent = r.paid ? (r.paid === 1 ? '1ª' : `1ª a ${r.paid}ª`) : '';
    }

    function receipt() {
        const mode = $('[data-vr-mode]:checked')?.value || 'nao';
        const count = Math.max(2, Math.min(100, Math.round(parse($('[data-vw-count]')?.value)) || 2));
        const paid = Math.max(0, Math.min(count, Math.round(parse($('[data-vr-paid]')?.value)) || 0));
        const account = $('[data-vw-account]')?.value;
        let text = 'A receber', fin = 'Ao marcar recebida';
        if (mode === 'sim') { text = 'Recebida'; fin = account ? 'Pago na data informada' : 'Não lançar'; }
        if (mode === 'parc') {
            text = paid === count ? 'Recebida' : (paid ? `Recebido ${Math.round(paid * 100 / count)}%` : 'A receber');
            fin = `${count} parcelas · ${paid} paga${paid === 1 ? '' : 's'}`;
        }
        return { mode, count, paid, text, fin };
    }

    function syncCondition() {
        const mode = $('[data-vr-mode]:checked')?.value || 'nao';
        const fields = $('[data-vr-fields]');
        if (fields) fields.hidden = mode === 'nao';
        $$('[data-vr-only]').forEach((el) => { el.hidden = el.dataset.vrOnly !== mode; });
        const card = $('[data-vr-card]');
        if (card) card.hidden = !($('[data-vr-method]')?.value || '').startsWith('Cartão');
        const on = $('[data-vr-delivered-on]');
        if (on) on.hidden = !$('[data-vr-delivered]')?.checked;
        const count = $('[data-vw-count]'); const paid = $('[data-vr-paid]');
        if (count && paid) paid.dataset.max = String(Math.max(2, Math.min(100, Math.round(parse(count.value)) || 2)));
        $$('[data-vr-dec], [data-vr-inc]').forEach((b) => {
            const input = $(`[name="${b.dataset.vrDec || b.dataset.vrInc}"]`);
            const v = Math.round(parse(input.value)) || 0;
            b.disabled = b.dataset.vrDec ? v <= Number(input.dataset.min) : v >= Number(input.dataset.max);
        });
    }

    // Contadores (parcelas e já recebidas) e máscara 100 → 1,00 nos valores.
    function clampPaid() {
        const count = $('[data-vw-count]'); const paid = $('[data-vr-paid]');
        if (!count || !paid) return;
        const max = Math.max(2, Math.min(100, Math.round(parse(count.value)) || 2));
        if ((Math.round(parse(paid.value)) || 0) > max) paid.value = String(max);
    }
    $$('[data-vr-dec], [data-vr-inc]').forEach((b) => b.addEventListener('click', () => {
        const input = $(`[name="${b.dataset.vrDec || b.dataset.vrInc}"]`);
        const v = (Math.round(parse(input.value)) || 0) + (b.dataset.vrInc ? 1 : -1);
        input.value = String(Math.max(Number(input.dataset.min), Math.min(Number(input.dataset.max), v)));
        clampPaid(); syncCondition(); refresh();
    }));
    ['[data-vw-count]', '[data-vr-paid]'].forEach((sel) => {
        const input = $(sel);
        if (!input) return;
        input.addEventListener('input', () => { input.value = input.value.replace(/\D/g, '').slice(0, 3); });
        input.addEventListener('blur', () => {
            const v = Math.round(parse(input.value)) || 0;
            input.value = String(Math.max(Number(input.dataset.min), Math.min(Number(input.dataset.max), v)));
            clampPaid(); syncCondition(); refresh();
        });
    });
    const maskDecimal = (el) => {
        const digits = el.value.replace(/\D/g, '').replace(/^0+(?=\d)/, '').slice(0, 13);
        el.value = digits ? (parseInt(digits, 10) / 100).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '';
    };
    $$('[data-mask="decimal"]').forEach((el) => {
        if (el.value && !el.value.includes(',')) el.value = parse(el.value).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        el.addEventListener('input', () => maskDecimal(el));
    });

    function error(msg) {
        const box = $('[data-vw-error]');
        box.hidden = !msg;
        box.textContent = msg || '';
        if (msg) box.scrollIntoView({ block: 'nearest', behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
    }

    function validate(n) {
        const v = values();
        if (n === 1) {
            if (!v.lines.length) return 'Escolha pelo menos um lote para continuar.';
            const bad = v.lines.find((l) => l.sacks <= 0);
            if (bad) return `Informe quantas sacas saem do lote ${bad.code}.`;
            const over = v.lines.find((l) => l.sacks > l.available + 0.0001);
            if (over) return `O lote ${over.code} tem só ${fmt(over.available)} sc de saldo.`;
        }
        if (n === 2) {
            if (!buyerName()) return 'Escolha ou cadastre o comprador.';
            if (v.price <= 0) return 'Informe o preço por saca.';
            if (v.discount > v.gross) return 'Os descontos não podem ser maiores que o valor da venda.';
            const parcel = receipt().mode === 'parc';
            if (parcel && !$('[data-vw-account]').value) return 'Para parcelar, escolha a conta que vai receber.';
            if (parcel && !$('[data-vw-first]').value) return 'Informe a data da primeira parcela.';
        }
        return '';
    }

    function review() {
        const v = values();
        const r = receipt();
        const statusLabel = r.text;
        const account = $('[data-vw-account]');
        const parcel = r.mode === 'parc';
        const count = parcel ? r.count : 1;
        const first = $('[data-vw-first]').value;
        const firstTxt = first ? first.split('-').reverse().join('/') : '';
        let fin = 'Esta venda não será lançada no Financeiro (nenhuma conta escolhida).';
        if (r.mode === 'nao') fin = 'Fica a receber. Conta e forma você informa ao marcar como recebida na lista de vendas.';
        else if (account.value) {
            const name = account.options[account.selectedIndex].text;
            fin = parcel
                ? `No Financeiro: ${count} parcelas de ${money(v.net / count)}${firstTxt ? `, a partir de ${firstTxt}` : ''}, na conta ${esc(name)}.`
                : `No Financeiro: ${money(v.net)} recebido na conta ${esc(name)}.`;
            if (parcel && r.paid) fin += ` As ${r.paid === 1 ? '1ª parcela entra paga' : `${r.paid} primeiras entram pagas`}.`;
        }
        $('[data-vw-review]').innerHTML = `
            <div class="vw-rev-card vw-rev-buyer"><span><small>Comprador</small><strong>${esc(buyerName())}</strong></span><span class="pd-chip is-ok">${esc(statusLabel)}</span></div>
            <div class="vw-rev-card"><p class="vw-rev-head">Saldo dos lotes</p>
                ${v.lines.map((l) => `<div class="vw-rev-row"><span><strong>${esc(l.code)}</strong> · sai ${fmt(l.sacks)} sc</span><span>${fmt(l.available)} → <strong>${fmt(l.available - l.sacks)} sc</strong></span></div>`).join('')}
            </div>
            <div class="vw-rev-card">
                <div class="vw-rev-row"><span>${fmt(v.sacks)} sc × ${money(v.price)}</span><span>${money(v.gross)}</span></div>
                ${v.discount ? `<div class="vw-rev-row"><span>Descontos (Funrural, corretagem e frete)</span><span>− ${money(v.discount)}</span></div>` : ''}
                <div class="vw-rev-row is-total"><strong>Você recebe</strong><strong>${money(v.net)}</strong></div>
            </div>
            <div class="pd-alert is-info">${fin}</div>`;
    }

    function go(n) {
        if (n > step) {
            for (let s = step; s < n; s += 1) {
                const msg = validate(s);
                if (msg) { error(msg); return; }
            }
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
        $('[data-vw-submit]').hidden = step !== 3;
        if (step === 3) review();
        const heading = $(`[data-vw-step="${step}"] h2`);
        if (heading) { heading.setAttribute('tabindex', '-1'); heading.focus({ preventScroll: false }); }
    }

    $('[data-vw-next]').addEventListener('click', () => go(step + 1));
    $('[data-vw-back]').addEventListener('click', () => go(step - 1));
    form.addEventListener('submit', (event) => {
        const msg = validate(1) || validate(2);
        if (msg) { event.preventDefault(); error(msg); return; }
        $('[data-vw-submit]').disabled = true;
    });
    form.addEventListener('input', refresh);
    form.addEventListener('change', () => { syncCondition(); refresh(); });
    form.addEventListener('buyer:change', refresh);
    form.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' && event.target.tagName === 'INPUT' && step < 3) { event.preventDefault(); go(step + 1); }
    });
    syncCondition();
    refresh();
    if (document.querySelector('.pd-alert.is-error[role=alert]:not([data-vw-error])')) go(lots.some((el) => $('.vw-lot-check', el).checked) ? 2 : 1);
})();

/* Gaveta "Registrar colheita" dentro da Nova venda; talhão fora da safra pode ser incluído ali mesmo. */
(function () {
  'use strict';
  const box = document.querySelector('[data-qh]');
  if (box) {
    const form = box.querySelector('[data-qh-form]');
    const plot = form.querySelector('[data-qh-plot]');
    const ask = form.querySelector('[data-qh-ask]');
    const okLine = form.querySelector('[data-qh-included]');
    const include = form.querySelector('[data-qh-include]');
    const err = form.querySelector('[data-qh-error]');
    let opener = null;
    const open = (btn) => { opener = btn; box.hidden = false; document.body.style.overflow = 'hidden'; plot.focus(); };
    const close = () => { box.hidden = true; document.body.style.overflow = ''; if (opener) opener.focus(); };
    document.querySelectorAll('[data-qh-open]').forEach((b) => b.addEventListener('click', () => open(b)));
    box.querySelectorAll('[data-qh-close]').forEach((b) => b.addEventListener('click', close));
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && !box.hidden) close(); });
    plot.addEventListener('change', () => {
      const opt = plot.selectedOptions[0];
      const out = opt && opt.dataset.out === '1';
      include.value = ''; okLine.hidden = true;
      ask.hidden = !out;
      if (out) ask.querySelector('[data-qh-plotname]').textContent = opt.textContent.replace(' · fora da safra', '');
    });
    ask.querySelector('[data-qh-no]').addEventListener('click', () => { plot.value = ''; ask.hidden = true; plot.focus(); });
    ask.querySelector('[data-qh-yes]').addEventListener('click', () => {
      include.value = '1'; ask.hidden = true;
      okLine.textContent = ask.querySelector('[data-qh-plotname]').textContent + ' será incluído na safra ao salvar.';
      okLine.hidden = false;
    });
    form.addEventListener('submit', (e) => {
      let msg = '';
      const opt = plot.selectedOptions[0];
      if (!plot.value) msg = 'Escolha o talhão.';
      else if (opt.dataset.out === '1' && include.value !== '1') msg = 'Inclua o talhão na safra para registrar a colheita.';
      else if (!form.harvest_date.value) msg = 'Informe a data da colheita.';
      else if (!(parseFloat(String(form.measured_quantity.value).replace(/\./g, '').replace(',', '.')) > 0)) msg = 'Informe quantas sacas foram colhidas.';
      if (msg) { e.preventDefault(); err.textContent = msg; err.hidden = false; }
    });
  }
  const toast = document.querySelector('[data-sv-toast]');
  if (toast) {
    let timer = null;
    const hide = () => { toast.classList.add('is-out'); setTimeout(() => toast.remove(), 260); };
    const arm = () => { clearTimeout(timer); timer = setTimeout(hide, 7000); };
    toast.addEventListener('mouseenter', () => clearTimeout(timer)); toast.addEventListener('mouseleave', arm);
    toast.querySelector('[data-sv-toast-close]').addEventListener('click', hide); arm();
  }
})();
