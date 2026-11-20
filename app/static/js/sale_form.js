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
        const pct = parse($('[data-vw-funrural]').value) + parse($('[data-vw-brokerage]').value);
        const freight = parse($('[data-vw-freight]').value);
        const discount = Math.round((gross * pct / 100 + freight) * 100) / 100;
        return { lines, sacks, price, gross, discount, net: Math.max(gross - discount, 0), freight, pct };
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
    }

    function syncCondition() {
        const parcel = $('[name=payment_condition]:checked')?.value === 'a_prazo';
        $$('[data-vw-installments]').forEach((el) => { el.hidden = !parcel; });
    }

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
            const parcel = $('[name=payment_condition]:checked')?.value === 'a_prazo';
            if (parcel && !$('[data-vw-account]').value) return 'Para parcelar, escolha a conta que vai receber.';
            if (parcel && !$('[data-vw-first]').value) return 'Informe a data da primeira parcela.';
        }
        return '';
    }

    function review() {
        const v = values();
        const status = $('[name=status]:checked');
        const statusLabel = status ? status.nextElementSibling.textContent : 'Negociada';
        const account = $('[data-vw-account]');
        const parcel = $('[name=payment_condition]:checked')?.value === 'a_prazo';
        const count = parcel ? parse($('[data-vw-count]').value) : 1;
        const first = $('[data-vw-first]').value;
        const firstTxt = first ? first.split('-').reverse().join('/') : '';
        let fin = 'Esta venda não será lançada no Financeiro (nenhuma conta escolhida).';
        if (account.value) {
            const name = account.options[account.selectedIndex].text;
            fin = parcel
                ? `No Financeiro: ${count} parcelas de ${money(v.net / count)}${firstTxt ? `, a partir de ${firstTxt}` : ''}, na conta ${esc(name)}.`
                : `No Financeiro: 1 conta a receber de ${money(v.net)} na conta ${esc(name)}.`;
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
