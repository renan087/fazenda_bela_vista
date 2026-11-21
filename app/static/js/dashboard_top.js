// Dashboard — topo (safra + pendências) e modal "Todas as pendências".
(() => {
    const $ = (s, r = document) => r.querySelector(s);
    const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
    const reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const money = (v, cents = true) => (v < 0 ? '− ' : '') + 'R$ ' + Math.abs(v).toLocaleString('pt-BR', { minimumFractionDigits: cents ? 2 : 0, maximumFractionDigits: cents ? 2 : 0 });

    // Números que contam do zero ao abrir.
    const countUp = (el) => {
        const target = Number(el.dataset.count) || 0;
        const isMoney = el.hasAttribute('data-money') || el.hasAttribute('data-dt-margin');
        const fmt = (v) => (isMoney ? money(v) : String(Math.round(v)));
        if (reduce || !target) { el.textContent = fmt(target); return; }
        const start = performance.now();
        const step = (now) => {
            const k = Math.min(1, (now - start) / 1100);
            el.textContent = fmt(target * (1 - Math.pow(1 - k, 3)));
            if (k < 1) requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
    };

    // ---- Safra ----
    const season = $('[data-dt-season]');
    if (season) {
        const marginEl = $('[data-dt-margin]', season);
        const fit = (el) => el && el.classList.toggle('is-long', el.textContent.length > 12);
        if (marginEl) { countUp(marginEl); fit(marginEl); }
        const input = $('[data-dt-price]', season);
        if (input) {
            const sacks = Number(season.dataset.sacks) || 0, cost = Number(season.dataset.cost) || 0;
            const min = Number(input.dataset.min), max = Number(input.dataset.max);
            const wrap = input.closest('.dt-range-wrap');
            const update = () => {
                const p = Number(input.value) || 0, revenue = sacks * p, margin = revenue - cost;
                const base = Math.max(revenue, cost) || 1, costPct = Math.min(100, cost / base * 100);
                wrap.style.setProperty('--p', ((p - min) / ((max - min) || 1) * 100).toFixed(1) + '%');
                $('[data-dt-price-label]', season).textContent = money(p);
                $('[data-dt-revenue]', season).textContent = money(revenue);
                $('[data-dt-margin-short]', season).textContent = money(Math.max(margin, 0));
                $('[data-dt-cost-bar]', season).style.width = costPct.toFixed(1) + '%';
                $('[data-dt-margin-bar]', season).style.width = (margin >= 0 ? 100 - costPct : 0).toFixed(1) + '%';
                marginEl.textContent = money(margin);
                fit(marginEl);
                const pct = $('[data-dt-margin-pct]', season);
                pct.textContent = revenue ? Math.round(margin / revenue * 100) + '%' : '—';
                [marginEl, pct].forEach((el) => el.classList.toggle('is-bad', margin < 0));
                input.setAttribute('aria-valuetext', money(p) + ' por saca');
            };
            input.addEventListener('input', update);
            $('[data-dt-price-reset]', season)?.addEventListener('click', (e) => { input.value = e.currentTarget.dataset.value; update(); });
        }
    }

    // ---- Filtros (cartão e modal) ----
    const bindTabs = (root, onChange) => {
        const tabs = $$('[data-dt-tab]', root);
        tabs.forEach((tab) => tab.addEventListener('click', () => {
            tabs.forEach((t) => t.setAttribute('aria-selected', t === tab ? 'true' : 'false'));
            onChange(tab.dataset.dtTab);
        }));
        return (kind) => tabs.forEach((t) => t.setAttribute('aria-selected', t.dataset.dtTab === kind ? 'true' : 'false'));
    };

    const pend = $('[data-dt-pend]');
    if (pend) {
        $$('[data-count]', pend).forEach(countUp);
        const rows = $$('[data-dt-row]', pend);
        const show = (kind) => {
            let shown = 0;
            rows.forEach((row) => {
                const ok = (kind === 'all' || row.dataset.kind === kind) && shown < 4;
                row.hidden = !ok;
                if (ok) { shown += 1; const r = $('.dt-row', row); r.style.animation = 'none'; void r.offsetWidth; r.style.animation = ''; r.style.animationDelay = (shown * 60) + 'ms'; }
            });
        };
        show('all');
        bindTabs(pend, show);
    }

    // ---- Modal ----
    const modal = $('[data-dt-modal]');
    if (!modal) return;
    let kind = 'all';
    const search = $('[data-dt-search]', modal);
    const mrows = $$('[data-dt-row]', modal);
    const filter = () => {
        const q = (search.value || '').trim().toLowerCase();
        let any = false;
        $$('[data-dt-sec]', modal).forEach((sec) => {
            const secRows = $$('[data-dt-row]', sec);
            let n = 0;
            secRows.forEach((row) => {
                const ok = (kind === 'all' || row.dataset.kind === kind) && (!q || row.dataset.search.includes(q));
                row.hidden = !ok;
                if (ok) n += 1;
            });
            any = any || n > 0;
            const empty = $('[data-dt-sec-empty]', sec);
            empty.hidden = n > 0 || Boolean(q) || kind === 'field';
            sec.hidden = n === 0 && (Boolean(q) || kind === 'field');
        });
        $('[data-dt-noresult]', modal).hidden = any || !q;
    };
    const setKind = bindTabs(modal, (k) => { kind = k; filter(); });
    search.addEventListener('input', filter);
    $$('[data-dt-go]', modal).forEach((b) => b.addEventListener('click', () => { kind = b.dataset.dtGo; setKind(kind); search.value = ''; filter(); }));

    $$('[data-dt-expand]', modal).forEach((b) => b.addEventListener('click', () => {
        const open = b.getAttribute('aria-expanded') !== 'true';
        b.setAttribute('aria-expanded', open ? 'true' : 'false');
        b.closest('[data-dt-row]').querySelector('.dt-kids').hidden = !open;
    }));

    const selbar = $('[data-dt-selbar]', modal), foot = $('[data-dt-foot]', modal);
    const checks = $$('[data-dt-select]', modal);
    const syncSel = () => {
        const on = checks.filter((c) => c.checked);
        const total = on.reduce((a, c) => a + (Number(c.dataset.amount) || 0), 0);
        checks.forEach((c) => c.closest('[data-dt-row]').classList.toggle('is-sel', c.checked));
        selbar.hidden = on.length === 0;
        foot.hidden = on.length > 0;
        $('[data-dt-sel-label]', modal).textContent = `${on.length} selecionada${on.length === 1 ? '' : 's'} · ${money(total)}`;
    };
    checks.forEach((c) => c.addEventListener('change', syncSel));
    $('[data-dt-sel-clear]', modal).addEventListener('click', () => { checks.forEach((c) => { c.checked = false; }); syncSel(); });

    const close = () => modal.close();
    $$('[data-dt-pend-open]').forEach((b) => b.addEventListener('click', () => {
        modal.showModal();
        $$('[data-count]', modal).forEach(countUp);
        mrows.forEach((row, i) => { const r = $('.dt-row', row); r.style.animation = 'none'; void r.offsetWidth; r.style.animation = ''; r.style.animationDelay = Math.min(i, 12) * 35 + 'ms'; });
    }));
    $$('[data-dt-pend-close]', modal).forEach((b) => b.addEventListener('click', close));
    modal.addEventListener('click', (e) => { if (e.target === modal) close(); });
})();
