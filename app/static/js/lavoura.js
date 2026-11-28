(() => {
    const page = document.querySelector('[data-lv-page]');
    if (!page) return;
    const rows = Array.from(page.querySelectorAll('[data-lv-item]'));
    const panels = Array.from(page.querySelectorAll('[data-lv-panel]'));
    const empty = page.querySelector('[data-lv-empty]');
    const sub = page.querySelector('[data-lv-list-sub]');
    const names = { nutri: 'Nutrição', san: 'Sanidade', agua: 'Água', solo: 'Solo' };
    const mobile = window.matchMedia('(max-width: 1023px)');
    let filter = '';
    let selected = page.dataset.selected || '';

    /* ---------- Filtro por assunto (resumo e chips sincronizados) ---------- */
    const applyFilter = (key) => {
        filter = filter === key ? '' : key;
        page.querySelectorAll('[data-lv-filter]').forEach((btn) => {
            const on = btn.dataset.lvFilter === filter;
            btn.setAttribute('aria-pressed', on ? 'true' : 'false');
            if (btn.classList.contains('lv-chip')) btn.classList.toggle('is-on', on);
        });
        page.querySelectorAll('[data-lv-seg]').forEach((seg) => seg.classList.toggle('is-on', seg.dataset.lvSeg === filter));
        page.querySelectorAll('.lv-row-head [data-lv-col]').forEach((col) => col.classList.toggle('is-on', col.dataset.lvCol === filter));
        let shown = 0;
        rows.forEach((row) => {
            const ok = !filter || (row.dataset.lvAttn || '').split(' ').includes(filter);
            row.hidden = !ok;
            if (ok) shown += 1;
        });
        if (empty) empty.classList.toggle('hidden', shown > 0);
        if (sub) sub.textContent = filter ? `${shown} de ${rows.length} pedem atenção em ${names[filter]}.` : sub.dataset.default;
        if (filter && shown) {
            const firstVisible = rows.find((r) => !r.hidden);
            if (firstVisible && rows.find((r) => r.dataset.lvItem === selected)?.hidden) select(firstVisible.dataset.lvItem, { open: false });
        }
    };
    page.querySelectorAll('[data-lv-filter]').forEach((btn) => btn.addEventListener('click', () => {
        const key = btn.dataset.lvFilter;
        if (key === '' && !filter) return;
        applyFilter(key);
        if (btn.classList.contains('lv-seg-btn')) page.querySelector('.lv-list')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }));

    /* ---------- Talhão selecionado ---------- */
    const menuPlot = page.querySelector('[data-lv-menu-plot]');
    const regLinks = Array.from(page.querySelectorAll('[data-lv-reg]'));
    const closeSheet = () => {
        page.classList.remove('lv-sheet-open');
        document.body.style.overflow = '';
    };
    function select(id, { open = true } = {}) {
        selected = String(id);
        let row = null;
        rows.forEach((r) => {
            const on = r.dataset.lvItem === selected;
            r.classList.toggle('is-selected', on);
            r.setAttribute('aria-pressed', on ? 'true' : 'false');
            if (on) row = r;
        });
        panels.forEach((p) => { p.hidden = p.dataset.lvPanel !== selected; });
        if (row && menuPlot) menuPlot.textContent = row.dataset.lvName;
        regLinks.forEach((a) => {
            try {
                const url = new URL(a.getAttribute('href'), window.location.origin);
                url.searchParams.set('plot_id', selected);
                a.setAttribute('href', url.pathname + url.search);
            } catch (e) { /* link sem talhão */ }
        });
        if (open && mobile.matches) {
            page.classList.add('lv-sheet-open');
            document.body.style.overflow = 'hidden';
        }
        try { const url = new URL(window.location.href); url.searchParams.set('talhao', selected); history.replaceState(null, '', url); } catch (e) { /* sem histórico */ }
    }
    rows.forEach((r) => r.addEventListener('click', () => select(r.dataset.lvItem)));
    page.querySelectorAll('[data-lv-close]').forEach((el) => el.addEventListener('click', closeSheet));
    mobile.addEventListener?.('change', () => { if (!mobile.matches) closeSheet(); });

    /* ---------- Menu Registrar no campo: fecha ao clicar fora e com Esc ---------- */
    const menuBtn = page.querySelector('[data-lv-menu-btn]');
    const menu = page.querySelector('[data-lv-menu]');
    const setMenu = (open) => {
        if (!menu) return;
        menu.hidden = !open;
        menuBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
        if (open) menu.querySelector('[role=menuitem]')?.focus();
    };
    menuBtn?.addEventListener('click', (event) => { event.stopPropagation(); setMenu(menu.hidden); });
    document.addEventListener('click', (event) => { if (menu && !menu.hidden && !event.target.closest('.lv-menu-wrap')) setMenu(false); });
    document.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape') return;
        if (menu && !menu.hidden) { setMenu(false); menuBtn.focus(); return; }
        if (page.classList.contains('lv-sheet-open')) closeSheet();
    });

    const initial = new URLSearchParams(window.location.search).get('talhao');
    const start = rows.find((r) => r.dataset.lvItem === initial) ? initial : selected;
    if (start) select(start, { open: false });
})();
