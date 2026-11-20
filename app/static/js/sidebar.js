/* Menu lateral do SiSFarm: trilho recolhido, grupos, favoritos, painéis flutuantes e gavetas do celular. */
(() => {
    const sb = document.querySelector('[data-sf-sidebar]');
    if (!sb) return;

    const body = document.body;
    const desktopMq = window.matchMedia('(min-width: 1024px)');
    const RAIL_KEY = 'sisfarm.desktopSidebarCollapsed';
    const FAV_KEY = 'sisfarm.menuFavorites.' + (sb.dataset.sfUser || 'anon');
    const $ = (sel, root = document) => root.querySelector(sel);
    const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
    const store = {
        get(key) { try { return localStorage.getItem(key); } catch (e) { return null; } },
        set(key, value) { try { localStorage.setItem(key, value); } catch (e) { /* armazenamento indisponível */ } },
    };
    const isRail = () => desktopMq.matches && body.classList.contains('erp-sidebar-collapsed');
    const svgIcon = (name, cls = 'sf-ic') => {
        const ns = 'http://www.w3.org/2000/svg';
        const svg = document.createElementNS(ns, 'svg');
        svg.setAttribute('class', cls);
        svg.setAttribute('aria-hidden', 'true');
        const use = document.createElementNS(ns, 'use');
        use.setAttribute('href', '#sf-' + name);
        svg.append(use);
        return svg;
    };

    /* ---------------- painéis flutuantes (Registrar, conta, grupos no trilho) ---------------- */
    const tip = $('[data-sf-tip-el]');
    const fly = $('[data-sf-fly-panel]');
    let openPanel = null;
    let openAnchor = null;

    const hideTip = () => { if (tip) tip.hidden = true; };
    const place = (panel, anchor) => {
        const r = anchor.getBoundingClientRect();
        const gap = 12;
        panel.style.width = '';
        if (isRail()) {
            const sbr = sb.getBoundingClientRect();
            panel.style.left = Math.round(sbr.right + gap) + 'px';
            const h = panel.offsetHeight;
            const top = Math.max(12, Math.min(r.top - 6, window.innerHeight - h - 12));
            panel.style.top = Math.round(top) + 'px';
            return;
        }
        panel.style.left = Math.round(r.left) + 'px';
        panel.style.width = Math.round(Math.max(r.width, 260)) + 'px';
        const h = panel.offsetHeight;
        const above = anchor.closest('.sf-foot') || r.bottom + gap + h > window.innerHeight - 8;
        panel.style.top = Math.round(above ? Math.max(12, r.top - gap - h) : r.bottom + 8) + 'px';
    };
    const closePanel = () => {
        if (!openPanel) return;
        openPanel.hidden = true;
        openPanel.classList.remove('is-on');
        if (openAnchor) {
            openAnchor.setAttribute('aria-expanded', 'false');
            openAnchor.classList.remove('is-open');
        }
        openPanel = null;
        openAnchor = null;
    };
    const showPanel = (panel, anchor) => {
        closePanel();
        hideTip();
        panel.hidden = false;
        place(panel, anchor);
        requestAnimationFrame(() => panel.classList.add('is-on'));
        anchor.setAttribute('aria-expanded', 'true');
        anchor.classList.add('is-open');
        openPanel = panel;
        openAnchor = anchor;
        const first = panel.querySelector('a, button');
        if (first && anchor.matches(':focus-visible')) first.focus();
    };

    $$('[data-sf-pop-open]').forEach((btn) => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const panel = document.getElementById(btn.dataset.sfPopOpen);
            if (!panel) return;
            if (openPanel === panel && openAnchor === btn) { closePanel(); return; }
            showPanel(panel, btn);
        });
    });

    /* ---------------- favoritos ---------------- */
    const readFavs = () => {
        try { const v = JSON.parse(store.get(FAV_KEY) || '[]'); return Array.isArray(v) ? v : []; } catch (e) { return []; }
    };
    let favs = readFavs();
    const source = (id) => $(`.sf-full a.sf-link[data-sf-id="${CSS.escape(id)}"]`);
    const favData = () => favs.map((id) => {
        const a = source(id);
        if (!a) return null;
        return { id, href: a.getAttribute('href'), label: (a.querySelector('span') || a).textContent.trim(), icon: a.dataset.sfIcon || 'star', group: a.dataset.sfGroupLabel || '', active: a.classList.contains('is-active'), target: a.getAttribute('target') };
    }).filter(Boolean);

    const favRow = (f) => {
        const wrap = document.createElement('div');
        wrap.className = 'sf-item sf-fav' + (f.active ? ' is-active' : '');
        const mark = document.createElement('span');
        mark.className = 'sf-mark';
        mark.setAttribute('aria-hidden', 'true');
        const a = document.createElement('a');
        a.className = 'sf-row' + (f.active ? ' is-active' : '');
        a.href = f.href;
        if (f.target) { a.target = f.target; a.rel = 'noopener'; }
        if (f.active) a.setAttribute('aria-current', 'page');
        const lbl = document.createElement('span');
        lbl.className = 'sf-lbl';
        lbl.textContent = f.label;
        a.append(svgIcon(f.icon), lbl);
        const un = document.createElement('button');
        un.type = 'button';
        un.className = 'sf-unpin';
        un.dataset.sfPin = f.id;
        un.setAttribute('aria-label', 'Tirar dos favoritos: ' + f.label);
        un.append(svgIcon('x', 'sf-ic sf-ic-s'));
        wrap.append(mark, a, un);
        return wrap;
    };
    const renderFavs = () => {
        const data = favData();
        const box = $('[data-sf-favs]');
        if (box) box.replaceChildren(...data.map(favRow));
        const count = $('[data-sf-fav-count]');
        if (count) count.textContent = data.length ? String(data.length) : '';
        const empty = $('[data-sf-fav-empty]');
        if (empty) empty.hidden = data.length > 0;
        const badge = $('[data-sf-fav-badge]');
        if (badge) { badge.hidden = !data.length; badge.textContent = String(data.length); }
        const mbox = $('[data-sf-m-favs]');
        if (mbox) {
            mbox.replaceChildren(...data.map((f) => {
                const a = document.createElement('a');
                a.className = 'sf-m-fav';
                a.href = f.href;
                const s = document.createElement('span');
                s.textContent = f.label;
                a.append(svgIcon(f.icon), s);
                return a;
            }));
        }
        const mEmpty = $('[data-sf-m-favs-empty]');
        if (mEmpty) mEmpty.hidden = data.length > 0;
        $$('[data-sf-pin]').forEach((b) => {
            if (b.classList.contains('sf-unpin')) return;
            const on = favs.includes(b.dataset.sfPin);
            b.classList.toggle('is-pinned', on);
            b.setAttribute('aria-pressed', on ? 'true' : 'false');
            const label = (b.getAttribute('aria-label') || '').replace(/^(Fixar nos favoritos|Tirar dos favoritos): /, '');
            b.setAttribute('aria-label', (on ? 'Tirar dos favoritos: ' : 'Fixar nos favoritos: ') + label);
        });
    };
    const togglePin = (id) => {
        favs = favs.includes(id) ? favs.filter((x) => x !== id) : favs.concat([id]);
        store.set(FAV_KEY, JSON.stringify(favs));
        renderFavs();
        if (openPanel === fly && fly.dataset.sfFor === 'favs' && openAnchor) fillFly('favs', openAnchor);
    };
    document.addEventListener('click', (e) => {
        const pin = e.target.closest('[data-sf-pin]');
        if (!pin) return;
        e.preventDefault();
        e.stopPropagation();
        togglePin(pin.dataset.sfPin);
    });

    /* ---------------- grupos (menu aberto) ---------------- */
    $$('.sf-group [data-sf-toggle]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const sub = document.getElementById(btn.getAttribute('aria-controls'));
            const open = btn.getAttribute('aria-expanded') !== 'true';
            btn.setAttribute('aria-expanded', open ? 'true' : 'false');
            btn.classList.toggle('is-open', open);
            if (sub) sub.classList.toggle('is-open', open);
        });
    });

    /* ---------------- trilho: grupos e favoritos no balão lateral ---------------- */
    const fillFly = (id, anchor) => {
        const title = $('[data-sf-fly-title]', fly);
        const icon = $('[data-sf-fly-icon]', fly);
        const bodyEl = $('[data-sf-fly-body]', fly);
        fly.dataset.sfFor = id;
        if (id === 'favs') {
            title.textContent = 'Favoritos';
            icon.setAttribute('href', '#sf-star');
            const data = favData();
            if (!data.length) {
                const p = document.createElement('p');
                p.className = 'sf-fly-empty';
                p.textContent = 'Nenhum favorito ainda. Abra o menu e toque na estrela de uma página.';
                bodyEl.replaceChildren(p);
            } else {
                const tree = document.createElement('div');
                tree.className = 'sf-tree';
                data.forEach((f) => {
                    const row = document.createElement('div');
                    row.className = 'sf-sub-row';
                    const a = document.createElement('a');
                    a.className = 'sf-link' + (f.active ? ' is-active' : '');
                    a.href = f.href;
                    const s = document.createElement('span');
                    s.textContent = f.label;
                    a.append(s);
                    const b = document.createElement('button');
                    b.type = 'button';
                    b.className = 'sf-pin is-pinned';
                    b.dataset.sfPin = f.id;
                    b.setAttribute('aria-label', 'Tirar dos favoritos: ' + f.label);
                    b.append(svgIcon('star', 'sf-ic sf-ic-s'));
                    row.append(a, b);
                    tree.append(row);
                });
                bodyEl.replaceChildren(tree);
            }
        } else {
            const group = $(`.sf-group[data-sf-group="${CSS.escape(id)}"]`);
            if (!group) return;
            title.textContent = group.dataset.sfLabel || '';
            icon.setAttribute('href', '#sf-' + (group.dataset.sfIcon || 'star'));
            const tree = $('.sf-tree', group);
            bodyEl.replaceChildren(tree ? tree.cloneNode(true) : document.createTextNode(''));
        }
        fly.setAttribute('aria-label', title.textContent);
        if (openPanel === fly) place(fly, anchor);
    };
    $$('[data-sf-fly]').forEach((btn) => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            if (openPanel === fly && openAnchor === btn) { closePanel(); return; }
            fillFly(btn.dataset.sfFly, btn);
            showPanel(fly, btn);
        });
    });

    /* ---------------- dicas (balões com o nome) no trilho ---------------- */
    let tipTimer = null;
    $$('[data-sf-tip]', sb).forEach((el) => {
        const show = () => {
            if (!isRail() || openPanel || !tip) return;
            clearTimeout(tipTimer);
            tipTimer = setTimeout(() => {
                const r = el.getBoundingClientRect();
                tip.textContent = el.dataset.sfTip;
                tip.hidden = false;
                tip.style.left = Math.round(sb.getBoundingClientRect().right + 10) + 'px';
                tip.style.top = Math.round(r.top + r.height / 2) + 'px';
            }, 120);
        };
        const hide = () => { clearTimeout(tipTimer); hideTip(); };
        el.addEventListener('mouseenter', show);
        el.addEventListener('focus', show);
        el.addEventListener('mouseleave', hide);
        el.addEventListener('blur', hide);
    });

    /* ---------------- recolher / expandir ---------------- */
    const railBtn = $('[data-sf-rail-toggle]', sb);
    const syncRailBtn = () => {
        if (!railBtn) return;
        const rail = body.classList.contains('erp-sidebar-collapsed');
        railBtn.setAttribute('aria-label', rail ? 'Expandir menu' : 'Recolher menu');
        railBtn.setAttribute('aria-expanded', rail ? 'false' : 'true');
    };
    railBtn?.addEventListener('click', () => {
        const next = !body.classList.contains('erp-sidebar-collapsed');
        closePanel();
        hideTip();
        body.classList.remove('erp-sidebar-collapsing', 'erp-sidebar-expanding');
        body.classList.toggle('erp-sidebar-collapsed', next);
        body.classList.add('sf-anim');
        clearTimeout(railBtn._sfAnim);
        railBtn._sfAnim = setTimeout(() => body.classList.remove('sf-anim'), 700);
        store.set(RAIL_KEY, next ? '1' : '0');
        syncRailBtn();
    });
    syncRailBtn();
    new MutationObserver(syncRailBtn).observe(body, { attributes: true, attributeFilter: ['class'] });

    /* ---------------- celular: gavetas ---------------- */
    const back = $('.sf-m-back');
    const trk = $('[data-sf-trk]');
    const mq = $('[data-sf-m-q]');
    let sheet = null;
    let sheetOpener = null;
    const closeSheet = () => {
        if (!sheet) return;
        sheet.classList.remove('is-on');
        sheet.setAttribute('inert', '');
        back?.classList.remove('is-on');
        body.classList.remove('sf-sheet-open');
        $$('[data-sf-sheet-open]').forEach((b) => { b.setAttribute('aria-expanded', 'false'); b.classList.remove('is-on'); });
        if (sheetOpener) sheetOpener.focus({ preventScroll: true });
        sheet = null;
        sheetOpener = null;
    };
    const openSheet = (name, opener) => {
        const el = document.getElementById('sf-sheet-' + name);
        if (!el) return;
        if (sheet === el) { closeSheet(); return; }
        closeSheet();
        el.removeAttribute('inert');
        el.classList.add('is-on');
        back?.classList.add('is-on');
        body.classList.add('sf-sheet-open');
        opener.setAttribute('aria-expanded', 'true');
        opener.classList.add('is-on');
        sheet = el;
        sheetOpener = opener;
        if (name === 'menu') {
            trk?.classList.remove('is-sub');
            if (mq) { mq.value = ''; renderMobileSearch(); }
        }
    };
    $$('[data-sf-sheet-open]').forEach((b) => b.addEventListener('click', () => openSheet(b.dataset.sfSheetOpen, b)));
    $$('[data-sf-sheet-close]').forEach((b) => b.addEventListener('click', closeSheet));
    $$('[data-sf-m-open]').forEach((b) => b.addEventListener('click', () => {
        $$('[data-sf-m-sub]').forEach((s) => { s.hidden = s.dataset.sfMSub !== b.dataset.sfMOpen; });
        trk?.classList.add('is-sub');
        const backBtn = $(`[data-sf-m-sub="${CSS.escape(b.dataset.sfMOpen)}"] [data-sf-m-back]`);
        setTimeout(() => backBtn?.focus({ preventScroll: true }), 380);
    }));
    $$('[data-sf-m-back]').forEach((b) => b.addEventListener('click', () => trk?.classList.remove('is-sub')));

    const norm = (t) => (t || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
    const renderMobileSearch = () => {
        const results = $('[data-sf-m-results]');
        const browse = $('[data-sf-m-browse]');
        if (!results || !browse) return;
        const q = norm(mq.value);
        results.hidden = !q;
        browse.hidden = !!q;
        if (!q) { results.replaceChildren(); return; }
        trk?.classList.remove('is-sub');
        const seen = new Set();
        const hits = $$('.sf-m-a').filter((a) => {
            const key = a.getAttribute('href');
            if (seen.has(key) || !norm(a.textContent).includes(q)) return false;
            seen.add(key);
            return true;
        }).slice(0, 10);
        const label = document.createElement('p');
        label.className = 'sf-m-lbl';
        label.textContent = 'Resultados';
        if (!hits.length) {
            const p = document.createElement('p');
            p.className = 'sf-m-empty';
            p.textContent = `Nada encontrado para “${mq.value.trim()}”. Tente “chuva” ou “estoque”, ou pergunte ao Pezinho.`;
            results.replaceChildren(label, p);
            return;
        }
        results.replaceChildren(label, ...hits.map((a) => {
            const sub = a.closest('[data-sf-m-sub]');
            const r = document.createElement('a');
            r.className = 'sf-m-row';
            r.href = a.getAttribute('href');
            const grow = document.createElement('span');
            grow.className = 'sf-grow';
            grow.textContent = a.textContent.trim();
            const small = document.createElement('small');
            small.textContent = sub ? (sub.querySelector('h3')?.textContent || '') : '';
            grow.append(small);
            r.append(grow, svgIcon('chev', 'sf-ic sf-m-chev'));
            return r;
        }));
    };
    mq?.addEventListener('input', renderMobileSearch);

    /* ---------------- fechar ao clicar fora / Esc / mudar tamanho ---------------- */
    document.addEventListener('click', (e) => {
        if (openPanel && !openPanel.contains(e.target) && !(openAnchor && openAnchor.contains(e.target))) closePanel();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key !== 'Escape') return;
        if (openPanel) { const a = openAnchor; closePanel(); a?.focus(); }
        if (sheet) closeSheet();
        hideTip();
    });
    window.addEventListener('resize', () => { closePanel(); hideTip(); if (desktopMq.matches) closeSheet(); });
    $('.sf-nav', sb)?.addEventListener('scroll', hideTip, { passive: true });
    $('.sf-r-nav', sb)?.addEventListener('scroll', () => { hideTip(); closePanel(); }, { passive: true });

    renderFavs();
})();
