(() => {
    const page = document.querySelector('[data-vr-page]');
    const modal = document.querySelector('[data-vr-form-modal]');
    const normalize = (value) => (value || '').toString().normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
    const mobile = window.matchMedia('(max-width: 1023px)');

    /* ---------- Lista, filtros e painel ---------- */
    if (page) {
        const items = Array.from(page.querySelectorAll('[data-vr-item]'));
        const empty = page.querySelector('[data-vr-empty]');
        const detail = page.querySelector('[data-vr-detail]');
        const search = page.querySelector('[data-vr-search]');
        let filter = 'todas';
        let selected = page.dataset.selected || '';

        const applyFilters = () => {
            const needle = normalize(search?.value);
            const visibleIds = new Set();
            items.forEach((el) => {
                const used = el.dataset.vrUsed === '1';
                const okFilter = filter === 'todas' || (filter === 'uso' ? used : !used);
                const ok = okFilter && (!needle || normalize(el.dataset.vrSearch).includes(needle));
                el.hidden = !ok;
                if (ok) visibleIds.add(el.dataset.vrItem);
            });
            if (empty) empty.classList.toggle('hidden', visibleIds.size > 0);
        };

        const select = (id, { open = true } = {}) => {
            selected = String(id);
            items.forEach((el) => {
                const on = el.dataset.vrItem === selected;
                el.classList.toggle('is-selected', on);
                el.setAttribute('aria-pressed', on ? 'true' : 'false');
            });
            page.querySelectorAll('[data-vr-panel]').forEach((panel) => { panel.hidden = panel.dataset.vrPanel !== selected; });
            if (open && mobile.matches) {
                page.classList.add('vr-sheet-open');
                document.body.style.overflow = 'hidden';
            }
            try { const url = new URL(window.location.href); url.searchParams.set('variedade', selected); url.searchParams.delete('edit_id'); history.replaceState(null, '', url); } catch (e) { /* sem histórico */ }
        };

        const closeSheet = () => {
            page.classList.remove('vr-sheet-open');
            document.body.style.overflow = '';
        };

        items.forEach((el) => el.addEventListener('click', () => select(el.dataset.vrItem)));
        page.querySelectorAll('[data-vr-close-detail]').forEach((el) => el.addEventListener('click', closeSheet));
        document.addEventListener('keydown', (event) => { if (event.key === 'Escape' && page.classList.contains('vr-sheet-open') && !page.querySelector('[data-vr-menu]:not([hidden])')) closeSheet(); });
        mobile.addEventListener?.('change', () => { if (!mobile.matches) closeSheet(); });

        search?.addEventListener('input', applyFilters);
        page.querySelectorAll('[data-vr-filter]').forEach((chip) => chip.addEventListener('click', () => {
            filter = chip.dataset.vrFilter;
            page.querySelectorAll('[data-vr-filter]').forEach((c) => { const on = c === chip; c.classList.toggle('is-on', on); c.setAttribute('aria-pressed', on ? 'true' : 'false'); });
            applyFilters();
        }));

        const showTab = (panel, name) => {
            panel.querySelectorAll('[data-vr-tab]').forEach((t) => t.setAttribute('aria-selected', t.dataset.vrTab === name ? 'true' : 'false'));
            panel.querySelectorAll('[data-vr-tabpanel]').forEach((p) => { p.hidden = p.dataset.vrTabpanel !== name; });
        };
        page.querySelectorAll('[data-vr-panel]').forEach((panel) => {
            panel.querySelectorAll('[data-vr-tab]').forEach((tab) => tab.addEventListener('click', () => showTab(panel, tab.dataset.vrTab)));
            const dialog = panel.querySelector('[data-vr-del]');
            panel.querySelector('[data-vr-del-open]')?.addEventListener('click', () => dialog?.showModal());
            dialog?.querySelectorAll('[data-vr-del-close]').forEach((btn) => btn.addEventListener('click', () => {
                dialog.close();
                if (btn.dataset.vrGotoTab) showTab(panel, btn.dataset.vrGotoTab);
            }));
        });

        /* Menu ⋯ do painel: fecha ao clicar fora e com Esc */
        const closeMenus = (except) => page.querySelectorAll('[data-vr-menu]').forEach((menu) => {
            if (menu === except || menu.hidden) return;
            menu.hidden = true;
            menu.parentElement.querySelector('[data-vr-menu-btn]')?.setAttribute('aria-expanded', 'false');
        });
        page.querySelectorAll('[data-vr-menu-btn]').forEach((btn) => btn.addEventListener('click', (event) => {
            event.stopPropagation();
            const menu = btn.parentElement.querySelector('[data-vr-menu]');
            closeMenus(menu);
            menu.hidden = !menu.hidden;
            btn.setAttribute('aria-expanded', menu.hidden ? 'false' : 'true');
            if (!menu.hidden) menu.querySelector('[role=menuitem]')?.focus();
        }));
        document.addEventListener('click', (event) => { if (!event.target.closest('.vr-menu-wrap')) closeMenus(); });
        document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeMenus(); });
        page.querySelectorAll('[data-vr-menu] [role=menuitem]').forEach((item) => item.addEventListener('click', () => closeMenus()));

        if (detail && selected) select(selected, { open: false });
        applyFilters();
    }

    /* ---------- Formulário em 3 passos ---------- */
    if (!modal) return;
    const form = modal.querySelector('[data-vr-form]');
    const editing = modal.dataset.editing === '1';
    const nameInput = form.querySelector('[data-vr-name]');
    const nameMsg = form.querySelector('[data-vr-name-msg]');
    const readJson = (sel, fallback) => { try { return JSON.parse(form.querySelector(sel)?.textContent || ''); } catch (e) { return fallback; } };
    const existing = readJson('[data-vr-existing]', []).map(normalize);
    const presets = readJson('[data-vr-presets]', []);
    const btnBack = form.querySelector('[data-vr-back]');
    const btnNext = form.querySelector('[data-vr-next]');
    const btnSubmit = form.querySelector('[data-vr-submit]');
    const btnSkip = form.querySelector('[data-vr-skip]');
    const MONTHS = ['abr', 'mai', 'jun', 'jul', 'ago', 'set'];
    const WIN = { pre: [0, 1], med: [1, 2], mtar: [2, 3], tar: [3, 4] };
    let step = 1;

    const checked = (name) => form.querySelector(`input[name="${name}"]:checked`);
    const color = () => checked('map_color')?.value || '#446A36';

    const updateWindow = () => {
        const key = checked('maturation_cycle')?.dataset.vrCycle || 'med';
        const [a, b] = WIN[key];
        form.querySelectorAll('[data-vr-window] .vr-window-bar').forEach((bar, i) => { bar.style.background = i >= a && i <= b ? color() : ''; });
        const label = form.querySelector('[data-vr-window-label]');
        if (label) label.textContent = `${MONTHS[a]} a ${MONTHS[b]}`;
    };

    const tags = () => {
        const list = Array.from(form.querySelectorAll('[data-vr-tag]:checked')).map((el) => el.value);
        const extra = (form.querySelector('[data-vr-tag-extra]')?.value || '').split(',').map((t) => t.trim()).filter(Boolean);
        return list.concat(extra);
    };

    const nameError = () => {
        const value = (nameInput.value || '').trim();
        if (!value) return 'Escreva o nome da variedade.';
        if (existing.includes(normalize(value))) return `${value} já está cadastrada. Edite a existente ou use outro nome (ex.: com a linhagem).`;
        return '';
    };
    const showNameError = (msg) => {
        nameMsg.textContent = msg;
        nameMsg.hidden = !msg;
        nameInput.classList.toggle('is-invalid', !!msg);
        nameInput.setAttribute('aria-invalid', msg ? 'true' : 'false');
    };

    const plotInputs = Array.from(form.querySelectorAll('[data-vr-plot]'));
    const refreshSwaps = () => {
        let anySwap = false;
        plotInputs.forEach((input) => {
            const swap = input.checked && !input.disabled && !!input.dataset.current;
            const tag = input.closest('label')?.querySelector('[data-vr-swap-tag]');
            if (tag) tag.hidden = !swap;
            input.closest('label')?.classList.toggle('is-on', input.checked);
            anySwap = anySwap || swap;
        });
        const note = form.querySelector('[data-vr-swap-note]');
        if (note) note.hidden = !anySwap;
    };

    const buildReview = () => {
        const name = nameInput.value.trim();
        form.querySelector('[data-vr-review-color]').style.background = color();
        form.querySelector('[data-vr-review-name]').textContent = name;
        const parts = [checked('species')?.value, checked('maturation_cycle') ? `ciclo ${checked('maturation_cycle').value.toLowerCase()}` : null,
            checked('plant_height') ? `porte ${checked('plant_height').value.toLowerCase()}` : null,
            checked('rust_resistance') ? `ferrugem: ${checked('rust_resistance').value.toLowerCase()}` : null].filter(Boolean);
        form.querySelector('[data-vr-review-meta]').textContent = parts.join(' · ');
        const t = tags();
        form.querySelector('[data-vr-review-tags]').textContent = `Na xícara: ${t.length ? t.join(', ').toLowerCase() : 'não informado'}`;
        const list = form.querySelector('[data-vr-changes]');
        list.replaceChildren();
        const add = (mark, text, cls) => {
            const li = document.createElement('li');
            const m = document.createElement('span'); m.className = `vr-change-mark ${cls || ''}`; m.textContent = mark; m.setAttribute('aria-hidden', 'true');
            const s = document.createElement('span'); s.textContent = text;
            li.append(m, s); list.append(li);
        };
        add(editing ? '✓' : '+', editing ? `Os dados de ${name} são atualizados.` : `${name} entra na lista de variedades.`, 'is-new');
        const chosen = plotInputs.filter((i) => i.checked && !i.disabled);
        chosen.forEach((i) => add('→', `${i.dataset.name} (${i.dataset.area} ha): ${i.dataset.current || 'sem variedade'} → ${name}`, 'is-swap'));
        add('•', chosen.length ? `${chosen.length} setor(es) passam a contar na área, nas plantas e na colheita de ${name}.` : 'Nenhum setor muda agora. Você pode vincular depois.');
        if (chosen.length) add('•', 'Colheitas e safras já lançadas continuam como estão.');
    };

    const goto = (n) => {
        step = n;
        form.querySelectorAll('[data-vr-step]').forEach((el) => { el.hidden = Number(el.dataset.vrStep) !== step; });
        form.querySelectorAll('[data-vr-step-ind]').forEach((el) => {
            const i = Number(el.dataset.vrStepInd);
            el.classList.toggle('is-done', i < step);
            el.classList.toggle('is-current', i === step);
        });
        btnBack.textContent = step === 1 ? 'Cancelar' : 'Voltar';
        btnNext.hidden = step === 3;
        btnSubmit.hidden = step !== 3;
        btnSkip.hidden = step !== 2;
        const echo = form.querySelector('[data-vr-name-echo]');
        if (echo) echo.textContent = nameInput.value.trim() || 'esta variedade';
        if (step === 3) buildReview();
        form.querySelector('[data-modal-scroll]')?.scrollTo?.({ top: 0 });
    };

    const closeModal = () => {
        if (editing && modal.dataset.closeUrl) { window.location.href = modal.dataset.closeUrl; return; }
        modal.close();
    };
    const openModal = (startStep = 1) => { goto(startStep); modal.showModal(); if (startStep === 1) setTimeout(() => nameInput.focus(), 30); };

    btnNext.addEventListener('click', () => {
        if (step === 1) {
            const msg = nameError();
            showNameError(msg);
            if (msg) { nameInput.focus(); return; }
        }
        goto(step + 1);
    });
    btnBack.addEventListener('click', () => (step === 1 ? closeModal() : goto(step - 1)));
    btnSkip.addEventListener('click', () => { plotInputs.forEach((i) => { if (!i.disabled) i.checked = false; }); refreshSwaps(); goto(3); });
    form.querySelectorAll('[data-vr-goto]').forEach((b) => b.addEventListener('click', () => goto(Number(b.dataset.vrGoto))));
    form.querySelectorAll('[data-vr-form-close]').forEach((b) => b.addEventListener('click', closeModal));
    modal.addEventListener('cancel', (event) => { event.preventDefault(); closeModal(); });
    nameInput.addEventListener('input', () => { if (!nameMsg.hidden) showNameError(nameError()); form.querySelectorAll('[data-vr-preset]').forEach((c) => { c.setAttribute('aria-pressed', 'false'); c.classList.remove('is-on'); }); });
    nameInput.addEventListener('keydown', (event) => { if (event.key === 'Enter') { event.preventDefault(); btnNext.click(); } });
    form.querySelectorAll('input[name="maturation_cycle"], input[name="map_color"]').forEach((el) => el.addEventListener('change', updateWindow));
    plotInputs.forEach((el) => el.addEventListener('change', refreshSwaps));

    form.querySelectorAll('[data-vr-preset]').forEach((chip) => chip.addEventListener('click', () => {
        const p = presets[Number(chip.dataset.vrPreset)];
        if (!p) return;
        nameInput.value = p.name;
        const setRadio = (name, value) => { const el = Array.from(form.querySelectorAll(`input[name="${name}"]`)).find((i) => normalize(i.value) === normalize(value)); if (el) el.checked = true; };
        setRadio('species', p.species); setRadio('maturation_cycle', p.cycle); setRadio('plant_height', p.height); setRadio('rust_resistance', p.rust);
        form.querySelectorAll('[data-vr-tag]').forEach((t) => { t.checked = (p.tags || []).includes(t.value); });
        form.querySelectorAll('[data-vr-preset]').forEach((c) => { const on = c === chip; c.classList.toggle('is-on', on); c.setAttribute('aria-pressed', on ? 'true' : 'false'); });
        showNameError(nameError());
        updateWindow();
    }));

    form.addEventListener('submit', (event) => {
        if (step !== 3) { event.preventDefault(); btnNext.click(); return; }
        const msg = nameError();
        if (msg) { event.preventDefault(); goto(1); showNameError(msg); return; }
        form.querySelector('[data-vr-flavor]').value = tags().join(', ').slice(0, 180);
        btnSubmit.disabled = true;
        btnSubmit.textContent = 'Salvando…';
    });

    document.querySelectorAll('[data-vr-new]').forEach((b) => b.addEventListener('click', () => openModal(1)));
    updateWindow();
    refreshSwaps();
    const params = new URLSearchParams(window.location.search);
    if (editing) openModal(params.get('passo') === '2' ? 2 : 1);
    else if (params.get('novo') === '1') openModal(1);
})();
