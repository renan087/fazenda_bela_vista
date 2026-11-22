/* Seletor de comprador da venda de café + modal de cadastro/edição (PF/PJ, dados para NF-e). */
(() => {
    const modal = document.querySelector('[data-bp-modal]');
    const pickers = [...document.querySelectorAll('[data-buyer-picker]')];
    if (!modal || !pickers.length) return;

    const form = modal.querySelector('[data-bp-form]');
    const $ = (sel, root = form) => root.querySelector(sel);
    const $$ = (sel, root = form) => [...root.querySelectorAll(sel)];
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const digits = (v) => String(v || '').replace(/\D/g, '');
    const csrf = () => $('[name=csrf_token]').value;
    let buyers = [];
    let loaded = false;
    let active = null;      // picker que abriu o modal
    let editingId = null;
    let duplicate = null;

    const initials = (name) => (name || '?').split(/\s+/).filter((w) => w.length > 2).slice(0, 2).map((w) => w[0]).join('').toUpperCase() || (name || '?')[0].toUpperCase();
    const where = (b) => (b.city ? `${b.city}/${b.state}` : 'sem endereço');

    const maskDoc = (value, pj) => {
        const d = digits(value).slice(0, pj ? 14 : 11);
        if (pj) return d.replace(/^(\d{2})(\d)/, '$1.$2').replace(/^(\d{2})\.(\d{3})(\d)/, '$1.$2.$3').replace(/\.(\d{3})(\d)/, '.$1/$2').replace(/(\d{4})(\d)/, '$1-$2');
        return d.replace(/(\d{3})(\d)/, '$1.$2').replace(/(\d{3})(\d)/, '$1.$2').replace(/(\d{3})(\d{1,2})$/, '$1-$2');
    };
    const maskCep = (value) => digits(value).slice(0, 8).replace(/^(\d{5})(\d)/, '$1-$2');

    async function loadBuyers(force) {
        if (loaded && !force) return buyers;
        try {
            const res = await fetch('/producao/compradores', { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
            buyers = res.ok ? (await res.json()).items : [];
            loaded = res.ok;
        } catch (e) { buyers = []; }
        return buyers;
    }

    /* ---------- seletor ---------- */
    function setupPicker(root) {
        const idInput = $('[data-bp-id]', root);
        const nameInput = $('[data-bp-name]', root);
        const card = $('[data-bp-card]', root);
        const choose = $('[data-bp-choose]', root);
        const warn = $('[data-bp-warn]', root);
        const panel = $('[data-bp-panel]', root);
        const query = $('[data-bp-query]', root);
        const results = $('[data-bp-results]', root);
        const newBtn = $('[data-bp-new]', root);
        const newLabel = $('[data-bp-new-label]', root);
        let selected = null;
        try { selected = JSON.parse(root.dataset.selected || '{}'); } catch (e) { selected = null; }
        if (!selected || !selected.id) selected = null;
        const legacyName = root.dataset.fallbackName || '';

        const api = {
            select(buyer) {
                selected = buyer;
                idInput.value = buyer ? buyer.id : '';
                nameInput.value = buyer ? buyer.name : '';
                render();
                closePanel();
                root.dispatchEvent(new CustomEvent('buyer:change', { bubbles: true, detail: buyer }));
            },
            current: () => selected,
        };

        function render() {
            const legacy = !selected && nameInput.value;
            card.hidden = !selected && !legacy;
            choose.hidden = !card.hidden;
            warn.hidden = true;
            if (selected) {
                const ok = selected.invoice_ready;
                card.innerHTML = `
                    <span class="bp-avatar" aria-hidden="true">${esc(initials(selected.name))}</span>
                    <div class="bp-card-text">
                        <strong>${esc(selected.name)}</strong>
                        <span>${selected.person_type === 'pf' ? 'CPF' : 'CNPJ'} ${esc(selected.document || 'não informado')} · ${esc(where(selected))}</span>
                        <span class="bp-chip ${ok ? 'is-ok' : 'is-warn'}">${ok ? 'Pronto para NF' : 'Faltam dados para NF'}</span>
                    </div>
                    <div class="bp-card-actions">
                        <button type="button" class="sf-btn sf-btn-secondary" data-bp-edit>Editar dados</button>
                        <button type="button" class="sf-btn sf-btn-secondary" data-bp-swap aria-expanded="false">Trocar</button>
                    </div>`;
                if (!ok) {
                    warn.hidden = false;
                    warn.innerHTML = `<span>Para emitir nota fiscal falta: <strong>${esc(selected.missing.join(', '))}</strong>. Você pode registrar a venda assim mesmo.</span>
                        <button type="button" data-bp-edit>Completar cadastro</button>`;
                }
            } else if (legacy) {
                card.innerHTML = `
                    <span class="bp-avatar" aria-hidden="true">${esc(initials(nameInput.value))}</span>
                    <div class="bp-card-text"><strong>${esc(nameInput.value)}</strong><span>Comprador digitado, ainda sem cadastro</span></div>
                    <div class="bp-card-actions">
                        <button type="button" class="sf-btn sf-btn-secondary" data-bp-register>Cadastrar</button>
                        <button type="button" class="sf-btn sf-btn-secondary" data-bp-swap aria-expanded="false">Trocar</button>
                    </div>`;
            }
        }

        function renderResults() {
            const q = query.value.trim();
            const fold = (s) => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
            const fq = fold(q);
            const dq = digits(q);
            const list = buyers.filter((b) => !fq || fold(b.name).includes(fq) || fold(b.trade_name).includes(fq) || (dq.length >= 3 && digits(b.document).includes(dq)));
            if (!loaded && !buyers.length) {
                results.innerHTML = '<p class="bp-empty">Carregando compradores…</p>';
            } else if (!list.length) {
                results.innerHTML = q
                    ? `<p class="bp-empty">Nenhum comprador encontrado com “${esc(q)}”.</p>`
                    : '<p class="bp-empty">Nenhum comprador cadastrado ainda. Cadastre o primeiro abaixo.</p>';
            } else {
                results.innerHTML = list.slice(0, 30).map((b) => `
                    <button type="button" class="bp-option" data-bp-pick="${b.id}">
                        <span class="bp-avatar is-sm" aria-hidden="true">${esc(initials(b.name))}</span>
                        <span class="bp-option-text"><strong>${esc(b.name)}</strong>
                        <small>${b.person_type === 'pf' ? 'CPF' : 'CNPJ'} ${esc(b.document || 'não informado')} · ${esc(where(b))}${b.last_sale ? ` · última venda ${esc(b.last_sale)}` : ''}</small></span>
                        <span class="bp-dot ${b.invoice_ready ? 'is-ok' : 'is-warn'}" title="${b.invoice_ready ? 'Pronto para NF' : 'Faltam dados para NF'}"></span>
                    </button>`).join('');
            }
            const looksDoc = dq.length >= 3 && dq.length === q.replace(/[\s./-]/g, '').length;
            newLabel.textContent = q && !looksDoc ? `Cadastrar “${q}” como novo comprador` : 'Cadastrar novo comprador';
        }

        async function openPanel() {
            panel.hidden = false;
            $$('[data-bp-swap]', root).forEach((b) => b.setAttribute('aria-expanded', 'true'));
            query.value = '';
            renderResults();
            query.focus();
            await loadBuyers();
            renderResults();
        }
        function closePanel() {
            panel.hidden = true;
            $$('[data-bp-swap]', root).forEach((b) => b.setAttribute('aria-expanded', 'false'));
        }

        root.addEventListener('click', (event) => {
            const t = event.target.closest('button');
            if (!t || !root.contains(t)) return;
            if (t.matches('[data-bp-choose], [data-bp-swap]')) { panel.hidden ? openPanel() : closePanel(); }
            else if (t.matches('[data-bp-edit]') && selected) openModal(api, { ...selected }, selected.id);
            else if (t.matches('[data-bp-register]')) openModal(api, { name: nameInput.value }, null);
            else if (t.matches('[data-bp-pick]')) api.select(buyers.find((b) => String(b.id) === t.dataset.bpPick));
            else if (t.matches('[data-bp-new]')) {
                const q = query.value.trim();
                const d = digits(q);
                const looksDoc = d.length >= 3 && d.length === q.replace(/[\s./-]/g, '').length;
                openModal(api, looksDoc ? { document: q, person_type: d.length === 11 ? 'pf' : 'pj' } : { name: q }, null);
            }
        });
        query.addEventListener('input', renderResults);
        query.addEventListener('keydown', (event) => { if (event.key === 'Escape') { event.preventDefault(); closePanel(); (choose.hidden ? $('[data-bp-swap]', root) : choose)?.focus(); } if (event.key === 'Enter') event.preventDefault(); });
        document.addEventListener('click', (event) => { if (!panel.hidden && !root.contains(event.target)) closePanel(); });

        const saleForm = root.closest('form');
        saleForm?.addEventListener('submit', (event) => {
            if (!nameInput.value.trim()) {
                event.preventDefault();
                openPanel();
            }
        });
        if (selected) render(); else { nameInput.value = legacyName; render(); }
        return api;
    }

    /* ---------- modal ---------- */
    const kindInputs = $$('[name=person_type]');
    const nfSwitch = $('[data-bp-nf]');
    const docInput = $('[data-bp-doc]');
    const exempt = $('[data-bp-exempt]');
    const rural = $('[data-bp-rural]');
    const ieInput = $('[name=state_registration]');
    const errorsBox = $('[data-bp-errors]');
    const dupBox = $('[data-bp-dup]');
    const isPJ = () => $('[name=person_type]:checked').value === 'pj';

    function setMsg(el, text, tone) {
        el.textContent = text || '';
        el.classList.toggle('is-error', tone === 'error');
        el.classList.toggle('is-ok', tone === 'ok');
    }

    function syncKind() {
        const pj = isPJ();
        $$('[data-bp-pj]').forEach((el) => { el.hidden = !pj; });
        $$('[data-bp-pf]').forEach((el) => { el.hidden = pj; });
        $('[data-bp-doc-label]').textContent = pj ? 'CNPJ' : 'CPF';
        $('[data-bp-name-label]').textContent = pj ? 'Razão social' : 'Nome completo';
        docInput.placeholder = pj ? '00.000.000/0000-00' : '000.000.000-00';
        docInput.value = maskDoc(docInput.value, pj);
        $('[data-bp-ie]').hidden = !pj && !rural.checked;
        $('[data-bp-ie-label]').textContent = pj ? 'Inscrição estadual' : 'IE de produtor rural';
        ieInput.disabled = pj && exempt.checked;
        ieInput.placeholder = ieInput.disabled ? 'Isento' : '';
        $('[data-bp-ie-req]').hidden = pj && exempt.checked;
        setMsg($('[data-bp-doc-msg]'), pj ? 'Digite o CNPJ e toque em “Buscar dados” para preencher o resto.' : 'Evita cadastrar a mesma pessoa duas vezes.');
        syncNf();
    }

    function syncNf() {
        const on = nfSwitch.checked;
        form.classList.toggle('is-nf', on);
        $('[data-bp-nf-hint]').textContent = on ? 'Campos marcados com * são exigidos na NF-e.' : 'Desligado, basta o nome. Os demais dados ficam opcionais.';
        $('[data-bp-addr-hint]').textContent = on ? '· necessário para a nota' : '· opcional';
        renderChecks();
    }

    function renderChecks() {
        const box = $('[data-bp-checks]');
        if (!nfSwitch.checked) { box.innerHTML = '<span class="bp-checks-note">Só o nome é obrigatório. Dá para completar depois.</span>'; return; }
        const pj = isPJ();
        const v = (n) => (form.elements[n].value || '').trim();
        const items = [[pj ? 'CNPJ' : 'CPF', digits(v('document')).length === (pj ? 14 : 11)], [pj ? 'Razão social' : 'Nome', !!v('name')]];
        if (pj) items.push(['Inscrição estadual', !!v('state_registration') || exempt.checked]);
        else if (rural.checked) items.push(['IE rural', !!v('state_registration')]);
        items.push(['Endereço', ['postal_code', 'street', 'number', 'district', 'city', 'state'].every((n) => v(n))]);
        box.innerHTML = '<span class="bp-checks-note">Para NF:</span>' + items.map(([label, ok]) => `<span class="bp-chip ${ok ? 'is-ok' : 'is-todo'}">${ok ? '✓' : '○'} ${esc(label)}</span>`).join('');
    }

    function checkDuplicate() {
        const d = digits(docInput.value);
        duplicate = d.length >= 11 ? buyers.find((b) => b.id !== editingId && digits(b.document) === d) : null;
        dupBox.hidden = !duplicate;
        if (duplicate) $('[data-bp-dup-text]').innerHTML = `Este ${isPJ() ? 'CNPJ' : 'CPF'} já está cadastrado como <strong>${esc(duplicate.name)}</strong>.`;
        $('[data-bp-save]').disabled = !!duplicate;
    }

    function fill(data) {
        Object.entries(data).forEach(([key, value]) => {
            const el = form.elements[key];
            if (!el || el.type === 'radio' || value == null) return;
            if (el.type === 'checkbox') el.checked = !!value; else el.value = value;
        });
    }

    async function openModal(picker, data, id) {
        active = picker;
        editingId = id;
        form.reset();
        errorsBox.hidden = true;
        dupBox.hidden = true;
        $('[data-bp-save]').disabled = false;
        setMsg($('[data-bp-cep-msg]'), '');
        $('[data-bp-title]').textContent = id ? 'Editar comprador' : 'Novo comprador';
        $('[data-bp-save]').textContent = id ? 'Salvar alterações' : 'Salvar e usar na venda';
        const kind = data.person_type === 'pf' ? 'pf' : 'pj';
        kindInputs.forEach((r) => { r.checked = r.value === kind; });
        fill({ ...data, document: data.document ? maskDoc(data.document, kind === 'pj') : '' });
        nfSwitch.checked = true;
        syncKind();
        if (typeof modal.showModal === 'function') modal.showModal(); else modal.setAttribute('open', '');
        (data.name ? docInput : form.elements.name).focus();
        await loadBuyers();
        checkDuplicate();
    }

    function closeModal() { if (modal.open) modal.close(); }

    form.addEventListener('change', (event) => {
        if (event.target.name === 'person_type') { docInput.value = ''; syncKind(); checkDuplicate(); }
        if (event.target === exempt || event.target === rural) { if (event.target === exempt && exempt.checked) ieInput.value = ''; syncKind(); }
        if (event.target === nfSwitch) syncNf();
        renderChecks();
    });
    form.addEventListener('input', (event) => {
        if (event.target === docInput) {
            docInput.value = maskDoc(docInput.value, isPJ());
            setMsg($('[data-bp-doc-msg]'), '');
            checkDuplicate();
        }
        if (event.target.name === 'postal_code') event.target.value = maskCep(event.target.value);
        renderChecks();
    });
    $$('[data-bp-close]').forEach((b) => b.addEventListener('click', closeModal));
    modal.addEventListener('click', (event) => { if (event.target === modal) closeModal(); });
    $('[data-bp-use-dup]').addEventListener('click', () => { if (duplicate && active) { active.select(duplicate); closeModal(); } });

    async function lookup(url, button, msgEl, onData) {
        button.disabled = true;
        const label = button.textContent;
        if (button.dataset.bpCnpj !== undefined) button.textContent = 'Buscando…';
        setMsg(msgEl, 'Consultando…');
        try {
            const res = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
            const body = await res.json().catch(() => ({}));
            if (!res.ok) { setMsg(msgEl, body.error || 'Não foi possível consultar agora. Preencha à mão.', 'error'); return; }
            onData(body.data);
        } catch (e) {
            setMsg(msgEl, 'Sem conexão para consultar agora. Preencha à mão.', 'error');
        } finally {
            button.disabled = false;
            button.textContent = label;
            renderChecks();
        }
    }

    $('[data-bp-cnpj]').addEventListener('click', (event) => {
        const d = digits(docInput.value);
        const msg = $('[data-bp-doc-msg]');
        if (d.length !== 14) { setMsg(msg, 'Digite os 14 números do CNPJ para buscar.', 'error'); docInput.focus(); return; }
        lookup(`/producao/compradores/consulta/cnpj/${d}`, event.currentTarget, msg, (data) => {
            const keep = (key) => (form.elements[key].value || '').trim();
            ['name', 'trade_name', 'postal_code', 'street', 'number', 'complement', 'district', 'city', 'state', 'municipality_code', 'phone', 'email']
                .forEach((key) => { if (data[key] && form.elements[key] && !keep(key)) form.elements[key].value = data[key]; });
            setMsg(msg, data.active === false ? 'Atenção: este CNPJ não está ativo na Receita.' : 'Dados preenchidos pelo CNPJ. Confira abaixo.', data.active === false ? 'error' : 'ok');
        });
    });

    $('[data-bp-cep-btn]').addEventListener('click', (event) => {
        const cep = form.elements.postal_code;
        const msg = $('[data-bp-cep-msg]');
        if (digits(cep.value).length !== 8) { setMsg(msg, 'O CEP precisa ter 8 números.', 'error'); cep.focus(); return; }
        lookup(`/producao/compradores/consulta/cep/${digits(cep.value)}`, event.currentTarget, msg, (data) => {
            ['street', 'district', 'city', 'state', 'municipality_code'].forEach((key) => { if (data[key] && form.elements[key]) form.elements[key].value = data[key]; });
            setMsg(msg, 'Endereço preenchido. Informe o número.', 'ok');
            form.elements.number.focus();
        });
    });

    form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const save = $('[data-bp-save]');
        if (!form.elements.name.value.trim()) {
            errorsBox.hidden = false;
            errorsBox.textContent = `Informe ${isPJ() ? 'a razão social' : 'o nome'} do comprador.`;
            form.elements.name.focus();
            return;
        }
        save.disabled = true;
        const body = new FormData(form);
        if (ieInput.disabled) body.set('state_registration', '');
        try {
            const res = await fetch(editingId ? `/producao/compradores/${editingId}` : '/producao/compradores', {
                method: 'POST', body, credentials: 'same-origin', headers: { Accept: 'application/json' },
            });
            const data = await res.json().catch(() => ({}));
            if (res.status === 409 && data.duplicate) {
                duplicate = data.duplicate;
                dupBox.hidden = false;
                $('[data-bp-dup-text]').innerHTML = `Este comprador já está cadastrado como <strong>${esc(duplicate.name)}</strong>.`;
                return;
            }
            if (!res.ok) {
                errorsBox.hidden = false;
                errorsBox.innerHTML = (data.errors || ['Não foi possível salvar. Tente de novo.']).map(esc).join('<br>');
                return;
            }
            await loadBuyers(true);
            active?.select(data.buyer);
            closeModal();
        } catch (e) {
            errorsBox.hidden = false;
            errorsBox.textContent = 'Sem conexão. Confira a internet e tente de novo.';
        } finally {
            save.disabled = !!duplicate && dupBox.hidden === false;
        }
    });

    pickers.forEach(setupPicker);
})();
