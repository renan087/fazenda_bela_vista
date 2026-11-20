/* Classificação: modo comparar lotes e gerenciador de atributos sensoriais. */
(() => {
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

    /* ---------- comparar lotes ---------- */
    const list = document.querySelector('[data-cl-list]');
    if (list) {
        const bar = list.querySelector('[data-cl-compare-bar]');
        const text = list.querySelector('[data-cl-compare-text]');
        const go = list.querySelector('[data-cl-compare-go]');
        const picks = [...list.querySelectorAll('[data-cl-pick]')];
        const mainToggle = document.querySelector('.cl-compare-btn');
        let on = false;
        const sync = () => {
            const chosen = picks.filter((p) => p.checked).map((p) => p.value);
            picks.forEach((p) => { p.disabled = !p.checked && chosen.length >= 3; });
            text.textContent = chosen.length ? `${chosen.length} lote${chosen.length > 1 ? 's' : ''} escolhido${chosen.length > 1 ? 's' : ''} (até 3)` : 'Marque até 3 lotes para comparar';
            go.href = `/producao/classificacao/comparar?lotes=${chosen.join(',')}`;
            go.toggleAttribute('aria-disabled', chosen.length < 2);
            go.classList.toggle('is-off', chosen.length < 2);
        };
        list.querySelectorAll('[data-cl-compare-toggle]').forEach((btn) => btn.addEventListener('click', () => {
            on = !on;
            bar.hidden = !on;
            mainToggle?.setAttribute('aria-pressed', String(on));
            picks.forEach((p) => { p.hidden = !on; p.nextElementSibling.hidden = on; if (!on) p.checked = false; });
            sync();
            if (on) picks[0]?.focus();
        }));
        picks.forEach((p) => p.addEventListener('change', sync));
        go.addEventListener('click', (e) => { if (go.classList.contains('is-off')) { e.preventDefault(); text.textContent = 'Marque pelo menos 2 lotes.'; } });
    }

    /* ---------- atributos sensoriais ---------- */
    const modal = document.querySelector('[data-cl-attr-modal]');
    const dataEl = document.querySelector('[data-cl-attr-data]');
    if (!modal || !dataEl) return;
    const initial = JSON.parse(dataEl.textContent);
    const $ = (s) => modal.querySelector(s);
    const PRESETS = { sca: ['aroma', 'sabor', 'finalizacao', 'acidez', 'corpo', 'balanco', 'docura', 'uniformidade', 'geral'], simples: ['aroma', 'sabor', 'acidez', 'corpo', 'docura'] };
    let state;
    let editing = null;
    let confirming = null;
    let dragKey = null;
    let tempId = 0;

    const reset = () => {
        state = { scale: initial.scale, preset: 'custom', deleted: [], items: initial.items.map((i) => ({ ...i })) };
        editing = confirming = null;
    };
    const active = () => state.items.filter((i) => i.active);
    const inactive = () => state.items.filter((i) => !i.active);
    const meta = (i) => [i.hint, i.uses ? `usado em ${i.uses} análise${i.uses > 1 ? 's' : ''}` : 'ainda não usado'].filter(Boolean).join(' · ');
    const icon = {
        up: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" aria-hidden="true"><path d="m6 15 6-6 6 6"/></svg>',
        down: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>',
        edit: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M4 20h4L19 9l-4-4L4 16v4Z"/></svg>',
        trash: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3"/></svg>',
        grip: '<svg viewBox="0 0 14 18" fill="currentColor" aria-hidden="true"><circle cx="4" cy="4" r="1.6"/><circle cx="10" cy="4" r="1.6"/><circle cx="4" cy="9" r="1.6"/><circle cx="10" cy="9" r="1.6"/><circle cx="4" cy="14" r="1.6"/><circle cx="10" cy="14" r="1.6"/></svg>',
    };
    const sw = (item) => `<button type="button" class="cl-switch" role="switch" aria-checked="${item.active}" aria-label="Mostrar ${esc(item.label)} na prova" data-act="toggle"><span></span></button>`;
    const trash = (item) => (!item.uses && initial.canDelete ? `<button type="button" class="cl-icon is-danger" data-act="ask" aria-label="Excluir ${esc(item.label)}" title="Excluir (ainda não usado)">${icon.trash}</button>` : '');
    const confirmBox = (item) => (confirming === item.key ? `<div class="cl-attr-confirm" role="alert"><span>Excluir “${esc(item.label)}”? Não dá para desfazer.</span><button type="button" class="sf-btn sf-btn-secondary" data-act="keep">Manter</button><button type="button" class="sf-btn cl-btn-danger" data-act="delete">Excluir</button></div>` : '');

    function rowHtml(item, index, total) {
        if (editing === item.key) {
            return `<li class="cl-attr-row is-editing" data-key="${esc(item.key)}">
                <div class="cl-attr-fields">
                    <label>Nome<input data-edit-label maxlength="80" value="${esc(item.label)}"></label>
                    <label>Explicação para a equipe<input data-edit-hint maxlength="160" value="${esc(item.hint || '')}" placeholder="Opcional, aparece como dica na prova"></label>
                </div>
                ${item.uses ? `<p class="cl-attr-note">Usado em ${item.uses} análises. Mudar o nome atualiza o histórico; as notas antigas ficam iguais.</p>` : ''}
                <p class="cl-attr-error" data-edit-error role="alert" hidden></p>
                <div class="cl-attr-btns"><button type="button" class="sf-btn sf-btn-secondary" data-act="cancel-edit">Cancelar</button><button type="button" class="sf-btn sf-btn-primary" data-act="save-edit">Salvar</button></div>
            </li>`;
        }
        return `<li class="cl-attr-row" data-key="${esc(item.key)}" draggable="true">
            <div class="cl-attr-line">
                <span class="cl-grip">${icon.grip}</span>
                <span class="cl-arrows"><button type="button" data-act="up" aria-label="Subir ${esc(item.label)}" ${index === 0 ? 'disabled' : ''}>${icon.up}</button><button type="button" data-act="down" aria-label="Descer ${esc(item.label)}" ${index === total - 1 ? 'disabled' : ''}>${icon.down}</button></span>
                <span class="cl-attr-name"><strong>${esc(item.label)}</strong><small>${esc(meta(item))}</small></span>
                <button type="button" class="cl-icon" data-act="edit" aria-label="Editar ${esc(item.label)}">${icon.edit}</button>
                ${trash(item)}${sw(item)}
            </div>${confirmBox(item)}
        </li>`;
    }

    function offHtml(item) {
        return `<li class="cl-attr-row is-off" data-key="${esc(item.key)}">
            <div class="cl-attr-line"><span class="cl-attr-name"><strong>${esc(item.label)}</strong><small>${esc(meta(item))}</small></span>${trash(item)}${sw(item)}</div>${confirmBox(item)}
        </li>`;
    }

    function preview() {
        const items = active();
        const n = Math.max(items.length, 3);
        const C = 120; const R = 80;
        const pt = (i, k) => { const a = -Math.PI / 2 + i * 2 * Math.PI / n; return `${(C + k * Math.cos(a)).toFixed(1)},${(C + k * Math.sin(a)).toFixed(1)}`; };
        const vals = [8, 7.5, 7.25, 7.75, 7.5, 8, 7, 7.5, 7.75, 7.25];
        const ring = (f) => Array.from({ length: n }, (_, i) => pt(i, R * f)).join(' ');
        const shape = items.length >= 3 ? items.map((_, i) => pt(i, R * vals[i % vals.length] / 10)).join(' ') : '';
        const labels = items.map((it, i) => { const a = -Math.PI / 2 + i * 2 * Math.PI / n; const l = it.label.length > 12 ? `${it.label.slice(0, 11)}…` : it.label;
            return `<text x="${(C + (R + 18) * Math.cos(a)).toFixed(0)}" y="${(C + (R + 18) * Math.sin(a) + 4).toFixed(0)}" text-anchor="middle" font-size="10" fill="#5B6660">${esc(l)}</text>`; }).join('');
        $('[data-cl-preview-svg]').innerHTML = `<polygon points="${ring(1)}" fill="none" stroke="#E3E8E2"/><polygon points="${ring(0.5)}" fill="none" stroke="#E3E8E2"/>${shape ? `<polygon points="${shape}" fill="#9BCB6B" fill-opacity=".45" stroke="#446A36" stroke-width="2"/>` : ''}${labels}`;
        $('[data-cl-readability]').textContent = items.length < 3 ? `Com ${items.length} ativo${items.length === 1 ? '' : 's'}, o gráfico não aparece (mínimo 3).`
            : items.length > 10 ? `Com ${items.length} ativos o gráfico fica poluído. O ideal é de 5 a 10.` : `Com ${items.length} ativos o gráfico fica fácil de ler.`;
    }

    function render() {
        const act = active(); const off = inactive();
        $('[data-cl-active-title]').textContent = `Aparecem na prova · ${act.length}`;
        $('[data-cl-inactive-title]').textContent = `Desativados · ${off.length}`;
        $('[data-cl-active-list]').innerHTML = act.map((item, i) => rowHtml(item, i, act.length)).join('');
        $('[data-cl-inactive-list]').innerHTML = off.length ? off.map(offHtml).join('') : '<li class="cl-attr-empty">Nenhum atributo desativado.</li>';
        modal.querySelectorAll('[data-preset]').forEach((b) => b.setAttribute('aria-checked', String(b.dataset.preset === state.preset)));
        modal.querySelectorAll('[data-scale]').forEach((b) => b.setAttribute('aria-checked', String(Number(b.dataset.scale) === state.scale)));
        preview();
        const field = modal.querySelector('[data-edit-label]');
        if (field) field.focus();
    }

    const find = (key) => state.items.find((i) => i.key === key);
    const dupName = (label, except) => state.items.some((i) => i.key !== except && i.label.trim().toLowerCase() === label.trim().toLowerCase());
    function moveWithinActive(fromKey, toKey, after) {
        const act = active(); const off = inactive();
        const moving = act.find((i) => i.key === fromKey);
        const rest = act.filter((i) => i.key !== fromKey);
        let idx = rest.findIndex((i) => i.key === toKey);
        if (idx < 0) return;
        if (after) idx += 1;
        rest.splice(idx, 0, moving);
        state.items = [...rest, ...off];
        state.preset = 'custom';
    }

    modal.addEventListener('click', (e) => {
        const btn = e.target.closest('button');
        if (!btn) return;
        const row = btn.closest('[data-key]');
        const item = row ? find(row.dataset.key) : null;
        const act = btn.dataset.act;
        if (btn.dataset.preset) {
            state.preset = btn.dataset.preset;
            if (PRESETS[state.preset]) {
                const set = PRESETS[state.preset];
                state.items.forEach((i) => { i.active = set.includes(i.key); });
                state.items.sort((a, b) => (set.indexOf(a.key) + 1 || 99) - (set.indexOf(b.key) + 1 || 99));
                state.scale = 10;
            }
            render(); return;
        }
        if (btn.dataset.scale) { state.scale = Number(btn.dataset.scale); render(); return; }
        if (!item) return;
        if (act === 'toggle') { item.active = !item.active; state.preset = 'custom'; confirming = null; render(); }
        else if (act === 'up' || act === 'down') {
            const acts = active(); const i = acts.indexOf(item); const other = acts[act === 'up' ? i - 1 : i + 1];
            if (other) { moveWithinActive(item.key, other.key, act === 'down'); render(); modal.querySelector(`[data-key="${CSS.escape(item.key)}"] [data-act="${act}"]`)?.focus(); }
        } else if (act === 'edit') { editing = item.key; confirming = null; render(); }
        else if (act === 'cancel-edit') { editing = null; render(); }
        else if (act === 'save-edit') {
            const label = row.querySelector('[data-edit-label]').value.trim().replace(/\s+/g, ' ');
            const err = row.querySelector('[data-edit-error]');
            if (!label) { err.hidden = false; err.textContent = 'Informe o nome do atributo.'; return; }
            if (dupName(label, item.key)) { err.hidden = false; err.textContent = 'Já existe um atributo com esse nome.'; return; }
            item.label = label; item.hint = row.querySelector('[data-edit-hint]').value.trim(); editing = null; state.preset = 'custom'; render();
        } else if (act === 'ask') { confirming = item.key; render(); modal.querySelector('[data-act="keep"]')?.focus(); }
        else if (act === 'keep') { confirming = null; render(); }
        else if (act === 'delete') {
            if (!item.key.startsWith('new-')) state.deleted.push(item.key);
            state.items = state.items.filter((i) => i !== item); confirming = null; render();
        }
    });

    modal.addEventListener('dragstart', (e) => {
        const row = e.target.closest('[data-key][draggable="true"]');
        if (!row) return;
        dragKey = row.dataset.key;
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', dragKey);
        requestAnimationFrame(() => row.classList.add('is-dragging'));
    });
    modal.addEventListener('dragover', (e) => {
        const row = e.target.closest('[data-cl-active-list] [data-key]');
        if (!row || !dragKey) return;
        e.preventDefault();
        modal.querySelectorAll('.is-over-top,.is-over-bottom').forEach((r) => r.classList.remove('is-over-top', 'is-over-bottom'));
        if (row.dataset.key === dragKey) return;
        const box = row.getBoundingClientRect();
        row.classList.add(e.clientY > box.top + box.height / 2 ? 'is-over-bottom' : 'is-over-top');
    });
    modal.addEventListener('drop', (e) => {
        const row = e.target.closest('[data-cl-active-list] [data-key]');
        if (!row || !dragKey) return;
        e.preventDefault();
        if (row.dataset.key !== dragKey) moveWithinActive(dragKey, row.dataset.key, row.classList.contains('is-over-bottom'));
        dragKey = null; render();
    });
    modal.addEventListener('dragend', () => { dragKey = null; render(); });

    /* toque no celular: segurar a alça e arrastar */
    modal.addEventListener('touchstart', (e) => {
        const grip = e.target.closest('.cl-grip');
        if (!grip) return;
        dragKey = grip.closest('[data-key]').dataset.key;
    }, { passive: true });
    modal.addEventListener('touchmove', (e) => {
        if (!dragKey) return;
        e.preventDefault();
        const touch = e.touches[0];
        const row = document.elementFromPoint(touch.clientX, touch.clientY)?.closest('[data-cl-active-list] [data-key]');
        modal.querySelectorAll('.is-over-top,.is-over-bottom').forEach((r) => r.classList.remove('is-over-top', 'is-over-bottom'));
        if (row && row.dataset.key !== dragKey) { const box = row.getBoundingClientRect(); row.classList.add(touch.clientY > box.top + box.height / 2 ? 'is-over-bottom' : 'is-over-top'); }
    }, { passive: false });
    modal.addEventListener('touchend', () => {
        const target = modal.querySelector('.is-over-top,.is-over-bottom');
        if (dragKey && target) moveWithinActive(dragKey, target.dataset.key, target.classList.contains('is-over-bottom'));
        if (dragKey) { dragKey = null; render(); }
    });

    const addForm = $('[data-cl-add-form]');
    const addOpen = $('[data-cl-add-open]');
    addOpen.addEventListener('click', () => { addForm.hidden = false; addOpen.hidden = true; $('[data-cl-new-label]').value = ''; $('[data-cl-new-hint]').value = ''; $('[data-cl-add-error]').hidden = true; $('[data-cl-new-label]').focus(); });
    $('[data-cl-add-cancel]').addEventListener('click', () => { addForm.hidden = true; addOpen.hidden = false; addOpen.focus(); });
    $('[data-cl-new-label]').addEventListener('input', (e) => {
        const err = $('[data-cl-add-error]');
        const dup = e.target.value.trim() && dupName(e.target.value, null);
        err.hidden = !dup;
        err.textContent = dup ? 'Já existe um atributo com esse nome. Para usá-lo, ative em “Desativados”.' : '';
    });
    $('[data-cl-add-confirm]').addEventListener('click', () => {
        const label = $('[data-cl-new-label]').value.trim().replace(/\s+/g, ' ');
        const err = $('[data-cl-add-error]');
        if (!label) { err.hidden = false; err.textContent = 'Informe o nome do atributo.'; return; }
        if (dupName(label, null)) { err.hidden = false; err.textContent = 'Já existe um atributo com esse nome. Para usá-lo, ative em “Desativados”.'; return; }
        const act = active(); const off = inactive();
        tempId += 1;
        state.items = [...act, { key: `new-${tempId}`, label, hint: $('[data-cl-new-hint]').value.trim(), active: true, uses: 0, builtin: false }, ...off];
        state.preset = 'custom';
        addForm.hidden = true; addOpen.hidden = false; render();
    });

    const open = () => { reset(); addForm.hidden = true; addOpen.hidden = false; $('[data-cl-save-error]').hidden = true; render(); modal.showModal(); };
    document.querySelectorAll('[data-cl-attr-open]').forEach((b) => b.addEventListener('click', open));
    modal.querySelectorAll('[data-cl-attr-close]').forEach((b) => b.addEventListener('click', () => modal.close()));
    $('[data-cl-attr-save]').addEventListener('click', async (e) => {
        const btn = e.currentTarget;
        const err = $('[data-cl-save-error]');
        if (!active().length) { err.hidden = false; err.textContent = 'Deixe pelo menos um atributo ativo.'; return; }
        btn.disabled = true;
        const body = new FormData();
        body.append('csrf_token', initial.csrf);
        body.append('payload', JSON.stringify({ scale: state.scale, preset: state.preset, deleted: state.deleted,
            items: state.items.map((i) => ({ key: i.key.startsWith('new-') ? '' : i.key, label: i.label, hint: i.hint || '', active: i.active })) }));
        try {
            const res = await fetch('/producao/classificacao/atributos/salvar', { method: 'POST', body, credentials: 'same-origin' });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) { err.hidden = false; err.textContent = data.error || 'Não foi possível salvar. Tente de novo.'; return; }
            window.location.reload();
        } catch (ex) {
            err.hidden = false; err.textContent = 'Sem conexão. Confira a internet e tente de novo.';
        } finally { btn.disabled = false; }
    });
    if (initial.open) open();
})();
