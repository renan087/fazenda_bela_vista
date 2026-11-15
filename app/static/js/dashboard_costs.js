// Dashboard: janela "Custo da safra" (detalhar). Navegacao em niveis: categoria > grupo > item > lancamento.
(() => {
    const modal = document.getElementById('db-cost-modal');
    if (!modal) return;
    const $ = (s, r = modal) => r.querySelector(s);
    const body = $('[data-db-c-body]');
    const crumbsEl = $('[data-db-c-crumbs]');
    const titleEl = $('[data-db-c-title]');
    const backBtn = $('[data-db-c-back]');
    let csrf = '';
    try { csrf = JSON.parse(document.getElementById('db-data').textContent).csrf || ''; } catch (e) {}

    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const brl = (v) => Number(v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 });
    const brl2 = (v) => Number(v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    const short = (v) => (Math.abs(v) >= 1000 ? `R$ ${(v / 1000).toLocaleString('pt-BR', { maximumFractionDigits: Math.abs(v) >= 100000 ? 0 : 1 })} mil` : brl(v));
    const pctTxt = (v) => `${Number(v).toLocaleString('pt-BR', { maximumFractionDigits: v < 10 ? 1 : 0 })}%`;

    let data = null;
    let path = [];       // caminhos (node.path) do nivel atual, do topo ate o no aberto
    let openKey = null;  // lancamento aberto
    let editing = false;
    let toast = '';

    const findNode = () => {
        let node = data.tree;
        const chain = [node];
        for (const p of path) {
            const next = (node.children || []).find((c) => c.path === p);
            if (!next) break;
            node = next; chain.push(node);
        }
        path = chain.slice(1).map((n) => n.path);
        return chain;
    };
    const colorOf = (chain) => (chain.find((n) => n.color) || {}).color || '#446a36';

    async function load(keep = false) {
        if (!keep) body.innerHTML = '<p class="db-muted db-cost-msg">Carregando…</p>';
        try {
            const res = await fetch('/api/dashboard/custos', { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
            data = res.ok ? await res.json() : { status: 'error' };
        } catch (e) { data = { status: 'error' }; }
        render();
    }

    function stats(node, chain) {
        const total = data.tree.value || 0;
        const delta = node.prev ? (node.value / node.prev - 1) * 100 : null;
        const items = [
            [chain.length === 1 ? 'Custo total' : 'Total', short(node.value), chain.length === 1 ? `${node.count} lançamento(s)` : `${pctTxt(total ? node.value / total * 100 : 0)} do custo da safra`],
            ['Por hectare', data.area ? brl(node.value / data.area) : '—', data.area ? `${data.area.toLocaleString('pt-BR')} ha` : 'área não informada'],
            ['Por saca', data.sacks ? brl(node.value / data.sacks) : '—', data.sacks ? `${Math.round(data.sacks).toLocaleString('pt-BR')} sc estimadas` : 'sem estimativa'],
            ['vs safra anterior', delta == null ? '—' : `${delta > 0 ? '+' : ''}${delta.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}%`, node.prev ? `era ${short(node.prev)}` : 'sem comparação',
                delta == null ? '' : delta > 8 ? ' is-up' : delta < -3 ? ' is-down' : ''],
        ];
        return `<dl class="db-cost-stats">${items.map(([l, v, s, c]) => `<div><dt>${l}</dt><dd class="${c || ''}">${v}</dd><dd class="db-muted">${esc(s)}</dd></div>`).join('')}</dl>`;
    }

    function childList(node, chain) {
        const color = colorOf(chain);
        const kids = node.children || [];
        const seg = kids.map((c) => `<span style="flex:${c.value};background:${c.color || color};opacity:${c.color ? 1 : (0.5 + 0.5 * c.value / node.value).toFixed(2)}" title="${esc(c.name)} · ${esc(brl(c.value))}"></span>`).join('');
        const rows = kids.map((c) => {
            const pct = node.value ? c.value / node.value * 100 : 0;
            const d = c.prev ? (c.value / c.prev - 1) * 100 : null;
            const flag = c.path.endsWith('/nao_identificado') ? '<span class="db-cost-flag is-warn">Revisar</span>'
                : d != null && Math.abs(d) >= 5 ? `<span class="db-cost-flag${d > 0 ? ' is-up' : ' is-down'}">${d > 0 ? '+' : '−'}${Math.abs(d).toFixed(0)}%</span>` : '';
            const sub = c.children ? `${c.children.length} ${c.children[0] && c.children[0].entries ? 'item(ns)' : 'grupo(s)'}` : '';
            const low = c.low ? ` · ${c.low} para revisar` : '';
            return `<button type="button" class="db-cost-row" data-go="${esc(c.path)}">
                <span class="db-cost-dot" style="background:${c.color || color}"></span>
                <span class="db-cost-row-main"><span class="db-cost-row-name">${esc(c.name)}${flag}</span>
                    <span class="db-muted">${c.count} lançamento(s)${sub ? ` · ${sub}` : ''}${low}</span>
                    <span class="db-cost-row-bar"><span style="width:${Math.max(2, pct)}%;background:${c.color || color}"></span></span></span>
                <span class="db-cost-row-val"><strong>${short(c.value)}</strong><span class="db-muted">${pctTxt(pct)}</span></span>
                <span class="db-cost-chev" aria-hidden="true">›</span></button>`;
        }).join('');
        return `${kids.length > 1 ? `<div class="db-cost-seg" aria-hidden="true">${seg}</div>` : ''}${rows}`;
    }

    function entryList(node) {
        return (node.entries || []).map((e) => `<button type="button" class="db-cost-row db-cost-entry${openKey === e.key ? ' is-open' : ''}" data-entry="${esc(e.key)}" aria-expanded="${openKey === e.key}">
            <span class="db-cost-date">${esc(e.date.slice(0, 5))}<small>${esc(e.date.slice(6))}</small></span>
            <span class="db-cost-row-main"><span class="db-cost-row-name">${esc(e.title)}${e.confidence < 70 ? '<span class="db-cost-flag is-warn">Revisar</span>' : ''}</span>
                <span class="db-muted">${esc([e.plot, e.supplier || e.origin, e.document].filter(Boolean).join(' · '))}</span></span>
            <span class="db-cost-row-val"><strong>${brl2(e.amount)}</strong></span></button>`).join('');
    }

    function subgroupOptions(current) {
        return data.groups.map((g) => `<optgroup label="${esc(g.name)}">${data.subgroups.filter((s) => s.group === g.code)
            .map((s) => `<option value="${s.code}"${s.code === current ? ' selected' : ''}>${esc(s.name)}</option>`).join('')}</optgroup>`).join('');
    }

    function detail(node, chain) {
        const e = (node.entries || []).find((x) => x.key === openKey);
        if (!e) return '';
        const conf = e.confidence >= 90 ? 'is-high' : e.confidence >= 70 ? 'is-mid' : 'is-low';
        const rows = [['Setor', e.plot], ['Fornecedor', e.supplier], ['Documento', e.document], ['Pagamento', e.payment], ['Origem', e.origin], ['Categoria lançada', e.category]]
            .filter(([, v]) => v).map(([k, v]) => `<dt>${k}</dt><dd>${esc(v)}</dd>`).join('');
        const actions = data.can_edit ? (editing
            ? `<form class="db-cost-edit" data-c-form><label class="sr-only" for="db-c-sub">Nova categoria</label><select id="db-c-sub" name="subgroup" class="sf-input">${subgroupOptions(e.sub)}</select>
                <button type="submit" class="sf-btn sf-btn-primary db-btn-sm">Salvar</button><button type="button" class="sf-btn sf-btn-secondary db-btn-sm" data-c-cancel>Cancelar</button></form>`
            : `<div class="db-cost-actions">${e.source === 'user' ? '' : '<button type="button" class="sf-btn sf-btn-primary db-btn-sm" data-c-confirm>Confirmar categoria</button>'}
                <button type="button" class="sf-btn sf-btn-secondary db-btn-sm" data-c-change>Mudar categoria</button>
                <a class="sf-btn sf-btn-secondary db-btn-sm" href="${esc(e.link)}">Abrir lançamento ↗</a></div>`)
            : `<div class="db-cost-actions"><a class="sf-btn sf-btn-secondary db-btn-sm" href="${esc(e.link)}">Abrir lançamento ↗</a></div>`;
        return `<div class="db-cost-detail">
            <div class="db-cost-detail-head"><div><span class="db-muted">Lançamento · ${esc(e.date)}</span><h3>${esc(e.title)}</h3></div><strong>${brl2(e.amount)}</strong></div>
            <dl class="db-cost-dl">${rows}</dl>
            <div class="db-cost-why ${conf}"><div><span>✦ Por que está nesta categoria</span><span>${e.confidence}% de certeza</span></div><p>${esc(e.why)}</p>
                ${toast ? `<p class="db-cost-ok">✓ ${esc(toast)}</p>` : ''}</div>
            ${actions}</div>`;
    }

    function side(node, chain) {
        const color = colorOf(chain);
        const m = node.monthly || [];
        const max = Math.max(1, ...m.map((x) => x.v));
        const bars = m.map((x, i) => `<span title="${esc(x.m)}/${String(x.y).slice(2)}: ${esc(brl(x.v))}"><span style="height:${x.v ? Math.max(4, x.v / max * 100) : 0}%;background:${color};opacity:${i === m.length - 1 ? 1 : 0.6}"></span></span>`).join('');
        const labels = m.map((x) => `<span>${esc(x.m)}</span>`).join('');
        const ins = (node.insights || []).map(([k, t]) => `<li><span class="db-cost-ins is-${k}" aria-hidden="true">${k === 'up' ? '↑' : k === 'down' ? '↓' : '✦'}</span><span>${esc(t)}</span></li>`).join('');
        return `<div class="db-cost-card"><div class="db-cost-card-head"><strong>Ao longo da safra</strong><span class="db-muted">${esc(data.period)}</span></div>
                <div class="db-cost-months" style="--n:${m.length}">${bars}</div><div class="db-cost-months-x" style="--n:${m.length}">${labels}</div></div>
            ${ins ? `<div class="db-cost-card"><strong>O que o sistema percebeu</strong><ul class="db-cost-insights">${ins}</ul></div>` : ''}`;
    }

    function render() {
        backBtn.hidden = true;
        crumbsEl.innerHTML = '';
        if (!data || data.status !== 'ok') {
            titleEl.textContent = 'Custo da safra';
            const msg = !data || data.status === 'error' ? 'Não foi possível carregar os custos agora. Tente de novo em instantes.'
                : data.status === 'no_season' ? 'Selecione uma fazenda e uma safra no topo da página.'
                : data.status === 'no_plots' ? 'Vincule setores à safra para calcular o custo.'
                : 'Ainda não há custos lançados nos setores desta safra.';
            body.innerHTML = `<p class="db-muted db-cost-msg">${msg}</p>`;
            return;
        }
        const chain = findNode();
        const node = chain[chain.length - 1];
        const isLeaf = !!node.entries;
        if (!isLeaf) openKey = null;
        titleEl.textContent = node.name;
        crumbsEl.innerHTML = chain.length === 1 ? '' : chain.map((n, i) => `${i ? '<span aria-hidden="true">›</span>' : ''}<button type="button" data-crumb="${i}"${i === chain.length - 1 ? ' aria-current="true"' : ''}>${esc(n.name)}</button>`).join('');
        backBtn.hidden = chain.length === 1;
        const listTitle = isLeaf ? `Lançamentos de ${node.name}` : chain.length === 1 ? 'Para onde vai o dinheiro' : `Dentro de ${node.name.toLowerCase()}`;
        const hint = isLeaf ? 'Toque num lançamento para ver os detalhes.' : 'Toque numa linha para se aprofundar.';
        body.innerHTML = `${stats(node, chain)}
            <div class="db-cost-grid">
                <div class="db-cost-main"><div class="db-cost-list-head"><h3>${esc(listTitle)}</h3><span class="db-muted">${hint}</span></div>
                    ${isLeaf ? entryList(node) : childList(node, chain)}</div>
                <div class="db-cost-side">${openKey ? detail(node, chain) : side(node, chain)}</div>
            </div>
            <p class="db-cost-foot"><span>As categorias são atribuídas pelo sistema a partir da categoria lançada, da descrição, do fornecedor, do catálogo de insumos e das suas correções.</span><a href="/gestao-financeira/contas">Ver todos os lançamentos →</a></p>`;
    }

    async function classify(sub, entry) {
        const fd = new FormData();
        fd.append('csrf_token', csrf); fd.append('entry_key', entry.key); fd.append('subgroup', sub); fd.append('learn_key', entry.item || '');
        try {
            const res = await fetch('/api/dashboard/custos/classificar', { method: 'POST', body: fd, credentials: 'same-origin' });
            if (!res.ok) throw new Error();
        } catch (e) { toast = ''; alert('Não foi possível salvar a categoria. Tente de novo.'); return; }
        editing = false;
        const moved = sub !== entry.sub;
        toast = moved ? 'Categoria alterada. Lançamentos parecidos também serão classificados assim.' : 'Confirmado. O sistema vai usar isso para classificar lançamentos parecidos.';
        await load(true);
        if (moved) {
            // Acompanha o lancamento ate o novo lugar.
            const walk = (n, trail) => {
                if (n.entries && n.entries.some((x) => x.key === entry.key)) return trail;
                for (const c of n.children || []) { const r = walk(c, [...trail, c.path]); if (r) return r; }
                return null;
            };
            path = walk(data.tree, []) || [];
            openKey = entry.key;
            render();
        }
    }

    body.addEventListener('click', (ev) => {
        const go = ev.target.closest('[data-go]');
        if (go) { path = [...path, go.dataset.go]; openKey = null; editing = false; toast = ''; render(); body.scrollTop = 0; return; }
        const ent = ev.target.closest('[data-entry]');
        if (ent) { openKey = openKey === ent.dataset.entry ? null : ent.dataset.entry; editing = false; toast = ''; render(); return; }
        const node = findNode().slice(-1)[0];
        const entry = (node.entries || []).find((x) => x.key === openKey);
        if (!entry) return;
        if (ev.target.closest('[data-c-confirm]')) classify(entry.sub, entry);
        else if (ev.target.closest('[data-c-change]')) { editing = true; toast = ''; render(); }
        else if (ev.target.closest('[data-c-cancel]')) { editing = false; render(); }
    });
    body.addEventListener('submit', (ev) => {
        const form = ev.target.closest('[data-c-form]');
        if (!form) return;
        ev.preventDefault();
        const node = findNode().slice(-1)[0];
        const entry = (node.entries || []).find((x) => x.key === openKey);
        if (entry) classify(form.subgroup.value, entry);
    });
    crumbsEl.addEventListener('click', (ev) => {
        const b = ev.target.closest('[data-crumb]');
        if (!b) return;
        path = path.slice(0, Number(b.dataset.crumb)); openKey = null; editing = false; toast = ''; render();
    });
    backBtn.addEventListener('click', () => {
        if (openKey) openKey = null; else path = path.slice(0, -1);
        editing = false; toast = ''; render();
    });
    document.querySelectorAll('[data-db-cost-open]').forEach((b) => b.addEventListener('click', () => {
        path = []; openKey = null; editing = false; toast = '';
        modal.showModal();
        if (data && data.status === 'ok') render(); else load();
    }));
    modal.querySelectorAll('[data-db-cost-close]').forEach((b) => b.addEventListener('click', () => modal.close()));
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.close(); });
})();
