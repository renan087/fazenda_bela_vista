/* Primeiros passos: tour guiado pelo Pezinho nas telas reais (canvas "Onboarding SiSFarm").
 * Regra de ouro: o Pezinho só aponta e explica. Quem clica, digita e salva é sempre o usuário.
 * O progresso vem do banco (fazenda, safra, setor); o navegador guarda só conveniências da tela. */
(() => {
    const dataEl = document.getElementById('sf-tour-data');
    if (!dataEl) return;
    let D;
    try { D = JSON.parse(dataEl.textContent); } catch (e) { return; }
    if (!D || !D.state) return;

    const IMG = window.__SF_TOUR_IMG || {};
    const reduce = window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;
    const path = location.pathname.replace(/\/(index\.html)?$/, '') || '/';
    const $ = (s, r = document) => r.querySelector(s);
    const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
    const esc = (t) => String(t ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const visible = (el) => !!el && typeof el.getClientRects === 'function' && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden';
    const firstVisible = (sels, root = document) => { for (const s of [].concat(sels)) { const el = $$(s, root).find(visible); if (el) return el; } return null; };
    const parseBr = (v) => parseFloat(String(v || '').replace(/\./g, '').replace(',', '.')) || 0;
    const mem = (local) => { try { return local ? window.localStorage : window.sessionStorage; } catch (e) { return null; } };
    const get = (k, local) => { try { return mem(local)?.getItem(k) ?? null; } catch (e) { return null; } };
    const set = (k, v, local) => { try { if (v === null) mem(local)?.removeItem(k); else mem(local)?.setItem(k, v); } catch (e) { /* sem armazenamento: segue sem lembrar */ } };

    const has = { farm: !!D.farm, season: !!D.season, plot: !!D.plot };
    const sec = !has.farm ? 'farm' : !has.season ? 'season' : (!has.plot && !D.plot_skipped) ? 'plot' : 'done';
    const doneCount = [has.farm, has.season, has.plot].filter(Boolean).length;
    const farmName = D.farm ? D.farm.name : 'a fazenda';
    const seasonName = D.season ? D.season.name : 'a safra';
    const CHECK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg>';
    const CONTINUE = '/primeiros-passos/continuar';

    const post = async (action) => {
        const body = new FormData();
        body.append('csrf_token', D.csrf);
        body.append('action', action);
        try {
            const r = await fetch('/primeiros-passos/tour', { method: 'POST', body, headers: { 'x-sf-fetch': '1' }, credentials: 'same-origin' });
            return r.ok ? await r.json() : null;
        } catch (e) { return null; }
    };

    /* ---------- Camada visual ---------- */
    const layer = document.createElement('div');
    layer.className = 'obt';
    layer.innerHTML = '<div class="obt-hole" hidden></div>'
        + '<div class="obt-coach" hidden role="status" aria-live="polite"><span class="obt-arrow" aria-hidden="true"><svg viewBox="0 0 30 30" fill="none" stroke="currentColor" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 28V5"/><path d="M6 13l9-9 9 9"/></svg></span>'
        + '<div class="obt-card"><span class="obt-face"><img src="' + esc(IMG.head) + '" alt=""></span><div class="obt-ct"><span class="obt-lbl"></span><b class="obt-tt"></b><span class="obt-sb"></span><div class="obt-acts"></div></div></div></div>'
        + '<div class="obt-dock" role="region" aria-label="Progresso dos primeiros passos" hidden></div>'
        + '<button type="button" class="obt-chip" hidden></button>'
        + '<div class="obt-modals"></div>';
    const hole = $('.obt-hole', layer), coach = $('.obt-coach', layer), dock = $('.obt-dock', layer), chip = $('.obt-chip', layer), modals = $('.obt-modals', layer);
    document.body.appendChild(layer);
    document.body.classList.add('obt-on');

    /* Um <dialog> aberto fica na camada do topo: o tour precisa morar dentro dele para aparecer por cima. */
    const host = () => {
        let dlg = null;
        try { dlg = $$('dialog[open]').find((d) => d.matches(':modal')) || null; } catch (e) { dlg = null; }
        const parent = dlg || document.body;
        if (layer.parentNode !== parent) parent.appendChild(layer);
    };

    /* ---------- Estado de tela (só conveniências) ---------- */
    let state = D.state;                 // active | paused
    let focusKey = null, fv = '', fvFor = null;
    let carDismissed = false, dirty = false, asking = false, modalOpen = false;
    const pageKey = 'sf-ob-ok:' + path;
    const confirmed = new Set((() => { try { return JSON.parse(get(pageKey) || '[]'); } catch (e) { return []; } })());
    const confirm = (k) => { if (!confirmed.has(k)) { confirmed.add(k); set(pageKey, JSON.stringify([...confirmed])); } };

    const reviewKey = get('sf-ob-review');
    const backUrl = get('sf-ob-back');
    const reviewing = (() => {
        if (reviewKey === 'farm' && D.farm && path.startsWith('/fazendas/' + D.farm.id)) return 'farm';
        if (reviewKey === 'season' && D.season && path.startsWith('/safras/' + D.season.id + '/editar')) return 'season';
        if (reviewKey) { set('sf-ob-review', null); set('sf-ob-back', null); }
        return null;
    })();

    /* ---------- Campos que o Pezinho acompanha ---------- */
    const field = (key, el, opts) => el ? Object.assign({
        key, el,
        valid: () => !!String(el.value || '').trim(),
        text: () => String(el.value || '').trim(),
        match(t) { return this.wrap.contains(t); },
    }, opts, { wrap: (opts.wrap && el.closest(opts.wrap)) || el }) : null;
    const optionText = (sel) => (sel && sel.selectedIndex >= 0 && sel.value ? sel.options[sel.selectedIndex].text : '');

    const fieldsNow = () => {
        const list = [];
        const add = (f) => { if (f && visible(f.el)) list.push(f); };
        if (path === '/fazendas/nova') {
            const step1 = $('[data-fz-wstep="1"]');
            if (visible(step1)) {
                add(field('name', $('#w-nome'), { wrap: '.fz-field', label: 'o nome da fazenda', valid: () => $('#w-nome').value.trim().length >= 2,
                    title: 'Como vocês chamam a fazenda?', sub: 'Use o nome que a equipe já conhece. Tem o CAR? Use a busca acima e economize tempo.' }));
                add(field('uf', $('#w-uf'), { wrap: '.fz-field', label: 'o estado', text: () => optionText($('#w-uf')),
                    title: 'Em qual estado ela fica?', sub: 'Escolha na lista.', impact: () => 'Se trocar, o município precisa ser escolhido de novo.' }));
                add(field('city', $('#w-mun'), { wrap: '.fz-field', label: 'o município', valid: () => !!$('#w-mun').value.trim() && !visible($('[data-fz-city-list]')),
                    title: 'Agora, o município', sub: 'Comece a digitar e escolha na lista. Ela mostra só as cidades do estado escolhido.' }));
                add(field('area', $('#w-area'), { wrap: '.fz-field', label: 'a área total', valid: () => parseBr($('#w-area').value) > 0, text: () => ($('#w-area').value || '').trim() + ' ha',
                    title: 'Qual a área total, em hectares?', sub: 'Digite só os números: 12050 vira 120,50.' }));
            }
        } else if (path === '/safras/nova' || (reviewing === 'season')) {
            const p0 = $('[data-szw-panel="0"]');
            if (visible(p0)) {
                const periods = $('[data-szw-periods]');
                add(field('speriod', periods, { label: 'o período', valid: () => true, needConfirm: !reviewing,
                    text: () => (($('[data-szw-periods] [aria-checked="true"]') || {}).textContent || '').replace(/\s+/g, ' ').trim(),
                    title: 'Confira o período', sub: 'No café, a safra vai de julho a junho. Se a sua for diferente, escolha outra opção.', act: 'Está certo',
                    impact: () => (D.plot ? 'O setor ' + D.plot.name + ' continua nesta safra.' : '') }));
                add(field('sname', $('[data-szw-name]'), { wrap: '.szw-field', label: 'o nome da safra', needConfirm: !reviewing,
                    title: 'Já sugerimos o nome da safra', sub: 'Pode trocar se vocês usam outro nome.', act: 'Está certo' }));
            }
        } else if (path === '/setores') {
            const p0 = $('#plots-launch-modal[open] [data-stw-panel="0"]');
            if (visible(p0)) {
                add(field('pname', $('[data-stw-name]', p0), { wrap: '.szw-field', label: 'o nome do setor',
                    title: 'Como vocês chamam este setor?', sub: 'Ex.: Talhão da Represa, Gleba Sul.' }));
                const area = $('[data-stw-area]', p0);
                add(field('parea', area, { wrap: '.szw-field', label: 'a área do setor', valid: () => parseBr(area.value) > 0, text: () => (area.value || '').trim() + ' ha',
                    title: 'Qual a área do setor?', sub: 'Em hectares.' + (D.farm ? ' Cabe até ' + D.farm.area + ' ha em ' + farmName + '.' : ''),
                    impact: () => (D.farm ? 'Cabe até ' + D.farm.area + ' ha na fazenda.' : '') }));
            }
        }
        return list;
    };

    /* Revisão de cadastro já salvo (ficha da fazenda): qualquer campo com rótulo vira um campo acompanhado. */
    const genericField = (t) => {
        if (reviewing !== 'farm' || !t || !t.matches || !t.matches('input:not([type=hidden]):not([type=checkbox]):not([type=radio]):not([type=file]), select, textarea') || layer.contains(t)) return null;
        const lab = (t.id && $('label[for="' + CSS.escape(t.id) + '"]')) || t.closest('label');
        const label = lab ? lab.textContent.replace(/\*/g, '').replace(/\s+/g, ' ').trim().toLowerCase() : '';
        if (!label) return null;
        const impact = () => {
            if (t.name === 'total_area' && D.plots_area) return 'Os setores já somam ' + D.plots_area + ' ha: a área da fazenda não pode ficar menor que isso.';
            if (t.name === 'location_uf') return 'Se trocar, o município precisa ser escolhido de novo.';
            if (t.name === 'name') return 'O nome novo aparece em todo o sistema.';
            return '';
        };
        return field('g:' + (t.name || t.id), t, { wrap: '.fz-field', label, text: () => (t.tagName === 'SELECT' ? optionText(t) : String(t.value || '').trim()), impact });
    };
    let genericNow = null;
    const allFields = () => (genericNow ? fieldsNow().concat([genericNow]) : fieldsNow());

    document.addEventListener('focusin', (e) => {
        if (state !== 'active' || layer.contains(e.target)) return;
        if (path === '/fazendas/nova' && e.target.id === 'w-car') { focusKey = 'car'; schedule(); return; }
        let f = fieldsNow().find((x) => x.match(e.target));
        if (!f) { const g = genericField(e.target); if (g) { genericNow = g; f = g; } }
        if (!f) return;
        if (focusKey !== f.key) {
            focusKey = f.key;
            if (fvFor !== f.key) { fv = f.valid() ? f.text() : ''; fvFor = f.key; }
        }
        schedule();
    });
    document.addEventListener('focusout', (e) => {
        if (state !== 'active' || layer.contains(e.target)) return;
        setTimeout(() => {
            if (focusKey === 'car') { if (!($('.fz-carbox') || document.body).contains(document.activeElement)) focusKey = null; schedule(); return; }
            const f = allFields().find((x) => x.match(e.target));
            if (!f || f.match(document.activeElement)) return;
            if (f.valid() && f.key !== 'speriod') confirm(f.key);
            if (focusKey === f.key) focusKey = null;
            schedule();
        }, 0);
    });
    document.addEventListener('click', (e) => {
        if (state !== 'active') return;
        if (e.target.closest && e.target.closest('[data-szw-periods] [role="radio"], [data-szw-periods] button')) { confirm('speriod'); schedule(); }
        setTimeout(schedule, 60);
    }, true);
    document.addEventListener('change', (e) => {
        const f = fieldsNow().find((x) => x.match(e.target));
        if (f && e.target.tagName === 'SELECT' && f.valid()) confirm(f.key);
        if (reviewing && layer.contains(e.target) === false && e.target.closest('form')) dirty = true;
        schedule();
    });
    document.addEventListener('input', (e) => { if (reviewing && !layer.contains(e.target) && e.target.closest('form')) dirty = true; });
    document.addEventListener('submit', () => { dirty = false; });

    /* ---------- O que o Pezinho mostra agora ---------- */
    const skipPlot = { label: 'Pular esta etapa', kind: 'skip', fn: async () => { await post('skip_plot'); location.reload(); } };

    const reviewCopy = (f, label) => {
        const now = f.text();
        const was = (now === fv ? 'Hoje: ' : 'Antes: ') + fv + '.';
        const impact = f.impact ? f.impact() : '';
        return { key: 'rv:' + f.key + ':' + (now === fv), el: f.wrap, label, title: 'Alterando ' + f.label, sub: was + (impact ? ' ' + impact : ''), light: !impact, warn: !!impact };
    };

    const fieldPlan = (fields, label, afterAll, extra = []) => {
        const cur = fields.find((f) => f.key === focusKey);
        if (cur) {
            if (fvFor === cur.key && fv && cur.valid()) return reviewCopy(cur, label);
            return { key: 'f:' + cur.key, el: cur.wrap, label, title: cur.title, sub: cur.sub, acts: (cur.act ? [{ label: cur.act, fn: () => { if (cur.valid()) { confirm(cur.key); focusKey = null; document.activeElement?.blur?.(); schedule(); } } }] : []).concat(extra) };
        }
        for (const f of fields) {
            const done = f.valid() && (confirmed.has(f.key) || (!f.needConfirm && reviewing));
            if (!done) return { key: 'f:' + f.key, el: f.wrap, label, title: f.title, sub: f.sub, acts: (f.act ? [{ label: f.act, fn: () => { if (f.valid()) { confirm(f.key); schedule(); } } }] : []).concat(extra) };
        }
        return afterAll();
    };

    const plan = () => {
        if (state !== 'active' || modalOpen) return null;
        const fields = fieldsNow();
        if (genericNow && !visible(genericNow.el)) genericNow = null;

        if (reviewing === 'farm') {
            if (genericNow && focusKey === genericNow.key) {
                if (fvFor === genericNow.key && fv) return reviewCopy(genericNow, 'Fazenda · Revisão');
                return { key: 'g:' + genericNow.key, el: genericNow.wrap, label: 'Fazenda · Revisão', title: 'Preencha ' + genericNow.label, sub: 'Depois é só salvar.', light: true };
            }
            return { key: 'rv-farm', free: true, label: 'Fazenda · Revisão', title: 'A fazenda já está salva. O que quer mudar?', sub: 'Abra o que quiser alterar na ficha e salve. Depois, clique em Voltar para onde parei.' };
        }

        if (path === '/fazendas' && sec === 'farm') {
            const cta = firstVisible(['.fz-empty a[href="/fazendas/nova"]', 'a[href="/fazendas/nova"]']);
            if (cta) return { key: 'fcta', el: cta, label: 'Fazenda', title: 'Vamos começar pela fazenda', sub: 'Clique em Cadastrar primeira fazenda.' };
        }

        if (path === '/fazendas/nova' && sec === 'farm') {
            const step = ($$('[data-fz-wstep]').find((s) => !s.hidden) || {}).dataset?.fzWstep || '1';
            const next = firstVisible('[data-fz-w-next]');
            if (step === '1') {
                const carOk = visible($('[data-fz-w-car-ok]'));
                if (carOk) ['name', 'uf', 'city', 'area'].forEach(confirm);
                if (focusKey === 'car' && !carOk && !carDismissed) {
                    return { key: 'car', el: $('.fz-carbox'), label: 'Fazenda · CAR', title: 'Cole aqui o número do CAR',
                        sub: 'Ele está no recibo de inscrição do CAR. Depois clique em Buscar dados: preenchemos nome, município, área e perímetro para você.',
                        acts: [{ label: 'Não tenho agora', fn: () => { carDismissed = true; focusKey = null; document.activeElement?.blur?.(); schedule(); } }] };
                }
                return fieldPlan(fields, 'Fazenda · Identificação', () => ({ key: 'fnext1', el: next, label: 'Fazenda · Identificação', title: 'Tudo certo por aqui', sub: 'Clique em Continuar para marcar o perímetro.' }));
            }
            if (step === '2') {
                const L = 'Fazenda · Perímetro';
                const chosen = $('[data-fz-popt].on, [data-fz-popt][aria-pressed="true"]');
                const opt = chosen ? chosen.dataset.fzPopt : '';
                if (!opt) return { key: 'popts', el: $('.fz-opts'), label: L, title: 'Como quer marcar o contorno?', sub: 'Desenhar leva um minuto. Se preferir, deixe para depois: dá para fazer na ficha.' };
                if (opt === 'draw' && !confirmed.has('pmap')) {
                    return { key: 'pmap', el: firstVisible('[data-fz-pview="draw"] .fz-mapwrap') || $('.fz-opts'), label: L, title: 'Marque os cantos da fazenda no mapa',
                        sub: 'Clique em Iniciar desenho e toque em cada canto. Se preferir, escolha Fazer depois.', acts: [{ label: 'Já desenhei', fn: () => { confirm('pmap'); schedule(); } }] };
                }
                if (opt === 'file' && !visible($('[data-fz-w-filemap]'))) {
                    return { key: 'pfile', el: $('[data-fz-w-drop]'), label: L, title: 'Escolha o arquivo do perímetro', sub: 'Serve o arquivo do CAR (.kml) ou um .geojson.' };
                }
                return { key: 'fnext2', el: next, label: L, title: opt === 'later' ? 'Combinado, fica para depois' : 'Contorno pronto', sub: 'Clique em Continuar para conferir os dados.' };
            }
            return { key: 'fsave', el: firstVisible('[data-fz-w-submit]'), label: 'Fazenda · Conferir', title: 'Confira e salve', sub: 'Nada foi gravado ainda. A fazenda só é criada quando você clicar em Salvar fazenda.' };
        }

        const seasonWizard = (path === '/safras/nova' && sec === 'season') || reviewing === 'season';
        if (seasonWizard && $('[data-season-wizard]')) {
            const panel = ($$('[data-szw-panel]').find((p) => !p.hidden) || {}).dataset?.szwPanel || '0';
            const next = firstVisible(['[data-szw-next]', '[data-szw-sheet-next]']);
            const L = reviewing ? 'Safra · Revisão' : 'Safra';
            if (panel === '0') {
                return fieldPlan(fields, L + (reviewing ? '' : ' · Período'), () => (reviewing
                    ? { key: 'snext-rv', el: next, label: L, title: 'A safra já está aberta. O que quer mudar?', sub: 'Clique no campo que quer alterar. Se estiver tudo certo, clique em Continuar.' }
                    : { key: 'snext0', el: next, label: L + ' · Período', title: 'Pronto, clique em Continuar', sub: 'Período e nome conferidos.' }));
            }
            if (panel === '1') return { key: 'snext1', el: next, label: L + ' · Setores', title: D.plot ? 'Confira os setores' : 'Os setores vêm logo depois', sub: D.plot ? 'Marque os setores que entram nesta safra e clique em Continuar.' : 'Ainda não há setores. Os que você criar no próximo passo já entram nesta safra. Clique em Continuar.' };
            if (panel === '2') return { key: 'snext2', el: next, label: L + ' · Metas', title: 'Metas podem esperar', sub: 'Produção e orçamento são opcionais. Clique em Continuar; dá para preencher depois.' };
            if (panel === '3') return { key: 'snext3', el: next, label: L + ' · Integrações', title: 'Isto também pode esperar', sub: 'Clique em Continuar para conferir.' };
            const submit = firstVisible(['[data-szw-submit]', '[data-szw-sheet-next]']);
            return reviewing
                ? { key: 'ssave-rv', el: submit, label: L, title: 'Salve as alterações', sub: 'Nenhuma safra nova é criada: só os dados de ' + seasonName + ' mudam.' }
                : { key: 'screate', el: submit, label: 'Safra · Conferir', title: 'Confira e crie a safra', sub: 'Ela fica ligada a ' + farmName + '. Deixe marcada a opção de safra ativa.' };
        }

        if (path === '/setores' && sec === 'plot') {
            const dlg = $('#plots-launch-modal');
            if (!dlg || !dlg.open) {
                const cta = firstVisible('[data-plots-launch-open]');
                if (cta) return { key: 'pcta', el: cta, label: 'Setor', title: 'Agora, os setores', sub: 'Setor é cada talhão da fazenda. Cadastre um agora; os outros você faz depois.', acts: [skipPlot] };
            } else {
                const panel = ($$('[data-stw-panel]', dlg).find((p) => !p.hidden) || {}).dataset?.stwPanel || '0';
                const next = firstVisible('[data-stw-next]', dlg);
                const L = 'Setor';
                if (panel === '0') return fieldPlan(fields, L + ' · Nome e área', () => ({ key: 'pnext0', el: next, label: L + ' · Nome e área', title: 'Tudo certo, clique em Continuar', sub: 'Agora vamos ver onde ele fica.', acts: [skipPlot] }), [skipPlot]);
                if (panel === '1') return { key: 'pnext1', el: next, label: L + ' · Onde fica', title: 'Quer desenhar o setor?', sub: 'Se quiser, marque o contorno no mapa. Pode deixar para depois e clicar em Continuar.', acts: [skipPlot] };
                if (panel === '2') return { key: 'pnext2', el: next, label: L + ' · Plantio', title: 'O que está plantado?', sub: 'Escolha a variedade e o mês do plantio. Se não souber agora, marque Deixar para depois e clique em Continuar.', acts: [skipPlot] };
                if (panel === '3') return { key: 'pnext3', el: next, label: L + ' · Irrigação e solo', title: 'Irrigação e solo podem esperar', sub: 'Marque Deixar para depois se quiser e clique em Continuar.', acts: [skipPlot] };
                return { key: 'pcreate', el: firstVisible('[data-stw-submit]', dlg), label: L + ' · Conferir', title: 'Confira e crie o setor', sub: 'Ele já entra na ' + seasonName + '. Deixe marcada a opção Incluir na safra.', acts: [skipPlot] };
            }
        }

        if (sec === 'done') return null;
        const nextTxt = { farm: ['Próximo: cadastrar a fazenda', 'Ir para Fazendas'], season: ['Próximo: abrir a safra', 'Abrir a safra'], plot: ['Próximo: cadastrar um setor', 'Ir para Setores'] }[sec];
        return { key: 'free-' + sec, free: true, label: 'Primeiros passos · ' + doneCount + ' de 3', title: nextTxt[0], sub: 'Quando quiser, clique no botão para seguir. O que já foi feito está salvo.', acts: [{ label: nextTxt[1], href: CONTINUE }].concat(sec === 'plot' ? [skipPlot] : []) };
    };

    /* ---------- Desenho ---------- */
    let current = null, lastSig = '', scrolledKey = '';
    const renderCoach = (p) => {
        const sig = p ? JSON.stringify([p.key, p.title, p.sub, p.label, (p.acts || []).map((a) => a.label)]) : '';
        if (sig === lastSig) return;
        lastSig = sig;
        if (!p) { coach.hidden = true; return; }
        $('.obt-lbl', coach).textContent = p.warn ? 'Atenção' : p.label;
        $('.obt-tt', coach).textContent = p.title;
        $('.obt-sb', coach).textContent = p.sub || '';
        const acts = $('.obt-acts', coach);
        acts.innerHTML = '';
        (p.acts || []).forEach((a) => {
            const b = document.createElement(a.href ? 'a' : 'button');
            if (a.href) b.href = a.href; else b.type = 'button';
            b.className = a.kind === 'skip' ? 'obt-skip' : 'obt-act';
            b.textContent = a.label;
            if (a.fn) b.addEventListener('click', (ev) => { ev.preventDefault(); a.fn(); });
            acts.appendChild(b);
        });
        coach.classList.toggle('is-light', !!p.light);
        coach.classList.toggle('is-warn', !!p.warn);
        const card = $('.obt-card', coach);
        card.style.animation = 'none'; void card.offsetWidth; card.style.animation = '';
        coach.hidden = false;
    };

    const position = () => {
        const p = current;
        const dockTop = visible(dock) ? dock.getBoundingClientRect().top : innerHeight;
        const floor = Math.min(innerHeight, dockTop) - 10;
        if (!p) { hole.hidden = true; return; }
        const target = p.free ? null : p.el;
        if (target && visible(target)) {
            const r = target.getBoundingClientRect();
            if (scrolledKey !== p.key && (r.top < 70 || r.bottom > floor - 40)) {
                scrolledKey = p.key;
                target.scrollIntoView({ block: 'center', behavior: reduce ? 'auto' : 'smooth' });
            } else if (scrolledKey !== p.key) scrolledKey = p.key;
            const pad = 6;
            Object.assign(hole.style, { top: (r.top - pad) + 'px', left: (r.left - pad) + 'px', width: (r.width + pad * 2) + 'px', height: (r.height + pad * 2) + 'px' });
            hole.hidden = false;
            const cw = coach.offsetWidth || 320, ch = coach.offsetHeight || 140, gap = 22;
            const below = r.bottom + pad + gap + ch <= floor || r.top - pad - gap - ch < 70;
            const top = below ? r.bottom + pad + gap + 12 : r.top - pad - gap - ch - 12;
            const left = Math.max(12, Math.min(r.left, innerWidth - cw - 12));
            coach.style.top = Math.max(8, Math.min(top, innerHeight - ch - 8)) + 'px';
            coach.style.left = left + 'px';
            const ax = Math.max(12, Math.min(r.left + Math.min(r.width / 2, 44) - left - 15, cw - 42));
            $('.obt-arrow', coach).style.left = ax + 'px';
            coach.classList.toggle('is-below', below);
            coach.classList.toggle('is-above', !below);
            coach.classList.remove('is-free');
        } else {
            hole.hidden = true;
            const cw = coach.offsetWidth || 320, ch = coach.offsetHeight || 140;
            coach.style.top = Math.max(8, floor - ch - 6) + 'px';
            coach.style.left = Math.max(12, (innerWidth - cw) / 2) + 'px';
            coach.classList.remove('is-below', 'is-above');
            coach.classList.add('is-free');
        }
    };

    const segsHtml = () => [['farm', 'Fazenda'], ['season', 'Safra'], ['plot', 'Setor']].map(([k, label], i) => {
        const done = has[k], skip = k === 'plot' && D.plot_skipped && !done, cur = (sec === k && !done) || reviewing === k;
        const cls = 'obt-seg' + (done ? ' done' : skip ? ' skip' : '') + (cur ? ' cur' : '');
        const mark = done ? '✓' : String(i + 1);
        const aria = label + (done ? ': concluído' : skip ? ': deixado para depois' : cur ? ': em andamento' : ': a fazer');
        const href = !reviewing && done && k === 'farm' ? '/fazendas/' + D.farm.id + '/editar' : !reviewing && done && k === 'season' ? '/safras/' + D.season.id + '/editar' : '';
        return href
            ? '<a class="' + cls + '" href="' + href + '" data-obt-review="' + k + '" aria-label="' + esc(aria) + '. Revisar"><i>' + mark + '</i><span class="obt-sl">' + label + '</span></a>'
            : '<span class="' + cls + '" aria-label="' + esc(aria) + '"><i>' + mark + '</i><span class="obt-sl">' + label + '</span></span>';
    }).join('');

    const renderDock = () => {
        if (state !== 'active' || modalOpen) { dock.hidden = true; return; }
        const face = '<span class="obt-dface" aria-hidden="true"><img src="' + esc(IMG.head) + '" alt=""></span>';
        const title = '<div class="obt-dt">Primeiros passos<span>' + doneCount + ' de 3 concluídos</span></div>';
        let right;
        if (reviewing && asking) right = '<span class="obt-dask">Descartar as alterações?</span><button type="button" class="obt-dbtn" data-obt="discard">Descartar</button><button type="button" class="obt-dbtn pri" data-obt="keep">Continuar editando</button>';
        else if (reviewing) right = '<button type="button" class="obt-dbtn pri" data-obt="back">Voltar para onde parei</button>';
        else right = '<button type="button" class="obt-dbtn" data-obt="pause">Pausar</button>';
        dock.classList.toggle('is-ask', !!asking);
        dock.innerHTML = face + title + '<div class="obt-segs">' + segsHtml() + '</div>' + right;
        dock.hidden = false;
    };

    const renderChip = () => {
        chip.hidden = !(state === 'paused' && !modalOpen);
        chip.innerHTML = '<span class="obt-dface" aria-hidden="true"><img src="' + esc(IMG.head) + '" alt=""></span><span>Continuar primeiros passos <small>· ' + doneCount + ' de 3</small></span>';
    };

    dock.addEventListener('click', async (e) => {
        const seg = e.target.closest('[data-obt-review]');
        if (seg) { set('sf-ob-review', seg.dataset.obtReview); set('sf-ob-back', location.pathname + location.search); return; }
        const b = e.target.closest('[data-obt]');
        if (!b) return;
        const what = b.dataset.obt;
        if (what === 'pause') { state = 'paused'; await post('pause'); refresh(); }
        if (what === 'back') { if (dirty) { asking = true; renderDock(); position(); } else goBack(); }
        if (what === 'discard') goBack();
        if (what === 'keep') { asking = false; renderDock(); }
    });
    const goBack = () => { const url = backUrl || CONTINUE; set('sf-ob-review', null); set('sf-ob-back', null); location.href = url; };
    coach.addEventListener('mousedown', (e) => { if (e.target.closest('button, a')) e.preventDefault(); });
    chip.addEventListener('click', () => openResume());

    /* ---------- Janelas: comemoração, retomar, conclusão ---------- */
    const confetti = () => {
        if (reduce) return '';
        const cols = ['#446A36', '#9BCB6B', '#D18A00', '#1F5FA8', '#E0312F', '#294127'];
        let h = '';
        for (let i = 0; i < 18; i++) h += '<span class="obt-conf" aria-hidden="true" style="left:' + (4 + (i * 53) % 92) + '%;background:' + cols[i % 6] + ';animation-delay:' + ((i % 6) * 0.12).toFixed(2) + 's"></span>';
        return h;
    };
    const openModal = (html, cls) => {
        modalOpen = true;
        modals.innerHTML = '<div class="obt-ov"><div class="obt-modal ' + (cls || '') + '" role="dialog" aria-modal="true" aria-labelledby="obt-mt">' + html + '</div></div>';
        refresh();
        const first = $('.obt-mbtn.pri', modals);
        if (first) first.focus();
    };
    const closeModal = () => { modalOpen = false; modals.innerHTML = ''; refresh(); };
    modals.addEventListener('keydown', (e) => { if (e.key === 'Escape' && $('[data-obt-m="close"]', modals)) closeModal(); });

    const bars = (n) => [0, 1, 2].map((i) => '<span class="' + (i < n - 1 ? 'done' : i === n - 1 ? 'now' : '') + '"></span>').join('');
    const celebrate = (kind) => {
        const farmStep = kind === 'farm';
        openModal(confetti()
            + '<img src="' + esc(IMG.cheer) + '" alt="Pezinho comemorando">'
            + '<span class="obt-kick" style="display:block;margin-top:6px">' + (farmStep ? '1 de 3 concluído' : '2 de 3 concluídos') + '</span>'
            + '<h2 id="obt-mt">' + (farmStep ? 'Fazenda cadastrada!' : 'Safra aberta!') + '</h2>'
            + '<p>' + (farmStep ? esc(farmName) + ' já está no SiSFarm. Agora vamos abrir a safra que você vai acompanhar.' : esc(seasonName) + ' está pronta em ' + esc(farmName) + '. Falta só o primeiro setor, e ele pode ficar para depois.') + '</p>'
            + '<div class="obt-bars" aria-hidden="true">' + bars(farmStep ? 1 : 2) + '</div>'
            + '<div class="obt-mrow"><a class="obt-mbtn pri" href="' + CONTINUE + '">' + (farmStep ? 'Abrir a safra' : 'Cadastrar o setor') + '</a>'
            + '<button type="button" class="obt-mbtn ghost" data-obt-m="pause">Pausar aqui</button>'
            + '<button type="button" class="obt-mbtn ghost" data-obt-m="close">Ficar nesta tela</button></div>');
    };
    const openResume = () => {
        const where = { farm: 'o cadastro da fazenda', season: 'a abertura da safra', plot: 'o cadastro do setor', done: 'o fim do tutorial' }[sec];
        openModal('<div style="display:flex;align-items:center;gap:12px"><span class="obt-dface" style="width:52px;height:52px" aria-hidden="true"><img src="' + esc(IMG.head) + '" alt="" style="width:46px;height:46px"></span>'
            + '<div><span class="obt-kick">Primeiros passos · ' + doneCount + ' de 3</span><h2 id="obt-mt" style="margin:2px 0 0">Vamos continuar?</h2></div></div>'
            + '<p style="margin-top:14px">Você parou em <b style="color:#16201A">' + where + '</b>. O que já foi cadastrado está salvo.</p>'
            + '<div class="obt-bar"><i style="width:' + Math.round(doneCount / 3 * 100) + '%"></i></div>'
            + '<div class="obt-mrow"><button type="button" class="obt-mbtn pri" data-obt-m="resume">Continuar de onde parei</button><button type="button" class="obt-mbtn ghost" data-obt-m="end">Encerrar tutorial</button><button type="button" class="obt-mbtn ghost" data-obt-m="close">Agora não</button></div>'
            + '<p style="margin:14px 0 0;font-size:12.5px">Se encerrar, os passos que faltam continuam no painel, em Primeiros passos.</p>', 'is-left');
    };
    const conclusion = () => {
        const rec = (title, sub) => '<div class="obt-rec"><span class="obt-rnum">' + CHECK + '</span><div><b>' + esc(title) + '</b><span>' + esc(sub) + '</span></div></div>';
        const plotRec = D.plot ? rec(D.plot.name, D.plot.area + ' ha · ' + seasonName)
            : '<div class="obt-rec"><span class="obt-rnum skip">3</span><div><b>Setores</b><span>Deixados para depois</span></div><button type="button" class="obt-mbtn" data-obt-m="unskip">Cadastrar agora</button></div>';
        openModal(confetti()
            + '<img src="' + esc(IMG.cheer) + '" alt="Pezinho comemorando">'
            + '<span class="obt-kick" style="display:block;margin-top:6px">Primeiros passos concluídos</span>'
            + '<h2 id="obt-mt">Tudo pronto' + (D.first_name ? ', ' + esc(D.first_name) : '') + '!</h2>'
            + '<p>A base de ' + esc(farmName) + ' está no SiSFarm. Daqui em diante é usar no dia a dia.</p>'
            + '<div class="obt-recs">' + rec(farmName, (D.farm.location ? D.farm.location + ' · ' : '') + D.farm.area + ' ha') + rec(seasonName, 'Safra ativa') + plotRec + '</div>'
            + '<div class="obt-next"><a class="obt-nx" href="/fertilizacao/agendamentos"><b>Agendar uma adubação</b><span>Programe as aplicações</span></a><a class="obt-nx" href="/pluviometria"><b>Registrar a chuva</b><span>O pluviômetro de hoje</span></a><a class="obt-nx" href="/usuarios"><b>Convidar a equipe</b><span>Gerente, agrônomo, operadores</span></a></div>'
            + '<div class="obt-mrow"><button type="button" class="obt-mbtn pri" data-obt-m="finish">Ir para o painel</button></div>');
        $$('.obt-nx', modals).forEach((a) => a.addEventListener('click', () => post('finish')));
    };
    modals.addEventListener('click', async (e) => {
        const b = e.target.closest('[data-obt-m]');
        if (!b) return;
        const what = b.dataset.obtM;
        if (what === 'close') closeModal();
        if (what === 'pause') { state = 'paused'; closeModal(); await post('pause'); }
        if (what === 'resume') { b.disabled = true; await post('resume'); location.href = get('sf-ob-last', true) || CONTINUE; }
        if (what === 'end') { await post('end'); state = 'ended'; closeModal(); teardown(); }
        if (what === 'finish') { b.disabled = true; await post('finish'); location.href = '/dashboard'; }
        if (what === 'unskip') { b.disabled = true; await post('unskip_plot'); location.href = '/setores'; }
    });

    const teardown = () => { layer.remove(); document.body.classList.remove('obt-on'); clearInterval(timer); };

    /* ---------- Ciclo ---------- */
    let raf = 0;
    const update = () => {
        raf = 0;
        if (state === 'ended') return;
        host();
        current = plan();
        renderCoach(current);
        position();
    };
    const schedule = () => { if (!raf) raf = requestAnimationFrame(update); };
    const refresh = () => { renderDock(); renderChip(); lastSig = '__'; schedule(); };
    addEventListener('scroll', () => position(), { passive: true, capture: true });
    addEventListener('resize', schedule);
    const timer = setInterval(update, 400);

    // Onde parou: só telas que fazem parte do tour (para "Continuar de onde parei").
    if (['/fazendas', '/fazendas/nova', '/safras/nova', '/setores'].includes(path) && !reviewing) set('sf-ob-last', path, true);

    if (state === 'active') {
        if (sec === 'done') { conclusion(); }
        else if (sec === 'season' && D.farm && !get('sf-ob-cel-f-' + D.farm.id, true) && path !== '/safras/nova') { set('sf-ob-cel-f-' + D.farm.id, '1', true); celebrate('farm'); }
        else if (sec === 'plot' && D.season && !get('sf-ob-cel-s-' + D.season.id, true) && path !== '/setores') { set('sf-ob-cel-s-' + D.season.id, '1', true); celebrate('season'); }
    }
    refresh();
})();
