(() => {
    // Pezinho: mascote da IA no canto da tela.
    const root = document.querySelector('[data-pzw]');
    if (!root) return;
    const $ = (sel) => root.querySelector(sel);
    const store = {
        get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
        set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* sem armazenamento */ } },
        sget(k) { try { return sessionStorage.getItem(k); } catch (e) { return null; } },
        sset(k, v) { try { sessionStorage.setItem(k, v); } catch (e) { /* sem armazenamento */ } },
    };
    const today = new Date().toISOString().slice(0, 10);
    if (store.get('sf-pz-hidden') === today) { root.remove(); return; }
    root.hidden = false;

    const base = root.dataset.imgBase;
    const img = $('[data-pzw-img]');
    const fab = $('[data-pzw-fab]');
    const panel = $('[data-pzw-panel]');
    const scrim = $('[data-pzw-scrim]');
    const bubble = $('[data-pzw-bubble]');
    const head = $('[data-pzw-head]');
    const dot = $('[data-pzw-dot]');
    const available = root.dataset.available === 'true';
    const pending = root.dataset.pending;
    const name = root.dataset.name;
    const STATES = ['acenando', 'pronto', 'falando', 'pensando', 'alerta', 'comemorando', 'dormindo', 'zangado'];
    let current = '';
    let timer = null;

    const set = (state, ms, next) => {
        if (!STATES.includes(state)) return;
        clearTimeout(timer);
        if (state !== current) { img.src = `${base}${state}.svg`; current = state; root.dataset.state = state; }
        if (ms) timer = setTimeout(() => set(next || (available ? 'pronto' : 'dormindo')), ms);
    };
    // Pré-carrega as outras poses quando o navegador estiver livre.
    const preload = () => STATES.forEach((s) => { const i = new Image(); i.src = `${base}${s}.svg`; });
    (window.requestIdleCallback || ((fn) => setTimeout(fn, 1500)))(preload);

    // Balão de aviso: uma vez por dia, ou quando surge um rascunho novo.
    const showBubble = (text) => {
        $('[data-pzw-bubble-text]').innerHTML = text;
        bubble.hidden = false;
    };
    const hideBubble = () => { bubble.hidden = true; };
    const hello = new Date().getHours() < 12 ? 'Bom dia' : new Date().getHours() < 18 ? 'Boa tarde' : 'Boa noite';
    const who = name ? `${hello}, ${name.replace(/[<>&"]/g, '')}!` : `${hello}!`;

    if (!available) {
        set('dormindo');
    } else if (pending && store.sget('sf-pz-knocked') !== pending) {
        store.sset('sf-pz-knocked', pending);
        set('alerta', 6000);
        showBubble(`<b>${who}</b> Tem um lançamento esperando você confirmar.`);
    } else if (store.get('sf-pz-greeted') !== today) {
        store.set('sf-pz-greeted', today);
        set('acenando', 4500);
        showBubble(`<b>${who}</b> Precisa lançar alguma coisa? É só me chamar.`);
        setTimeout(hideBubble, 12000);
    } else {
        set('pronto');
    }
    bubble.addEventListener('click', (e) => { if (!e.target.closest('[data-pzw-dismiss]')) open(true); });
    $('[data-pzw-dismiss]').addEventListener('click', (e) => { e.stopPropagation(); hideBubble(); });

    // Painel
    const isOpen = () => !panel.hidden;
    const open = (on) => {
        panel.hidden = !on;
        scrim.hidden = !on;
        root.classList.toggle('is-open', on);
        fab.setAttribute('aria-expanded', on ? 'true' : 'false');
        if (on) {
            hideBubble();
            if (available) set('falando', 2600);
            const input = $('[data-pzw-input]');
            if (input && window.matchMedia('(min-width: 640px)').matches) input.focus({ preventScroll: true });
        } else if (available && current !== 'pensando') {
            set('pronto');
        }
    };
    fab.addEventListener('click', () => open(!isOpen()));
    $('[data-pzw-close]').addEventListener('click', () => { open(false); fab.focus(); });
    scrim.addEventListener('click', () => open(false));
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && isOpen()) { open(false); fab.focus(); } });
    document.addEventListener('click', (e) => { if (isOpen() && !root.contains(e.target)) open(false); });

    $('[data-pzw-hide]').addEventListener('click', () => { store.set('sf-pz-hidden', today); root.remove(); });

    // A conversa usa o mesmo motor e os mesmos rascunhos da página do assistente.
    const form = $('[data-pzw-form]');
    if (form) {
        const input = $('[data-pzw-input]');
        const body = $('[data-pzw-body]');
        const welcome = $('[data-pzw-welcome]');
        const messages = $('[data-pzw-messages]');
        const full = $('[data-pzw-full]');
        const send = $('[data-pzw-send]');
        const csrf = form.querySelector('[name="csrf_token"]').value;
        const key = `sf-pz-thread:${root.dataset.scope}`;
        let threadId = store.get(key) || '';
        let busy = false;
        let restored = false;
        const make = (tag, css, text) => {
            const element = document.createElement(tag);
            if (css) element.className = css;
            if (text != null) element.textContent = text;
            return element;
        };
        const scroll = () => { body.scrollTop = body.scrollHeight; };
        const setBusy = (on) => {
            busy = on; send.disabled = on; input.readOnly = on;
            if (on) { set('pensando'); if (head) head.src = `${base}cabeca-pensando.svg`; }
            else { set(available ? 'pronto' : 'dormindo'); if (head) head.src = `${base}cabeca-pronto.svg`; }
        };
        const showError = (text) => { messages.append(make('p', 'pzw-error', text)); scroll(); };
        const request = async (url, options) => {
            const response = await fetch(url, { credentials: 'same-origin', ...options });
            let data;
            try { data = await response.json(); } catch (e) { throw new Error('Não consegui concluir agora. Tente novamente.'); }
            if (!response.ok) {
                const error = new Error(typeof data.detail === 'string' ? data.detail : 'Não consegui concluir agora. Tente novamente.');
                error.status = response.status;
                throw error;
            }
            return data;
        };
        const render = (data) => {
            threadId = data.thread_id; store.set(key, threadId);
            full.href = `/mobile/assistente?c=${encodeURIComponent(threadId)}`;
            full.textContent = 'Abrir conversa completa';
            welcome.hidden = true; messages.replaceChildren();
            for (const entry of data.messages || []) {
                const row = make('div', `pzw-message ${entry.role === 'user' ? 'is-user' : 'is-assistant'}`);
                const text = make('p', 'pzw-message-text', entry.content || '');
                row.append(text);
                if (entry.attachment_id && entry.attachment_name) {
                    const link = make('a', 'pzw-file', `Abrir ${entry.attachment_name}`);
                    link.href = `/mobile/assistente/arquivo/${encodeURIComponent(entry.attachment_id)}`;
                    link.target = '_blank'; link.rel = 'noopener noreferrer'; row.append(link);
                }
                if (entry.audio_id) {
                    const audio = document.createElement('audio'); audio.controls = true; audio.preload = 'none';
                    audio.src = `/mobile/assistente/audio/${encodeURIComponent(entry.audio_id)}`; row.append(audio);
                }
                const d = entry.draft;
                if (d) {
                    const card = make('div', 'pzw-draft');
                    card.append(make('strong', '', d.kind === 'compra' ? 'Compra para conferir' : 'Consumo para conferir'));
                    const details = [d.item, d.is_new ? `Novo cadastro · ${d.category || ''}` : '',
                        d.packs_label || (d.quantity ? `${d.quantity} ${d.unit || ''}` : ''),
                        d.packs_label && d.quantity ? `Total: ${d.quantity} ${d.unit || ''}` : '',
                        d.total, d.unit_price ? `${d.unit_price} por embalagem` : '',
                        d.plot && d.kind !== 'compra' ? d.plot : '', d.date_label, d.payment, d.finance,
                        d.supplier ? `Fornecedor: ${d.supplier}` : '', d.lot,
                        d.minimum ? `Avisar estoque baixo: ${d.minimum}` : '', d.notes,
                        d.balance != null ? `Estoque: ${d.balance} → ${d.after} ${d.unit || ''}` : ''].filter(Boolean);
                    details.forEach((value) => card.append(make('div', 'pzw-draft-line', value)));
                    if (d.state === 'pending') {
                        card.append(make('p', 'pzw-draft-hint', 'Nada será registrado sem sua confirmação.'));
                        const actions = make('div', 'pzw-actions');
                        for (const [action, label] of [['confirmar', 'Confirmar'], ['ajustar', 'Ajustar'], ['descartar', 'Descartar']]) {
                            const button = make('button', action === 'confirmar' ? 'is-primary' : '', label);
                            button.type = 'button'; button.dataset.action = action; button.dataset.draftId = d.id; actions.append(button);
                        }
                        card.append(actions);
                    } else card.append(make('p', 'pzw-draft-hint', ({confirmed: 'Registrado', discarded: 'Descartado', replaced: 'Substituído', expired: 'Expirado'})[d.state] || 'Indisponível'));
                    row.append(card);
                }
                messages.append(row);
            }
            scroll();
        };
        const loadThread = async (id) => {
            if (!id || busy) return;
            setBusy(true);
            try { render(await request(`/mobile/assistente/pezinho?thread_id=${encodeURIComponent(id)}`)); }
            catch (error) {
                if (error.status === 404) { store.set(key, ''); threadId = ''; }
                else restored = false;
                showError(error.message);
            }
            finally { setBusy(false); }
        };
        fab.addEventListener('click', () => {
            if (isOpen() && !restored && threadId) { restored = true; loadThread(threadId); }
        });
        bubble.addEventListener('click', (event) => {
            if (!event.target.closest('[data-pzw-dismiss]') && isOpen() && !restored && threadId) {
                restored = true; loadThread(threadId);
            }
        });
        const pendingLink = $('[data-pzw-pending-thread]');
        if (pendingLink) pendingLink.addEventListener('click', (event) => {
            event.preventDefault(); restored = true; loadThread(pendingLink.dataset.pzwPendingThread);
        });
        root.querySelectorAll('[data-pzw-suggestion]').forEach((button) => button.addEventListener('click', () => {
            if (busy) return;
            input.value = button.dataset.pzwSuggestion; form.requestSubmit();
        }));
        messages.addEventListener('click', async (event) => {
            const button = event.target.closest('[data-action]');
            if (!button || busy) return;
            if (button.dataset.action === 'ajustar') { input.value = 'Quero ajustar: '; input.focus(); grow(); return; }
            if (button.dataset.action === 'confirmar' && !window.confirm('Confirmar este lançamento?')) return;
            const data = new FormData(); data.append('csrf_token', csrf); data.append('thread_id', threadId);
            setBusy(true);
            try { render(await request(`/mobile/assistente/pezinho/${encodeURIComponent(button.dataset.draftId)}/${button.dataset.action}`, {method: 'POST', body: data})); }
            catch (error) { showError(error.message); }
            finally { setBusy(false); }
        });
        const grow = () => { input.style.height = ''; input.style.height = Math.min(input.scrollHeight, 120) + 'px'; };
        input.addEventListener('input', grow);
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); if (input.value.trim()) form.requestSubmit(); }
        });
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            const text = input.value.trim();
            if (!text || busy) return;
            const data = new FormData(); data.append('csrf_token', csrf); data.append('message', text);
            if (threadId) data.append('thread_id', threadId);
            welcome.hidden = true; messages.append(make('p', 'pzw-message is-user', text));
            const thinking = make('p', 'pzw-thinking', 'Pezinho está pensando...'); messages.append(thinking); scroll();
            input.value = ''; grow(); setBusy(true);
            try { render(await request('/mobile/assistente/pezinho', {method: 'POST', body: data})); restored = true; }
            catch (error) { thinking.remove(); input.value = text; grow(); showError(error.message); }
            finally { setBusy(false); }
        });
    }

    // Outras telas podem mudar a pose: window.Pezinho.set('comemorando', 3000)
    window.Pezinho = {
        set: (state, ms) => set(state, ms),
        say: (html, state) => { if (state) set(state, 4000); showBubble(html); },
        badge: (on) => { if (dot) dot.hidden = !on; },
    };
})();
