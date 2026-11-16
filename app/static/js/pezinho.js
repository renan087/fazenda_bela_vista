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

    // Envio: ele pensa enquanto a página do Assistente carrega a resposta.
    const form = $('[data-pzw-form]');
    if (form) {
        const input = $('[data-pzw-input]');
        const grow = () => { input.style.height = ''; input.style.height = Math.min(input.scrollHeight, 120) + 'px'; };
        input.addEventListener('input', grow);
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); if (input.value.trim()) form.requestSubmit(); }
        });
        form.addEventListener('submit', (e) => {
            if (!input.value.trim()) { e.preventDefault(); return; }
            set('pensando');
            if (head) head.src = `${base}cabeca-pensando.svg`;
            $('[data-pzw-send]').disabled = true;
            input.readOnly = true;
        });
    }
    root.querySelectorAll('.pzw-sug, .pzw-pending').forEach((a) => a.addEventListener('click', () => set('pensando')));

    // Outras telas podem mudar a pose: window.Pezinho.set('comemorando', 3000)
    window.Pezinho = {
        set: (state, ms) => set(state, ms),
        say: (html, state) => { if (state) set(state, 4000); showBubble(html); },
        badge: (on) => { if (dot) dot.hidden = !on; },
    };
})();
