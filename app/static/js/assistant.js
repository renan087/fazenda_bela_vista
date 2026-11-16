(() => {
    const shell = document.querySelector('[data-as-shell]');
    if (!shell) return;
    const $ = (sel, root = shell) => root.querySelector(sel);
    const $$ = (sel, root = shell) => Array.from(root.querySelectorAll(sel));
    const store = { get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
                    set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* sem armazenamento */ } } };

    // Altura: a conversa ocupa a tela, com o campo de mensagem sempre visível.
    const fit = () => {
        const top = shell.getBoundingClientRect().top + window.scrollY;
        const bottom = window.innerWidth < 640 ? 0 : 24;
        shell.style.setProperty('--as-h', Math.max(448, window.innerHeight - top - bottom) + 'px');
    };
    fit();
    window.addEventListener('resize', fit);

    const scroller = $('[data-as-scroll]');
    const hasThread = !shell.querySelector('.as-welcome');
    const toBottom = () => { if (scroller && hasThread) scroller.scrollTop = scroller.scrollHeight; };
    toBottom();

    // Lista de conversas em gaveta (tablet e celular)
    const scrim = $('[data-as-scrim]');
    const openThreads = (open) => {
        shell.classList.toggle('is-threads-open', open);
        if (scrim) scrim.hidden = !open;
        if (open) { const s = $('[data-as-search]'); if (s) s.focus({ preventScroll: true }); }
    };
    $$('[data-as-threads-open]').forEach((b) => b.addEventListener('click', () => openThreads(true)));
    $$('[data-as-threads-close]').forEach((b) => b.addEventListener('click', () => openThreads(false)));
    if (scrim) scrim.addEventListener('click', () => openThreads(false));
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') openThreads(false); });

    // Busca de conversas
    const search = $('[data-as-search]');
    if (search) search.addEventListener('input', () => {
        const q = search.value.trim().toLowerCase();
        let any = false;
        $$('[data-as-grp]').forEach((grp) => {
            let shown = 0;
            for (let el = grp.nextElementSibling; el && el.hasAttribute('data-as-cv'); el = el.nextElementSibling) {
                const hit = !q || el.textContent.toLowerCase().includes(q);
                el.hidden = !hit;
                if (hit) shown += 1;
            }
            grp.hidden = shown === 0;
            any = any || shown > 0;
        });
        const none = $('[data-as-noresult]');
        if (none) none.hidden = any || !q;
    });

    // Painel de contexto (desktop largo)
    const ctxBtn = $('[data-as-ctx-toggle]');
    const applyCtx = (hidden) => {
        shell.classList.toggle('is-ctx-hidden', hidden);
        if (ctxBtn) ctxBtn.setAttribute('aria-pressed', hidden ? 'false' : 'true');
    };
    applyCtx(store.get('sf-assistant-ctx') === 'hidden');
    if (ctxBtn) ctxBtn.addEventListener('click', () => {
        const hidden = !shell.classList.contains('is-ctx-hidden');
        applyCtx(hidden);
        store.set('sf-assistant-ctx', hidden ? 'hidden' : 'shown');
    });

    // Confirmações e botões ocupados
    $$('form[data-as-confirm]').forEach((f) => f.addEventListener('submit', (e) => {
        if (!window.confirm(f.dataset.asConfirm)) e.preventDefault();
    }));
    $$('form[data-as-busy]').forEach((f) => f.addEventListener('submit', () => {
        const b = f.querySelector('button');
        if (b) { b.disabled = true; b.lastChild.textContent = f.dataset.asBusy; }
    }));

    const form = $('[data-as-form]');
    const box = form ? $('[data-as-input]', form) : null;
    const grow = () => {
        if (!box) return;
        box.style.height = '';
        if (box.value) box.style.height = Math.min(box.scrollHeight, 160) + 'px';
        box.style.overflowY = box.scrollHeight > 160 ? 'auto' : 'hidden';
    };
    window.addEventListener('load', () => { fit(); grow(); toBottom(); });

    // Sugestões e atalhos preenchem o campo (o usuário completa e envia)
    $$('[data-as-fill]').forEach((b) => b.addEventListener('click', () => {
        if (!box) return;
        box.value = b.dataset.asFill;
        if (b.dataset.asFillHint) box.placeholder = b.dataset.asFillHint;
        grow();
        box.focus();
        box.setSelectionRange(box.value.length, box.value.length);
    }));

    // Copiar e avaliar respostas
    const csrf = form ? form.querySelector('[name=csrf_token]').value : '';
    const threadId = form ? form.querySelector('[name=thread_id]').value : '';
    $$('[data-as-rate]').forEach((bar) => {
        const text = bar.parentElement.querySelector('[data-as-text]');
        const copy = $('[data-as-copy]', bar);
        if (copy && text) copy.addEventListener('click', async () => {
            try { await navigator.clipboard.writeText(text.textContent.trim()); copy.title = 'Copiado'; } catch (e) { /* sem permissão */ }
        });
        $$('[data-as-fb]', bar).forEach((btn) => btn.addEventListener('click', () => {
            const on = btn.getAttribute('aria-pressed') !== 'true';
            $$('[data-as-fb]', bar).forEach((o) => o.setAttribute('aria-pressed', 'false'));
            btn.setAttribute('aria-pressed', on ? 'true' : 'false');
            if (!threadId) return;
            const body = new URLSearchParams({ csrf_token: csrf, index: bar.dataset.asRate, value: on ? btn.dataset.asFb : '' });
            fetch(`/mobile/assistente/conversa/${threadId}/avaliar`, { method: 'POST', body, credentials: 'same-origin' }).catch(() => {});
        }));
    });

    if (!form || !box) return;
    if (window.matchMedia('(max-width: 639px)').matches) box.placeholder = 'Escreva uma mensagem…';
    grow();
    box.addEventListener('input', grow);
    if (window.matchMedia('(min-width: 640px)').matches) box.focus({ preventScroll: true });
    box.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); form.requestSubmit(); }
    });

    // Foto
    const photo = $('[data-as-photo]', form);
    const chip = $('[data-as-attach]', form);
    const showPhoto = () => {
        const file = photo.files && photo.files[0];
        chip.hidden = !file;
        $('[data-as-attach-name]', chip).textContent = file ? file.name : '';
        if (file && file.size > 5 * 1024 * 1024) { window.alert('O arquivo precisa ter até 5 MB.'); photo.value = ''; chip.hidden = true; }
    };
    if (photo && chip) {
        photo.addEventListener('change', showPhoto);
        $('[data-as-attach-clear]', chip).addEventListener('click', () => { photo.value = ''; showPhoto(); });
    }

    // Gravação local; a transcrição acontece após o envio.
    const mic = $('[data-as-mic]', form);
    let recorder = null, sendWhenStopped = false, recordedUrl = null;
    if (window.MediaRecorder && navigator.mediaDevices && navigator.mediaDevices.getUserMedia && mic) {
        mic.hidden = false;
        let meterTimer = null, audioContext = null;
        const recording = $('[data-as-recording]', form);
        const wave = $('[data-as-wave]', form);
        $('[data-as-stop]', form).addEventListener('click', () => { if (recorder) recorder.stop(); });
        mic.addEventListener('click', async () => {
            if (recorder) { recorder.stop(); return; }
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                $('[data-as-audio-id]', form).value = '';
                const chunks = [];
                recorder = new MediaRecorder(stream);
                recorder.ondataavailable = (event) => { if (event.data.size) chunks.push(event.data); };
                recorder.onstop = () => {
                    const status = $('[data-as-audio-status]', form);
                    clearInterval(meterTimer); recording.hidden = true;
                    if (audioContext) { audioContext.close(); audioContext = null; }
                    const type = (recorder.mimeType || 'audio/webm').split(';')[0];
                    const blob = new Blob(chunks, { type });
                    stream.getTracks().forEach((track) => track.stop());
                    recorder = null;
                    mic.setAttribute('aria-pressed', 'false');
                    try {
                        if (!blob.size || blob.size > 5 * 1024 * 1024) throw new Error('O áudio precisa ter até 5 MB.');
                        const transfer = new DataTransfer();
                        transfer.items.add(new File([blob], 'voz', { type }));
                        $('[data-as-voice-input]', form).files = transfer.files;
                        if (recordedUrl) URL.revokeObjectURL(recordedUrl);
                        recordedUrl = URL.createObjectURL(blob);
                        const preview = $('[data-as-audio-preview]', form);
                        preview.src = recordedUrl; preview.hidden = false;
                        status.textContent = 'Áudio pronto para enviar.';
                        if (sendWhenStopped) { sendWhenStopped = false; form.requestSubmit(); }
                    } catch (error) { sendWhenStopped = false; window.alert(error.message || 'Não foi possível preparar o áudio.'); }
                };
                recorder.start();
                recording.hidden = false;
                wave.replaceChildren(...Array.from({length: 24}, () => document.createElement('i')));
                const started = Date.now();
                let analyser = null;
                try {
                    const AC = window.AudioContext || window.webkitAudioContext;
                    if (AC) { audioContext = new AC(); analyser = audioContext.createAnalyser(); analyser.fftSize = 256;
                        audioContext.createMediaStreamSource(stream).connect(analyser); }
                } catch (error) { /* O cronômetro continua disponível sem medidor. */ }
                meterTimer = setInterval(() => {
                    const elapsed = Math.floor((Date.now() - started) / 1000);
                    $('[data-as-duration]', form).textContent = `${Math.floor(elapsed / 60)}:${String(elapsed % 60).padStart(2, '0')}`;
                    if (analyser) {
                        const samples = new Uint8Array(analyser.frequencyBinCount); analyser.getByteFrequencyData(samples);
                        wave.querySelectorAll('i').forEach((bar, index) => { bar.style.height = `${3 + samples[index * 2] / 12}px`; });
                    }
                    if (elapsed >= 180 && recorder) recorder.stop();
                }, 120);
                mic.setAttribute('aria-pressed', 'true');
                mic.title = 'Parar gravação';
                $('[data-as-audio-status]', form).textContent = 'Gravando. Toque na seta para enviar.';
            } catch (error) { window.alert('Não foi possível acessar o microfone.'); }
        });
    }

    // Frases do dia a dia do cafeicultor, alternadas enquanto a resposta nao chega
    const THINKING = [
        'Esperando o vento baixar...', 'Adubando o raciocínio...', 'Lubrificando as engrenagens...',
        'Eita, que o café vai dar bom esse ano...', 'Conferindo o pluviômetro...', 'Passando um cafezinho rapidinho...',
        'Contando as sacas no terreiro...', 'Esperando a florada abrir...', 'Rodando o café no terreiro...',
        'Calibrando o pulverizador...', 'Olhando se a broca deu as caras...', 'Subindo o morro do talhão 3...',
        'Desatolando o trator...', 'Medindo a umidade do grão...', 'Chamando a turma da colheita...',
        'Abanando a peneira...', 'Fazendo a conta da saca na ponta do lápis...', 'Afiando a derriçadeira...',
        'Esperando a chuva do caju...', 'Torcendo pra geada passar longe...', 'Tirando o barro da botina...',
        'Consultando o compadre da fazenda vizinha...', 'Escolhendo o grão cereja...', 'Abastecendo a caminhonete...',
        'Conferindo a cotação na cooperativa...', 'Espantando a seriema do terreiro...', 'Ligando o secador...',
        'Contando os pés de café, um por um...', 'Arruando debaixo da saia do café...', 'Pegando sinal no alto do morro...',
    ];
    const startThinking = (label) => {
        const pool = THINKING.slice().sort(() => Math.random() - 0.5);
        let index = 0;
        const swap = () => {
            label.classList.add('is-swapping');
            setTimeout(() => {
                label.textContent = pool[index++ % pool.length];
                label.classList.remove('is-swapping');
            }, 220);
        };
        setTimeout(() => { swap(); setInterval(swap, 2200); }, 1400);
    };

    $$('[data-as-voice]').forEach((voice) => {
        const audio = voice.querySelector('audio');
        const play = $('[data-voice-play]', voice);
        const wave = $('[data-voice-wave]', voice);
        const time = $('[data-voice-time]', voice);
        const transcript = $('[data-voice-text]', voice);
        const toggle = $('[data-voice-transcript]', voice);
        wave.replaceChildren(...Array.from({length: 36}, (_, i) => {
            const bar = document.createElement('i');
            bar.style.height = `${5 + ((i * 17 + i * i * 3) % 22)}px`;
            return bar;
        }));
        play.addEventListener('click', () => { if (audio.paused) audio.play().catch(() => window.alert('Áudio indisponível.')); else audio.pause(); });
        audio.addEventListener('play', () => { play.textContent = '❚❚'; play.setAttribute('aria-label', 'Pausar áudio'); });
        audio.addEventListener('pause', () => { play.textContent = '▶'; play.setAttribute('aria-label', 'Reproduzir áudio'); });
        const update = () => {
            const seconds = Math.floor(audio.currentTime || 0);
            time.textContent = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
            wave.querySelectorAll('i').forEach((bar, i) => bar.classList.toggle('played', audio.duration > 0 && i / 36 < audio.currentTime / audio.duration));
        };
        audio.addEventListener('timeupdate', update);
        wave.addEventListener('click', (event) => { if (audio.duration) audio.currentTime = audio.duration * Math.max(0, Math.min(1, (event.clientX - wave.getBoundingClientRect().left) / wave.clientWidth)); });
        toggle.addEventListener('click', () => { transcript.hidden = !transcript.hidden; toggle.textContent = transcript.hidden ? 'Ver transcrição' : 'Ocultar transcrição'; toggle.setAttribute('aria-expanded', String(!transcript.hidden)); });
    });

    // Envio: mostra a mensagem e o "Pensando…" enquanto o servidor responde
    form.addEventListener('submit', (e) => {
        if (recorder) { e.preventDefault(); if (!sendWhenStopped) { sendWhenStopped = true; recorder.stop(); } return; }
        const hasVoice = !!$('[data-as-voice-input]', form).files.length;
        const text = box.value.trim() || (hasVoice ? '[mensagem de áudio]' : '');
        if (!text || form.dataset.sending) { e.preventDefault(); return; }
        form.dataset.sending = '1';
        const chat = $('[data-as-chat]');
        const welcome = $('.as-welcome', chat);
        if (welcome) welcome.remove();
        const mine = document.createElement('div');
        mine.className = 'as-msg-user';
        const bubble = document.createElement('div');
        bubble.className = 'as-ubub';
        if (hasVoice && recordedUrl) {
            const audio = document.createElement('audio'); audio.controls = true; audio.src = recordedUrl;
            bubble.append(audio, document.createTextNode('Enviando áudio...'));
        } else bubble.textContent = text;
        mine.append(bubble);
        const ai = document.createElement('div');
        ai.className = 'as-msg-ai';
        ai.innerHTML = '<span class="as-av as-av-ai"></span><div class="as-msg-body"><div class="as-who">Assistente SiSFarm</div>'
            + '<div class="as-typing" role="status" aria-live="polite"><i></i><i></i><i></i><span data-as-thinking>Pensando...</span></div></div>';
        ai.firstChild.innerHTML = shell.querySelector('.as-av-ai svg') ? shell.querySelector('.as-av-ai svg').outerHTML : '';
        chat.append(mine, ai);
        startThinking(ai.querySelector('[data-as-thinking]'));
        scroller.scrollTop = scroller.scrollHeight;
        $('[data-as-send]', form).disabled = true;
        const submitted = document.createElement('input'); submitted.type = 'hidden'; submitted.name = 'message'; submitted.value = text;
        form.append(submitted); box.removeAttribute('name'); box.value = ''; grow();
        box.readOnly = true;
    });
})();
