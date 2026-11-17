(() => {
    const shell = document.querySelector('[data-as-shell]');
    if (!shell) return;
    const $ = (sel, root = shell) => root.querySelector(sel);
    const $$ = (sel, root = shell) => Array.from(root.querySelectorAll(sel));
    const store = { get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
                    set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* sem armazenamento */ } } };

    const ready = shell.dataset.asReady === 'true';
    document.documentElement.classList.add('as-page-active');
    if (ready) document.documentElement.classList.add('as-page-ready');

    // A viewport visual acompanha o teclado e os deslocamentos do Safari no iOS.
    const fit = () => {
        if (ready && window.matchMedia('(max-width: 639px)').matches) {
            const viewport = window.visualViewport;
            shell.style.setProperty('--as-top', (viewport ? viewport.offsetTop : 0) + 'px');
            shell.style.setProperty('--as-h', (viewport ? viewport.height : window.innerHeight) + 'px');
            return;
        }
        const overflow = document.documentElement.scrollHeight - window.innerHeight;
        shell.style.setProperty('--as-h', Math.max(448, shell.offsetHeight - overflow) + 'px');
    };
    fit();
    window.addEventListener('resize', fit);
    if (window.visualViewport) {
        window.visualViewport.addEventListener('resize', fit);
        window.visualViewport.addEventListener('scroll', fit);
    }

    const scroller = $('[data-as-scroll]');
    const hasThread = !shell.querySelector('.as-welcome');
    const toBottom = () => { if (scroller && hasThread) scroller.scrollTop = scroller.scrollHeight; };
    toBottom();

    // Lista de conversas em gaveta (tablet e celular)
    const scrim = $('[data-as-scrim]');
    const openThreads = (open) => {
        shell.classList.toggle('is-threads-open', open);
        if (scrim) scrim.hidden = !open;
        if (open && window.matchMedia('(min-width: 768px) and (pointer: fine)').matches) {
            const s = $('[data-as-search]');
            if (s) s.focus({ preventScroll: true });
        }
    };
    $$('[data-as-threads-open]').forEach((b) => b.addEventListener('click', () => openThreads(true)));
    $$('[data-as-threads-close]').forEach((b) => b.addEventListener('click', () => openThreads(false)));
    const threadsToggles = $$('[data-as-threads-toggle]');
    const setThreadsCollapsed = (collapsed) => {
        shell.classList.toggle('is-threads-collapsed', collapsed);
        threadsToggles.forEach((b) => b.setAttribute('aria-expanded', String(!collapsed)));
    };
    setThreadsCollapsed(store.get('sisfarm_assistant_threads_collapsed') === '1');
    threadsToggles.forEach((b) => b.addEventListener('click', () => {
        const collapsed = !shell.classList.contains('is-threads-collapsed');
        setThreadsCollapsed(collapsed);
        store.set('sisfarm_assistant_threads_collapsed', collapsed ? '1' : '0');
        const next = collapsed ? $('.as-threads-expand') : $('.as-threads-row [data-as-threads-toggle]');
        if (next) next.focus({ preventScroll: true });
    }));
    if (scrim) scrim.addEventListener('click', () => openThreads(false));
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') openThreads(false); });

    // Busca e filtros de conversas (Todas, Pendentes, WhatsApp)
    const search = $('[data-as-search]');
    let filter = 'todas';
    const applyList = () => {
        const q = search ? search.value.trim().toLowerCase() : '';
        let any = false;
        $$('[data-as-grp]').forEach((grp) => {
            let shown = 0;
            for (let el = grp.nextElementSibling; el && el.hasAttribute('data-as-cv'); el = el.nextElementSibling) {
                const okFilter = filter === 'todas' || (filter === 'pendentes' && el.dataset.pending === 'true') || (filter === 'whatsapp' && el.dataset.channel === 'whatsapp');
                const hit = okFilter && (!q || el.textContent.toLowerCase().includes(q));
                el.hidden = !hit;
                if (hit) shown += 1;
            }
            grp.hidden = shown === 0;
            any = any || shown > 0;
        });
        const none = $('[data-as-noresult]');
        if (none) {
            none.hidden = any || (!q && filter === 'todas');
            none.textContent = filter === 'pendentes' && !q ? 'Nenhum lançamento aguardando você.'
                : filter === 'whatsapp' && !q ? 'Nenhuma conversa pelo WhatsApp ainda.' : 'Nenhuma conversa encontrada.';
        }
    };
    const setFilter = (value) => {
        filter = value;
        $$('[data-as-filter]').forEach((b) => b.setAttribute('aria-pressed', b.dataset.asFilter === value ? 'true' : 'false'));
        applyList();
    };
    if (search) search.addEventListener('input', applyList);
    $$('[data-as-filter]').forEach((b) => b.addEventListener('click', () => setFilter(b.dataset.asFilter)));
    // Cmd+K / Ctrl+K: buscar conversas
    document.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k' && search) {
            e.preventDefault();
            if (window.matchMedia('(max-width: 1023px)').matches) openThreads(true);
            search.focus(); search.select();
        }
    });
    // Sino (celular e tablet): abre a lista já filtrada nas pendências
    $$('[data-as-show-pending]').forEach((b) => b.addEventListener('click', () => { setFilter('pendentes'); openThreads(true); }));

    // Painel de contexto (desktop largo)
    // Abaixo de 1600 px ele começa fechado (a conversa precisa de espaço); a escolha da pessoa vale depois.
    const ctxBtn = $('[data-as-ctx-toggle]');
    const ctxEl = $('[data-as-ctx]');
    const ctxVisible = () => Boolean(ctxEl) && getComputedStyle(ctxEl).display !== 'none';
    const applyCtx = (pref) => {
        shell.classList.toggle('is-ctx-hidden', pref === 'hidden');
        shell.classList.toggle('is-ctx-shown', pref === 'shown');
        if (ctxBtn) ctxBtn.setAttribute('aria-pressed', ctxVisible() ? 'true' : 'false');
    };
    applyCtx(store.get('sf-assistant-ctx'));
    if (ctxBtn) ctxBtn.addEventListener('click', () => {
        const pref = ctxVisible() ? 'hidden' : 'shown';
        applyCtx(pref);
        store.set('sf-assistant-ctx', pref);
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

    const initVoice = (voice) => {
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
        if (toggle && transcript) toggle.addEventListener('click', () => { transcript.hidden = !transcript.hidden; toggle.textContent = transcript.hidden ? 'Ver transcrição' : 'Ocultar transcrição'; toggle.setAttribute('aria-expanded', String(!transcript.hidden)); });
    };
    $$('[data-as-voice]').forEach(initVoice);
    if (!form || !box) return;
    if (window.matchMedia('(max-width: 639px)').matches) box.placeholder = 'Escreva uma mensagem…';
    grow();
    box.addEventListener('input', grow);
    if (window.matchMedia('(min-width: 640px)').matches) box.focus({ preventScroll: true });
    box.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); form.requestSubmit(); }
    });

    // Fotos e PDFs compartilham a validação do seletor e do arrastar e soltar.
    const photo = $('[data-as-photo]', form);
    const chip = $('[data-as-attach]', form);
    const attachmentStatus = $('[data-as-attachment-status]', form);
    const allowedAttachments = new Set(['image/jpeg', 'image/png', 'image/webp', 'application/pdf']);
    const attachmentError = (file) => {
        if (!file || !allowedAttachments.has(file.type)) return 'Selecione uma foto JPG, PNG, WEBP ou um PDF.';
        if (!file.size || file.size > 5 * 1024 * 1024) return 'O arquivo precisa ter conteúdo e no máximo 5 MB.';
        return '';
    };
    const showPhoto = () => {
        const file = photo.files && photo.files[0];
        const error = file ? attachmentError(file) : '';
        if (error) { photo.value = ''; chip.hidden = true; attachmentStatus.textContent = error; return; }
        attachmentStatus.textContent = '';
        chip.hidden = !file;
        $('[data-as-attach-name]', chip).textContent = file ? file.name : '';
    };
    if (photo && chip) {
        photo.addEventListener('change', showPhoto);
        $('[data-as-attach-clear]', chip).addEventListener('click', () => { photo.value = ''; showPhoto(); });
        const dropZone = shell.querySelector('.as-chat');
        let dragDepth = 0;
        const isFileDrag = (event) => Array.from(event.dataTransfer?.types || []).includes('Files');
        dropZone.addEventListener('dragenter', (event) => {
            if (!isFileDrag(event)) return;
            event.preventDefault();
            dragDepth += 1;
            dropZone.classList.add('is-file-dragging');
        });
        dropZone.addEventListener('dragover', (event) => {
            if (!isFileDrag(event)) return;
            event.preventDefault();
            event.dataTransfer.dropEffect = 'copy';
        });
        dropZone.addEventListener('dragleave', (event) => {
            if (!isFileDrag(event)) return;
            dragDepth = Math.max(0, dragDepth - 1);
            if (!dragDepth) dropZone.classList.remove('is-file-dragging');
        });
        dropZone.addEventListener('drop', (event) => {
            if (!isFileDrag(event)) return;
            event.preventDefault();
            dragDepth = 0;
            dropZone.classList.remove('is-file-dragging');
            const files = event.dataTransfer.files;
            if (files.length !== 1) { attachmentStatus.textContent = 'Envie apenas um arquivo por vez.'; return; }
            const error = attachmentError(files[0]);
            if (error) { attachmentStatus.textContent = error; return; }
            try {
                const transfer = new DataTransfer();
                transfer.items.add(files[0]);
                photo.files = transfer.files;
                showPhoto();
                box.focus();
            } catch (error) {
                attachmentStatus.textContent = 'Não foi possível anexar o arquivo. Use o botão de anexo.';
            }
        });
    }

    // Gravação local; a transcrição acontece após o envio.
    const mic = $('[data-as-mic]', form);
    let recorder = null, sendWhenStopped = false, recordedUrl = null;
    if (window.MediaRecorder && navigator.mediaDevices && navigator.mediaDevices.getUserMedia && mic) {
        mic.hidden = false;
        let meterTimer = null, audioContext = null, opening = false;
        const recording = $('[data-as-recording]', form);
        const wave = $('[data-as-wave]', form);
        $('[data-as-stop]', form).addEventListener('click', () => { if (recorder) recorder.stop(); });
        mic.addEventListener('click', async () => {
            if (recorder) { recorder.stop(); return; }
            if (opening) return;
            opening = true;
            const preview = $('[data-as-audio-preview]', form);
            const previewAudio = preview.querySelector('audio');
            previewAudio.pause(); previewAudio.removeAttribute('src'); preview.hidden = true;
            $('[data-voice-time]', preview).textContent = '0:00';
            preview.querySelectorAll('[data-voice-wave] i').forEach((bar) => bar.classList.remove('played'));
            $('[data-as-voice-input]', form).value = '';
            $('[data-as-audio-id]', form).value = '';
            $('[data-as-audio-status]', form).textContent = '';
            if (recordedUrl) { URL.revokeObjectURL(recordedUrl); recordedUrl = null; }
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
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
                        previewAudio.src = recordedUrl; preview.hidden = false;
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
            finally { opening = false; }
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

    initVoice($('[data-as-audio-preview]', form));

    // Envio: mostra a mensagem e o "Pensando…" enquanto o servidor responde
    form.addEventListener('submit', (e) => {
        if (recorder) { e.preventDefault(); if (!sendWhenStopped) { sendWhenStopped = true; recorder.stop(); } return; }
        const hasVoice = !!$('[data-as-voice-input]', form).files.length;
        const hasAttachment = !!(photo && photo.files && photo.files.length);
        const text = box.value.trim() || (hasVoice ? '[mensagem de áudio]' : hasAttachment ? 'Analise o arquivo enviado.' : '');
        if (!text || form.dataset.sending) { e.preventDefault(); return; }
        form.dataset.sending = '1';
        const chat = $('[data-as-chat]');
        const welcome = $('.as-welcome', chat);
        if (welcome) welcome.remove();
        const mine = document.createElement('div');
        mine.className = 'as-msg-user';
        const bubble = document.createElement('div');
        bubble.className = 'as-ubub';
        if (hasAttachment) {
            const pendingFile = document.createElement('span');
            pendingFile.className = 'as-photo-tag';
            pendingFile.textContent = `${photo.files[0].name} · Enviando arquivo...`;
            bubble.append(pendingFile);
            chip.hidden = true;
            attachmentStatus.textContent = '';
        }
        if (hasVoice && recordedUrl) {
            const voice = document.createElement('div');
            voice.className = 'as-voice';
            voice.innerHTML = '<audio preload="metadata"></audio><div class="as-voice-player">'
                + '<button type="button" data-voice-play aria-label="Reproduzir áudio">&#9654;</button>'
                + '<span class="as-voice-wave" data-voice-wave aria-hidden="true"></span><span data-voice-time>0:00</span>'
                + '</div><div class="as-voice-actions"><span>Enviando áudio...</span></div>';
            voice.querySelector('audio').src = recordedUrl;
            bubble.append(voice);
            initVoice(voice);
            const preview = $('[data-as-audio-preview]', form);
            const previewAudio = preview.querySelector('audio');
            previewAudio.pause(); preview.hidden = true; previewAudio.removeAttribute('src');
            $('[data-as-audio-status]', form).textContent = '';
        } else bubble.append(document.createTextNode(text));
        mine.append(bubble);
        const ai = document.createElement('div');
        ai.className = 'as-msg-ai';
        ai.innerHTML = '<span class="as-av as-av-ai"></span><div class="as-msg-body"><div class="as-who">Assistente SiSFarm</div>'
            + '<div class="as-typing" role="status" aria-live="polite"><i></i><i></i><i></i><span data-as-thinking>Pensando...</span></div></div>';
        const avatar = shell.querySelector('.as-av-ai img');
        if (avatar) ai.firstChild.innerHTML = `<img src="${avatar.src.replace(/cabeca-\w+\.svg/, 'cabeca-pensando.svg')}" alt="">`;
        chat.append(mine, ai);
        startThinking(ai.querySelector('[data-as-thinking]'));
        scroller.scrollTop = scroller.scrollHeight;
        $('[data-as-send]', form).disabled = true;
        const submitted = document.createElement('input'); submitted.type = 'hidden'; submitted.name = 'message'; submitted.value = text;
        form.append(submitted); box.removeAttribute('name'); box.value = ''; grow();
        box.readOnly = true;
    });

    // Atalhos que abrem o seletor de arquivo e a câmera
    $$('[data-as-pick-file]').forEach((b) => b.addEventListener('click', () => { if (photo && !photo.disabled) photo.click(); }));
    const camera = $('[data-as-camera]', form);
    if (camera && photo) camera.addEventListener('click', () => {
        const accept = photo.getAttribute('accept');
        photo.setAttribute('accept', 'image/*');
        photo.setAttribute('capture', 'environment');
        photo.click();
        setTimeout(() => { photo.setAttribute('accept', accept); photo.removeAttribute('capture'); }, 800);
    });

    // Cartão de conferência: editar os campos ali mesmo e mandar a correção ao Pezinho
    $$('[data-as-draft]').forEach((card) => {
        const start = $('[data-as-edit-start]', card);
        if (!start) return;
        const view = $('[data-as-draft-view]', card);
        const edit = $('[data-as-draft-edit]', card);
        const inputs = $$('[data-as-edit]', card);
        const toggle = (on) => {
            card.classList.toggle('is-editing', on);
            view.hidden = on;
            edit.hidden = !on;
            inputs.forEach((input) => {
                input.hidden = !on;
                const shown = input.parentElement.querySelector('[data-as-view]');
                if (shown) shown.hidden = on;
                if (!on) input.value = input.dataset.original;
            });
            const first = inputs[1] || inputs[0];
            if (on && first) first.focus();
        };
        start.addEventListener('click', () => toggle(true));
        $('[data-as-edit-cancel]', card).addEventListener('click', () => toggle(false));
        inputs.forEach((input) => input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); $('[data-as-edit-send]', card).click(); }
            if (e.key === 'Escape') toggle(false);
        }));
        $('[data-as-edit-send]', card).addEventListener('click', () => {
            const changes = inputs.filter((i) => i.value.trim() && i.value.trim() !== i.dataset.original.trim())
                .map((i) => i.dataset.label + ': ' + i.value.trim());
            if (!changes.length) { toggle(false); return; }
            box.value = 'Corrija o rascunho: ' + changes.join('; ') + '.';
            grow();
            form.requestSubmit();
        });
    });
})();
