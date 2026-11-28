(() => {
    const root = document.querySelector('[data-profile-page]');
    if (!root) return;

    const csrfToken = root.getAttribute('data-csrf') || '';
    const isMobileViewport = () => window.matchMedia('(max-width: 639px)').matches;

    // --- Toast ---
    const toastEl = document.querySelector('[data-mp-toast]');
    const toastText = document.querySelector('[data-mp-toast-text]');
    let toastTimer = null;
    const showToast = (message) => {
        if (!toastEl || !toastText) return;
        toastText.textContent = message;
        toastEl.classList.add('is-open');
        clearTimeout(toastTimer);
        toastTimer = setTimeout(() => toastEl.classList.remove('is-open'), 2600);
    };

    // --- Prevenir envio duplo ---
    document.querySelectorAll('[data-disable-on-submit]').forEach((form) => {
        form.addEventListener('submit', () => {
            const btn = form.querySelector('[data-submit-button]');
            if (!btn) return;
            btn.disabled = true;
        });
    });

    // --- Dialogs (<dialog>) ---
    const closeDialog = (dlg) => {
        if (dlg && dlg.open) dlg.close();
    };
    document.querySelectorAll('dialog.mp-dialog').forEach((dlg) => {
        dlg.addEventListener('click', (event) => {
            if (event.target === dlg) closeDialog(dlg);
        });
        dlg.querySelectorAll('[data-dialog-close]').forEach((btn) => {
            btn.addEventListener('click', () => closeDialog(dlg));
        });
    });

    // =========================================================================
    // FOTO DO PERFIL (Menu mobile ao tocar + Editor com recorte e zoom)
    // =========================================================================
    const avatarTrigger = root.querySelector('[data-avatar-trigger]');
    const avatarOpenEditorBtn = root.querySelector('[data-avatar-open-editor]');
    const avatarMenuDlg = document.querySelector('[data-avatar-menu]');
    const avatarMenuEditBtn = document.querySelector('[data-avatar-menu-edit]');
    const editorDlg = document.querySelector('[data-avatar-editor-dialog]');
    const uploadForm = document.querySelector('[data-avatar-upload-form]');
    const hiddenFileInput = document.querySelector('[data-avatar-hidden-input]');
    const pickerInput = document.querySelector('[data-avatar-picker]');
    const canvas = document.querySelector('[data-avatar-canvas]');
    const zoomInput = document.querySelector('[data-avatar-zoom]');
    const zoomVal = document.querySelector('[data-avatar-zoom-val]');
    const resetBtn = document.querySelector('[data-avatar-reset]');
    const applyBtn = document.querySelector('[data-avatar-apply]');
    const feedbackEl = document.querySelector('[data-avatar-feedback]');

    const avState = {
        img: null,
        objectUrl: null,
        fileName: 'avatar',
        baseScale: 1,
        scale: 1,
        offsetX: 0,
        offsetY: 0,
        pointerId: null,
        startX: 0,
        startY: 0,
        startOffsetX: 0,
        startOffsetY: 0,
    };

    const ctx = canvas ? canvas.getContext('2d') : null;
    const viewportSize = canvas ? canvas.width : 240;

    const clampAvatarOffsets = () => {
        if (!avState.img) return;
        const drawW = avState.img.width * avState.scale;
        const drawH = avState.img.height * avState.scale;
        const maxX = Math.max(0, (drawW - viewportSize) / 2);
        const maxY = Math.max(0, (drawH - viewportSize) / 2);
        avState.offsetX = Math.min(maxX, Math.max(-maxX, avState.offsetX));
        avState.offsetY = Math.min(maxY, Math.max(-maxY, avState.offsetY));
    };

    const drawAvatar = (size = viewportSize, targetCtx = ctx) => {
        if (!targetCtx) return;
        targetCtx.clearRect(0, 0, size, size);
        targetCtx.fillStyle = '#EEF3EB';
        targetCtx.fillRect(0, 0, size, size);
        if (!avState.img) {
            targetCtx.fillStyle = '#5B6660';
            targetCtx.font = '500 13px Inter, sans-serif';
            targetCtx.textAlign = 'center';
            targetCtx.fillText('Escolha uma imagem', size / 2, size / 2 + 4);
            return;
        }
        const ratio = size / viewportSize;
        const drawW = avState.img.width * avState.scale * ratio;
        const drawH = avState.img.height * avState.scale * ratio;
        const dx = (size - drawW) / 2 + avState.offsetX * ratio;
        const dy = (size - drawH) / 2 + avState.offsetY * ratio;
        targetCtx.drawImage(avState.img, dx, dy, drawW, drawH);
    };

    const resetAvatarDraft = () => {
        if (avState.objectUrl) URL.revokeObjectURL(avState.objectUrl);
        avState.img = null;
        avState.objectUrl = null;
        avState.scale = 1;
        avState.baseScale = 1;
        avState.offsetX = 0;
        avState.offsetY = 0;
        if (zoomInput) zoomInput.value = '1';
        if (zoomVal) zoomVal.textContent = '100%';
        if (pickerInput) pickerInput.value = '';
        if (feedbackEl) feedbackEl.textContent = 'Escolha uma imagem e arraste para ajustar o enquadramento.';
        drawAvatar();
    };

    const openEditorDialog = () => {
        resetAvatarDraft();
        if (editorDlg && typeof editorDlg.showModal === 'function') {
            editorDlg.showModal();
        }
    };

    if (avatarTrigger) {
        avatarTrigger.addEventListener('click', () => {
            if (isMobileViewport() && avatarMenuDlg && typeof avatarMenuDlg.showModal === 'function') {
                avatarMenuDlg.showModal();
            } else {
                openEditorDialog();
            }
        });
    }

    if (avatarOpenEditorBtn) {
        avatarOpenEditorBtn.addEventListener('click', openEditorDialog);
    }

    if (avatarMenuEditBtn) {
        avatarMenuEditBtn.addEventListener('click', () => {
            closeDialog(avatarMenuDlg);
            openEditorDialog();
        });
    }

    if (pickerInput) {
        pickerInput.addEventListener('change', (event) => {
            const [file] = event.target.files || [];
            if (!file) return;
            if (!file.type.startsWith('image/')) {
                if (feedbackEl) feedbackEl.textContent = 'Selecione um arquivo de imagem válido.';
                return;
            }
            const nextUrl = URL.createObjectURL(file);
            const img = new Image();
            img.onload = () => {
                if (avState.objectUrl) URL.revokeObjectURL(avState.objectUrl);
                avState.objectUrl = nextUrl;
                avState.img = img;
                avState.fileName = file.name || 'avatar';
                avState.baseScale = Math.max(viewportSize / img.width, viewportSize / img.height);
                avState.scale = avState.baseScale;
                avState.offsetX = 0;
                avState.offsetY = 0;
                if (zoomInput) zoomInput.value = '1';
                if (zoomVal) zoomVal.textContent = '100%';
                clampAvatarOffsets();
                drawAvatar();
                if (feedbackEl) feedbackEl.textContent = 'Arraste para centralizar e clique em Salvar foto.';
            };
            img.onerror = () => {
                URL.revokeObjectURL(nextUrl);
                if (feedbackEl) feedbackEl.textContent = 'Não foi possível abrir a imagem selecionada.';
            };
            img.src = nextUrl;
        });
    }

    if (zoomInput) {
        zoomInput.addEventListener('input', () => {
            const z = Number(zoomInput.value || 1);
            if (zoomVal) zoomVal.textContent = `${Math.round(z * 100)}%`;
            if (!avState.img) return;
            avState.scale = avState.baseScale * z;
            clampAvatarOffsets();
            drawAvatar();
        });
    }

    if (resetBtn) {
        resetBtn.addEventListener('click', () => {
            if (!avState.img) return;
            avState.scale = avState.baseScale;
            avState.offsetX = 0;
            avState.offsetY = 0;
            if (zoomInput) zoomInput.value = '1';
            if (zoomVal) zoomVal.textContent = '100%';
            drawAvatar();
        });
    }

    if (canvas) {
        canvas.addEventListener('pointerdown', (event) => {
            if (!avState.img) return;
            avState.pointerId = event.pointerId;
            avState.startX = event.clientX;
            avState.startY = event.clientY;
            avState.startOffsetX = avState.offsetX;
            avState.startOffsetY = avState.offsetY;
            canvas.setPointerCapture(event.pointerId);
        });
        canvas.addEventListener('pointermove', (event) => {
            if (avState.pointerId !== event.pointerId || !avState.img) return;
            avState.offsetX = avState.startOffsetX + (event.clientX - avState.startX);
            avState.offsetY = avState.startOffsetY + (event.clientY - avState.startY);
            clampAvatarOffsets();
            drawAvatar();
        });
        const release = (event) => {
            if (avState.pointerId !== event.pointerId) return;
            canvas.releasePointerCapture(event.pointerId);
            avState.pointerId = null;
        };
        canvas.addEventListener('pointerup', release);
        canvas.addEventListener('pointercancel', release);
    }

    if (applyBtn && uploadForm && hiddenFileInput) {
        applyBtn.addEventListener('click', async () => {
            if (!avState.img) {
                if (feedbackEl) feedbackEl.textContent = 'Escolha uma imagem antes de salvar.';
                return;
            }
            applyBtn.disabled = true;
            if (feedbackEl) feedbackEl.textContent = 'Preparando e enviando foto…';
            const out = document.createElement('canvas');
            out.width = 512;
            out.height = 512;
            drawAvatar(512, out.getContext('2d'));

            const toBlob = (type, quality) => new Promise((resolve) => out.toBlob(resolve, type, quality));
            let blob = await toBlob('image/webp', 0.86);
            if (!blob || !blob.type || blob.type === 'image/png') {
                blob = await toBlob('image/jpeg', 0.88);
            }
            if (!blob) {
                applyBtn.disabled = false;
                if (feedbackEl) feedbackEl.textContent = 'Não foi possível preparar a foto. Tente outra imagem.';
                return;
            }
            const ext = blob.type === 'image/webp' ? 'webp' : 'jpg';
            const baseName = (avState.fileName || 'avatar')
                .replace(/\.[^.]+$/, '')
                .replace(/[^a-zA-Z0-9_-]+/g, '-')
                .replace(/^-+|-+$/g, '') || 'avatar';
            const file = new File([blob], `${baseName}-avatar.${ext}`, { type: blob.type });
            const dt = new DataTransfer();
            dt.items.add(file);
            hiddenFileInput.files = dt.files;
            uploadForm.submit();
        });
    }

    // =========================================================================
    // ABA PERFIL: máscara de telefone, barra de alterações pendentes e e-mail
    // =========================================================================
    const profileForm = root.querySelector('[data-profile-form]');
    if (profileForm) {
        const trackedFields = Array.from(profileForm.querySelectorAll('[data-track-field]'));
        const dirtyBar = root.querySelector('[data-dirty-bar]');
        const discardBtn = root.querySelector('[data-dirty-discard]');
        const initialValues = new Map(trackedFields.map((el) => [el.name, el.value]));

        const syncDirty = () => {
            const dirty = trackedFields.some((el) => el.value !== initialValues.get(el.name));
            if (dirtyBar) dirtyBar.hidden = !dirty;
        };

        const phoneInput = profileForm.querySelector('[data-phone-mask]');
        if (phoneInput) {
            phoneInput.addEventListener('input', () => {
                const digits = phoneInput.value.replace(/\D/g, '').slice(0, 11);
                if (digits.length > 6) {
                    const split = digits.length === 11 ? 7 : 6;
                    phoneInput.value = `(${digits.slice(0, 2)}) ${digits.slice(2, split)}-${digits.slice(split)}`;
                } else if (digits.length > 2) {
                    phoneInput.value = `(${digits.slice(0, 2)}) ${digits.slice(2)}`;
                } else if (digits.length > 0) {
                    phoneInput.value = `(${digits}`;
                }
            });
        }

        trackedFields.forEach((el) => el.addEventListener('input', syncDirty));

        if (discardBtn) {
            discardBtn.addEventListener('click', () => {
                trackedFields.forEach((el) => {
                    el.value = initialValues.get(el.name) || '';
                });
                syncDirty();
                showToast('Alterações descartadas.');
            });
        }

        profileForm.addEventListener('submit', () => {
            const saveBtn = root.querySelector('[data-dirty-save]');
            if (saveBtn) saveBtn.disabled = true;
        });
    }

    const emailToggleBtn = root.querySelector('[data-email-toggle]');
    const emailBox = root.querySelector('[data-email-box]');
    const emailCancelBtn = root.querySelector('[data-email-cancel]');
    const emailInput = root.querySelector('[data-email-input]');
    if (emailToggleBtn && emailBox) {
        emailToggleBtn.addEventListener('click', () => {
            emailBox.hidden = !emailBox.hidden;
            if (!emailBox.hidden && emailInput) emailInput.focus();
        });
    }
    if (emailCancelBtn && emailBox) {
        emailCancelBtn.addEventListener('click', () => {
            emailBox.hidden = true;
            if (emailInput) emailInput.value = '';
        });
    }

    // =========================================================================
    // ABA SEGURANÇA: visibilidade de senha, medidor de força, beacon OTP e revogação
    // =========================================================================
    root.querySelectorAll('[data-password-visibility]').forEach((wrap) => {
        const input = wrap.querySelector('[data-password-input]');
        const toggle = wrap.querySelector('[data-password-toggle]');
        const eyeOpen = wrap.querySelector('[data-eye-open]');
        const eyeClosed = wrap.querySelector('[data-eye-closed]');
        if (!input || !toggle || !eyeOpen || !eyeClosed) return;

        toggle.addEventListener('click', () => {
            const show = input.type === 'password';
            input.type = show ? 'text' : 'password';
            toggle.setAttribute('aria-label', show ? 'Ocultar senha' : 'Mostrar senha');
            toggle.setAttribute('aria-pressed', show ? 'true' : 'false');
            eyeOpen.classList.toggle('hidden', show);
            eyeClosed.classList.toggle('hidden', !show);
            input.focus({ preventScroll: true });
        });
    });

    const pwNewInput = root.querySelector('[data-pw-new]');
    const pwConfirmInput = root.querySelector('[data-pw-confirm]');
    const pwMeter = root.querySelector('[data-pw-meter]');
    const pwBars = root.querySelectorAll('[data-pw-bar]');
    const pwStr = root.querySelector('[data-pw-str]');
    const pwMismatch = root.querySelector('[data-pw-mismatch]');
    if (pwNewInput && pwMeter && pwStr) {
        const updateStrength = () => {
            const v = pwNewInput.value;
            pwMeter.hidden = v.length === 0;
            const score = [
                v.length >= 8,
                /\d/.test(v),
                /[a-z]/.test(v) && /[A-Z]/.test(v),
                /[^A-Za-z0-9]/.test(v),
            ].filter(Boolean).length;
            const colors = ['#B42318', '#D18A00', '#446A36', '#1F7A3A'];
            const labels = ['Fraca', 'Razoável', 'Boa', 'Forte'];
            pwBars.forEach((bar, i) => {
                bar.style.backgroundColor = i < score ? colors[Math.max(0, score - 1)] : '#E3E8E2';
            });
            pwStr.textContent = score ? `Força da senha: ${labels[score - 1]}` : 'Muito curta';
        };
        const checkMatch = () => {
            if (!pwConfirmInput || !pwMismatch) return;
            const bad = pwConfirmInput.value.length > 0 && pwConfirmInput.value !== pwNewInput.value;
            pwMismatch.classList.toggle('hidden', !bad);
        };
        pwNewInput.addEventListener('input', () => { updateStrength(); checkMatch(); });
        if (pwConfirmInput) pwConfirmInput.addEventListener('input', checkMatch);
    }

    const passwordConfirmForm = root.querySelector('[data-password-confirm-form]');
    if (passwordConfirmForm) {
        let keepPending = false;
        root.querySelectorAll('[data-security-flow-form]').forEach((form) => {
            form.addEventListener('submit', () => { keepPending = true; });
        });
        window.addEventListener('pagehide', () => {
            if (keepPending) return;
            const payload = new FormData();
            payload.append('csrf_token', csrfToken);
            navigator.sendBeacon('/meu-perfil/alterar-senha/cancelar', payload);
        });
    }

    const revokeOpenBtn = root.querySelector('[data-revoke-others-open]');
    const revokeConfirmBox = root.querySelector('[data-revoke-others-confirm]');
    const revokeCancelBtn = root.querySelector('[data-revoke-others-cancel]');
    if (revokeOpenBtn && revokeConfirmBox) {
        revokeOpenBtn.addEventListener('click', () => { revokeConfirmBox.hidden = false; });
    }
    if (revokeCancelBtn && revokeConfirmBox) {
        revokeCancelBtn.addEventListener('click', () => { revokeConfirmBox.hidden = true; });
    }

    // =========================================================================
    // ABA NOTIFICAÇÕES: salvamento imediato via fetch JSON
    // =========================================================================
    const notifSection = root.querySelector('[data-notifications-section]');
    if (notifSection) {
        const postNotifications = async (payload, toastMsg) => {
            try {
                const res = await fetch('/meu-perfil/notificacoes', {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
                    body: JSON.stringify({ csrf_token: csrfToken, ...payload }),
                });
                if (!res.ok) throw new Error('Falha ao salvar');
                showToast(toastMsg);
            } catch (_err) {
                showToast('Não foi possível salvar agora. Tente novamente.');
            }
        };

        notifSection.querySelectorAll('[data-notif-toggle]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const next = btn.getAttribute('aria-pressed') !== 'true';
                btn.setAttribute('aria-pressed', next ? 'true' : 'false');
                const key = btn.getAttribute('data-notif-key');
                const channel = btn.getAttribute('data-notif-channel');
                postNotifications(
                    { notifications: { [key]: { [channel]: next } } },
                    'Preferência de notificação salva.'
                );
            });
        });

        const quietSwitch = notifSection.querySelector('[data-quiet-switch]');
        const quietGrid = notifSection.querySelector('[data-quiet-grid]');
        const quietStart = notifSection.querySelector('[data-quiet-start]');
        const quietEnd = notifSection.querySelector('[data-quiet-end]');

        const sendQuiet = (msg) => {
            const enabled = quietSwitch ? quietSwitch.getAttribute('aria-checked') === 'true' : true;
            postNotifications(
                {
                    quiet_hours: {
                        enabled,
                        start: quietStart ? quietStart.value : '21:00',
                        end: quietEnd ? quietEnd.value : '07:00',
                    },
                },
                msg
            );
        };

        if (quietSwitch) {
            quietSwitch.addEventListener('click', () => {
                const next = quietSwitch.getAttribute('aria-checked') !== 'true';
                quietSwitch.setAttribute('aria-checked', next ? 'true' : 'false');
                if (quietGrid) quietGrid.classList.toggle('is-disabled', !next);
                if (quietStart) quietStart.disabled = !next;
                if (quietEnd) quietEnd.disabled = !next;
                sendQuiet(next ? 'Horário silencioso ativado.' : 'Horário silencioso desativado.');
            });
        }
        if (quietStart) quietStart.addEventListener('change', () => sendQuiet('Horário silencioso atualizado.'));
        if (quietEnd) quietEnd.addEventListener('change', () => sendQuiet('Horário silencioso atualizado.'));
    }

    // =========================================================================
    // ABA PREFERÊNCIAS: salvamento imediato + densidade em tempo real
    // =========================================================================
    const prefSection = root.querySelector('[data-preferences-section]');
    if (prefSection) {
        const postPreferences = async (payload, toastMsg) => {
            try {
                const res = await fetch('/meu-perfil/preferencias', {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
                    body: JSON.stringify({ csrf_token: csrfToken, ...payload }),
                });
                if (!res.ok) throw new Error('Falha ao salvar');
                showToast(toastMsg);
            } catch (_err) {
                showToast('Não foi possível salvar agora. Tente novamente.');
            }
        };

        const startSelect = prefSection.querySelector('[data-pref-start]');
        const ctxSelect = prefSection.querySelector('[data-pref-ctx]');
        const farmWrap = prefSection.querySelector('[data-pref-farm-wrap]');
        const farmSelect = prefSection.querySelector('[data-pref-farm]');

        if (startSelect) {
            startSelect.addEventListener('change', () => {
                postPreferences({ start_page: startSelect.value }, 'Página inicial atualizada.');
            });
        }
        if (ctxSelect) {
            ctxSelect.addEventListener('change', () => {
                const isFixed = ctxSelect.value === 'fixed';
                if (farmWrap) farmWrap.hidden = !isFixed;
                postPreferences({ context_mode: ctxSelect.value }, 'Preferência de entrada salva.');
            });
        }
        if (farmSelect) {
            farmSelect.addEventListener('change', () => {
                postPreferences(
                    { context_mode: 'fixed', default_farm_id: farmSelect.value ? Number(farmSelect.value) : null },
                    'Fazenda padrão atualizada.'
                );
            });
        }

        const densityBtns = prefSection.querySelectorAll('[data-pref-density]');
        densityBtns.forEach((btn) => {
            btn.addEventListener('click', () => {
                const density = btn.getAttribute('data-pref-density') || 'comfortable';
                document.body.setAttribute('data-ui-density', density);
                densityBtns.forEach((other) => {
                    const active = other === btn;
                    other.classList.toggle('is-selected', active);
                    other.setAttribute('aria-checked', active ? 'true' : 'false');
                });
                postPreferences(
                    { density },
                    density === 'compact' ? 'Densidade compacta aplicada.' : 'Densidade confortável aplicada.'
                );
            });
        });
    }
})();
