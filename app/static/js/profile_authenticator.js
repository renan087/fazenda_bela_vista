(() => {
    const panel = document.querySelector('[data-authenticator-panel]');
    if (!panel) return;

    const BASE = '/meu-perfil/seguranca/autenticador';
    const rootPage = document.querySelector('[data-profile-page]');
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

    const statusPill = panel.querySelector('[data-auth-status-pill]');
    const errorBox = panel.querySelector('[data-auth-error]');
    const infoBox = panel.querySelector('[data-auth-info]');

    const recoveryReveal = panel.querySelector('[data-auth-recovery-reveal]');
    const recoveryGrid = panel.querySelector('[data-auth-recovery-grid]');
    const copyRecoveryBtn = panel.querySelector('[data-auth-copy-recovery]');
    const recoveryAck = panel.querySelector('[data-auth-recovery-ack]');
    const recoveryDoneBtn = panel.querySelector('[data-auth-recovery-done]');

    const enrollWrap = panel.querySelector('[data-auth-enroll-wrap]');
    const unavailableBanner = panel.querySelector('[data-auth-unavailable-banner]');
    const startForm = panel.querySelector('[data-auth-start-form]');
    const startBtn = panel.querySelector('[data-auth-start-btn]');
    const emailForm = panel.querySelector('[data-auth-email-form]');
    const emailTarget = panel.querySelector('[data-auth-email-target]');
    const totpForm = panel.querySelector('[data-auth-totp-form]');
    const qrImg = panel.querySelector('[data-auth-qr-img]');
    const manualKeyEl = panel.querySelector('[data-auth-manual-key]');
    const copyKeyBtn = panel.querySelector('[data-auth-copy-key]');
    const totpMetaEl = panel.querySelector('[data-auth-totp-meta]');

    const activeWrap = panel.querySelector('[data-auth-active-wrap]');
    const recoverySummary = panel.querySelector('[data-auth-recovery-summary]');
    const prefButtons = Array.from(panel.querySelectorAll('[data-auth-pref-opt]'));
    const manageForms = Array.from(panel.querySelectorAll('[data-auth-manage-form]'));

    // Estado em memória (nunca persiste QR, chave manual ou códigos em storage/telemetria)
    const state = {
        csrfToken: rootPage?.getAttribute('data-csrf') || '',
        enabled: panel.getAttribute('data-auth-enabled') === '1',
        enrollmentAvailable: panel.getAttribute('data-auth-enrollment-available') === '1',
        emailEnabled: panel.getAttribute('data-auth-email-enabled') === '1',
        preferredMethod: panel.getAttribute('data-auth-preferred') || 'none',
        recoveryRemaining: Number.parseInt(panel.getAttribute('data-auth-recovery-remaining') || '0', 10) || 0,
        enrollmentStage: 'start', // 'start' | 'email' | 'totp'
        manualKey: '',
        recoveryCodes: [],
        pendingPreferredMethod: null,
        cooldownUntil: 0,
        cooldownTimer: null,
    };

    const clearMessages = () => {
        if (errorBox) { errorBox.hidden = true; errorBox.textContent = ''; }
        if (infoBox) { infoBox.hidden = true; infoBox.textContent = ''; }
    };

    const showError = (message) => {
        if (!errorBox) return;
        errorBox.textContent = message;
        errorBox.hidden = false;
        if (infoBox) infoBox.hidden = true;
    };

    const showInfo = (message) => {
        if (!infoBox) return;
        infoBox.textContent = message;
        infoBox.hidden = false;
        if (errorBox) errorBox.hidden = true;
    };

    const clearSensitiveMemory = () => {
        state.manualKey = '';
        state.recoveryCodes = [];
        if (qrImg) qrImg.removeAttribute('src');
        if (manualKeyEl) manualKeyEl.textContent = '';
        if (recoveryGrid) recoveryGrid.innerHTML = '';
        if (recoveryAck) recoveryAck.checked = false;
        if (recoveryDoneBtn) recoveryDoneBtn.disabled = true;
    };

    const extractError = (payload, fallback) => {
        const detail = payload && payload.detail;
        if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
            return {
                code: String(detail.code || ''),
                message: String(detail.message || fallback),
            };
        }
        if (Array.isArray(detail) && detail.length > 0) {
            return { code: 'validation_error', message: String(detail[0].msg || fallback) };
        }
        if (typeof detail === 'string' && detail.trim()) {
            return { code: '', message: detail.trim() };
        }
        return { code: '', message: fallback };
    };

    const applyStatusPayload = (data) => {
        if (!data || typeof data !== 'object') return;
        if (data.csrf_token) state.csrfToken = data.csrf_token;
        if (typeof data.enabled === 'boolean') state.enabled = data.enabled;
        if (typeof data.enrollment_available === 'boolean') state.enrollmentAvailable = data.enrollment_available;
        if (typeof data.email_enabled === 'boolean') state.emailEnabled = data.email_enabled;
        if (typeof data.preferred_method === 'string') state.preferredMethod = data.preferred_method;
        if (typeof data.recovery_codes_remaining === 'number') state.recoveryRemaining = data.recovery_codes_remaining;
    };

    const closeAllManageForms = () => {
        state.pendingPreferredMethod = null;
        manageForms.forEach((form) => {
            form.hidden = true;
            form.reset();
            setFormMethod(form, 'totp');
        });
    };

    const renderView = () => {
        // Pílula de status no cabeçalho
        if (statusPill) {
            if (state.enabled) {
                statusPill.textContent = 'Ativo';
                statusPill.className = 'mp-pill mp-pill-ok';
            } else if (state.enrollmentAvailable) {
                statusPill.textContent = 'Não vinculado';
                statusPill.className = 'mp-pill mp-pill-muted';
            } else {
                statusPill.textContent = 'Indisponível';
                statusPill.className = 'mp-pill mp-pill-warn';
            }
        }

        // Se há códigos de recuperação recém-gerados em memória, exibe-os com prioridade
        const showingRecovery = state.recoveryCodes.length > 0;
        if (recoveryReveal) recoveryReveal.hidden = !showingRecovery;
        if (enrollWrap) enrollWrap.hidden = showingRecovery || state.enabled;
        if (activeWrap) activeWrap.hidden = showingRecovery || !state.enabled;

        if (showingRecovery) {
            if (recoveryGrid) {
                recoveryGrid.innerHTML = '';
                state.recoveryCodes.forEach((code) => {
                    const item = document.createElement('div');
                    item.className = 'mp-recovery-code';
                    item.textContent = code;
                    recoveryGrid.appendChild(item);
                });
            }
            return;
        }

        // Estado não vinculado
        if (!state.enabled) {
            if (unavailableBanner) unavailableBanner.hidden = state.enrollmentAvailable;
            if (startForm) {
                startForm.hidden = state.enrollmentStage !== 'start';
                const pwInput = startForm.querySelector('input[name="current_password"]');
                if (pwInput) pwInput.disabled = !state.enrollmentAvailable;
            }
            if (startBtn) startBtn.disabled = !state.enrollmentAvailable;
            if (emailForm) emailForm.hidden = state.enrollmentStage !== 'email';
            if (totpForm) totpForm.hidden = state.enrollmentStage !== 'totp';
            return;
        }

        // Estado vinculado
        if (recoverySummary) {
            const count = state.recoveryRemaining;
            recoverySummary.textContent = `${count} de 10 código${count === 1 ? '' : 's'} disponíve${count === 1 ? 'l' : 'is'} para acesso de emergência.`;
        }
        const effectivePref = state.pendingPreferredMethod || (state.preferredMethod === 'email_then_totp' ? 'email_then_totp' : 'totp');
        prefButtons.forEach((btn) => {
            const val = btn.getAttribute('data-auth-pref-opt');
            const isSelected = val === effectivePref;
            const isDisabled = val === 'email_then_totp' && !state.emailEnabled;
            btn.classList.toggle('is-selected', isSelected);
            btn.classList.toggle('is-disabled', isDisabled);
            btn.disabled = isDisabled;
            btn.setAttribute('aria-checked', isSelected ? 'true' : 'false');
            btn.setAttribute('aria-disabled', isDisabled ? 'true' : 'false');
        });
    };

    const setFormMethod = (form, method) => {
        form.dataset.selectedMethod = method;
        const codeInput = form.querySelector('input[name="code"]');
        const codeLabel = form.querySelector('[data-code-label]');
        form.querySelectorAll('[data-method-btn]').forEach((btn) => {
            const active = btn.getAttribute('data-method-btn') === method;
            btn.setAttribute('aria-pressed', active ? 'true' : 'false');
        });
        if (codeInput && codeLabel) {
            if (method === 'recovery') {
                codeLabel.textContent = 'Código de recuperação';
                codeInput.inputMode = 'text';
                codeInput.maxLength = 64;
                codeInput.placeholder = 'Ex.: ABCD-1234';
                codeInput.autocomplete = 'off';
            } else {
                codeLabel.textContent = 'Código do aplicativo (6 dígitos)';
                codeInput.inputMode = 'numeric';
                codeInput.maxLength = 6;
                codeInput.placeholder = '000000';
                codeInput.autocomplete = 'one-time-code';
            }
            codeInput.value = '';
        }
    };

    const startRateLimitCountdown = (seconds, baseMessage) => {
        let remaining = Math.max(1, Number.parseInt(String(seconds || 10), 10) || 10);
        if (state.cooldownTimer) window.clearInterval(state.cooldownTimer);
        const renderMsg = () => showError(`${baseMessage} Tente novamente em ${remaining}s.`);
        renderMsg();
        state.cooldownTimer = window.setInterval(() => {
            remaining -= 1;
            if (remaining <= 0) {
                window.clearInterval(state.cooldownTimer);
                state.cooldownTimer = null;
                clearMessages();
            } else {
                renderMsg();
            }
        }, 1000);
    };

    const fetchStatus = async ({ keepError = false } = {}) => {
        if (!keepError) clearMessages();
        try {
            const res = await fetch(BASE, {
                method: 'GET',
                credentials: 'same-origin',
                headers: { Accept: 'application/json' },
            });
            if (res.status === 401 || res.redirected) {
                window.location.href = '/login';
                return;
            }
            const data = await res.json().catch(() => null);
            if (res.ok && data) {
                applyStatusPayload(data);
                renderView();
            }
        } catch (_err) {
            // Mantém estado SSR em caso de falha transitória
        }
    };

    const postAction = async (path, fields) => {
        const fd = new FormData();
        fd.append('csrf_token', state.csrfToken);
        Object.entries(fields || {}).forEach(([k, v]) => {
            if (v !== undefined && v !== null) fd.append(k, String(v));
        });
        const res = await fetch(`${BASE}${path}`, {
            method: 'POST',
            credentials: 'same-origin',
            headers: { Accept: 'application/json' },
            body: fd,
        });
        if (res.status === 401 || (res.redirected && res.url.includes('/login'))) {
            showError('Sua sessão expirou. Redirecionando para o login…');
            window.location.href = '/login';
            return { ok: false, handled: true };
        }
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            const err = extractError(data, 'Não foi possível concluir a operação agora.');
            if (res.status === 429) {
                const retryAfter = res.headers.get('Retry-After');
                startRateLimitCountdown(retryAfter || 30, err.message);
                return { ok: false, handled: true, status: 429, err };
            }
            if (res.status === 409) {
                clearSensitiveMemory();
                state.enrollmentStage = 'start';
                closeAllManageForms();
                showError(err.message);
                await fetchStatus({ keepError: true });
                return { ok: false, handled: true, status: 409, err };
            }
            if (err.code === 'reused_code') {
                showError(`${err.message} Aguarde o próximo código de 30 segundos no aplicativo.`);
            } else {
                showError(err.message);
            }
            return { ok: false, handled: true, status: res.status, err };
        }
        return { ok: true, data };
    };

    const enterTotpStage = (data) => {
        state.enrollmentStage = 'totp';
        state.manualKey = String(data.manual_key || '');
        if (qrImg && data.qr_code_data_url) {
            qrImg.src = data.qr_code_data_url;
        }
        if (manualKeyEl) manualKeyEl.textContent = state.manualKey;
        if (totpMetaEl) {
            const digits = data.digits || 6;
            const period = data.period || 30;
            const algo = data.algorithm || 'SHA1';
            totpMetaEl.textContent = `${digits} dígitos · ${period}s · ${algo}`;
        }
        renderView();
        totpForm?.querySelector('input[name="code"]')?.focus();
    };

    // --- Etapa 1: Iniciar vinculação ---
    if (startForm) {
        startForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            clearMessages();
            const pwInput = startForm.querySelector('input[name="current_password"]');
            const password = (pwInput?.value || '').trim();
            if (!password) {
                showError('Informe sua senha atual para iniciar a vinculação.');
                pwInput?.focus();
                return;
            }
            if (startBtn) startBtn.disabled = true;
            const result = await postAction('/vincular/iniciar', { current_password: password });
            if (startBtn) startBtn.disabled = !state.enrollmentAvailable;
            if (!result.ok) return;

            startForm.reset();
            if (result.data.stage === 'email') {
                state.enrollmentStage = 'email';
                if (emailTarget && result.data.email) emailTarget.textContent = result.data.email;
                renderView();
                emailForm?.querySelector('input[name="code"]')?.focus();
            } else {
                enterTotpStage(result.data);
            }
        });
    }

    // --- Etapa 2A: Confirmar código do e-mail ---
    if (emailForm) {
        emailForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            clearMessages();
            const input = emailForm.querySelector('input[name="code"]');
            const btn = emailForm.querySelector('[data-auth-email-btn]');
            const code = (input?.value || '').trim();
            if (!code) {
                showError('Informe o código enviado para o seu e-mail.');
                input?.focus();
                return;
            }
            if (btn) btn.disabled = true;
            const result = await postAction('/vincular/email', { code });
            if (btn) btn.disabled = false;
            if (!result.ok) return;
            emailForm.reset();
            enterTotpStage(result.data);
        });
    }

    // --- Etapa 2B: Confirmar código TOTP e ativar ---
    if (totpForm) {
        totpForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            clearMessages();
            const input = totpForm.querySelector('input[name="code"]');
            const btn = totpForm.querySelector('[data-auth-confirm-btn]');
            const code = (input?.value || '').replace(/\s+/g, '');
            if (!code) {
                showError('Digite o código de 6 dígitos exibido no aplicativo.');
                input?.focus();
                return;
            }
            if (btn) btn.disabled = true;
            const result = await postAction('/vincular/confirmar', { code });
            if (btn) btn.disabled = false;
            if (!result.ok) return;

            totpForm.reset();
            clearSensitiveMemory();
            applyStatusPayload(result.data);
            state.enrollmentStage = 'start';
            state.recoveryCodes = Array.isArray(result.data.recovery_codes) ? result.data.recovery_codes.slice() : [];
            showToast('Aplicativo autenticador ativado.');
            renderView();
        });
    }

    // --- Cancelar vinculação ---
    panel.querySelectorAll('[data-auth-cancel-btn]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            clearMessages();
            await postAction('/vincular/cancelar', {});
            clearSensitiveMemory();
            state.enrollmentStage = 'start';
            emailForm?.reset();
            totpForm?.reset();
            renderView();
            showToast('Vinculação cancelada.');
        });
    });

    // --- Copiar chave manual ---
    if (copyKeyBtn) {
        copyKeyBtn.addEventListener('click', async () => {
            if (!state.manualKey) return;
            try {
                await navigator.clipboard.writeText(state.manualKey);
                showToast('Chave copiada para a área de transferência.');
            } catch (_err) {
                showToast('Selecione e copie a chave manualmente.');
            }
        });
    }

    // --- Códigos de recuperação (copiar + confirmação obrigatória) ---
    if (copyRecoveryBtn) {
        copyRecoveryBtn.addEventListener('click', async () => {
            if (!state.recoveryCodes.length) return;
            try {
                await navigator.clipboard.writeText(state.recoveryCodes.join('\n'));
                showToast('10 códigos de recuperação copiados.');
            } catch (_err) {
                showToast('Não foi possível copiar automaticamente. Anote os códigos exibidos.');
            }
        });
    }

    if (recoveryAck && recoveryDoneBtn) {
        recoveryAck.addEventListener('change', () => {
            recoveryDoneBtn.disabled = !recoveryAck.checked;
        });
        recoveryDoneBtn.addEventListener('click', () => {
            clearSensitiveMemory();
            clearMessages();
            renderView();
            showInfo('Aplicativo autenticador configurado e pronto para uso.');
        });
    }

    // --- Gerenciamento (quando ativo): preferência, regenerar códigos, desvincular ---
    prefButtons.forEach((btn) => {
        btn.addEventListener('click', () => {
            if (btn.disabled) return;
            const chosen = btn.getAttribute('data-auth-pref-opt');
            const current = state.preferredMethod === 'email_then_totp' ? 'email_then_totp' : 'totp';
            clearMessages();
            closeAllManageForms();
            if (chosen === current) {
                renderView();
                return;
            }
            state.pendingPreferredMethod = chosen;
            renderView();
            const form = panel.querySelector('[data-auth-manage-form="preference"]');
            if (form) {
                form.hidden = false;
                form.querySelector('input[name="current_password"]')?.focus();
            }
        });
    });

    panel.querySelectorAll('[data-open-manage]').forEach((btn) => {
        btn.addEventListener('click', () => {
            const action = btn.getAttribute('data-open-manage');
            clearMessages();
            closeAllManageForms();
            renderView();
            const form = panel.querySelector(`[data-auth-manage-form="${action}"]`);
            if (form) {
                form.hidden = false;
                form.querySelector('input[name="current_password"]')?.focus();
            }
        });
    });

    manageForms.forEach((form) => {
        setFormMethod(form, 'totp');

        form.querySelectorAll('[data-method-btn]').forEach((btn) => {
            btn.addEventListener('click', () => {
                setFormMethod(form, btn.getAttribute('data-method-btn') || 'totp');
            });
        });

        form.querySelector('[data-manage-cancel]')?.addEventListener('click', () => {
            clearMessages();
            closeAllManageForms();
            renderView();
        });

        form.addEventListener('submit', async (event) => {
            event.preventDefault();
            clearMessages();
            const action = form.getAttribute('data-auth-manage-form');
            const method = form.dataset.selectedMethod || 'totp';
            const pw = (form.querySelector('input[name="current_password"]')?.value || '').trim();
            const code = (form.querySelector('input[name="code"]')?.value || '').trim();
            const submitBtn = form.querySelector('[data-manage-submit]');

            if (!pw) {
                showError('Informe sua senha atual.');
                form.querySelector('input[name="current_password"]')?.focus();
                return;
            }
            if (!code) {
                showError(method === 'recovery' ? 'Informe um código de recuperação.' : 'Informe o código de 6 dígitos do aplicativo.');
                form.querySelector('input[name="code"]')?.focus();
                return;
            }

            const payload = { action, current_password: pw, code, method };
            if (action === 'preference') {
                payload.preferred_method = state.pendingPreferredMethod || 'totp';
            }

            if (submitBtn) submitBtn.disabled = true;
            const result = await postAction('/gerenciar', payload);
            if (submitBtn) submitBtn.disabled = false;
            if (!result.ok) return;

            closeAllManageForms();
            applyStatusPayload(result.data);

            if (action === 'regenerate_recovery_codes' && Array.isArray(result.data.recovery_codes)) {
                state.recoveryCodes = result.data.recovery_codes.slice();
                showToast('Novos códigos de recuperação gerados.');
            } else if (action === 'disable') {
                clearSensitiveMemory();
                state.enrollmentStage = 'start';
                showInfo('Aplicativo autenticador desvinculado. Sua conta voltou à verificação por e-mail.');
                showToast('Aplicativo desvinculado.');
            } else {
                showToast('Preferência de autenticação atualizada.');
            }
            renderView();
        });
    });

    // Limpa QR, chave e códigos da memória ao sair da tela
    window.addEventListener('pagehide', clearSensitiveMemory);

    renderView();
    fetchStatus();
})();
