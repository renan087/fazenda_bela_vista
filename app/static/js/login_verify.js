(() => {
    const root = document.querySelector('[data-login-verify]');
    if (!root) return;

    const form = root.querySelector('[data-lv-form]');
    const csrfInput = root.querySelector('[data-lv-csrf]');
    const methodInput = root.querySelector('[data-lv-method]');
    const codeInput = root.querySelector('[data-lv-code-input]');
    const codeLabel = root.querySelector('[data-lv-code-label]');
    const submitBtn = root.querySelector('[data-lv-submit]');
    const errorBox = root.querySelector('[data-lv-error]');
    const infoBox = root.querySelector('[data-lv-info]');

    const heroTitle = root.querySelector('[data-lv-hero-title]');
    const heroDesc = root.querySelector('[data-lv-hero-desc]');
    const emailCard = root.querySelector('[data-lv-email-card]');
    const emailDisplay = root.querySelector('[data-lv-email-display]');
    const appCard = root.querySelector('[data-lv-app-card]');

    const eyebrow = root.querySelector('[data-lv-eyebrow]');
    const titleEl = root.querySelector('[data-lv-title]');
    const subtitleEl = root.querySelector('[data-lv-subtitle]');

    const trustWrap = root.querySelector('[data-lv-trust-wrap]');
    const trustInput = root.querySelector('[data-lv-trust-input]');

    const switchWrap = root.querySelector('[data-lv-method-switch-wrap]');
    const switchBtn = root.querySelector('[data-lv-method-switch]');

    const resendForm = root.querySelector('[data-resend-form]');
    const resendCsrf = root.querySelector('[data-resend-csrf]');
    const resendTrust = root.querySelector('[data-resend-trust-browser]');
    const resendBtn = root.querySelector('[data-resend-button]');

    const state = {
        csrfToken: root.dataset.csrf || (csrfInput ? csrfInput.value : ''),
        authenticatorRequired: root.dataset.authRequired === '1',
        stage: root.dataset.initialStage || 'email',
        methods: (root.dataset.initialMethods || 'email').split(',').filter(Boolean),
        activeMethod: (root.dataset.initialStage === 'totp') ? 'totp' : 'email',
        email: root.dataset.initialEmail || '',
        resendSeconds: Number.parseInt(root.dataset.initialCooldown || '0', 10) || 0,
        resendTimer: null,
        rateLimitTimer: null,
    };

    const setError = (msg) => {
        if (!errorBox) return;
        errorBox.textContent = msg;
        errorBox.classList.toggle('hidden', !msg);
        if (msg && infoBox) infoBox.classList.add('hidden');
    };

    const setInfo = (msg) => {
        if (!infoBox) return;
        infoBox.textContent = msg;
        infoBox.classList.toggle('hidden', !msg);
        if (msg && errorBox) errorBox.classList.add('hidden');
    };

    const extractError = (payload, fallback) => {
        const detail = payload && payload.detail;
        if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
            return { code: String(detail.code || ''), message: String(detail.message || fallback) };
        }
        if (Array.isArray(detail) && detail.length > 0) {
            return { code: 'validation_error', message: String(detail[0].msg || fallback) };
        }
        if (typeof detail === 'string' && detail.trim()) {
            return { code: '', message: detail.trim() };
        }
        return { code: '', message: fallback };
    };

    const syncCsrf = (token) => {
        if (!token) return;
        state.csrfToken = token;
        if (csrfInput) csrfInput.value = token;
        if (resendCsrf) resendCsrf.value = token;
    };

    const renderResendButton = () => {
        if (!resendBtn) return;
        if (state.resendSeconds > 0) {
            resendBtn.disabled = true;
            resendBtn.textContent = `Novo código em ${state.resendSeconds}s`;
        } else {
            resendBtn.disabled = false;
            resendBtn.textContent = 'Novo código';
        }
    };

    const startResendCountdown = (seconds) => {
        state.resendSeconds = Math.max(0, Number.parseInt(String(seconds || 0), 10) || 0);
        if (state.resendTimer) {
            window.clearInterval(state.resendTimer);
            state.resendTimer = null;
        }
        renderResendButton();
        if (state.resendSeconds <= 0) return;
        state.resendTimer = window.setInterval(() => {
            state.resendSeconds -= 1;
            renderResendButton();
            if (state.resendSeconds <= 0 && state.resendTimer) {
                window.clearInterval(state.resendTimer);
                state.resendTimer = null;
            }
        }, 1000);
    };

    const startRateLimitCountdown = (seconds, baseMessage) => {
        let remaining = Math.max(1, Number.parseInt(String(seconds || 15), 10) || 15);
        if (state.rateLimitTimer) window.clearInterval(state.rateLimitTimer);
        if (submitBtn) submitBtn.disabled = true;
        setError(`${baseMessage} Aguarde ${remaining}s para tentar novamente.`);
        state.rateLimitTimer = window.setInterval(() => {
            remaining -= 1;
            if (remaining <= 0) {
                window.clearInterval(state.rateLimitTimer);
                state.rateLimitTimer = null;
                if (submitBtn) submitBtn.disabled = false;
                setError('');
            } else {
                setError(`${baseMessage} Aguarde ${remaining}s para tentar novamente.`);
            }
        }, 1000);
    };

    const renderUI = () => {
        const isTotpStage = state.stage === 'totp';
        const isRecovery = isTotpStage && state.activeMethod === 'recovery';
        const hasRecovery = state.methods.includes('recovery');
        const hasTotp = state.methods.includes('totp');

        if (methodInput) methodInput.value = state.activeMethod;
        if (emailDisplay && state.email) emailDisplay.textContent = state.email;
        if (emailCard) emailCard.hidden = isTotpStage;
        if (appCard) appCard.hidden = !isTotpStage;

        if (heroTitle && heroDesc) {
            if (isTotpStage) {
                heroTitle.textContent = 'Confirme seu acesso com o aplicativo autenticador.';
                heroDesc.textContent = isRecovery
                    ? 'Use um dos seus códigos de recuperação de uso único para acessar sua conta com segurança.'
                    : 'Abra o Google Authenticator ou Microsoft Authenticator no celular e digite o código exibido para entrar com segurança.';
            } else {
                heroTitle.textContent = 'Confirme seu acesso com autenticação em duas etapas.';
                heroDesc.textContent = 'Enviamos um código numérico para o e-mail informado. Digite o código para liberar o acesso com segurança.';
            }
        }

        if (eyebrow) {
            if (state.authenticatorRequired && !isTotpStage) eyebrow.textContent = 'Etapa 1 de 2 · E-mail';
            else if (isRecovery) eyebrow.textContent = 'Código de recuperação';
            else if (isTotpStage) eyebrow.textContent = 'Verificação por aplicativo';
            else eyebrow.textContent = 'Verificação';
        }

        if (titleEl && subtitleEl) {
            if (isRecovery) {
                titleEl.textContent = 'Usar código de recuperação';
                subtitleEl.textContent = 'Informe um dos códigos de uso único guardados na ativação do autenticador.';
            } else if (isTotpStage) {
                titleEl.textContent = 'Código do autenticador';
                subtitleEl.textContent = 'Digite o código de 6 dígitos gerado no Google ou Microsoft Authenticator.';
            } else {
                titleEl.textContent = 'Informe o código de acesso';
                subtitleEl.textContent = 'Use o código numérico de 6 dígitos enviado para seu e-mail.';
            }
        }

        if (codeLabel && codeInput) {
            if (isRecovery) {
                codeLabel.textContent = 'Código de recuperação';
                codeInput.inputMode = 'text';
                codeInput.maxLength = 64;
                codeInput.placeholder = 'Ex.: ABCD-1234';
                codeInput.autocomplete = 'off';
                codeInput.className = 'w-full rounded-2xl border-slate-200 bg-slate-50 px-4 py-3 text-center font-mono text-lg tracking-widest';
            } else {
                codeLabel.textContent = isTotpStage ? 'Código de 6 dígitos' : 'Código';
                codeInput.inputMode = 'numeric';
                codeInput.maxLength = 6;
                codeInput.placeholder = '000000';
                codeInput.autocomplete = 'one-time-code';
                codeInput.className = 'w-full rounded-2xl border-slate-200 bg-slate-50 text-center text-2xl tracking-[0.55em]';
            }
        }

        // trust_browser só existe no fluxo legado de e-mail (authenticator_required = false)
        const allowTrust = !state.authenticatorRequired && !isTotpStage;
        if (trustWrap) trustWrap.classList.toggle('hidden', !allowTrust);
        if (trustInput) trustInput.disabled = !allowTrust;

        // Reenvio só existe na etapa de e-mail
        if (resendForm) resendForm.classList.toggle('hidden', isTotpStage);

        // Alternador Aplicativo <-> Recuperação (apenas em stage: totp quando ambos disponíveis)
        if (switchWrap && switchBtn) {
            const showSwitch = isTotpStage && hasRecovery && hasTotp;
            switchWrap.classList.toggle('hidden', !showSwitch);
            switchBtn.textContent = isRecovery ? 'Usar código do aplicativo' : 'Usar código de recuperação';
        }
    };

    const applyStateFromServer = (data) => {
        if (!data || typeof data !== 'object') return;
        syncCsrf(data.csrf_token);
        if (typeof data.authenticator_required === 'boolean') {
            state.authenticatorRequired = data.authenticator_required;
        }
        if (typeof data.stage === 'string') {
            state.stage = data.stage;
        }
        if (Array.isArray(data.methods) && data.methods.length) {
            state.methods = data.methods.slice();
        }
        if (data.email) state.email = data.email;
        if (state.stage === 'email') {
            state.activeMethod = 'email';
        } else if (!state.methods.includes(state.activeMethod)) {
            state.activeMethod = state.methods.includes('totp') ? 'totp' : (state.methods[0] || 'recovery');
        }
        if (typeof data.resend_available_in === 'number') {
            startResendCountdown(data.resend_available_in);
        }
        renderUI();
    };

    const refreshState = async () => {
        try {
            const res = await fetch('/login/verificacao/estado', {
                method: 'GET',
                credentials: 'same-origin',
                headers: { Accept: 'application/json' },
            });
            const data = await res.json().catch(() => null);
            if (res.status === 409) {
                const err = extractError(data, 'A verificação expirou. Entre novamente.');
                setError(err.message);
                return;
            }
            if (res.ok && data) {
                applyStateFromServer(data);
            }
        } catch (_err) {
            // Mantém estado renderizado pelo servidor
        }
    };

    if (switchBtn) {
        switchBtn.addEventListener('click', () => {
            setError('');
            state.activeMethod = state.activeMethod === 'recovery' ? 'totp' : 'recovery';
            renderUI();
            if (codeInput) {
                codeInput.value = '';
                codeInput.focus();
            }
        });
    }

    if (trustInput && resendTrust) {
        const syncTrust = () => { resendTrust.value = trustInput.checked ? 'on' : ''; };
        trustInput.addEventListener('change', syncTrust);
        syncTrust();
    }

    // Submissão via JSON quando autenticador ativo (suporta transição email -> totp -> complete)
    if (form) {
        form.addEventListener('submit', async (event) => {
            if (!state.authenticatorRequired) {
                return; // Fluxo legado de e-mail segue POST HTML tradicional
            }
            event.preventDefault();
            setError('');
            const code = (codeInput?.value || '').trim();
            if (!code) {
                setError('Informe o código para continuar.');
                codeInput?.focus();
                return;
            }

            const fd = new FormData();
            fd.append('csrf_token', state.csrfToken);
            fd.append('method', state.activeMethod);
            fd.append('code', code);

            if (submitBtn) submitBtn.disabled = true;
            try {
                const res = await fetch('/login/verificacao', {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { Accept: 'application/json' },
                    body: fd,
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) {
                    const err = extractError(data, 'Não foi possível validar o código agora.');
                    if (res.status === 429) {
                        startRateLimitCountdown(res.headers.get('Retry-After') || 20, err.message);
                        return;
                    }
                    if (submitBtn) submitBtn.disabled = false;
                    if (res.status === 409) {
                        setError(err.message);
                        if (err.code === 'wrong_stage') {
                            await refreshState();
                        }
                        return;
                    }
                    if (res.status === 503 && state.methods.includes('recovery') && state.activeMethod === 'totp') {
                        setError(`${err.message} Você ainda pode entrar usando um código de recuperação.`);
                        return;
                    }
                    if (err.code === 'reused_code') {
                        setError(`${err.message} Aguarde o próximo código no aplicativo.`);
                    } else {
                        setError(err.message);
                    }
                    return;
                }

                if (data.stage === 'complete' && data.redirect_url) {
                    window.location.href = data.redirect_url;
                    return;
                }

                // Transição email -> totp
                if (submitBtn) submitBtn.disabled = false;
                if (codeInput) codeInput.value = '';
                applyStateFromServer(data);
                setInfo('E-mail confirmado. Agora informe o código do aplicativo autenticador.');
                codeInput?.focus();
            } catch (_err) {
                if (submitBtn) submitBtn.disabled = false;
                setError('Erro de conexão ao validar o código. Tente novamente.');
            }
        });
    }

    // Reenvio por e-mail (JSON quando autenticador ativo, tradicional no legado)
    if (resendForm) {
        resendForm.addEventListener('submit', async (event) => {
            if (!state.authenticatorRequired) {
                if (trustInput && resendTrust) resendTrust.value = trustInput.checked ? 'on' : '';
                startResendCountdown(60);
                return;
            }
            event.preventDefault();
            setError('');
            const fd = new FormData();
            fd.append('csrf_token', state.csrfToken);
            if (resendBtn) resendBtn.disabled = true;
            try {
                const res = await fetch('/login/verificacao/reenviar', {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { Accept: 'application/json' },
                    body: fd,
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) {
                    const err = extractError(data, 'Não foi possível reenviar o código agora.');
                    if (res.status === 429) {
                        startResendCountdown(res.headers.get('Retry-After') || 60);
                    } else {
                        renderResendButton();
                    }
                    setError(err.message);
                    return;
                }
                applyStateFromServer(data);
                setInfo('Enviamos um novo código para o seu e-mail.');
                if (typeof data.resend_available_in !== 'number') {
                    startResendCountdown(60);
                }
            } catch (_err) {
                renderResendButton();
                setError('Erro de conexão ao reenviar o código.');
            }
        });
    }

    renderUI();
    startResendCountdown(state.resendSeconds);
    refreshState();
})();
