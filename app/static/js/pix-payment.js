(() => {
    const panel = document.querySelector('[data-pix-panel]');
    if (!panel) return;

    const modal = panel.querySelector('[data-pix-modal]');
    const openBtns = panel.querySelectorAll('[data-pix-open-modal]');
    const closeBtns = panel.querySelectorAll('[data-pix-close-modal]');
    const copyBtns = panel.querySelectorAll('[data-pix-copy-btn]');
    const codeField = panel.querySelector('[data-pix-code]');
    const feedback = panel.querySelector('[data-pix-feedback]');
    const badges = panel.querySelectorAll('[data-pix-status-badge]');
    const countdowns = panel.querySelectorAll('[data-pix-countdown]');
    const expiredBox = panel.querySelector('[data-pix-expired-box]');
    const statusUrl = panel.dataset.pixStatusUrl || '';
    const expiresAtRaw = panel.dataset.pixExpiresAt || '';

    const openModal = () => {
        if (!modal) return;
        if (typeof modal.showModal === 'function') {
            if (!modal.open) modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    };

    const closeModal = () => {
        if (!modal) return;
        if (typeof modal.close === 'function' && modal.open) {
            modal.close();
        } else {
            modal.removeAttribute('open');
        }
    };

    openBtns.forEach((btn) => btn.addEventListener('click', openModal));
    closeBtns.forEach((btn) => btn.addEventListener('click', closeModal));

    if (modal) {
        modal.addEventListener('click', (event) => {
            if (event.target === modal) closeModal();
        });
        // Abre automaticamente ao chegar da geração do Pix (#pix-pagamento) ou ao acessar com Pix pendente recém-criado.
        if (modal.dataset.pixAutoOpen === 'true' && window.location.hash === '#pix-pagamento') {
            openModal();
        }
    }

    const setFeedback = (text, tone) => {
        if (!feedback) return;
        feedback.textContent = text;
        feedback.className = `px-live-feedback${tone ? ` is-${tone}` : ''}`;
    };

    const copyPixCode = async (triggerBtn) => {
        if (!codeField) return;
        const value = (codeField.value || '').trim();
        if (!value) return;
        const labelEl = triggerBtn.querySelector('span') || triggerBtn;
        const original = labelEl.textContent;
        let copied = false;
        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                await navigator.clipboard.writeText(value);
                copied = true;
            }
        } catch (_err) {
            copied = false;
        }
        if (!copied) {
            codeField.focus();
            codeField.select();
            try {
                copied = document.execCommand('copy');
            } catch (_err) {
                copied = false;
            }
        }
        if (copied) {
            labelEl.textContent = 'Código copiado!';
            setFeedback('Código Pix copiado. Cole no app do seu banco.', 'ok');
            setTimeout(() => { labelEl.textContent = original; }, 2400);
        } else {
            setFeedback('Selecione o código acima e copie manualmente.', 'warn');
        }
    };

    copyBtns.forEach((btn) => {
        btn.addEventListener('click', () => copyPixCode(btn));
    });

    const markExpired = () => {
        badges.forEach((el) => {
            el.className = 'ms-badge ms-badge-muted';
            el.textContent = 'Pix expirado';
        });
        countdowns.forEach((el) => { el.textContent = 'Expirado'; });
        if (expiredBox) expiredBox.hidden = false;
    };

    let countdownTimer = null;
    if (countdowns.length && expiresAtRaw) {
        const expiresMs = Date.parse(expiresAtRaw);
        if (!Number.isNaN(expiresMs)) {
            const renderClock = () => {
                const diff = Math.floor((expiresMs - Date.now()) / 1000);
                if (diff <= 0) {
                    markExpired();
                    if (countdownTimer) clearInterval(countdownTimer);
                    return;
                }
                const hours = Math.floor(diff / 3600);
                const mins = Math.floor((diff % 3600) / 60);
                const secs = diff % 60;
                const pad = (n) => String(n).padStart(2, '0');
                const text = hours > 0
                    ? `Expira em ${pad(hours)}h ${pad(mins)}min ${pad(secs)}s`
                    : `Expira em ${pad(mins)}min ${pad(secs)}s`;
                countdowns.forEach((el) => { el.textContent = text; });
            };
            renderClock();
            countdownTimer = setInterval(renderClock, 1000);
        }
    }

    if (statusUrl) {
        let stopped = false;
        let inFlight = false;
        const poll = async () => {
            if (stopped || inFlight || document.hidden) return;
            inFlight = true;
            try {
                const response = await fetch(statusUrl, {
                    headers: { Accept: 'application/json' },
                    credentials: 'same-origin',
                });
                if (!response.ok) return;
                const data = await response.json();
                if (data.confirmed) {
                    stopped = true;
                    badges.forEach((el) => {
                        el.className = 'ms-badge ms-badge-ok';
                        el.textContent = 'Pagamento confirmado';
                    });
                    setFeedback('Pagamento confirmado! Atualizando sua assinatura…', 'ok');
                    setTimeout(() => window.location.reload(), 900);
                } else if (data.expired || data.status === 'EXPIRED' || data.status === 'CANCELLED') {
                    stopped = true;
                    markExpired();
                }
            } catch (_err) {
                // Erro temporário de rede: tenta novamente no próximo ciclo.
            } finally {
                inFlight = false;
            }
        };
        const pollTimer = setInterval(() => {
            if (stopped) { clearInterval(pollTimer); return; }
            poll();
        }, 5000);
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden && !stopped) poll();
        });
    }
})();
