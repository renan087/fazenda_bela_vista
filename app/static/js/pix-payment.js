(() => {
    const panel = document.querySelector('[data-pix-panel]');
    if (!panel) return;

    const copyBtn = panel.querySelector('[data-pix-copy-btn]');
    const codeField = panel.querySelector('[data-pix-code]');
    const feedback = panel.querySelector('[data-pix-feedback]');
    const badge = panel.querySelector('[data-pix-status-badge]');
    const countdown = panel.querySelector('[data-pix-countdown]');
    const expiredBox = panel.querySelector('[data-pix-expired-box]');
    const statusUrl = panel.dataset.pixStatusUrl || '';
    const expiresAtRaw = panel.dataset.pixExpiresAt || '';

    const setFeedback = (text, tone) => {
        if (!feedback) return;
        feedback.textContent = text;
        feedback.className = `px-live-feedback${tone ? ` is-${tone}` : ''}`;
    };

    if (copyBtn && codeField) {
        copyBtn.addEventListener('click', async () => {
            const value = (codeField.value || '').trim();
            if (!value) return;
            const original = copyBtn.textContent;
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
                copyBtn.textContent = 'Código Pix copiado!';
                setFeedback('Código Pix copiado. Cole no aplicativo do seu banco para pagar.', 'ok');
                setTimeout(() => { copyBtn.textContent = original; }, 2600);
            } else {
                setFeedback('Selecione o código acima e copie manualmente.', 'warn');
            }
        });
    }

    const markExpired = () => {
        if (badge) {
            badge.className = 'ms-badge ms-badge-muted';
            badge.textContent = 'Pix expirado';
        }
        if (countdown) countdown.textContent = 'Expirado';
        if (expiredBox) expiredBox.hidden = false;
    };

    let countdownTimer = null;
    if (countdown && expiresAtRaw) {
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
                countdown.textContent = hours > 0
                    ? `Expira em ${pad(hours)}h ${pad(mins)}min ${pad(secs)}s`
                    : `Expira em ${pad(mins)}min ${pad(secs)}s`;
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
                    if (badge) {
                        badge.className = 'ms-badge ms-badge-ok';
                        badge.textContent = 'Pagamento confirmado';
                    }
                    setFeedback('Pagamento Pix confirmado! Atualizando sua assinatura…', 'ok');
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
