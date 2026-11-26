(() => {
    const page = document.querySelector('[data-my-subscription]');
    if (!page) return;

    const overview = page.querySelector('[data-subscription-overview]');
    const payments = page.querySelector('[data-subscription-payments]');
    const tabs = page.querySelectorAll('[data-subscription-tab]');
    const details = page.querySelector('[data-plan-details]');
    const dialogTriggers = new WeakMap();

    function showSection(scroll) {
        const hash = window.location.hash;
        const showPayments = hash === '#pagamentos' || hash === '#faturas-em-aberto';
        const currentTab = showPayments ? 'payments' : 'overview';
        overview.hidden = showPayments;
        payments.hidden = !showPayments;

        tabs.forEach(tab => {
            if (tab.dataset.subscriptionTab === currentTab) tab.setAttribute('aria-current', 'page');
            else tab.removeAttribute('aria-current');
        });
        if (hash === '#detalhes-do-plano' && details) details.open = true;

        if (hash === '#faturas-em-aberto' || hash === '#detalhes-do-plano') {
            page.querySelector(hash)?.scrollIntoView();
        } else if (scroll && (hash === '#visao-geral' || hash === '#pagamentos')) {
            page.scrollIntoView();
        }
    }

    showSection(false);
    window.addEventListener('hashchange', () => showSection(true));

    page.querySelectorAll('dialog').forEach(dialog => {
        dialog.addEventListener('close', () => {
            dialogTriggers.get(dialog)?.focus();
            dialogTriggers.delete(dialog);
        });
    });

    page.addEventListener('click', async event => {
        const opener = event.target.closest('[data-dialog-open]');
        if (opener) {
            const dialog = document.getElementById(opener.dataset.dialogOpen);
            if (!dialog || dialog.open) return;
            const previous = opener.closest('dialog');
            if (previous) {
                dialogTriggers.set(dialog, dialogTriggers.get(previous));
                dialogTriggers.delete(previous);
                previous.close();
            } else {
                dialogTriggers.set(dialog, opener);
            }
            dialog.showModal();
            return;
        }

        if (event.target.closest('[data-plan-details-link]') && details) details.open = true;
        if (event.target.closest('[data-dialog-close]') || event.target.tagName === 'DIALOG') {
            event.target.closest('dialog')?.close();
            return;
        }

        const copy = event.target.closest('[data-copy-link]');
        if (!copy) return;
        try {
            await navigator.clipboard.writeText(copy.dataset.copyLink);
            copy.textContent = 'Link copiado';
            setTimeout(() => { copy.textContent = 'Copiar link'; }, 2000);
        } catch (error) {
            window.prompt('Copie o link:', copy.dataset.copyLink);
        }
    });

    const filter = page.querySelector('[data-payment-filter]');
    if (filter) {
        const rows = page.querySelectorAll('[data-payment-status]');
        const empty = page.querySelector('[data-payment-filter-empty]');
        const updateHistory = () => {
            let visible = 0;
            rows.forEach(row => {
                row.hidden = filter.value !== 'all' && row.dataset.paymentStatus !== filter.value;
                if (!row.hidden) visible += 1;
            });
            empty.hidden = visible > 0;
        };
        filter.closest('label').hidden = false;
        filter.addEventListener('change', updateHistory);
        updateHistory();
    }

    page.querySelectorAll('[data-busy-form]').forEach(form => {
        form.addEventListener('submit', event => {
            if (event.defaultPrevented) return;
            const button = event.submitter || form.querySelector('button');
            if (button) {
                button.disabled = true;
                button.textContent = 'Abrindo…';
            }
        });
    });
})();
