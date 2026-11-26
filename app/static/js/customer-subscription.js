(() => {
    const page = document.querySelector('[data-my-subscription]');
    if (!page) return;

    const overview = page.querySelector('[data-subscription-overview]');
    const payments = page.querySelector('[data-subscription-payments]');
    const tabs = page.querySelectorAll('[data-subscription-tab]');
    const details = page.querySelector('[data-plan-details]');
    const toast = page.querySelector('[data-ms-toast]');
    const dialogTriggers = new WeakMap();
    let toastTimer = null;

    function showToast(message) {
        if (!toast) return;
        clearTimeout(toastTimer);
        toast.textContent = message;
        toast.hidden = false;
        toastTimer = setTimeout(() => { toast.hidden = true; }, 3500);
    }

    function showSection(scroll) {
        const hash = window.location.hash;
        const showPayments = hash === '#pagamentos' || hash === '#faturas-em-aberto';
        const currentTab = showPayments ? 'payments' : 'overview';
        overview.hidden = showPayments;
        payments.hidden = !showPayments;

        tabs.forEach((tab) => {
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

    page.querySelectorAll('dialog').forEach((dialog) => {
        dialog.addEventListener('close', () => {
            dialogTriggers.get(dialog)?.focus();
            dialogTriggers.delete(dialog);
        });
    });

    function openInvoiceDialog(trigger) {
        const dialog = document.getElementById('invoice-detail-dialog');
        if (!dialog) return;
        dialog.querySelector('[data-inv-subtitle]').textContent = trigger.dataset.invoiceDate || '';
        dialog.querySelector('[data-inv-amount]').textContent = trigger.dataset.invoiceAmount || '—';
        dialog.querySelector('[data-inv-date]').textContent = trigger.dataset.invoiceDate || '—';
        dialog.querySelector('[data-inv-method]').textContent = trigger.dataset.invoiceMethod || '—';
        dialog.querySelector('[data-inv-provider]').textContent = trigger.dataset.invoiceProvider || '—';
        const badge = dialog.querySelector('[data-inv-badge]');
        badge.textContent = trigger.dataset.invoiceStatus || '—';
        badge.className = 'ms-badge ms-badge-' + (trigger.dataset.invoiceTone || 'muted');
        const link = dialog.querySelector('[data-inv-url]');
        if (trigger.dataset.invoiceUrl) {
            link.href = trigger.dataset.invoiceUrl;
            link.hidden = false;
        } else {
            link.hidden = true;
        }
        dialogTriggers.set(dialog, trigger);
        dialog.showModal();
    }

    page.addEventListener('click', async (event) => {
        const invoiceBtn = event.target.closest('[data-invoice-detail]');
        if (invoiceBtn) {
            openInvoiceDialog(invoiceBtn);
            return;
        }

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
            const original = copy.textContent;
            copy.textContent = 'Link copiado';
            showToast('Link de pagamento copiado!');
            setTimeout(() => { copy.textContent = original; }, 2200);
        } catch (_error) {
            window.prompt('Copie o link:', copy.dataset.copyLink);
        }
    });

    // Filtros de historico: chips + busca + select sincronizado
    const historySection = page.querySelector('[data-payment-history]');
    if (historySection) {
        const select = historySection.querySelector('[data-payment-filter]');
        const chips = historySection.querySelectorAll('[data-payment-chips] [data-filter-value]');
        const search = historySection.querySelector('[data-payment-search]');
        const rows = historySection.querySelectorAll('[data-payment-status]');
        const empty = historySection.querySelector('[data-payment-filter-empty]');
        let currentStatus = 'all';

        const updateHistory = () => {
            const term = (search?.value || '').trim().toLowerCase();
            let visible = 0;
            rows.forEach((row) => {
                const matchStatus = currentStatus === 'all' || row.dataset.paymentStatus === currentStatus;
                const matchSearch = !term || (row.dataset.paymentText || '').includes(term);
                row.hidden = !(matchStatus && matchSearch);
                if (!row.hidden) visible += 1;
            });
            if (empty) empty.hidden = visible > 0;
        };

        chips.forEach((chip) => {
            chip.addEventListener('click', () => {
                currentStatus = chip.dataset.filterValue || 'all';
                chips.forEach((c) => c.setAttribute('aria-pressed', String(c === chip)));
                if (select) select.value = currentStatus;
                updateHistory();
            });
        });

        if (select) {
            select.addEventListener('change', () => {
                currentStatus = select.value;
                chips.forEach((c) => c.setAttribute('aria-pressed', String(c.dataset.filterValue === currentStatus)));
                updateHistory();
            });
        }

        if (search) {
            search.addEventListener('input', updateHistory);
        }

        updateHistory();
    }

    page.querySelectorAll('[data-busy-form]').forEach((form) => {
        form.addEventListener('submit', (event) => {
            if (event.defaultPrevented) return;
            const button = event.submitter || form.querySelector('button');
            if (button) {
                button.disabled = true;
                button.textContent = 'Abrindo…';
            }
        });
    });
})();
