(() => {
    const catalog = document.querySelector('[data-plan-upgrade]');
    if (!catalog) return;

    const brl = (value) => Number(value).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

    // Seletor interativo de periodicidade (Mensal / Anual)
    const cyclePicker = catalog.querySelector('[data-cycle-picker]');
    if (cyclePicker) {
        const buttons = cyclePicker.querySelectorAll('[data-cycle-switch]');
        const cards = catalog.querySelectorAll('[data-plan-card]');

        const applyCycle = (cycleCode) => {
            const key = cycleCode.toLowerCase();
            buttons.forEach((btn) => {
                btn.setAttribute('aria-pressed', String(btn.dataset.cycleSwitch === cycleCode));
            });
            cards.forEach((card) => {
                const box = card.querySelector('[data-plan-pricing]');
                if (!box) return;
                const priceRaw = box.dataset['price' + key.charAt(0).toUpperCase() + key.slice(1)];
                const unitRaw = box.dataset['unit' + key.charAt(0).toUpperCase() + key.slice(1)];
                if (!priceRaw) return;
                const priceVal = Number(priceRaw);
                const valueEl = box.querySelector('[data-price-value]');
                const unitEl = box.querySelector('[data-price-unit]');
                const hintEl = box.querySelector('[data-price-hint]');
                if (valueEl) valueEl.textContent = brl(priceVal);
                if (unitEl && unitRaw) unitEl.textContent = '/ ' + unitRaw;
                if (hintEl) {
                    const monthly = Number(box.dataset.priceMonthly || 0);
                    const yearly = Number(box.dataset.priceYearly || 0);
                    if (cycleCode === 'YEARLY') {
                        hintEl.textContent = `Equivale a ${brl(priceVal / 12)}/mês`;
                    } else if (cycleCode === 'MONTHLY' && yearly > 0 && monthly > 0) {
                        const pct = Math.round(((monthly * 12 - yearly) / (monthly * 12)) * 100);
                        hintEl.textContent = pct > 0
                            ? `Ou ${brl(yearly)}/ano com ${pct}% de desconto`
                            : `Ou ${brl(yearly)}/ano`;
                    } else {
                        hintEl.textContent = '';
                    }
                }
                const link = card.querySelector('[data-subscribe-link]');
                if (link && link.dataset.baseHref) {
                    link.href = `${link.dataset.baseHref}&cycle=${encodeURIComponent(cycleCode)}`;
                }
            });
        };

        buttons.forEach((btn) => {
            btn.addEventListener('click', () => applyCycle(btn.dataset.cycleSwitch));
        });
    }

    // Comparativo de planos
    const comparison = catalog.querySelector('[data-plan-comparison]');
    if (comparison) {
        const title = comparison.querySelector('h2');
        const description = comparison.querySelector('[data-comparison-highlight]');
        const planCells = comparison.querySelectorAll('[data-comparison-plan]');
        const filter = comparison.querySelector('[data-comparison-filter]');
        const feedback = comparison.querySelector('[data-comparison-feedback]');
        const groups = comparison.querySelectorAll('[data-comparison-group]');
        const header = document.querySelector('[data-hd]');
        if (header) {
            const setHeaderOffset = () => catalog.style.setProperty('--pc-header-offset', header.offsetHeight + 'px');
            setHeaderOffset();
            if (typeof ResizeObserver !== 'undefined') new ResizeObserver(setHeaderOffset).observe(header);
        }

        catalog.addEventListener('click', (event) => {
            const link = event.target.closest('[data-comparison-focus]');
            if (!link || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
            event.preventDefault();
            const planId = link.dataset.comparisonFocus;
            for (const cell of planCells) {
                const highlighted = cell.dataset.comparisonPlan === planId;
                cell.classList.toggle('pc-highlighted', highlighted);
                if (highlighted && cell.scope === 'col' && description) {
                    description.textContent = 'Plano em destaque: ' + cell.textContent.trim() + '.';
                }
            }
            title?.focus({ preventScroll: true });
            const reduceMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;
            comparison.scrollIntoView({ behavior: reduceMotion ? 'auto' : 'smooth', block: 'start' });
        });

        if (filter) {
            filter.hidden = false;
            filter.addEventListener('click', () => {
                const filtered = filter.getAttribute('aria-pressed') !== 'true';
                filter.setAttribute('aria-pressed', String(filtered));
                let hasDifferences = false;
                for (const group of groups) {
                    const rows = group.querySelectorAll('[data-comparison-row]');
                    for (const row of rows) row.hidden = filtered && row.hasAttribute('data-comparison-common');
                    group.hidden = Array.from(rows).every((row) => row.hidden);
                    hasDifferences ||= !group.hidden;
                }
                if (feedback) feedback.hidden = !filtered || hasDifferences;
            });
        }
    }

    // Modais
    catalog.addEventListener('click', (event) => {
        const opener = event.target.closest('[data-dialog-open]');
        if (opener) {
            document.getElementById(opener.dataset.dialogOpen)?.showModal();
            return;
        }
        if (event.target.closest('[data-dialog-close]') || event.target.tagName === 'DIALOG') {
            event.target.closest('dialog')?.close();
        }
    });

    // Dialog de mudanca de plano (self-service)
    for (const form of catalog.querySelectorAll('[data-plan-change]')) {
        const sync = () => {
            const option = form.querySelector('[data-cycle-option]:checked');
            if (!option) return;
            const price = Number(option.dataset.price);
            const label = brl(price) + ' / ' + option.dataset.unit;
            const newPriceEl = form.querySelector('[data-new-price]');
            if (newPriceEl) newPriceEl.textContent = label;
            form.querySelectorAll('[data-next-price]').forEach((element) => { element.textContent = label; });
            const diff = price / Number(option.dataset.months) - Number(form.dataset.currentMonthly);
            const chip = form.querySelector('[data-diff]');
            if (chip) {
                chip.textContent = Math.abs(diff) < 0.005 ? 'Mesmo valor mensal' : (diff > 0 ? '+ ' : '− ') + brl(Math.abs(diff)) + ' por mês';
                chip.classList.toggle('bg-amber-50', diff > 0.005);
                chip.classList.toggle('text-amber-800', diff > 0.005);
                chip.classList.toggle('bg-emerald-50', diff <= 0.005);
                chip.classList.toggle('text-emerald-800', diff <= 0.005);
            }
        };
        form.addEventListener('change', (event) => { if (event.target.matches('[data-cycle-option]')) sync(); });
        sync();
    }

    for (const form of catalog.querySelectorAll('[data-busy-form]')) {
        form.addEventListener('submit', (event) => {
            const submitter = event.submitter || form.querySelector('button:not([type="button"])');
            if (submitter) {
                submitter.disabled = true;
                submitter.textContent = 'Enviando…';
            }
        });
    }
})();
