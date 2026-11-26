(() => {
    const catalog = document.querySelector('[data-plan-upgrade]');
    if (!catalog) return;

    const brl = (value) => Number(value).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    let currentCycle = catalog.dataset.initialCycle || 'MONTHLY';
    let activeSlideIndex = Number(catalog.dataset.initialSlide || 0);
    let ctaPulseTimer = null;

    // 1. Economia dinamica no topo (atualiza conforme plano central + ciclo)
    const savingsEl = catalog.querySelector('[data-dynamic-savings]');
    const updateSavingsBanner = (slide) => {
        if (!savingsEl || !slide) return;
        const monthly = Number(slide.dataset.monthly || 0);
        const yearly = Number(slide.dataset.yearly || 0);
        const name = slide.dataset.planName || '';
        const save = (monthly * 12) - yearly;

        if (currentCycle === 'YEARLY' && save > 0) {
            savingsEl.innerHTML = '';
            const s1 = document.createElement('span');
            s1.innerHTML = `Economize <strong>${brl(save)}/ano</strong> no <strong></strong>`;
            s1.querySelectorAll('strong')[1].textContent = name;
            const sep = document.createElement('span');
            sep.className = 'pc-savings-sep';
            sep.setAttribute('aria-hidden', 'true');
            sep.textContent = '·';
            const bonus = document.createElement('span');
            bonus.className = 'pc-savings-bonus';
            bonus.textContent = '2 meses por nossa conta';
            savingsEl.append(s1, sep, bonus);
        } else if (save > 0) {
            savingsEl.innerHTML = '';
            const s1 = document.createElement('span');
            s1.textContent = 'No ciclo anual você ganha';
            const bonus = document.createElement('span');
            bonus.className = 'pc-savings-bonus';
            bonus.textContent = '2 meses por nossa conta';
            savingsEl.append(s1, bonus);
        } else {
            savingsEl.textContent = '';
        }
    };

    // 2. Seletor deslizante Mensal / Anual
    const cyclePicker = catalog.querySelector('[data-cycle-picker]');
    const applyCycle = (cycleCode) => {
        currentCycle = cycleCode;
        if (cyclePicker) {
            cyclePicker.dataset.cycle = cycleCode;
            cyclePicker.querySelectorAll('[data-cycle-switch]').forEach((btn) => {
                btn.setAttribute('aria-pressed', String(btn.dataset.cycleSwitch === cycleCode));
            });
        }
        const key = cycleCode.toLowerCase();
        const capKey = key.charAt(0).toUpperCase() + key.slice(1);

        catalog.querySelectorAll('[data-plan-card]').forEach((card) => {
            const box = card.querySelector('[data-plan-pricing]');
            if (!box) return;
            const priceRaw = box.dataset['price' + capKey];
            const unitRaw = box.dataset['unit' + capKey];
            if (!priceRaw) return;

            const priceVal = Number(priceRaw);
            const monthly = Number(box.dataset.priceMonthly || 0);
            const yearly = Number(box.dataset.priceYearly || 0);
            const annualSave = (monthly * 12) - yearly;

            const valueEl = box.querySelector('[data-price-value]');
            const unitEl = box.querySelector('[data-price-unit]');
            const strikeEl = box.querySelector('[data-price-strike]');
            const hintEl = box.querySelector('[data-price-hint]');

            const shownNumber = cycleCode === 'YEARLY' ? (priceVal / 12) : priceVal;
            if (valueEl) {
                valueEl.textContent = shownNumber.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                valueEl.classList.remove('pc-price-animate');
                void valueEl.offsetWidth;
                valueEl.classList.add('pc-price-animate');
            }
            if (unitEl) {
                unitEl.textContent = '/ ' + (cycleCode === 'YEARLY' ? 'mês' : (unitRaw || 'mês'));
            }
            if (strikeEl) {
                strikeEl.hidden = !(cycleCode === 'YEARLY' && monthly > 0 && annualSave > 0);
            }
            if (hintEl) {
                hintEl.innerHTML = '';
                if (cycleCode === 'YEARLY') {
                    const pill = document.createElement('span');
                    pill.className = 'pc-save-pill';
                    pill.textContent = annualSave > 0
                        ? `${brl(priceVal)}/ano · poupe ${brl(annualSave)}`
                        : `${brl(priceVal)}/ano`;
                    hintEl.appendChild(pill);
                } else if (yearly > 0 && annualSave > 0) {
                    hintEl.textContent = `Sem fidelidade · poupe ${brl(annualSave)} no anual`;
                } else {
                    hintEl.textContent = 'Sem fidelidade · cancele quando quiser';
                }
            }

            const link = card.querySelector('[data-subscribe-link]');
            if (link && link.dataset.baseHref) {
                link.href = `${link.dataset.baseHref}&cycle=${encodeURIComponent(cycleCode)}`;
            }
        });

        const activeSlide = catalog.querySelector(`[data-slide-index="${activeSlideIndex}"]`);
        updateSavingsBanner(activeSlide);
    };

    if (cyclePicker) {
        cyclePicker.querySelectorAll('[data-cycle-switch]').forEach((btn) => {
            btn.addEventListener('click', () => applyCycle(btn.dataset.cycleSwitch));
        });
    }

    // 3. Carrossel Flutuante estilo App Nativo (sem setas + halo ambiente + interpolação 3D)
    const stage = catalog.querySelector('[data-floating-stage]');
    const track = catalog.querySelector('[data-floating-track]');
    const slides = track ? Array.from(track.querySelectorAll('[data-slide-index]')) : [];
    const dots = Array.from(catalog.querySelectorAll('.pc-native-dot'));
    const navPills = Array.from(catalog.querySelectorAll('.pc-nav-pill'));

    const scrollToSlide = (idx) => {
        if (!track || !slides[idx]) return;
        const slide = slides[idx];
        const target = slide.offsetLeft - (track.clientWidth - slide.offsetWidth) / 2;
        track.scrollTo({ left: target, behavior: reducedMotion ? 'auto' : 'smooth' });
    };

    const startCenterCtaMorph = () => {
        clearInterval(ctaPulseTimer);
        if (reducedMotion) return;
        let showAlt = false;
        ctaPulseTimer = setInterval(() => {
            const centerBtn = catalog.querySelector('.pc-floating-slide.is-center [data-cta-morph]');
            catalog.querySelectorAll('[data-cta-morph]').forEach((btn) => {
                if (btn !== centerBtn) btn.classList.remove('is-alt');
            });
            if (!centerBtn || centerBtn.matches(':hover')) return;
            showAlt = !showAlt;
            centerBtn.classList.toggle('is-alt', showAlt);
        }, 2800);
    };

    if (track && slides.length) {
        const updateTransforms = () => {
            const trackCenter = track.scrollLeft + track.clientWidth / 2;
            let closestIdx = 0;
            let closestDist = Infinity;

            slides.forEach((slide, i) => {
                const slideCenter = slide.offsetLeft + slide.offsetWidth / 2;
                const delta = (slideCenter - trackCenter) / Math.max(slide.offsetWidth, 1);
                const absDelta = Math.abs(delta);
                if (absDelta < closestDist) {
                    closestDist = absDelta;
                    closestIdx = i;
                }
                const t = Math.min(absDelta, 1.35);
                const scale = reducedMotion ? 1 : (1.045 - t * 0.145);
                const liftY = reducedMotion ? 0 : (-10 + t * 14);
                const rotY = reducedMotion ? 0 : Math.max(-8, Math.min(8, -delta * 6.5));
                const opacity = Math.max(0.5, 1 - t * 0.35);

                slide.style.transform = `translateY(${liftY.toFixed(1)}px) scale(${scale.toFixed(3)}) rotateY(${rotY.toFixed(2)}deg)`;
                slide.style.opacity = opacity.toFixed(3);
                slide.style.zIndex = String(10 - Math.round(t * 5));
            });

            activeSlideIndex = closestIdx;
            slides.forEach((s, i) => s.classList.toggle('is-center', i === closestIdx));
            dots.forEach((d, i) => d.setAttribute('aria-current', String(i === closestIdx)));
            navPills.forEach((p, i) => p.setAttribute('aria-current', String(i === closestIdx)));
            if (stage && slides[closestIdx]) {
                stage.dataset.activeTier = slides[closestIdx].dataset.tier || 'profissional';
            }
            updateSavingsBanner(slides[closestIdx]);
        };

        const initSlide = slides[Math.min(activeSlideIndex, slides.length - 1)];
        if (initSlide) {
            track.scrollLeft = initSlide.offsetLeft - (track.clientWidth - initSlide.offsetWidth) / 2;
        }
        updateTransforms();
        startCenterCtaMorph();

        let rafId = null;
        track.addEventListener('scroll', () => {
            if (rafId) return;
            rafId = requestAnimationFrame(() => {
                rafId = null;
                updateTransforms();
            });
        }, { passive: true });

        window.addEventListener('resize', () => updateTransforms());

        slides.forEach((slide, idx) => {
            slide.addEventListener('click', (event) => {
                if (!slide.classList.contains('is-center')) {
                    event.preventDefault();
                    event.stopPropagation();
                    scrollToSlide(idx);
                }
            });
        });

        let isDown = false;
        let startX = 0;
        let startLeft = 0;
        track.addEventListener('mousedown', (event) => {
            if (event.target.closest('a, button')) return;
            isDown = true;
            track.classList.add('is-dragging');
            startX = event.pageX;
            startLeft = track.scrollLeft;
        });
        window.addEventListener('mousemove', (event) => {
            if (!isDown) return;
            track.scrollLeft = startLeft - (event.pageX - startX) * 1.15;
        });
        window.addEventListener('mouseup', () => {
            if (!isDown) return;
            isDown = false;
            track.classList.remove('is-dragging');
            scrollToSlide(activeSlideIndex);
        });

        track.addEventListener('keydown', (event) => {
            if (event.key === 'ArrowLeft') {
                event.preventDefault();
                scrollToSlide(Math.max(0, activeSlideIndex - 1));
            } else if (event.key === 'ArrowRight') {
                event.preventDefault();
                scrollToSlide(Math.min(slides.length - 1, activeSlideIndex + 1));
            }
        });
    }

    // 4. Comparativo de planos (recolher/expandir, categorias, Só diferenças e mobile VS 2 a 2)
    const comparison = catalog.querySelector('[data-plan-comparison]');
    if (comparison) {
        const title = comparison.querySelector('h2');
        const description = comparison.querySelector('[data-comparison-highlight]');
        const planCells = comparison.querySelectorAll('[data-comparison-plan]');
        const filterDesktop = comparison.querySelector('[data-comparison-filter]');
        const filterMobile = comparison.querySelector('[data-comparison-filter-mobile]');
        const catButtons = comparison.querySelectorAll('[data-comp-cat]');
        const catSelect = comparison.querySelector('[data-comp-cat-select]');
        const feedback = comparison.querySelector('[data-comparison-feedback]');
        const groups = comparison.querySelectorAll('[data-comparison-group]');
        const limitsBody = comparison.querySelector('[data-comparison-limits]');
        const toggleBtn = comparison.querySelector('[data-comparison-toggle]');
        const toggleLabel = comparison.querySelector('[data-comparison-toggle-label]');
        const compBody = comparison.querySelector('[data-comparison-body]');
        const mobSelectA = comparison.querySelector('[data-mobile-plan="a"]');
        const mobSelectB = comparison.querySelector('[data-mobile-plan="b"]');
        const header = document.querySelector('[data-hd]');

        if (header) {
            const setHeaderOffset = () => catalog.style.setProperty('--pc-header-offset', header.offsetHeight + 'px');
            setHeaderOffset();
            if (typeof ResizeObserver !== 'undefined') new ResizeObserver(setHeaderOffset).observe(header);
        }

        let onlyDiff = false;
        let currentCat = 'all';

        const applyComparisonFilters = () => {
            let hasVisible = false;
            if (limitsBody) limitsBody.hidden = currentCat !== 'all';
            groups.forEach((group) => {
                const matchCat = currentCat === 'all' || group.dataset.compGroupId === currentCat;
                const rows = group.querySelectorAll('[data-comparison-row]');
                rows.forEach((row) => {
                    row.hidden = !matchCat || (onlyDiff && row.hasAttribute('data-comparison-common'));
                });
                group.hidden = !matchCat || Array.from(rows).every((row) => row.hidden);
                if (!group.hidden) hasVisible = true;
            });
            if (feedback) feedback.hidden = !(onlyDiff && !hasVisible);
            if (filterDesktop) filterDesktop.setAttribute('aria-pressed', String(onlyDiff));
            if (filterMobile) filterMobile.setAttribute('aria-pressed', String(onlyDiff));
            catButtons.forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.compCat === currentCat)));
            if (catSelect) catSelect.value = currentCat;
        };

        const applyMobileVsColumns = () => {
            if (!mobSelectA || !mobSelectB) return;
            const visibleIds = new Set([mobSelectA.value, mobSelectB.value]);
            planCells.forEach((cell) => {
                cell.classList.toggle('pc-col-mob-hidden', !visibleIds.has(cell.dataset.comparisonPlan));
            });
        };

        if (toggleBtn && compBody) {
            toggleBtn.addEventListener('click', () => {
                const expanded = toggleBtn.getAttribute('aria-expanded') === 'true';
                toggleBtn.setAttribute('aria-expanded', String(!expanded));
                compBody.hidden = expanded;
                if (toggleLabel) toggleLabel.textContent = expanded ? 'Ver tudo' : 'Ocultar';
            });
        }

        [filterDesktop, filterMobile].forEach((btn) => {
            if (!btn) return;
            btn.addEventListener('click', () => {
                onlyDiff = !onlyDiff;
                applyComparisonFilters();
            });
        });

        catButtons.forEach((btn) => {
            btn.addEventListener('click', () => {
                currentCat = btn.dataset.compCat || 'all';
                applyComparisonFilters();
            });
        });

        if (catSelect) {
            catSelect.addEventListener('change', () => {
                currentCat = catSelect.value;
                applyComparisonFilters();
            });
        }

        [mobSelectA, mobSelectB].forEach((sel) => {
            if (sel) sel.addEventListener('change', applyMobileVsColumns);
        });
        applyMobileVsColumns();

        catalog.addEventListener('click', (event) => {
            const link = event.target.closest('[data-comparison-focus]');
            if (!link || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
            event.preventDefault();
            const planId = link.dataset.comparisonFocus;
            if (compBody && compBody.hidden && toggleBtn) {
                compBody.hidden = false;
                toggleBtn.setAttribute('aria-expanded', 'true');
                if (toggleLabel) toggleLabel.textContent = 'Ocultar';
            }
            for (const cell of planCells) {
                const highlighted = cell.dataset.comparisonPlan === planId;
                cell.classList.toggle('pc-highlighted', highlighted);
                if (highlighted && cell.scope === 'col' && description) {
                    description.textContent = 'Plano em destaque: ' + cell.textContent.trim() + '.';
                }
            }
            if (mobSelectA) {
                mobSelectA.value = planId;
                applyMobileVsColumns();
            }
            title?.focus({ preventScroll: true });
            comparison.scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'start' });
        });
    }

    // 5. Navegacao por pilulas/dots, modais e atalho "Ver plano compativel"
    catalog.addEventListener('click', (event) => {
        const jumpBtn = event.target.closest('[data-jump-slide]');
        if (jumpBtn) {
            scrollToSlide(Number(jumpBtn.dataset.jumpSlide));
            return;
        }

        const goPlanBtn = event.target.closest('[data-go-plan]');
        if (goPlanBtn) {
            const targetId = goPlanBtn.dataset.goPlan;
            goPlanBtn.closest('dialog')?.close();
            const idx = slides.findIndex((s) => s.dataset.planId === targetId);
            if (idx >= 0) scrollToSlide(idx);
            return;
        }

        const opener = event.target.closest('[data-dialog-open]');
        if (opener) {
            document.getElementById(opener.dataset.dialogOpen)?.showModal();
            return;
        }
        if (event.target.closest('[data-dialog-close]') || event.target.tagName === 'DIALOG') {
            event.target.closest('dialog')?.close();
        }
    });

    // 6. Dialog de mudanca de plano (self-service Asaas)
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
