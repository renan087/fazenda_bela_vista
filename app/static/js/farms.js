/* Módulo Fazendas: filtros, modais de mapa/plano/ações e wizard de 3 etapas. */
(() => {
    const fmtBr = (n, dec = 2) =>
        Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });

    const maskCCIR = (val) => {
        const d = String(val || '').replace(/\D/g, '').slice(0, 13);
        let out = d.slice(0, 3);
        if (d.length > 3) out += '.' + d.slice(3, 6);
        if (d.length > 6) out += '.' + d.slice(6, 9);
        if (d.length > 9) out += '.' + d.slice(9, 12);
        if (d.length > 12) out += '-' + d.slice(12, 13);
        return out;
    };

    const maskCIB = (val) => {
        const d = String(val || '').replace(/[^a-zA-Z0-9]/g, '').toUpperCase().slice(0, 8);
        let out = d.slice(0, 1);
        if (d.length > 1) out += '.' + d.slice(1, 4);
        if (d.length > 4) out += '.' + d.slice(4, 7);
        if (d.length > 7) out += '-' + d.slice(7, 8);
        return out;
    };

    const parseJsonScript = (id, fallback) => {
        try {
            const el = document.getElementById(id);
            if (el && el.textContent.trim()) return JSON.parse(el.textContent);
        } catch (e) { /* ignore */ }
        return fallback;
    };

    const openDialog = (dlg) => {
        if (!dlg) return;
        if (typeof dlg.showModal === 'function') dlg.showModal();
        else dlg.setAttribute('open', '');
    };
    const closeDialog = (dlg) => {
        if (!dlg) return;
        if (typeof dlg.close === 'function') dlg.close();
        else dlg.removeAttribute('open');
    };

    /* =========================================================================
       1. PÁGINA PRINCIPAL: FILTROS E BUSCA
       ========================================================================= */
    const listPage = document.querySelector('[data-fm-list-page]');
    if (listPage) {
        const rows = [...listPage.querySelectorAll('[data-fm-row]')];
        const chips = [...listPage.querySelectorAll('[data-fm-filter]')];
        const searchInput = listPage.querySelector('[data-fm-search]');
        const emptyEl = listPage.querySelector('[data-fm-empty-search]');
        const attentionBtn = listPage.querySelector('[data-fm-jump-attention]');
        const resetBtn = listPage.querySelector('[data-fm-reset-filters]');
        let activeFilter = 'todas';

        const applyListFilters = () => {
            const q = (searchInput?.value || '').trim().toLowerCase();
            let visible = 0;
            rows.forEach((row) => {
                const hasGeo = row.dataset.fmGeo === '1';
                const attention = row.dataset.fmAttention === '1';
                const hay = (row.dataset.fmSearchText || '').toLowerCase();
                let ok = true;
                if (activeFilter === 'mapa' && !hasGeo) ok = false;
                if (activeFilter === 'sem_mapa' && hasGeo) ok = false;
                if (activeFilter === 'atencao' && !attention) ok = false;
                if (q && !hay.includes(q)) ok = false;
                row.hidden = !ok;
                if (ok) visible += 1;
            });
            chips.forEach((c) => c.setAttribute('aria-pressed', String(c.dataset.fmFilter === activeFilter)));
            if (emptyEl) emptyEl.hidden = !(rows.length > 0 && visible === 0);
        };

        chips.forEach((c) => c.addEventListener('click', () => {
            activeFilter = c.dataset.fmFilter || 'todas';
            applyListFilters();
        }));
        searchInput?.addEventListener('input', applyListFilters);
        attentionBtn?.addEventListener('click', () => {
            activeFilter = 'atencao';
            applyListFilters();
            listPage.querySelector('[data-fm-list]')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        });
        resetBtn?.addEventListener('click', () => {
            activeFilter = 'todas';
            if (searchInput) searchInput.value = '';
            applyListFilters();
        });
    }

    /* =========================================================================
       2. MODAIS COMPARTILHADOS: PLANO, AÇÕES SENSÍVEIS E MAPA AMPLIADO
       ========================================================================= */
    const planModal = document.querySelector('[data-fm-plan-modal]');
    document.querySelectorAll('[data-fm-plan-open]').forEach((b) => b.addEventListener('click', () => openDialog(planModal)));
    planModal?.querySelectorAll('[data-fm-dialog-close]').forEach((b) => b.addEventListener('click', () => closeDialog(planModal)));

    const actionDialog = document.querySelector('[data-fm-action-dialog]');
    if (actionDialog) {
        const form = actionDialog.querySelector('[data-fm-action-form]');
        const title = actionDialog.querySelector('[data-fm-action-title]');
        const farmName = actionDialog.querySelector('[data-fm-action-farm]');
        const description = actionDialog.querySelector('[data-fm-action-description]');
        const confirm = actionDialog.querySelector('[data-fm-action-submit]');
        const cancel = actionDialog.querySelector('[data-fm-action-cancel]');
        const redirect = actionDialog.querySelector('[data-fm-action-redirect]');
        const actionValue = actionDialog.querySelector('[data-fm-action-value]');
        const icons = actionDialog.querySelectorAll('[data-fm-action-icon]');
        const actions = {
            pause: {
                title: 'Pausar operações?',
                description: 'O histórico de setores, safras e financeiro fica disponível para consulta. Novos lançamentos ficam bloqueados até a reativação.',
                label: 'Pausar operações',
            },
            resume: {
                title: 'Reativar operações?',
                description: 'A fazenda voltará a aceitar novos lançamentos se houver espaço no plano atual.',
                label: 'Reativar operações',
            },
            delete: {
                title: 'Excluir fazenda?',
                description: 'A fazenda será excluída permanentemente. Esta ação não pode ser desfeita.',
                label: 'Excluir fazenda',
            },
        };
        let openMenu = null;

        const closeMenu = () => {
            if (!openMenu) return;
            const { trigger, panel } = openMenu;
            openMenu = null;
            trigger.setAttribute('aria-expanded', 'false');
            panel.hidden = true;
        };

        const showConfirmation = (action, farm) => {
            const settings = actions[action];
            const deleting = action === 'delete';
            const linkedPlots = Number(farm.farmPlots);
            const blocked = deleting && linkedPlots > 0;

            title.textContent = settings.title;
            farmName.textContent = farm.farmName;
            description.textContent = blocked
                ? (linkedPlots === 1
                    ? 'Esta fazenda tem 1 setor vinculado. Remova ou desvincule esse setor antes de excluir a fazenda.'
                    : `Esta fazenda tem ${linkedPlots} setores vinculados. Remova ou desvincule esses setores antes de excluir a fazenda.`)
                : settings.description;
            redirect.value = farm.redirectTo;
            redirect.disabled = deleting;
            actionValue.value = action;
            actionValue.disabled = deleting;
            form.action = `/fazendas/${farm.farmId}/${deleting ? 'excluir' : 'regularizacao'}`;
            actionDialog.dataset.fmAction = action;
            icons.forEach((icon) => { icon.hidden = icon.dataset.fmActionIcon !== action; });
            confirm.textContent = settings.label;
            confirm.className = `sf-btn ${deleting ? 'sf-btn-danger' : 'sf-btn-primary'}`;
            confirm.hidden = blocked;
            confirm.disabled = blocked;
            cancel.textContent = blocked ? 'Entendi' : 'Cancelar';
            openDialog(actionDialog);
            cancel.focus();
        };

        document.querySelectorAll('[data-fm-action-menu]').forEach((wrapper) => {
            const trigger = wrapper.querySelector('[data-fm-menu-trigger]');
            const panel = wrapper.querySelector('.nx-menu');
            const items = [...panel.querySelectorAll('[data-fm-confirm]')];

            const showMenu = (item) => {
                closeMenu();
                openMenu = { trigger, panel };
                trigger.setAttribute('aria-expanded', 'true');
                panel.hidden = false;
                panel.style.removeProperty('--fm-menu-shift');
                const triggerRect = trigger.getBoundingClientRect();
                const spaceBelow = window.innerHeight - triggerRect.bottom;
                panel.classList.toggle('is-up', spaceBelow < panel.offsetHeight + 14 && triggerRect.top > spaceBelow);
                const left = panel.getBoundingClientRect().left;
                if (left < 8) panel.style.setProperty('--fm-menu-shift', `${8 - left}px`);
                item.focus();
            };

            trigger.addEventListener('click', () => {
                if (openMenu?.trigger === trigger) closeMenu();
                else showMenu(items[0]);
            });

            items.forEach((item) => item.addEventListener('click', () => {
                closeMenu();
                trigger.focus();
                showConfirmation(item.dataset.fmConfirm, wrapper.dataset);
            }));

            wrapper.addEventListener('focusout', (event) => {
                if (openMenu?.trigger === trigger && !wrapper.contains(event.relatedTarget)) closeMenu();
            });

            wrapper.addEventListener('keydown', (event) => {
                if (openMenu?.trigger !== trigger) {
                    if (event.target !== trigger || !['ArrowDown', 'ArrowUp'].includes(event.key)) return;
                    event.preventDefault();
                    showMenu(event.key === 'ArrowDown' ? items[0] : items.at(-1));
                    return;
                }

                const index = items.indexOf(document.activeElement);
                switch (event.key) {
                    case 'Escape':
                        event.preventDefault();
                        closeMenu();
                        trigger.focus();
                        break;
                    case 'ArrowDown':
                        event.preventDefault();
                        items[(index + 1) % items.length].focus();
                        break;
                    case 'ArrowUp':
                        event.preventDefault();
                        items[(index <= 0 ? items.length : index) - 1].focus();
                        break;
                    case 'Home':
                    case 'End':
                        event.preventDefault();
                        (event.key === 'Home' ? items[0] : items.at(-1)).focus();
                        break;
                }
            });
        });

        document.addEventListener('click', (event) => {
            if (openMenu && !event.target.closest('[data-fm-action-menu]')) closeMenu();
        });
        actionDialog.querySelectorAll('[data-fm-dialog-close]').forEach((button) => {
            button.addEventListener('click', () => closeDialog(actionDialog));
        });
        form.addEventListener('submit', (event) => {
            if (confirm.disabled) event.preventDefault();
        });
    }

    /* Modal de Mapa Ampliado + Editor de Polígono com descarte seguro ao cancelar */
    const mapModal = document.querySelector('[data-fm-map-modal]');
    if (mapModal) {
        const geometries = parseJsonScript('fm-boundary-geometries-data', {});
        const plotGeoms = parseJsonScript('fm-plot-geometries-data', []);
        const titleEl = mapModal.querySelector('[data-fm-map-title]');
        const kmlLink = mapModal.querySelector('[data-fm-map-kml]');
        const viewPane = mapModal.querySelector('[data-fm-map-view]');
        const editPane = mapModal.querySelector('[data-fm-map-edit]');
        const previewImg = mapModal.querySelector('[data-fm-map-img]');
        const startEditBtn = mapModal.querySelector('[data-fm-map-start-edit]');
        const cancelEditBtn = mapModal.querySelector('[data-fm-map-cancel-edit]');
        const closeBtn = mapModal.querySelector('[data-fm-map-close-view]');
        const saveForm = mapModal.querySelector('[data-fm-map-form]');
        const geoInput = mapModal.querySelector('[data-fm-map-input]');
        const undoBtn = mapModal.querySelector('[data-fm-map-undo]');
        const removeBtn = mapModal.querySelector('[data-fm-map-remove]');
        const verticesEl = mapModal.querySelector('[data-fm-map-vertices]');
        const areaEl = mapModal.querySelector('[data-fm-map-area]');
        const guideEl = mapModal.querySelector('[data-fm-map-guide]');
        const applyAreaInput = mapModal.querySelector('[data-fm-map-apply-area]');
        const mapAreaModal = document.querySelector('[data-fm-map-area-modal]');
        const mapDiffDeclared = mapAreaModal?.querySelector('[data-fm-map-declared]');
        const mapDiffCalc = mapAreaModal?.querySelector('[data-fm-map-calculated]');

        let editor = null;
        let currentFarmId = null;
        let currentFarmArea = 0;
        let modalPolyArea = 0;
        let modalAreaChoiceSig = null;

        const modalDiffSig = () => `${currentFarmArea.toFixed(2)}|${modalPolyArea.toFixed(2)}`;
        const isModalAreaDivergent = () =>
            currentFarmArea > 0 && modalPolyArea > 0 && (Math.abs(modalPolyArea - currentFarmArea) / currentFarmArea) * 100 > 5.0001;

        const openMapAreaConfirm = () => {
            if (!mapAreaModal) return false;
            if (mapDiffDeclared) mapDiffDeclared.textContent = `${fmtBr(currentFarmArea, 2)} ha`;
            if (mapDiffCalc) mapDiffCalc.textContent = `${fmtBr(modalPolyArea, 2)} ha`;
            openDialog(mapAreaModal);
            return true;
        };

        mapAreaModal?.querySelectorAll('[data-fm-map-area-choice]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const choice = btn.dataset.fmMapAreaChoice;
                modalAreaChoiceSig = modalDiffSig();
                if (applyAreaInput) applyAreaInput.value = choice === 'map' ? '1' : '0';
                closeDialog(mapAreaModal);
                if (editor?.syncToHidden) editor.syncToHidden();
                saveForm?.submit();
            });
        });

        mapAreaModal?.querySelectorAll('[data-fm-map-area-close]').forEach((b) => {
            b.addEventListener('click', () => closeDialog(mapAreaModal));
        });

        saveForm?.addEventListener('submit', (e) => {
            if (isModalAreaDivergent() && modalAreaChoiceSig !== modalDiffSig()) {
                e.preventDefault();
                openMapAreaConfirm();
            }
        });

        const destroyModalEditor = () => {
            if (editor) {
                try { editor.destroy(); } catch (e) { /* ignore */ }
                editor = null;
            }
            modalPolyArea = 0;
            modalAreaChoiceSig = null;
            if (applyAreaInput) applyAreaInput.value = '0';
            closeDialog(mapAreaModal);
        };

        const setModalMode = (editing) => {
            if (viewPane) viewPane.hidden = editing;
            if (editPane) editPane.hidden = !editing;
            if (startEditBtn) startEditBtn.hidden = editing;
            if (cancelEditBtn) cancelEditBtn.hidden = !editing;
            if (saveForm) saveForm.hidden = !editing;
            if (closeBtn) closeBtn.hidden = editing;
        };

        const mountModalEditor = async () => {
            destroyModalEditor();
            if (typeof window.initFarmBoundaryEditor !== 'function') return;
            const geom = geometries[String(currentFarmId)] || null;
            editor = await window.initFarmBoundaryEditor({
                containerId: 'fm-modal-boundary-map',
                hiddenInput: geoInput,
                initialGeometry: geom,
                contextPlotGeometries: plotGeoms,
                removePointButton: removeBtn,
                undoButton: undoBtn,
                onStats: ({ vertices, area, selectedIndex, canUndo }) => {
                    modalPolyArea = vertices >= 3 ? Number(area || 0) : 0;
                    if (verticesEl) verticesEl.textContent = String(vertices);
                    if (areaEl) areaEl.textContent = `${fmtBr(area, 2)} ha`;
                    if (undoBtn) undoBtn.disabled = !canUndo;
                    if (removeBtn) removeBtn.disabled = selectedIndex === null || vertices <= 3;
                    if (guideEl) {
                        guideEl.textContent = vertices < 3
                            ? `Marque pelo menos ${3 - vertices} ponto(s) no mapa para fechar o perímetro.`
                            : 'Arraste os cantos para ajustar · Toque no (+) da borda para adicionar ponto · Selecione um canto para remover.';
                    }
                },
            });
            editor?.invalidate();
        };

        document.querySelectorAll('[data-fm-map-open]').forEach((btn) => {
            btn.addEventListener('click', () => {
                currentFarmId = btn.dataset.farmId;
                currentFarmArea = parseFloat(btn.dataset.farmArea || '0') || 0;
                const name = btn.dataset.farmName || 'Fazenda';
                const src = btn.dataset.mapSrc || '';
                if (titleEl) titleEl.textContent = `Mapa — ${name}`;
                if (previewImg) previewImg.src = src;
                if (kmlLink) kmlLink.href = `/fazendas/${currentFarmId}/geomapa.kml`;
                if (saveForm) saveForm.action = `/fazendas/${currentFarmId}/geometria`;
                setModalMode(false);
                openDialog(mapModal);
            });
        });

        startEditBtn?.addEventListener('click', () => {
            setModalMode(true);
            mountModalEditor();
        });

        cancelEditBtn?.addEventListener('click', () => {
            if (editor?.resetToInitial) editor.resetToInitial();
            destroyModalEditor();
            setModalMode(false);
        });

        mapModal.querySelectorAll('[data-fm-dialog-close]').forEach((b) => b.addEventListener('click', () => {
            destroyModalEditor();
            setModalMode(false);
            closeDialog(mapModal);
        }));
        mapModal.addEventListener('close', () => {
            destroyModalEditor();
            setModalMode(false);
        });
    }

    /* Copiar CAR na Ficha da Fazenda */
    document.querySelectorAll('[data-fm-copy]').forEach((btn) => {
        btn.addEventListener('click', async () => {
            const text = btn.dataset.fmCopy || '';
            if (!text) return;
            try {
                await navigator.clipboard.writeText(text);
                const prev = btn.textContent;
                btn.textContent = 'Copiado ✓';
                window.setTimeout(() => { btn.textContent = prev; }, 1800);
            } catch (e) { /* clipboard bloqueado */ }
        });
    });

    /* =========================================================================
       3. WIZARD DE CADASTRO / EDIÇÃO EM 3 ETAPAS (farm_form.html)
       ========================================================================= */
    const wizard = document.querySelector('[data-fm-wizard]');
    if (wizard) {
        const stepsBar = wizard.querySelector('[data-fm-steps]');
        const stepBtns = [...wizard.querySelectorAll('[data-fm-step-btn]')];
        const stepPanels = [...wizard.querySelectorAll('[data-fm-step-panel]')];
        const prevBtn = wizard.querySelector('[data-fm-prev]');
        const nextBtn = wizard.querySelector('[data-fm-next]');
        const submitBtn = wizard.querySelector('[data-fm-submit]');
        const step1Err = wizard.querySelector('[data-fm-step1-error]');

        const nameInput = wizard.querySelector('[data-fm-in="name"]');
        const locInput = wizard.querySelector('[data-fm-in="location"]');
        const areaInput = wizard.querySelector('[data-fm-in="area"]');
        const carInput = wizard.querySelector('[data-fm-in="car"]');
        const ccirInput = wizard.querySelector('[data-fm-in="ccir"]');
        const itrInput = wizard.querySelector('[data-fm-in="itr"]');
        const matInput = wizard.querySelector('[data-fm-in="matricula"]');
        const cartInput = wizard.querySelector('[data-fm-in="cartorio"]');
        const notesInput = wizard.querySelector('[data-fm-in="notes"]');
        const latInput = wizard.querySelector('[data-fm-in="lat"]');
        const lngInput = wizard.querySelector('[data-fm-in="lng"]');
        const geoInput = wizard.querySelector('[data-fm-boundary-input]');
        const geoFileInput = wizard.querySelector('[data-fm-geo-file]');

        const carLookupBtn = wizard.querySelector('[data-fm-car-lookup]');
        const carFeedback = wizard.querySelector('[data-fm-car-feedback]');

        const loadMapBtn = wizard.querySelector('[data-fm-load-map]');
        const mapPrompt = wizard.querySelector('[data-fm-map-prompt]');
        const mapWrap = wizard.querySelector('[data-fm-map-wrap]');
        const undoMapBtn = wizard.querySelector('[data-fm-wiz-undo]');
        const removeMapBtn = wizard.querySelector('[data-fm-wiz-remove]');
        const clearMapBtn = wizard.querySelector('[data-fm-wiz-clear]');
        const resetMapBtn = wizard.querySelector('[data-fm-wiz-reset]');
        const geoDropzone = wizard.querySelector('[data-fm-geo-dropzone]');
        const geoStatusEl = wizard.querySelector('[data-fm-geo-status]');
        const areaModal = document.querySelector('[data-fm-area-modal]');
        const areaDeclaredEl = areaModal?.querySelector('[data-fm-area-declared]');
        const areaCalcEl = areaModal?.querySelector('[data-fm-area-calculated]');
        const wizVerticesEl = wizard.querySelector('[data-fm-wiz-vertices]');
        const wizPolyAreaEl = wizard.querySelector('[data-fm-wiz-poly-area]');
        const wizPolyBadge = wizard.querySelector('[data-fm-poly-badge]');

        const planMeta = parseJsonScript('fm-plan-meta', {});
        const initialGeom = parseJsonScript('fm-edit-geometry-data', null);
        const locationData = parseJsonScript('fm-location-data', { states: [], initial_uf: '', initial_city: '', initial_municipalities: [] });
        let wizStep = Number(wizard.dataset.initialStep || 1);
        let formEditor = null;
        let currentGeom = initialGeom;
        let currentVertexCount = initialGeom ? (window.extractFarmBoundaryPoints?.(initialGeom)?.length || 0) : 0;
        let currentPolyArea = 0;
        let areaChoiceSignature = null;
        let pendingAreaAction = null;

        /* =====================================================================
           SELETORES EM CASCATA COM BUSCA: ESTADO (UF) -> MUNICÍPIO
           ===================================================================== */
        const escHtml = (str) => String(str || '').replace(/[&<>"']/g, (ch) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]
        ));
        const normText = (str) => String(str || '')
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .trim();
        const highlightMatch = (label, rawQuery) => {
            const safe = escHtml(label);
            const q = normText(rawQuery);
            if (!q) return safe;
            const normLabel = normText(label);
            const idx = normLabel.indexOf(q);
            if (idx === -1) return safe;
            const orig = String(label || '');
            return `${escHtml(orig.slice(0, idx))}<mark>${escHtml(orig.slice(idx, idx + q.length))}</mark>${escHtml(orig.slice(idx + q.length))}`;
        };
        const CHECK_SVG = '<svg class="fm-combo-check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6 9 17l-5-5"/></svg>';
        const PIN_SVG = '<svg class="fm-combo-pin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 21s-6-4.35-6-10a6 6 0 1 1 12 0c0 5.65-6 10-6 10Z"/><circle cx="12" cy="11" r="2.2"/></svg>';

        const stateField = wizard.querySelector('[data-fm-combo="state"]');
        const cityField = wizard.querySelector('[data-fm-combo="city"]');
        const cityCache = new Map();
        if (locationData.initial_uf && Array.isArray(locationData.initial_municipalities) && locationData.initial_municipalities.length) {
            cityCache.set(String(locationData.initial_uf).toUpperCase(), locationData.initial_municipalities);
        }
        let selectedUf = String(locationData.initial_uf || '').toUpperCase();
        let selectedCity = String(locationData.initial_city || '');
        const openCombos = new Set();

        const createSearchableCombo = (rootEl, config) => {
            if (!rootEl) return null;
            const trigger = rootEl.querySelector('[data-fm-combo-trigger]');
            const display = rootEl.querySelector('[data-fm-combo-display]');
            const panel = rootEl.querySelector('[data-fm-combo-panel]');
            const search = rootEl.querySelector('[data-fm-combo-search]');
            const clearBtn = rootEl.querySelector('[data-fm-combo-clear]');
            const meta = rootEl.querySelector('[data-fm-combo-meta]');
            const list = rootEl.querySelector('[data-fm-combo-list]');
            const empty = rootEl.querySelector('[data-fm-combo-empty]');

            let items = [];
            let filtered = [];
            let selectedValue = '';
            let focusedIndex = -1;
            let isOpen = false;

            const close = ({ restoreFocus = false } = {}) => {
                if (!isOpen) return;
                isOpen = false;
                openCombos.delete(api);
                panel.hidden = true;
                trigger.setAttribute('aria-expanded', 'false');
                if (restoreFocus) trigger.focus();
            };

            const renderList = () => {
                const rawQ = search ? search.value : '';
                const q = normText(rawQ);
                if (clearBtn) clearBtn.hidden = !rawQ;
                filtered = q
                    ? items.filter((item) => item.searchKey.includes(q))
                    : items;

                if (meta) {
                    meta.textContent = config.formatMeta
                        ? config.formatMeta(filtered.length, items.length, Boolean(q))
                        : `${filtered.length} de ${items.length}`;
                }

                if (!filtered.length) {
                    list.innerHTML = '';
                    if (empty) {
                        empty.textContent = q
                            ? `${config.emptyPrefix || 'Nenhum resultado'} para "${rawQ.trim()}".`
                            : (config.emptyDefault || 'Nenhuma opção disponível.');
                        empty.hidden = false;
                    }
                    focusedIndex = -1;
                    return;
                }

                if (empty) empty.hidden = true;
                if (focusedIndex >= filtered.length) focusedIndex = 0;

                list.innerHTML = filtered.map((item, idx) => {
                    const isSel = item.value === selectedValue;
                    const isFoc = idx === focusedIndex;
                    const badge = item.badge ? `<span class="fm-combo-uf">${escHtml(item.badge)}</span>` : '';
                    return `<li class="fm-combo-option${isSel ? ' is-selected' : ''}${isFoc ? ' is-focused' : ''}"
                                role="option"
                                id="${escHtml(config.idPrefix)}-opt-${idx}"
                                aria-selected="${isSel ? 'true' : 'false'}"
                                data-idx="${idx}">
                                ${badge}
                                <span class="fm-combo-opt-label">${highlightMatch(item.label, rawQ)}</span>
                                ${isSel ? CHECK_SVG : ''}
                            </li>`;
                }).join('');
            };

            const syncFocusVisual = () => {
                const opts = list.querySelectorAll('.fm-combo-option');
                opts.forEach((el, idx) => {
                    const on = idx === focusedIndex;
                    el.classList.toggle('is-focused', on);
                    if (on) el.scrollIntoView({ block: 'nearest' });
                });
            };

            const selectItem = (item, { silent = false } = {}) => {
                selectedValue = item ? item.value : '';
                if (item) {
                    display.classList.remove('is-placeholder');
                    display.innerHTML = config.renderSelected(item);
                } else {
                    display.classList.add('is-placeholder');
                    display.textContent = config.placeholder;
                }
                close({ restoreFocus: false });
                if (!silent && config.onSelect) config.onSelect(item || null);
            };

            const open = () => {
                if (trigger.disabled) return;
                openCombos.forEach((other) => { if (other !== api) other.close(); });
                isOpen = true;
                openCombos.add(api);
                panel.hidden = false;
                trigger.setAttribute('aria-expanded', 'true');
                if (search) search.value = '';
                focusedIndex = Math.max(0, items.findIndex((i) => i.value === selectedValue));
                renderList();
                window.requestAnimationFrame(() => {
                    search?.focus();
                    const selEl = list.querySelector('.fm-combo-option.is-selected');
                    if (selEl) selEl.scrollIntoView({ block: 'nearest' });
                });
            };

            trigger.addEventListener('click', () => {
                if (isOpen) close({ restoreFocus: true });
                else open();
            });

            trigger.addEventListener('keydown', (e) => {
                if (e.key === 'ArrowDown' || e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    open();
                }
            });

            search?.addEventListener('input', () => {
                focusedIndex = 0;
                renderList();
            });

            search?.addEventListener('keydown', (e) => {
                if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    if (filtered.length) {
                        focusedIndex = (focusedIndex + 1) % filtered.length;
                        syncFocusVisual();
                    }
                } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    if (filtered.length) {
                        focusedIndex = (focusedIndex - 1 + filtered.length) % filtered.length;
                        syncFocusVisual();
                    }
                } else if (e.key === 'Enter') {
                    e.preventDefault();
                    const target = filtered[focusedIndex] || filtered[0];
                    if (target) selectItem(target);
                } else if (e.key === 'Escape') {
                    e.preventDefault();
                    close({ restoreFocus: true });
                } else if (e.key === 'Tab') {
                    close();
                }
            });

            clearBtn?.addEventListener('click', () => {
                if (search) {
                    search.value = '';
                    search.focus();
                }
                focusedIndex = 0;
                renderList();
            });

            list.addEventListener('click', (e) => {
                const opt = e.target.closest('.fm-combo-option');
                if (!opt) return;
                const item = filtered[Number(opt.dataset.idx)];
                if (item) selectItem(item);
            });

            const api = {
                open,
                close,
                setItems(nextItems) {
                    items = nextItems || [];
                    if (isOpen) renderList();
                },
                setDisabled(flag, placeholderText) {
                    trigger.disabled = Boolean(flag);
                    if (placeholderText) config.placeholder = placeholderText;
                    if (!selectedValue) {
                        display.classList.add('is-placeholder');
                        display.textContent = config.placeholder;
                    }
                },
                setLoading(flag) {
                    trigger.classList.toggle('is-loading', Boolean(flag));
                    if (flag) {
                        trigger.disabled = true;
                        display.classList.add('is-placeholder');
                        display.textContent = 'Carregando municípios...';
                    }
                },
                setValue(val, { silent = true } = {}) {
                    const found = items.find((i) => i.value === val) || null;
                    if (found) {
                        selectItem(found, { silent });
                    } else if (val) {
                        selectedValue = val;
                        display.classList.remove('is-placeholder');
                        display.innerHTML = config.renderSelected({ value: val, label: val, badge: '' });
                    } else {
                        selectItem(null, { silent });
                    }
                },
                getValue: () => selectedValue,
                contains: (node) => rootEl.contains(node),
            };
            return api;
        };

        document.addEventListener('click', (e) => {
            openCombos.forEach((combo) => {
                if (!combo.contains(e.target)) combo.close();
            });
        });

        const syncLocationHidden = () => {
            if (!locInput) return;
            if (selectedCity && selectedUf) {
                locInput.value = `${selectedCity}, ${selectedUf}`;
            } else if (selectedCity) {
                locInput.value = selectedCity;
            } else {
                locInput.value = '';
            }
            if (step1Err && !step1Err.hidden && selectedUf && selectedCity) {
                step1Err.hidden = true;
            }
            syncSummary();
        };

        const fetchMunicipalitiesForUf = async (uf) => {
            const code = String(uf || '').toUpperCase();
            if (!code) return [];
            if (cityCache.has(code)) return cityCache.get(code);
            const res = await fetch(`/fazendas/municipios?uf=${encodeURIComponent(code)}`);
            if (!res.ok) return [];
            const payload = await res.json();
            const list = Array.isArray(payload.municipalities) ? payload.municipalities : [];
            if (list.length) cityCache.set(code, list);
            return list;
        };

        const cityCombo = createSearchableCombo(cityField, {
            idPrefix: 'fm-city',
            placeholder: 'Selecione o estado primeiro',
            emptyPrefix: 'Nenhum município encontrado',
            emptyDefault: 'Nenhum município disponível para este estado.',
            formatMeta: (shown, total, hasQ) => (hasQ
                ? `${shown} de ${total} municípios em ${selectedUf}`
                : `${total} municípios em ${selectedUf}`),
            renderSelected: (item) => `${PIN_SVG}<span>${escHtml(item.label)}</span>`,
            onSelect: (item) => {
                selectedCity = item ? item.value : '';
                syncLocationHidden();
            },
        });

        const loadCitiesForState = async (uf, { preselectCity = '', autoOpen = false } = {}) => {
            if (!cityCombo) return;
            if (!uf) {
                selectedCity = '';
                cityCombo.setItems([]);
                cityCombo.setValue('', { silent: true });
                cityCombo.setDisabled(true, 'Selecione o estado primeiro');
                syncLocationHidden();
                return;
            }
            if (!cityCache.has(uf)) cityCombo.setLoading(true);
            let names = [];
            try {
                names = await fetchMunicipalitiesForUf(uf);
            } catch (e) {
                names = [];
            }
            cityCombo.setLoading(false);
            const cityItems = names.map((name) => ({
                value: name,
                label: name,
                searchKey: normText(name),
            }));
            cityCombo.setItems(cityItems);
            cityCombo.setDisabled(false, `Selecione o município (${uf})`);

            if (preselectCity) {
                const targetNorm = normText(preselectCity);
                const matched = cityItems.find((c) => c.searchKey === targetNorm);
                selectedCity = matched ? matched.value : preselectCity;
                cityCombo.setValue(selectedCity, { silent: true });
            } else {
                selectedCity = '';
                cityCombo.setValue('', { silent: true });
            }
            syncLocationHidden();
            if (autoOpen && !selectedCity) {
                window.setTimeout(() => cityCombo.open(), 60);
            }
        };

        const stateItems = (locationData.states || []).map((st) => ({
            value: st.uf,
            label: st.name,
            badge: st.uf,
            searchKey: `${normText(st.uf)} ${normText(st.name)}`,
        }));

        const stateCombo = createSearchableCombo(stateField, {
            idPrefix: 'fm-state',
            placeholder: 'Selecione o estado',
            emptyPrefix: 'Nenhum estado encontrado',
            emptyDefault: 'Nenhum estado disponível.',
            formatMeta: (shown, total, hasQ) => (hasQ ? `${shown} de ${total} estados` : `${total} estados`),
            renderSelected: (item) => `<span class="fm-combo-uf">${escHtml(item.badge || item.value)}</span><span>${escHtml(item.label)}</span>`,
            onSelect: (item) => {
                const nextUf = item ? item.value : '';
                const changed = nextUf !== selectedUf;
                selectedUf = nextUf;
                if (changed) {
                    selectedCity = '';
                    loadCitiesForState(selectedUf, { preselectCity: '', autoOpen: true });
                }
            },
        });

        const applyLocationFromText = async (rawLocation) => {
            const text = String(rawLocation || '').trim();
            if (!text || !stateCombo) return;
            const match = text.match(/(?:[,\-/|]|\()\s*([A-Za-z]{2})\s*\)?$/);
            if (!match) return;
            const uf = match[1].toUpperCase();
            if (!stateItems.some((s) => s.value === uf)) return;
            const city = text.slice(0, match.index).replace(/[,\-/|()\s]+$/, '').trim();
            selectedUf = uf;
            stateCombo.setValue(uf, { silent: true });
            await loadCitiesForState(uf, { preselectCity: city, autoOpen: false });
        };

        const getDeclaredArea = () => parseFloat(String(areaInput?.value || '').replace(',', '.')) || 0;
        const areaDiffSig = () => `${getDeclaredArea().toFixed(2)}|${currentPolyArea.toFixed(2)}`;
        const isWizardAreaDivergent = () => {
            const declared = getDeclaredArea();
            return declared > 0 && currentPolyArea > 0 && (Math.abs(currentPolyArea - declared) / declared) * 100 > 5.0001;
        };

        areaModal?.querySelectorAll('[data-fm-area-choice]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const choice = btn.dataset.fmAreaChoice;
                if (choice === 'map' && currentPolyArea > 0 && areaInput) {
                    areaInput.value = currentPolyArea.toFixed(2);
                }
                areaChoiceSignature = areaDiffSig();
                syncSummary();
                closeDialog(areaModal);
                const nextAction = pendingAreaAction;
                pendingAreaAction = null;
                if (nextAction === 'step3') {
                    wizStep = 3;
                    renderStep();
                } else if (nextAction === 'submit') {
                    if (formEditor?.syncToHidden) formEditor.syncToHidden();
                    wizard.submit();
                }
            });
        });

        areaModal?.querySelectorAll('[data-fm-area-modal-close]').forEach((b) => {
            b.addEventListener('click', () => {
                pendingAreaAction = null;
                closeDialog(areaModal);
            });
        });

        const requireAreaDecisionIfNeeded = (onResolvedAction) => {
            if (isWizardAreaDivergent() && areaChoiceSignature !== areaDiffSig()) {
                pendingAreaAction = onResolvedAction || 'step3';
                if (areaDeclaredEl) areaDeclaredEl.textContent = `${fmtBr(getDeclaredArea(), 2)} ha`;
                if (areaCalcEl) areaCalcEl.textContent = `${fmtBr(currentPolyArea, 2)} ha`;
                openDialog(areaModal);
                return false;
            }
            return true;
        };

        if (ccirInput && ccirInput.value) ccirInput.value = maskCCIR(ccirInput.value);
        if (itrInput && itrInput.value) itrInput.value = maskCIB(itrInput.value);
        ccirInput?.addEventListener('input', () => { ccirInput.value = maskCCIR(ccirInput.value); });
        itrInput?.addEventListener('input', () => { itrInput.value = maskCIB(itrInput.value); });

        const syncSummary = () => {
            const name = nameInput?.value.trim() || 'Nova fazenda';
            const loc = locInput?.value.trim() || 'Informe a localização';
            const area = parseFloat(String(areaInput?.value || '').replace(',', '.')) || 0;
            const hasCar = Boolean(carInput?.value.trim());
            const hasGeo = currentVertexCount >= 3 || Boolean(geoFileInput?.files?.length);

            const setText = (sel, txt) => {
                wizard.querySelectorAll(sel).forEach((el) => { el.textContent = txt; });
            };
            setText('[data-fm-sum="name"]', name);
            setText('[data-fm-sum="location"]', loc);
            setText('[data-fm-sum="area"]', area > 0 ? `${fmtBr(area, 2)} ha` : '—');
            setText('[data-fm-sum="car"]', hasCar ? 'Informado' : 'Não informado');
            setText('[data-fm-sum="poly"]', hasGeo ? (currentVertexCount >= 3 ? `Pronto (${currentVertexCount} vértices)` : 'Arquivo GeoJSON') : 'Sem desenho');

            if (wizPolyBadge) {
                wizPolyBadge.textContent = hasGeo ? 'Perímetro pronto' : (currentVertexCount > 0 ? `${currentVertexCount}/3 pontos` : 'Sem perímetro');
                wizPolyBadge.className = `pd-chip ${hasGeo ? 'is-done' : 'is-warn'}`;
            }

            // Badges vivos dos blocos de Informações adicionais
            const regFilled = [ccirInput, itrInput, matInput, cartInput].filter((i) => i?.value.trim()).length;
            const bReg = wizard.querySelector('[data-fm-badge="reg"]');
            if (bReg) {
                bReg.textContent = regFilled ? `${regFilled} de 4` : 'Opcional';
                bReg.className = `pd-chip ${regFilled ? 'is-done' : 'is-muted'}`;
            }
            const hasNotes = Boolean(notesInput?.value.trim());
            const bNotes = wizard.querySelector('[data-fm-badge="notes"]');
            if (bNotes) {
                bNotes.textContent = hasNotes ? 'Preenchido' : 'Opcional';
                bNotes.className = `pd-chip ${hasNotes ? 'is-done' : 'is-muted'}`;
            }

            // Impacto no plano (antes → depois)
            const beforeArea = Number(planMeta.used_area || 0);
            const editArea = Number(planMeta.edit_area || 0);
            const afterArea = Math.max(0, beforeArea - editArea + area);
            const maxHa = planMeta.max_hectares ? Number(planMeta.max_hectares) : null;
            const maxFarms = planMeta.max_farms ? Number(planMeta.max_farms) : null;
            const beforeFarms = Number(planMeta.used_farms || 0);
            const afterFarms = planMeta.is_edit ? beforeFarms : beforeFarms + 1;

            const bar = wizard.querySelector('[data-fm-plan-bar]');
            const pctEl = wizard.querySelector('[data-fm-plan-pct]');
            const hintEl = wizard.querySelector('[data-fm-plan-hint]');
            const impactEl = wizard.querySelector('[data-fm-impact]');

            if (maxHa) {
                const pct = Math.min(100, Math.round((afterArea / maxHa) * 100));
                if (pctEl) pctEl.textContent = `${pct}%`;
                if (bar) {
                    bar.style.width = `${pct}%`;
                    bar.classList.toggle('is-over', afterArea > maxHa);
                }
                if (hintEl) {
                    hintEl.textContent = `${fmtBr(afterArea, 1)} ha de ${fmtBr(maxHa, 0)} ha contratados${afterArea > maxHa ? ' (acima do limite)' : ''}`;
                }
            }
            if (impactEl) {
                const parts = [];
                if (maxFarms) parts.push(`Fazendas: ${beforeFarms} → ${afterFarms} de ${maxFarms}`);
                if (maxHa) parts.push(`Área: ${fmtBr(beforeArea, 1)} → ${fmtBr(afterArea, 1)} ha de ${fmtBr(maxHa, 0)} ha`);
                impactEl.textContent = parts.length
                    ? `Impacto no plano (${planMeta.plan_name || 'Plano ativo'}): ${parts.join(' · ')}`
                    : `Resumo: ${fmtBr(area, 2)} ha em ${loc}.`;
            }
        };

        const validateStep1 = () => {
            const name = nameInput?.value.trim();
            const loc = locInput?.value.trim();
            const area = parseFloat(String(areaInput?.value || '').replace(',', '.')) || 0;
            const missingStateCity = Boolean(stateCombo) && (!selectedUf || !selectedCity) && !loc;
            if (!name || !loc || missingStateCity || area <= 0) {
                if (step1Err) {
                    if (stateCombo && (!selectedUf || !selectedCity) && !loc) {
                        step1Err.textContent = 'Selecione o estado e o município da fazenda para avançar.';
                    } else {
                        step1Err.textContent = 'Preencha nome da fazenda, estado, município e área total maior que zero para avançar.';
                    }
                    step1Err.hidden = false;
                }
                return false;
            }
            if (step1Err) step1Err.hidden = true;
            return true;
        };

        const mountWizardMap = async (geomOverride, centerOverride) => {
            if (typeof window.initFarmBoundaryEditor !== 'function') return;
            if (formEditor) {
                try { formEditor.destroy(); } catch (e) { /* ignore */ }
                formEditor = null;
            }
            if (mapPrompt) mapPrompt.hidden = true;
            if (mapWrap) mapWrap.hidden = false;
            const mapEl = document.getElementById('fm-wizard-boundary-map');
            if (mapEl) mapEl.classList.add('is-expanded');
            formEditor = await window.initFarmBoundaryEditor({
                containerId: 'fm-wizard-boundary-map',
                hiddenInput: geoInput,
                initialGeometry: geomOverride !== undefined ? geomOverride : currentGeom,
                initialCenter: centerOverride || undefined,
                removePointButton: removeMapBtn,
                undoButton: undoMapBtn,
                clearButton: clearMapBtn,
                onStats: ({ vertices, area, selectedIndex, canUndo }) => {
                    currentVertexCount = vertices;
                    currentPolyArea = vertices >= 3 ? Number(area || 0) : 0;
                    if (wizVerticesEl) wizVerticesEl.textContent = String(vertices);
                    if (wizPolyAreaEl) wizPolyAreaEl.textContent = `${fmtBr(area, 2)} ha`;
                    if (undoMapBtn) undoMapBtn.disabled = !canUndo;
                    if (removeMapBtn) removeMapBtn.disabled = selectedIndex === null || vertices <= 3;
                    syncSummary();
                },
            });
            formEditor?.invalidate();
        };

        const renderStep = () => {
            stepPanels.forEach((p) => {
                p.hidden = Number(p.dataset.fmStepPanel) !== wizStep;
            });
            stepBtns.forEach((btn) => {
                const n = Number(btn.dataset.fmStepBtn);
                btn.toggleAttribute('aria-current', n === wizStep);
                if (n === wizStep) btn.setAttribute('aria-current', 'step');
                else btn.removeAttribute('aria-current');
                btn.classList.toggle('is-done', n < wizStep);
                const badge = btn.querySelector('span');
                if (badge) badge.textContent = n < wizStep ? '✓' : String(n);
            });
            if (prevBtn) prevBtn.textContent = wizStep === 1 ? 'Cancelar' : '← Voltar';
            if (nextBtn) nextBtn.hidden = wizStep === 3;
            if (submitBtn) submitBtn.hidden = wizStep !== 3;

            if (wizStep === 2 && currentGeom && !formEditor) {
                mountWizardMap(currentGeom);
            } else if (wizStep === 2 && formEditor) {
                formEditor.invalidate();
            }

            // Mantém o passo atual sempre visível por completo na barra horizontal no celular
            if (stepsBar) {
                const activeBtn = stepBtns.find((b) => Number(b.dataset.fmStepBtn) === wizStep);
                if (activeBtn) {
                    window.requestAnimationFrame(() => {
                        const maxScroll = Math.max(0, stepsBar.scrollWidth - stepsBar.clientWidth);
                        const target = wizStep === 1
                            ? 0
                            : wizStep === 3
                                ? maxScroll
                                : Math.min(maxScroll, Math.max(0, activeBtn.offsetLeft - (stepsBar.clientWidth - activeBtn.offsetWidth) / 2));
                        stepsBar.scrollTo({ left: target, behavior: 'smooth' });
                    });
                }
            }
            syncSummary();
        };

        stepBtns.forEach((btn) => btn.addEventListener('click', () => {
            const target = Number(btn.dataset.fmStepBtn);
            if (target === wizStep) return;
            if (target > 1 && !validateStep1()) {
                wizStep = 1;
                renderStep();
                return;
            }
            if (target === 3 && !requireAreaDecisionIfNeeded('step3')) return;
            wizStep = target;
            renderStep();
        }));

        prevBtn?.addEventListener('click', () => {
            if (wizStep === 1) {
                window.location.href = wizard.dataset.cancelUrl || '/fazendas';
                return;
            }
            wizStep -= 1;
            renderStep();
        });

        nextBtn?.addEventListener('click', () => {
            if (wizStep === 1 && !validateStep1()) return;
            if (wizStep === 2 && !requireAreaDecisionIfNeeded('step3')) return;
            wizStep = Math.min(3, wizStep + 1);
            renderStep();
        });

        loadMapBtn?.addEventListener('click', async () => {
            let center = null;
            const lat = parseFloat(String(latInput?.value || '').replace(',', '.'));
            const lng = parseFloat(String(lngInput?.value || '').replace(',', '.'));
            if (!Number.isNaN(lat) && !Number.isNaN(lng)) {
                center = { lat, lng };
            } else if (locInput?.value.trim().length >= 2) {
                loadMapBtn.disabled = true;
                try {
                    const res = await fetch(`/fazendas/geocodificar?q=${encodeURIComponent(locInput.value.trim())}`);
                    const data = await res.json();
                    if (data.ok && typeof data.lat === 'number' && typeof data.lng === 'number') {
                        center = { lat: data.lat, lng: data.lng };
                    }
                } catch (e) { /* fallback centro padrão */ }
                loadMapBtn.disabled = false;
            }
            mountWizardMap(currentGeom, center);
        });

        resetMapBtn?.addEventListener('click', () => {
            if (formEditor?.resetToInitial) formEditor.resetToInitial();
        });

        /* Parse de arquivos geodésicos (.kml, .geojson, .json) + Drag & Drop com preview imediato no mapa */
        const parseKmlToGeoJson = (xmlText) => {
            const doc = new DOMParser().parseFromString(xmlText, 'application/xml');
            if (doc.querySelector('parsererror')) return null;
            const byLocalName = (root, name) =>
                [...root.getElementsByTagName('*')].filter((el) => (el.localName || el.tagName.split(':').pop()).toLowerCase() === name);
            const blobs = [];
            byLocalName(doc, 'outerboundaryis').forEach((ob) => {
                byLocalName(ob, 'coordinates').forEach((c) => {
                    if (c.textContent && c.textContent.trim()) blobs.push(c.textContent.trim());
                });
            });
            if (!blobs.length) {
                byLocalName(doc, 'coordinates').forEach((c) => {
                    if (c.textContent && c.textContent.trim()) blobs.push(c.textContent.trim());
                });
            }
            for (const blob of blobs) {
                const pts = [];
                blob.split(/\s+/).forEach((tok) => {
                    const parts = tok.split(',');
                    if (parts.length < 2) return;
                    const lng = parseFloat(parts[0]);
                    const lat = parseFloat(parts[1]);
                    if (!Number.isNaN(lng) && !Number.isNaN(lat) && lng >= -180 && lng <= 180 && lat >= -90 && lat <= 90) {
                        pts.push([lng, lat]);
                    }
                });
                if (pts.length >= 3) {
                    const first = pts[0];
                    const last = pts[pts.length - 1];
                    if (first[0] !== last[0] || first[1] !== last[1]) pts.push([first[0], first[1]]);
                    if (pts.length >= 4) return { type: 'Polygon', coordinates: [pts] };
                }
            }
            return null;
        };

        const parseGeodesicFileText = (rawText) => {
            const text = String(rawText || '').replace(/^\uFEFF/, '').trim();
            if (!text) return null;
            if (text.startsWith('{') || text.startsWith('[')) {
                try {
                    const obj = JSON.parse(text);
                    const pts = window.extractFarmBoundaryPoints?.(obj) || [];
                    if (pts.length >= 3) {
                        const closed = [...pts.map((p) => [p.lng, p.lat]), [pts[0].lng, pts[0].lat]];
                        return { type: 'Polygon', coordinates: [closed] };
                    }
                } catch (e) { /* tenta KML abaixo */ }
            }
            if (text.includes('<')) {
                return parseKmlToGeoJson(text);
            }
            return null;
        };

        const showGeoStatus = (msg, kind) => {
            if (!geoStatusEl) return;
            geoStatusEl.textContent = msg;
            geoStatusEl.className = `pd-alert ${kind === 'error' ? 'is-error' : 'is-info'}`;
            geoStatusEl.hidden = false;
        };

        const handleGeodesicFile = async (file) => {
            if (!file) return;
            const lower = (file.name || '').toLowerCase();
            if (!lower.endsWith('.kml') && !lower.endsWith('.geojson') && !lower.endsWith('.json')) {
                showGeoStatus('Formato não suportado. Envie um arquivo .kml, .geojson ou .json.', 'error');
                return;
            }
            try {
                const text = await file.text();
                const parsed = parseGeodesicFileText(text);
                if (!parsed) {
                    showGeoStatus(`Não foi possível encontrar um polígono válido em "${file.name}".`, 'error');
                    return;
                }
                currentGeom = parsed;
                const pts = window.extractFarmBoundaryPoints?.(parsed) || [];
                currentVertexCount = pts.length;
                if (geoInput) geoInput.value = JSON.stringify(parsed);
                if (geoFileInput) geoFileInput.value = '';
                await mountWizardMap(parsed);
                showGeoStatus(`"${file.name}" carregado no mapa (${pts.length} vértices). Você pode ajustar os pontos antes de salvar.`, 'info');
                syncSummary();
            } catch (err) {
                showGeoStatus('Erro ao ler o arquivo selecionado. Tente novamente.', 'error');
            }
        };

        geoFileInput?.addEventListener('change', () => {
            const file = geoFileInput.files && geoFileInput.files[0];
            if (file) handleGeodesicFile(file);
        });

        if (geoDropzone) {
            ['dragenter', 'dragover'].forEach((evt) => {
                geoDropzone.addEventListener(evt, (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    geoDropzone.classList.add('is-dragover');
                });
            });
            ['dragleave', 'dragend'].forEach((evt) => {
                geoDropzone.addEventListener(evt, () => {
                    geoDropzone.classList.remove('is-dragover');
                });
            });
            geoDropzone.addEventListener('drop', (e) => {
                e.preventDefault();
                e.stopPropagation();
                geoDropzone.classList.remove('is-dragover');
                const file = e.dataTransfer?.files && e.dataTransfer.files[0];
                if (file) handleGeodesicFile(file);
            });
        }

        carLookupBtn?.addEventListener('click', async () => {
            const raw = (carInput?.value || '').trim();
            if (!raw) {
                if (carFeedback) {
                    carFeedback.textContent = 'Informe o número do CAR para consultar.';
                    carFeedback.className = 'pd-alert is-warn';
                    carFeedback.hidden = false;
                }
                return;
            }
            carLookupBtn.disabled = true;
            try {
                const res = await fetch(`/fazendas/consultar-car?car=${encodeURIComponent(raw)}`);
                const payload = await res.json();
                if (!res.ok || !payload.ok) {
                    if (carFeedback) {
                        carFeedback.textContent = payload.message || 'Não foi possível consultar o CAR agora.';
                        carFeedback.className = 'pd-alert is-error';
                        carFeedback.hidden = false;
                    }
                } else {
                    const d = payload.data || {};
                    if (d.car_number && carInput) carInput.value = d.car_number;
                    if (d.location && locInput && !locInput.value.trim()) {
                        locInput.value = d.location;
                        await applyLocationFromText(d.location);
                    }
                    if (d.area_hectares && areaInput && !areaInput.value.trim()) areaInput.value = d.area_hectares;
                    if (d.geometry) {
                        currentGeom = d.geometry;
                        if (geoInput) geoInput.value = JSON.stringify(d.geometry);
                        currentVertexCount = window.extractFarmBoundaryPoints?.(d.geometry)?.length || 0;
                        if (formEditor) mountWizardMap(d.geometry);
                    }
                    if (carFeedback) {
                        carFeedback.textContent = d.message || 'CAR consultado com sucesso! Dados preenchidos.';
                        carFeedback.className = 'pd-alert is-info';
                        carFeedback.hidden = false;
                    }
                    syncSummary();
                }
            } catch (e) {
                if (carFeedback) {
                    carFeedback.textContent = 'Erro de conexão ao consultar o CAR.';
                    carFeedback.className = 'pd-alert is-error';
                    carFeedback.hidden = false;
                }
            } finally {
                carLookupBtn.disabled = false;
            }
        });

        [nameInput, locInput, areaInput, carInput, ccirInput, itrInput, matInput, cartInput, notesInput, geoFileInput].forEach((el) => {
            el?.addEventListener('input', syncSummary);
            el?.addEventListener('change', syncSummary);
        });

        wizard.addEventListener('submit', (e) => {
            if (!validateStep1()) {
                e.preventDefault();
                wizStep = 1;
                renderStep();
                return;
            }
            if (!requireAreaDecisionIfNeeded('submit')) {
                e.preventDefault();
                return;
            }
            if (formEditor?.syncToHidden) formEditor.syncToHidden();
        });

        if (stateCombo) {
            stateCombo.setItems(stateItems);
            if (selectedUf) {
                stateCombo.setValue(selectedUf, { silent: true });
                loadCitiesForState(selectedUf, { preselectCity: selectedCity, autoOpen: false });
            } else if (cityCombo) {
                cityCombo.setDisabled(true, 'Selecione o estado primeiro');
            }
        }

        renderStep();
    }
})();
