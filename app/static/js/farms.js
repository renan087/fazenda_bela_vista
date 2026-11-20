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

    const dangerModal = document.querySelector('[data-fm-danger-modal]');
    if (dangerModal) {
        const titleEl = dangerModal.querySelector('[data-fm-danger-title]');
        const pauseForm = dangerModal.querySelector('[data-fm-pause-form]');
        const pauseActionInput = dangerModal.querySelector('[data-fm-pause-action]');
        const pauseHeading = dangerModal.querySelector('[data-fm-pause-heading]');
        const pauseBtn = dangerModal.querySelector('[data-fm-pause-btn]');
        const deleteForm = dangerModal.querySelector('[data-fm-delete-form]');
        const deleteNote = dangerModal.querySelector('[data-fm-delete-note]');

        document.querySelectorAll('[data-fm-danger-open]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const id = btn.dataset.farmId;
                const name = btn.dataset.farmName || 'Fazenda';
                const paused = btn.dataset.farmPaused === '1';
                const plotsCount = Number(btn.dataset.farmPlots || 0);
                if (titleEl) titleEl.textContent = name;
                if (pauseForm) pauseForm.action = `/fazendas/${id}/regularizacao`;
                if (pauseActionInput) pauseActionInput.value = paused ? 'resume' : 'pause';
                if (pauseHeading) pauseHeading.textContent = paused ? 'Reativar operações' : 'Pausar operações';
                if (pauseBtn) pauseBtn.textContent = paused ? 'Reativar operações agora' : 'Pausar operações';
                if (deleteForm) deleteForm.action = `/fazendas/${id}/excluir`;
                if (deleteNote) {
                    deleteNote.textContent = plotsCount > 0
                        ? `Esta fazenda possui ${plotsCount} setor(es) vinculado(s). Remova ou desvincule os setores antes de excluir.`
                        : `Excluir "${name}" permanentemente? Esta ação não pode ser desfeita.`;
                }
                openDialog(dangerModal);
            });
        });
        dangerModal.querySelectorAll('[data-fm-dialog-close]').forEach((b) => b.addEventListener('click', () => closeDialog(dangerModal)));
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

        let editor = null;
        let currentFarmId = null;

        const destroyModalEditor = () => {
            if (editor) {
                try { editor.destroy(); } catch (e) { /* ignore */ }
                editor = null;
            }
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
        const wizVerticesEl = wizard.querySelector('[data-fm-wiz-vertices]');
        const wizPolyAreaEl = wizard.querySelector('[data-fm-wiz-poly-area]');
        const wizPolyBadge = wizard.querySelector('[data-fm-poly-badge]');

        const planMeta = parseJsonScript('fm-plan-meta', {});
        const initialGeom = parseJsonScript('fm-edit-geometry-data', null);
        let wizStep = Number(wizard.dataset.initialStep || 1);
        let formEditor = null;
        let currentGeom = initialGeom;
        let currentVertexCount = initialGeom ? (window.extractFarmBoundaryPoints?.(initialGeom)?.length || 0) : 0;

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
            if (!name || !loc || area <= 0) {
                if (step1Err) {
                    step1Err.textContent = 'Preencha nome da fazenda, localização e área total maior que zero para avançar.';
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
                    if (d.location && locInput && !locInput.value.trim()) locInput.value = d.location;
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

        wizard.addEventListener('submit', () => {
            if (formEditor?.syncToHidden) formEditor.syncToHidden();
        });

        renderStep();
    }
})();
