(() => {
    const root = document.querySelector('[data-og-page]');
    const dataEl = document.getElementById('og-data');
    if (!root || !dataEl) return;

    let payload;
    try {
        payload = JSON.parse(dataEl.textContent || '{}');
    } catch {
        return;
    }

    const csrfToken = root.dataset.csrf || '';
    const state = {
        currentUserId: payload.currentUserId || null,
        orgs: Array.isArray(payload.organizations) ? payload.organizations : [],
        users: Array.isArray(payload.users) ? payload.users : [],
        farms: Array.isArray(payload.farms) ? payload.farms : [],
    };
    if (!state.orgs.length && !state.users.length && !state.farms.length) return;

    const ORG_ACCENTS = [
        { bar: '#294127', soft: '#EEF3EB', text: '#294127' },
        { bar: '#446A36', soft: '#EEF6EB', text: '#2F5024' },
        { bar: '#2C6A8F', soft: '#EAF3F9', text: '#1F5373' },
        { bar: '#9A6324', soft: '#FAF2E6', text: '#7A4C17' },
        { bar: '#6B5B8A', soft: '#F2EFF8', text: '#50426B' },
        { bar: '#3E7A6E', soft: '#EAF5F2', text: '#295950' },
    ];
    const AVATAR_PALETTE = [
        { bg: '#E6F4E2', fg: '#1F5A2E' },
        { bg: '#E8F1FB', fg: '#1F5FA8' },
        { bg: '#FFF1DB', fg: '#8A5300' },
        { bg: '#F1EBF9', fg: '#5B3A8C' },
        { bg: '#E8EFEA', fg: '#294127' },
    ];
    const UF_LIST = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'];
    const GROUP_PREVIEW_LIMIT = 5;

    const ICONS = {
        dots: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="5" cy="12" r="1.9"/><circle cx="12" cy="12" r="1.9"/><circle cx="19" cy="12" r="1.9"/></svg>',
        search: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>',
        chev: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>',
        close: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18"/></svg>',
        transfer: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M17 3l4 4-4 4"/><path d="M3 7h18"/><path d="M7 21l-4-4 4-4"/><path d="M21 17H3"/></svg>',
        star: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
        unlock: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 9.9-1"/></svg>',
        building: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 21V5l8-3 8 3v16M2 21h20M9 21v-4h6v4"/></svg>',
        compare: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="4" width="7" height="16" rx="1.5"/><rect x="14" y="4" width="7" height="16" rx="1.5"/></svg>',
        pause: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M10 9v6M14 9v6"/></svg>',
        play: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><polygon points="10 8 16 12 10 16 10 8"/></svg>',
        shield: '<svg class="og-mi-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 2 4 5v6c0 5 3 8.3 8 11 5-2.7 8-6 8-11V5L12 2Z"/></svg>',
        external: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M13 5h6v6M19 5l-9 9M18 13v5H5V6h5"/></svg>',
    };

    const params = new URLSearchParams(location.search);
    const initialTab = ['orgs', 'users', 'farms'].includes(params.get('aba')) ? params.get('aba') : 'orgs';
    const initialOrgParam = /^[1-9]\d*$/.test(params.get('organizacao') || '') ? Number(params.get('organizacao')) : null;

    const ui = {
        mainTab: initialTab,
        orgFilter: 'all',
        orgQuery: '',
        userFilter: 'all',
        userOrgFilter: '',
        userQuery: '',
        userView: 'grouped',
        collapsedUserGroups: {},
        expandedUserGroupRows: {},
        farmFilter: 'all',
        farmOrgFilter: '',
        farmQuery: '',
        farmView: 'grouped',
        collapsedFarmGroups: {},
        expandedFarmGroupRows: {},
        drawerOrgId: null,
        drawerTab: 'people',
        drawerQuickAdd: null,
        drawerQuickQuery: '',
        drawerListQuery: '',
        drawerConfirmDelete: false,
        transfer: {
            kind: 'user',
            entityId: null,
            entityQuery: '',
            targetOrgId: null,
            targetOrgQuery: '',
        },
    };

    const workspaceEl = root.querySelector('[data-og-workspace]');
    const attentionPanel = root.querySelector('[data-og-attention]');
    const attentionCount = root.querySelector('[data-og-attention-count]');
    const attentionGrid = root.querySelector('[data-og-attention-grid]');
    const menuEl = document.querySelector('[data-og-menu]');
    const tipEl = document.querySelector('[data-og-tip]');
    const drawerBackdrop = document.querySelector('[data-og-drawer-backdrop]');
    const drawerEl = document.querySelector('[data-og-drawer]');
    const transferDialog = document.querySelector('[data-og-transfer-dialog]');
    const transferBody = document.querySelector('[data-og-transfer-body]');
    const createDialog = document.querySelector('[data-og-create-dialog]');
    const actionForm = document.querySelector('[data-og-action-form]');

    const norm = (v) => (v || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
    const initials = (name) => (name || '?').trim().split(/\s+/).slice(0, 2).map((p) => p[0] || '').join('').toUpperCase() || '?';
    const fmtArea = (val) => Number(val || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 });
    const orgAccent = (id) => ORG_ACCENTS[Math.abs((Number(id) || 1) - 1) % ORG_ACCENTS.length];
    const avatarColor = (id) => AVATAR_PALETTE[Math.abs(Number(id) || 0) % AVATAR_PALETTE.length];

    const orgById = new Map(state.orgs.map((o) => [o.id, o]));
    const userById = new Map(state.users.map((u) => [u.id, u]));
    const farmById = new Map(state.farms.map((f) => [f.id, f]));

    const getOrg = (id) => (id == null ? null : orgById.get(Number(id)) || null);
    const getUser = (id) => (id == null ? null : userById.get(Number(id)) || null);
    const getFarm = (id) => (id == null ? null : farmById.get(Number(id)) || null);
    const orgUsers = (orgId) => state.users.filter((u) => u.orgId === orgId);
    const orgFarms = (orgId) => state.farms.filter((f) => f.orgId === orgId);
    const orgActiveArea = (orgId) => orgFarms(orgId).filter((f) => !f.paused).reduce((s, f) => s + Number(f.area || 0), 0);
    const orgTotalArea = (orgId) => orgFarms(orgId).reduce((s, f) => s + Number(f.area || 0), 0);

    const dupFarmCounts = {};
    state.farms.forEach((f) => {
        const k = norm(f.name);
        if (k) dupFarmCounts[k] = (dupFarmCounts[k] || 0) + 1;
    });
    const isDuplicateFarm = (f) => (dupFarmCounts[norm(f.name)] || 0) > 1;
    const isPrimaryUser = (u) => {
        const o = getOrg(u.orgId);
        return Boolean(o && o.primaryUserId === u.id);
    };
    const orgNeedsPrimary = (org) => {
        const p = getUser(org.primaryUserId);
        return !p || p.orgId !== org.id || !p.isActive;
    };
    const orgHasIssues = (org) => orgNeedsPrimary(org) || orgUsers(org.id).some((u) => u.isBlocked);

    /* ================= POST SEGURO COM CSRF ================= */
    function submitPost(actionUrl, fields = {}) {
        actionForm.action = actionUrl;
        actionForm.innerHTML = `<input type="hidden" name="csrf_token" value="${esc(csrfToken)}">` +
            Object.entries(fields)
                .filter(([, val]) => val !== undefined && val !== null)
                .map(([k, val]) => `<input type="hidden" name="${esc(k)}" value="${esc(val)}">`)
                .join('');
        actionForm.submit();
    }

    /* ================= MENU FLUTUANTE ··· + TOOLTIP COM ATRASO ================= */
    let openMenuKey = null;
    let activeMenuBtn = null;
    let tipTimer = null;

    function hideTip() {
        clearTimeout(tipTimer);
        tipEl.classList.remove('is-visible');
    }

    function scheduleTip(btn, title, text) {
        hideTip();
        if (!text) return;
        tipTimer = setTimeout(() => {
            tipEl.innerHTML = `<strong>${esc(title)}</strong>${esc(text)}`;
            const r = btn.getBoundingClientRect();
            const tipW = 248;
            let left = r.left - tipW - 10;
            if (left < 12) left = r.right + 10;
            if (left + tipW > window.innerWidth - 12) left = Math.max(12, window.innerWidth - tipW - 12);
            let top = r.top - 4;
            if (top + 84 > window.innerHeight - 12) top = Math.max(12, window.innerHeight - 92);
            tipEl.style.left = `${left}px`;
            tipEl.style.top = `${top}px`;
            tipEl.classList.add('is-visible');
        }, 420);
    }

    function closeMenu() {
        hideTip();
        menuEl.hidden = true;
        if (activeMenuBtn) activeMenuBtn.setAttribute('aria-expanded', 'false');
        activeMenuBtn = null;
        openMenuKey = null;
    }

    function positionMenu(trigger) {
        const r = trigger.getBoundingClientRect();
        menuEl.hidden = false;
        const mw = menuEl.offsetWidth || 256;
        const mh = menuEl.offsetHeight || 190;
        let left = r.right - mw;
        if (left < 12) left = 12;
        if (left + mw > window.innerWidth - 12) left = window.innerWidth - mw - 12;
        let top = r.bottom + 6;
        if (top + mh > window.innerHeight - 12) top = Math.max(12, r.top - mh - 6);
        menuEl.style.left = `${left}px`;
        menuEl.style.top = `${top}px`;
    }

    function bindMenuEvents() {
        menuEl.querySelectorAll('[data-tip-text]').forEach((btn) => {
            btn.addEventListener('mouseenter', () => scheduleTip(btn, btn.dataset.tipTitle, btn.dataset.tipText));
            btn.addEventListener('mouseleave', hideTip);
            btn.addEventListener('focus', () => scheduleTip(btn, btn.dataset.tipTitle, btn.dataset.tipText));
            btn.addEventListener('blur', hideTip);
        });
    }

    function openUserMenu(trigger, userId) {
        const key = `u-${userId}`;
        if (openMenuKey === key) { closeMenu(); return; }
        closeMenu();
        const u = getUser(userId);
        if (!u) return;
        const org = getOrg(u.orgId);
        const isSelf = u.id === state.currentUserId;
        const canMakePrimary = org && !org.isProtected && u.isActive && org.primaryUserId !== u.id;

        const items = [];
        if (u.isBlocked) {
            items.push(`<button type="button" role="menuitem" class="og-mi is-danger" data-og-menu-action="unlock-user" data-id="${u.id}"
                data-tip-title="Desbloquear acesso"
                data-tip-text="Remove o bloqueio por excesso de tentativas de senha para este usuário voltar a entrar agora.">
                ${ICONS.unlock}<span>Desbloquear login</span></button>`);
        }
        if (!isSelf) {
            items.push(`<button type="button" role="menuitem" class="og-mi" data-og-menu-action="transfer-user" data-id="${u.id}"
                data-tip-title="${org ? 'Trocar organização' : 'Vincular à conta'}"
                data-tip-text="Abre a conferência Antes → Depois para mover o usuário, verificando cota e impacto no responsável.">
                ${ICONS.transfer}<span>${org ? 'Mover para outra organização' : 'Vincular a uma organização'}</span></button>`);
        } else {
            items.push(`<button type="button" role="menuitem" class="og-mi" disabled
                data-tip-title="Conta em uso"
                data-tip-text="Por segurança, o usuário atualmente logado não pode trocar a própria organização.">
                ${ICONS.shield}<span>Sua conta atual (protegida)</span></button>`);
        }
        if (canMakePrimary) {
            items.push(`<button type="button" role="menuitem" class="og-mi" data-og-menu-action="make-primary" data-id="${u.id}" data-org-id="${org.id}"
                data-tip-title="Responsável principal"
                data-tip-text="Define ${esc(u.name.split(' ')[0])} como contato de referência de ${esc(org.name)} e registra no histórico.">
                ${ICONS.star}<span>Tornar responsável principal</span></button>`);
        }
        if (org) {
            items.push('<div class="og-mi-sep" role="separator"></div>');
            items.push(`<button type="button" role="menuitem" class="og-mi" data-og-menu-action="open-org" data-org-id="${org.id}" data-tab="people"
                data-tip-title="Raio-X da organização"
                data-tip-text="Abre a gaveta lateral de ${esc(org.name)} para ver toda a equipe, fazendas, dados comerciais e limites.">
                ${ICONS.building}<span>Abrir ${esc(org.name)}</span></button>`);
        }

        menuEl.innerHTML = `
            <div class="og-menu-head">
                <strong>${esc(u.name)}</strong>
                <small>${esc(u.email)}</small>
            </div>
            ${items.join('')}
        `;
        activeMenuBtn = trigger;
        activeMenuBtn.setAttribute('aria-expanded', 'true');
        openMenuKey = key;
        positionMenu(trigger);
        bindMenuEvents();
    }

    function openFarmMenu(trigger, farmId) {
        const key = `f-${farmId}`;
        if (openMenuKey === key) { closeMenu(); return; }
        closeMenu();
        const f = getFarm(farmId);
        if (!f) return;
        const org = getOrg(f.orgId);

        const items = [
            `<button type="button" role="menuitem" class="og-mi" data-og-menu-action="transfer-farm" data-id="${f.id}"
                data-tip-title="${org ? 'Transferir propriedade' : 'Vincular propriedade'}"
                data-tip-text="Move esta fazenda e todo o seu histórico de talhões, safras e estoque, checando colisão de nome e cota de área.">
                ${ICONS.transfer}<span>${org ? 'Transferir para outra organização' : 'Vincular a uma organização'}</span></button>`,
        ];
        if (isDuplicateFarm(f)) {
            items.push(`<button type="button" role="menuitem" class="og-mi is-warn" data-og-menu-action="compare-dups"
                data-tip-title="Identificar homônimas"
                data-tip-text="Coloca lado a lado todas as fazendas chamadas '${esc(f.name)}' para conferir cidade, CAR, área e conta dona.">
                ${ICONS.compare}<span>Comparar nomes repetidos</span></button>`);
        }
        items.push(`<button type="button" role="menuitem" class="og-mi" data-og-menu-action="toggle-farm-pause" data-id="${f.id}" data-op="${f.paused ? 'resume' : 'pause'}"
            data-tip-title="${f.paused ? 'Retomar operação' : 'Pausar operação'}"
            data-tip-text="${f.paused ? 'Reativa esta fazenda para voltar a contar nos limites ativos do plano da organização.' : 'Pausa a operação da fazenda sem apagar o histórico; ela deixa de consumir cota ativa.'}">
            ${f.paused ? ICONS.play : ICONS.pause}<span>${f.paused ? 'Retomar operações' : 'Pausar operações'}</span></button>`);
        if (org) {
            items.push('<div class="og-mi-sep" role="separator"></div>');
            items.push(`<button type="button" role="menuitem" class="og-mi" data-og-menu-action="open-org" data-org-id="${org.id}" data-tab="farms"
                data-tip-title="Ver conta dona"
                data-tip-text="Abre a gaveta lateral de ${esc(org.name)} na aba Fazendas para gerenciar limites de área e propriedades.">
                ${ICONS.building}<span>Abrir ${esc(org.name)}</span></button>`);
        }

        menuEl.innerHTML = `
            <div class="og-menu-head">
                <strong>${esc(f.name)} <span>#${f.id}</span></strong>
                <small>${esc(f.location || 'Local não informado')} · ${fmtArea(f.area)} ha</small>
            </div>
            ${items.join('')}
        `;
        activeMenuBtn = trigger;
        activeMenuBtn.setAttribute('aria-expanded', 'true');
        openMenuKey = key;
        positionMenu(trigger);
        bindMenuEvents();
    }

    /* ================= ATUALIZAR KPIs E PRECISA DE ATENÇÃO ================= */
    function renderTopSummary() {
        const orgsNoPrimary = state.orgs.filter((o) => o.isActive && orgNeedsPrimary(o));
        const unassignedUsers = state.users.filter((u) => !u.orgId);
        const unassignedFarms = state.farms.filter((f) => !f.orgId);
        const blockedUsers = state.users.filter((u) => u.isBlocked);
        const dupFarms = state.farms.filter(isDuplicateFarm);
        const dupUniqueNames = [...new Set(dupFarms.map((f) => f.name))];
        const totalHa = state.farms.reduce((s, f) => s + Number(f.area || 0), 0);
        const totalPlots = state.farms.reduce((s, f) => s + Number(f.plots || 0), 0);

        root.querySelectorAll('[data-og-kpi-tab]').forEach((el) => {
            el.classList.toggle('is-active', el.dataset.ogKpiTab === ui.mainTab);
        });
        const orgNote = root.querySelector('[data-og-kpi-orgs-note]');
        if (orgNote) {
            orgNote.textContent = orgsNoPrimary.length ? `${orgsNoPrimary.length} sem responsável` : 'Todas com responsável';
            orgNote.className = orgsNoPrimary.length ? 'is-warn' : 'is-ok';
        }
        const uLeft = root.querySelector('[data-og-kpi-users-left]');
        if (uLeft) {
            uLeft.textContent = unassignedUsers.length ? `${unassignedUsers.length} sem organização` : 'Todos vinculados';
            uLeft.className = unassignedUsers.length ? 'is-warn' : '';
        }
        const uRight = root.querySelector('[data-og-kpi-users-right]');
        if (uRight) {
            uRight.textContent = blockedUsers.length ? `${blockedUsers.length} bloqueado${blockedUsers.length > 1 ? 's' : ''}` : 'Sem bloqueios';
            uRight.className = blockedUsers.length ? 'is-err' : '';
        }
        const fSub = root.querySelector('[data-og-kpi-farms-sub]');
        if (fSub) fSub.textContent = `${fmtArea(totalHa)} ha · ${totalPlots} talhões`;
        const fRight = root.querySelector('[data-og-kpi-farms-right]');
        if (fRight) {
            fRight.textContent = dupUniqueNames.length ? `${dupUniqueNames.length} nome${dupUniqueNames.length > 1 ? 's' : ''} se repetem` : 'Sem nomes repetidos';
            fRight.className = dupUniqueNames.length ? 'is-warn' : 'is-ok';
        }

        const alerts = [];
        if (dupFarms.length) {
            alerts.push(`<button type="button" class="og-att-item is-warn" data-og-jump="dup-farms">
                <span class="og-att-text"><strong>${dupFarms.length} fazendas com nome repetido</strong><small>${esc(dupUniqueNames.slice(0, 3).join(', '))}${dupUniqueNames.length > 3 ? '…' : ''}</small></span>
                <span class="og-att-cta">Comparar →</span>
            </button>`);
        }
        if (orgsNoPrimary.length) {
            const first = orgsNoPrimary[0];
            alerts.push(`<button type="button" class="og-att-item is-warn" data-og-open-drawer="${first.id}" data-drawer-tab="people">
                <span class="og-att-text"><strong>${orgsNoPrimary.length} conta${orgsNoPrimary.length > 1 ? 's' : ''} sem responsável</strong><small>${esc(first.name)} #${first.id}${first.suggestedPrimary ? ' · sugestão pronta' : ''}</small></span>
                <span class="og-att-cta">Definir →</span>
            </button>`);
        }
        if (unassignedUsers.length) {
            alerts.push(`<button type="button" class="og-att-item is-info" data-og-jump="unassigned-users">
                <span class="og-att-text"><strong>${unassignedUsers.length} usuário${unassignedUsers.length > 1 ? 's' : ''} sem organização</strong><small>Vincule para liberar o acesso às fazendas</small></span>
                <span class="og-att-cta">Vincular →</span>
            </button>`);
        }
        if (unassignedFarms.length) {
            alerts.push(`<button type="button" class="og-att-item is-info" data-og-jump="unassigned-farms">
                <span class="og-att-text"><strong>${unassignedFarms.length} fazenda${unassignedFarms.length > 1 ? 's' : ''} sem organização</strong><small>Vincule à conta proprietária</small></span>
                <span class="og-att-cta">Vincular →</span>
            </button>`);
        }

        if (!alerts.length) {
            attentionPanel.hidden = true;
        } else {
            attentionPanel.hidden = false;
            attentionCount.textContent = `${alerts.length} pendência${alerts.length > 1 ? 's' : ''}`;
            attentionGrid.innerHTML = alerts.join('');
        }
    }

    /* ================= ABA 1: ORGANIZAÇÕES ================= */
    function renderOrgsTab() {
        const q = norm(ui.orgQuery).replace(/^#/, '');
        const counts = {
            all: state.orgs.length,
            active: state.orgs.filter((o) => o.isActive).length,
            inactive: state.orgs.filter((o) => !o.isActive).length,
            'no-primary': state.orgs.filter(orgNeedsPrimary).length,
            issues: state.orgs.filter(orgHasIssues).length,
        };

        const filtered = state.orgs.map((org) => {
            const members = orgUsers(org.id);
            const farms = orgFarms(org.id);
            const primary = getUser(org.primaryUserId);
            const validPrimary = primary && primary.orgId === org.id ? primary : null;

            let matchHint = '';
            if (q) {
                const direct = norm(`${org.name} ${org.id} ${org.billing?.cpfCnpj || ''} ${org.billing?.contactName || ''} ${org.billing?.contactEmail || ''}`).includes(q);
                const mUser = members.find((u) => norm(`${u.name} ${u.email}`).includes(q));
                const mFarm = farms.find((f) => norm(`${f.name} ${f.location} ${f.id} ${f.car}`).includes(q));
                if (!direct && !mUser && !mFarm) return null;
                if (!direct && mFarm) matchHint = `Fazenda: ${mFarm.name}${mFarm.location ? ` (${mFarm.location})` : ''}`;
                else if (!direct && mUser) matchHint = `Usuário: ${mUser.name} (${mUser.email})`;
            }
            if (ui.orgFilter === 'active' && !org.isActive) return null;
            if (ui.orgFilter === 'inactive' && org.isActive) return null;
            if (ui.orgFilter === 'no-primary' && !orgNeedsPrimary(org)) return null;
            if (ui.orgFilter === 'issues' && !orgHasIssues(org)) return null;

            return { org, members, farms, validPrimary, matchHint };
        }).filter(Boolean);

        const rowsHtml = filtered.length ? filtered.map(({ org, members, farms, validPrimary, matchHint }) => {
            const acc = orgAccent(org.id);
            const avatars = members.slice(0, 3).map((u) => {
                const c = avatarColor(u.id);
                return `<span class="og-avatar" style="background:${c.bg};color:${c.fg}" title="${esc(u.name)} (${esc(u.email)})">${esc(initials(u.name))}</span>`;
            }).join('');
            const extraMembers = members.length > 3 ? `<span class="og-avatar is-extra">+${members.length - 3}</span>` : '';

            // Escala segura para 0, 1 ou 30+ fazendas (altura sempre fixa!)
            const totalHa = orgTotalArea(org.id);
            const dupInOrg = farms.filter(isDuplicateFarm).length;
            let farmsCell = '<span class="og-muted-sm">Nenhuma fazenda</span>';
            if (farms.length === 1) {
                const f = farms[0];
                const city = (f.location || '').split('/')[0].trim();
                farmsCell = `<div class="og-farm-preview">
                    <span class="og-farm-pill ${isDuplicateFarm(f) ? 'is-dup' : ''}"><strong>${esc(f.name)}</strong>${city ? `<span>· ${esc(city)}</span>` : ''}</span>
                    <small>${fmtArea(f.area)} ha · ${f.plots} talh${f.plots === 1 ? 'ão' : 'ões'}</small>
                </div>`;
            } else if (farms.length > 1) {
                const rest = farms.length - 1;
                farmsCell = `<div class="og-farm-preview">
                    <div class="og-farm-pills-row">
                        <span class="og-farm-pill"><strong>${esc(farms[0].name)}</strong></span>
                        <span class="og-farm-more">+${rest} ${rest === 1 ? 'fazenda' : 'fazendas'}</span>
                    </div>
                    <small>${farms.length} no total · ${fmtArea(totalHa)} ha${dupInOrg ? ` · <em class="is-warn">${dupInOrg} repetida${dupInOrg > 1 ? 's' : ''}</em>` : ''}</small>
                </div>`;
            }

            const blockedCount = members.filter((u) => u.isBlocked).length;
            return `<div class="og-row og-org-grid" id="organizacao-${org.id}" data-og-open-drawer="${org.id}" data-drawer-tab="people">
                <div class="og-cell-main">
                    <div class="og-title-line">
                        <span class="og-dot" style="--og-accent:${acc.bar}"></span>
                        <strong>${esc(org.name)}</strong>
                        <span class="og-id">#${org.id}</span>
                        <span class="og-badge ${org.isActive ? 'is-ok' : 'is-muted'}">${org.isActive ? 'Ativa' : 'Inativa'}</span>
                        <span class="og-badge is-${esc(org.planTone)}">${esc(org.planLabel)}</span>
                    </div>
                    ${matchHint ? `<p class="og-match-hint">Encontrado em ${esc(matchHint)}</p>` : ''}
                    ${blockedCount ? `<p class="og-inline-err">● ${blockedCount} usuário com login bloqueado</p>` : ''}
                </div>
                <div class="og-cell-sub og-hide-m">
                    ${validPrimary
                        ? `<strong>${esc(validPrimary.name)}${!validPrimary.isActive ? ' <span class="og-badge is-warn">Inativo</span>' : ''}</strong><small>${esc(validPrimary.email)}</small>`
                        : '<span class="og-badge is-warn">Sem responsável</span>'}
                </div>
                <div class="og-cell-people og-hide-m" data-og-open-drawer="${org.id}" data-drawer-tab="people">
                    <div class="og-avatar-line">
                        <span class="og-avatar-stack">${avatars}${extraMembers}</span>
                        <strong>${members.length}</strong>
                    </div>
                    <small>${members.length ? esc(members.slice(0, 2).map((m) => m.name.split(' ')[0]).join(', ') + (members.length > 2 ? ` +${members.length - 2}` : '')) : 'Nenhum usuário'}</small>
                </div>
                <div class="og-cell-farms og-hide-m" data-og-open-drawer="${org.id}" data-drawer-tab="farms">
                    ${farmsCell}
                </div>
                <div class="og-cell-act">
                    <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-open-drawer="${org.id}" data-drawer-tab="people">Gerenciar →</button>
                </div>
            </div>`;
        }).join('') : `<div class="pd-empty"><strong>Nenhuma organização encontrada neste filtro.</strong><span>Tente buscar pelo nome da fazenda, cidade ou e-mail do usuário.</span><button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-clear="orgs">Limpar filtros</button></div>`;

        return `<div class="pd-card og-panel-card">
            <div class="og-toolbar">
                <div class="sl-chips" role="group" aria-label="Filtrar organizações">
                    ${[
                        ['all', 'Todas', counts.all],
                        ['active', 'Ativas', counts.active],
                        ['inactive', 'Inativas', counts.inactive],
                        ['no-primary', 'Sem responsável', counts['no-primary']],
                        ['issues', 'Com pendências', counts.issues],
                    ].map(([k, lbl, n]) => `<button type="button" class="sl-chip" aria-pressed="${ui.orgFilter === k}" data-og-org-filter="${k}">${lbl} <span>${n}</span></button>`).join('')}
                </div>
                <label class="sl-search og-search-box">
                    ${ICONS.search}
                    <span class="sr-only">Buscar organização, usuário ou fazenda</span>
                    <input type="search" data-og-search="orgs" value="${esc(ui.orgQuery)}" placeholder="Organização, #ID, usuário ou fazenda">
                </label>
            </div>
            <div class="og-table-head og-org-grid og-hide-m" aria-hidden="true">
                <span>Organização & Plano</span><span>Responsável principal</span><span>Pessoas</span><span>Fazendas</span><span class="og-right">Ações</span>
            </div>
            <div>${rowsHtml}</div>
        </div>`;
    }

    /* ================= ABA 2: USUÁRIOS · QUEM ESTÁ ONDE ================= */
    function renderUsersTab() {
        const q = norm(ui.userQuery);
        const counts = {
            all: state.users.length,
            active: state.users.filter((u) => u.isActive).length,
            primary: state.users.filter(isPrimaryUser).length,
            blocked: state.users.filter((u) => u.isBlocked).length,
            unassigned: state.users.filter((u) => !u.orgId).length,
        };

        const filtered = state.users.filter((u) => {
            const o = getOrg(u.orgId);
            if (ui.userFilter === 'active' && !u.isActive) return false;
            if (ui.userFilter === 'primary' && !isPrimaryUser(u)) return false;
            if (ui.userFilter === 'blocked' && !u.isBlocked) return false;
            if (ui.userFilter === 'unassigned' && u.orgId) return false;
            if (ui.userOrgFilter && String(u.orgId || 'none') !== ui.userOrgFilter) return false;
            if (q && !norm(`${u.name} ${u.email} ${u.jobTitle} ${o ? o.name : 'sem organizacao'}`).includes(q)) return false;
            return true;
        });

        const toolbar = `<div class="pd-card og-toolbar">
            <div class="sl-chips" role="group" aria-label="Filtrar usuários">
                ${[
                    ['all', 'Todos', counts.all],
                    ['active', 'Ativos', counts.active],
                    ['primary', 'Responsáveis', counts.primary],
                    ['blocked', 'Bloqueados', counts.blocked],
                    ['unassigned', 'Sem org', counts.unassigned],
                ].map(([k, lbl, n]) => `<button type="button" class="sl-chip" aria-pressed="${ui.userFilter === k}" data-og-user-filter="${k}">${lbl} <span>${n}</span></button>`).join('')}
            </div>
            <div class="og-toolbar-right">
                <div class="og-seg" role="group" aria-label="Modo de visualização">
                    <button type="button" aria-pressed="${ui.userView === 'grouped'}" data-og-user-view="grouped">Por organização</button>
                    <button type="button" aria-pressed="${ui.userView === 'list'}" data-og-user-view="list">Lista</button>
                </div>
                <select class="og-select" aria-label="Filtrar por organização" data-og-user-org>
                    <option value="">Todas as organizações</option>
                    ${state.orgs.map((o) => `<option value="${o.id}" ${ui.userOrgFilter === String(o.id) ? 'selected' : ''}>${esc(o.name)} #${o.id}</option>`).join('')}
                    <option value="none" ${ui.userOrgFilter === 'none' ? 'selected' : ''}>Sem organização</option>
                </select>
                <label class="sl-search og-search-sm">
                    ${ICONS.search}
                    <span class="sr-only">Buscar usuário</span>
                    <input type="search" data-og-search="users" value="${esc(ui.userQuery)}" placeholder="Nome, e-mail ou cargo">
                </label>
            </div>
        </div>`;

        if (!filtered.length) {
            return toolbar + `<div class="pd-card pd-empty"><strong>Nenhum usuário encontrado.</strong><button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-clear="users">Ver todos os usuários</button></div>`;
        }

        const menuBtn = (u) => `<div class="og-cell-act"><button type="button" class="og-menu-btn" aria-haspopup="menu" aria-expanded="false" aria-label="Opções para ${esc(u.name)}" data-og-user-menu="${u.id}">${ICONS.dots}</button></div>`;

        if (ui.userView === 'grouped') {
            const groups = [];
            const unassigned = filtered.filter((u) => !u.orgId);
            if (unassigned.length) groups.push({ org: null, items: unassigned });
            state.orgs.forEach((org) => {
                const items = filtered.filter((u) => u.orgId === org.id);
                if (items.length) groups.push({ org, items });
            });

            const groupsHtml = groups.map(({ org, items }) => {
                const key = org ? String(org.id) : 'none';
                const collapsed = Boolean(ui.collapsedUserGroups[key]);
                const acc = org ? orgAccent(org.id) : null;
                const activeInOrg = org ? orgUsers(org.id).filter((u) => u.isActive).length : 0;
                const expanded = Boolean(ui.expandedUserGroupRows[key]) || Boolean(q);
                const visible = expanded ? items : items.slice(0, GROUP_PREVIEW_LIMIT);
                const hiddenCount = items.length - visible.length;

                const rows = visible.map((u) => {
                    const c = avatarColor(u.id);
                    const af = getFarm(u.activeFarmId);
                    const isSelf = u.id === state.currentUserId;
                    return `<div class="og-item-row og-grouped-user-grid">
                        <div class="og-person-cell">
                            <span class="og-avatar is-lg" style="background:${c.bg};color:${c.fg}">${esc(initials(u.name))}</span>
                            <div class="og-cell-main">
                                <div class="og-title-line">
                                    <strong>${esc(u.name)}</strong>
                                    ${isPrimaryUser(u) ? '<span class="og-badge is-brand">★ Responsável</span>' : ''}
                                    ${isSelf ? '<span class="og-badge is-muted">Você</span>' : ''}
                                    ${u.isBlocked ? '<span class="og-badge is-err">Login bloqueado</span>' : ''}
                                </div>
                                <small>${esc(u.email)}</small>
                            </div>
                        </div>
                        <div class="og-cell-sub og-hide-m">
                            <strong>${u.isAdmin ? 'Administrador' : esc(u.jobTitle || 'Equipe')}</strong>
                            <small>${af ? `Fazenda ativa: ${esc(af.name)}` : 'Sem fazenda ativa'}</small>
                        </div>
                        <div class="og-cell-sub og-hide-m">
                            <span class="og-badge ${u.isActive ? 'is-ok' : 'is-muted'}">${u.isActive ? 'Ativo' : 'Inativo'}</span>
                            <small>${esc(u.lastLogin)}</small>
                        </div>
                        ${menuBtn(u)}
                    </div>`;
                }).join('');

                const quotaTxt = org && org.maxUsers ? ` (cota ${activeInOrg}/${org.maxUsers})` : '';
                const footer = hiddenCount > 0
                    ? `<div class="og-group-foot"><span>Exibindo ${visible.length} de ${items.length} usuários</span><button type="button" class="og-link-btn" data-og-expand-group="users:${key}">Mostrar mais ${hiddenCount} usuário${hiddenCount > 1 ? 's' : ''} ↓</button></div>`
                    : (items.length > GROUP_PREVIEW_LIMIT && !q
                        ? `<div class="og-group-foot is-end"><button type="button" class="og-link-btn" data-og-expand-group="users:${key}">Recolher para os primeiros ${GROUP_PREVIEW_LIMIT} ↑</button></div>`
                        : '');

                return `<article class="og-group ${!org ? 'is-unassigned' : ''} ${collapsed ? 'is-collapsed' : ''}" ${acc ? `style="--og-accent:${acc.bar}"` : ''}>
                    <header class="og-group-head">
                        <div class="og-group-title">
                            <button type="button" class="og-collapse-btn" aria-expanded="${!collapsed}" data-og-collapse="users:${key}" title="${collapsed ? 'Expandir' : 'Recolher'}">${ICONS.chev}</button>
                            ${org ? `
                                <span class="og-dot" style="--og-accent:${acc.bar}"></span>
                                <button type="button" class="og-group-name" data-og-open-drawer="${org.id}" data-drawer-tab="people">${esc(org.name)}</button>
                                <span class="og-id">#${org.id}</span>
                                <span class="og-badge ${org.isActive ? 'is-ok' : 'is-muted'}">${org.isActive ? 'Ativa' : 'Inativa'}</span>
                                <span class="og-group-meta">· <strong>${items.length}</strong> usuário${items.length === 1 ? '' : 's'}${quotaTxt}</span>
                            ` : `
                                <span class="og-badge is-warn">Sem organização</span>
                                <span class="og-group-meta is-warn">${items.length} usuário${items.length === 1 ? '' : 's'} aguardando vínculo</span>
                            `}
                        </div>
                        ${org ? `<div class="og-group-acts">
                            <button type="button" class="og-link-btn" data-og-open-drawer="${org.id}" data-drawer-tab="people" data-drawer-quick="user">+ Trazer usuário</button>
                            <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-open-drawer="${org.id}" data-drawer-tab="people">Abrir conta →</button>
                        </div>` : ''}
                    </header>
                    ${collapsed ? '' : `<div>${rows}${footer}</div>`}
                </article>`;
            }).join('');

            return toolbar + `<div class="og-groups-stack">${groupsHtml}</div>`;
        }

        const listRows = filtered.map((u) => {
            const o = getOrg(u.orgId);
            const acc = o ? orgAccent(o.id) : null;
            const c = avatarColor(u.id);
            return `<div class="og-row og-user-grid">
                <div class="og-person-cell">
                    <span class="og-avatar is-lg" style="background:${c.bg};color:${c.fg}">${esc(initials(u.name))}</span>
                    <div class="og-cell-main">
                        <div class="og-title-line">
                            <strong>${esc(u.name)}</strong>
                            ${u.isBlocked ? '<span class="og-badge is-err">Bloqueado</span>' : ''}
                        </div>
                        <small>${esc(u.email)}</small>
                    </div>
                </div>
                <div class="og-hide-m">
                    ${o ? `<button type="button" class="og-org-chip" style="background:${acc.soft};color:${acc.text}" data-og-open-drawer="${o.id}" data-drawer-tab="people"><span class="og-dot" style="--og-accent:${acc.bar}"></span><span>${esc(o.name)}</span><em>#${o.id}</em></button>`
                        : '<span class="og-badge is-warn">Sem organização</span>'}
                </div>
                <div class="og-hide-m">
                    ${isPrimaryUser(u) ? '<span class="og-badge is-brand">★ Responsável</span>' : `<span class="og-muted-sm">${u.isAdmin ? 'Admin' : esc(u.jobTitle || 'Equipe')}</span>`}
                </div>
                <div class="og-hide-m"><span class="og-badge ${u.isActive ? 'is-ok' : 'is-muted'}">${u.isActive ? 'Ativo' : 'Inativo'}</span></div>
                ${menuBtn(u)}
            </div>`;
        }).join('');

        return toolbar + `<div class="pd-card og-panel-card">
            <div class="og-table-head og-user-grid og-hide-m" aria-hidden="true">
                <span>Usuário</span><span>Organização atual</span><span>Papel</span><span>Status</span><span class="og-right">Ações</span>
            </div>
            <div>${listRows}</div>
        </div>`;
    }

    /* ================= ABA 3: FAZENDAS · ONDE ESTÁ CADA UMA ================= */
    function renderFarmsTab() {
        const q = norm(ui.farmQuery).replace(/^#/, '');
        const counts = {
            all: state.farms.length,
            duplicates: state.farms.filter(isDuplicateFarm).length,
            paused: state.farms.filter((f) => f.paused).length,
            unassigned: state.farms.filter((f) => !f.orgId).length,
        };

        const filtered = state.farms.filter((f) => {
            const o = getOrg(f.orgId);
            if (ui.farmFilter === 'duplicates' && !isDuplicateFarm(f)) return false;
            if (ui.farmFilter === 'paused' && !f.paused) return false;
            if (ui.farmFilter === 'unassigned' && f.orgId) return false;
            if (ui.farmOrgFilter && String(f.orgId || 'none') !== ui.farmOrgFilter) return false;
            if (q && !norm(`${f.name} ${f.id} ${f.location} ${f.car} ${o ? o.name : 'sem organizacao'}`).includes(q)) return false;
            return true;
        });

        const toolbar = `<div class="pd-card og-toolbar">
            <div class="sl-chips" role="group" aria-label="Filtrar fazendas">
                ${[
                    ['all', 'Todas', counts.all],
                    ['duplicates', 'Nomes repetidos', counts.duplicates],
                    ['paused', 'Pausadas', counts.paused],
                    ['unassigned', 'Sem org', counts.unassigned],
                ].map(([k, lbl, n]) => `<button type="button" class="sl-chip" aria-pressed="${ui.farmFilter === k}" data-og-farm-filter="${k}">${lbl} <span>${n}</span></button>`).join('')}
            </div>
            <div class="og-toolbar-right">
                ${ui.farmFilter !== 'duplicates' ? `<div class="og-seg" role="group" aria-label="Modo de visualização">
                    <button type="button" aria-pressed="${ui.farmView === 'grouped'}" data-og-farm-view="grouped">Por organização</button>
                    <button type="button" aria-pressed="${ui.farmView === 'list'}" data-og-farm-view="list">Lista</button>
                </div>` : ''}
                <select class="og-select" aria-label="Filtrar por organização" data-og-farm-org>
                    <option value="">Todas as organizações</option>
                    ${state.orgs.map((o) => `<option value="${o.id}" ${ui.farmOrgFilter === String(o.id) ? 'selected' : ''}>${esc(o.name)} #${o.id}</option>`).join('')}
                    <option value="none" ${ui.farmOrgFilter === 'none' ? 'selected' : ''}>Sem organização</option>
                </select>
                <label class="sl-search og-search-sm">
                    ${ICONS.search}
                    <span class="sr-only">Buscar fazenda</span>
                    <input type="search" data-og-search="farms" value="${esc(ui.farmQuery)}" placeholder="Fazenda, #ID, cidade ou CAR">
                </label>
            </div>
        </div>`;

        if (!filtered.length) {
            return toolbar + `<div class="pd-card pd-empty"><strong>Nenhuma fazenda encontrada.</strong><button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-clear="farms">Ver todas as fazendas</button></div>`;
        }

        const menuBtn = (f) => `<div class="og-cell-act"><button type="button" class="og-menu-btn" aria-haspopup="menu" aria-expanded="false" aria-label="Opções para ${esc(f.name)}" data-og-farm-menu="${f.id}">${ICONS.dots}</button></div>`;

        if (ui.farmFilter === 'duplicates') {
            const byName = new Map();
            filtered.forEach((f) => {
                const k = norm(f.name);
                if (!byName.has(k)) byName.set(k, { title: f.name, items: [] });
                byName.get(k).items.push(f);
            });

            const dupHtml = [...byName.values()].map((group) => `<article class="pd-card og-dup-card">
                <header class="og-dup-head">
                    <div class="og-group-title">
                        <span class="og-badge is-warn">Nome repetido</span>
                        <strong>"${esc(group.title)}"</strong>
                        <span class="og-group-meta is-warn">aparece em ${group.items.length} contas diferentes</span>
                    </div>
                    <small>Compare cidade, área, CAR e conta dona</small>
                </header>
                <div class="og-dup-grid">
                    ${group.items.map((f) => {
                        const o = getOrg(f.orgId);
                        const acc = o ? orgAccent(o.id) : null;
                        const primary = o ? getUser(o.primaryUserId) : null;
                        return `<div class="og-dup-col" ${acc ? `style="border-top:3px solid ${acc.bar}"` : ''}>
                            <div class="og-dup-top">
                                ${o ? `<button type="button" class="og-org-chip" style="background:${acc.soft};color:${acc.text}" data-og-open-drawer="${o.id}" data-drawer-tab="farms"><span class="og-dot" style="--og-accent:${acc.bar}"></span><span>${esc(o.name)}</span><em>#${o.id}</em></button>`
                                    : '<span class="og-badge is-warn">Sem organização</span>'}
                                ${menuBtn(f)}
                            </div>
                            <dl class="og-dup-meta">
                                <div><dt>Cidade / Local</dt><dd>${esc(f.location || 'Não informado')}</dd></div>
                                <div><dt>Área e talhões</dt><dd>${fmtArea(f.area)} ha · ${f.plots} talh${f.plots === 1 ? 'ão' : 'ões'}</dd></div>
                                <div><dt>ID · CAR</dt><dd>#${f.id}${f.car ? ` · ${esc(f.car)}` : ''}</dd></div>
                                <div><dt>Responsável</dt><dd>${primary ? esc(primary.name) : 'Não definido'}</dd></div>
                            </dl>
                            <div class="og-dup-foot">
                                <span class="og-badge ${f.paused ? 'is-muted' : 'is-ok'}">${f.paused ? 'Pausada' : 'Em operação'}</span>
                                <small>Use ··· para transferir ou pausar</small>
                            </div>
                        </div>`;
                    }).join('')}
                </div>
            </article>`).join('');

            return toolbar + `<div class="og-groups-stack">${dupHtml}</div>`;
        }

        if (ui.farmView === 'grouped') {
            const groups = [];
            const unassigned = filtered.filter((f) => !f.orgId);
            if (unassigned.length) groups.push({ org: null, items: unassigned });
            state.orgs.forEach((org) => {
                const items = filtered.filter((f) => f.orgId === org.id);
                if (items.length) groups.push({ org, items });
            });

            const groupsHtml = groups.map(({ org, items }) => {
                const key = org ? String(org.id) : 'none';
                const collapsed = Boolean(ui.collapsedFarmGroups[key]);
                const acc = org ? orgAccent(org.id) : null;
                const groupHa = items.reduce((s, f) => s + Number(f.area || 0), 0);
                const primary = org ? getUser(org.primaryUserId) : null;
                const expanded = Boolean(ui.expandedFarmGroupRows[key]) || Boolean(q);
                const visible = expanded ? items : items.slice(0, GROUP_PREVIEW_LIMIT);
                const hiddenCount = items.length - visible.length;

                const rows = visible.map((f) => {
                    const dc = dupFarmCounts[norm(f.name)] || 1;
                    return `<div class="og-item-row og-grouped-farm-grid">
                        <div class="og-cell-main">
                            <div class="og-title-line">
                                <strong>${esc(f.name)}</strong>
                                <span class="og-id">#${f.id}</span>
                                ${dc > 1 ? `<button type="button" class="og-badge is-warn is-clickable" data-og-jump="dup-farms">Nome repetido · ${dc} contas</button>` : ''}
                            </div>
                            <small>${esc(f.location || 'Local não informado')}${f.car ? ` · CAR ${esc(f.car)}` : ''}</small>
                        </div>
                        <div class="og-cell-sub og-hide-m">
                            <strong>${fmtArea(f.area)} ha</strong>
                            <small>${f.plots} talh${f.plots === 1 ? 'ão' : 'ões'}</small>
                        </div>
                        <div class="og-hide-m">
                            <span class="og-badge ${f.paused ? 'is-muted' : 'is-ok'}">${f.paused ? 'Pausada' : 'Em operação'}</span>
                        </div>
                        ${menuBtn(f)}
                    </div>`;
                }).join('');

                const footer = hiddenCount > 0
                    ? `<div class="og-group-foot"><span>Exibindo ${visible.length} de ${items.length} fazendas</span><button type="button" class="og-link-btn" data-og-expand-group="farms:${key}">Mostrar mais ${hiddenCount} fazenda${hiddenCount > 1 ? 's' : ''} ↓</button></div>`
                    : (items.length > GROUP_PREVIEW_LIMIT && !q
                        ? `<div class="og-group-foot is-end"><button type="button" class="og-link-btn" data-og-expand-group="farms:${key}">Recolher para as primeiras ${GROUP_PREVIEW_LIMIT} ↑</button></div>`
                        : '');

                return `<article class="og-group ${!org ? 'is-unassigned' : ''} ${collapsed ? 'is-collapsed' : ''}" ${acc ? `style="--og-accent:${acc.bar}"` : ''}>
                    <header class="og-group-head">
                        <div class="og-group-title">
                            <button type="button" class="og-collapse-btn" aria-expanded="${!collapsed}" data-og-collapse="farms:${key}" title="${collapsed ? 'Expandir' : 'Recolher'}">${ICONS.chev}</button>
                            ${org ? `
                                <span class="og-dot" style="--og-accent:${acc.bar}"></span>
                                <button type="button" class="og-group-name" data-og-open-drawer="${org.id}" data-drawer-tab="farms">${esc(org.name)}</button>
                                <span class="og-id">#${org.id}</span>
                                <span class="og-badge ${org.isActive ? 'is-ok' : 'is-muted'}">${org.isActive ? 'Ativa' : 'Inativa'}</span>
                                <span class="og-group-meta">· <strong>${items.length}</strong> fazenda${items.length === 1 ? '' : 's'} · <strong>${fmtArea(groupHa)} ha</strong>${primary ? ` · Resp.: ${esc(primary.name.split(' ')[0])}` : ' · Sem responsável'}</span>
                            ` : `
                                <span class="og-badge is-warn">Sem organização</span>
                                <span class="og-group-meta is-warn">${items.length} fazenda${items.length === 1 ? '' : 's'} aguardando vínculo (${fmtArea(groupHa)} ha)</span>
                            `}
                        </div>
                        ${org ? `<div class="og-group-acts">
                            <button type="button" class="og-link-btn" data-og-open-drawer="${org.id}" data-drawer-tab="farms" data-drawer-quick="farm">+ Vincular fazenda</button>
                            <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-open-drawer="${org.id}" data-drawer-tab="farms">Abrir conta →</button>
                        </div>` : ''}
                    </header>
                    ${collapsed ? '' : `<div>${rows}${footer}</div>`}
                </article>`;
            }).join('');

            return toolbar + `<div class="og-groups-stack">${groupsHtml}</div>`;
        }

        const listRows = filtered.map((f) => {
            const o = getOrg(f.orgId);
            const acc = o ? orgAccent(o.id) : null;
            const dc = dupFarmCounts[norm(f.name)] || 1;
            return `<div class="og-row og-farm-grid">
                <div class="og-cell-main">
                    <div class="og-title-line">
                        <strong>${esc(f.name)}</strong>
                        <span class="og-id">#${f.id}</span>
                        ${dc > 1 ? `<button type="button" class="og-badge is-warn is-clickable" data-og-jump="dup-farms">Nome repetido · ${dc}</button>` : ''}
                    </div>
                    <small>${esc(f.location || 'Local não informado')}${f.car ? ` · CAR ${esc(f.car)}` : ''}</small>
                </div>
                <div class="og-cell-sub og-hide-m"><strong>${fmtArea(f.area)} ha</strong><small>${f.plots} talh${f.plots === 1 ? 'ão' : 'ões'}</small></div>
                <div class="og-hide-m">
                    ${o ? `<button type="button" class="og-org-chip" style="background:${acc.soft};color:${acc.text}" data-og-open-drawer="${o.id}" data-drawer-tab="farms"><span class="og-dot" style="--og-accent:${acc.bar}"></span><span>${esc(o.name)}</span><em>#${o.id}</em></button>`
                        : '<span class="og-badge is-warn">Sem organização</span>'}
                </div>
                <div class="og-hide-m"><span class="og-badge ${f.paused ? 'is-muted' : 'is-ok'}">${f.paused ? 'Pausada' : 'Em operação'}</span></div>
                ${menuBtn(f)}
            </div>`;
        }).join('');

        return toolbar + `<div class="pd-card og-panel-card">
            <div class="og-table-head og-farm-grid og-hide-m" aria-hidden="true">
                <span>Fazenda · Local · CAR</span><span>Porte</span><span>Organização dona</span><span>Status</span><span class="og-right">Ações</span>
            </div>
            <div>${listRows}</div>
        </div>`;
    }

    /* ================= GAVETA LATERAL (RAIO-X DA ORGANIZAÇÃO) ================= */
    function renderDrawer() {
        if (!ui.drawerOrgId) {
            drawerBackdrop.classList.remove('is-open');
            drawerBackdrop.setAttribute('aria-hidden', 'true');
            return;
        }
        const org = getOrg(ui.drawerOrgId);
        if (!org) return;
        drawerBackdrop.classList.add('is-open');
        drawerBackdrop.setAttribute('aria-hidden', 'false');

        const acc = orgAccent(org.id);
        const members = orgUsers(org.id);
        const farms = orgFarms(org.id);
        const activeHa = orgActiveArea(org.id);
        const primary = getUser(org.primaryUserId);
        const validPrimary = primary && primary.orgId === org.id ? primary : null;

        let body = '';
        if (ui.drawerTab === 'people') {
            const qAdd = norm(ui.drawerQuickQuery);
            const qLocal = norm(ui.drawerListQuery);
            const filteredMembers = qLocal ? members.filter((u) => norm(`${u.name} ${u.email} ${u.jobTitle}`).includes(qLocal)) : members;
            const candidates = state.users.filter((u) =>
                u.orgId !== org.id && u.id !== state.currentUserId &&
                (!qAdd || norm(`${u.name} ${u.email} ${getOrg(u.orgId)?.name || ''}`).includes(qAdd))
            );

            body = `<div class="og-drawer-stack">
                ${!validPrimary && org.suggestedPrimary ? `<div class="og-banner is-warn">
                    <div><strong>Sugestão de responsável (teste grátis)</strong><small>${esc(org.suggestedPrimary.name)} · ${esc(org.suggestedPrimary.email)}</small></div>
                    <button type="button" class="sf-btn sf-btn-primary og-btn-sm" data-og-set-primary="${org.suggestedPrimary.id}" data-org-id="${org.id}">Definir agora</button>
                </div>` : ''}

                ${!validPrimary && !org.suggestedPrimary ? '<p class="og-muted-sm">Responsável principal não definido.</p>' : ''}

                <div class="og-subhead">
                    <div><h3>Usuários vinculados (${members.length})</h3><p>Defina o responsável principal ou mova pessoas entre contas.</p></div>
                    <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-drawer-quick="user">${ui.drawerQuickAdd === 'user' ? 'Fechar busca' : '+ Trazer usuário'}</button>
                </div>

                ${ui.drawerQuickAdd === 'user' ? `<div class="og-quick-box">
                    <strong>Buscar usuário para trazer para ${esc(org.name)}</strong>
                    <label class="sl-search">${ICONS.search}<input type="search" data-og-drawer-quick-q value="${esc(ui.drawerQuickQuery)}" placeholder="Digite nome ou e-mail..."></label>
                    <div class="og-quick-list">
                        ${candidates.length ? candidates.slice(0, 20).map((u) => {
                            const fo = getOrg(u.orgId);
                            return `<div class="og-quick-item">
                                <div><strong>${esc(u.name)}</strong><small>${esc(u.email)} · ${fo ? `Hoje em ${esc(fo.name)}` : 'Sem organização'}</small></div>
                                <button type="button" class="sf-btn sf-btn-primary og-btn-sm" data-og-start-transfer="user" data-entity-id="${u.id}" data-target-org="${org.id}">Conferir →</button>
                            </div>`;
                        }).join('') : '<p class="og-muted-sm">Nenhum outro usuário encontrado.</p>'}
                    </div>
                </div>` : ''}

                ${members.length > 4 ? `<label class="sl-search">${ICONS.search}<input type="search" data-og-drawer-list-q value="${esc(ui.drawerListQuery)}" placeholder="Filtrar nos ${members.length} usuários..."></label>` : ''}

                <div class="og-drawer-list">
                    ${filteredMembers.length ? filteredMembers.map((u) => {
                        const c = avatarColor(u.id);
                        const isPrim = validPrimary && validPrimary.id === u.id;
                        return `<div class="og-drawer-row">
                            <div class="og-person-cell">
                                <span class="og-avatar is-lg" style="background:${c.bg};color:${c.fg}">${esc(initials(u.name))}</span>
                                <div class="og-cell-main">
                                    <div class="og-title-line">
                                        <strong>${esc(u.name)}</strong>
                                        ${isPrim ? '<span class="og-badge is-brand">★ Responsável</span>' : ''}
                                        ${u.isBlocked ? '<span class="og-badge is-err">Bloqueado</span>' : ''}
                                        ${!u.isActive ? '<span class="og-badge is-muted">Inativo</span>' : ''}
                                    </div>
                                    <small>${esc(u.email)}${u.jobTitle ? ` · ${esc(u.jobTitle)}` : ''}</small>
                                </div>
                            </div>
                            <button type="button" class="og-menu-btn" aria-haspopup="menu" aria-expanded="false" aria-label="Opções para ${esc(u.name)}" data-og-user-menu="${u.id}">${ICONS.dots}</button>
                        </div>`;
                    }).join('') : '<p class="og-drawer-empty">Nenhum usuário encontrado.</p>'}
                </div>

                ${!org.isProtected ? `<form method="post" action="/organizacoes/${org.id}/responsavel" class="og-box-card">
                    <input type="hidden" name="csrf_token" value="${esc(csrfToken)}">
                    <label class="og-field">
                        <span>Responsável principal (atalho direto)</span>
                        <select name="primary_user_id">
                            <option value="">Não definido</option>
                            ${members.filter((m) => m.isActive).map((m) => `<option value="${m.id}" ${validPrimary && validPrimary.id === m.id ? 'selected' : ''}>${esc(m.name)} · ${esc(m.email)}${m.isAdmin ? ' · Administrador' : ''}</option>`).join('')}
                            ${validPrimary && !validPrimary.isActive ? `<option value="${validPrimary.id}" selected>${esc(validPrimary.name)} · usuário inativo</option>` : ''}
                        </select>
                    </label>
                    <button type="submit" class="sf-btn sf-btn-secondary og-btn-sm">Salvar responsável</button>
                </form>` : ''}

                <div class="og-history-box">
                    <h4>Últimas alterações</h4>
                    <ul>
                        ${org.history.length ? org.history.map((h) => `<li><strong>${esc(h.date)}</strong> · ${esc(h.actor)}<br>${esc(h.oldLabel)} → ${esc(h.newLabel)}</li>`).join('') : '<li>Nenhuma alteração registrada.</li>'}
                    </ul>
                </div>
            </div>`;
        } else if (ui.drawerTab === 'farms') {
            const qAdd = norm(ui.drawerQuickQuery);
            const qLocal = norm(ui.drawerListQuery).replace(/^#/, '');
            const filteredFarms = qLocal ? farms.filter((f) => norm(`${f.name} ${f.location} ${f.id} ${f.car}`).includes(qLocal)) : farms;
            const candidates = state.farms.filter((f) =>
                f.orgId !== org.id &&
                (!qAdd || norm(`${f.name} ${f.location} ${f.id} ${f.car} ${getOrg(f.orgId)?.name || ''}`).includes(qAdd))
            );

            body = `<div class="og-drawer-stack">
                <div class="og-subhead">
                    <div>
                        <h3>Fazendas vinculadas (${farms.length})</h3>
                        <p>Área ativa: ${fmtArea(activeHa)} ha${org.maxHectares ? ` de ${fmtArea(org.maxHectares)} ha do plano` : ''}.</p>
                    </div>
                    <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-drawer-quick="farm">${ui.drawerQuickAdd === 'farm' ? 'Fechar busca' : '+ Vincular fazenda'}</button>
                </div>

                ${ui.drawerQuickAdd === 'farm' ? `<div class="og-quick-box">
                    <strong>Buscar fazenda existente (com cidade e #ID)</strong>
                    <label class="sl-search">${ICONS.search}<input type="search" data-og-drawer-quick-q value="${esc(ui.drawerQuickQuery)}" placeholder="Nome da fazenda, cidade, CAR ou #ID..."></label>
                    <div class="og-quick-list">
                        ${candidates.length ? candidates.slice(0, 20).map((f) => {
                            const fo = getOrg(f.orgId);
                            return `<div class="og-quick-item">
                                <div><strong>${esc(f.name)} <span>#${f.id}</span></strong><small>${esc(f.location || 'Local não informado')} · ${fmtArea(f.area)} ha · ${fo ? `Em ${esc(fo.name)}` : 'Sem org'}</small></div>
                                <button type="button" class="sf-btn sf-btn-primary og-btn-sm" data-og-start-transfer="farm" data-entity-id="${f.id}" data-target-org="${org.id}">Conferir →</button>
                            </div>`;
                        }).join('') : '<p class="og-muted-sm">Nenhuma fazenda encontrada.</p>'}
                    </div>
                </div>` : ''}

                ${farms.length > 4 ? `<label class="sl-search">${ICONS.search}<input type="search" data-og-drawer-list-q value="${esc(ui.drawerListQuery)}" placeholder="Filtrar nas ${farms.length} fazendas por nome, cidade, #ID ou CAR..."></label>` : ''}

                <div class="og-drawer-list">
                    ${filteredFarms.length ? filteredFarms.map((f) => `<div class="og-drawer-row">
                        <div class="og-cell-main">
                            <div class="og-title-line">
                                <strong>${esc(f.name)}</strong>
                                <span class="og-id">#${f.id}</span>
                                ${isDuplicateFarm(f) ? '<span class="og-badge is-warn">Nome repetido</span>' : ''}
                                ${f.paused ? '<span class="og-badge is-muted">Pausada</span>' : ''}
                            </div>
                            <small>${esc(f.location || 'Local não informado')} · <strong>${fmtArea(f.area)} ha</strong> · ${f.plots} talh${f.plots === 1 ? 'ão' : 'ões'}</small>
                        </div>
                        <button type="button" class="og-menu-btn" aria-haspopup="menu" aria-expanded="false" aria-label="Opções para ${esc(f.name)}" data-og-farm-menu="${f.id}">${ICONS.dots}</button>
                    </div>`).join('') : '<p class="og-drawer-empty">Nenhuma fazenda encontrada.</p>'}
                </div>
            </div>`;
        } else if (ui.drawerTab === 'details') {
            const b = org.billing || {};
            body = `<div class="og-drawer-stack">
                <div class="og-box-card">
                    <h4>Identificação da conta</h4>
                    ${!org.isProtected ? `<form method="post" action="/organizacoes/${org.id}/nome" class="og-inline-form">
                        <input type="hidden" name="csrf_token" value="${esc(csrfToken)}">
                        <input type="text" name="name" value="${esc(org.name)}" maxlength="160" required aria-label="Nome da organização">
                        <button type="submit" class="sf-btn sf-btn-primary og-btn-sm">Salvar nome</button>
                    </form>
                    <div class="og-split-row">
                        <span>Situação da organização</span>
                        <form method="post" action="/organizacoes/${org.id}/status">
                            <input type="hidden" name="csrf_token" value="${esc(csrfToken)}">
                            <input type="hidden" name="is_active" value="${org.isActive ? 'false' : 'true'}">
                            <button type="submit" class="sf-btn sf-btn-secondary og-btn-sm">${org.isActive ? 'Inativar organização' : 'Reativar organização'}</button>
                        </form>
                    </div>` : '<p class="og-muted-sm">A organização padrão SiSFarm é protegida contra renomeação e exclusão.</p>'}
                </div>

                <form method="post" action="/organizacoes/${org.id}/dados-comerciais" class="og-box-card og-billing-grid">
                    <input type="hidden" name="csrf_token" value="${esc(csrfToken)}">
                    <div class="og-span-2 og-split-row">
                        <h4>Dados comerciais e fiscais</h4>
                        ${b.asaasCustomerId ? `<span class="og-badge is-ok">Asaas · ${esc(b.asaasCustomerId)}</span>` : '<span class="og-badge is-muted">Sem cliente Asaas</span>'}
                    </div>
                    <label class="og-field og-span-2"><span>Nome do responsável financeiro</span><input name="contact_name" value="${esc(b.contactName || '')}" maxlength="160" placeholder="Nome completo" required></label>
                    <label class="og-field"><span>E-mail de cobrança</span><input type="email" name="contact_email" value="${esc(b.contactEmail || '')}" maxlength="254" placeholder="financeiro@fazenda.com" required></label>
                    <label class="og-field"><span>Telefone (opcional)</span><input type="tel" name="contact_phone" value="${esc(b.contactPhone || '')}" maxlength="40" placeholder="(11) 99999-0000"></label>
                    <label class="og-field"><span>CPF ou CNPJ</span><input name="cpf_cnpj" value="${esc(b.cpfCnpj || '')}" ${b.asaasCustomerId ? 'readonly' : ''} placeholder="12.345.678/0001-90"></label>
                    <label class="og-field"><span>CEP</span><input name="postal_code" value="${esc(b.postalCode || '')}" placeholder="12345-678"></label>
                    <label class="og-field"><span>Logradouro</span><input name="address" maxlength="255" value="${esc(b.address || '')}" placeholder="Rua das Palmeiras"></label>
                    <label class="og-field"><span>Número</span><input name="address_number" maxlength="40" value="${esc(b.addressNumber || '')}" placeholder="123"></label>
                    <label class="og-field"><span>Complemento</span><input name="address_complement" maxlength="255" value="${esc(b.addressComplement || '')}"></label>
                    <label class="og-field"><span>Bairro</span><input name="province" maxlength="120" value="${esc(b.province || '')}"></label>
                    <label class="og-field"><span>Município</span><input name="city" maxlength="120" value="${esc(b.city || '')}"></label>
                    <label class="og-field"><span>Estado (UF)</span><select name="state"><option value="">Selecione</option>${UF_LIST.map((uf) => `<option value="${uf}" ${b.state === uf ? 'selected' : ''}>${uf}</option>`).join('')}</select></label>
                    <p class="og-muted-sm og-span-2">Município e estado ficam registrados no SiSFarm. No Asaas, a localização é identificada pelo CEP.</p>
                    <div class="og-span-2 og-form-actions">
                        ${!b.asaasCustomerId ? '<button type="submit" name="intent" value="consultar" class="sf-btn sf-btn-secondary og-btn-sm">Salvar e consultar no Asaas</button>' : ''}
                        <button type="submit" name="intent" value="salvar" class="sf-btn sf-btn-primary og-btn-sm">Salvar dados comerciais</button>
                    </div>
                </form>

                ${!org.isProtected ? `<div class="og-danger-box">
                    <div class="og-split-row">
                        <div><strong>Zona de risco</strong><small>Excluir remove permanentemente usuários e fazendas vinculados (histórico comercial preservado).</small></div>
                        ${!ui.drawerConfirmDelete ? `<button type="button" class="sf-btn og-btn-danger og-btn-sm" data-og-ask-delete>Excluir</button>` : ''}
                    </div>
                    ${ui.drawerConfirmDelete ? `<div class="og-confirm-delete" role="alert">
                        <p>Confirma excluir <strong>${esc(org.name)}</strong> (#${org.id})? Esta ação não pode ser desfeita.</p>
                        <div class="og-form-actions">
                            <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-cancel-delete>Cancelar</button>
                            <form method="post" action="/organizacoes/${org.id}/excluir">
                                <input type="hidden" name="csrf_token" value="${esc(csrfToken)}">
                                <button type="submit" class="sf-btn og-btn-danger og-btn-sm">Confirmar exclusão</button>
                            </form>
                        </div>
                    </div>` : ''}
                </div>` : ''}
            </div>`;
        } else if (ui.drawerTab === 'billing') {
            const activeMembers = members.filter((u) => u.isActive).length;
            const activeFarms = farms.filter((f) => !f.paused).length;
            const meterRow = (label, used, max, suffix = '') => {
                const pct = max ? Math.min(100, Math.round((used / max) * 100)) : 0;
                const tone = max ? (used >= max ? 'is-err' : (used / max >= 0.8 ? 'is-warn' : '')) : '';
                return `<div class="og-quota-row">
                    <div class="og-split-row"><span>${label}</span><strong>${fmtArea(used)}${suffix} ${max ? `de ${fmtArea(max)}${suffix}` : '· sem limite'}</strong></div>
                    ${max ? `<span class="og-meter"><i class="${tone}" style="width:${pct}%"></i></span>` : ''}
                </div>`;
            };
            body = `<div class="og-drawer-stack">
                <div class="og-plan-banner">
                    <div><p class="pd-eyebrow">Plano atual</p><strong>${esc(org.planLabel)}</strong></div>
                    <a class="sf-btn sf-btn-primary og-btn-sm" href="/assinaturas?organizacao=${org.id}">Gerenciar assinatura ${ICONS.external}</a>
                </div>
                <div class="og-box-card">
                    <h4>Ocupação das cotas do plano</h4>
                    ${meterRow('Usuários ativos', activeMembers, org.maxUsers)}
                    ${meterRow('Fazendas em operação', activeFarms, org.maxFarms)}
                    ${meterRow('Área cadastrada ativa', activeHa, org.maxHectares, ' ha')}
                </div>
            </div>`;
        }

        drawerEl.innerHTML = `
            <header class="og-drawer-head" style="border-top:4px solid ${acc.bar}">
                <div class="og-cell-main">
                    <div class="og-title-line">
                        <h2 id="og-drawer-title">${esc(org.name)}</h2>
                        <span class="og-id">#${org.id}</span>
                        <span class="og-badge ${org.isActive ? 'is-ok' : 'is-muted'}">${org.isActive ? 'Ativa' : 'Inativa'}</span>
                    </div>
                    <small>${members.length} usuário${members.length === 1 ? '' : 's'} · ${farms.length} fazenda${farms.length === 1 ? '' : 's'} · ${esc(org.planLabel)}</small>
                </div>
                <button type="button" class="og-icon-close" data-og-drawer-close aria-label="Fechar gaveta">${ICONS.close}</button>
            </header>
            <nav class="pd-tabs og-drawer-tabs" role="tablist">
                ${[
                    ['people', `Pessoas (${members.length})`],
                    ['farms', `Fazendas (${farms.length})`],
                    ['details', 'Cadastro'],
                    ['billing', 'Assinatura & Limites'],
                ].map(([k, lbl]) => `<button type="button" role="tab" class="og-tab" aria-selected="${ui.drawerTab === k}" data-og-drawer-tab="${k}">${lbl}</button>`).join('')}
            </nav>
            <div class="og-drawer-body">${body}</div>
        `;
    }

    /* ================= MODAL DE TRANSFERÊNCIA SEGURA (ANTES → DEPOIS) ================= */
    function renderTransferModal() {
        const isUser = ui.transfer.kind === 'user';
        const entity = isUser ? getUser(ui.transfer.entityId) : getFarm(ui.transfer.entityId);
        const fromOrg = entity ? getOrg(entity.orgId) : null;
        const targetOrg = getOrg(ui.transfer.targetOrgId);

        const warnings = [];
        let blockingError = null;

        if (entity && targetOrg) {
            if (entity.orgId === targetOrg.id) {
                blockingError = 'Já está vinculado a esta organização.';
            } else if (isUser) {
                const activeInTarget = orgUsers(targetOrg.id).filter((u) => u.isActive).length;
                if (entity.isActive && targetOrg.maxUsers && !targetOrg.exempt && activeInTarget >= targetOrg.maxUsers) {
                    blockingError = `Limite de usuários do plano de ${targetOrg.name} atingido (${activeInTarget}/${targetOrg.maxUsers}).`;
                }
                if (fromOrg && fromOrg.primaryUserId === entity.id) {
                    warnings.push(`${entity.name} é o responsável principal de ${fromOrg.name}. A conta de origem ficará sem responsável até você escolher outro.`);
                }
                if (entity.activeFarmId) {
                    warnings.push('A fazenda e a safra ativas do usuário serão reiniciadas e os papéis RBAC ajustados para a nova conta.');
                }
            } else {
                const activeFarmsInTarget = orgFarms(targetOrg.id).filter((f) => !f.paused).length;
                const areaInTarget = orgActiveArea(targetOrg.id);
                const sameName = orgFarms(targetOrg.id).find((f) => norm(f.name) === norm(entity.name));
                if (sameName) {
                    blockingError = `${targetOrg.name} já possui uma fazenda chamada "${sameName.name}" (#${sameName.id}). Renomeie antes de transferir.`;
                } else if (!entity.paused && !targetOrg.exempt && targetOrg.maxFarms && activeFarmsInTarget >= targetOrg.maxFarms) {
                    blockingError = `Limite de fazendas do plano de ${targetOrg.name} atingido (${activeFarmsInTarget}/${targetOrg.maxFarms}).`;
                } else if (!entity.paused && !targetOrg.exempt && targetOrg.maxHectares && (areaInTarget + Number(entity.area || 0)) > targetOrg.maxHectares) {
                    blockingError = `A área total excederia o limite de ${targetOrg.name} (${fmtArea(areaInTarget + Number(entity.area || 0))} ha > ${fmtArea(targetOrg.maxHectares)} ha).`;
                }
                if (fromOrg) {
                    warnings.push(`Todo o histórico operacional (talhões, safras, estoque e financeiro) de ${entity.name} passa a ser visível apenas em ${targetOrg.name}.`);
                }
            }
        }

        const qEnt = norm(ui.transfer.entityQuery).replace(/^#/, '');
        const entList = isUser
            ? state.users.filter((u) => u.id !== state.currentUserId && (!qEnt || norm(`${u.name} ${u.email} ${getOrg(u.orgId)?.name || ''}`).includes(qEnt)))
            : state.farms.filter((f) => !qEnt || norm(`${f.name} ${f.location} ${f.id} ${f.car} ${getOrg(f.orgId)?.name || ''}`).includes(qEnt));

        const qOrg = norm(ui.transfer.targetOrgQuery).replace(/^#/, '');
        const orgList = state.orgs.filter((o) => (!entity || o.id !== entity.orgId) && (!qOrg || norm(`${o.name} ${o.id}`).includes(qOrg)));

        const actionUrl = isUser ? '/organizacoes/usuarios/vincular' : '/organizacoes/fazendas/vincular';
        const entityFieldName = isUser ? 'user_id' : 'farm_id';

        transferBody.innerHTML = `
            <header class="og-modal-head">
                <div>
                    <h2 id="og-transfer-title">Transferir vínculo com segurança</h2>
                    <p>Confira origem, destino e impacto nas cotas antes de confirmar.</p>
                </div>
                <button type="button" class="og-icon-close" data-og-transfer-close aria-label="Fechar">${ICONS.close}</button>
            </header>
            <div class="og-modal-scroll og-transfer-steps">
                <div class="og-seg og-seg-full" role="group" aria-label="Tipo de transferência">
                    <button type="button" aria-pressed="${isUser}" data-og-transfer-kind="user">Mover usuário</button>
                    <button type="button" aria-pressed="${!isUser}" data-og-transfer-kind="farm">Transferir fazenda</button>
                </div>

                <div class="og-step-block">
                    <p class="pd-eyebrow">1. ${isUser ? 'Qual usuário?' : 'Qual fazenda?'}</p>
                    ${entity ? `<div class="og-picked is-brand">
                        <div>
                            <strong>${esc(entity.name)}${!isUser ? ` <span>#${entity.id}</span>` : ''}</strong>
                            <small>${isUser ? esc(entity.email) : `${esc(entity.location || 'Local não informado')} · ${fmtArea(entity.area)} ha`} · Origem: <strong>${fromOrg ? esc(fromOrg.name) : 'Sem organização'}</strong></small>
                        </div>
                        <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-transfer-clear="entity">Trocar</button>
                    </div>` : `
                        <label class="sl-search">${ICONS.search}<input type="search" data-og-transfer-q="entity" value="${esc(ui.transfer.entityQuery)}" placeholder="${isUser ? 'Buscar por nome ou e-mail...' : 'Buscar por nome, cidade, CAR ou #ID...'}"></label>
                        <div class="og-pick-list">
                            ${entList.slice(0, 30).map((item) => {
                                const o = getOrg(item.orgId);
                                const acc = o ? orgAccent(o.id) : null;
                                return `<button type="button" class="og-pick-item" data-og-pick-entity="${item.id}">
                                    <span><strong>${esc(item.name)}${!isUser ? ` <em>#${item.id}</em>` : ''}</strong><small>${isUser ? esc(item.email) : `${esc(item.location || 'Local não informado')} · ${fmtArea(item.area)} ha`}</small></span>
                                    ${o ? `<span class="og-org-chip is-sm" style="background:${acc.soft};color:${acc.text}"><span class="og-dot" style="--og-accent:${acc.bar}"></span>${esc(o.name)}</span>` : '<span class="og-badge is-warn">Sem org</span>'}
                                </button>`;
                            }).join('') || '<p class="og-drawer-empty">Nenhum item encontrado.</p>'}
                        </div>
                    `}
                </div>

                <div class="og-step-block">
                    <p class="pd-eyebrow">2. Para qual organização?</p>
                    ${targetOrg ? `<div class="og-picked">
                        <div>
                            <strong><span class="og-dot" style="--og-accent:${orgAccent(targetOrg.id).bar}"></span> ${esc(targetOrg.name)} <span>#${targetOrg.id}</span></strong>
                            <small>${orgUsers(targetOrg.id).length}${targetOrg.maxUsers ? `/${targetOrg.maxUsers}` : ''} usuários · ${orgFarms(targetOrg.id).length}${targetOrg.maxFarms ? `/${targetOrg.maxFarms}` : ''} fazendas · ${esc(targetOrg.planLabel)}</small>
                        </div>
                        <button type="button" class="sf-btn sf-btn-secondary og-btn-sm" data-og-transfer-clear="target">Trocar</button>
                    </div>` : `
                        <label class="sl-search">${ICONS.search}<input type="search" data-og-transfer-q="target" value="${esc(ui.transfer.targetOrgQuery)}" placeholder="Buscar organização destino..."></label>
                        <div class="og-pick-list">
                            ${orgList.slice(0, 30).map((o) => `<button type="button" class="og-pick-item" data-og-pick-target="${o.id}">
                                <span><strong><span class="og-dot" style="--og-accent:${orgAccent(o.id).bar}"></span> ${esc(o.name)} <em>#${o.id}</em></strong><small>${orgUsers(o.id).length}${o.maxUsers ? `/${o.maxUsers}` : ''} usuários · ${orgFarms(o.id).length}${o.maxFarms ? `/${o.maxFarms}` : ''} fazendas</small></span>
                                <span class="og-badge is-${esc(o.planTone)}">${esc(o.planLabel)}</span>
                            </button>`).join('') || '<p class="og-drawer-empty">Nenhuma organização disponível.</p>'}
                        </div>
                    `}
                </div>

                ${entity && targetOrg ? `<div class="og-impact-box ${blockingError ? 'is-err' : 'is-ok'}">
                    <p class="pd-eyebrow">3. Conferir impacto (Antes → Depois)</p>
                    <div class="og-impact-flow">
                        <div><span>Sai de</span><strong>${fromOrg ? esc(fromOrg.name) : 'Sem organização'}</strong></div>
                        <b>→</b>
                        <div class="is-dest"><span>Entra em</span><strong>${esc(targetOrg.name)}</strong></div>
                    </div>
                    ${blockingError ? `<div class="og-impact-err" role="alert">✕ ${esc(blockingError)}</div>` : ''}
                    ${!blockingError && warnings.length ? `<ul class="og-impact-warn">${warnings.map((w) => `<li>⚠ ${esc(w)}</li>`).join('')}</ul>` : ''}
                </div>` : ''}
            </div>
            <footer class="og-modal-foot">
                <button type="button" class="sf-btn sf-btn-secondary" data-og-transfer-close>Cancelar</button>
                <form method="post" action="${actionUrl}">
                    <input type="hidden" name="csrf_token" value="${esc(csrfToken)}">
                    <input type="hidden" name="${entityFieldName}" value="${entity ? entity.id : ''}">
                    <input type="hidden" name="organization_id" value="${targetOrg ? targetOrg.id : ''}">
                    <input type="hidden" name="return_tab" value="${esc(ui.mainTab)}">
                    ${ui.drawerOrgId ? `<input type="hidden" name="return_org" value="${ui.drawerOrgId}">` : ''}
                    <button type="submit" class="sf-btn sf-btn-primary" ${(!entity || !targetOrg || blockingError) ? 'disabled' : ''}>Confirmar transferência</button>
                </form>
            </footer>
        `;
    }

    function openTransfer(kind = 'user', entityId = null, targetOrgId = null) {
        closeMenu();
        ui.transfer = { kind, entityId, entityQuery: '', targetOrgId, targetOrgQuery: '' };
        renderTransferModal();
        if (!transferDialog.open) transferDialog.showModal();
    }

    function openDrawer(orgId, tab = 'people', quickAdd = null) {
        closeMenu();
        ui.drawerOrgId = Number(orgId);
        ui.drawerTab = tab;
        ui.drawerQuickAdd = quickAdd;
        ui.drawerQuickQuery = '';
        ui.drawerListQuery = '';
        ui.drawerConfirmDelete = false;
        renderDrawer();
    }

    function closeDrawer() {
        closeMenu();
        ui.drawerOrgId = null;
        renderDrawer();
    }

    function renderAll() {
        closeMenu();
        root.querySelectorAll('[data-og-main-tab]').forEach((tab) => {
            tab.setAttribute('aria-selected', String(tab.dataset.ogMainTab === ui.mainTab));
        });
        renderTopSummary();
        if (ui.mainTab === 'orgs') workspaceEl.innerHTML = renderOrgsTab();
        else if (ui.mainTab === 'users') workspaceEl.innerHTML = renderUsersTab();
        else workspaceEl.innerHTML = renderFarmsTab();
        renderDrawer();
    }

    /* ================= EVENTOS DELEGADOS ================= */
    root.addEventListener('click', (e) => {
        const el = e.target.closest('[data-og-main-tab],[data-og-kpi-tab],[data-og-org-filter],[data-og-user-filter],[data-og-farm-filter],[data-og-user-view],[data-og-farm-view],[data-og-collapse],[data-og-expand-group],[data-og-clear],[data-og-jump],[data-og-user-menu],[data-og-farm-menu],[data-og-open-drawer],[data-og-open-transfer],[data-og-create-open]');
        if (!el) return;

        if (el.hasAttribute('data-og-create-open')) { createDialog?.showModal(); return; }
        if (el.hasAttribute('data-og-open-transfer')) { openTransfer(ui.mainTab === 'farms' ? 'farm' : 'user', null, null); return; }
        if (el.dataset.ogUserMenu) { e.stopPropagation(); openUserMenu(el, Number(el.dataset.ogUserMenu)); return; }
        if (el.dataset.ogFarmMenu) { e.stopPropagation(); openFarmMenu(el, Number(el.dataset.ogFarmMenu)); return; }
        if (el.dataset.ogMainTab) { ui.mainTab = el.dataset.ogMainTab; renderAll(); return; }
        if (el.dataset.ogKpiTab) { ui.mainTab = el.dataset.ogKpiTab; renderAll(); return; }
        if (el.dataset.ogOrgFilter) { ui.orgFilter = el.dataset.ogOrgFilter; renderAll(); return; }
        if (el.dataset.ogUserFilter) { ui.userFilter = el.dataset.ogUserFilter; renderAll(); return; }
        if (el.dataset.ogFarmFilter) { ui.farmFilter = el.dataset.ogFarmFilter; renderAll(); return; }
        if (el.dataset.ogUserView) { ui.userView = el.dataset.ogUserView; renderAll(); return; }
        if (el.dataset.ogFarmView) { ui.farmView = el.dataset.ogFarmView; renderAll(); return; }
        if (el.dataset.ogCollapse) {
            const [kind, key] = el.dataset.ogCollapse.split(':');
            if (kind === 'users') ui.collapsedUserGroups[key] = !ui.collapsedUserGroups[key];
            else ui.collapsedFarmGroups[key] = !ui.collapsedFarmGroups[key];
            renderAll(); return;
        }
        if (el.dataset.ogExpandGroup) {
            const [kind, key] = el.dataset.ogExpandGroup.split(':');
            if (kind === 'users') ui.expandedUserGroupRows[key] = !ui.expandedUserGroupRows[key];
            else ui.expandedFarmGroupRows[key] = !ui.expandedFarmGroupRows[key];
            renderAll(); return;
        }
        if (el.dataset.ogClear) {
            if (el.dataset.ogClear === 'orgs') { ui.orgFilter = 'all'; ui.orgQuery = ''; }
            if (el.dataset.ogClear === 'users') { ui.userFilter = 'all'; ui.userOrgFilter = ''; ui.userQuery = ''; }
            if (el.dataset.ogClear === 'farms') { ui.farmFilter = 'all'; ui.farmOrgFilter = ''; ui.farmQuery = ''; }
            renderAll(); return;
        }
        if (el.dataset.ogJump) {
            e.stopPropagation();
            if (el.dataset.ogJump === 'dup-farms') { ui.mainTab = 'farms'; ui.farmFilter = 'duplicates'; ui.farmOrgFilter = ''; ui.farmQuery = ''; }
            if (el.dataset.ogJump === 'unassigned-users') { ui.mainTab = 'users'; ui.userFilter = 'unassigned'; ui.userOrgFilter = ''; ui.userQuery = ''; }
            if (el.dataset.ogJump === 'unassigned-farms') { ui.mainTab = 'farms'; ui.farmFilter = 'unassigned'; ui.farmOrgFilter = ''; ui.farmQuery = ''; }
            renderAll(); return;
        }
        if (el.dataset.ogOpenDrawer) {
            e.stopPropagation();
            openDrawer(el.dataset.ogOpenDrawer, el.dataset.drawerTab || 'people', el.dataset.drawerQuick || null);
        }
    });

    root.addEventListener('keydown', (e) => {
        if ((e.key === 'Enter' || e.key === ' ') && e.target.matches('[data-og-kpi-tab]')) {
            e.preventDefault();
            ui.mainTab = e.target.dataset.ogKpiTab;
            renderAll();
        }
    });

    root.addEventListener('input', (e) => {
        const inp = e.target;
        if (inp.dataset.ogSearch === 'orgs') {
            ui.orgQuery = inp.value; renderAll();
            const el = root.querySelector('[data-og-search="orgs"]');
            if (el) { el.focus(); el.setSelectionRange(ui.orgQuery.length, ui.orgQuery.length); }
        } else if (inp.dataset.ogSearch === 'users') {
            ui.userQuery = inp.value; renderAll();
            const el = root.querySelector('[data-og-search="users"]');
            if (el) { el.focus(); el.setSelectionRange(ui.userQuery.length, ui.userQuery.length); }
        } else if (inp.dataset.ogSearch === 'farms') {
            ui.farmQuery = inp.value; renderAll();
            const el = root.querySelector('[data-og-search="farms"]');
            if (el) { el.focus(); el.setSelectionRange(ui.farmQuery.length, ui.farmQuery.length); }
        }
    });

    root.addEventListener('change', (e) => {
        if (e.target.matches('[data-og-user-org]')) { ui.userOrgFilter = e.target.value; renderAll(); }
        if (e.target.matches('[data-og-farm-org]')) { ui.farmOrgFilter = e.target.value; renderAll(); }
    });

    menuEl.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-og-menu-action]');
        if (!btn || btn.disabled) return;
        const act = btn.dataset.ogMenuAction;
        closeMenu();
        if (act === 'unlock-user') submitPost(`/organizacoes/usuarios/${btn.dataset.id}/desbloquear-login`);
        else if (act === 'transfer-user') openTransfer('user', Number(btn.dataset.id), null);
        else if (act === 'transfer-farm') openTransfer('farm', Number(btn.dataset.id), null);
        else if (act === 'make-primary') submitPost(`/organizacoes/${btn.dataset.orgId}/responsavel`, { primary_user_id: btn.dataset.id });
        else if (act === 'open-org') openDrawer(Number(btn.dataset.orgId), btn.dataset.tab || 'people');
        else if (act === 'compare-dups') { closeDrawer(); ui.mainTab = 'farms'; ui.farmFilter = 'duplicates'; ui.farmOrgFilter = ''; ui.farmQuery = ''; renderAll(); }
        else if (act === 'toggle-farm-pause') submitPost(`/organizacoes/fazendas/${btn.dataset.id}/operacoes`, { action: btn.dataset.op, return_tab: ui.mainTab, return_org: ui.drawerOrgId });
    });

    drawerBackdrop.addEventListener('click', (e) => {
        if (e.target === drawerBackdrop) { closeDrawer(); return; }
        const btn = e.target.closest('[data-og-drawer-close],[data-og-drawer-tab],[data-og-drawer-quick],[data-og-start-transfer],[data-og-set-primary],[data-og-user-menu],[data-og-farm-menu],[data-og-ask-delete],[data-og-cancel-delete]');
        if (!btn) return;
        if (btn.hasAttribute('data-og-drawer-close')) { closeDrawer(); return; }
        if (btn.dataset.ogDrawerTab) { ui.drawerTab = btn.dataset.ogDrawerTab; ui.drawerQuickAdd = null; ui.drawerQuickQuery = ''; ui.drawerListQuery = ''; ui.drawerConfirmDelete = false; renderDrawer(); return; }
        if (btn.dataset.ogDrawerQuick) { ui.drawerQuickAdd = ui.drawerQuickAdd === btn.dataset.ogDrawerQuick ? null : btn.dataset.ogDrawerQuick; ui.drawerQuickQuery = ''; renderDrawer(); return; }
        if (btn.dataset.ogStartTransfer) { openTransfer(btn.dataset.ogStartTransfer, Number(btn.dataset.entityId), Number(btn.dataset.targetOrg)); return; }
        if (btn.dataset.ogSetPrimary) { submitPost(`/organizacoes/${btn.dataset.orgId}/responsavel`, { primary_user_id: btn.dataset.ogSetPrimary }); return; }
        if (btn.dataset.ogUserMenu) { e.stopPropagation(); openUserMenu(btn, Number(btn.dataset.ogUserMenu)); return; }
        if (btn.dataset.ogFarmMenu) { e.stopPropagation(); openFarmMenu(btn, Number(btn.dataset.ogFarmMenu)); return; }
        if (btn.hasAttribute('data-og-ask-delete')) { ui.drawerConfirmDelete = true; renderDrawer(); return; }
        if (btn.hasAttribute('data-og-cancel-delete')) { ui.drawerConfirmDelete = false; renderDrawer(); }
    });

    drawerBackdrop.addEventListener('input', (e) => {
        if (e.target.matches('[data-og-drawer-quick-q]')) {
            ui.drawerQuickQuery = e.target.value; renderDrawer();
            const el = drawerEl.querySelector('[data-og-drawer-quick-q]');
            if (el) { el.focus(); el.setSelectionRange(ui.drawerQuickQuery.length, ui.drawerQuickQuery.length); }
        } else if (e.target.matches('[data-og-drawer-list-q]')) {
            ui.drawerListQuery = e.target.value; renderDrawer();
            const el = drawerEl.querySelector('[data-og-drawer-list-q]');
            if (el) { el.focus(); el.setSelectionRange(ui.drawerListQuery.length, ui.drawerListQuery.length); }
        }
    });

    transferDialog.addEventListener('click', (e) => {
        if (e.target === transferDialog) { transferDialog.close(); return; }
        const btn = e.target.closest('[data-og-transfer-close],[data-og-transfer-kind],[data-og-transfer-clear],[data-og-pick-entity],[data-og-pick-target]');
        if (!btn) return;
        if (btn.hasAttribute('data-og-transfer-close')) { transferDialog.close(); return; }
        if (btn.dataset.ogTransferKind) { ui.transfer = { kind: btn.dataset.ogTransferKind, entityId: null, entityQuery: '', targetOrgId: ui.transfer.targetOrgId, targetOrgQuery: '' }; renderTransferModal(); return; }
        if (btn.dataset.ogTransferClear === 'entity') { ui.transfer.entityId = null; renderTransferModal(); return; }
        if (btn.dataset.ogTransferClear === 'target') { ui.transfer.targetOrgId = null; renderTransferModal(); return; }
        if (btn.dataset.ogPickEntity) { ui.transfer.entityId = Number(btn.dataset.ogPickEntity); renderTransferModal(); return; }
        if (btn.dataset.ogPickTarget) { ui.transfer.targetOrgId = Number(btn.dataset.ogPickTarget); renderTransferModal(); }
    });

    transferDialog.addEventListener('input', (e) => {
        if (e.target.dataset.ogTransferQ === 'entity') {
            ui.transfer.entityQuery = e.target.value; renderTransferModal();
            const el = transferDialog.querySelector('[data-og-transfer-q="entity"]');
            if (el) { el.focus(); el.setSelectionRange(ui.transfer.entityQuery.length, ui.transfer.entityQuery.length); }
        } else if (e.target.dataset.ogTransferQ === 'target') {
            ui.transfer.targetOrgQuery = e.target.value; renderTransferModal();
            const el = transferDialog.querySelector('[data-og-transfer-q="target"]');
            if (el) { el.focus(); el.setSelectionRange(ui.transfer.targetOrgQuery.length, ui.transfer.targetOrgQuery.length); }
        }
    });

    createDialog?.addEventListener('click', (e) => {
        if (e.target === createDialog || e.target.closest('[data-og-create-close]')) createDialog.close();
    });

    document.addEventListener('click', (e) => {
        if (!menuEl.hidden && !menuEl.contains(e.target)) closeMenu();
    });
    window.addEventListener('scroll', () => { if (!menuEl.hidden) closeMenu(); }, true);
    window.addEventListener('resize', () => { if (!menuEl.hidden) closeMenu(); });
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            if (!menuEl.hidden) closeMenu();
            else if (ui.drawerOrgId && !transferDialog.open) closeDrawer();
        }
    });

    renderAll();
    if (initialOrgParam && getOrg(initialOrgParam)) {
        openDrawer(initialOrgParam, ui.mainTab === 'farms' ? 'farms' : 'people');
    }
})();
