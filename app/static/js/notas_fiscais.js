/* Notas Fiscais (lista, detalhe, emissão e configurações). Sem dependências. */
(function () {
  'use strict';
  var ICONS = {
    download: 'M12 4v11M7 10l5 5 5-5M5 20h14', code: 'M8 8l-4 4 4 4M16 8l4 4-4 4M13 5l-2 14', send: 'M4 12 20 4l-6 16-3-7z',
    edit: 'M4 20h4L19 9l-4-4L4 16zM13 7l4 4', copy: 'M8 8h11v11H8zM5 16V5h11', x: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM9 9l6 6M15 9l-6 6',
    refresh: 'M20 11a8 8 0 1 0-2.3 5.7M20 5v6h-6', check: 'M5 12l5 5 9-10',
    eye: 'M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12zM12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6z',
    trash: 'M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3', alert: 'M12 3l10 18H2zM12 10v4M12 17h.01',
    cce: 'M6 3h9l3 3v15H6zM9 13l2 2 4-4', ext: 'M14 4h6v6M20 4l-9 9M18 14v6H4V6h6'
  };
  var SVGNS = 'http://www.w3.org/2000/svg';

  function icon(name, size) {
    var svg = document.createElementNS(SVGNS, 'svg');
    svg.setAttribute('width', size || 18); svg.setAttribute('height', size || 18);
    svg.setAttribute('viewBox', '0 0 24 24'); svg.setAttribute('fill', 'none'); svg.setAttribute('stroke', 'currentColor');
    svg.setAttribute('stroke-width', '1.9'); svg.setAttribute('stroke-linecap', 'round'); svg.setAttribute('stroke-linejoin', 'round');
    svg.setAttribute('aria-hidden', 'true');
    var p = document.createElementNS(SVGNS, 'path'); p.setAttribute('d', ICONS[name] || ''); svg.appendChild(p);
    return svg;
  }
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }
  function postForm(action, csrf, label, cls, confirmText) {
    var f = document.createElement('form');
    f.method = 'post'; f.action = action;
    var h = document.createElement('input'); h.type = 'hidden'; h.name = 'csrf_token'; h.value = csrf; f.appendChild(h);
    if (confirmText) f.addEventListener('submit', function (e) { if (!window.confirm(confirmText)) e.preventDefault(); });
    return f;
  }

  /* ---------------------------------------------------------------- lista */
  function initList(root) {
    var data = [];
    try { data = JSON.parse(document.getElementById('nx-data').textContent || '[]'); } catch (e) { data = []; }
    var byId = {}; data.forEach(function (n) { byId[n.id] = n; });
    var csrf = root.getAttribute('data-csrf');
    var canWrite = root.getAttribute('data-can-write') === '1';
    var state = { tab: '', kpi: null, q: '', period: '', buyer: '', value: '', sel: null, menu: null };
    var rows = Array.prototype.slice.call(root.querySelectorAll('[data-nx-id]'));
    var cards = Array.prototype.slice.call(root.querySelectorAll('[data-nx-card]'));
    var panel = root.querySelector('[data-nx-panel]');
    var scrim = root.querySelector('[data-nx-scrim]');
    var menuBox = root.querySelector('[data-nx-menu-box]');
    var list = root.querySelector('[data-nx-list]');
    var today = new Date();
    function iso(d) { return d.toISOString().slice(0, 10); }
    var monthPrefix = iso(today).slice(0, 7);
    var weekAgo = iso(new Date(today.getTime() - 6 * 86400000));

    function matches(elm) {
      var st = elm.getAttribute('data-status');
      if (state.kpi) { if (state.kpi.indexOf(st) < 0) return false; }
      else if (state.tab && st !== state.tab) return false;
      if (state.q && (elm.getAttribute('data-search') || '').indexOf(state.q) < 0) return false;
      var day = elm.getAttribute('data-day') || '';
      if (state.period === 'mes' && day.slice(0, 7) !== monthPrefix) return false;
      if (state.period === '7d' && (!day || day < weekAgo)) return false;
      if (state.buyer && elm.getAttribute('data-buyer') !== state.buyer) return false;
      var v = parseFloat(elm.getAttribute('data-value') || '0');
      if (state.value === 'ate100' && v > 100000) return false;
      if (state.value === 'mais100' && v <= 100000) return false;
      return true;
    }
    function baseMatches(elm) { var keep = { tab: state.tab, kpi: state.kpi }; state.tab = ''; state.kpi = null; var ok = matches(elm); state.tab = keep.tab; state.kpi = keep.kpi; return ok; }

    function render() {
      var visible = 0;
      rows.forEach(function (r) { var ok = matches(r); r.hidden = !ok; if (ok) visible++; r.classList.toggle('is-selected', r.getAttribute('data-nx-id') === state.sel); });
      cards.forEach(function (c) { c.hidden = !matches(c); });
      root.querySelectorAll('[data-nx-tab]').forEach(function (t) {
        var key = t.getAttribute('data-nx-tab');
        t.setAttribute('aria-selected', !state.kpi && state.tab === key ? 'true' : 'false');
        var count = rows.filter(function (r) { return baseMatches(r) && (!key || r.getAttribute('data-status') === key); }).length;
        var span = t.querySelector('[data-nx-tab-count]'); if (span) span.textContent = count;
      });
      root.querySelectorAll('[data-nx-kpi]').forEach(function (k) {
        var on = state.kpi && state.kpi.join(',') === k.getAttribute('data-nx-kpi');
        k.setAttribute('aria-pressed', on ? 'true' : 'false');
        k.querySelector('[data-nx-cta]').textContent = on ? 'Filtrando ✓' : 'Ver notas →';
      });
      var empty = root.querySelector('[data-nx-empty]');
      if (empty && data.length) empty.hidden = visible > 0;
      var footer = root.querySelector('[data-nx-footer]');
      if (footer && data.length) footer.textContent = visible + (visible === 1 ? ' nota' : ' notas') + ' · clique numa linha para ver as ações';
      var chips = [];
      if (state.kpi) chips.push(root.querySelector('[data-nx-kpi="' + state.kpi.join(',') + '"] .nx-kpi-title').textContent);
      if (state.period === 'mes') chips.push('Este mês'); if (state.period === '7d') chips.push('Últimos 7 dias');
      if (state.buyer) chips.push(state.buyer);
      if (state.value === 'ate100') chips.push('Até R$ 100 mil'); if (state.value === 'mais100') chips.push('Acima de R$ 100 mil');
      if (state.q) chips.push('“' + state.q + '”');
      var act = root.querySelector('[data-nx-active]'); var holder = root.querySelector('[data-nx-active-list]');
      holder.textContent = ''; chips.forEach(function (c) { holder.appendChild(el('span', 'nx-active-chip', c)); holder.appendChild(document.createTextNode(' ')); });
      act.hidden = chips.length === 0;
      var fc = [state.period, state.buyer, state.value].filter(Boolean).length;
      var badge = root.querySelector('[data-nx-filter-count]'); badge.hidden = fc === 0; badge.textContent = fc;
      syncChecks();
    }

    function syncChecks() {
      var checked = rows.filter(function (r) { var cb = r.querySelector('[data-nx-check]'); return cb && cb.checked && !r.hidden; });
      var bulk = root.querySelector('[data-nx-bulk]');
      if (bulk) {
        bulk.hidden = checked.length === 0;
        root.querySelector('[data-nx-bulk-label]').textContent = checked.length === 1 ? '1 nota selecionada' : checked.length + ' notas selecionadas';
        var ids = root.querySelector('[data-nx-bulk-ids]'); ids.textContent = '';
        checked.forEach(function (r) { var i = document.createElement('input'); i.type = 'hidden'; i.name = 'ids'; i.value = r.getAttribute('data-nx-id'); ids.appendChild(i); });
      }
      var all = root.querySelector('[data-nx-check-all]');
      var vis = rows.filter(function (r) { return !r.hidden; });
      if (all) all.checked = vis.length > 0 && vis.every(function (r) { return r.querySelector('[data-nx-check]').checked; });
    }

    function actionNode(a, kind) {
      var node;
      if (a.post && canWrite) {
        var f = postForm(a.post, csrf, a.label, '', a.confirm);
        node = el('button'); node.type = 'submit'; f.appendChild(node);
        node._wrap = f;
      } else if (a.href && !a.soon && !a.disabled) {
        node = el('a'); node.href = a.href;
      } else {
        node = el('button'); node.type = 'button'; node.setAttribute('aria-disabled', 'true'); node.disabled = kind === 'menu';
      }
      return node;
    }

    function fillPanel(n) {
      if (!panel || !n) return;
      function q(k) { return panel.querySelector('[data-p="' + k + '"]'); }
      q('kind').textContent = n.kind; q('title').textContent = n.title;
      var chip = q('chip'); chip.className = 'nx-chip is-' + n.tone; chip.textContent = n.label;
      q('buyer').textContent = n.buyer; q('value').textContent = n.value_br; q('product').textContent = n.product;
      q('box').className = 'nx-box is-' + n.box.tone; q('box-title').textContent = n.box.title; q('box-text').textContent = n.box.text;
      var p = q('primary'); p.textContent = '';
      var pa = n.actions.primary; var pn = actionNode(pa, 'panel');
      pn.className = 'sf-btn sf-btn-primary nx-primary'; pn.appendChild(icon(pa.icon, 20)); pn.appendChild(document.createTextNode(pa.label));
      p.appendChild(pn._wrap || pn);
      var tiles = q('tiles'); tiles.textContent = '';
      n.actions.tiles.forEach(function (t) {
        var tn = actionNode(t, 'panel'); tn.className = 'nx-tile';
        var ic = el('span', 'nx-tile-ico'); ic.appendChild(icon(t.icon, 19)); tn.appendChild(ic);
        tn.appendChild(el('span', '', t.label));
        if (t.soon) tn.appendChild(el('span', 'nx-soon', 'Em breve')); else if (t.hint) tn.appendChild(el('small', '', t.hint));
        tiles.appendChild(tn._wrap || tn);
      });
      tiles.hidden = n.actions.tiles.length === 0;
      var d = q('danger'); d.textContent = '';
      if (n.actions.danger) {
        var da = n.actions.danger; var dn = actionNode(da, 'panel'); dn.className = 'nx-danger-btn';
        dn.appendChild(icon(da.icon, 17)); dn.appendChild(document.createTextNode(da.label));
        d.appendChild(dn._wrap || dn); if (da.hint) d.appendChild(el('small', '', '· ' + da.hint));
        d.hidden = false;
      } else { d.hidden = true; }
      q('detail').href = n.detail;
    }

    function select(id, openSheet) {
      state.sel = id; fillPanel(byId[id]); render();
      if (openSheet && window.matchMedia('(max-width: 639px)').matches && panel) { panel.classList.add('is-open'); if (scrim) scrim.hidden = false; }
    }
    function closeSheet() { if (panel) panel.classList.remove('is-open'); if (scrim) scrim.hidden = true; }

    function closeMenu() {
      if (!state.menu) return;
      var btn = root.querySelector('[data-nx-id="' + state.menu + '"] [data-nx-menu]');
      if (btn) btn.setAttribute('aria-expanded', 'false');
      state.menu = null; menuBox.hidden = true; menuBox.textContent = '';
    }
    function openMenu(id, btn) {
      closeMenu();
      var n = byId[id]; if (!n) return;
      state.menu = id; btn.setAttribute('aria-expanded', 'true');
      var head = el('div', 'nx-menu-head'); head.appendChild(el('strong', '', n.title));
      var chip = el('span', 'nx-chip is-' + n.tone, n.label); head.appendChild(chip); menuBox.appendChild(head);
      var items = [n.actions.primary].concat(n.actions.tiles);
      items.forEach(function (a, i) {
        var node = actionNode(a, 'menu'); node.className = 'nx-mi'; node.setAttribute('role', 'menuitem');
        node.appendChild(icon(a.icon)); node.appendChild(el('span', '', a.label));
        if (a.soon) node.appendChild(el('span', 'nx-soon', 'Em breve')); else if (a.hint) node.appendChild(el('em', '', a.hint));
        if (i === 0) node.style.fontWeight = '600';
        menuBox.appendChild(node._wrap || node);
      });
      if (n.actions.danger) {
        menuBox.appendChild(el('div', 'nx-sep'));
        var da = n.actions.danger; var dn = actionNode(da, 'menu'); dn.className = 'nx-mi is-danger'; dn.setAttribute('role', 'menuitem');
        dn.appendChild(icon(da.icon)); dn.appendChild(el('span', '', da.label)); if (da.hint) dn.appendChild(el('em', '', da.hint));
        menuBox.appendChild(dn._wrap || dn);
      }
      menuBox.appendChild(el('div', 'nx-sep'));
      var det = el('a', 'nx-mi'); det.href = n.detail; det.setAttribute('role', 'menuitem'); det.appendChild(icon('ext')); det.appendChild(el('span', '', 'Abrir detalhes e histórico'));
      menuBox.appendChild(det);
      var lr = list.getBoundingClientRect(); var br = btn.getBoundingClientRect();
      menuBox.hidden = false;
      menuBox.style.right = '0.75rem';
      menuBox.style.top = (br.bottom - lr.top + 6) + 'px';
      var first = menuBox.querySelector('a, button:not([disabled])'); if (first) first.focus();
    }

    root.addEventListener('click', function (e) {
      var t = e.target;
      var kpi = t.closest('[data-nx-kpi]');
      if (kpi) { var key = kpi.getAttribute('data-nx-kpi'); state.kpi = state.kpi && state.kpi.join(',') === key ? null : key.split(','); state.tab = ''; closeMenu(); render(); return; }
      var tab = t.closest('[data-nx-tab]');
      if (tab) { state.tab = tab.getAttribute('data-nx-tab'); state.kpi = null; closeMenu(); render(); return; }
      var pill = t.closest('.nx-pill');
      if (pill) {
        var group = pill.parentNode.getAttribute('data-nx-group'); var val = pill.getAttribute('data-value');
        if (group !== 'period' && state[group] === val) val = '';
        state[group] = val;
        pill.parentNode.querySelectorAll('.nx-pill').forEach(function (p) { p.setAttribute('aria-pressed', p.getAttribute('data-value') === val ? 'true' : 'false'); });
        render(); return;
      }
      if (t.closest('[data-nx-filters-toggle]')) {
        var btn = t.closest('[data-nx-filters-toggle]'); var box = document.getElementById('nx-filters');
        box.hidden = !box.hidden; btn.setAttribute('aria-expanded', box.hidden ? 'false' : 'true'); return;
      }
      if (t.closest('[data-nx-clear]')) {
        state = { tab: '', kpi: null, q: '', period: '', buyer: '', value: '', sel: state.sel, menu: null };
        root.querySelector('[data-nx-q]').value = '';
        root.querySelectorAll('[data-nx-group]').forEach(function (g) { g.querySelectorAll('.nx-pill').forEach(function (p) { p.setAttribute('aria-pressed', p.getAttribute('data-value') === '' ? 'true' : 'false'); }); });
        closeMenu(); render(); return;
      }
      var dots = t.closest('[data-nx-menu]');
      if (dots) { var id = dots.closest('[data-nx-id]').getAttribute('data-nx-id'); if (state.menu === id) { closeMenu(); } else { select(id); openMenu(id, dots); } return; }
      if (t.closest('[data-nx-menu-box]')) return;
      if (t.closest('[data-nx-panel-close]') || t.closest('[data-nx-scrim]')) { closeSheet(); return; }
      if (t.closest('.dt-chk') || t.closest('a')) return;
      var row = t.closest('[data-nx-id]');
      if (row) { closeMenu(); select(row.getAttribute('data-nx-id'), false); return; }
      var card = t.closest('[data-nx-card]');
      if (card) { select(card.getAttribute('data-nx-card'), true); return; }
    });
    document.addEventListener('click', function (e) {
      if (state.menu && !e.target.closest('[data-nx-menu-box]') && !e.target.closest('[data-nx-menu]')) closeMenu();
    });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') { closeMenu(); closeSheet(); } });
    root.addEventListener('change', function (e) {
      if (e.target.matches('[data-nx-check]')) syncChecks();
      if (e.target.matches('[data-nx-check-all]')) {
        var on = e.target.checked;
        rows.forEach(function (r) { if (!r.hidden) r.querySelector('[data-nx-check]').checked = on; });
        syncChecks();
      }
    });
    var qi = root.querySelector('[data-nx-q]');
    if (qi) qi.addEventListener('input', function () { state.q = qi.value.trim().toLowerCase(); closeMenu(); render(); });
    if (data.length) select(data[0].id, false); else render();
  }

  /* ---------------------------------------------------------------- menus "Mais ações" e diálogos */
  function initMoreMenus(root) {
    root.querySelectorAll('[data-nx-more]').forEach(function (wrap) {
      var btn = wrap.querySelector('[data-nx-more-btn]'); var menu = wrap.querySelector('.nx-menu');
      function close() { menu.hidden = true; btn.setAttribute('aria-expanded', 'false'); }
      btn.addEventListener('click', function (e) { e.stopPropagation(); var open = menu.hidden; menu.hidden = !open; btn.setAttribute('aria-expanded', open ? 'true' : 'false'); });
      document.addEventListener('click', function (e) { if (!menu.hidden && !wrap.contains(e.target)) close(); });
      document.addEventListener('keydown', function (e) { if (e.key === 'Escape') close(); });
      menu.addEventListener('click', function (e) { if (e.target.closest('[data-nx-dialog-open]')) close(); });
    });
    root.querySelectorAll('[data-nx-dialog-open]').forEach(function (b) {
      b.addEventListener('click', function () { var d = document.getElementById(b.getAttribute('data-nx-dialog-open')); if (d && d.showModal) d.showModal(); });
    });
    root.querySelectorAll('dialog').forEach(function (d) {
      d.addEventListener('click', function (e) { if (e.target === d) d.close(); });
      d.querySelectorAll('[data-nx-dialog-close]').forEach(function (b) { b.addEventListener('click', function () { d.close(); }); });
      if (d.hasAttribute('data-nx-autoopen') && d.showModal) d.showModal();
    });
  }

  function initCounters(root) {
    root.querySelectorAll('[data-nx-min]').forEach(function (input) {
      var min = parseInt(input.getAttribute('data-nx-min'), 10);
      var out = root.querySelector('[data-nx-counter="' + input.id + '"]');
      var target = root.querySelector('[data-nx-enable="' + input.id + '"]');
      function update() {
        var n = input.value.trim().length; var ok = n >= min;
        if (out) { out.textContent = ok ? 'Motivo ok' : 'Mínimo de ' + min + ' caracteres (' + n + '/' + min + ')'; out.classList.toggle('is-ok', ok); }
        if (target) target.disabled = !ok;
      }
      input.addEventListener('input', update); update();
    });
    root.querySelectorAll('[data-nx-len]').forEach(function (input) {
      var out = root.querySelector('[data-nx-len-out="' + input.id + '"]'); var max = input.getAttribute('maxlength');
      function upd() { if (out) out.textContent = input.value.length + '/' + max + ' caracteres'; }
      input.addEventListener('input', upd); upd();
    });
    root.querySelectorAll('[data-nx-gate]').forEach(function (cb) {
      var target = root.querySelector('[data-nx-gated="' + cb.getAttribute('data-nx-gate') + '"]');
      function upd() { if (target) target.disabled = !cb.checked; }
      cb.addEventListener('change', upd); upd();
    });
    root.querySelectorAll('[data-nx-toggle]').forEach(function (btn) {
      var box = document.getElementById(btn.getAttribute('data-nx-toggle'));
      var on = btn.getAttribute('data-label-on'); var off = btn.getAttribute('data-label-off');
      btn.addEventListener('click', function () { box.hidden = !box.hidden; btn.setAttribute('aria-expanded', box.hidden ? 'false' : 'true'); if (on && off) btn.textContent = box.hidden ? off : on; });
    });
  }

  function initConfig(root) {
    root.querySelectorAll('[data-nx-sec]').forEach(function (link) {
      link.addEventListener('click', function (e) {
        var key = link.getAttribute('data-nx-sec');
        var target = root.querySelector('[data-nx-section="' + key + '"]'); if (!target) return;
        e.preventDefault();
        root.querySelectorAll('[data-nx-section]').forEach(function (s) { s.hidden = s !== target; });
        root.querySelectorAll('.nx-sn[data-nx-sec]').forEach(function (s) { if (s.getAttribute('data-nx-sec') === key) s.setAttribute('aria-current', 'page'); else s.removeAttribute('aria-current'); });
        try { history.replaceState(null, '', '?secao=' + key); } catch (err) { /* sem histórico */ }
        var from = root.querySelector('[data-fc-from]'); if (from) from.value = key;
        var openId = link.getAttribute('data-fc-open'); if (openId) { var d = document.getElementById(openId); if (d) { d.open = true; d.scrollIntoView({ block: 'start' }); return; } }
        window.scrollTo({ top: 0 });
        var h = target.querySelector('h2'); if (h) { h.setAttribute('tabindex', '-1'); h.focus({ preventScroll: true }); }
      });
    });
  }

  /* ---------------------------------------------------------------- configurações fiscais: máscara, CEP e prévias */
  function decFormat(n) {
    return n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  function decMask(raw, maxDigits) {
    var d = String(raw || '').replace(/\D/g, '').replace(/^0+(?=\d)/, '').slice(0, maxDigits);
    if (!d) return '';
    return decFormat(parseInt(d, 10) / 100);
  }
  function initFiscalForms(root) {
    /* Campos decimais: digitar 100 vira 1,00. Campos inteiros não usam esta máscara. */
    root.querySelectorAll('[data-fc-mask="dec"]').forEach(function (input) {
      var max = parseInt(input.getAttribute('data-fc-digits') || '8', 10);
      var v = String(input.value || '').trim();
      if (v) {
        var n = parseFloat(v.indexOf(',') >= 0 ? v.replace(/\./g, '').replace(',', '.') : v);
        input.value = isFinite(n) ? decFormat(n) : '';
      }
      input.addEventListener('input', function () {
        input.value = decMask(input.value, max);
        try { input.setSelectionRange(input.value.length, input.value.length); } catch (err) { /* campo sem seleção */ }
      });
    });
    var emit = root.querySelector('[data-fc-emit]');
    if (emit) {
      var docLabels = root.querySelectorAll('[data-fc-doc-label]');
      emit.querySelectorAll('[data-fc-person]').forEach(function (r) {
        r.addEventListener('change', function () { docLabels.forEach(function (l) { l.textContent = r.value === 'cnpj' ? 'CNPJ' : 'CPF'; }); });
      });
      var defaults = {};
      root.querySelectorAll('[data-fc-out]').forEach(function (el) { defaults[el.getAttribute('data-fc-out')] = el.textContent; });
      function mirror(input) {
        var key = input.getAttribute('data-fc-mirror');
        root.querySelectorAll('[data-fc-out="' + key + '"]').forEach(function (el) {
          var v = input.value.trim(); if (key === 'name') v = v.toUpperCase();
          el.textContent = v || (key === 'number' ? 's/n' : defaults[key]);
        });
      }
      emit.querySelectorAll('[data-fc-mirror]').forEach(function (input) { input.addEventListener('input', function () { mirror(input); }); });
      var sn = emit.querySelector('[data-fc-sn]'); var num = emit.querySelector('[name="number"]');
      if (sn && num) sn.addEventListener('change', function () {
        num.readOnly = sn.checked; num.value = sn.checked ? 'S/N' : ''; if (!sn.checked) num.focus(); mirror(num);
      });
      var docIn = emit.querySelector('[data-fc-doc]');
      function personType() { var r = emit.querySelector('[data-fc-person]:checked'); return r ? r.value : 'pf'; }
      function docMask() {
        if (!docIn) return;
        var cnpj = personType() === 'cnpj'; var d = docIn.value.replace(/\D/g, '').slice(0, cnpj ? 14 : 11);
        docIn.value = cnpj
          ? d.replace(/^(\d{2})(\d)/, '$1.$2').replace(/^(\d{2})\.(\d{3})(\d)/, '$1.$2.$3').replace(/\.(\d{3})(\d)/, '.$1/$2').replace(/(\d{4})(\d)/, '$1-$2')
          : d.replace(/(\d{3})(\d)/, '$1.$2').replace(/(\d{3})(\d)/, '$1.$2').replace(/(\d{3})(\d{1,2})$/, '$1-$2');
        docIn.placeholder = cnpj ? '00.000.000/0000-00' : '000.000.000-00';
        mirror(docIn);
      }
      if (docIn) {
        docIn.addEventListener('input', docMask);
        emit.querySelectorAll('[data-fc-person]').forEach(function (r) { r.addEventListener('change', docMask); });
        docMask();
      }
      var btn = emit.querySelector('[data-fc-cep]'); var cep = emit.querySelector('[name="postal_code"]');
      var msg = emit.querySelector('[data-fc-cep-msg]'); var help = emit.querySelector('[data-fc-cep-help]');
      var tag = emit.querySelector('[data-fc-af]'); var ibgeOut = root.querySelector('[data-fc-ibge-out]');
      function say(text) { if (!msg) return; msg.textContent = text || ''; msg.hidden = !text; if (help) help.hidden = !!text; }
      function flash(el) { el.classList.remove('fc-filled'); void el.offsetWidth; el.classList.add('fc-filled'); }
      if (cep) cep.addEventListener('input', function () {
        var d = cep.value.replace(/\D/g, '').slice(0, 8); cep.value = d.length > 5 ? d.slice(0, 5) + '-' + d.slice(5) : d; say('');
      });
      function lookup() {
        var digits = cep.value.replace(/\D/g, '');
        if (digits.length !== 8) { say('O CEP precisa ter 8 números.'); cep.focus(); return; }
        btn.disabled = true; btn.classList.add('is-loading'); say('');
        fetch('/producao/compradores/consulta/cep/' + digits, { credentials: 'same-origin', headers: { Accept: 'application/json' } })
          .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
          .then(function (res) {
            if (!res.ok || !res.j.data) { say((res.j && res.j.error) || 'CEP não encontrado. Preencha o endereço à mão.'); return; }
            var d = res.j.data; var delay = 0;
            cep.value = d.postal_code || cep.value; mirror(cep);
            emit.querySelectorAll('[data-fc-fill]').forEach(function (el) {
              var k = el.getAttribute('data-fc-fill'); if (!d[k]) return;
              setTimeout(function () { el.value = d[k]; flash(el); if (el.hasAttribute('data-fc-mirror')) mirror(el); }, delay); delay += 90;
            });
            if (ibgeOut && d.municipality_code) setTimeout(function () { ibgeOut.textContent = d.municipality_code; flash(ibgeOut); }, delay);
            if (tag) tag.hidden = false;
            setTimeout(function () { if (num && !num.readOnly) num.focus(); }, delay + 120);
          })
          .catch(function () { say('Não foi possível buscar agora. Preencha o endereço à mão.'); })
          .then(function () { btn.disabled = false; btn.classList.remove('is-loading'); });
      }
      if (btn && cep) {
        btn.addEventListener('click', lookup);
        cep.addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); lookup(); } });
      }
      var ib = root.querySelector('[data-fc-ibge]');
      if (ib) {
        var ibBtn = ib.querySelector('[data-fc-ibge-btn]'); var view = ib.querySelector('[data-fc-ibge-view]');
        var edit = ib.querySelector('[data-fc-ibge-edit]'); var ibIn = ib.querySelector('[data-fc-ibge-in]');
        ibIn.addEventListener('input', function () { ibIn.value = ibIn.value.replace(/\D/g, '').slice(0, 7); });
        ibBtn.addEventListener('click', function () {
          var open = edit.hidden; edit.hidden = !open; view.hidden = open;
          ibBtn.textContent = open ? 'Pronto' : 'Corrigir'; ibBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
          if (open) ibIn.focus(); else if (ibgeOut) ibgeOut.textContent = ibIn.value || 'preenchido ao salvar';
        });
      }
    }
    root.querySelectorAll('[data-fc-drop]').forEach(function (zone) {
      var input = zone.querySelector('[data-fc-file]'); var name = zone.querySelector('[data-fc-drop-name]');
      var hint = zone.querySelector('[data-fc-drop-hint]'); var prev = zone.querySelector('[data-fc-drop-preview]');
      var form = zone.closest('form'); var go = form && form.querySelector('[data-fc-upload-go]');
      var original = { name: name.textContent, hint: hint.textContent, prev: prev ? prev.innerHTML : '' };
      function show() {
        var f = input.files && input.files[0];
        zone.classList.toggle('has-file', !!f);
        if (!f) { name.textContent = original.name; hint.textContent = original.hint; if (prev) prev.innerHTML = original.prev; if (go) go.hidden = true; return; }
        name.textContent = f.name; hint.textContent = (f.size / 1024).toLocaleString('pt-BR', { maximumFractionDigits: 0 }) + ' KB · pronto para enviar';
        if (prev && /^image\//.test(f.type)) { var img = document.createElement('img'); img.alt = ''; img.src = URL.createObjectURL(f); prev.innerHTML = ''; prev.appendChild(img); }
        if (go) go.hidden = false;
      }
      input.addEventListener('change', show);
      ['dragenter', 'dragover'].forEach(function (t) { zone.addEventListener(t, function (e) { e.preventDefault(); zone.classList.add('is-over'); }); });
      ['dragleave', 'dragend'].forEach(function (t) { zone.addEventListener(t, function (e) { if (!zone.contains(e.relatedTarget)) zone.classList.remove('is-over'); }); });
      zone.addEventListener('drop', function (e) {
        e.preventDefault(); zone.classList.remove('is-over');
        var files = e.dataTransfer && e.dataTransfer.files; if (!files || !files.length) return;
        try { var dt = new DataTransfer(); dt.items.add(files[0]); input.files = dt.files; } catch (err) { input.files = files; }
        show();
      });
      var cancel = form && form.querySelector('[data-fc-upload-cancel]');
      if (cancel) cancel.addEventListener('click', function () { input.value = ''; show(); });
    });
    root.querySelectorAll('[data-fc-eye]').forEach(function (b) {
      var input = document.getElementById(b.getAttribute('data-fc-eye'));
      b.addEventListener('click', function () {
        var show = input.type === 'password'; input.type = show ? 'text' : 'password';
        b.setAttribute('aria-pressed', show ? 'true' : 'false'); b.setAttribute('aria-label', show ? 'Esconder senha' : 'Mostrar senha');
        var slash = b.querySelector('[data-fc-eye-slash]'); if (slash) slash.style.display = show ? 'none' : '';
      });
    });
    var prod = root.querySelector('[data-fc-product]');
    if (prod) {
      var ncm = prod.querySelector('[data-fc-ncm]');
      var ncmMsg = prod.querySelector('[data-fc-ncm-msg]');
      function ncmMask() {
        if (!ncm) return;
        var d = ncm.value.replace(/\D/g, '').slice(0, 8);
        ncm.value = d.replace(/^(\d{4})(\d)/, '$1.$2').replace(/^(\d{4})\.(\d{2})(\d)/, '$1.$2.$3');
        var ok = d.length === 8; ncm.classList.toggle('is-missing', !ok);
        if (ncmMsg) { ncmMsg.textContent = ok ? '' : (d ? 'O NCM tem 8 números.' : 'Falta o NCM. Sem ele a nota não pode ser emitida.'); ncmMsg.classList.toggle('fc-warn', !ok); }
      }
      if (ncm) { ncm.addEventListener('input', ncmMask); if (ncm.value) ncmMask(); }
      prod.querySelectorAll('[data-fc-ncm-pick]').forEach(function (b) {
        b.addEventListener('click', function () { if (ncm) { ncm.value = b.getAttribute('data-fc-ncm-pick'); ncmMask(); ncm.focus(); } });
      });
      var conv = prod.querySelector('[data-fc-conv]');
      function convText() {
        if (!conv) return;
        var sale = prod.querySelector('[data-fc-unit="sale"]').value; var tax = prod.querySelector('[data-fc-unit="tax"]').value;
        var raw = prod.querySelector('[data-fc-unit="kg"]').value.replace(/\./g, '').replace(',', '.'); var kg = parseFloat(raw);
        if (sale === tax || !isFinite(kg) || kg <= 0) { conv.textContent = ''; return; }
        var fmt = function (n) { return n.toLocaleString('pt-BR', { maximumFractionDigits: 2 }); };
        conv.textContent = sale === 'SC'
          ? 'Exemplo: uma venda de 100 sacas sai na nota como ' + fmt(100 * kg) + ' kg.'
          : 'Exemplo: uma venda de 6.000 kg sai na nota como ' + fmt(6000 / kg) + ' sacas.';
      }
      prod.addEventListener('input', convText); prod.addEventListener('change', convText); convText();
    }
    var numbering = root.querySelector('[data-fc-numbering]');
    if (numbering) {
      var series = numbering.querySelector('[data-fc-series]'); var next = numbering.querySelector('[data-fc-next]'); var outN = numbering.querySelector('[data-fc-nxt-out]');
      var upd = function () {
        var n = parseInt(next.value, 10);
        outN.textContent = 'Série ' + (series.value || '—') + ' · Nº ' + (isFinite(n) ? n.toLocaleString('pt-BR') : '—');
      };
      series.addEventListener('input', upd); next.addEventListener('input', upd);
    }
  }

  /* ---------------------------------------------------------------- emitir NF: modal do comprador, transporte e carga */
  function initEmit(root) {
    root.querySelectorAll('dialog[data-nx-reset-on-cancel]').forEach(function (d) {
      var saving = false;
      d.addEventListener('submit', function () { saving = true; });
      d.querySelectorAll('[type="submit"]').forEach(function (b) { b.addEventListener('click', function () { saving = true; }); });
      d.addEventListener('close', function () {
        if (saving) return;
        d.querySelectorAll('input, select, textarea').forEach(function (el) {
          if (el.tagName === 'SELECT') { Array.prototype.forEach.call(el.options, function (o) { o.selected = o.defaultSelected; }); }
          else if (el.type !== 'hidden') { el.value = el.defaultValue; }
        });
      });
    });
    root.querySelectorAll('[role="button"][data-nx-dialog-open]').forEach(function (el) {
      el.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); el.click(); } });
    });
    root.querySelectorAll('[data-nx-goto]').forEach(function (b) {
      b.addEventListener('click', function () {
        var t = document.getElementById(b.getAttribute('data-nx-goto')); if (!t) return;
        t.scrollIntoView({ behavior: 'smooth', block: 'center' }); var i = t.querySelector('input'); if (i) i.focus({ preventScroll: true });
      });
    });
    var ship = root.querySelector('[data-nx-ship]');
    if (!ship) return;
    function freight() {
      var on = ship.querySelector('[data-nx-freight]:checked'); var k = on ? on.value : '';
      ship.querySelectorAll('[data-nx-show]').forEach(function (box) { box.hidden = box.getAttribute('data-nx-show').split(' ').indexOf(k) < 0; });
    }
    ship.querySelectorAll('[data-nx-freight]').forEach(function (r) { r.addEventListener('change', freight); });
    var sp = ship.querySelector('[data-nx-species]'); var other = ship.querySelector('[data-nx-species-other]');
    var autos = ship.querySelectorAll('[data-nx-auto]'); var restore = ship.querySelector('[data-nx-restore]');
    function num(v) { var r = String(v || '').replace(/\s/g, ''); if (r.indexOf(',') >= 0) r = r.replace(/\./g, '').replace(',', '.'); var n = parseFloat(r); return isFinite(n) ? n : null; }
    function fmt(n) { return n === null ? '—' : n.toLocaleString('pt-BR', { maximumFractionDigits: 3 }); }
    function sync() {
      if (other) other.hidden = !(sp && sp.value === '__outra');
      var q = ship.querySelector('[name="volume_quantity"]'); var otherIn = ship.querySelector('[name="volume_species_other"]');
      var spName = sp ? (sp.value === '__outra' ? (otherIn && otherIn.value) || 'volumes' : sp.value) : '';
      var set = function (k, t) { var el = ship.querySelector('[data-nx-sum="' + k + '"]'); if (el) el.textContent = t; };
      set('qty', ((q && q.value) || '—') + ' ' + spName.toLowerCase());
      set('net', fmt(num(ship.querySelector('[name="net_weight_kg"]').value)) + ' kg líquido');
      set('gross', fmt(num(ship.querySelector('[name="gross_weight_kg"]').value)) + ' kg bruto');
      var changed = false;
      autos.forEach(function (el) { var a = el.getAttribute('data-nx-auto'); if (a && el.value !== a) changed = true; });
      if (restore) restore.hidden = !changed;
    }
    ship.addEventListener('input', sync); ship.addEventListener('change', sync);
    if (restore) restore.addEventListener('click', function () {
      autos.forEach(function (el) { var a = el.getAttribute('data-nx-auto'); if (a) el.value = a; });
      sync();
    });
    freight(); sync();
  }

  /* ---------------------------------------------------------------- operação fiscal: abas e exemplo de tributos */
  function initOperationEditor(root) {
    var tabs = root.querySelectorAll('[data-nx-optab]');
    function show(key) {
      tabs.forEach(function (t) { t.setAttribute('aria-selected', t.getAttribute('data-nx-optab') === key ? 'true' : 'false'); });
      root.querySelectorAll('[data-nx-oppanel]').forEach(function (p) { p.hidden = p.getAttribute('data-nx-oppanel') !== key; });
    }
    tabs.forEach(function (t, i) {
      t.addEventListener('click', function () { show(t.getAttribute('data-nx-optab')); });
      t.addEventListener('keydown', function (e) {
        if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
        var next = tabs[(i + (e.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length];
        next.focus(); show(next.getAttribute('data-nx-optab'));
      });
    });
    var form = tabs.length ? tabs[0].closest('form') : null;
    if (form) form.addEventListener('invalid', function (e) {
      var panel = e.target.closest('[data-nx-oppanel]'); if (panel && panel.hidden) show(panel.getAttribute('data-nx-oppanel'));
    }, true);
    var calc = root.querySelector('[data-nx-taxcalc]');
    if (!calc) return;
    var GROSS = 156000;
    function field(k) { return calc.querySelector('[data-nx-tax="' + k + '"]'); }
    function out(k, v) { var el = calc.querySelector('[data-nx-out="' + k + '"]'); if (el) el.textContent = v; }
    function rate(k) { var el = field(k); var raw = String(el && el.value || '').replace(/[%\s]/g, ''); if (raw.indexOf(',') >= 0) raw = raw.replace(/\./g, '').replace(',', '.'); var n = parseFloat(raw); return isFinite(n) && n >= 0 ? n : 0; }
    function has(k) { var el = field(k); return !!(el && el.value.trim()); }
    function brl(n) { return 'R$ ' + n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
    function pct(n) { return n.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 4 }) + '%'; }
    var labels = { '40': 'isento', '41': 'não tributada', '50': 'suspenso', '60': 'cobrado antes (ST)', '90': 'outras situações' };
    function update() {
      var cst = field('cst').value; var icms = field('icms'); var on = cst === '00';
      icms.disabled = !on; icms.classList.toggle('is-off', !on);
      var help = calc.querySelector('[data-nx-tax-help]');
      if (help) help.textContent = on ? 'Usada para calcular base e valor em cada lote' : 'Não se aplica nesta situação';
      out('icms', !cst ? 'escolha a situação' : on ? brl(GROSS * rate('icms') / 100) + ' (' + pct(rate('icms')) + ')' : 'R$ 0,00 · ' + (labels[cst] || ''));
      var any = has('cbs') || has('ibsuf') || has('ibsmun');
      out('cbsl', 'CBS' + (any ? ' (' + pct(rate('cbs')) + ')' : ''));
      out('ibsl', 'IBS' + (any ? ' (' + pct(rate('ibsuf') + rate('ibsmun')) + ')' : ''));
      out('cbs', any ? brl(GROSS * rate('cbs') / 100) : 'sem alíquota');
      out('ibs', any ? brl(GROSS * (rate('ibsuf') + rate('ibsmun')) / 100) : 'sem alíquota');
      var fun = calc.querySelector('[data-nx-tax="fun"]:checked') || field('fun');
      var funText = !fun ? '—' : fun.options ? fun.options[fun.selectedIndex].text : ((fun.closest('label') || fun).querySelector('b') || {}).textContent || '';
      out('fun', funText.toLowerCase());
    }
    calc.addEventListener('input', update); calc.addEventListener('change', update); update();
  }

  document.addEventListener('DOMContentLoaded', function () {
    var root = document.querySelector('[data-nx-page]');
    if (!root) return;
    var kind = root.getAttribute('data-nx-page');
    if (kind === 'list') initList(root);
    initMoreMenus(root);
    root.querySelectorAll('[data-nx-tl-all]').forEach(function (b) {
      b.addEventListener('click', function () {
        var all = root.querySelectorAll('.nx-tl2-more'); var open = Array.prototype.some.call(all, function (d) { return !d.open; });
        all.forEach(function (d) { d.open = open; });
        b.textContent = b.getAttribute(open ? 'data-label-on' : 'data-label-off');
      });
    });
    initCounters(root);
    if (kind === 'config') { initConfig(root); initFiscalForms(root); initOperationEditor(root); }
    if (kind === 'emit') initEmit(root);
  });
})();
