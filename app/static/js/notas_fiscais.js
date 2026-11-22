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
        var h = target.querySelector('h2'); if (h) { h.setAttribute('tabindex', '-1'); h.focus({ preventScroll: true }); }
      });
    });
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
      var fun = field('fun'); out('fun', fun ? fun.options[fun.selectedIndex].text.toLowerCase() : '—');
    }
    calc.addEventListener('input', update); calc.addEventListener('change', update); update();
  }

  document.addEventListener('DOMContentLoaded', function () {
    var root = document.querySelector('[data-nx-page]');
    if (!root) return;
    var kind = root.getAttribute('data-nx-page');
    if (kind === 'list') initList(root);
    initMoreMenus(root);
    initCounters(root);
    if (kind === 'config') { initConfig(root); initOperationEditor(root); }
  });
})();
