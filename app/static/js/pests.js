/* Pragas (/pragas): filtros, painel do foco e gavetas (registrar, corrigir, controle, reinspeção, ronda). */
(function () {
  'use strict';
  var page = document.querySelector('[data-pests-page]');
  if (!page) return;
  var data = {};
  var openInfo = {};
  try { data = JSON.parse(document.getElementById('pz-data').textContent || '{}'); } catch (e) { data = {}; }
  try { openInfo = JSON.parse(document.getElementById('pz-open').textContent || '{}'); } catch (e) { openInfo = {}; }
  var panels = data.panels || {};
  var canWrite = page.dataset.canWrite === '1';

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $$(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function norm(v) { return String(v || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[-/]/g, ' ').replace(/\s+/g, ' ').trim(); }
  function fmt(n, d) { return Number(n).toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d }); }
  function pct(n) { return fmt(n, 1) + '%'; }
  function intVal(el) { var v = parseInt(String(el.value || '').replace(/\D/g, ''), 10); return isNaN(v) ? null : v; }
  function decVal(el) { var v = parseFloat(String(el.value || '').replace(/\./g, '').replace(',', '.')); return isNaN(v) ? null : v; }
  function parseISO(s) { var p = String(s || '').split('-').map(Number); return p.length === 3 ? new Date(p[0], p[1] - 1, p[2]) : null; }
  function addDays(d, n) { return new Date(d.getFullYear(), d.getMonth(), d.getDate() + n); }
  function ddmm(d) { return String(d.getDate()).padStart(2, '0') + '/' + String(d.getMonth() + 1).padStart(2, '0'); }
  function ddmmyyyy(d) { return ddmm(d) + '/' + d.getFullYear(); }
  function onlyDigits(el) { el.addEventListener('input', function () { var c = el.value.replace(/\D/g, ''); if (c !== el.value) el.value = c; }); }
  var isWide = function () { return window.matchMedia('(min-width: 1400px)').matches; };

  /* ---------- Menus suspensos: fecham ao clicar fora e com Esc ---------- */
  function closeMenus(except) {
    $$('[data-pz-menu]').forEach(function (wrap) {
      if (wrap === except) return;
      var m = $('.pz-menu', wrap); var b = $('[data-pz-menu-btn]', wrap);
      if (m) m.hidden = true; if (b) b.setAttribute('aria-expanded', 'false');
    });
  }
  document.addEventListener('click', function (ev) {
    var btn = ev.target.closest('[data-pz-menu-btn]');
    if (btn) {
      var wrap = btn.closest('[data-pz-menu]'); var menu = $('.pz-menu', wrap);
      var willOpen = menu.hidden;
      closeMenus(wrap);
      menu.hidden = !willOpen; btn.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
      return;
    }
    if (!ev.target.closest('.pz-menu')) closeMenus(null);
  });

  /* ---------- Aviso de clima ---------- */
  var alertBox = $('[data-pz-alert]');
  if (alertBox) {
    try { if (sessionStorage.getItem('pz-alert-off') === '1') alertBox.hidden = true; } catch (e) { /* sem storage */ }
    $('[data-pz-alert-close]', alertBox).addEventListener('click', function () {
      alertBox.hidden = true;
      try { sessionStorage.setItem('pz-alert-off', '1'); } catch (e) { /* sem storage */ }
    });
  }

  /* ---------- Filtros da lista ---------- */
  var rows = $$('[data-pz-row]');
  var filt = { status: null, plot: '', pest: '', q: '' };
  var segLabels = {};
  $$('[data-pz-seg]').forEach(function (b) { segLabels[b.dataset.pzSeg] = $('.pz-seg-l', b).textContent.trim(); });
  var qInput = $('[data-pz-q]'), plotSel = $('[data-pz-plot-f]'), pestSel = $('[data-pz-pest-f]');

  function applyFilters() {
    var q = norm(filt.q), shown = 0;
    rows.forEach(function (r) {
      var st = r.dataset.status;
      var okStatus = filt.status ? st === filt.status : st !== 'controlado';
      var ok = okStatus && (!filt.plot || r.dataset.plot === filt.plot) && (!filt.pest || r.dataset.pest === filt.pest) && (!q || r.dataset.search.indexOf(q) !== -1);
      r.hidden = !ok; if (ok) shown++;
    });
    var nores = $('[data-pz-nores]'); if (nores) nores.hidden = shown > 0 || !rows.length;
    var count = $('[data-pz-count]');
    if (count) count.textContent = shown + ' de ' + rows.length + ' focos' + (filt.status ? '' : ' · mostrando os ativos');
    $$('[data-pz-seg]').forEach(function (b) { b.setAttribute('aria-pressed', b.dataset.pzSeg === filt.status ? 'true' : 'false'); });
    var bits = [];
    if (filt.status) bits.push(segLabels[filt.status]);
    if (filt.plot && plotSel) bits.push(plotSel.options[plotSel.selectedIndex].text);
    if (filt.pest) bits.push(filt.pest);
    if (filt.q) bits.push('“' + filt.q + '”');
    var bar = $('[data-pz-filterbar]');
    if (bar) { bar.hidden = !bits.length; $('[data-pz-filter-txt]', bar).textContent = bits.join(' · '); }
  }
  function clearFilters() {
    filt = { status: null, plot: '', pest: '', q: '' };
    if (qInput) qInput.value = ''; if (plotSel) plotSel.value = ''; if (pestSel) pestSel.value = '';
    applyFilters();
  }
  $$('[data-pz-seg]').forEach(function (b) {
    b.addEventListener('click', function () { filt.status = filt.status === b.dataset.pzSeg ? null : b.dataset.pzSeg; setView('lista'); applyFilters(); });
  });
  if (qInput) qInput.addEventListener('input', function () { filt.q = qInput.value.trim(); applyFilters(); });
  if (plotSel) plotSel.addEventListener('change', function () { filt.plot = plotSel.value; applyFilters(); });
  if (pestSel) pestSel.addEventListener('change', function () { filt.pest = pestSel.value; applyFilters(); });
  $$('[data-pz-clear]').forEach(function (b) { b.addEventListener('click', clearFilters); });

  function setView(v) {
    $$('[data-pz-view]').forEach(function (b) { var on = b.dataset.pzView === v; b.classList.toggle('is-on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); });
    $$('[data-pz-view-pane]').forEach(function (p) { p.hidden = p.dataset.pzViewPane !== v; });
  }
  $$('[data-pz-view]').forEach(function (b) { b.addEventListener('click', function () { setView(b.dataset.pzView); }); });
  $$('[data-pz-tile]').forEach(function (t) {
    t.addEventListener('click', function () { filt.plot = t.dataset.pzTile; filt.status = null; if (plotSel) plotSel.value = filt.plot; setView('lista'); applyFilters(); });
  });
  applyFilters();

  /* ---------- Painel do foco ---------- */
  var panel = $('[data-pz-panel]'), panelBody = $('[data-pz-panel-body]'), panelEmpty = $('[data-pz-panel-empty]');
  var panelScrim = $('[data-pz-panel-close]');
  var currentId = null;
  var ICON_X = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>';
  var ICON_MORE = '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="5" cy="12" r="1.8"/><circle cx="12" cy="12" r="1.8"/><circle cx="19" cy="12" r="1.8"/></svg>';
  var ACTION_LABEL = { controle: 'Planejar controle', reinspecao: 'Reinspecionar', reabrir: 'Reabrir foco' };
  var TONE = { err: ['#FDECEA', '#B42318'], warn: ['#FFF6E5', '#8A5A00'], info: ['#E8F1FB', '#1F5FA8'], ok: ['#E6F4E2', '#1F7A3A'] };

  function renderPanel(p) {
    var t = TONE[p.next.tone] || TONE.info;
    var h = '<div class="pz-ph"><div class="pz-grow1"><span class="pz-small pz-muted">' + esc(p.category) + ' · ' + esc(p.plot) + (p.variety ? ' · ' + esc(p.variety) : '') + '</span>' +
      '<h2>' + esc(p.name) + '</h2><span class="pz-badge" style="background:' + p.bg + ';color:' + p.fg + '"><span class="pz-dot" style="background:' + p.fg + '"></span>' + esc(p.status_label) + '</span></div>' +
      '<div class="pz-ph-btns">' +
      (canWrite ? '<div class="pz-menu-wrap" data-pz-menu><button type="button" class="pz-btn pz-btn-ghost pz-btn-ic" aria-label="Mais ações do foco" aria-haspopup="menu" aria-expanded="false" data-pz-menu-btn>' + ICON_MORE + '</button>' +
        '<div class="pz-menu" role="menu" hidden style="min-width:230px"><button type="button" role="menuitem" data-pz-action="editar" data-id="' + p.id + '">Corrigir registro</button>' +
        '<button type="button" role="menuitem" disabled>Enviar ao agrônomo <span class="pz-soon">Em breve</span></button>' +
        '<button type="button" role="menuitem" class="is-danger" data-pz-action="excluir" data-id="' + p.id + '">Excluir foco</button></div></div>' : '') +
      '<button type="button" class="pz-btn pz-btn-ghost pz-btn-ic" aria-label="Fechar detalhes" data-pz-panel-x>' + ICON_X + '</button></div></div>';
    h += '<div class="pz-next" style="background:' + t[0] + '"><div class="pz-next-k" style="color:' + t[1] + '">' + esc(p.next.kicker) + '</div><p>' + esc(p.next.text) + '</p>';
    if (canWrite) {
      h += '<div class="pz-row-btns" style="justify-content:flex-start"><button type="button" class="pz-btn pz-btn-pri" data-pz-action="' + p.next.primary + '" data-id="' + p.id + '">' + ACTION_LABEL[p.next.primary] + '</button>';
      if (p.next.secondary) h += '<button type="button" class="pz-btn pz-btn-sec" data-pz-action="' + p.next.secondary + '" data-id="' + p.id + '">' + (p.next.secondary === 'controle' && p.status !== 'precisa' ? 'Novo controle' : ACTION_LABEL[p.next.secondary]) + '</button>';
      h += '</div>';
    }
    h += '</div>';
    if (p.bars && p.bars.length) {
      var label = 'Incidência ao longo do tempo: ' + p.bars.map(function (b) { return b.label + ' ' + b.value; }).join(', ') + '. Nível de controle ' + p.threshold_txt + '.';
      h += '<div class="pz-sec"><h3 class="pz-h3">Evolução da incidência</h3><div class="pz-between pz-baseline" style="margin-bottom:8px"><span><strong style="font-size:22px">' + esc(p.incidence_txt) + '</strong> <span class="pz-muted">' + esc(p.unit) + '</span></span><span class="pz-small pz-muted">- - nível de controle ' + esc(p.threshold_txt) + '</span></div>' +
        '<div class="pz-chart" role="img" aria-label="' + esc(label) + '"><div class="pz-chart-nc" style="bottom:' + p.threshold_bottom + 'px"></div>' +
        p.bars.map(function (b) { return '<div class="pz-chart-c" title="' + esc(b.label + ': ' + b.value) + '"><span>' + esc(b.value) + '</span><i style="height:' + b.height + 'px;background:' + (b.above ? '#B42318' : '#9AA59E') + '"></i></div>'; }).join('') +
        '</div><div class="pz-chart-x">' + p.bars.map(function (b) { return '<span>' + esc(b.label) + '</span>'; }).join('') + '</div></div>';
    } else {
      h += '<div class="pz-sec"><h3 class="pz-h3">Evolução da incidência</h3><p class="pz-small pz-muted" style="margin:0">' + esc(p.incidence_txt) + '. Faça uma reinspeção com contagem para acompanhar a evolução.</p></div>';
    }
    if (p.withdrawal_until) {
      h += '<div class="pz-sec" style="display:flex;gap:10px;align-items:flex-start"><svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#1F5FA8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" style="flex:none"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>' +
        '<div><strong>Carência até ' + esc(p.withdrawal_until) + '</strong><div class="pz-small pz-muted">Evite colher este talhão até essa data.</div></div></div>';
    }
    h += '<div class="pz-sec"><h3 class="pz-h3">Histórico</h3><div>' + (p.events || []).map(function (e) {
      return '<div class="pz-tl-i"><span class="pz-tl-dot" style="background:' + e.color + '"></span><div><div class="pz-between"><strong>' + esc(e.title) + '</strong><span class="pz-small pz-muted">' + esc(e.label) + '</span></div>' +
        (e.text ? '<div class="pz-small pz-muted">' + esc(e.text) + '</div>' : '') +
        (e.op_id ? '<a class="pz-linkb" href="/pragas/pulverizacoes?edit_id=' + e.op_id + '">Ver pulverização</a>' : '') + '</div></div>';
    }).join('') + '</div></div>';
    h += '<div class="pz-sec"><h3 class="pz-h3">Foto e detalhes</h3>' +
      (p.has_photo ? '<img class="pz-photo" src="/pragas/' + p.id + '/foto" alt="Foto de ' + esc(p.name) + ' no ' + esc(p.plot) + '" loading="lazy">' : '') +
      '<dl class="pz-kv pz-small"><dt>Onde</dt><dd>' + esc(p.location) + '</dd><dt>Visto em</dt><dd>' + esc(p.since) + '</dd><dt>Registrado por</dt><dd>' + esc(p.by) + '</dd>' +
      (p.notes ? '<dt>Observação</dt><dd style="white-space:pre-line">' + esc(p.notes) + '</dd>' : '') + '</dl></div>';
    return h;
  }

  function selectIncident(id, opts) {
    var p = panels[String(id)];
    if (!p || !panel) return;
    currentId = String(id);
    panelBody.innerHTML = renderPanel(p);
    panelBody.hidden = false; panelEmpty.hidden = true;
    rows.forEach(function (r) { r.classList.toggle('is-sel', r.dataset.pzRow === currentId); });
    if (!isWide() && !(opts && opts.silent)) { panel.classList.add('is-open'); panelScrim.hidden = false; }
    var x = $('[data-pz-panel-x]', panelBody); if (x) x.addEventListener('click', closePanel);
  }
  function closePanel() {
    if (!panel) return;
    panel.classList.remove('is-open'); if (panelScrim) panelScrim.hidden = true;
    if (isWide()) { panelBody.hidden = true; panelEmpty.hidden = false; rows.forEach(function (r) { r.classList.remove('is-sel'); }); currentId = null; }
  }
  if (panelScrim) panelScrim.addEventListener('click', closePanel);
  $$('[data-pz-select]').forEach(function (b) { b.addEventListener('click', function () { selectIncident(b.dataset.pzSelect); }); });

  /* ---------- Gavetas ---------- */
  var scrim = $('[data-pz-scrim]');
  var openDrawer = null;
  function showDrawer(kind) {
    closeMenus(null);
    if (!isWide() && panel && panel.classList.contains('is-open')) closePanel();
    if (openDrawer) openDrawer.hidden = true;
    openDrawer = $('[data-pz-drawer="' + kind + '"]');
    if (!openDrawer) return null;
    openDrawer.hidden = false; if (scrim) scrim.hidden = false;
    document.body.style.overflow = 'hidden';
    var focusEl = $('input:not([type=hidden]):not(.sr-only):not([disabled]), button:not([disabled])', openDrawer);
    if (focusEl) setTimeout(function () { focusEl.focus(); }, 30);
    return openDrawer;
  }
  function closeDrawer() {
    if (openDrawer) openDrawer.hidden = true;
    openDrawer = null; if (scrim) scrim.hidden = true; document.body.style.overflow = '';
  }
  if (scrim) scrim.addEventListener('click', closeDrawer);
  $$('[data-pz-close]').forEach(function (b) { b.addEventListener('click', closeDrawer); });
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    if ($$('.pz-menu').some(function (m) { return !m.hidden; })) { closeMenus(null); return; }
    if (openDrawer) { closeDrawer(); return; }
    if (panel && panel.classList.contains('is-open')) closePanel();
  });
  $$('[data-pz-open]').forEach(function (b) { b.addEventListener('click', function () { if (b.dataset.pzOpen === 'novo') openNovo(); else if (b.dataset.pzOpen === 'ronda') openRonda(); }); });
  document.addEventListener('click', function (ev) {
    var b = ev.target.closest('[data-pz-action]');
    if (!b || b.disabled) return;
    closeMenus(null);
    var id = b.dataset.id, a = b.dataset.pzAction;
    if (a === 'controle') openControle(id);
    else if (a === 'reinspecao') openReinspecao(id);
    else if (a === 'editar') openEditar(id);
    else if (a === 'excluir') openExcluir(id);
    else if (a === 'reabrir') { var f = $('[data-pz-reopen]'); if (f) { f.action = '/pragas/' + id + '/reabrir'; f.submit(); } }
  });

  /* ---------- Registrar praga ---------- */
  var nf = $('[data-pz-drawer="novo"]');
  var catalog = {};
  (data.catalog || []).forEach(function (c) { catalog[c.key] = c; });
  var nfState = { step: 1, key: null, onlySel: false };
  function nfSpec() {
    if (!nfState.key) return null;
    if (nfState.key === 'outra') {
      var nm = $('[data-nf-oname]', nf).value.trim();
      var th = intVal($('[data-nf-onc]', nf)) || 10;
      return { name: nm || 'Outra praga', threshold: th, unit: 'plantas com sintoma', noun: 'Plantas avaliadas', days: 10, threshold_txt: th + '%' };
    }
    return catalog[nfState.key];
  }
  function nfChosen() { return $$('[data-nf-plot]', nf).filter(function (c) { return c.checked; }); }
  function nfIncidence() {
    var s = intVal($('[data-nf-sampled]', nf)), a = intVal($('[data-nf-affected]', nf));
    return (s && s > 0 && a !== null && a <= s) ? { v: a / s * 100, s: s, a: a } : null;
  }
  function nfDupFor(plotId) {
    var spec = nfSpec(); if (!spec) return null;
    var key = norm(spec.name);
    return (data.active || []).find(function (x) { return x.plot_id === plotId && norm(x.key) === key; }) || null;
  }
  function nfRender() {
    if (!nf) return;
    var spec = nfSpec();
    $$('[data-nf-step]', nf).forEach(function (s) { s.hidden = Number(s.dataset.nfStep) !== nfState.step; });
    $$('[data-nf-step-ind]', nf).forEach(function (s) { var n = Number(s.dataset.nfStepInd); s.className = 'pz-step' + (n < nfState.step ? ' is-done' : (n === nfState.step ? ' is-on' : '')); });
    $$('[data-nf-pest]', nf).forEach(function (b) { b.setAttribute('aria-pressed', b.dataset.nfPest === nfState.key ? 'true' : 'false'); });
    $('[data-nf-other]', nf).hidden = nfState.key !== 'outra';
    $('[data-nf-key]', nf).value = nfState.key || '';
    // talhões
    var chosen = nfChosen();
    $('[data-nf-selcount]', nf).textContent = chosen.length;
    var chipsBox = $('[data-nf-selchips]', nf), sumBox = $('[data-nf-selsum]', nf);
    chipsBox.hidden = !(chosen.length > 0 && chosen.length <= 4);
    sumBox.hidden = chosen.length <= 4;
    if (!chipsBox.hidden) {
      chipsBox.innerHTML = chosen.map(function (c) { return '<button type="button" class="pz-selchip" data-nf-unsel="' + c.value + '" aria-label="Remover ' + esc(c.dataset.name) + '">' + esc(c.dataset.name) + ICON_X + '</button>'; }).join('') + '<button type="button" class="pz-linkb" data-nf-clearplots>Limpar</button>';
    }
    if (!sumBox.hidden) {
      var ha = chosen.reduce(function (t, c) { return t + (parseFloat(c.dataset.area) || 0); }, 0);
      $('[data-nf-selsum-n]', nf).textContent = chosen.length + ' talhões';
      $('[data-nf-selsum-ha]', nf).textContent = fmt(ha, 1) + ' ha no total';
      $('[data-nf-only]', nf).textContent = nfState.onlySel ? 'Mostrar todos' : 'Ver só os selecionados';
    }
    var plq = norm(($('[data-nf-plq]', nf) || {}).value || ''), visible = 0, anyDup = false;
    $$('[data-nf-plot-row]', nf).forEach(function (row) {
      var cb = $('[data-nf-plot]', row);
      var show = (!nfState.onlySel || cb.checked) && (!plq || norm(row.dataset.search).indexOf(plq) !== -1);
      row.hidden = !show; if (show) visible++;
      var dup = cb.checked ? nfDupFor(Number(cb.value)) : null;
      var dupBox = $('[data-nf-dup]', row);
      dupBox.hidden = !dup; if (dup) { anyDup = true; $('[data-nf-asrei]', row).dataset.id = dup.id; }
    });
    var nores = $('[data-nf-plot-nores]', nf); if (nores) nores.hidden = visible > 0;
    var wrap = $('[data-nf-plist-wrap]', nf); if (wrap) wrap.classList.toggle('has-more', visible > 6);
    var secBox = $('[data-nf-sectors]', nf);
    if (data.sectors && data.sectors.length) {
      secBox.hidden = false;
      $$('[data-nf-sector]', secBox).forEach(function (b) {
        var ids = JSON.parse(b.dataset.ids); var k = ids.filter(function (id) { var c = $('[data-nf-plot][value="' + id + '"]', nf); return c && c.checked; }).length;
        var all = k === ids.length;
        b.setAttribute('aria-pressed', all ? 'true' : 'false');
        b.textContent = b.dataset.name + ' · ' + (k && !all ? k + '/' : '') + ids.length;
      });
    }
    // incidência
    var inc = nfIncidence();
    if (spec) {
      $('[data-nf-pestname]', nf).textContent = spec.name;
      $('[data-nf-noun]', nf).textContent = spec.noun;
      $('[data-nf-nc]', nf).textContent = spec.threshold_txt || (spec.threshold + '%');
    }
    var above = !!(inc && spec && inc.v > spec.threshold);
    $('[data-nf-inc]', nf).textContent = inc ? pct(inc.v) : '—';
    var bar = $('[data-nf-bar]', nf);
    bar.style.width = (inc && spec ? Math.min(100, inc.v / (spec.threshold * 2) * 100) : 0) + '%';
    bar.style.background = above ? '#B42318' : '#D18A00';
    var verdict = $('[data-nf-verdict]', nf);
    verdict.textContent = !inc ? 'Conte as ' + (spec ? spec.noun.toLowerCase() : 'amostras') + ' e quantas tinham sintoma.' : (above ? 'Acima do nível de controle: vai pedir um controle.' : 'Abaixo do nível de controle: fica em observação, sem aplicar.');
    verdict.style.color = !inc ? '#5B6660' : (above ? '#B42318' : '#8A5A00');
    // validação e rodapé
    var hint = '', block = false;
    if (nfState.step === 1) {
      if (!spec) { hint = 'Escolha uma opção'; block = true; }
      else if (nfState.key === 'outra' && !$('[data-nf-oname]', nf).value.trim()) { hint = 'Digite o nome'; block = true; }
    } else if (nfState.step === 2) {
      if (!chosen.length) { hint = 'Marque ao menos um talhão'; block = true; }
      else if (!inc) { hint = 'Informe quantas tinham sintoma'; block = true; }
      else if (anyDup) { hint = 'Resolva o aviso de duplicidade'; block = true; }
    }
    $('[data-nf-hint]', nf).textContent = hint;
    var next = $('[data-nf-next]', nf);
    next.disabled = block;
    next.textContent = nfState.step === 3 ? 'Confirmar registro' : (nfState.step === 2 ? 'Conferir' : 'Continuar');
    $('[data-nf-back]', nf).textContent = nfState.step === 1 ? 'Cancelar' : 'Voltar';
    if (nfState.step === 3 && spec && inc) {
      var dateEl = $('[data-nf-date]', nf), d = parseISO(dateEl.value) || new Date();
      $('[data-nf-r-pest]', nf).textContent = spec.name;
      $('[data-nf-r-plots]', nf).textContent = chosen.length > 6 ? chosen.length + ' talhões' : chosen.map(function (c) { return c.dataset.name; }).join(', ');
      $('[data-nf-r-inc]', nf).textContent = pct(inc.v) + ' (' + inc.a + ' de ' + inc.s + ' ' + spec.unit + ')';
      $('[data-nf-r-loc]', nf).textContent = $('[data-nf-loc]', nf).value;
      $('[data-nf-r-date]', nf).textContent = ddmmyyyy(d);
      var imp = [
        ['Cria ' + chosen.length + ' foco' + (chosen.length > 1 ? 's' : '') + ' com a situação “' + (above ? 'Precisa de controle' : 'Em observação') + '”.', above ? '#B42318' : '#D18A00'],
        [above ? 'O próximo passo será planejar a pulverização, com produto do estoque e carência.' : 'Reinspeção lembrada para ' + ddmm(addDays(d, spec.days || 10)) + ' (' + (spec.days || 10) + ' dias).', '#1F5FA8'],
        ['Aparece no painel do talhão em Lavoura e no Mapa.', '#446A36'],
      ];
      $('[data-nf-impacts]', nf).innerHTML = imp.map(function (i) { return '<div class="pz-impact"><span class="pz-dot" style="background:' + i[1] + '"></span><span>' + esc(i[0]) + '</span></div>'; }).join('');
      $('[data-nf-plan-wrap]', nf).hidden = !above;
    }
  }
  function openNovo(plotId) {
    if (!nf) return;
    nf.reset();
    nfState = { step: 1, key: null, onlySel: false };
    $('[data-nf-loc]', nf).value = (data.locations || ['Bordadura'])[0];
    $$('[data-nf-locbtn]', nf).forEach(function (b, i) { b.setAttribute('aria-pressed', i === 0 ? 'true' : 'false'); b.classList.toggle('is-on', i === 0); });
    $$('[data-nf-pest]', nf).forEach(function (b) { b.hidden = false; });
    $('[data-nf-photo-txt]', nf).textContent = 'Adicionar foto';
    var cbs = $$('[data-nf-plot]', nf);
    if (plotId) cbs.forEach(function (c) { c.checked = String(c.value) === String(plotId); });
    else if (cbs.length === 1) cbs[0].checked = true;
    showDrawer('novo');
    nfRender();
  }
  if (nf) {
    var secBox = $('[data-nf-sectors]', nf);
    (data.sectors || []).forEach(function (s) {
      var b = document.createElement('button');
      b.type = 'button'; b.className = 'pz-chip pz-chip-sm'; b.dataset.nfSector = '1'; b.dataset.name = s.name; b.dataset.ids = JSON.stringify(s.ids);
      b.addEventListener('click', function () {
        var all = b.getAttribute('aria-pressed') === 'true';
        s.ids.forEach(function (id) { var c = $('[data-nf-plot][value="' + id + '"]', nf); if (c) c.checked = !all; });
        nfRender();
      });
      secBox.appendChild(b);
    });
    $$('[data-nf-pest]', nf).forEach(function (b) { b.addEventListener('click', function () { nfState.key = b.dataset.nfPest; nfRender(); }); });
    var pq = $('[data-nf-pq]', nf);
    pq.addEventListener('input', function () { var q = norm(pq.value); $$('[data-nf-pest]', nf).forEach(function (b) { b.hidden = !!q && b.dataset.nfPest !== 'outra' && norm(b.dataset.name).indexOf(q) === -1; }); });
    $('[data-nf-oname]', nf).addEventListener('input', nfRender);
    onlyDigits($('[data-nf-onc]', nf)); $('[data-nf-onc]', nf).addEventListener('input', nfRender);
    ['[data-nf-sampled]', '[data-nf-affected]'].forEach(function (s) { var el = $(s, nf); onlyDigits(el); el.addEventListener('input', nfRender); });
    var plq = $('[data-nf-plq]', nf); if (plq) plq.addEventListener('input', nfRender);
    nf.addEventListener('change', function (ev) { if (ev.target.matches('[data-nf-plot]')) nfRender(); });
    nf.addEventListener('click', function (ev) {
      var t = ev.target.closest('[data-nf-unsel],[data-nf-clearplots],[data-nf-only],[data-nf-asrei],[data-nf-goto],[data-nf-locbtn]');
      if (!t) return;
      if (t.dataset.nfUnsel) { var c = $('[data-nf-plot][value="' + t.dataset.nfUnsel + '"]', nf); if (c) c.checked = false; }
      else if (t.hasAttribute('data-nf-clearplots')) { $$('[data-nf-plot]', nf).forEach(function (c) { c.checked = false; }); nfState.onlySel = false; }
      else if (t.hasAttribute('data-nf-only')) { nfState.onlySel = !nfState.onlySel; }
      else if (t.hasAttribute('data-nf-asrei')) { openReinspecao(t.dataset.id); return; }
      else if (t.dataset.nfGoto) { nfState.step = Number(t.dataset.nfGoto); }
      else if (t.dataset.nfLocbtn) {
        $('[data-nf-loc]', nf).value = t.dataset.nfLocbtn;
        $$('[data-nf-locbtn]', nf).forEach(function (b) { var on = b === t; b.setAttribute('aria-pressed', on ? 'true' : 'false'); b.classList.toggle('is-on', on); });
      }
      nfRender();
    });
    $('[data-nf-photo]', nf).addEventListener('change', function (ev) { var f = ev.target.files && ev.target.files[0]; $('[data-nf-photo-txt]', nf).textContent = f ? '1 foto · trocar' : 'Adicionar foto'; });
    $('[data-nf-back]', nf).addEventListener('click', function () { if (nfState.step === 1) closeDrawer(); else { nfState.step -= 1; nfRender(); } });
    $('[data-nf-next]', nf).addEventListener('click', function () {
      if (this.disabled) return;
      if (nfState.step < 3) { nfState.step += 1; nfRender(); return; }
      this.disabled = true; this.textContent = 'Salvando…';
      nf.submit();
    });
  }

  /* ---------- Corrigir registro ---------- */
  var ed = $('[data-pz-drawer="editar"]');
  function edRender() {
    var s = intVal($('[data-ed-sampled]', ed)), a = intVal($('[data-ed-affected]', ed));
    var ok = s && s > 0 && a !== null && a <= s;
    $('[data-ed-inc]', ed).textContent = ok ? pct(a / s * 100) : '—';
    $('[data-ed-submit]', ed).disabled = !ok;
  }
  function openEditar(id) {
    var p = panels[String(id)]; if (!p || !ed) return;
    ed.reset();
    ed.action = '/pragas/' + p.id + '/editar';
    $('[data-ed-sub]', ed).textContent = p.name + ' · ' + p.plot;
    $('[data-ed-noun]', ed).textContent = p.noun;
    $('[data-ed-sampled]', ed).value = p.sampled || '';
    $('[data-ed-affected]', ed).value = p.affected == null ? '' : p.affected;
    $('[data-ed-date]', ed).value = p.occurrence_iso;
    $('[data-ed-notes]', ed).value = p.notes || '';
    $('[data-ed-loc]', ed).value = p.location_raw || '';
    $$('[data-ed-locbtn]', ed).forEach(function (b) { var on = b.dataset.edLocbtn === p.location_raw; b.setAttribute('aria-pressed', on ? 'true' : 'false'); });
    showDrawer('editar'); edRender();
  }
  if (ed) {
    ['[data-ed-sampled]', '[data-ed-affected]'].forEach(function (s) { var el = $(s, ed); onlyDigits(el); el.addEventListener('input', edRender); });
    $$('[data-ed-locbtn]', ed).forEach(function (b) {
      b.addEventListener('click', function () { $('[data-ed-loc]', ed).value = b.dataset.edLocbtn; $$('[data-ed-locbtn]', ed).forEach(function (x) { x.setAttribute('aria-pressed', x === b ? 'true' : 'false'); }); });
    });
  }

  /* ---------- Planejar controle ---------- */
  var ct = $('[data-pz-drawer="controle"]');
  var products = {}; (data.products || []).forEach(function (p) { products[String(p.id)] = p; });
  var ctPanel = null;
  function ctProduct() { var r = ct && $('[data-ct-prod]:checked', ct); return r ? products[r.value] : null; }
  function ctDay() {
    var sel = $('[data-ct-day][aria-pressed="true"]', ct);
    if (!sel) return null;
    if (sel.dataset.ctDay === 'custom') return parseISO($('[data-ct-custom-date]', ct).value);
    return parseISO(sel.dataset.ctDay);
  }
  function ctRender() {
    if (!ct || !ctPanel || !$('[data-ct-submit]', ct)) return;
    var prod = ctProduct(), dose = decVal($('[data-ct-dose]', ct)), area = ctPanel.plot_area || 0;
    var car = intVal($('[data-ct-car]', ct)), day = ctDay();
    var unit = prod ? prod.unit : '';
    $('[data-ct-unit]', ct).textContent = unit || 'un';
    var need = prod && dose ? dose * area : null;
    $('[data-ct-need]', ct).textContent = need != null ? fmt(need, 2) + ' ' + unit : '—';
    var after = prod && need != null ? prod.stock - need : null;
    $('[data-ct-stock]', ct).textContent = prod ? fmt(prod.stock, 2) + ' ' + unit + (after != null ? ' → ' + (after < 0 ? 'falta' : fmt(after, 2) + ' ' + unit) : '') : '—';
    var short = after != null && after < 0;
    $('[data-ct-stockbox]', ct).style.background = short ? '#FDECEA' : '';
    $('[data-ct-short]', ct).hidden = !short;
    if (short) $('[data-ct-short-txt]', ct).textContent = 'Estoque insuficiente: faltam ' + fmt(-after, 2) + ' ' + unit + '. Ajuste a dose, escolha outro produto ou registre a compra.';
    $('[data-ct-date]', ct).value = day ? day.getFullYear() + '-' + String(day.getMonth() + 1).padStart(2, '0') + '-' + String(day.getDate()).padStart(2, '0') : '';
    var cs = $('[data-ct-custom-short]', ct);
    var customOn = $('[data-ct-day="custom"]', ct).getAttribute('aria-pressed') === 'true';
    cs.textContent = customOn && day ? ddmm(day) : 'escolher';
    $('[data-ct-custom]', ct).hidden = !customOn;
    var legend = $('[data-ct-legend]', ct); if (legend) legend.hidden = customOn;
    $('[data-ct-imp-car]', ct).innerHTML = day && car != null ? 'Colheita liberada neste talhão a partir de <strong>' + ddmmyyyy(addDays(day, car)) + '</strong> (carência de ' + car + ' dias).' : 'Informe a carência para ver quando a colheita fica liberada neste talhão.';
    $('[data-ct-imp-cost]', ct).innerHTML = need != null && prod && prod.unit_cost ? 'Custo estimado <strong>R$ ' + fmt(need * prod.unit_cost, 2) + '</strong>; entra no custo do ' + esc(ctPanel.plot) + ' quando a aplicação for marcada como realizada.' : 'O produto sai do estoque e o custo entra no ' + esc(ctPanel.plot) + ' quando a aplicação for marcada como realizada.';
    $('[data-ct-imp-rei]', ct).innerHTML = day ? 'Reinspeção lembrada para <strong>' + ddmm(addDays(day, 7)) + '</strong>, 7 dias após a aplicação.' : 'Escolha o dia para agendar a reinspeção.';
    var hint = !prod ? 'Escolha o produto' : (!dose ? 'Informe a dose' : (short ? 'Estoque insuficiente' : (car == null ? 'Informe a carência' : (!day ? 'Escolha o dia' : ''))));
    $('[data-ct-hint]', ct).textContent = hint;
    $('[data-ct-submit]', ct).disabled = !!hint;
  }
  function ctPickDay(btn) { $$('[data-ct-day]', ct).forEach(function (b) { b.setAttribute('aria-pressed', b === btn ? 'true' : 'false'); }); ctRender(); }
  function ctFillProduct() {
    var prod = ctProduct(), carEl = $('[data-ct-car]', ct);
    if (prod && prod.withdrawal != null && !carEl.dataset.touched) carEl.value = prod.withdrawal;
    ctRender();
  }
  function openControle(id) {
    var p = panels[String(id)]; if (!p || !ct) return;
    ctPanel = p;
    ct.reset();
    ct.action = '/pragas/' + p.id + '/controle';
    $('[data-ct-sub]', ct).textContent = p.name + ' · ' + p.plot + ' (' + fmt(p.plot_area || 0, 1) + ' ha) · incidência ' + p.incidence_txt;
    if (!$('[data-ct-submit]', ct)) { showDrawer('controle'); return; }
    var carEl = $('[data-ct-car]', ct); delete carEl.dataset.touched;
    var days = $$('[data-ct-day]', ct);
    var best = days.find(function (b) { return $('.is-ideal', b); }) || days.find(function (b) { return b.dataset.ctDay !== 'custom'; }) || days[days.length - 1];
    if (best && best.dataset.ctDay === 'custom') $('[data-ct-custom-date]', ct).value = data.today;
    days.forEach(function (b) { b.setAttribute('aria-pressed', b === best ? 'true' : 'false'); });
    showDrawer('controle');
    ctFillProduct();
  }
  if (ct && $('[data-ct-submit]', ct)) {
    $$('[data-ct-prod]', ct).forEach(function (r) { r.addEventListener('change', ctFillProduct); });
    var doseEl = $('[data-ct-dose]', ct);
    doseEl.addEventListener('input', function () {
      var digits = doseEl.value.replace(/\D/g, '').replace(/^0+(?=\d)/, '');
      doseEl.value = digits ? fmt(parseInt(digits, 10) / 100, 2) : '';
      ctRender();
    });
    var carEl2 = $('[data-ct-car]', ct); onlyDigits(carEl2);
    carEl2.addEventListener('input', function () { carEl2.dataset.touched = '1'; ctRender(); });
    $$('[data-ct-day]', ct).forEach(function (b) {
      b.addEventListener('click', function () {
        if (b.dataset.ctDay === 'custom' && !$('[data-ct-custom-date]', ct).value) $('[data-ct-custom-date]', ct).value = data.today;
        ctPickDay(b);
      });
    });
    $('[data-ct-custom-date]', ct).addEventListener('change', ctRender);
    ct.addEventListener('submit', function (ev) { ctRender(); var s = $('[data-ct-submit]', ct); if (s.disabled) { ev.preventDefault(); return; } s.disabled = true; s.textContent = 'Salvando…'; });
  }

  /* ---------- Reinspecionar ---------- */
  var ri = $('[data-pz-drawer="reinspecao"]');
  var riPanel = null, riTouched = false;
  function riRender() {
    if (!ri || !riPanel) return;
    var s = intVal($('[data-ri-sampled]', ri)), a = intVal($('[data-ri-affected]', ri));
    var ok = s && s > 0 && a !== null && a <= s;
    var v = ok ? a / s * 100 : null, prev = riPanel.prev_value, th = riPanel.threshold;
    $('[data-ri-new]', ri).textContent = ok ? pct(v) : '—';
    var tr = $('[data-ri-trend]', ri);
    if (!ok || prev == null) { tr.textContent = ok ? 'primeira contagem' : 'informe a contagem'; tr.style.background = '#EEF1ED'; tr.style.color = '#5B6660'; }
    else if (Math.abs(v - prev) < 0.05) { tr.textContent = 'igual'; tr.style.background = '#EEF1ED'; tr.style.color = '#5B6660'; }
    else if (v > prev) { tr.textContent = 'subiu ' + fmt(v - prev, 1) + ' pontos'; tr.style.background = '#FDECEA'; tr.style.color = '#B42318'; }
    else { tr.textContent = 'caiu ' + fmt(prev - v, 1) + ' pontos'; tr.style.background = '#E6F4E2'; tr.style.color = '#1F7A3A'; }
    var rec = ok ? (v < th * 0.4 ? 'encerrar' : (v <= th ? 'manter' : 'novo')) : null;
    $$('[data-ri-rec]', ri).forEach(function (t) { t.hidden = t.dataset.riRec !== rec; });
    if (rec && !riTouched) { var r = $('[data-ri-dec][value="' + rec + '"]', ri); if (r) r.checked = true; }
    var dec = $('[data-ri-dec]:checked', ri);
    $('[data-ri-hint]', ri).textContent = ok ? (dec ? '' : 'Escolha o que fazer') : 'Informe a contagem';
    $('[data-ri-submit]', ri).disabled = !ok || !dec;
  }
  function openReinspecao(id) {
    var p = panels[String(id)]; if (!p || !ri) return;
    riPanel = p; riTouched = false;
    ri.reset();
    ri.action = '/pragas/' + p.id + '/reinspecao';
    $('[data-ri-sub]', ri).textContent = p.name + ' · ' + p.plot + (p.prev_label ? ' · última avaliação ' + p.prev_label + ': ' + p.prev_txt : '');
    $('[data-ri-noun]', ri).textContent = p.noun;
    $('[data-ri-nc]', ri).textContent = p.threshold_txt;
    $('[data-ri-prev]', ri).textContent = p.prev_value != null ? p.prev_txt : 'sem contagem';
    showDrawer('reinspecao');
    riRender();
  }
  if (ri) {
    ['[data-ri-sampled]', '[data-ri-affected]'].forEach(function (s) { var el = $(s, ri); onlyDigits(el); el.addEventListener('input', riRender); });
    $$('[data-ri-dec]', ri).forEach(function (r) { r.addEventListener('change', function () { riTouched = true; riRender(); }); });
    ri.addEventListener('submit', function (ev) { var s = $('[data-ri-submit]', ri); if (s.disabled) { ev.preventDefault(); return; } s.disabled = true; s.textContent = 'Salvando…'; });
  }

  /* ---------- Excluir ---------- */
  var del = $('[data-pz-drawer="excluir"]');
  function openExcluir(id) {
    var p = panels[String(id)]; if (!p || !del) return;
    del.action = '/pragas/' + p.id + '/excluir';
    $('[data-del-txt]', del).textContent = p.name + ' no ' + p.plot + '. O histórico de reinspeções e a foto serão apagados. Pulverizações já registradas continuam em Pulverizações.';
    showDrawer('excluir');
  }

  /* ---------- Nova ronda ---------- */
  var rd = $('[data-pz-drawer="ronda"]');
  function rdRender() {
    if (!rd) return;
    var nP = $$('[data-rd-plot]:checked', rd).length, nX = $$('[data-rd-pest]:checked', rd).length;
    $('[data-rd-summary]', rd).textContent = nP + ' talhões × ' + nX + ' pragas. Leva cerca de ' + Math.max(10, nP * nX * 2) + ' min. No celular funciona sem internet: as marcações ficam salvas e são enviadas quando houver sinal.';
    $$('button[type=submit]', rd).forEach(function (b) { b.disabled = !nP || !nX; });
  }
  function openRonda() { if (!rd) return; showDrawer('ronda'); rdRender(); }
  if (rd) rd.addEventListener('change', rdRender);

  /* ---------- Abertura inicial (vindo de um redirecionamento ou link) ---------- */
  var sel = openInfo.selected;
  if (sel && panels[String(sel)]) selectIncident(sel, { silent: !!openInfo.drawer });
  else if (isWide()) {
    var firstVisible = rows.find(function (r) { return !r.hidden; });
    if (firstVisible) selectIncident(firstVisible.dataset.pzRow, { silent: true });
  }
  if (sel && !openInfo.drawer) {
    var row = $('[data-pz-row="' + sel + '"]'); if (row && row.scrollIntoView) row.scrollIntoView({ block: 'nearest' });
  }
  var d = openInfo.drawer;
  if (d && canWrite) {
    if (d.kind === 'novo') openNovo(d.plot_id);
    else if (d.kind === 'controle') openControle(d.id);
    else if (d.kind === 'reinspecao') openReinspecao(d.id);
    else if (d.kind === 'editar') openEditar(d.id);
  }
})();
