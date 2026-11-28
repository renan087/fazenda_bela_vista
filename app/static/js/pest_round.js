/* Ronda de monitoramento no celular: marca ausente/baixa/média/alta por praga, salva no aparelho e envia. */
(function () {
  'use strict';
  var root = document.querySelector('[data-pest-round]');
  if (!root) return;
  var data = {};
  try { data = JSON.parse(document.getElementById('prd-data').textContent || '{}'); } catch (e) { data = {}; }
  var net = root.querySelector('[data-prd-net]');
  function updateNet() { if (net) net.hidden = navigator.onLine !== false; }
  window.addEventListener('online', updateNet); window.addEventListener('offline', updateNet); updateNet();
  var form = root.querySelector('[data-prd-form]');
  if (!form || data.done) return;

  var KEY = 'pz-round-' + data.id;
  var state = { screen: 'plots', cur: null, marks: {}, done: {}, pending: false };
  try { var saved = JSON.parse(localStorage.getItem(KEY) || 'null'); if (saved) { state.marks = saved.marks || {}; state.done = saved.done || {}; state.pending = !!saved.pending; } } catch (e) { /* sem storage */ }
  function save() { try { localStorage.setItem(KEY, JSON.stringify({ marks: state.marks, done: state.done, pending: state.pending })); } catch (e) { /* sem storage */ } }
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var LV = data.levels || ['Ausente', 'Baixa', 'Média', 'Alta'];
  var HINT = ['', 'Abaixo do nível de controle', 'Perto do nível de controle', 'Acima do nível de controle'];
  var plots = data.plots || [], pests = data.pests || [];
  var $ = function (s) { return root.querySelector(s); };
  var title = $('[data-prd-title]'), sub = $('[data-prd-sub]'), subDefault = sub.textContent;
  var foot = $('[data-prd-foot]');
  var CHECK = '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12l4 4 10-10"/></svg>';
  var CHEV = '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#7A857E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 6l6 6-6 6"/></svg>';

  function marksOf(pid) { return state.marks[pid] || (state.marks[pid] = {}); }
  function show(screen) {
    state.screen = screen;
    root.querySelectorAll('[data-prd-screen]').forEach(function (s) { s.hidden = s.dataset.prdScreen !== screen; });
    render();
    window.scrollTo(0, 0);
  }
  function render() {
    var nDone = plots.filter(function (p) { return state.done[p.id]; }).length;
    if (state.screen === 'plots') {
      title.textContent = 'Ronda de monitoramento'; sub.textContent = subDefault;
      $('[data-prd-progress]').textContent = nDone + ' de ' + plots.length + ' talhões';
      $('[data-prd-progbar]').style.width = (plots.length ? nDone / plots.length * 100 : 0) + '%';
      $('[data-prd-plots]').innerHTML = plots.map(function (p, i) {
        var d = !!state.done[p.id], m = marksOf(p.id);
        var found = pests.filter(function (x) { return m[x.key] > 0; }).length;
        return '<button type="button" class="prd-plot" data-go="' + p.id + '"><span class="prd-plot-ic' + (d ? ' is-done' : '') + '">' + (d ? CHECK : '<strong>' + (i + 1) + '</strong>') + '</span>' +
          '<span class="pz-grow1"><strong class="pz-block">' + esc(p.name) + '</strong><span class="pz-small pz-muted">' + (p.variety ? esc(p.variety) + ' · ' : '') + (d ? (found ? found + ' com sintoma' : 'Sem sintomas') : 'A fazer') + '</span></span>' + CHEV + '</button>';
      }).join('');
      foot.innerHTML = state.pending
        ? '<p class="pz-small prd-pending">Sem internet: a ronda está salva no celular e será enviada assim que houver sinal.</p>'
        : '<button type="button" class="pz-btn pz-btn-pri prd-wide" data-review' + (nDone ? '' : ' disabled') + '>Conferir e enviar ronda</button>';
    } else if (state.screen === 'check') {
      var cur = plots.find(function (p) { return p.id === state.cur; });
      title.textContent = cur ? cur.name : ''; sub.textContent = (cur && cur.variety ? cur.variety + ' · ' : '') + 'marque cada praga';
      var m = marksOf(state.cur);
      $('[data-prd-checks]').innerHTML = pests.map(function (x) {
        var v = m[x.key], prev = (data.active || {})[state.cur + ':' + x.key];
        return '<div class="prd-row"><div class="pz-between pz-baseline"><strong>' + esc(x.name) + '</strong><span class="pz-small pz-muted">' + esc(x.category) + '</span></div>' +
          (prev ? '<div class="pz-small prd-prev">Foco ativo: ' + esc(prev) + '. O que marcar aqui vira reinspeção.</div>' : '') +
          '<div class="prd-lvl" role="group" aria-label="Nível de ' + esc(x.name) + '">' + LV.map(function (l, i) {
            return '<button type="button" class="' + (v === i ? 'on' + i : '') + '" aria-pressed="' + (v === i) + '" data-mark="' + x.key + '" data-level="' + i + '">' + esc(l) + '</button>';
          }).join('') + '</div>' + (v > 0 ? '<span class="pz-small pz-muted">' + HINT[v] + '</span>' : '') + '</div>';
      }).join('');
      var left = pests.filter(function (x) { return m[x.key] == null; }).length;
      foot.innerHTML = '<span class="pz-small pz-muted pz-grow1">' + (left ? 'Faltam ' + left + ' pragas' : 'Tudo marcado') + '</span><button type="button" class="pz-btn pz-btn-pri" data-finish' + (left ? ' disabled' : '') + '>Concluir talhão</button>';
    } else if (state.screen === 'review') {
      title.textContent = 'Conferir ronda'; sub.textContent = nDone + ' de ' + plots.length + ' talhões feitos';
      var novos = [], reins = [], ok = [];
      plots.forEach(function (p) {
        if (!state.done[p.id]) return;
        var m2 = marksOf(p.id), zero = 0;
        pests.forEach(function (x) {
          var v2 = m2[x.key], prev2 = (data.active || {})[p.id + ':' + x.key];
          if (v2 > 0) (prev2 ? reins : novos).push(x.name + ' · ' + p.name + ' · ' + LV[v2]);
          else { zero++; if (prev2) reins.push(x.name + ' · ' + p.name + ' · não encontrado'); }
        });
        ok.push(p.name + ': ' + zero + ' sem sintoma');
      });
      var groups = [['Novos focos', '#B42318', novos], ['Reinspeções de focos ativos', '#1F5FA8', reins], ['Sem sintoma', '#1F7A3A', ok]];
      $('[data-prd-review]').innerHTML = groups.map(function (g) {
        return '<div class="prd-row"><div class="pz-between"><span class="pz-between"><span class="pz-dot" style="background:' + g[1] + '"></span><strong>' + g[0] + '</strong></span><span class="pz-small pz-muted">' + g[2].length + '</span></div>' +
          (g[2].length ? g[2] : ['Nenhum']).map(function (t) { return '<div class="pz-small pz-muted">' + esc(t) + '</div>'; }).join('') + '</div>';
      }).join('');
      foot.innerHTML = '<button type="button" class="pz-btn pz-btn-sec" data-back>Voltar</button><button type="button" class="pz-btn pz-btn-pri pz-grow1" data-send>Confirmar e enviar</button>';
    }
  }
  function buildInputs() {
    var box = $('[data-prd-inputs]'); box.innerHTML = '';
    plots.forEach(function (p) {
      if (!state.done[p.id]) return;
      var m = marksOf(p.id);
      pests.forEach(function (x) {
        if (m[x.key] == null) return;
        var i = document.createElement('input'); i.type = 'hidden'; i.name = 'level_' + p.id + '_' + x.key; i.value = m[x.key]; box.appendChild(i);
      });
    });
  }
  function send() {
    buildInputs();
    if (navigator.onLine === false) { state.pending = true; save(); show('plots'); return; }
    state.pending = false;
    try { localStorage.removeItem(KEY); } catch (e) { /* sem storage */ }
    form.submit();
  }
  window.addEventListener('online', function () { if (state.pending) send(); });
  root.addEventListener('click', function (ev) {
    var t = ev.target.closest('[data-go],[data-mark],[data-finish],[data-review],[data-back],[data-send],[data-prd-back]');
    if (!t || t.disabled) return;
    if (t.hasAttribute('data-prd-back') && state.screen !== 'plots') { ev.preventDefault(); show('plots'); return; }
    if (t.dataset.go) { state.cur = Number(t.dataset.go); show('check'); }
    else if (t.dataset.mark) { marksOf(state.cur)[t.dataset.mark] = Number(t.dataset.level); save(); render(); }
    else if (t.hasAttribute('data-finish')) { state.done[state.cur] = 1; save(); show('plots'); }
    else if (t.hasAttribute('data-review')) show('review');
    else if (t.hasAttribute('data-back')) show('plots');
    else if (t.hasAttribute('data-send')) { t.disabled = true; send(); }
  });
  if (state.pending && navigator.onLine !== false) send(); else show('plots');
})();
