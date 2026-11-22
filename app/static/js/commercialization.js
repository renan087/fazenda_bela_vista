/* Comercialização: menu "⋯" de cada venda (fecha ao clicar fora e com Esc). */
(function () {
  'use strict';
  document.addEventListener('DOMContentLoaded', function () {
    var wraps = document.querySelectorAll('[data-sl-more]');
    function closeAll(except) {
      wraps.forEach(function (w) {
        if (w === except) return;
        var m = w.querySelector('.sl-menu'); var b = w.querySelector('[data-sl-more-btn]');
        if (m && !m.hidden) { m.hidden = true; b.setAttribute('aria-expanded', 'false'); w.closest('.sl-sale') && w.closest('.sl-sale').classList.remove('is-menu'); }
      });
    }
    wraps.forEach(function (w) {
      var btn = w.querySelector('[data-sl-more-btn]'); var menu = w.querySelector('.sl-menu');
      btn.addEventListener('click', function (e) {
        e.stopPropagation(); closeAll(w);
        var open = menu.hidden; menu.hidden = !open; btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        var card = w.closest('.sl-sale'); if (card) card.classList.toggle('is-menu', open);
        if (open) { var first = menu.querySelector('.sl-mi'); if (first) first.focus(); }
      });
      menu.addEventListener('keydown', function (e) {
        if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
        var items = Array.prototype.slice.call(menu.querySelectorAll('.sl-mi')); var i = items.indexOf(document.activeElement);
        e.preventDefault(); items[(i + (e.key === 'ArrowDown' ? 1 : items.length - 1)) % items.length].focus();
      });
    });
    document.addEventListener('click', function (e) { if (!e.target.closest('[data-sl-more]')) closeAll(null); });
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      var open = document.querySelector('[data-sl-more] .sl-menu:not([hidden])');
      closeAll(null); if (open) open.closest('[data-sl-more]').querySelector('[data-sl-more-btn]').focus();
    });
  });
})();
