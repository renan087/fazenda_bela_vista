/* Ajusta direção do menu (para cima e/ou alinhado à direita) quando não cabe na tela. */
window.svPlaceMenu = function (menu) {
  if (!menu) return;
  menu.classList.remove('is-up', 'is-right');
  if (getComputedStyle(menu).position === 'fixed') return;
  var r = menu.getBoundingClientRect();
  var trigger = menu.parentElement.getBoundingClientRect();
  if (r.bottom > window.innerHeight - 8 && trigger.top - r.height - 8 > 0) menu.classList.add('is-up');
  if (r.right > window.innerWidth - 8 && trigger.right - r.width > 8) menu.classList.add('is-right');
};

/* Comercialização: menu "⋯", submenus (nota, entrega, recebimento), parcelas, gaveta e aviso rápido. */
(function () {
  'use strict';
  function brl(v) { return 'R$ ' + Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
  function resetPopStyles(pop) {
    if (!pop) return;
    pop.classList.remove('is-sub', 'is-up', 'is-right');
    pop.style.left = '';
    pop.style.top = '';
    pop.style.right = '';
    pop.style.bottom = '';
    pop.style.maxHeight = '';
    pop.style.overflowY = '';
  }

  document.addEventListener('DOMContentLoaded', function () {
    var wraps = Array.prototype.slice.call(document.querySelectorAll('[data-sl-more]'));
    var pops = Array.prototype.slice.call(document.querySelectorAll('[data-sv-pop]'));
    var canHover = window.matchMedia && window.matchMedia('(hover: hover)').matches;
    var activeSubBtn = null;
    var activeSubPop = null;
    var subCloseTimer = null;

    function clearSubTimer() {
      if (subCloseTimer) { clearTimeout(subCloseTimer); subCloseTimer = null; }
    }

    function clearSubTriggers(scope) {
      var root = scope || document;
      root.querySelectorAll('[data-sv-submenu]').forEach(function (b) {
        b.setAttribute('aria-expanded', 'false');
        b.classList.remove('is-sub-open');
      });
    }

    function closePops(except) {
      clearSubTimer();
      pops.forEach(function (w) {
        if (w === except) return;
        var p = w.querySelector('.sv-pop'); var b = w.querySelector('[data-sv-pop-btn]');
        if (p && !p.hidden) {
          p.hidden = true;
          resetPopStyles(p);
          if (b) b.setAttribute('aria-expanded', 'false');
        }
      });
      if (!except) {
        clearSubTriggers(document);
        activeSubBtn = null;
        activeSubPop = null;
      }
    }

    function closeMenus(except) {
      clearSubTimer();
      wraps.forEach(function (w) {
        if (w === except) return;
        var m = w.querySelector('.sl-menu'); var b = w.querySelector('[data-sl-more-btn]');
        if (m && !m.hidden) {
          m.hidden = true;
          if (b) b.setAttribute('aria-expanded', 'false');
          clearSubTriggers(m);
          var card = w.closest('.sl-sale');
          if (card) card.classList.remove('is-menu');
        }
      });
      if (activeSubPop && (!except || !except.contains(activeSubBtn))) {
        closePops(null);
      }
    }

    function positionSubmenu(subBtn, menu, pop) {
      if (window.innerWidth <= 639) {
        resetPopStyles(pop);
        pop.classList.add('is-sub');
        return;
      }
      resetPopStyles(pop);
      pop.classList.add('is-sub');
      pop.style.left = '-9999px';
      pop.style.top = '0px';
      pop.hidden = false;
      var menuRect = menu.getBoundingClientRect();
      var itemRect = subBtn.getBoundingClientRect();
      var popRect = pop.getBoundingClientRect();
      var vw = window.innerWidth;
      var vh = window.innerHeight;
      var left = menuRect.left - popRect.width - 6;
      if (left < 8) {
        var rightCandidate = menuRect.right + 6;
        left = (rightCandidate + popRect.width <= vw - 8) ? rightCandidate : Math.max(8, vw - popRect.width - 8);
      }
      var top = itemRect.top - 6;
      if (top + popRect.height > vh - 8) top = vh - popRect.height - 8;
      if (top < 8) top = 8;
      if (popRect.height > vh - 16) {
        pop.style.maxHeight = (vh - 16) + 'px';
        pop.style.overflowY = 'auto';
      }
      pop.style.left = Math.round(left) + 'px';
      pop.style.top = Math.round(top) + 'px';
      pop.style.right = 'auto';
      pop.style.bottom = 'auto';
    }

    function openSubmenu(subBtn, menu, focusFirst) {
      clearSubTimer();
      var row = subBtn.closest('.sv-row');
      var kind = subBtn.getAttribute('data-sv-submenu');
      var wrap = row && row.querySelector('[data-sv-pop-kind="' + kind + '"]');
      var pop = wrap && wrap.querySelector('.sv-pop');
      if (!pop) return;
      if (activeSubPop && activeSubPop !== pop) {
        activeSubPop.hidden = true;
        resetPopStyles(activeSubPop);
      }
      pops.forEach(function (w) {
        if (w === wrap) return;
        var p = w.querySelector('.sv-pop'); var b = w.querySelector('[data-sv-pop-btn]');
        if (p && !p.hidden) { p.hidden = true; resetPopStyles(p); if (b) b.setAttribute('aria-expanded', 'false'); }
      });
      clearSubTriggers(menu);
      if (window.innerWidth <= 639) {
        closeMenus(null);
        resetPopStyles(pop);
        pop.classList.add('is-sub');
        pop.hidden = false;
        activeSubBtn = null;
        activeSubPop = pop;
      } else {
        subBtn.setAttribute('aria-expanded', 'true');
        subBtn.classList.add('is-sub-open');
        activeSubBtn = subBtn;
        activeSubPop = pop;
        positionSubmenu(subBtn, menu, pop);
      }
      if (focusFirst) {
        var f = pop.querySelector('.sv-pi:not([disabled]), input:not([disabled])');
        if (f) f.focus();
      }
    }

    function scheduleSubClose() {
      clearSubTimer();
      subCloseTimer = setTimeout(function () {
        if (!activeSubPop || activeSubPop.hidden) return;
        if (activeSubPop.matches(':hover')) return;
        if (activeSubBtn && activeSubBtn.matches(':hover')) return;
        if (activeSubPop.contains(document.activeElement)) return;
        closePops(null);
      }, 220);
    }

    wraps.forEach(function (w) {
      var btn = w.querySelector('[data-sl-more-btn]'); var menu = w.querySelector('.sl-menu');
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        closePops(null);
        closeMenus(w);
        var open = menu.hidden;
        menu.hidden = !open;
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        var card = w.closest('.sl-sale'); if (card) card.classList.toggle('is-menu', open);
        if (open) window.svPlaceMenu(menu);
        if (open) { var first = menu.querySelector('.sl-mi:not([disabled])'); if (first) first.focus(); }
      });
      menu.addEventListener('keydown', function (e) {
        if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
          var sub = document.activeElement && document.activeElement.closest('[data-sv-submenu]');
          if (sub && e.key === 'ArrowLeft') { e.preventDefault(); openSubmenu(sub, menu, true); }
          return;
        }
        if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
        var items = Array.prototype.slice.call(menu.querySelectorAll('.sl-mi:not([disabled])'));
        if (!items.length) return;
        var i = items.indexOf(document.activeElement);
        e.preventDefault();
        items[(i + (e.key === 'ArrowDown' ? 1 : items.length - 1)) % items.length].focus();
      });
      menu.querySelectorAll('[data-sv-submenu]').forEach(function (subBtn) {
        subBtn.addEventListener('click', function (e) {
          e.preventDefault();
          e.stopPropagation();
          var isSameOpen = activeSubBtn === subBtn && activeSubPop && !activeSubPop.hidden;
          if (isSameOpen && e.detail > 0 && !canHover) {
            closePops(null);
          } else {
            openSubmenu(subBtn, menu, e.detail === 0);
          }
        });
        if (canHover) {
          subBtn.addEventListener('mouseenter', function () {
            openSubmenu(subBtn, menu, false);
          });
          subBtn.addEventListener('mouseleave', scheduleSubClose);
        }
      });
      if (canHover) {
        menu.querySelectorAll('.sl-mi:not([data-sv-submenu]), .sl-mi-tipwrap').forEach(function (other) {
          other.addEventListener('mouseenter', function () {
            if (activeSubPop && !activeSubPop.hidden) closePops(null);
          });
        });
      }
    });

    pops.forEach(function (w) {
      var btn = w.querySelector('[data-sv-pop-btn]'); var pop = w.querySelector('.sv-pop');
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        closeMenus(null);
        closePops(w);
        resetPopStyles(pop);
        var open = pop.hidden;
        pop.hidden = !open;
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        if (open) window.svPlaceMenu(pop);
        if (open) { var f = pop.querySelector('.sv-pi:not([disabled]), input:not([disabled])'); if (f) f.focus(); }
      });
      if (canHover && pop) {
        pop.addEventListener('mouseenter', function () {
          if (pop.classList.contains('is-sub')) clearSubTimer();
        });
        pop.addEventListener('mouseleave', function () {
          if (pop.classList.contains('is-sub')) scheduleSubClose();
        });
      }
    });

    document.addEventListener('click', function (e) {
      var inMore = e.target.closest('[data-sl-more]');
      var inPop = e.target.closest('[data-sv-pop]');
      if (!inMore && !inPop) {
        closeMenus(null);
        closePops(null);
      }
    });
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      var dw = document.querySelector('[data-sv-drawer]:not([hidden])');
      if (dw) { closeDrawer(dw); return; }
      if (activeSubPop && !activeSubPop.hidden) {
        var back = activeSubBtn;
        closePops(null);
        if (back) back.focus();
        return;
      }
      var openPop = pops.filter(function (w) { return !w.querySelector('.sv-pop').hidden; })[0];
      if (openPop) {
        closePops(null);
        var pb = openPop.querySelector('[data-sv-pop-btn]');
        if (pb) pb.focus();
        return;
      }
      var openMenu = document.querySelector('[data-sl-more] .sl-menu:not([hidden])');
      if (openMenu) {
        closeMenus(null);
        openMenu.closest('[data-sl-more]').querySelector('[data-sl-more-btn]').focus();
      }
    });
    window.addEventListener('resize', function () {
      if (activeSubBtn && activeSubPop && !activeSubPop.hidden) {
        var menu = activeSubBtn.closest('.sl-menu');
        if (menu && !menu.hidden) positionSubmenu(activeSubBtn, menu, activeSubPop);
      }
    });

    // Clicar na linha abre a venda (menos em botões, links e menus).
    document.querySelectorAll('[data-sv-href]').forEach(function (row) {
      row.addEventListener('click', function (e) {
        if (e.target.closest('a, button, input, select, label, form, .sv-pop, .sl-menu, [data-sv-pop], [data-sl-more]')) return;
        window.location.href = row.getAttribute('data-sv-href');
      });
    });

    // Alterar data / prazo: mostra a data ali mesmo.
    document.querySelectorAll('[data-sv-date-open]').forEach(function (b) {
      b.addEventListener('click', function () {
        var line = b.closest('[data-sv-date]').querySelector('.sv-dateline'); line.hidden = !line.hidden;
        if (!line.hidden) line.querySelector('input').focus();
      });
    });

    // Lista curta de parcelas (e a gaveta): "Receber N · R$ X".
    document.querySelectorAll('[data-sv-pick]').forEach(function (form) {
      var btn = form.querySelector('[data-sv-pick-btn]'); var sum = form.querySelector('[data-sv-pick-sum]');
      function update() {
        var boxes = form.querySelectorAll('input[name="parcela"]:checked'); var total = 0;
        boxes.forEach(function (c) { total += parseFloat(c.getAttribute('data-amount')) || 0; });
        var n = boxes.length; btn.disabled = !n;
        var label = btn.querySelector('span');
        if (sum) { sum.textContent = n ? n + (n === 1 ? ' parcela · ' : ' parcelas · ') + brl(total) : 'Nenhuma parcela'; }
        else { label.textContent = n ? 'Receber ' + n + ' · ' + brl(total) : 'Selecione as parcelas'; }
        form.querySelectorAll('[data-sv-year]').forEach(function (yr) {
          var all = yr.querySelectorAll('input[name="parcela"]'); var on = yr.querySelectorAll('input[name="parcela"]:checked');
          var head = yr.querySelector('[data-sv-year-all] input');
          head.checked = all.length && on.length === all.length; head.indeterminate = on.length > 0 && on.length < all.length;
        });
      }
      form.addEventListener('change', update);
      form.querySelectorAll('[data-sv-year-all]').forEach(function (h) {
        var box = h.querySelector('input');
        h.addEventListener('click', function (e) { e.stopPropagation(); });
        box.addEventListener('change', function () {
          h.closest('[data-sv-year]').querySelectorAll('input[name="parcela"]').forEach(function (c) { c.checked = box.checked; });
          update();
        });
      });
      update();
    });

    // Muitas parcelas: "Receber as próximas [−] N [+]".
    document.querySelectorAll('[data-sv-step]').forEach(function (form) {
      var ids = JSON.parse(form.getAttribute('data-sv-step')); var amounts = JSON.parse(form.getAttribute('data-sv-step-amounts'));
      var nums = JSON.parse(form.getAttribute('data-sv-step-nums'));
      var out = form.querySelector('[data-sv-step-n]'); var sub = form.querySelector('[data-sv-step-sub]');
      var hidden = form.querySelector('[data-sv-step-ids]'); var btn = form.querySelector('[data-sv-step-btn] span');
      var minus = form.querySelector('[data-sv-step-minus]'); var plus = form.querySelector('[data-sv-step-plus]');
      var n = parseInt(form.querySelector('[data-sv-step-btn]').getAttribute('data-start'), 10) || 1;
      function render() {
        n = Math.max(1, Math.min(ids.length, n));
        var total = amounts.slice(0, n).reduce(function (a, b) { return a + b; }, 0);
        out.textContent = n; hidden.value = ids.slice(0, n).join(',');
        sub.textContent = (n === 1 ? nums[0] + 'ª' : nums[0] + 'ª a ' + nums[n - 1] + 'ª') + ' · ' + brl(total);
        btn.textContent = 'Receber ' + n + ' · ' + brl(total);
        minus.disabled = n <= 1; plus.disabled = n >= ids.length;
      }
      minus.addEventListener('click', function () { n -= 1; render(); });
      plus.addEventListener('click', function () { n += 1; render(); });
      var late = form.querySelector('[data-sv-step-late]');
      if (late) late.addEventListener('click', function () { n = parseInt(late.getAttribute('data-sv-step-late'), 10) || 1; render(); });
      render();
    });

    // Gaveta "Escolher parcelas".
    var lastTrigger = null;
    function closeDrawer(dw) { dw.hidden = true; document.body.style.overflow = ''; if (lastTrigger) lastTrigger.focus(); }
    document.querySelectorAll('[data-sv-drawer-open]').forEach(function (b) {
      b.addEventListener('click', function () {
        var dw = document.getElementById(b.getAttribute('data-sv-drawer-open')); if (!dw) return;
        closePops(null); lastTrigger = b; dw.hidden = false; document.body.style.overflow = 'hidden';
        var first = dw.querySelector('input[name="parcela"]'); if (first) first.focus();
      });
    });
    document.querySelectorAll('[data-sv-drawer-close]').forEach(function (b) {
      b.addEventListener('click', function () { closeDrawer(b.closest('[data-sv-drawer]')); });
    });

    // "Por página": muda ao escolher.
    document.querySelectorAll('[data-sv-autosubmit]').forEach(function (el) { el.addEventListener('change', function () { el.form.submit(); }); });

    // Aviso rápido: some sozinho (pausa com o mouse ou foco em cima).
    var toast = document.querySelector('[data-sv-toast]');
    if (toast) {
      var timer = null;
      function hide() { toast.classList.add('is-out'); setTimeout(function () { toast.remove(); }, 260); }
      function arm() { clearTimeout(timer); timer = setTimeout(hide, 7000); }
      toast.addEventListener('mouseenter', function () { clearTimeout(timer); });
      toast.addEventListener('mouseleave', arm);
      toast.addEventListener('focusin', function () { clearTimeout(timer); });
      toast.querySelector('[data-sv-toast-close]').addEventListener('click', hide);
      arm();
    }
  });
})();

/* Confirmação antes de excluir (forms com data-confirm). */
(function () {
  'use strict';
  document.addEventListener('submit', function (e) {
    var form = e.target;
    if (!form.matches || !form.matches('form[data-confirm]') || form.dataset.confirmed === '1') return;
    e.preventDefault();
    var dlg = document.createElement('dialog');
    dlg.className = 'lt-del';
    dlg.innerHTML = '<form method="dialog"><h2></h2><div class="lt-del-foot"><button type="submit" value="no" class="sf-btn sf-btn-secondary">Cancelar</button><button type="submit" value="yes" class="sf-btn lt-del-go">Excluir</button></div></form>';
    dlg.querySelector('h2').textContent = form.dataset.confirm;
    document.body.appendChild(dlg);
    dlg.addEventListener('close', function () {
      var ok = dlg.returnValue === 'yes';
      dlg.remove();
      if (ok) { form.dataset.confirmed = '1'; form.submit(); }
    });
    dlg.showModal();
  }, true);
})();
