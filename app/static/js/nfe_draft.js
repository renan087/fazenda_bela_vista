(() => {
  'use strict';

  // Copiar chave simulada
  document.querySelectorAll('[data-nf-copy]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const text = btn.getAttribute('data-nf-copy') || '';
      if (!text) return;
      const original = btn.textContent;
      try {
        await navigator.clipboard.writeText(text);
        btn.textContent = 'Chave copiada';
      } catch (_err) {
        const helper = document.createElement('textarea');
        helper.value = text;
        helper.setAttribute('readonly', '');
        helper.style.position = 'absolute';
        helper.style.left = '-9999px';
        document.body.appendChild(helper);
        helper.select();
        document.execCommand('copy');
        document.body.removeChild(helper);
        btn.textContent = 'Chave copiada';
      }
      window.setTimeout(() => { btn.textContent = original; }, 2000);
    });
  });

  // Expandir / recolher lotes no bloco avançado
  document.querySelectorAll('[data-nf-lots-toggle]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const open = btn.getAttribute('data-nf-lots-toggle') === 'expand';
      document.querySelectorAll('[data-nf-lot]').forEach((el) => { el.open = open; });
    });
  });

  // Modais elegantes (Vendedor / Comprador)
  document.querySelectorAll('[data-nf-open-modal]').forEach((trigger) => {
    trigger.addEventListener('click', () => {
      const id = trigger.getAttribute('data-nf-open-modal');
      const dlg = id ? document.getElementById(id) : null;
      if (!dlg) return;
      if (typeof dlg.showModal === 'function') dlg.showModal();
      else dlg.setAttribute('open', '');
    });
  });
  document.querySelectorAll('dialog.nf-dialog').forEach((dlg) => {
    dlg.addEventListener('click', (e) => {
      if (e.target === dlg) dlg.close();
    });
    dlg.querySelectorAll('[data-nf-close-modal]').forEach((btn) => {
      btn.addEventListener('click', () => dlg.close());
    });
  });

  // Toggles inline (ex.: alterar produto / alterar entrega)
  document.querySelectorAll('[data-nf-toggle]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const targetId = btn.getAttribute('data-nf-toggle');
      const box = targetId ? document.getElementById(targetId) : null;
      if (!box) return;
      const opened = box.classList.toggle('hidden') === false;
      btn.setAttribute('aria-expanded', String(opened));
    });
  });
})();
