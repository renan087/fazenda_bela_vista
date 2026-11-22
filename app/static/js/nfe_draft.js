(() => {
  'use strict';

  const copyButtons = document.querySelectorAll('[data-nf-copy]');
  copyButtons.forEach((btn) => {
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
      window.setTimeout(() => {
        btn.textContent = original;
      }, 2000);
    });
  });

  const lotToggleButtons = document.querySelectorAll('[data-nf-lots-toggle]');
  lotToggleButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const action = btn.getAttribute('data-nf-lots-toggle');
      const open = action === 'expand';
      document.querySelectorAll('[data-nf-lot]').forEach((el) => {
        el.open = open;
      });
    });
  });

  const navLinks = Array.from(document.querySelectorAll('[data-nf-nav-link]'));
  if (navLinks.length) {
    const byId = new Map();
    navLinks.forEach((link) => {
      const href = link.getAttribute('href') || '';
      if (href.startsWith('#')) {
        const target = document.getElementById(href.slice(1));
        if (target) byId.set(target, link);
      }
      link.addEventListener('click', () => {
        navLinks.forEach((l) => l.classList.remove('is-active'));
        link.classList.add('is-active');
      });
    });

    if ('IntersectionObserver' in window && byId.size) {
      const observer = new IntersectionObserver(
        (entries) => {
          const visible = entries.find((e) => e.isIntersecting);
          if (!visible) return;
          const active = byId.get(visible.target);
          if (!active) return;
          navLinks.forEach((l) => l.classList.remove('is-active'));
          active.classList.add('is-active');
        },
        { rootMargin: '-15% 0px -65% 0px', threshold: 0.05 }
      );
      byId.forEach((_link, section) => observer.observe(section));
    }
  }
})();
