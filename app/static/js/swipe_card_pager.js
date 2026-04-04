(() => {
    const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

    const createSwipePager = ({
        root,
        items = [],
        dotsContainer = null,
        itemsPerPage = 1,
        mobileOnly = true,
        mobileBreakpoint = '(max-width: 899px)',
        threshold = 52,
    }) => {
        if (!(root instanceof HTMLElement) || !items.length) return null;

        const mediaQuery = window.matchMedia(mobileBreakpoint);
        const pageSize = Math.max(1, Number(itemsPerPage) || 1);
        const totalPages = Math.ceil(items.length / pageSize);
        let currentPage = 0;
        let startX = 0;
        let startY = 0;
        let lastX = 0;
        let lastY = 0;
        let tracking = false;
        let lockDirection = null;
        let animationTimer = null;

        root.classList.add('swipe-card-pager-surface');
        root.style.touchAction = 'pan-y';

        const shouldHandleSwipe = () => !mobileOnly || mediaQuery.matches;

        const animate = (direction) => {
            if (!direction) return;
            root.classList.remove('is-page-transition-next', 'is-page-transition-prev');
            window.clearTimeout(animationTimer);
            void root.offsetWidth;
            root.classList.add(direction === 'next' ? 'is-page-transition-next' : 'is-page-transition-prev');
            animationTimer = window.setTimeout(() => {
                root.classList.remove('is-page-transition-next', 'is-page-transition-prev');
            }, 240);
        };

        const syncDots = () => {
            if (!(dotsContainer instanceof HTMLElement)) return;
            const dots = Array.from(dotsContainer.querySelectorAll('[data-swipe-pager-dot]'));
            dots.forEach((dot, index) => {
                const active = index === currentPage;
                dot.classList.toggle('bg-brand-500', active);
                dot.classList.toggle('scale-110', active);
                dot.classList.toggle('bg-slate-300', !active);
                dot.setAttribute('aria-current', active ? 'true' : 'false');
            });
        };

        const setPage = (pageIndex, direction = null) => {
            const nextPage = clamp(pageIndex, 0, totalPages - 1);
            if (nextPage === currentPage && direction) return;
            currentPage = nextPage;
            items.forEach((item, index) => {
                const page = Math.floor(index / pageSize);
                item.classList.toggle('hidden', page !== currentPage);
            });
            syncDots();
            animate(direction);
        };

        if (dotsContainer instanceof HTMLElement) {
            dotsContainer.innerHTML = '';
            for (let pageIndex = 0; pageIndex < totalPages; pageIndex += 1) {
                const dot = document.createElement('button');
                dot.type = 'button';
                dot.className = 'h-2.5 w-2.5 rounded-full bg-slate-300 transition hover:bg-brand-300';
                dot.setAttribute('aria-label', `Ir para página ${pageIndex + 1}`);
                dot.dataset.swipePagerDot = String(pageIndex);
                dot.addEventListener('click', () => {
                    const direction = pageIndex > currentPage ? 'next' : 'prev';
                    setPage(pageIndex, direction);
                });
                dotsContainer.appendChild(dot);
            }
        }

        const resetTracking = () => {
            tracking = false;
            lockDirection = null;
            startX = 0;
            startY = 0;
            lastX = 0;
            lastY = 0;
        };

        root.addEventListener('touchstart', (event) => {
            if (!shouldHandleSwipe() || event.touches.length !== 1) return;
            if (event.target instanceof Element && event.target.closest('a, button, input, select, textarea, label')) return;
            const touch = event.touches[0];
            tracking = true;
            lockDirection = null;
            startX = touch.clientX;
            startY = touch.clientY;
            lastX = touch.clientX;
            lastY = touch.clientY;
        }, { passive: true });

        root.addEventListener('touchmove', (event) => {
            if (!tracking || !shouldHandleSwipe() || event.touches.length !== 1) return;
            const touch = event.touches[0];
            lastX = touch.clientX;
            lastY = touch.clientY;
            const deltaX = lastX - startX;
            const deltaY = lastY - startY;
            if (!lockDirection) {
                if (Math.abs(deltaX) > 10 && Math.abs(deltaX) > Math.abs(deltaY) + 6) {
                    lockDirection = 'horizontal';
                } else if (Math.abs(deltaY) > 10) {
                    lockDirection = 'vertical';
                }
            }
            if (lockDirection === 'horizontal') {
                event.preventDefault();
            }
        }, { passive: false });

        root.addEventListener('touchend', () => {
            if (!tracking || !shouldHandleSwipe()) {
                resetTracking();
                return;
            }
            const deltaX = lastX - startX;
            const deltaY = lastY - startY;
            const horizontalIntent = Math.abs(deltaX) > threshold && Math.abs(deltaX) > Math.abs(deltaY) * 1.2;
            if (horizontalIntent) {
                if (deltaX < 0 && currentPage < totalPages - 1) {
                    setPage(currentPage + 1, 'next');
                } else if (deltaX > 0 && currentPage > 0) {
                    setPage(currentPage - 1, 'prev');
                }
            }
            resetTracking();
        });

        root.addEventListener('touchcancel', resetTracking);
        mediaQuery.addEventListener('change', () => setPage(currentPage));
        setPage(0);

        return {
            setPage,
            getPage: () => currentPage,
            getTotalPages: () => totalPages,
        };
    };

    window.SiSFarmSwipePager = {
        create: createSwipePager,
    };
})();
