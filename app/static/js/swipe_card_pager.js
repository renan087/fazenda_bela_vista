(() => {
    const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

    const createSwipePager = ({
        root,
        items = [],
        dotsContainer = null,
        itemsPerPage = 1,
        itemsPerPageMobile = null,
        itemsPerPageDesktop = null,
        mobileOnly = true,
        mobileBreakpoint = '(max-width: 899px)',
        threshold = 52,
    }) => {
        if (!(root instanceof HTMLElement) || !items.length) return null;

        const mediaQuery = window.matchMedia(mobileBreakpoint);
        let currentPage = 0;
        let startX = 0;
        let startY = 0;
        let lastX = 0;
        let lastY = 0;
        let tracking = false;
        let lockDirection = null;
        let animationTimer = null;
        let dotsAnimationTimer = null;
        let isTransitioning = false;
        let currentPageSize = 1;
        let currentTotalPages = 1;

        root.classList.add('swipe-card-pager-surface');
        root.style.touchAction = 'pan-y';

        const shouldHandleSwipe = () => !mobileOnly || mediaQuery.matches;
        const getPageSize = () => {
            const fallback = Math.max(1, Number(itemsPerPage) || 1);
            const mobileSize = Math.max(1, Number(itemsPerPageMobile) || fallback);
            const desktopSize = Math.max(1, Number(itemsPerPageDesktop) || fallback);
            return mediaQuery.matches ? mobileSize : desktopSize;
        };

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

        const clearDotsAnimation = () => {
            if (!(dotsContainer instanceof HTMLElement)) return;
            dotsContainer.classList.remove(
                'is-pre-settling-prev',
                'is-transitioning-next',
                'is-transitioning-prev',
                'is-settling-next',
                'is-settling-prev',
            );
        };

        const buildDots = () => {
            if (!(dotsContainer instanceof HTMLElement)) return;
            dotsContainer.innerHTML = '';
            for (let slotIndex = 0; slotIndex < 3; slotIndex += 1) {
                const dot = document.createElement('button');
                dot.type = 'button';
                dot.className = 'swipe-card-pager-dot';
                dot.dataset.swipePagerSlot = String(slotIndex);
                dot.addEventListener('click', () => {
                    const targetPage = Number(dot.dataset.pageIndex || '');
                    if (!Number.isInteger(targetPage)) return;
                    const direction = targetPage > currentPage ? 'next' : 'prev';
                    setPage(targetPage, direction);
                });
                dotsContainer.appendChild(dot);
            }
        };

        const syncDots = () => {
            if (!(dotsContainer instanceof HTMLElement)) return;
            const dots = Array.from(dotsContainer.querySelectorAll('[data-swipe-pager-slot]'));
            const slotPages = [
                currentPage > 0 ? currentPage - 1 : null,
                currentPage,
                currentPage < currentTotalPages - 1 ? currentPage + 1 : null,
            ];
            const slotRoles = ['prev', 'current', 'next'];

            dots.forEach((dot, index) => {
                const pageIndex = slotPages[index];
                const role = slotRoles[index];
                const isVisible = Number.isInteger(pageIndex);
                const active = role === 'current';
                dot.classList.toggle('hidden', !isVisible);
                dot.classList.toggle('is-active', active);
                dot.classList.toggle('is-inactive', isVisible && !active);
                dot.dataset.role = role;
                if (!isVisible) {
                    dot.removeAttribute('data-page-index');
                    dot.removeAttribute('aria-label');
                    dot.removeAttribute('aria-current');
                    dot.disabled = true;
                    return;
                }
                dot.dataset.pageIndex = String(pageIndex);
                dot.setAttribute('aria-label', active ? `Página atual ${pageIndex + 1}` : `Ir para página ${pageIndex + 1}`);
                dot.setAttribute('aria-current', active ? 'true' : 'false');
                dot.disabled = active;
            });
        };

        const setPage = (pageIndex, direction = null) => {
            const pageSize = getPageSize();
            if (pageSize !== currentPageSize) {
                currentPageSize = pageSize;
                currentTotalPages = Math.ceil(items.length / currentPageSize);
                buildDots();
            }
            const nextPage = clamp(pageIndex, 0, currentTotalPages - 1);
            if (nextPage === currentPage && direction) return;

            const renderPage = (targetPage) => {
                currentPage = targetPage;
                items.forEach((item, index) => {
                    const page = Math.floor(index / currentPageSize);
                    item.classList.toggle('hidden', page !== currentPage);
                });
                syncDots();
            };

            if (!direction || !(dotsContainer instanceof HTMLElement) || !shouldHandleSwipe()) {
                renderPage(nextPage);
                animate(direction);
                return;
            }

            if (isTransitioning) return;
            isTransitioning = true;
            clearDotsAnimation();
            window.clearTimeout(dotsAnimationTimer);
            void dotsContainer.offsetWidth;
            dotsContainer.classList.add(direction === 'next' ? 'is-transitioning-next' : 'is-transitioning-prev');

            dotsAnimationTimer = window.setTimeout(() => {
                renderPage(nextPage);
                animate(direction);
                clearDotsAnimation();
                void dotsContainer.offsetWidth;
                if (direction === 'prev') {
                    dotsContainer.classList.add('is-pre-settling-prev');
                    void dotsContainer.offsetWidth;
                }
                dotsContainer.classList.add(direction === 'next' ? 'is-settling-next' : 'is-settling-prev');
                if (direction === 'prev') {
                    dotsContainer.classList.remove('is-pre-settling-prev');
                }
                dotsAnimationTimer = window.setTimeout(() => {
                    clearDotsAnimation();
                    isTransitioning = false;
                }, 440);
            }, 180);
        };

        currentPageSize = getPageSize();
        currentTotalPages = Math.ceil(items.length / currentPageSize);
        buildDots();

        const resetTracking = () => {
            tracking = false;
            lockDirection = null;
            startX = 0;
            startY = 0;
            lastX = 0;
            lastY = 0;
        };

        root.addEventListener('touchstart', (event) => {
            if (!shouldHandleSwipe() || isTransitioning || event.touches.length !== 1) return;
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
                if (deltaX < 0 && currentPage < currentTotalPages - 1) {
                    setPage(currentPage + 1, 'next');
                } else if (deltaX > 0 && currentPage > 0) {
                    setPage(currentPage - 1, 'prev');
                }
            }
            resetTracking();
        });

        root.addEventListener('touchcancel', resetTracking);
        mediaQuery.addEventListener('change', () => {
            if (isTransitioning) return;
            setPage(currentPage);
        });
        setPage(0);

        return {
            setPage,
            getPage: () => currentPage,
            getTotalPages: () => currentTotalPages,
        };
    };

    window.SiSFarmSwipePager = {
        create: createSwipePager,
    };
})();
