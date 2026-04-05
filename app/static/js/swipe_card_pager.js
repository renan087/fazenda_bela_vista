(() => {
    const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

    const createSwipePager = ({
        root,
        items = [],
        dotsContainer = null,
        scrollViewport = null,
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
        let intersectionObserver = null;
        let resizeRaf = null;

        root.classList.add('swipe-card-pager-surface');
        root.style.touchAction = 'pan-y';

        const shouldHandleSwipe = () => !mobileOnly || mediaQuery.matches;
        const isScrollCarousel = () =>
            scrollViewport instanceof HTMLElement && shouldHandleSwipe();

        const getPageSize = () => {
            const fallback = Math.max(1, Number(itemsPerPage) || 1);
            const mobileSize = Math.max(1, Number(itemsPerPageMobile) || fallback);
            const desktopSize = Math.max(1, Number(itemsPerPageDesktop) || fallback);
            return mediaQuery.matches ? mobileSize : desktopSize;
        };

        const disconnectObserver = () => {
            if (intersectionObserver) {
                intersectionObserver.disconnect();
                intersectionObserver = null;
            }
        };

        const layoutScrollSlides = () => {
            if (!isScrollCarousel()) return;
            const w = scrollViewport.clientWidth;
            if (w <= 0) return;
            items.forEach((el) => {
                el.style.flex = `0 0 ${w}px`;
                el.style.width = `${w}px`;
                el.style.maxWidth = `${w}px`;
                el.style.boxSizing = 'border-box';
            });
        };

        const scrollToPageIndex = (pageIndex, smooth) => {
            if (!isScrollCarousel()) return;
            const el = items[pageIndex];
            if (!el) return;
            const left = el.offsetLeft;
            scrollViewport.scrollTo({
                left,
                behavior: smooth ? 'smooth' : 'auto',
            });
        };

        const bindScrollObserver = () => {
            disconnectObserver();
            if (!isScrollCarousel()) return;
            intersectionObserver = new IntersectionObserver(
                (entries) => {
                    if (isTransitioning) return;
                    const candidates = entries
                        .filter((e) => e.isIntersecting && e.intersectionRatio >= 0.45)
                        .sort((a, b) => b.intersectionRatio - a.intersectionRatio);
                    if (!candidates.length) return;
                    const idx = items.indexOf(candidates[0].target);
                    if (idx >= 0 && idx !== currentPage) {
                        currentPage = idx;
                        syncDots();
                    }
                },
                { root: scrollViewport, rootMargin: '0px', threshold: [0.45, 0.55, 0.65] },
            );
            items.forEach((el) => intersectionObserver.observe(el));
        };

        const applyCarouselMode = () => {
            disconnectObserver();
            if (isScrollCarousel()) {
                root.classList.remove('swipe-card-pager-surface');
                root.classList.add('swipe-card-pager-scroll-track', 'swipe-card-pager-scroll-active');
                root.style.touchAction = 'pan-x';
                scrollViewport.classList.add(
                    'swipe-card-pager-scroll-viewport',
                    'is-carousel-active',
                );
                items.forEach((item) => item.classList.remove('hidden'));
            } else {
                scrollViewport?.classList.remove('swipe-card-pager-scroll-viewport', 'is-carousel-active');
                root.classList.remove('swipe-card-pager-scroll-track', 'swipe-card-pager-scroll-active');
                root.classList.add('swipe-card-pager-surface');
                root.style.touchAction = 'pan-y';
                items.forEach((el) => {
                    el.style.flex = '';
                    el.style.width = '';
                    el.style.maxWidth = '';
                });
            }
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

        const renderPageDesktop = (targetPage) => {
            currentPage = targetPage;
            items.forEach((item, index) => {
                const page = Math.floor(index / currentPageSize);
                item.classList.toggle('hidden', page !== currentPage);
            });
            syncDots();
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

            applyCarouselMode();

            if (isScrollCarousel()) {
                layoutScrollSlides();

                const finishScroll = (smooth) => {
                    window.requestAnimationFrame(() => {
                        layoutScrollSlides();
                        scrollToPageIndex(nextPage, smooth);
                        bindScrollObserver();
                    });
                };

                if (!direction || !(dotsContainer instanceof HTMLElement)) {
                    currentPage = nextPage;
                    syncDots();
                    finishScroll(Boolean(direction));
                    return;
                }

                if (isTransitioning) return;
                isTransitioning = true;
                clearDotsAnimation();
                window.clearTimeout(dotsAnimationTimer);
                void dotsContainer.offsetWidth;
                dotsContainer.classList.add(direction === 'next' ? 'is-transitioning-next' : 'is-transitioning-prev');

                dotsAnimationTimer = window.setTimeout(() => {
                    currentPage = nextPage;
                    syncDots();
                    finishScroll(true);
                    animate(direction);
                    clearDotsAnimation();
                    void dotsContainer.offsetWidth;
                    dotsContainer.classList.add(direction === 'next' ? 'is-settling-next' : 'is-settling-prev');
                    dotsAnimationTimer = window.setTimeout(() => {
                        clearDotsAnimation();
                        isTransitioning = false;
                    }, 440);
                }, 180);
                return;
            }

            if (!direction || !(dotsContainer instanceof HTMLElement) || !shouldHandleSwipe()) {
                renderPageDesktop(nextPage);
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
                renderPageDesktop(nextPage);
                animate(direction);
                clearDotsAnimation();
                void dotsContainer.offsetWidth;
                dotsContainer.classList.add(direction === 'next' ? 'is-settling-next' : 'is-settling-prev');
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

        root.addEventListener(
            'touchstart',
            (event) => {
                if (isScrollCarousel()) return;
                if (!shouldHandleSwipe() || isTransitioning || event.touches.length !== 1) return;
                if (event.target instanceof Element && event.target.closest('a, button, input, select, textarea, label'))
                    return;
                const touch = event.touches[0];
                tracking = true;
                lockDirection = null;
                startX = touch.clientX;
                startY = touch.clientY;
                lastX = touch.clientX;
                lastY = touch.clientY;
            },
            { passive: true },
        );

        root.addEventListener(
            'touchmove',
            (event) => {
                if (isScrollCarousel()) return;
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
            },
            { passive: false },
        );

        root.addEventListener('touchend', () => {
            if (isScrollCarousel()) {
                resetTracking();
                return;
            }
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

        const scheduleLayout = () => {
            if (!isScrollCarousel()) return;
            if (resizeRaf !== null) return;
            resizeRaf = window.requestAnimationFrame(() => {
                resizeRaf = null;
                layoutScrollSlides();
                scrollToPageIndex(currentPage, false);
            });
        };

        window.addEventListener('resize', scheduleLayout, { passive: true });

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
