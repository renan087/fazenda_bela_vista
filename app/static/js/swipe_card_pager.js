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
        allowInteractiveSwipe = false,
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
        let programmaticScroll = false;
        let scrollDotsRaf = null;
        let lastCarouselScrollLeft = null;
        let lastCarouselPullNext = true;
        let suppressNextClick = false;

        const listenerAbort = new AbortController();
        const listenerSignal = listenerAbort.signal;

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
            programmaticScroll = true;
            const left = el.offsetLeft;
            scrollViewport.scrollTo({
                left,
                behavior: smooth ? 'smooth' : 'auto',
            });
            if (!smooth) {
                window.requestAnimationFrame(() => {
                    window.requestAnimationFrame(() => {
                        programmaticScroll = false;
                        if (isScrollCarousel()) updateDotsFromScroll();
                    });
                });
            } else {
                const clearProgrammaticScroll = () => {
                    if (!programmaticScroll) return;
                    programmaticScroll = false;
                    if (isScrollCarousel()) updateDotsFromScroll();
                };
                scrollViewport.addEventListener('scrollend', clearProgrammaticScroll, { once: true, passive: true });
                window.setTimeout(clearProgrammaticScroll, 650);
            }
        };

        const CAROUSEL_SIDE_DOT_HIDE_STRETCH = 0.26;

        const setCarouselDotsLinkedVars = (stretch, pullNext) => {
            if (!(dotsContainer instanceof HTMLElement)) return;
            dotsContainer.style.setProperty('--carousel-dot-stretch', String(stretch));
            dotsContainer.classList.toggle('swipe-carousel-pull-next', pullNext);
            dotsContainer.classList.toggle('swipe-carousel-pull-prev', !pullNext);
            const hideAdjacent = stretch >= CAROUSEL_SIDE_DOT_HIDE_STRETCH;
            dotsContainer.classList.toggle('swipe-carousel-hide-adjacent-next', hideAdjacent && pullNext);
            dotsContainer.classList.toggle('swipe-carousel-hide-adjacent-prev', hideAdjacent && !pullNext);
        };

        const clearCarouselDotsLinkedVars = () => {
            if (!(dotsContainer instanceof HTMLElement)) return;
            dotsContainer.classList.remove(
                'swipe-carousel-dots-linked',
                'swipe-carousel-pull-next',
                'swipe-carousel-pull-prev',
                'swipe-carousel-hide-adjacent-next',
                'swipe-carousel-hide-adjacent-prev',
            );
            dotsContainer.style.removeProperty('--carousel-dot-stretch');
            lastCarouselScrollLeft = null;
        };

        const updateDotsFromScroll = () => {
            if (!isScrollCarousel() || !(dotsContainer instanceof HTMLElement)) return;
            if (isTransitioning) return;

            const n = items.length;
            const left = scrollViewport.scrollLeft;

            if (n <= 1) {
                currentPage = 0;
                dotsContainer.classList.add('swipe-carousel-dots-linked');
                setCarouselDotsLinkedVars(0, lastCarouselPullNext);
                syncDots();
                return;
            }

            let pullNext = lastCarouselPullNext;
            if (lastCarouselScrollLeft !== null) {
                const delta = left - lastCarouselScrollLeft;
                if (Math.abs(delta) > 0.35) {
                    pullNext = delta > 0;
                    lastCarouselPullNext = pullNext;
                }
            }
            lastCarouselScrollLeft = left;

            let i = 0;
            for (; i < n - 1; i += 1) {
                if (items[i + 1].offsetLeft > left + 0.5) break;
            }
            i = Math.min(i, n - 2);

            const start = items[i].offsetLeft;
            const end = items[i + 1].offsetLeft;
            const span = end - start;
            const t = span > 0 ? clamp((left - start) / span, 0, 1) : 0;
            const stretchMag = 1 - Math.abs(t - 0.5) * 2;
            currentPage = clamp(Math.round(i + t), 0, n - 1);

            dotsContainer.classList.add('swipe-carousel-dots-linked');
            setCarouselDotsLinkedVars(stretchMag, pullNext);
            syncDots();
        };

        const scheduleDotsFromScroll = () => {
            if (!isScrollCarousel() || isTransitioning) return;
            if (scrollDotsRaf !== null) return;
            scrollDotsRaf = window.requestAnimationFrame(() => {
                scrollDotsRaf = null;
                if (!isScrollCarousel() || isTransitioning) return;
                updateDotsFromScroll();
            });
        };

        const applyCarouselMode = () => {
            disconnectObserver();
            if (isScrollCarousel()) {
                root.classList.remove('swipe-card-pager-surface');
                root.classList.add('swipe-card-pager-scroll-track', 'swipe-card-pager-scroll-active');
                root.style.touchAction = 'pan-x pan-y';
                scrollViewport.style.touchAction = 'pan-x pan-y';
                scrollViewport.classList.add(
                    'swipe-card-pager-scroll-viewport',
                    'is-carousel-active',
                );
                items.forEach((item) => item.classList.remove('hidden'));
            } else {
                scrollViewport?.classList.remove('swipe-card-pager-scroll-viewport', 'is-carousel-active');
                scrollViewport?.style.removeProperty('touch-action');
                clearCarouselDotsLinkedVars();
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
                        if (!smooth) updateDotsFromScroll();
                    });
                };

                if (!direction || !(dotsContainer instanceof HTMLElement)) {
                    currentPage = nextPage;
                    syncDots();
                    finishScroll(Boolean(direction));
                    return;
                }

                clearDotsAnimation();
                finishScroll(true);
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
                if (
                    !allowInteractiveSwipe &&
                    event.target instanceof Element &&
                    event.target.closest('a, button, input, select, textarea, label')
                )
                    return;
                const touch = event.touches[0];
                tracking = true;
                lockDirection = null;
                startX = touch.clientX;
                startY = touch.clientY;
                lastX = touch.clientX;
                lastY = touch.clientY;
            },
            { passive: true, signal: listenerSignal },
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
            { passive: false, signal: listenerSignal },
        );

        root.addEventListener(
            'touchend',
            () => {
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
                        suppressNextClick = true;
                        setPage(currentPage + 1, 'next');
                    } else if (deltaX > 0 && currentPage > 0) {
                        suppressNextClick = true;
                        setPage(currentPage - 1, 'prev');
                    }
                }
                resetTracking();
            },
            { signal: listenerSignal },
        );

        root.addEventListener('touchcancel', resetTracking, { signal: listenerSignal });

        root.addEventListener(
            'click',
            (event) => {
                if (!suppressNextClick) return;
                suppressNextClick = false;
                event.preventDefault();
                event.stopPropagation();
            },
            { capture: true, signal: listenerSignal },
        );

        const scheduleLayout = () => {
            if (!isScrollCarousel()) return;
            if (resizeRaf !== null) return;
            resizeRaf = window.requestAnimationFrame(() => {
                resizeRaf = null;
                layoutScrollSlides();
                scrollToPageIndex(currentPage, false);
                updateDotsFromScroll();
            });
        };

        window.addEventListener('resize', scheduleLayout, { passive: true, signal: listenerSignal });

        const onMediaQueryChange = () => {
            if (isTransitioning) return;
            setPage(currentPage);
        };
        mediaQuery.addEventListener('change', onMediaQueryChange, { signal: listenerSignal });
        setPage(0);

        if (scrollViewport instanceof HTMLElement) {
            scrollViewport.addEventListener(
                'scroll',
                () => {
                    if (!isScrollCarousel()) return;
                    scheduleDotsFromScroll();
                },
                { passive: true, signal: listenerSignal },
            );

            scrollViewport.addEventListener(
                'scrollend',
                () => {
                    if (!isScrollCarousel() || isTransitioning) return;
                    updateDotsFromScroll();
                },
                { passive: true, signal: listenerSignal },
            );
        }

        const destroy = () => {
            listenerAbort.abort();
            window.clearTimeout(animationTimer);
            window.clearTimeout(dotsAnimationTimer);
            disconnectObserver();
            clearCarouselDotsLinkedVars();
            if (scrollViewport instanceof HTMLElement) {
                scrollViewport.classList.remove('swipe-card-pager-scroll-viewport', 'is-carousel-active');
                scrollViewport.style.removeProperty('touch-action');
                scrollViewport.scrollLeft = 0;
            }
            root.classList.remove(
                'swipe-card-pager-surface',
                'swipe-card-pager-scroll-track',
                'swipe-card-pager-scroll-active',
                'is-page-transition-next',
                'is-page-transition-prev',
            );
            root.style.removeProperty('touch-action');
            items.forEach((el) => {
                el.classList.remove('hidden');
                el.style.removeProperty('flex');
                el.style.removeProperty('width');
                el.style.removeProperty('max-width');
            });
            if (dotsContainer instanceof HTMLElement) {
                dotsContainer.innerHTML = '';
                dotsContainer.classList.remove(
                    'swipe-carousel-dots-linked',
                    'swipe-carousel-pull-next',
                    'swipe-carousel-pull-prev',
                    'swipe-carousel-hide-adjacent-next',
                    'swipe-carousel-hide-adjacent-prev',
                    'is-transitioning-next',
                    'is-transitioning-prev',
                    'is-settling-next',
                    'is-settling-prev',
                );
                dotsContainer.style.removeProperty('--carousel-dot-stretch');
            }
        };

        return {
            setPage,
            getPage: () => currentPage,
            getTotalPages: () => currentTotalPages,
            destroy,
        };
    };

    window.SiSFarmSwipePager = {
        create: createSwipePager,
    };
})();
