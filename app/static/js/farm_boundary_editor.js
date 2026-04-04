/**
 * Editor de polígono do perímetro da fazenda.
 * Com GOOGLE_MAPS_API_KEY (window.__GOOGLE_MAPS_API_KEY): Google Maps satélite.
 * Sem chave: Leaflet + OpenStreetMap.
 */
(function () {
    'use strict';

    function extractPoints(geometry) {
        if (!geometry || typeof geometry !== 'object') return [];
        if (geometry.type === 'Polygon' && geometry.coordinates && geometry.coordinates[0]) {
            const ring = geometry.coordinates[0];
            return ring.slice(0, -1).map((c) => ({ lat: c[1], lng: c[0] }));
        }
        if (
            geometry.type === 'MultiPolygon' &&
            geometry.coordinates &&
            geometry.coordinates[0] &&
            geometry.coordinates[0][0]
        ) {
            const ring = geometry.coordinates[0][0];
            return ring.slice(0, -1).map((c) => ({ lat: c[1], lng: c[0] }));
        }
        return [];
    }

    function buildGeometry(points) {
        if (points.length < 3) return null;
        const closed = [...points.map((p) => [p.lng, p.lat]), [points[0].lng, points[0].lat]];
        return { type: 'Polygon', coordinates: [closed] };
    }

    function calculateArea(coords) {
        if (coords.length < 3) return 0;
        const avgLat = coords.reduce((s, p) => s + p.lat, 0) / coords.length;
        const metersPerLat = 111320;
        const metersPerLng = 111320 * Math.cos((avgLat * Math.PI) / 180);
        const projected = coords.map((p) => ({ x: p.lng * metersPerLng, y: p.lat * metersPerLat }));
        let area = 0;
        projected.forEach((point, index) => {
            const next = projected[(index + 1) % projected.length];
            area += point.x * next.y - next.x * point.y;
        });
        return Math.abs(area) / 2 / 10000;
    }

    function nullStub() {
        return {
            destroy() {},
            syncToHidden() {},
            invalidate() {},
        };
    }

    let googleMapsLoadPromise = null;

    function loadGoogleMapsScript(apiKey) {
        if (typeof window === 'undefined') return Promise.reject(new Error('no window'));
        if (window.google && window.google.maps && window.google.maps.Map) {
            return Promise.resolve();
        }
        if (googleMapsLoadPromise) return googleMapsLoadPromise;
        googleMapsLoadPromise = new Promise((resolve, reject) => {
            const s = document.createElement('script');
            s.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(apiKey)}&libraries=geometry`;
            s.async = true;
            s.defer = true;
            s.onload = () => resolve();
            s.onerror = () => {
                googleMapsLoadPromise = null;
                reject(new Error('Falha ao carregar Google Maps'));
            };
            document.head.appendChild(s);
        });
        return googleMapsLoadPromise;
    }

    function initLeafletEditor(options) {
        const { containerId, hiddenInput, initialGeometry, removePointButton, onStats } = options;
        const el = document.getElementById(containerId);
        if (!el || typeof L === 'undefined') {
            return nullStub();
        }

        const map = L.map(el, { scrollWheelZoom: true }).setView([-20.7442, -42.8721], 14);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap',
        }).addTo(map);

        let points = extractPoints(initialGeometry);
        let selectedIndex = null;
        let editLayer = null;
        const markerLayer = L.layerGroup().addTo(map);

        const markerIcon = (active) =>
            L.divIcon({
                className: '',
                html: `<div style="width:16px;height:16px;border-radius:9999px;background:${active ? '#e11d48' : '#446a36'};border:3px solid #fff;box-shadow:0 4px 12px rgba(15,23,42,.2)"></div>`,
                iconSize: [16, 16],
                iconAnchor: [8, 8],
            });

        const emitStats = () => {
            if (typeof onStats === 'function') {
                const area = calculateArea(points);
                onStats({ vertices: points.length, area, selectedIndex });
            }
        };

        const syncToHidden = () => {
            if (!hiddenInput) return;
            const geom = buildGeometry(points);
            hiddenInput.value = geom ? JSON.stringify(geom) : '';
            emitStats();
        };

        const renderEditor = () => {
            if (editLayer) {
                map.removeLayer(editLayer);
                editLayer = null;
            }
            markerLayer.clearLayers();
            if (points.length >= 2) {
                editLayer = L.polygon(points, {
                    color: '#e11d48',
                    weight: 3,
                    fillColor: '#fb7185',
                    fillOpacity: 0.18,
                }).addTo(map);
            }
            points.forEach((point, index) => {
                const marker = L.marker(point, { draggable: true, icon: markerIcon(index === selectedIndex) }).addTo(
                    markerLayer,
                );
                marker.on('click', (e) => {
                    if (e.originalEvent) L.DomEvent.stopPropagation(e.originalEvent);
                    selectedIndex = index;
                    renderEditor();
                });
                marker.on('drag', (event) => {
                    points[index] = event.target.getLatLng();
                    syncToHidden();
                    if (editLayer) editLayer.setLatLngs(points);
                });
                marker.on('dragend', () => renderEditor());
            });
            syncToHidden();
        };

        map.on('click', (event) => {
            points.push(event.latlng);
            selectedIndex = points.length - 1;
            renderEditor();
        });

        let removeHandler = null;
        if (removePointButton) {
            removeHandler = () => {
                if (selectedIndex === null || points.length <= 3) return;
                points.splice(selectedIndex, 1);
                selectedIndex = null;
                renderEditor();
            };
            removePointButton.addEventListener('click', removeHandler);
        }

        renderEditor();
        if (points.length) {
            const bounds = L.latLngBounds(points);
            if (bounds.isValid()) map.fitBounds(bounds.pad(0.28));
        }

        return {
            map,
            syncToHidden,
            invalidate() {
                window.requestAnimationFrame(() => {
                    map.invalidateSize();
                });
            },
            destroy() {
                if (removePointButton && removeHandler) {
                    removePointButton.removeEventListener('click', removeHandler);
                }
                try {
                    map.off();
                    map.remove();
                } catch (e) {
                    /* ignore */
                }
                el.innerHTML = '';
            },
        };
    }

    async function initGoogleEditor(options, apiKey) {
        const { containerId, hiddenInput, initialGeometry, removePointButton, onStats } = options;
        const el = document.getElementById(containerId);
        if (!el) {
            return nullStub();
        }

        await loadGoogleMapsScript(apiKey);

        const map = new google.maps.Map(el, {
            center: { lat: -20.7442, lng: -42.8721 },
            zoom: 14,
            mapTypeId: 'satellite',
            mapTypeControl: true,
            streetViewControl: false,
            rotateControl: false,
            fullscreenControl: true,
        });

        let points = extractPoints(initialGeometry).map((p) => ({ lat: p.lat, lng: p.lng }));
        let selectedIndex = null;
        let polygon = null;
        const markers = [];
        const listenerHandles = [];
        let skipNextMapClick = false;

        const emitStats = () => {
            if (typeof onStats === 'function') {
                const area = calculateArea(points);
                onStats({ vertices: points.length, area, selectedIndex });
            }
        };

        const syncToHidden = () => {
            if (!hiddenInput) return;
            const geom = buildGeometry(points);
            hiddenInput.value = geom ? JSON.stringify(geom) : '';
            emitStats();
        };

        const renderPolygon = () => {
            if (polygon) {
                polygon.setMap(null);
                polygon = null;
            }
            if (points.length >= 2) {
                polygon = new google.maps.Polygon({
                    paths: points,
                    strokeColor: '#e11d48',
                    strokeWeight: 3,
                    fillColor: '#fb7185',
                    fillOpacity: 0.18,
                    map,
                });
            }
        };

        const clearMarkers = () => {
            markers.forEach((m) => m.setMap(null));
            markers.length = 0;
        };

        const renderMarkers = () => {
            clearMarkers();
            points.forEach((pt, index) => {
                const marker = new google.maps.Marker({
                    position: pt,
                    map,
                    draggable: true,
                    icon: {
                        path: google.maps.SymbolPath.CIRCLE,
                        scale: 8,
                        fillColor: index === selectedIndex ? '#e11d48' : '#446a36',
                        fillOpacity: 1,
                        strokeColor: '#ffffff',
                        strokeWeight: 3,
                    },
                });
                listenerHandles.push(
                    google.maps.event.addListener(marker, 'click', () => {
                        skipNextMapClick = true;
                        window.setTimeout(() => {
                            skipNextMapClick = false;
                        }, 80);
                        selectedIndex = index;
                        renderAll();
                    }),
                );
                listenerHandles.push(
                    google.maps.event.addListener(marker, 'drag', () => {
                        const pos = marker.getPosition();
                        points[index] = { lat: pos.lat(), lng: pos.lng() };
                        syncToHidden();
                        if (polygon) polygon.setPath(points);
                    }),
                );
                listenerHandles.push(google.maps.event.addListener(marker, 'dragend', () => renderAll()));
                markers.push(marker);
            });
        };

        const renderAll = () => {
            renderPolygon();
            renderMarkers();
            syncToHidden();
        };

        listenerHandles.push(
            google.maps.event.addListener(map, 'click', (e) => {
                if (skipNextMapClick) return;
                points.push({ lat: e.latLng.lat(), lng: e.latLng.lng() });
                selectedIndex = points.length - 1;
                renderAll();
            }),
        );

        let removeHandler = null;
        if (removePointButton) {
            removeHandler = () => {
                if (selectedIndex === null || points.length <= 3) return;
                points.splice(selectedIndex, 1);
                selectedIndex = null;
                renderAll();
            };
            removePointButton.addEventListener('click', removeHandler);
        }

        renderAll();
        if (points.length) {
            const bounds = new google.maps.LatLngBounds();
            points.forEach((p) => bounds.extend(p));
            map.fitBounds(bounds, 48);
        }

        return {
            map,
            syncToHidden,
            invalidate() {
                window.requestAnimationFrame(() => {
                    if (window.google && window.google.maps) {
                        google.maps.event.trigger(map, 'resize');
                    }
                });
            },
            destroy() {
                listenerHandles.forEach((h) => google.maps.event.removeListener(h));
                listenerHandles.length = 0;
                clearMarkers();
                if (polygon) {
                    polygon.setMap(null);
                    polygon = null;
                }
                if (removePointButton && removeHandler) {
                    removePointButton.removeEventListener('click', removeHandler);
                }
                el.innerHTML = '';
            },
        };
    }

    /**
     * @returns {Promise<{ destroy: function, syncToHidden: function, invalidate: function }>}
     */
    window.initFarmBoundaryEditor = async function initFarmBoundaryEditor(options) {
        const key =
            typeof window !== 'undefined' && window.__GOOGLE_MAPS_API_KEY
                ? String(window.__GOOGLE_MAPS_API_KEY).trim()
                : '';
        if (key) {
            try {
                return await initGoogleEditor(options, key);
            } catch (err) {
                console.warn('[farm_boundary_editor] Google Maps indisponível, usando OpenStreetMap.', err);
            }
        }
        return initLeafletEditor(options);
    };

    window.extractFarmBoundaryPoints = extractPoints;
})();
