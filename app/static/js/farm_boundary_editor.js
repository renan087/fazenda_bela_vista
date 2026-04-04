/**
 * Editor de polígono (Leaflet) para perímetro de fazenda — usado no formulário e no modal de prévia.
 * Depende de L (Leaflet) global.
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

    function markerIcon(active) {
        return L.divIcon({
            className: '',
            html: `<div style="width:16px;height:16px;border-radius:9999px;background:${active ? '#e11d48' : '#446a36'};border:3px solid #fff;box-shadow:0 4px 12px rgba(15,23,42,.2)"></div>`,
            iconSize: [16, 16],
            iconAnchor: [8, 8],
        });
    }

    /**
     * @param {object} options
     * @param {string} options.containerId
     * @param {HTMLInputElement|null} options.hiddenInput
     * @param {object|null} options.initialGeometry
     * @param {HTMLButtonElement|null} options.removePointButton
     * @param {function(number):void} [options.onStats]
     */
    window.initFarmBoundaryEditor = function initFarmBoundaryEditor(options) {
        const { containerId, hiddenInput, initialGeometry, removePointButton, onStats } = options;
        const el = document.getElementById(containerId);
        if (!el || typeof L === 'undefined') {
            return {
                destroy() {},
                syncToHidden() {},
                invalidate() {},
            };
        }

        const map = L.map(el, { scrollWheelZoom: true }).setView([-20.7442, -42.8721], 14);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap',
        }).addTo(map);

        let points = extractPoints(initialGeometry);
        let selectedIndex = null;
        let editLayer = null;
        const markerLayer = L.layerGroup().addTo(map);

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

        if (removePointButton) {
            removePointButton.addEventListener('click', () => {
                if (selectedIndex === null || points.length <= 3) return;
                points.splice(selectedIndex, 1);
                selectedIndex = null;
                renderEditor();
            });
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
                try {
                    map.off();
                    map.remove();
                } catch (e) {
                    /* ignore */
                }
                el.innerHTML = '';
            },
        };
    };

    window.extractFarmBoundaryPoints = extractPoints;
})();
