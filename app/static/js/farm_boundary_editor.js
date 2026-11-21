/**
 * Editor de polígono do perímetro da fazenda.
 * Com GOOGLE_MAPS_API_KEY (window.__GOOGLE_MAPS_API_KEY): Google Maps satélite.
 * Sem chave: Leaflet + OpenStreetMap.
 */
(function () {
    'use strict';

    function extractPoints(geometry) {
        if (!geometry || typeof geometry !== 'object') return [];
        if (geometry.type === 'Feature' && geometry.geometry) {
            return extractPoints(geometry.geometry);
        }
        if (geometry.type === 'FeatureCollection' && geometry.features && geometry.features[0]) {
            return extractPoints(geometry.features[0]);
        }
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

    /* Limite do setor: ponto fora do perímetro de referência (fazenda) vai para a divisa mais próxima. */
    function pointInRing(pt, ring) {
        let inside = false;
        for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
            const a = ring[i];
            const b = ring[j];
            if ((a.lat > pt.lat) !== (b.lat > pt.lat)
                && pt.lng < ((b.lng - a.lng) * (pt.lat - a.lat)) / ((b.lat - a.lat) || 1e-12) + a.lng) {
                inside = !inside;
            }
        }
        return inside;
    }

    function nearestOnRing(pt, ring) {
        let best = null;
        let bestDist = Infinity;
        for (let i = 0; i < ring.length; i += 1) {
            const a = ring[i];
            const b = ring[(i + 1) % ring.length];
            const dx = b.lng - a.lng;
            const dy = b.lat - a.lat;
            const len = dx * dx + dy * dy;
            let t = len ? ((pt.lng - a.lng) * dx + (pt.lat - a.lat) * dy) / len : 0;
            t = Math.max(0, Math.min(1, t));
            const q = { lat: a.lat + t * dy, lng: a.lng + t * dx };
            const dist = (q.lat - pt.lat) ** 2 + (q.lng - pt.lng) ** 2;
            if (dist < bestDist) {
                bestDist = dist;
                best = q;
            }
        }
        return best;
    }

    function makeClamp(refPts, options) {
        if (!options || !options.clipToReference || !refPts || refPts.length < 3) return (pt) => pt;
        return (pt) => {
            if (pointInRing(pt, refPts)) return pt;
            const q = nearestOnRing(pt, refPts) || pt;
            if (typeof options.onClamp === 'function') options.onClamp(q);
            return q;
        };
    }

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

    /** Estilos para outros setores (somente leitura), distintos do limite da fazenda (verde) e do polígono em edição (rosa). */
    const CONTEXT_PLOT_LEAFLET_STYLES = [
        { color: '#0369a1', weight: 2, fillColor: '#38bdf8', fillOpacity: 0.32 },
        { color: '#6d28d9', weight: 2, fillColor: '#a78bfa', fillOpacity: 0.3 },
        { color: '#b45309', weight: 2, fillColor: '#fbbf24', fillOpacity: 0.3 },
        { color: '#0f766e', weight: 2, fillColor: '#2dd4bf', fillOpacity: 0.28 },
        { color: '#1d4ed8', weight: 2, fillColor: '#60a5fa', fillOpacity: 0.28 },
        { color: '#a21caf', weight: 2, fillColor: '#e879f9', fillOpacity: 0.26 },
    ];

    const CONTEXT_PLOT_GOOGLE_STYLES = [
        { strokeColor: '#0369a1', fillColor: '#38bdf8', fillOpacity: 0.32 },
        { strokeColor: '#6d28d9', fillColor: '#a78bfa', fillOpacity: 0.3 },
        { strokeColor: '#b45309', fillColor: '#fbbf24', fillOpacity: 0.3 },
        { strokeColor: '#0f766e', fillColor: '#2dd4bf', fillOpacity: 0.28 },
        { strokeColor: '#1d4ed8', fillColor: '#60a5fa', fillOpacity: 0.28 },
        { strokeColor: '#a21caf', fillColor: '#e879f9', fillOpacity: 0.26 },
    ];

    /** @param {object} [styleOverride] opções de estilo Leaflet (interactive sempre false) */
    function leafletGeoJsonLayer(geometry, styleOverride) {
        if (!geometry || typeof geometry !== 'object') return null;
        const baseStyle = {
            color: '#418436',
            weight: 2,
            fillColor: '#5BB34A',
            fillOpacity: 0.37,
            interactive: false,
        };
        const style = styleOverride ? { ...baseStyle, ...styleOverride, interactive: false } : baseStyle;
        try {
            if (geometry.type === 'Feature' || geometry.type === 'FeatureCollection') {
                return L.geoJSON(geometry, {
                    interactive: false,
                    style,
                });
            }
            if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
                return L.geoJSON(
                    { type: 'Feature', properties: {}, geometry },
                    {
                        interactive: false,
                        style,
                    },
                );
            }
        } catch (e) {
            console.warn('[farm_boundary_editor] GeoJSON de referência inválido', e);
        }
        return null;
    }

    const MAP_PIN_SVG =
        '<svg xmlns="http://www.w3.org/2000/svg" width="34" height="42" viewBox="0 0 34 42" fill="none">' +
        '<ellipse cx="17" cy="39.5" rx="7" ry="2.2" fill="rgba(0,0,0,0.34)"/>' +
        '<path d="M17 2C9.268 2 3 8.268 3 16c0 9.6 12.1 21.3 13.3 22.4a1.05 1.05 0 0 0 1.4 0C18.9 37.3 31 25.6 31 16 31 8.268 24.732 2 17 2Z" fill="#446A36" stroke="#FFFFFF" stroke-width="2.4"/>' +
        '<circle cx="17" cy="15.5" r="5" fill="#FFFFFF"/>' +
        '</svg>';
    const MAP_PIN_DATA_URL = `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(MAP_PIN_SVG)}`;

    function initLeafletEditor(options) {
        const {
            containerId,
            hiddenInput,
            initialGeometry,
            referenceBoundaryGeometry,
            contextPlotGeometries,
            removePointButton,
            undoButton,
            clearButton,
            onStats,
            onPinChange,
            initialCenter,
            initialZoom,
        } = options;
        const contextGeoms = Array.isArray(contextPlotGeometries) ? contextPlotGeometries : [];
        const el = document.getElementById(containerId);
        if (!el || typeof L === 'undefined') {
            return nullStub();
        }

        const refPoints = extractPoints(referenceBoundaryGeometry);
        const clamp = makeClamp(refPoints, options);
        const lat0 =
            initialCenter && typeof initialCenter.lat === 'number'
                ? initialCenter.lat
                : refPoints[0]
                  ? refPoints[0].lat
                  : -20.7442;
        const lng0 =
            initialCenter && typeof initialCenter.lng === 'number'
                ? initialCenter.lng
                : refPoints[0]
                  ? refPoints[0].lng
                  : -42.8721;
        const zoom0 = typeof initialZoom === 'number' && initialZoom > 0 ? initialZoom : 14;
        const map = L.map(el, { scrollWheelZoom: true }).setView([lat0, lng0], zoom0);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap',
        }).addTo(map);

        const contextLayers = [];
        contextGeoms.forEach((geom, idx) => {
            if (!geom || typeof geom !== 'object') return;
            const layer = leafletGeoJsonLayer(geom, CONTEXT_PLOT_LEAFLET_STYLES[idx % CONTEXT_PLOT_LEAFLET_STYLES.length]);
            if (layer) contextLayers.push(layer);
        });
        const referenceLayer = leafletGeoJsonLayer(referenceBoundaryGeometry);
        const underlayParts = [...contextLayers];
        if (referenceLayer) underlayParts.push(referenceLayer);
        let underlayGroup = null;
        if (underlayParts.length) {
            underlayGroup = L.layerGroup(underlayParts);
            underlayGroup.addTo(map);
        }

        const clonePts = (arr) => arr.map((p) => ({ lat: p.lat, lng: p.lng }));
        const initialSnapshot = clonePts(extractPoints(initialGeometry));
        let points = clonePts(initialSnapshot);
        let history = [];
        let selectedIndex = null;
        let editLayer = null;
        let dragStarted = false;
        const markerLayer = L.layerGroup().addTo(map);
        const midpointLayer = L.layerGroup().addTo(map);

        const pushHistory = () => {
            history.push(clonePts(points));
            if (history.length > 30) history.shift();
        };

        const markerIcon = (active, index) =>
            L.divIcon({
                className: '',
                html: `<div style="width:20px;height:20px;border-radius:9999px;background:${active ? '#9bcb6b' : '#446a36'};border:3px solid #fff;box-shadow:0 4px 12px rgba(15,23,42,.28);display:grid;place-items:center;color:${active ? '#16201a' : '#fff'};font-size:10px;font-weight:700;">${index + 1}</div>`,
                iconSize: [20, 20],
                iconAnchor: [10, 10],
            });

        const midpointIcon = () =>
            L.divIcon({
                className: '',
                html: '<div style="width:16px;height:16px;border-radius:9999px;background:rgba(255,255,255,.92);border:1.5px solid #446a36;color:#294127;display:grid;place-items:center;font-size:12px;font-weight:700;line-height:1;box-shadow:0 2px 6px rgba(15,23,42,.2);">+</div>',
                iconSize: [16, 16],
                iconAnchor: [8, 8],
            });

        const emitStats = () => {
            if (typeof onStats === 'function') {
                const area = calculateArea(points);
                onStats({ vertices: points.length, area, selectedIndex, canUndo: history.length > 0 });
            }
        };

        const syncToHidden = () => {
            if (!hiddenInput) return;
            const geom = buildGeometry(points);
            hiddenInput.value = geom ? JSON.stringify(geom) : '';
            emitStats();
        };

        let interactionMode = 'draw';
        let pinPoint = null;
        let pinMarker = null;

        const renderPin = () => {
            if (pinMarker) {
                map.removeLayer(pinMarker);
                pinMarker = null;
            }
            if (interactionMode === 'pin' && pinPoint) {
                pinMarker = L.marker([pinPoint.lat, pinPoint.lng], {
                    interactive: false,
                    zIndexOffset: 500,
                    icon: L.divIcon({
                        className: 'stw-map-pin-icon',
                        html: MAP_PIN_SVG,
                        iconSize: [34, 42],
                        iconAnchor: [17, 40],
                    }),
                }).addTo(map);
            }
        };

        const renderEditor = () => {
            if (editLayer) {
                map.removeLayer(editLayer);
                editLayer = null;
            }
            markerLayer.clearLayers();
            midpointLayer.clearLayers();
            if (interactionMode === 'pin') {
                renderPin();
                syncToHidden();
                return;
            }
            renderPin();
            if (points.length >= 3) {
                editLayer = L.polygon(points, {
                    color: '#ffffff',
                    weight: 3,
                    fillColor: '#9bcb6b',
                    fillOpacity: 0.3,
                }).addTo(map);
            } else if (points.length === 2) {
                editLayer = L.polyline(points, {
                    color: '#ffffff',
                    weight: 3,
                    dashArray: '6, 4',
                }).addTo(map);
            }
            if (points.length >= 3) {
                points.forEach((pt, idx) => {
                    const next = points[(idx + 1) % points.length];
                    const mid = { lat: (pt.lat + next.lat) / 2, lng: (pt.lng + next.lng) / 2 };
                    const midMarker = L.marker(mid, { icon: midpointIcon(), interactive: true }).addTo(midpointLayer);
                    midMarker.on('click', (e) => {
                        if (e.originalEvent) L.DomEvent.stopPropagation(e.originalEvent);
                        pushHistory();
                        points.splice(idx + 1, 0, mid);
                        selectedIndex = idx + 1;
                        renderEditor();
                    });
                });
            }
            points.forEach((point, index) => {
                const marker = L.marker(point, { draggable: true, icon: markerIcon(index === selectedIndex, index) }).addTo(
                    markerLayer,
                );
                marker.on('click', (e) => {
                    if (e.originalEvent) L.DomEvent.stopPropagation(e.originalEvent);
                    selectedIndex = index;
                    renderEditor();
                });
                marker.on('dragstart', () => {
                    if (!dragStarted) {
                        pushHistory();
                        dragStarted = true;
                    }
                    selectedIndex = index;
                });
                marker.on('drag', (event) => {
                    const ll = event.target.getLatLng();
                    points[index] = { lat: ll.lat, lng: ll.lng };
                    syncToHidden();
                    if (editLayer) editLayer.setLatLngs(points);
                });
                marker.on('dragend', () => {
                    dragStarted = false;
                    points[index] = clamp(points[index]);
                    renderEditor();
                });
            });
            syncToHidden();
        };

        map.on('click', (event) => {
            const pt = clamp({ lat: event.latlng.lat, lng: event.latlng.lng });
            if (interactionMode === 'pin') {
                pinPoint = pt;
                renderPin();
                if (typeof onPinChange === 'function') onPinChange(pinPoint);
                return;
            }
            pushHistory();
            points.push(pt);
            selectedIndex = points.length - 1;
            renderEditor();
        });

        let removeHandler = null;
        if (removePointButton) {
            removeHandler = () => {
                if (!points.length) return;
                const removeAt = selectedIndex === null || selectedIndex >= points.length ? points.length - 1 : selectedIndex;
                pushHistory();
                points.splice(removeAt, 1);
                selectedIndex = null;
                renderEditor();
            };
            removePointButton.addEventListener('click', removeHandler);
        }

        const undo = () => {
            if (!history.length) return;
            points = history.pop();
            selectedIndex = null;
            renderEditor();
        };
        const clearPoints = () => {
            if (!points.length) return;
            pushHistory();
            points = [];
            selectedIndex = null;
            renderEditor();
        };
        const resetToInitial = () => {
            points = clonePts(initialSnapshot);
            history = [];
            selectedIndex = null;
            renderEditor();
        };

        if (undoButton) undoButton.addEventListener('click', undo);
        if (clearButton) clearButton.addEventListener('click', clearPoints);

        renderEditor();
        const fitInitialBounds = () => {
            const refLatLngs = refPoints.map((p) => L.latLng(p.lat, p.lng));
            const editLatLngs = points.map((p) => L.latLng(p.lat, p.lng));
            const contextLatLngs = [];
            contextGeoms.forEach((geom) => {
                extractPoints(geom).forEach((p) => contextLatLngs.push(L.latLng(p.lat, p.lng)));
            });
            const combined = [...contextLatLngs, ...refLatLngs, ...editLatLngs];
            if (!combined.length) return;
            const b = L.latLngBounds(combined);
            if (b.isValid()) map.fitBounds(b.pad(0.14));
        };
        fitInitialBounds();

        return {
            map,
            syncToHidden,
            pointCount: () => points.length,
            fit() {
                map.invalidateSize();
                fitInitialBounds();
            },
            addPointAtCenter() {
                const c = map.getCenter();
                pushHistory();
                points.push(clamp({ lat: c.lat, lng: c.lng }));
                selectedIndex = points.length - 1;
                renderEditor();
            },
            undo,
            clearPoints,
            resetToInitial,
            setInteractionMode(mode) {
                interactionMode = mode === 'pin' ? 'pin' : 'draw';
                el.classList.toggle('is-pin-mode', interactionMode === 'pin');
                renderEditor();
            },
            setPin(lat, lng, silent) {
                if (Number.isFinite(lat) && Number.isFinite(lng)) {
                    pinPoint = { lat, lng };
                } else {
                    pinPoint = null;
                }
                renderPin();
                if (!silent && typeof onPinChange === 'function') onPinChange(pinPoint);
            },
            clearPin() {
                pinPoint = null;
                renderPin();
                if (typeof onPinChange === 'function') onPinChange(null);
            },
            invalidate() {
                window.requestAnimationFrame(() => {
                    map.invalidateSize();
                });
            },
            destroy() {
                if (removePointButton && removeHandler) {
                    removePointButton.removeEventListener('click', removeHandler);
                }
                if (undoButton) undoButton.removeEventListener('click', undo);
                if (clearButton) clearButton.removeEventListener('click', clearPoints);
                underlayGroup = null;
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
        const {
            containerId,
            hiddenInput,
            initialGeometry,
            referenceBoundaryGeometry,
            contextPlotGeometries,
            removePointButton,
            undoButton,
            clearButton,
            onStats,
            onPinChange,
            initialCenter,
            initialZoom,
        } = options;
        const contextGeoms = Array.isArray(contextPlotGeometries) ? contextPlotGeometries : [];
        const el = document.getElementById(containerId);
        if (!el) {
            return nullStub();
        }

        await loadGoogleMapsScript(apiKey);

        const refPts = extractPoints(referenceBoundaryGeometry);
        const clamp = makeClamp(refPts, options);
        const lat0 =
            initialCenter && typeof initialCenter.lat === 'number'
                ? initialCenter.lat
                : refPts[0]
                  ? refPts[0].lat
                  : -20.7442;
        const lng0 =
            initialCenter && typeof initialCenter.lng === 'number'
                ? initialCenter.lng
                : refPts[0]
                  ? refPts[0].lng
                  : -42.8721;
        const zoom0 = typeof initialZoom === 'number' && initialZoom > 0 ? initialZoom : 14;
        const map = new google.maps.Map(el, {
            center: { lat: lat0, lng: lng0 },
            zoom: zoom0,
            mapTypeId: 'satellite',
            mapTypeControl: true,
            streetViewControl: false,
            rotateControl: false,
            fullscreenControl: true,
        });

        const contextPolygons = [];
        contextGeoms.forEach((geom, idx) => {
            const pts = extractPoints(geom);
            if (pts.length < 3) return;
            const st = CONTEXT_PLOT_GOOGLE_STYLES[idx % CONTEXT_PLOT_GOOGLE_STYLES.length];
            contextPolygons.push(
                new google.maps.Polygon({
                    paths: pts.map((p) => ({ lat: p.lat, lng: p.lng })),
                    strokeColor: st.strokeColor,
                    strokeOpacity: 1,
                    strokeWeight: 2,
                    fillColor: st.fillColor,
                    fillOpacity: st.fillOpacity,
                    clickable: false,
                    editable: false,
                    draggable: false,
                    zIndex: 10,
                    map,
                }),
            );
        });

        let referencePolygon = null;
        if (refPts.length >= 3) {
            referencePolygon = new google.maps.Polygon({
                paths: refPts.map((p) => ({ lat: p.lat, lng: p.lng })),
                strokeColor: '#418436',
                strokeOpacity: 1,
                strokeWeight: 2,
                fillColor: '#5BB34A',
                fillOpacity: 0.37,
                clickable: false,
                editable: false,
                draggable: false,
                zIndex: 20,
                map,
            });
        }

        const clonePts = (arr) => arr.map((p) => ({ lat: p.lat, lng: p.lng }));
        const initialSnapshot = clonePts(extractPoints(initialGeometry));
        let points = clonePts(initialSnapshot);
        let history = [];
        let selectedIndex = null;
        let polygon = null;
        const markers = [];
        const listenerHandles = [];
        let skipNextMapClick = false;

        const pushHistory = () => {
            history.push(clonePts(points));
            if (history.length > 30) history.shift();
        };

        const emitStats = () => {
            if (typeof onStats === 'function') {
                const area = calculateArea(points);
                onStats({ vertices: points.length, area, selectedIndex, canUndo: history.length > 0 });
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
                    strokeColor: '#ffffff',
                    strokeWeight: 3,
                    fillColor: '#9bcb6b',
                    fillOpacity: 0.3,
                    clickable: false,
                    zIndex: 100,
                    map,
                });
            }
        };

        const markerListeners = [];
        const clearMarkers = () => {
            markerListeners.forEach((h) => google.maps.event.removeListener(h));
            markerListeners.length = 0;
            markers.forEach((m) => m.setMap(null));
            markers.length = 0;
        };

        const renderMarkers = () => {
            clearMarkers();
            // Alças de ponto médio (+) entre vértices consecutivos para inserir pontos na borda certa
            if (points.length >= 3) {
                points.forEach((pt, idx) => {
                    const next = points[(idx + 1) % points.length];
                    const mid = { lat: (pt.lat + next.lat) / 2, lng: (pt.lng + next.lng) / 2 };
                    const midMarker = new google.maps.Marker({
                        position: mid,
                        map,
                        draggable: true,
                        zIndex: 180,
                        title: 'Adicionar ponto nesta borda',
                        icon: {
                            path: google.maps.SymbolPath.CIRCLE,
                            scale: 8.5,
                            fillColor: '#ffffff',
                            fillOpacity: 0.94,
                            strokeColor: '#446a36',
                            strokeWeight: 2,
                        },
                        label: {
                            text: '+',
                            color: '#294127',
                            fontSize: '13px',
                            fontWeight: '700',
                        },
                    });
                    const insertAtMid = (latLng) => {
                        skipNextMapClick = true;
                        window.setTimeout(() => { skipNextMapClick = false; }, 100);
                        pushHistory();
                        const newPt = latLng ? { lat: latLng.lat(), lng: latLng.lng() } : mid;
                        points.splice(idx + 1, 0, newPt);
                        selectedIndex = idx + 1;
                        renderAll();
                    };
                    markerListeners.push(
                        google.maps.event.addListener(midMarker, 'click', () => insertAtMid(null)),
                    );
                    markerListeners.push(
                        google.maps.event.addListener(midMarker, 'dragend', () => insertAtMid(midMarker.getPosition())),
                    );
                    markers.push(midMarker);
                });
            }

            // Vértices numerados (cantos)
            points.forEach((pt, index) => {
                const isSelected = index === selectedIndex;
                const marker = new google.maps.Marker({
                    position: pt,
                    map,
                    draggable: true,
                    zIndex: isSelected ? 220 : 200,
                    title: `Vértice ${index + 1} — arraste para mover ou clique para selecionar`,
                    icon: {
                        path: google.maps.SymbolPath.CIRCLE,
                        scale: isSelected ? 12 : 10.5,
                        fillColor: isSelected ? '#9bcb6b' : '#446a36',
                        fillOpacity: 1,
                        strokeColor: '#ffffff',
                        strokeWeight: 2.5,
                    },
                    label: {
                        text: String(index + 1),
                        color: isSelected ? '#16201a' : '#ffffff',
                        fontSize: '10px',
                        fontWeight: '700',
                    },
                });
                markerListeners.push(
                    google.maps.event.addListener(marker, 'click', () => {
                        skipNextMapClick = true;
                        window.setTimeout(() => {
                            skipNextMapClick = false;
                        }, 100);
                        selectedIndex = index;
                        renderAll();
                    }),
                );
                markerListeners.push(
                    google.maps.event.addListener(marker, 'dragstart', () => {
                        pushHistory();
                        selectedIndex = index;
                    }),
                );
                markerListeners.push(
                    google.maps.event.addListener(marker, 'drag', () => {
                        const pos = marker.getPosition();
                        points[index] = { lat: pos.lat(), lng: pos.lng() };
                        syncToHidden();
                        if (polygon) polygon.setPath(points);
                    }),
                );
                markerListeners.push(google.maps.event.addListener(marker, 'dragend', () => {
                    points[index] = clamp(points[index]);
                    renderAll();
                }));
                markers.push(marker);
            });
        };

        let interactionMode = 'draw';
        let pinPoint = null;
        let pinMarker = null;

        const renderPin = () => {
            if (pinMarker) {
                pinMarker.setMap(null);
                pinMarker = null;
            }
            if (interactionMode === 'pin' && pinPoint) {
                pinMarker = new google.maps.Marker({
                    position: pinPoint,
                    map,
                    clickable: false,
                    zIndex: 260,
                    icon: {
                        url: MAP_PIN_DATA_URL,
                        scaledSize: new google.maps.Size(34, 42),
                        anchor: new google.maps.Point(17, 40),
                    },
                });
            }
        };

        const renderAll = () => {
            if (interactionMode === 'pin') {
                if (polygon) {
                    polygon.setMap(null);
                    polygon = null;
                }
                clearMarkers();
                renderPin();
                syncToHidden();
                return;
            }
            renderPin();
            renderPolygon();
            renderMarkers();
            syncToHidden();
        };

        listenerHandles.push(
            google.maps.event.addListener(map, 'click', (e) => {
                if (skipNextMapClick) return;
                const pt = clamp({ lat: e.latLng.lat(), lng: e.latLng.lng() });
                if (interactionMode === 'pin') {
                    pinPoint = pt;
                    renderPin();
                    if (typeof onPinChange === 'function') onPinChange(pinPoint);
                    return;
                }
                pushHistory();
                points.push(pt);
                selectedIndex = points.length - 1;
                renderAll();
            }),
        );

        let removeHandler = null;
        if (removePointButton) {
            removeHandler = () => {
                if (!points.length) return;
                const removeAt = selectedIndex === null || selectedIndex >= points.length ? points.length - 1 : selectedIndex;
                pushHistory();
                points.splice(removeAt, 1);
                selectedIndex = null;
                renderAll();
            };
            removePointButton.addEventListener('click', removeHandler);
        }

        const undo = () => {
            if (!history.length) return;
            points = history.pop();
            selectedIndex = null;
            renderAll();
        };
        const clearPoints = () => {
            if (!points.length) return;
            pushHistory();
            points = [];
            selectedIndex = null;
            renderAll();
        };
        const resetToInitial = () => {
            points = clonePts(initialSnapshot);
            history = [];
            selectedIndex = null;
            renderAll();
        };
        if (undoButton) undoButton.addEventListener('click', undo);
        if (clearButton) clearButton.addEventListener('click', clearPoints);

        renderAll();
        const fitInitialGoogleBounds = () => {
            if (!refPts.length && !points.length && !contextGeoms.length) return;
            const bounds = new google.maps.LatLngBounds();
            contextGeoms.forEach((geom) => {
                extractPoints(geom).forEach((p) => bounds.extend(p));
            });
            refPts.forEach((p) => bounds.extend(p));
            points.forEach((p) => bounds.extend(p));
            try {
                map.fitBounds(bounds, 48);
            } catch (e) {
                /* ignore */
            }
        };
        fitInitialGoogleBounds();

        return {
            map,
            syncToHidden,
            pointCount: () => points.length,
            fit() {
                google.maps.event.trigger(map, 'resize');
                fitInitialGoogleBounds();
            },
            addPointAtCenter() {
                const c = map.getCenter();
                pushHistory();
                points.push(clamp({ lat: c.lat(), lng: c.lng() }));
                selectedIndex = points.length - 1;
                renderAll();
            },
            undo,
            clearPoints,
            resetToInitial,
            setInteractionMode(mode) {
                interactionMode = mode === 'pin' ? 'pin' : 'draw';
                el.classList.toggle('is-pin-mode', interactionMode === 'pin');
                map.setOptions({ draggableCursor: interactionMode === 'pin' ? 'crosshair' : null });
                renderAll();
            },
            setPin(lat, lng, silent) {
                if (Number.isFinite(lat) && Number.isFinite(lng)) {
                    pinPoint = { lat, lng };
                } else {
                    pinPoint = null;
                }
                renderPin();
                if (!silent && typeof onPinChange === 'function') onPinChange(pinPoint);
            },
            clearPin() {
                pinPoint = null;
                renderPin();
                if (typeof onPinChange === 'function') onPinChange(null);
            },
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
                if (pinMarker) {
                    pinMarker.setMap(null);
                    pinMarker = null;
                }
                if (polygon) {
                    polygon.setMap(null);
                    polygon = null;
                }
                if (referencePolygon) {
                    referencePolygon.setMap(null);
                    referencePolygon = null;
                }
                contextPolygons.forEach((p) => p.setMap(null));
                contextPolygons.length = 0;
                if (removePointButton && removeHandler) {
                    removePointButton.removeEventListener('click', removeHandler);
                }
                if (undoButton) undoButton.removeEventListener('click', undo);
                if (clearButton) clearButton.removeEventListener('click', clearPoints);
                el.innerHTML = '';
            },
        };
    }

    /**
     * @param {object} options
     * @param {object[]} [options.contextPlotGeometries] GeoJSON de outros setores (somente visualização).
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

    /* Arquivo geodésico (.kml, .geojson, .json) -> Polygon GeoJSON; null quando não há polígono. */
    function parseKml(xmlText) {
        const doc = new DOMParser().parseFromString(xmlText, 'application/xml');
        if (doc.querySelector('parsererror')) return null;
        const byName = (root, name) => [...root.getElementsByTagName('*')].filter((el) => (el.localName || el.tagName.split(':').pop()).toLowerCase() === name);
        const blobs = [];
        byName(doc, 'outerboundaryis').forEach((ob) => byName(ob, 'coordinates').forEach((c) => { if (c.textContent.trim()) blobs.push(c.textContent.trim()); }));
        if (!blobs.length) byName(doc, 'coordinates').forEach((c) => { if (c.textContent.trim()) blobs.push(c.textContent.trim()); });
        for (const blob of blobs) {
            const pts = [];
            blob.split(/\s+/).forEach((tok) => {
                const [lng, lat] = tok.split(',').map(parseFloat);
                if (Number.isFinite(lng) && Number.isFinite(lat) && Math.abs(lng) <= 180 && Math.abs(lat) <= 90) pts.push([lng, lat]);
            });
            if (pts.length >= 3) {
                const first = pts[0];
                const last = pts[pts.length - 1];
                if (first[0] !== last[0] || first[1] !== last[1]) pts.push([first[0], first[1]]);
                if (pts.length >= 4) return { type: 'Polygon', coordinates: [pts] };
            }
        }
        return null;
    }
    window.parseGeodesicText = function parseGeodesicText(rawText) {
        const text = String(rawText || '').replace(/^\uFEFF/, '').trim();
        if (!text) return null;
        if (text.startsWith('{') || text.startsWith('[')) {
            try {
                const pts = extractPoints(JSON.parse(text));
                if (pts.length >= 3) return buildGeometry(pts);
            } catch (e) { /* tenta KML */ }
        }
        return text.includes('<') ? parseKml(text) : null;
    };
    /* Ajusta na divisa os cantos que ficam fora do perímetro de referência (fazenda). */
    window.clampGeometryToBoundary = function clampGeometryToBoundary(geometry, referenceGeometry) {
        const ref = extractPoints(referenceGeometry);
        const pts = extractPoints(geometry);
        if (ref.length < 3 || pts.length < 3) return { geometry, changed: 0 };
        let changed = 0;
        const clamp = makeClamp(ref, { clipToReference: true, onClamp: () => { changed += 1; } });
        const fixed = pts.map((pt) => clamp(pt));
        return { geometry: changed ? buildGeometry(fixed) : geometry, changed };
    };
})();
