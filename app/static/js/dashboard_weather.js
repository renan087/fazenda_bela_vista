// Dashboard: previsao do tempo da fazenda (Open-Meteo; ECMWF principal, GFS/ICON so como reserva no backend).
(() => {
    const root = document.querySelector('[data-db-weather]');
    if (!root) return;
    const card = root.closest('.db-card');
    const meta = card.querySelector('[data-db-wx-meta]');
    const src = card.querySelector('[data-db-wx-src]');
    const heavy = Number(root.dataset.heavy) || 30;
    let data = null;
    let timer = null;

    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const num = (v, d = 0) => Number(v).toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d });
    const mm = (v) => (v == null ? '—' : num(v, v > 0 && v < 10 ? 1 : 0));
    const today = new Date().toLocaleDateString('sv-SE');

    const ICONS = {
        sun: '<circle cx="12" cy="12" r="4" fill="#f5b83d" stroke="#e39b16"/><path stroke="#e39b16" d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
        cloud: '<path fill="#e8edf2" stroke="#8a99a8" d="M7 18a4 4 0 0 1-.6-8 5.5 5.5 0 0 1 10.6 1.5A3.3 3.3 0 0 1 16.7 18Z"/><circle cx="17" cy="6" r="2.4" fill="#f5b83d" stroke="#e39b16"/>',
        rain: '<path fill="#dfe8f3" stroke="#6f8aa6" d="M7 14a4 4 0 0 1-.6-8 5.5 5.5 0 0 1 10.6 1.5A3.3 3.3 0 0 1 16.7 14Z"/><path stroke="#3b7bc8" d="M8 17l-1 3M12 17l-1 3M16 17l-1 3"/>',
        storm: '<path fill="#d3dbe6" stroke="#5c6f86" d="M7 13a4 4 0 0 1-.6-8 5.5 5.5 0 0 1 10.6 1.5A3.3 3.3 0 0 1 16.7 13Z"/><path stroke="#f08c2e" fill="#f5b83d" d="m12.5 14-2.5 4h3l-1.5 4"/><path stroke="#3b7bc8" d="M7.5 16l-1 3M17 16l-1 3"/>',
    };
    const icon = (d) => {
        const p = d.precip ?? 0, prob = d.prob ?? 0;
        const kind = p >= heavy ? 'storm' : (p >= 1 || prob >= 60) ? 'rain' : (p > 0 || prob >= 30) ? 'cloud' : 'sun';
        const label = { storm: 'Chuva forte', rain: 'Chuva', cloud: 'Nublado / pancadas', sun: 'Tempo firme' }[kind];
        return `<svg class="db-wx-ico" viewBox="0 0 24 24" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" role="img" aria-label="${label}">${ICONS[kind]}</svg>`;
    };
    const empty = (html, iconPath) => `<div class="db-wx-empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">${iconPath}</svg><div>${html}</div></div>`;
    const INFO = '<circle cx="12" cy="12" r="9"/><path d="M12 8h.01M11 12h1v4h1"/>';

    const FULL = { Seg: 'na segunda', Ter: 'na terça', Qua: 'na quarta', Qui: 'na quinta', Sex: 'na sexta', 'Sáb': 'no sábado', Dom: 'no domingo' };
    function renderForecast() {
        const vals = data.days.map((d) => d.precip ?? 0);
        const max = Math.max(heavy * 0.6, ...vals);
        const days = data.days.map((d, i) => {
            const p = d.precip, isToday = d.date === today, wd = isToday ? 'Hoje' : esc(d.weekday);
            const hv = p != null && p >= heavy, wet = p != null && p >= 0.5;
            const h = wet ? Math.max(3, Math.round((p / max) * 100)) : 0;
            const op = (0.45 + (d.prob ?? 50) / 180).toFixed(2);
            const tip = `<b>${wd}</b> · ${p == null ? '—' : `${mm(p)} mm`}${d.prob == null ? '' : ` · ${num(d.prob)}% de chance`}${d.tmax == null ? '' : `<br><span>Máx. ${num(d.tmax)}°${d.tmin == null ? '' : ` · Mín. ${num(d.tmin)}°`}</span>`}`;
            const pos = i === 0 ? ' is-start' : i === data.days.length - 1 ? ' is-end' : '';
            return `<div class="db-wx-day${isToday ? ' is-today' : ''}${pos}" tabindex="0">
                <span class="db-wx-wd">${wd}</span>${icon(d)}
                <span class="db-wx-t">${d.tmax == null ? '—' : `${num(d.tmax)}°`}</span>
                <span class="db-wx-bar">${wet ? `<span class="${hv ? 'is-heavy' : ''}" style="height:${h}%;opacity:${op}"></span>` : '<span class="is-dry"></span>'}</span>
                <span class="db-wx-mm${hv ? ' is-heavy' : wet ? '' : ' is-dry'}">${wet ? mm(p) : '—'}</span>
                <span class="db-wx-tip" role="tooltip">${tip}</span></div>`;
        }).join('');
        const first = data.days.find((d) => (d.precip ?? 0) >= heavy);
        const alert = first ? `<div class="db-wx-alert"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 3 2.5 20h19L12 3Z"/><path d="M12 10v4M12 17h.01"/></svg><span><b>Chuva forte ${first.date === today ? 'hoje' : `${FULL[first.weekday] || `na ${esc(first.weekday)}`}`}</b> · ${mm(first.precip)} mm previstos</span></div>` : '';
        return `${alert}<div class="db-wx-days">${days}</div>`;
    }

    function render() {
        if (!data) return;
        const ok = data.status === 'ok' || data.status === 'stale';
        if (!ok) {
            meta.textContent = 'Previsão para os próximos 7 dias';
            src.hidden = true;
            if (data.status === 'no_farm') root.innerHTML = empty('Selecione uma fazenda no topo da página para ver a previsão do tempo.', INFO);
            else if (data.status === 'no_coords') root.innerHTML = empty(`A previsão usa a localização da fazenda no mapa. <a href="/fazendas?edit_id=${encodeURIComponent(data.farm_id)}">Desenhe o contorno da ${esc(data.farm)}</a> ou dos setores para ativar.`, '<path d="M12 21s-7-6.2-7-11.5a7 7 0 0 1 14 0C19 14.8 12 21 12 21Z"/><circle cx="12" cy="9.5" r="2.5"/>');
            else root.innerHTML = empty('Previsão do tempo indisponível no momento. Tentaremos de novo em alguns minutos. <button type="button" data-db-wx-retry>Tentar agora</button>', INFO);
            return;
        }
        meta.innerHTML = `<strong>${mm(data.total_precip)} mm</strong> previstos nos próximos 7 dias`;
        const hour = String(data.updated_at || '').split('às ')[1] || '';
        src.textContent = `Open-Meteo${hour ? ` · ${hour}` : ''}`;
        src.title = `Previsão Open-Meteo para ${data.farm || 'a fazenda'} · atualizada em ${data.updated_at || '—'}`;
        src.hidden = false;
        root.innerHTML = renderForecast()
            + (data.status === 'stale' ? '<p class="db-wx-stale">Não foi possível atualizar agora; exibindo a última previsão disponível.</p>' : '');
    }

    async function load() {
        clearTimeout(timer);
        let next = 10 * 60;
        try {
            const res = await fetch('/api/dashboard/clima', { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
            if (res.status === 403) { card.querySelector('[data-db-weather]').remove(); return; }
            data = res.ok ? await res.json() : { status: 'error' };
            if (data.next_refresh_seconds) next = data.next_refresh_seconds + 30;
        } catch (e) {
            data = data && (data.status === 'ok' || data.status === 'stale') ? { ...data, status: 'stale' } : { status: 'error' };
        }
        render();
        if (data.status !== 'no_farm' && data.status !== 'no_coords') timer = setTimeout(load, next * 1000);
    }

    root.addEventListener('click', (e) => { if (e.target.closest('[data-db-wx-retry]')) load(); });
    // Ao voltar para a aba depois de muito tempo, confere se ja passou da hora de atualizar.
    let hiddenAt = 0;
    document.addEventListener('visibilitychange', () => {
        if (document.hidden) hiddenAt = Date.now();
        else if (hiddenAt && Date.now() - hiddenAt > 30 * 60 * 1000) load();
    });
    load();
})();
