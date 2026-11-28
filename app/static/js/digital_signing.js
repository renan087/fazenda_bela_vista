const vendor = "/static/vendor/pdfjs/";
const states = {valid: "Válida", invalid: "Inválida", indeterminate: "Indeterminada", unsigned: "Sem assinatura"};
const formatDate = new Intl.DateTimeFormat("pt-BR", {dateStyle: "short", timeZone: "America/Bahia"});
const formatTime = new Intl.DateTimeFormat("pt-BR", {dateStyle: "short", timeStyle: "short", timeZone: "America/Bahia"});

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function feedback(node, text = "", error = false) {
  if (!node) return;
  node.hidden = !text;
  node.textContent = text;
  node.classList.toggle("is-error", error);
}

function formatMmSs(totalSeconds) {
  const s = Math.max(0, Math.floor(totalSeconds));
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return `${String(m).padStart(2, "0")}:${String(rem).padStart(2, "0")}`;
}

function createOtpController(root, { onResend, onFallbackEmail }) {
  const sentTitle = root.querySelector("[data-ds-otp-sent-title]");
  const destEl = root.querySelector("[data-ds-otp-dest]");
  const expiryEl = root.querySelector("[data-ds-otp-expiry]");
  const resendBtn = root.querySelector("[data-ds-otp-resend]");
  const fallbackBtn = root.querySelector("[data-ds-otp-fallback-email]");
  const submitBtn = root.querySelector("[data-ds-otp-submit]");

  let timerId = null;
  let expiresAtMs = 0;
  let resendAtMs = 0;
  let currentChannel = "email";
  let isExpired = false;
  let sending = false;

  function stop() {
    if (timerId !== null) {
      window.clearInterval(timerId);
      timerId = null;
    }
  }

  function tick() {
    const now = Date.now();
    const expRem = Math.max(0, Math.ceil((expiresAtMs - now) / 1000));
    const resRem = Math.max(0, Math.ceil((resendAtMs - now) / 1000));

    isExpired = expRem <= 0;
    if (expiryEl) {
      expiryEl.textContent = isExpired ? "Expirado" : formatMmSs(expRem);
      expiryEl.classList.toggle("is-warn", !isExpired && expRem <= 60);
      expiryEl.classList.toggle("is-expired", isExpired);
    }
    if (submitBtn && !sending) {
      submitBtn.disabled = isExpired;
    }
    if (resendBtn) {
      if (sending) {
        resendBtn.disabled = true;
        resendBtn.textContent = "Reenviando código…";
      } else if (resRem > 0) {
        resendBtn.disabled = true;
        resendBtn.textContent = `Reenviar código em ${resRem}s`;
      } else {
        resendBtn.disabled = false;
        resendBtn.textContent = "Reenviar código";
      }
    }
    if (fallbackBtn && !fallbackBtn.hidden) {
      fallbackBtn.disabled = sending || resRem > 0;
    }
    if (isExpired && resRem <= 0) {
      stop();
    }
  }

  function start({
    channel = "email",
    destinationMasked = "",
    expiresSeconds = 600,
    resendAfterSeconds = 60,
    fallbackEmailAvailable = false,
    fallbackEmailMasked = "",
    resent = false,
  } = {}) {
    stop();
    currentChannel = channel;
    const now = Date.now();
    expiresAtMs = now + Math.max(10, Number(expiresSeconds) || 600) * 1000;
    resendAtMs = now + Math.max(0, Number(resendAfterSeconds) || 60) * 1000;
    isExpired = false;

    const chanLabel = channel === "whatsapp" ? "WhatsApp" : "E-mail";
    if (sentTitle) {
      sentTitle.textContent = resent
        ? `Novo código enviado via ${chanLabel}`
        : `Código enviado via ${chanLabel}`;
    }
    if (destEl) {
      destEl.textContent = destinationMasked ? `${chanLabel} · ${destinationMasked}` : chanLabel;
    }
    if (fallbackBtn) {
      const showFallback = Boolean(channel === "whatsapp" && fallbackEmailAvailable);
      fallbackBtn.hidden = !showFallback;
      if (showFallback) {
        fallbackBtn.textContent = fallbackEmailMasked
          ? `Prefere receber por e-mail (${fallbackEmailMasked})?`
          : "Prefere receber por e-mail?";
      }
    }
    tick();
    timerId = window.setInterval(tick, 1000);
  }

  resendBtn?.addEventListener("click", async () => {
    if (sending || resendBtn.disabled) return;
    sending = true;
    tick();
    try {
      await onResend?.(currentChannel);
    } finally {
      sending = false;
      tick();
    }
  });

  fallbackBtn?.addEventListener("click", async () => {
    if (sending || fallbackBtn.disabled) return;
    sending = true;
    tick();
    try {
      await onFallbackEmail?.();
    } finally {
      sending = false;
      tick();
    }
  });

  return { start, stop, isExpired: () => isExpired };
}

function formatted(value, formatter) {
  const date = new Date(value);
  return value && Number.isFinite(date.getTime()) ? formatter.format(date) : "";
}

function badge(state) {
  return element("span", states[state] || "A verificar", `ds-pill ds-pill--${states[state] ? state : "unsigned"}`);
}

function maskPhone(value) {
  const d = String(value || "").replace(/\D/g, "").slice(0, 11);
  if (d.length <= 2) return d ? `(${d}` : "";
  if (d.length <= 6) return `(${d.slice(0, 2)}) ${d.slice(2)}`;
  if (d.length <= 10) return `(${d.slice(0, 2)}) ${d.slice(2, 6)}-${d.slice(6)}`;
  return `(${d.slice(0, 2)}) ${d.slice(2, 7)}-${d.slice(7)}`;
}

function maskCpf(value) {
  const d = String(value || "").replace(/\D/g, "").slice(0, 11);
  if (d.length <= 3) return d;
  if (d.length <= 6) return `${d.slice(0, 3)}.${d.slice(3)}`;
  if (d.length <= 9) return `${d.slice(0, 3)}.${d.slice(3, 6)}.${d.slice(6)}`;
  return `${d.slice(0, 3)}.${d.slice(3, 6)}.${d.slice(6, 9)}-${d.slice(9)}`;
}

function maskCpfPublic(value) {
  const d = String(value || "").replace(/\D/g, "");
  if (d.length !== 11) return "";
  return `***.${d.slice(3, 6)}.${d.slice(6, 9)}-**`;
}

function maskVerificationCode(value) {
  const clean = String(value || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 14);
  if (clean.length <= 2) return clean;
  if (clean.length <= 6) return `${clean.slice(0, 2)}-${clean.slice(2)}`;
  if (clean.length <= 10) return `${clean.slice(0, 2)}-${clean.slice(2, 6)}-${clean.slice(6)}`;
  return `${clean.slice(0, 2)}-${clean.slice(2, 6)}-${clean.slice(6, 10)}-${clean.slice(10)}`;
}

function isValidCpf(value) {
  const d = String(value || "").replace(/\D/g, "");
  if (d.length !== 11 || /^(\d)\1{10}$/.test(d)) return false;
  let sum = 0;
  for (let i = 0; i < 9; i++) sum += Number(d[i]) * (10 - i);
  let rem = (sum * 10) % 11;
  if (rem === 10) rem = 0;
  if (rem !== Number(d[9])) return false;
  sum = 0;
  for (let i = 0; i < 10; i++) sum += Number(d[i]) * (11 - i);
  rem = (sum * 10) % 11;
  if (rem === 10) rem = 0;
  return rem === Number(d[10]);
}

function initPasswordToggles() {
  for (const btn of document.querySelectorAll("[data-ds-toggle-pw]")) {
    btn.addEventListener("click", () => {
      const input = document.getElementById(btn.dataset.dsTogglePw);
      if (!input) return;
      const showing = input.type === "text";
      input.type = showing ? "password" : "text";
      btn.setAttribute("aria-pressed", String(!showing));
      btn.setAttribute("aria-label", showing ? "Visualizar senha" : "Ocultar senha");
      const showIc = btn.querySelector('[data-pw-icon="show"]');
      const hideIc = btn.querySelector('[data-pw-icon="hide"]');
      if (showIc) showIc.hidden = !showing;
      if (hideIc) hideIc.hidden = showing;
    });
  }
}

function initInputMasks() {
  for (const input of document.querySelectorAll("[data-ds-phone-mask]")) {
    if (input.value) input.value = maskPhone(input.value);
    input.addEventListener("input", () => {
      input.value = maskPhone(input.value);
    });
  }
  for (const input of document.querySelectorAll("[data-ds-cpf-mask]")) {
    const field = input.closest(".ds-field");
    const hint = field?.querySelector(".ds-cpf-hint");
    const defaultHint = hint ? hint.textContent : "";
    const updateCpfState = (force = false) => {
      input.value = maskCpf(input.value);
      const digits = input.value.replace(/\D/g, "");
      if (!digits.length) {
        input.classList.remove("ds-input-valid", "ds-input-invalid");
        if (hint) {
          hint.textContent = defaultHint || "Digite os 11 dígitos do CPF.";
          hint.className = "ds-cpf-hint";
        }
        return true;
      }
      if (digits.length === 11 || force) {
        const ok = isValidCpf(input.value);
        input.classList.toggle("ds-input-valid", ok);
        input.classList.toggle("ds-input-invalid", !ok);
        if (hint) {
          hint.textContent = ok ? "✓ CPF válido" : "CPF inválido. Confira os 11 dígitos digitados.";
          hint.className = `ds-cpf-hint ${ok ? "is-ok" : "is-err"}`;
        }
        return ok;
      }
      input.classList.remove("ds-input-valid", "ds-input-invalid");
      if (hint) {
        hint.textContent = "Digite os 11 dígitos do CPF.";
        hint.className = "ds-cpf-hint";
      }
      return true;
    };
    input.addEventListener("input", () => updateCpfState(false));
    input.addEventListener("blur", () => updateCpfState(true));
  }
  for (const input of document.querySelectorAll("[data-ds-code-mask]")) {
    if (input.value) input.value = maskVerificationCode(input.value);
    input.addEventListener("input", () => {
      input.value = maskVerificationCode(input.value);
    });
  }
}

function initCopyButtons() {
  document.addEventListener("click", async (event) => {
    const btn = event.target.closest("[data-ds-copy-text]");
    if (!btn) return;
    const text = btn.dataset.dsCopyText || "";
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      const orig = btn.innerHTML;
      btn.textContent = "✓ Copiado";
      setTimeout(() => { btn.innerHTML = orig; }, 1800);
    } catch {
      window.prompt("Copie o código abaixo:", text);
    }
  });
}

function reportInto(container, report, filename) {
  const content = element("div", "", "ds-result-content");
  const bannerTone = report.state === "valid" ? "is-valid" : report.state === "invalid" ? "is-invalid" : "is-warn";
  const banner = element("div", "", `ds-result-banner ${bannerTone}`);
  const bannerBody = element("div");
  const headline = {
    valid: "Documento autêntico e íntegro",
    invalid: "Atenção: assinatura inválida ou documento alterado",
    indeterminate: "Validação pendente ou indeterminada",
    unsigned: "Nenhuma assinatura digital encontrada",
  }[report.state] || "Resultado da conferência";
  bannerBody.append(element("strong", filename ? `${headline} — ${filename}` : headline));
  const detailText = report.message || (
    report.state === "valid"
      ? "Nenhuma alteração foi feita após a assinatura. Compatível com o VALIDAR do ITI."
      : report.state === "unsigned"
      ? "Este PDF ainda não possui assinaturas criptográficas."
      : "Verifique a origem do arquivo antes de aceitá-lo."
  );
  bannerBody.append(element("span", detailText));
  banner.append(bannerBody);
  content.append(banner);

  if (report.checked_at) {
    content.append(element("p", `Verificado em ${formatted(report.checked_at, formatTime)}`, "ds-hint"));
  }

  for (const [index, signature] of (report.signatures || []).entries()) {
    const item = element("div", "", "ds-sig-item");
    const head = element("div", "", "ds-sig-item-head");
    head.append(element("strong", signature.holder || `Assinatura ${index + 1}`), badge(signature.state));
    item.append(head);
    const parts = [];
    if (signature.issuer) parts.push(signature.issuer);
    if (signature.signed_at) parts.push(`Assinado em ${formatted(signature.signed_at, formatTime)}`);
    if (signature.valid_until) parts.push(`Certificado válido até ${formatted(signature.valid_until, formatDate)}`);
    if (parts.length) item.append(element("p", parts.join(" · ")));
    if (signature.message) item.append(element("p", signature.message));
    content.append(item);
  }

  container.replaceChildren(content);
  container.hidden = false;
}

async function request(root, path, {method = "POST", json, pdf, name, title, category, ownerSigns, signingPolicy} = {}) {
  const headers = {"X-CSRF-Token": root.dataset.csrf, Accept: "application/json"};
  if (json) headers["Content-Type"] = "application/json";
  if (pdf) headers["Content-Type"] = "application/pdf";
  if (name) headers["X-Document-Name"] = encodeURIComponent(name);
  if (title) headers["X-Document-Title"] = encodeURIComponent(title);
  if (category) headers["X-Document-Category"] = encodeURIComponent(category);
  if (typeof ownerSigns === "boolean") headers["X-Document-Owner-Signs"] = ownerSigns ? "1" : "0";
  if (signingPolicy) headers["X-Document-Signing-Policy"] = signingPolicy;
  const response = await fetch(path, {
    method, headers, body: json ? JSON.stringify(json) : pdf, cache: "no-store",
  });
  if (response.redirected && new URL(response.url).pathname === "/login") {
    throw new Error("Sua sessão terminou. Entre no SiSFarm novamente.");
  }
  const data = await response.json().catch(() => ({}));
  if (!response.ok || response.redirected) {
    throw new Error(typeof data.detail === "string" ? data.detail : "A operação não pôde ser concluída. Tente novamente.");
  }
  return data;
}

function initActionMenus() {
  const closeAll = (except = null) => {
    for (const wrap of document.querySelectorAll("[data-nx-more]")) {
      if (wrap === except) continue;
      const btn = wrap.querySelector("[data-nx-more-btn]");
      const menu = wrap.querySelector(".nx-menu");
      if (btn) btn.setAttribute("aria-expanded", "false");
      if (menu) {
        menu.hidden = true;
        menu.classList.remove("is-up");
      }
    }
  };

  document.addEventListener("click", (event) => {
    const trigger = event.target.closest("[data-nx-more-btn]");
    if (trigger) {
      event.preventDefault();
      const wrap = trigger.closest("[data-nx-more]");
      const menu = wrap.querySelector(".nx-menu");
      const opening = menu.hidden;
      closeAll(opening ? wrap : null);
      if (opening) {
        const rect = trigger.getBoundingClientRect();
        menu.hidden = false;
        const neededHeight = Math.max(menu.offsetHeight || 220, 220);
        const spaceBelow = window.innerHeight - rect.bottom;
        menu.classList.toggle("is-up", spaceBelow < neededHeight + 16 && rect.top > spaceBelow);
      } else {
        menu.hidden = true;
        menu.classList.remove("is-up");
      }
      trigger.setAttribute("aria-expanded", String(opening));
      return;
    }
    if (!event.target.closest("[data-nx-more]")) {
      closeAll();
    } else if (event.target.closest(".nx-mi")) {
      closeAll();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeAll();
  });
}

function bindAnimatedDropzone(dropEl, {dragTitle, dragSub, onFile}) {
  if (!dropEl) return;
  const titleEl = dropEl.querySelector("[data-ds-drop-title], [data-ds-reg-file-label], strong");
  const subEl = dropEl.querySelector("[data-ds-drop-sub], [data-ds-reg-file-sub], p");
  let origTitle = titleEl ? titleEl.textContent : "";
  let origSub = subEl ? subEl.textContent : "";
  let depth = 0;

  const enterDrag = () => {
    if (!dropEl.classList.contains("is-dragging")) {
      origTitle = titleEl ? titleEl.textContent : "";
      origSub = subEl ? subEl.textContent : "";
      dropEl.classList.add("is-dragging");
      if (titleEl) titleEl.textContent = dragTitle;
      if (subEl) subEl.textContent = dragSub;
    }
  };

  const leaveDrag = () => {
    depth = 0;
    if (dropEl.classList.contains("is-dragging")) {
      dropEl.classList.remove("is-dragging");
      if (titleEl) titleEl.textContent = origTitle;
      if (subEl) subEl.textContent = origSub;
    }
  };

  dropEl.addEventListener("dragenter", (event) => {
    event.preventDefault();
    depth++;
    enterDrag();
  });
  dropEl.addEventListener("dragover", (event) => {
    event.preventDefault();
    enterDrag();
  });
  dropEl.addEventListener("dragleave", (event) => {
    depth = Math.max(0, depth - 1);
    if (depth === 0 || !dropEl.contains(event.relatedTarget)) {
      leaveDrag();
    }
  });
  dropEl.addEventListener("drop", (event) => {
    event.preventDefault();
    leaveDrag();
    const file = event.dataTransfer?.files?.[0];
    if (file && onFile) onFile(file);
  });
}

function initHome(root) {
  const panels = {
    overview: root.querySelector('[data-ds-panel="overview"]'),
    validate: root.querySelector('[data-ds-panel="validate"]'),
  };
  const overviewTabLink = root.querySelector('.ds-tabs a[href="/assinatura-digital"]');
  const validateLinks = root.querySelectorAll("[data-ds-tab-validate]");

  function switchTab(tab) {
    const targetTab = tab === "validate" ? "validate" : "overview";
    if (panels.overview) panels.overview.hidden = targetTab !== "overview";
    if (panels.validate) panels.validate.hidden = targetTab !== "validate";
    if (overviewTabLink) {
      if (targetTab === "overview") overviewTabLink.setAttribute("aria-current", "page");
      else overviewTabLink.removeAttribute("aria-current");
    }
    for (const link of root.querySelectorAll(".ds-tabs [data-ds-tab-validate]")) {
      if (targetTab === "validate") link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    }
    const url = new URL(location.href);
    if (targetTab === "validate") url.searchParams.set("aba", "validar");
    else url.searchParams.delete("aba");
    url.hash = "";
    window.history.replaceState({}, "", url);
  }

  overviewTabLink?.addEventListener("click", (event) => {
    event.preventDefault();
    switchTab("overview");
  });
  for (const link of validateLinks) {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      switchTab("validate");
    });
  }

  for (const select of root.querySelectorAll("[data-ds-submit-on-change]")) {
    select.addEventListener("change", () => select.form.requestSubmit());
  }

  // Delete unsigned document from (...) menu
  const listStatus = root.querySelector("[data-ds-list-status]");
  for (const btn of root.querySelectorAll("[data-ds-delete-doc]")) {
    btn.addEventListener("click", async () => {
      const docTitle = btn.dataset.dsDeleteTitle || "este documento";
      if (!window.confirm(`Excluir "${docTitle}"? Esta ação não pode ser desfeita.`)) return;
      feedback(listStatus, "Excluindo documento…");
      try {
        await request(root, `/assinatura-digital/documentos/${btn.dataset.dsDeleteDoc}`, {method: "DELETE"});
        location.reload();
      } catch (error) {
        feedback(listStatus, error.message, true);
      }
    });
  }

  // Validate PDF tab
  const validateDrop = root.querySelector('[data-ds-drop="validate"]');
  const validateStatus = root.querySelector("[data-ds-validate-status]");
  const validatePlaceholder = root.querySelector("[data-ds-validate-placeholder]");
  const validateResult = root.querySelector("[data-ds-result]");
  if (validateDrop) {
    const input = validateDrop.querySelector("input");
    let busy = false;
    const runValidation = async (file) => {
      if (!file || busy) return;
      if (!file.name.toLowerCase().endsWith(".pdf") || file.size > 12 * 1024 * 1024) {
        feedback(validateStatus, "Escolha um PDF de até 12 MB.", true);
        return;
      }
      busy = true;
      validateDrop.setAttribute("aria-busy", "true");
      feedback(validateStatus, "Verificando as assinaturas do PDF…");
      try {
        const data = await request(root, "/assinatura-digital/validar", {pdf: file, name: file.name});
        if (validatePlaceholder) validatePlaceholder.hidden = true;
        reportInto(validateResult, data, file.name);
        validateResult.focus();
        feedback(validateStatus);
      } catch (error) {
        feedback(validateStatus, error.message, true);
      } finally {
        input.value = "";
        busy = false;
        validateDrop.removeAttribute("aria-busy");
      }
    };
    input.addEventListener("change", () => runValidation(input.files?.[0]));
    bindAnimatedDropzone(validateDrop, {
      dragTitle: "Solte o PDF aqui para validar",
      dragSub: "A conferência das assinaturas iniciará automaticamente",
      onFile: runValidation,
    });
  }
}

const DS_MAIL_SVG = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 7 9 6 9-6"/></svg>';
const DS_WA_SVG = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5Z"/></svg>';

function dsEsc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function dsNorm(s) {
  return String(s || "").normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase();
}

function dsIsValidPhone(p) {
  return String(p || "").replace(/\D/g, "").length >= 10;
}

function dsIsValidEmail(e) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(e || "").trim());
}

function renderChannelSelectorHtml({ keyPrefix, activeChannel, email, phone, allowEditEmail = false, editingKey = null }) {
  const emailKey = `${keyPrefix}:email`;
  const waKey = `${keyPrefix}:whatsapp`;
  const emailActive = activeChannel === "email";
  const waActive = activeChannel === "whatsapp";
  const hasEmail = dsIsValidEmail(email);
  const hasWa = dsIsValidPhone(phone);

  const emailBoxCls = emailActive ? (hasEmail ? "is-active" : "is-missing") : "";
  const waBoxCls = waActive ? (hasWa ? "is-active-wa" : "is-missing") : "";
  const isEditing = editingKey === emailKey || editingKey === waKey;
  const editingWa = editingKey === waKey;

  return `
    <div class="ds-ch-grid" role="radiogroup" aria-label="Canal de entrega">
      <div class="ds-ch-box ${emailBoxCls}">
        <button type="button" role="radio" aria-checked="${emailActive}" data-ds-set-ch="${dsEsc(emailKey)}" class="ds-ch-box-btn">
          <span class="ds-ch-radio" aria-hidden="true"></span>
          <span class="ds-ch-text">
            <span class="ds-ch-title">${DS_MAIL_SVG}<span>E-mail</span></span>
            <span class="ds-ch-dest ${hasEmail ? "" : "is-warn"}">${hasEmail ? dsEsc(email) : "Sem e-mail · informar"}</span>
          </span>
        </button>
        ${allowEditEmail ? `
          <button type="button" data-ds-edit-contact="${dsEsc(emailKey)}" class="ds-ch-edit-btn ${hasEmail ? "" : "is-warn"}">${hasEmail ? "Editar" : "Informar"}</button>
        ` : ""}
      </div>
      <div class="ds-ch-box ${waBoxCls}">
        <button type="button" role="radio" aria-checked="${waActive}" data-ds-set-ch="${dsEsc(waKey)}" class="ds-ch-box-btn">
          <span class="ds-ch-radio" aria-hidden="true"></span>
          <span class="ds-ch-text">
            <span class="ds-ch-title">${DS_WA_SVG}<span>WhatsApp</span></span>
            <span class="ds-ch-dest ${hasWa ? "" : "is-warn"}">${hasWa ? dsEsc(maskPhone(phone)) : "Sem nº cadastrado"}</span>
          </span>
        </button>
        <button type="button" data-ds-edit-contact="${dsEsc(waKey)}" class="ds-ch-edit-btn ${hasWa ? "" : "is-warn"}">${hasWa ? "Editar" : "Informar"}</button>
      </div>
    </div>
    ${isEditing ? `
      <div class="ds-inline-edit-bar">
        <input type="${editingWa ? "tel" : "email"}"
               data-ds-inline-input="${dsEsc(editingKey)}"
               value="${dsEsc(editingWa ? maskPhone(phone) : email)}"
               placeholder="${editingWa ? "WhatsApp com DDD: (35) 99999-0000" : "email@empresa.com.br"}">
        <div class="ds-inline-edit-actions">
          <button type="button" class="sf-btn sf-btn-secondary ds-btn-sm" data-ds-inline-cancel>Cancelar</button>
          <button type="button" class="sf-btn sf-btn-primary ds-btn-sm" data-ds-inline-save="${dsEsc(editingKey)}">Salvar</button>
        </div>
      </div>
    ` : ""}
  `;
}

function describeStampConfig(appearance) {
  if (!appearance || !appearance.visible) {
    return "Sem selo visual (assinatura com Manifesto final)";
  }
  const styleTxt = appearance.style === "compact" ? "Selo Enxuto" : "Selo Completo";
  if (typeof appearance.page === "number" && Array.isArray(appearance.box) && appearance.box.length === 4) {
    return `${styleTxt} · Página ${appearance.page + 1} (posição definida)`;
  }
  return `${styleTxt} · posição padrão (ajuste no PDF se quiser)`;
}

function createStampPlacer(dialog) {
  if (!dialog) return { open: () => {} };
  const statusEl = dialog.querySelector("[data-ds-placer-status]");
  const sheet = dialog.querySelector("[data-ds-placer-sheet]");
  const canvas = dialog.querySelector("[data-ds-placer-canvas]");
  const stage = dialog.querySelector("[data-ds-placer-stage]");
  const stamp = dialog.querySelector("[data-ds-placer-stamp]");
  const stampNameEl = dialog.querySelector("[data-ds-placer-stamp-name]");
  const stampCpfEl = dialog.querySelector("[data-ds-placer-stamp-cpf]");
  const subEl = dialog.querySelector("[data-ds-placer-sub]");
  const visCheck = dialog.querySelector("[data-ds-placer-visible]");
  const styleWrap = dialog.querySelector("[data-ds-placer-style-wrap]");
  const styleBtns = [...dialog.querySelectorAll("[data-ds-placer-style]")];
  const prevBtn = dialog.querySelector("[data-ds-placer-prev]");
  const nextBtn = dialog.querySelector("[data-ds-placer-next]");
  const pgEl = dialog.querySelector("[data-ds-placer-pg]");
  const pagesEl = dialog.querySelector("[data-ds-placer-pages]");
  const confirmBtn = dialog.querySelector("[data-ds-placer-confirm]");

  let pdf = null;
  let pageNumber = 1;
  let renderedPage = 0;
  let viewport = null;
  let drawTask = null;
  let drawVersion = 0;
  let chosen = null;
  let stampStyle = "full";
  let showStamp = true;
  let drag = null;
  let onSaveCb = null;
  let currentSourceKey = null;

  const pixelRect = (loc) => {
    const [x1, y1, x2, y2] = viewport.convertToViewportRectangle(loc.box);
    return [Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.abs(y2 - y1)];
  };
  const fromPixels = (left, top, width, height) => {
    const [x1, y1] = viewport.convertToPdfPoint(left, top);
    const [x2, y2] = viewport.convertToPdfPoint(left + width, top + height);
    return { page: renderedPage - 1, box: [Math.min(x1, x2), Math.min(y1, y2), Math.max(x1, x2), Math.max(y1, y2)] };
  };
  const defaultLoc = () => {
    if (!viewport) return;
    const isCompact = stampStyle === "compact";
    const width = viewport.width * (isCompact ? 0.30 : 0.42);
    const height = Math.min(
      viewport.height * (isCompact ? 0.18 : 0.24),
      Math.max(viewport.height * (isCompact ? 0.095 : 0.12), width * (433 / 1024)),
    );
    chosen = fromPixels(
      viewport.width - width - viewport.width * 0.06,
      viewport.height - height - viewport.height * 0.06,
      width,
      height,
    );
  };

  const drawStamp = () => {
    if (styleWrap) styleWrap.hidden = !showStamp;
    for (const b of styleBtns) b.setAttribute("aria-checked", String(b.dataset.dsPlacerStyle === stampStyle));
    if (!stamp) return;
    stamp.classList.toggle("is-compact", stampStyle === "compact");
    if (!showStamp || !viewport || !chosen) {
      stamp.hidden = true;
      return;
    }
    if (chosen.page + 1 !== renderedPage) {
      // Bring stamp to current page when user navigates and wants to place it here
      stamp.hidden = true;
      return;
    }
    stamp.hidden = false;
    const [left, top, width, height] = pixelRect(chosen);
    Object.assign(stamp.style, { left: `${left}px`, top: `${top}px`, width: `${width}px`, height: `${height}px` });
  };

  const renderPage = async () => {
    if (!pdf) return;
    const version = ++drawVersion;
    if (drawTask) drawTask.cancel();
    try {
      const page = await pdf.getPage(pageNumber);
      if (version !== drawVersion) return;
      const raw = page.getViewport({ scale: 1 });
      const avail = Math.max(240, Math.min((stage?.clientWidth || 640) - 36, 720));
      const scale = Math.min(avail / raw.width, 1.35);
      const nextVp = page.getViewport({ scale: Math.max(0.05, scale) });
      const ratio = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(nextVp.width * ratio);
      canvas.height = Math.round(nextVp.height * ratio);
      canvas.style.width = `${nextVp.width}px`;
      canvas.style.height = `${nextVp.height}px`;
      drawTask = page.render({ canvas, viewport: nextVp, transform: ratio === 1 ? null : [ratio, 0, 0, ratio, 0, 0] });
      await drawTask.promise;
      if (version !== drawVersion) return;
      viewport = nextVp;
      renderedPage = pageNumber;
      if (statusEl) statusEl.hidden = true;
      if (sheet) sheet.hidden = false;
      if (pgEl) pgEl.textContent = String(pageNumber);
      if (pagesEl) pagesEl.textContent = String(pdf.numPages);
      if (prevBtn) prevBtn.disabled = pageNumber <= 1;
      if (nextBtn) nextBtn.disabled = pageNumber >= pdf.numPages;
      if (showStamp && !chosen) defaultLoc();
      drawStamp();
    } catch (err) {
      if (version !== drawVersion || err.name === "RenderingCancelledException") return;
      if (statusEl) {
        statusEl.hidden = false;
        statusEl.textContent = "Não foi possível desenhar esta página do PDF.";
      }
    }
  };

  const moveOrResize = (dx, dy, start, resize) => {
    let [left, top, width, height] = start;
    if (resize) {
      const min = Math.max(16, viewport.scale * 29);
      width = Math.max(Math.min(width, min), Math.min(viewport.width - left, width + dx));
      height = Math.max(Math.min(height, min), Math.min(viewport.height - top, height + dy));
    } else {
      left = Math.max(0, Math.min(viewport.width - width, left + dx));
      top = Math.max(0, Math.min(viewport.height - height, top + dy));
    }
    chosen = fromPixels(left, top, width, height);
    drawStamp();
  };

  stamp?.addEventListener("pointerdown", (ev) => {
    if (!viewport || !chosen || ev.button !== 0) return;
    stamp.setPointerCapture(ev.pointerId);
    drag = { x: ev.clientX, y: ev.clientY, start: pixelRect(chosen), resize: ev.target.matches("[data-ds-placer-handle]") };
    ev.preventDefault();
  });
  stamp?.addEventListener("pointermove", (ev) => {
    if (drag) moveOrResize(ev.clientX - drag.x, ev.clientY - drag.y, drag.start, drag.resize);
  });
  stamp?.addEventListener("lostpointercapture", () => { drag = null; });
  canvas?.addEventListener("click", (ev) => {
    if (!showStamp || !viewport) return;
    const rect = canvas.getBoundingClientRect();
    const isCompact = stampStyle === "compact";
    const width = viewport.width * (isCompact ? 0.30 : 0.42);
    const height = Math.min(viewport.height * (isCompact ? 0.18 : 0.24), Math.max(viewport.height * (isCompact ? 0.095 : 0.12), width * (433 / 1024)));
    const left = Math.max(0, Math.min(viewport.width - width, ev.clientX - rect.left - width / 2));
    const top = Math.max(0, Math.min(viewport.height - height, ev.clientY - rect.top - height / 2));
    chosen = fromPixels(left, top, width, height);
    drawStamp();
  });

  visCheck?.addEventListener("change", () => {
    showStamp = visCheck.checked;
    if (showStamp && (!chosen || chosen.page + 1 !== renderedPage)) defaultLoc();
    drawStamp();
  });
  for (const b of styleBtns) {
    b.addEventListener("click", () => {
      stampStyle = b.dataset.dsPlacerStyle === "compact" ? "compact" : "full";
      defaultLoc();
      drawStamp();
    });
  }
  prevBtn?.addEventListener("click", () => {
    if (!pdf || pageNumber <= 1) return;
    pageNumber -= 1;
    renderPage().then(() => { if (showStamp) { defaultLoc(); drawStamp(); } });
  });
  nextBtn?.addEventListener("click", () => {
    if (!pdf || pageNumber >= pdf.numPages) return;
    pageNumber += 1;
    renderPage().then(() => { if (showStamp) { defaultLoc(); drawStamp(); } });
  });
  confirmBtn?.addEventListener("click", () => {
    const result = showStamp && chosen
      ? { visible: true, style: stampStyle, page: chosen.page, box: chosen.box.map((v) => Math.round(v * 100) / 100) }
      : { visible: false };
    dialog.close();
    if (typeof onSaveCb === "function") onSaveCb(result);
  });
  dialog.addEventListener("click", (ev) => {
    if (ev.target === dialog) dialog.close();
  });
  for (const b of dialog.querySelectorAll("[data-ds-close-dialog]")) {
    b.addEventListener("click", () => dialog.close());
  }

  return {
    async open({ file = null, pdfUrl = "", signerName = "", cpf = "", appearance = null, onSave = null }) {
      onSaveCb = onSave;
      showStamp = appearance ? Boolean(appearance.visible) : true;
      stampStyle = appearance?.style === "compact" ? "compact" : "full";
      if (visCheck) visCheck.checked = showStamp;
      if (stampNameEl) stampNameEl.textContent = (signerName || "SIGNATÁRIO").trim().toUpperCase();
      if (stampCpfEl) stampCpfEl.textContent = cpf ? `CPF ${maskCpfPublic(cpf)}` : "CPF •••.•••.•••-••";
      if (subEl) subEl.textContent = `Arraste o selo ou clique na folha para definir onde ${signerName || "o signatário"} irá assinar.`;
      chosen = (appearance && appearance.visible && Array.isArray(appearance.box) && appearance.box.length === 4)
        ? { page: Number(appearance.page || 0), box: [...appearance.box] }
        : null;

      dialog.showModal();
      const sourceKey = file ? `file:${file.name}:${file.size}:${file.lastModified}` : `url:${pdfUrl}`;
      if (pdf && currentSourceKey === sourceKey) {
        pageNumber = chosen ? Math.min(pdf.numPages, Math.max(1, chosen.page + 1)) : pdf.numPages;
        await renderPage();
        return;
      }
      if (statusEl) {
        statusEl.hidden = false;
        statusEl.textContent = "Carregando prévia do PDF…";
      }
      if (sheet) sheet.hidden = true;
      try {
        const lib = await import(vendor + "legacy/build/pdf.min.mjs");
        lib.GlobalWorkerOptions.workerSrc = vendor + "legacy/build/pdf.worker.min.mjs";
        const docParams = {
          cMapUrl: vendor + "cmaps/", cMapPacked: true,
          standardFontDataUrl: vendor + "standard_fonts/", wasmUrl: vendor + "wasm/",
          isEvalSupported: false, enableXfa: false, maxImageSize: 24_000_000, stopAtErrors: true,
        };
        if (file) {
          docParams.data = new Uint8Array(await file.arrayBuffer());
        } else if (pdfUrl) {
          docParams.url = pdfUrl;
        } else {
          throw new Error("Nenhum arquivo PDF selecionado.");
        }
        pdf = await lib.getDocument(docParams).promise;
        currentSourceKey = sourceKey;
        pageNumber = chosen ? Math.min(pdf.numPages, Math.max(1, chosen.page + 1)) : pdf.numPages;
        await renderPage();
      } catch (err) {
        if (statusEl) {
          statusEl.hidden = false;
          statusEl.textContent = err.message || "Não foi possível carregar o PDF para posicionamento.";
        }
      }
    },
  };
}

function initWizard(root) {
  const form = root.querySelector("[data-ds-wiz-form]");
  if (!form) return;

  const parseJsonScript = (sel, fallback) => {
    const el = root.querySelector(sel);
    if (!el) return fallback;
    try { return JSON.parse(el.textContent || ""); } catch { return fallback; }
  };

  const colleagues = parseJsonScript("[data-ds-colleagues-data]", []).map((c) => ({
    id: String(c.id),
    name: c.name || "",
    initials: c.initials || "US",
    role: c.role || "",
    email: c.email || "",
    phone: c.phone ? maskPhone(c.phone) : "",
    selected: false,
    channel: "email",
  }));
  const selfUser = parseJsonScript("[data-ds-self-data]", { name: "Você", initials: "VC", email: "", phone: "" });

  const dropEl = root.querySelector("[data-ds-wiz-drop]");
  const fileInput = form.elements.file;
  const loadedBox = root.querySelector("[data-ds-wiz-loaded]");
  const loadedName = root.querySelector("[data-ds-wiz-file-name]");
  const loadedMeta = root.querySelector("[data-ds-wiz-file-meta]");
  const clearFileBtn = root.querySelector("[data-ds-wiz-clear-file]");
  const titleInput = root.querySelector("[data-ds-wiz-title]");
  const catSelect = root.querySelector("[data-ds-wiz-category]");
  const headingEl = root.querySelector("[data-ds-wiz-title-heading]");
  const crumbEl = root.querySelector("[data-ds-wiz-crumb]");
  const subtitle = root.querySelector("[data-ds-wiz-subtitle]");
  const pane1 = root.querySelector('[data-ds-wiz-pane="1"]');
  const pane2 = root.querySelector('[data-ds-wiz-pane="2"]');
  const cancelBtn = root.querySelector("[data-ds-wiz-cancel]");
  const backBtn = root.querySelector("[data-ds-wiz-back]");
  const nextBtn = root.querySelector("[data-ds-wiz-next]");
  const submitBtn = root.querySelector("[data-ds-wiz-submit]");
  const statusEl = root.querySelector("[data-ds-wiz-status]");

  const whoBtns = [...root.querySelectorAll("[data-ds-who]")];
  const whoThumb = root.querySelector("[data-ds-who-thumb]");
  const whoHintText = root.querySelector("[data-ds-who-hint-text]");

  const policyToggleBtn = root.querySelector("[data-ds-policy-toggle]");
  const policyToggleTxt = root.querySelector("[data-ds-policy-toggle-txt]");
  const policyDrawer = root.querySelector("[data-ds-policy-drawer]");
  const policyLabel = root.querySelector("[data-ds-policy-label]");
  const policyBadge = root.querySelector("[data-ds-policy-badge]");
  const policyDesc = root.querySelector("[data-ds-policy-desc]");
  const policyOpts = [...root.querySelectorAll("[data-ds-policy]")];
  const policySheet = root.querySelector("[data-ds-policy-sheet]");

  const stepDoc = root.querySelector('[data-wiz-step="doc"]');
  const stepSigners = root.querySelector('[data-wiz-step="signers"]');
  const stepReview = root.querySelector('[data-wiz-step="review"]');
  const stepConfirm = root.querySelector('[data-wiz-step="confirm"]');
  const stepSendReview = root.querySelector('[data-wiz-step="send_review"]');

  const sumStep = root.querySelector("[data-ds-sum-step]");
  const sumTitle = root.querySelector("[data-ds-sum-title]");
  const sumCat = root.querySelector("[data-ds-sum-cat]");
  const sumFile = root.querySelector("[data-ds-sum-file]");
  const sumPolicy = root.querySelector("[data-ds-sum-policy]");
  const sumWho = root.querySelector("[data-ds-sum-who]");
  const sumNext = root.querySelector("[data-ds-sum-next]");
  const sumPeopleWrap = root.querySelector("[data-ds-sum-people-wrap]");
  const sumPeopleList = root.querySelector("[data-ds-sum-people]");

  const mobSummary = root.querySelector("[data-ds-mob-summary]");
  const mobStepPill = root.querySelector("[data-ds-mob-step-pill]");
  const mobCta = root.querySelector("[data-ds-mob-cta]");

  const pickerWrap = root.querySelector("[data-ds-picker-wrap]");
  const teamSearch = root.querySelector("[data-ds-team-search]");
  const teamDropdown = root.querySelector("[data-ds-team-dropdown]");
  const quickWrap = root.querySelector("[data-ds-quick-wrap]");
  const quickChips = root.querySelector("[data-ds-quick-chips]");
  const selectedTitle = root.querySelector("[data-ds-selected-title]");
  const selectedList = root.querySelector("[data-ds-selected-list]");

  const toggleExtFormBtn = root.querySelector("[data-ds-toggle-ext-form]");
  const extIcpWarn = root.querySelector("[data-ds-ext-icp-warn]");
  const extAdder = root.querySelector("[data-ds-ext-adder]");
  const extCancelBtn = root.querySelector("[data-ds-ext-cancel]");
  const extName = root.querySelector("[data-ds-ext-name]");
  const extCpf = root.querySelector("[data-ds-ext-cpf]");
  const extRole = root.querySelector("[data-ds-ext-role]");
  const extChannel = root.querySelector("[data-ds-ext-channel]");
  const extPhoneWrap = root.querySelector("[data-ds-ext-phone-wrap]");
  const extEmailWrap = root.querySelector("[data-ds-ext-email-wrap]");
  const extPhone = root.querySelector("[data-ds-ext-phone]");
  const extEmail = root.querySelector("[data-ds-ext-email]");
  const extShowStamp = root.querySelector("[data-ds-ext-show-stamp]");
  const extStampSummary = root.querySelector("[data-ds-ext-stamp-summary]");
  const extStampActions = root.querySelector("[data-ds-ext-stamp-actions]");
  const extOpenPlacerBtn = root.querySelector("[data-ds-ext-open-placer]");
  const extAddBtn = root.querySelector("[data-ds-ext-add-btn]");
  const extErr = root.querySelector("[data-ds-ext-err]");
  const manifestoNote = root.querySelector("[data-ds-manifesto-note]");
  const placer = createStampPlacer(root.querySelector("[data-ds-placer-dialog]"));

  // Mobile Bottom Sheets
  const teamSheet = root.querySelector("[data-ds-team-sheet]");
  const mobOpenTeamBtn = root.querySelector("[data-ds-mob-open-team]");
  const mobTeamSearch = root.querySelector("[data-ds-mob-team-search]");
  const mobTeamList = root.querySelector("[data-ds-mob-team-list]");
  const mobTeamConfirm = root.querySelector("[data-ds-mob-team-confirm]");

  const extSheet = root.querySelector("[data-ds-ext-sheet]");
  const mobOpenExtBtn = root.querySelector("[data-ds-mob-open-ext]");
  const mobExtName = root.querySelector("[data-ds-mob-ext-name]");
  const mobExtCpf = root.querySelector("[data-ds-mob-ext-cpf]");
  const mobExtRole = root.querySelector("[data-ds-mob-ext-role]");
  const mobExtChBtns = [...root.querySelectorAll("[data-ds-mob-ext-ch]")];
  const mobExtContactLbl = root.querySelector("[data-ds-mob-ext-contact-lbl]");
  const mobExtContact = root.querySelector("[data-ds-mob-ext-contact]");
  const mobExtShowStamp = root.querySelector("[data-ds-mob-ext-show-stamp]");
  const mobExtErr = root.querySelector("[data-ds-mob-ext-err]");
  const mobExtSave = root.querySelector("[data-ds-mob-ext-save]");

  let selectedFile = null;
  let whoSigns = "only_me";
  let signingPolicy = "flexible";
  let currentStep = 1;
  let deskPolicyOpen = false;
  let editingKey = null;
  let mobExtChannel = "whatsapp";
  let pendingExtAppearance = { visible: true, style: "full" };
  const externalSigners = [];

  const WHO_META = {
    only_me: {
      idx: 0,
      short: "Só eu",
      hint: "Você revisa o PDF e assina agora mesmo na próxima tela.",
    },
    me_and_others: {
      idx: 1,
      short: "Eu + outros",
      hint: "Na próxima tela você adiciona pessoas ou links externos.",
    },
    only_others: {
      idx: 2,
      short: "Só outros",
      hint: "Você não assina: na próxima tela você seleciona pessoas ou links externos.",
    },
  };

  const POLICY_META = {
    flexible: {
      label: "Livre escolha",
      badge: "Padrão recomendado",
      desc: "Cada pessoa escolhe entre Eletrônica SiSFarm ou ICP-Brasil A1.",
    },
    electronic: {
      label: "Somente Eletrônica SiSFarm",
      badge: "Sem certificado",
      desc: "CPF + código por WhatsApp/e-mail, QR Code e Manifesto automático.",
    },
    icp_brasil: {
      label: "Somente ICP-Brasil A1",
      badge: "Exige A1",
      desc: "Exige certificado digital A1 de todos os participantes.",
    },
  };

  const isMobileViewport = () => window.matchMedia("(max-width: 639px)").matches;

  function renderDesktopDropdown() {
    if (!teamDropdown) return;
    const q = dsNorm(teamSearch?.value.trim() || "");
    const matches = colleagues.filter(
      (m) => !q || dsNorm(m.name).includes(q) || dsNorm(m.role).includes(q) || dsNorm(m.email).includes(q) || dsNorm(m.phone).includes(q)
    );
    if (!matches.length) {
      teamDropdown.innerHTML = `<div class="ds-picker-empty">${colleagues.length ? "Nenhuma pessoa encontrada na equipe." : "Não há outros usuários cadastrados nesta organização."}</div>`;
      return;
    }
    teamDropdown.innerHTML = matches.map((m) => `
      <button type="button" class="ds-picker-item ${m.selected ? "is-picked" : ""}" data-ds-pick-member="${dsEsc(m.id)}">
        <span class="ds-picker-item-left">
          <span class="ds-avatar ${m.selected ? "" : "is-soft"}">${m.selected ? "✓" : dsEsc(m.initials)}</span>
          <span class="ds-picker-item-copy">
            <strong>${dsEsc(m.name)}</strong>
            <small>${m.role ? `${dsEsc(m.role)} · ` : ""}${dsEsc(m.email)}${m.phone ? ` · WhatsApp ${dsEsc(m.phone)}` : " · Sem WhatsApp"}</small>
          </span>
        </span>
        <span class="ds-picker-item-act">${m.selected ? "Adicionado" : "+ Incluir"}</span>
      </button>
    `).join("");
    for (const btn of teamDropdown.querySelectorAll("[data-ds-pick-member]")) {
      btn.addEventListener("click", () => {
        const item = colleagues.find((x) => x.id === btn.dataset.dsPickMember);
        if (item) {
          item.selected = !item.selected;
          feedback(statusEl);
          syncUI();
          renderDesktopDropdown();
        }
      });
    }
  }

  function renderMobileTeamSheet() {
    if (!mobTeamList) return;
    const q = dsNorm(mobTeamSearch?.value.trim() || "");
    const matches = colleagues.filter(
      (m) => !q || dsNorm(m.name).includes(q) || dsNorm(m.role).includes(q) || dsNorm(m.email).includes(q) || dsNorm(m.phone).includes(q)
    );
    if (!matches.length) {
      mobTeamList.innerHTML = `<div class="ds-picker-empty">${colleagues.length ? "Nenhuma pessoa encontrada na equipe." : "Não há outros usuários cadastrados nesta organização."}</div>`;
    } else {
      mobTeamList.innerHTML = matches.map((m) => `
        <button type="button" class="ds-sheet-team-item ${m.selected ? "is-picked" : ""}" data-ds-mob-pick="${dsEsc(m.id)}">
          <span class="ds-picker-item-left">
            <span class="ds-avatar ${m.selected ? "" : "is-soft"}">${m.selected ? "✓" : dsEsc(m.initials)}</span>
            <span class="ds-picker-item-copy">
              <strong>${dsEsc(m.name)}</strong>
              <small>${m.role ? `${dsEsc(m.role)} · ` : ""}${m.phone ? dsEsc(m.phone) : "Sem WhatsApp"}</small>
            </span>
          </span>
          <span class="ds-picker-item-act">${m.selected ? "Selecionado" : "Selecionar"}</span>
        </button>
      `).join("");
    }
    const count = colleagues.filter((m) => m.selected).length;
    if (mobTeamConfirm) mobTeamConfirm.textContent = count ? `Confirmar seleção (${count})` : "Concluir";
    for (const btn of mobTeamList.querySelectorAll("[data-ds-mob-pick]")) {
      btn.addEventListener("click", () => {
        const item = colleagues.find((x) => x.id === btn.dataset.dsMobPick);
        if (item) {
          item.selected = !item.selected;
          feedback(statusEl);
          renderMobileTeamSheet();
          syncUI();
        }
      });
    }
  }

  function focusInlineEditor() {
    if (!editingKey || !selectedList) return;
    const inp = selectedList.querySelector(`[data-ds-inline-input="${CSS.escape(editingKey)}"]`);
    if (inp) {
      inp.focus();
      inp.setSelectionRange(inp.value.length, inp.value.length);
    }
  }

  function renderSelectedSigners() {
    const picked = colleagues.filter((m) => m.selected);
    const activeExt = signingPolicy === "icp_brasil" ? [] : externalSigners;
    const selfSigns = whoSigns !== "only_others";
    const total = (selfSigns ? 1 : 0) + picked.length + activeExt.length;

    if (selectedTitle) selectedTitle.textContent = `Signatários deste documento (${total})`;

    if (quickChips && quickWrap) {
      quickWrap.hidden = colleagues.length === 0;
      quickChips.innerHTML = colleagues.slice(0, 4).map((m) => `
        <button type="button" class="ds-quick-chip ${m.selected ? "is-picked" : ""}" data-ds-quick="${dsEsc(m.id)}">
          ${m.selected ? "✓ " : "+ "}${dsEsc(m.name)}
        </button>
      `).join("");
      for (const btn of quickChips.querySelectorAll("[data-ds-quick]")) {
        btn.addEventListener("click", () => {
          const item = colleagues.find((x) => x.id === btn.dataset.dsQuick);
          if (item) {
            item.selected = !item.selected;
            feedback(statusEl);
            syncUI();
          }
        });
      }
    }

    if (!selectedList) return;
    const rows = [];
    if (selfSigns) {
      rows.push(`
        <div class="ds-signer-card is-self">
          <div class="ds-signer-card-head">
            <div class="ds-signer-card-person">
              <span class="ds-avatar is-self">${dsEsc(selfUser.initials || "VC")}</span>
              <div class="ds-signer-card-copy">
                <strong>${dsEsc(selfUser.name || "Você")} <span>(Você)</span></strong>
                <small>Assina na próxima etapa (Revisar e assinar)</small>
              </div>
            </div>
            <span class="ds-policy-badge">Automático</span>
          </div>
        </div>
      `);
    }

    for (const m of picked) {
      rows.push(`
        <div class="ds-signer-card">
          <div class="ds-signer-card-head">
            <div class="ds-signer-card-person">
              <span class="ds-avatar">${dsEsc(m.initials)}</span>
              <div class="ds-signer-card-copy">
                <strong>${dsEsc(m.name)}${m.role ? ` <span>· ${dsEsc(m.role)}</span>` : ""}</strong>
              </div>
            </div>
            <button type="button" class="ds-rm-btn" data-ds-rm-team="${dsEsc(m.id)}" aria-label="Remover ${dsEsc(m.name)}" title="Remover">✕</button>
          </div>
          <span class="ds-signer-ch-label">Forma de envio do link e código:</span>
          ${renderChannelSelectorHtml({
            keyPrefix: `team:${m.id}`,
            activeChannel: m.channel,
            email: m.email,
            phone: m.phone,
            allowEditEmail: false,
            editingKey,
          })}
        </div>
      `);
    }

    activeExt.forEach((ext, idx) => {
      rows.push(`
        <div class="ds-signer-card is-ext">
          <div class="ds-signer-card-head">
            <div class="ds-signer-card-person">
              <span class="ds-avatar is-ext">EXT</span>
              <div class="ds-signer-card-copy">
                <strong>${dsEsc(ext.name)} <span>· ${dsEsc(ext.role_label || "Signatário")}</span></strong>
                ${ext.cpf ? `<small>CPF ${dsEsc(ext.cpf)} · somente conferência para o signatário</small>` : ""}
              </div>
            </div>
            <button type="button" class="ds-rm-btn" data-ds-rm-ext="${idx}" aria-label="Remover ${dsEsc(ext.name)}" title="Remover">✕</button>
          </div>
          <span class="ds-signer-ch-label">Canal travado de envio do link e código:</span>
          ${renderChannelSelectorHtml({
            keyPrefix: `ext:${idx}`,
            activeChannel: ext.channel,
            email: ext.email,
            phone: ext.phone,
            allowEditEmail: true,
            editingKey,
          })}
          <div class="ds-signer-stamp-bar">
            <span><strong>Selo no PDF:</strong> ${dsEsc(describeStampConfig(ext.appearance))}</span>
            <button type="button" class="ds-inline-link" data-ds-pos-ext="${idx}">Posicionar / alterar selo</button>
          </div>
        </div>
      `);
    });

    selectedList.innerHTML = rows.length
      ? rows.join("")
      : '<p class="ds-hint">Nenhuma pessoa selecionada ainda. Use a busca acima ou convide uma pessoa externa.</p>';

    for (const btn of selectedList.querySelectorAll("[data-ds-set-ch]")) {
      btn.addEventListener("click", () => {
        const [scope, id, ch] = btn.dataset.dsSetCh.split(":");
        feedback(statusEl);
        if (scope === "team") {
          const item = colleagues.find((x) => x.id === id);
          if (!item) return;
          item.channel = ch;
          if (ch === "whatsapp" && !dsIsValidPhone(item.phone)) {
            editingKey = `team:${id}:whatsapp`;
          } else if (editingKey && editingKey.startsWith(`team:${id}:`)) {
            editingKey = null;
          }
        } else if (scope === "ext") {
          const ext = externalSigners[Number(id)];
          if (!ext) return;
          ext.channel = ch;
          if ((ch === "whatsapp" && !dsIsValidPhone(ext.phone)) || (ch === "email" && !dsIsValidEmail(ext.email))) {
            editingKey = `ext:${id}:${ch}`;
          } else if (editingKey && editingKey.startsWith(`ext:${id}:`)) {
            editingKey = null;
          }
        }
        syncUI();
        if (editingKey) focusInlineEditor();
      });
    }

    for (const btn of selectedList.querySelectorAll("[data-ds-edit-contact]")) {
      btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        editingKey = editingKey === btn.dataset.dsEditContact ? null : btn.dataset.dsEditContact;
        syncUI();
        if (editingKey) focusInlineEditor();
      });
    }

    for (const btn of selectedList.querySelectorAll("[data-ds-inline-cancel]")) {
      btn.addEventListener("click", () => {
        editingKey = null;
        syncUI();
      });
    }

    const saveInline = (key, rawVal) => {
      const [scope, id, ch] = key.split(":");
      const val = ch === "whatsapp" ? maskPhone(rawVal) : String(rawVal || "").trim();
      if (ch === "whatsapp" && !dsIsValidPhone(val)) {
        feedback(statusEl, "Informe um número de WhatsApp válido com DDD (ex.: (35) 99999-0000).", true);
        return;
      }
      if (ch === "email" && !dsIsValidEmail(val)) {
        feedback(statusEl, "Informe um e-mail válido.", true);
        return;
      }
      if (scope === "team") {
        const item = colleagues.find((x) => x.id === id);
        if (item) {
          if (ch === "whatsapp") item.phone = val;
          else item.email = val;
          item.channel = ch;
        }
      } else if (scope === "ext") {
        const ext = externalSigners[Number(id)];
        if (ext) {
          if (ch === "whatsapp") ext.phone = val;
          else ext.email = val;
          ext.channel = ch;
        }
      }
      editingKey = null;
      feedback(statusEl);
      syncUI();
    };

    for (const btn of selectedList.querySelectorAll("[data-ds-inline-save]")) {
      btn.addEventListener("click", () => {
        const key = btn.dataset.dsInlineSave;
        const inp = selectedList.querySelector(`[data-ds-inline-input="${CSS.escape(key)}"]`);
        if (inp) saveInline(key, inp.value);
      });
    }

    for (const inp of selectedList.querySelectorAll("[data-ds-inline-input]")) {
      if (inp.dataset.dsInlineInput.endsWith(":whatsapp")) {
        inp.addEventListener("input", () => { inp.value = maskPhone(inp.value); });
      }
      inp.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter") { ev.preventDefault(); saveInline(inp.dataset.dsInlineInput, inp.value); }
        if (ev.key === "Escape") { editingKey = null; syncUI(); }
      });
    }

    for (const btn of selectedList.querySelectorAll("[data-ds-rm-team]")) {
      btn.addEventListener("click", () => {
        const item = colleagues.find((x) => x.id === btn.dataset.dsRmTeam);
        if (item) { item.selected = false; feedback(statusEl); syncUI(); }
      });
    }

    for (const btn of selectedList.querySelectorAll("[data-ds-rm-ext]")) {
      btn.addEventListener("click", () => {
        externalSigners.splice(Number(btn.dataset.dsRmExt), 1);
        feedback(statusEl);
        syncUI();
      });
    }

    for (const btn of selectedList.querySelectorAll("[data-ds-pos-ext]")) {
      btn.addEventListener("click", () => {
        const idx = Number(btn.dataset.dsPosExt);
        const ext = externalSigners[idx];
        const file = selectedFile || fileInput?.files?.[0];
        if (!ext || !file) return;
        placer.open({
          file,
          signerName: ext.name,
          cpf: ext.cpf || "",
          appearance: ext.appearance || { visible: true, style: "full" },
          onSave: (res) => {
            ext.appearance = res;
            syncUI();
          },
        });
      });
    }
  }

  function validateStep2Signers() {
    const picked = colleagues.filter((m) => m.selected);
    const activeExt = signingPolicy === "icp_brasil" ? [] : externalSigners;
    for (const m of picked) {
      if (m.channel === "whatsapp" && !dsIsValidPhone(m.phone)) {
        editingKey = `team:${m.id}:whatsapp`;
        feedback(statusEl, `Informe o WhatsApp (com DDD) de ${m.name} ou selecione E-mail.`, true);
        syncUI();
        focusInlineEditor();
        return false;
      }
    }
    for (let i = 0; i < activeExt.length; i++) {
      const ext = activeExt[i];
      if (ext.channel === "whatsapp" && !dsIsValidPhone(ext.phone)) {
        editingKey = `ext:${i}:whatsapp`;
        feedback(statusEl, `Informe o WhatsApp (com DDD) de ${ext.name} ou selecione E-mail.`, true);
        syncUI();
        focusInlineEditor();
        return false;
      }
      if (ext.channel === "email" && !dsIsValidEmail(ext.email)) {
        editingKey = `ext:${i}:email`;
        feedback(statusEl, `Informe o e-mail de ${ext.name} ou selecione WhatsApp.`, true);
        syncUI();
        focusInlineEditor();
        return false;
      }
    }
    return true;
  }

  function syncUI() {
    const needsStep2 = whoSigns !== "only_me";
    if (!needsStep2) currentStep = 1;
    const totalSteps = whoSigns === "me_and_others" ? 4 : 3;
    const allowExternal = signingPolicy !== "icp_brasil";
    const wMeta = WHO_META[whoSigns] || WHO_META.only_me;
    const pMeta = POLICY_META[signingPolicy] || POLICY_META.flexible;

    pane1.hidden = currentStep !== 1;
    pane2.hidden = currentStep !== 2;
    if (cancelBtn) cancelBtn.hidden = currentStep !== 1;
    if (backBtn) backBtn.hidden = currentStep !== 2;
    if (nextBtn) nextBtn.hidden = !(currentStep === 1 && needsStep2);
    if (submitBtn) submitBtn.hidden = currentStep === 1 && needsStep2;

    if (headingEl) headingEl.textContent = currentStep === 1 ? "Novo documento" : "Quem vai assinar";
    if (crumbEl) crumbEl.textContent = currentStep === 1 ? "Novo documento" : "Quem vai assinar";
    if (subtitle) subtitle.textContent = "Envie o PDF, escolha a modalidade e defina quem vai assinar.";

    if (whoThumb) whoThumb.style.transform = `translateX(${wMeta.idx * 100}%)`;
    if (whoHintText) whoHintText.textContent = wMeta.hint;
    for (const btn of whoBtns) btn.setAttribute("aria-checked", String(btn.dataset.dsWho === whoSigns));

    if (policyLabel) policyLabel.textContent = pMeta.label;
    if (policyBadge) policyBadge.textContent = pMeta.badge;
    if (policyDesc) policyDesc.textContent = pMeta.desc;
    if (policyDrawer) policyDrawer.hidden = !deskPolicyOpen;
    if (policyToggleTxt) policyToggleTxt.textContent = deskPolicyOpen ? "Fechar" : "Alterar";
    if (policyToggleBtn) policyToggleBtn.setAttribute("aria-expanded", String(deskPolicyOpen));
    for (const opt of policyOpts) opt.setAttribute("aria-checked", String(opt.dataset.dsPolicy === signingPolicy));

    if (extIcpWarn) extIcpWarn.hidden = allowExternal;
    if (toggleExtFormBtn) toggleExtFormBtn.hidden = !allowExternal;
    if (mobOpenExtBtn) mobOpenExtBtn.hidden = !allowExternal;
    if (!allowExternal && extAdder) extAdder.hidden = true;
    if (manifestoNote) manifestoNote.hidden = !allowExternal;

    // Top step indicators
    stepDoc.classList.toggle("is-done", currentStep > 1);
    if (currentStep === 1) stepDoc.setAttribute("aria-current", "step");
    else stepDoc.removeAttribute("aria-current");
    stepDoc.querySelector("span").textContent = currentStep > 1 ? "✓" : "1";

    stepSigners.hidden = !needsStep2;
    if (currentStep === 2) stepSigners.setAttribute("aria-current", "step");
    else stepSigners.removeAttribute("aria-current");

    stepReview.hidden = whoSigns === "only_others";
    stepConfirm.hidden = whoSigns === "only_others";
    stepSendReview.hidden = whoSigns !== "only_others";

    if (whoSigns === "only_me") {
      stepReview.querySelector("[data-wiz-num]").textContent = "2";
      stepConfirm.querySelector("[data-wiz-num]").textContent = "3";
    } else if (whoSigns === "me_and_others") {
      stepReview.querySelector("[data-wiz-num]").textContent = "3";
      stepConfirm.querySelector("[data-wiz-num]").textContent = "4";
    } else {
      stepSendReview.querySelector("[data-wiz-num]").textContent = "3";
    }

    const submitLabel = whoSigns === "only_others"
      ? "Enviar convites e abrir documento →"
      : "Continuar para Revisar e assinar →";
    if (submitBtn) submitBtn.textContent = submitLabel;

    // Summary sidebar
    if (sumStep) sumStep.textContent = `Passo ${currentStep} de ${totalSteps}`;
    const docName = (titleInput?.value || "").trim() || (selectedFile ? selectedFile.name.replace(/\.pdf$/i, "") : "Nenhum arquivo selecionado");
    if (sumTitle) sumTitle.textContent = docName;
    if (sumCat) sumCat.textContent = catSelect?.value || "Outros";
    if (sumFile) sumFile.textContent = selectedFile ? selectedFile.name : "Aguardando envio";
    if (sumPolicy) sumPolicy.textContent = pMeta.label;
    const picked = colleagues.filter((m) => m.selected);
    const activeExternals = allowExternal ? externalSigners : [];
    const totalOthers = picked.length + activeExternals.length;
    if (sumWho) {
      if (whoSigns === "only_me") sumWho.textContent = "Apenas você";
      else if (whoSigns === "me_and_others") sumWho.textContent = totalOthers ? `Você + ${totalOthers} pessoa(s)` : "Você e outras pessoas";
      else sumWho.textContent = totalOthers ? `${totalOthers} pessoa(s)` : "Apenas outras pessoas";
    }
    if (sumNext) {
      if (currentStep === 1 && needsStep2) sumNext.textContent = "Quem assina";
      else sumNext.textContent = whoSigns === "only_others" ? "Conferir e enviar" : "Revisar e assinar";
    }
    if (sumPeopleWrap && sumPeopleList) {
      sumPeopleWrap.hidden = !needsStep2 || totalOthers === 0;
      const items = [
        ...picked.map((m) => {
          const chTxt = m.channel === "whatsapp" ? (m.phone ? `WhatsApp ${m.phone}` : "WhatsApp pendente") : "E-mail";
          return element("li", `• ${m.name} (${chTxt})`);
        }),
        ...activeExternals.map((ext) => {
          const chTxt = ext.channel === "whatsapp" ? `WhatsApp ${ext.phone || "pendente"}` : `E-mail ${ext.email || "pendente"}`;
          return element("li", `• ${ext.name} (externo · ${chTxt})`);
        }),
      ];
      sumPeopleList.replaceChildren(...items);
    }

    // Mobile sticky bar
    if (mobSummary) mobSummary.textContent = `${wMeta.short} · ${pMeta.label}`;
    if (mobStepPill) mobStepPill.textContent = `${currentStep}/${totalSteps}`;
    if (mobCta) {
      mobCta.textContent = currentStep === 1 && needsStep2
        ? "Continuar para Quem assina →"
        : submitLabel;
    }

    renderSelectedSigners();
  }

  const pickFile = (file) => {
    if (!file) return;
    if (!file.name.toLowerCase().endsWith(".pdf") || file.size > 12 * 1024 * 1024) {
      feedback(statusEl, "Escolha um arquivo PDF de até 12 MB.", true);
      return;
    }
    selectedFile = file;
    feedback(statusEl);
    if (dropEl) dropEl.hidden = true;
    if (loadedBox) loadedBox.hidden = false;
    if (loadedName) loadedName.textContent = file.name;
    if (loadedMeta) loadedMeta.textContent = `${Math.max(1, Math.round(file.size / 1024))} KB · PDF pronto para assinatura`;
    if (titleInput && !titleInput.value.trim()) {
      titleInput.value = file.name.replace(/\.pdf$/i, "");
    }
    syncUI();
  };

  fileInput?.addEventListener("change", () => pickFile(fileInput.files?.[0]));
  clearFileBtn?.addEventListener("click", () => {
    selectedFile = null;
    if (fileInput) fileInput.value = "";
    if (loadedBox) loadedBox.hidden = true;
    if (dropEl) dropEl.hidden = false;
    syncUI();
  });

  bindAnimatedDropzone(dropEl, {
    dragTitle: "Solte o PDF aqui para enviar",
    dragSub: "O arquivo será carregado automaticamente",
    onFile: (file) => {
      if (fileInput) {
        const dt = new DataTransfer();
        dt.items.add(file);
        fileInput.files = dt.files;
      }
      pickFile(file);
    },
  });

  for (const btn of whoBtns) {
    btn.addEventListener("click", () => {
      whoSigns = btn.dataset.dsWho || "only_me";
      syncUI();
    });
  }

  policyToggleBtn?.addEventListener("click", () => {
    if (isMobileViewport() && policySheet) {
      policySheet.showModal();
    } else {
      deskPolicyOpen = !deskPolicyOpen;
      syncUI();
    }
  });

  for (const opt of policyOpts) {
    opt.addEventListener("click", () => {
      signingPolicy = opt.dataset.dsPolicy || "flexible";
      deskPolicyOpen = false;
      if (policySheet?.open) policySheet.close();
      syncUI();
    });
  }

  for (const btn of root.querySelectorAll("[data-ds-goto-step]")) {
    btn.addEventListener("click", () => {
      const target = Number(btn.dataset.dsGotoStep);
      if (target === 1) {
        currentStep = 1;
        feedback(statusEl);
        syncUI();
        window.scrollTo({ top: 0, behavior: "smooth" });
      } else if (target === 2 && whoSigns !== "only_me") {
        const file = selectedFile || fileInput?.files?.[0];
        if (!file) {
          feedback(statusEl, "Selecione ou arraste um arquivo PDF antes de continuar.", true);
          return;
        }
        currentStep = 2;
        feedback(statusEl);
        syncUI();
        window.scrollTo({ top: 0, behavior: "smooth" });
      }
    });
  }

  for (const btn of root.querySelectorAll("[data-ds-close-dialog]")) {
    btn.addEventListener("click", () => btn.closest("dialog")?.close());
  }
  for (const dlg of [policySheet, teamSheet, extSheet].filter(Boolean)) {
    dlg.addEventListener("click", (ev) => {
      if (ev.target === dlg) dlg.close();
    });
  }

  // Desktop Search Dropdown
  teamSearch?.addEventListener("focus", () => {
    renderDesktopDropdown();
    if (teamDropdown) teamDropdown.hidden = false;
  });
  teamSearch?.addEventListener("input", () => {
    renderDesktopDropdown();
    if (teamDropdown) teamDropdown.hidden = false;
  });
  document.addEventListener("click", (ev) => {
    if (pickerWrap && teamDropdown && !pickerWrap.contains(ev.target)) {
      teamDropdown.hidden = true;
    }
  });

  // Mobile Team Sheet
  mobOpenTeamBtn?.addEventListener("click", () => {
    renderMobileTeamSheet();
    teamSheet?.showModal();
  });
  mobTeamSearch?.addEventListener("input", renderMobileTeamSheet);

  // Desktop External Signer Form
  toggleExtFormBtn?.addEventListener("click", () => {
    if (!extAdder) return;
    extAdder.hidden = !extAdder.hidden;
    if (!extAdder.hidden) extName?.focus();
  });
  extCancelBtn?.addEventListener("click", () => {
    if (extAdder) extAdder.hidden = true;
    feedback(extErr);
  });
  extChannel?.addEventListener("change", () => {
    const isWa = extChannel.value === "whatsapp";
    if (extPhoneWrap) extPhoneWrap.hidden = !isWa;
    if (extEmailWrap) extEmailWrap.hidden = isWa;
    feedback(extErr);
  });

  const syncPendingStampUI = () => {
    const show = Boolean(extShowStamp?.checked);
    if (!show) pendingExtAppearance = { visible: false };
    else if (!pendingExtAppearance.visible) pendingExtAppearance = { visible: true, style: "full" };
    if (extStampActions) extStampActions.hidden = !show;
    if (extStampSummary) extStampSummary.textContent = describeStampConfig(pendingExtAppearance);
  };
  extShowStamp?.addEventListener("change", syncPendingStampUI);
  extOpenPlacerBtn?.addEventListener("click", () => {
    const file = selectedFile || fileInput?.files?.[0];
    if (!file) {
      feedback(extErr, "Escolha um arquivo PDF no Passo 1 antes de posicionar o selo.", true);
      return;
    }
    placer.open({
      file,
      signerName: (extName?.value || "").trim() || "Signatário externo",
      cpf: (extCpf?.value || "").trim(),
      appearance: pendingExtAppearance,
      onSave: (res) => {
        pendingExtAppearance = res;
        if (extShowStamp) extShowStamp.checked = Boolean(res.visible);
        syncPendingStampUI();
      },
    });
  });

  function tryCommitPendingExternal() {
    if (signingPolicy === "icp_brasil" || extAdder?.hidden) return true;
    const name = (extName?.value || "").trim();
    const cpf = maskCpf(extCpf?.value || "");
    const phone = (extPhone?.value || "").trim();
    const email = (extEmail?.value || "").trim();
    if (!name && !cpf && !phone && !email) return true;
    const role_label = (extRole?.value || "").trim() || "Signatário";
    const channel = extChannel?.value === "email" ? "email" : "whatsapp";
    if (name.length < 3) {
      feedback(extErr, "Informe o nome completo do signatário externo.", true);
      extName?.focus();
      return false;
    }
    if (!isValidCpf(cpf)) {
      feedback(extErr, "Informe um CPF válido para o signatário externo.", true);
      extCpf?.focus();
      return false;
    }
    if (channel === "whatsapp" && !dsIsValidPhone(phone)) {
      feedback(extErr, "Informe um número de WhatsApp válido com DDD.", true);
      extPhone?.focus();
      return false;
    }
    if (channel === "email" && !dsIsValidEmail(email)) {
      feedback(extErr, "Informe um e-mail válido para o signatário externo.", true);
      extEmail?.focus();
      return false;
    }
    externalSigners.push({
      name,
      cpf,
      role_label,
      channel,
      phone: channel === "whatsapp" ? maskPhone(phone) : "",
      email: channel === "email" ? email : "",
      appearance: { ...pendingExtAppearance },
    });
    if (extName) extName.value = "";
    if (extCpf) extCpf.value = "";
    if (extPhone) extPhone.value = "";
    if (extEmail) extEmail.value = "";
    pendingExtAppearance = { visible: true, style: "full" };
    if (extShowStamp) extShowStamp.checked = true;
    syncPendingStampUI();
    if (extAdder) extAdder.hidden = true;
    feedback(extErr);
    syncUI();
    return true;
  }

  extAddBtn?.addEventListener("click", () => {
    tryCommitPendingExternal();
  });

  // Mobile External Signer Sheet
  mobOpenExtBtn?.addEventListener("click", () => {
    feedback(mobExtErr);
    extSheet?.showModal();
  });
  for (const btn of mobExtChBtns) {
    btn.addEventListener("click", () => {
      mobExtChannel = btn.dataset.dsMobExtCh === "email" ? "email" : "whatsapp";
      for (const b of mobExtChBtns) {
        const isWa = b.dataset.dsMobExtCh === "whatsapp";
        const active = b.dataset.dsMobExtCh === mobExtChannel;
        b.setAttribute("aria-checked", String(active));
        b.classList.toggle("is-wa-active", active && isWa);
        b.classList.toggle("is-mail-active", active && !isWa);
      }
      if (mobExtContactLbl) mobExtContactLbl.textContent = mobExtChannel === "whatsapp" ? "WhatsApp (com DDD)" : "E-mail do signatário";
      if (mobExtContact) {
        mobExtContact.type = mobExtChannel === "whatsapp" ? "tel" : "email";
        mobExtContact.placeholder = mobExtChannel === "whatsapp" ? "(35) 99999-0000" : "contato@empresa.com.br";
        mobExtContact.value = "";
      }
      feedback(mobExtErr);
    });
  }
  mobExtContact?.addEventListener("input", () => {
    if (mobExtChannel === "whatsapp") mobExtContact.value = maskPhone(mobExtContact.value);
  });
  mobExtSave?.addEventListener("click", () => {
    const name = (mobExtName?.value || "").trim();
    const cpf = maskCpf(mobExtCpf?.value || "");
    const role_label = (mobExtRole?.value || "").trim() || "Signatário";
    const contact = (mobExtContact?.value || "").trim();
    if (name.length < 3) {
      feedback(mobExtErr, "Informe o nome completo do signatário.", true);
      mobExtName?.focus();
      return;
    }
    if (!isValidCpf(cpf)) {
      feedback(mobExtErr, "Informe um CPF válido para o signatário.", true);
      mobExtCpf?.focus();
      return;
    }
    if (mobExtChannel === "whatsapp" && !dsIsValidPhone(contact)) {
      feedback(mobExtErr, "Informe um WhatsApp válido com DDD.", true);
      mobExtContact?.focus();
      return;
    }
    if (mobExtChannel === "email" && !dsIsValidEmail(contact)) {
      feedback(mobExtErr, "Informe um e-mail válido.", true);
      mobExtContact?.focus();
      return;
    }
    const showStampMob = mobExtShowStamp ? Boolean(mobExtShowStamp.checked) : true;
    externalSigners.push({
      name,
      cpf,
      role_label,
      channel: mobExtChannel,
      phone: mobExtChannel === "whatsapp" ? maskPhone(contact) : "",
      email: mobExtChannel === "email" ? contact : "",
      appearance: showStampMob ? { visible: true, style: "full" } : { visible: false },
    });
    if (mobExtName) mobExtName.value = "";
    if (mobExtCpf) mobExtCpf.value = "";
    if (mobExtContact) mobExtContact.value = "";
    feedback(mobExtErr);
    extSheet?.close();
    syncUI();
  });

  titleInput?.addEventListener("input", syncUI);
  catSelect?.addEventListener("change", syncUI);

  const goStep2 = () => {
    const file = selectedFile || fileInput?.files?.[0];
    if (!file) {
      feedback(statusEl, "Selecione ou arraste um arquivo PDF antes de continuar.", true);
      return;
    }
    feedback(statusEl);
    currentStep = 2;
    syncUI();
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  nextBtn?.addEventListener("click", goStep2);
  backBtn?.addEventListener("click", () => {
    currentStep = 1;
    feedback(statusEl);
    syncUI();
  });
  mobCta?.addEventListener("click", () => {
    if (currentStep === 1 && whoSigns !== "only_me") goStep2();
    else form.requestSubmit();
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = selectedFile || fileInput?.files?.[0];
    if (!file) {
      feedback(statusEl, "Selecione ou arraste um arquivo PDF antes de continuar.", true);
      return;
    }
    if (whoSigns !== "only_me" && !tryCommitPendingExternal()) {
      feedback(statusEl, "Corrija os dados do signatário externo antes de continuar.", true);
      return;
    }
    const chosen = whoSigns !== "only_me" ? colleagues.filter((m) => m.selected) : [];
    const activeExternals = (whoSigns !== "only_me" && signingPolicy !== "icp_brasil") ? externalSigners : [];
    if (whoSigns !== "only_me" && chosen.length === 0 && activeExternals.length === 0) {
      feedback(statusEl, "Selecione pelo menos um usuário da equipe ou adicione um signatário externo.", true);
      return;
    }
    if (whoSigns !== "only_me" && !validateStep2Signers()) return;

    if (submitBtn) submitBtn.disabled = true;
    if (mobCta) mobCta.disabled = true;
    feedback(statusEl, "Enviando e preparando o documento…");
    try {
      const created = await request(root, "/assinatura-digital/documentos", {
        pdf: file,
        name: file.name,
        title: (titleInput?.value || "").trim(),
        category: catSelect?.value || "Outros",
        ownerSigns: whoSigns !== "only_others",
        signingPolicy,
      });
      const warnings = [];
      for (const m of chosen) {
        try {
          const payload = { email: m.email, channel: m.channel, consent: true };
          if (m.channel === "whatsapp" && m.phone) payload.phone = m.phone;
          const res = await request(root, `${created.url}/participantes`, { json: payload });
          if (res && res.notification_error) warnings.push(res.notification_error);
        } catch (err) {
          warnings.push(err.message || `Não foi possível compartilhar com ${m.name}.`);
        }
      }
      for (const ext of activeExternals) {
        try {
          const res = await request(root, `${created.url}/externos`, { json: ext });
          if (res && res.notification_error) warnings.push(res.notification_error);
        } catch (err) {
          warnings.push(err.message || `Não foi possível convidar ${ext.name}.`);
        }
      }
      const params = new URLSearchParams();
      if (whoSigns === "only_others") {
        params.set("aba", "detalhes");
      } else {
        params.set("fluxo", whoSigns);
      }
      if (warnings.length) {
        params.set("aviso", warnings.join(" "));
      }
      location.assign(`${created.url}?${params.toString()}`);
    } catch (error) {
      feedback(statusEl, error.message, true);
      if (submitBtn) submitBtn.disabled = false;
      if (mobCta) mobCta.disabled = false;
    }
  });

  syncUI();
}

function initWorkspace(root) {
  const find = (selector) => root.querySelector(selector);
  const options = JSON.parse(find("[data-ds-options]").textContent);
  const endpoint = root.dataset.documentUrl;
  const view = find("[data-ds-scroll]");
  const previewStatus = find("[data-ds-preview-status]");
  const sheet = find("[data-ds-sheet]");
  const canvas = find("[data-ds-canvas]");
  const thumbnails = find("[data-ds-thumbs]");
  const thumbnailToggle = find("[data-ds-thumbs-toggle]");
  const pageInput = find("[data-ds-page]");
  const previous = find("[data-ds-prev]");
  const next = find("[data-ds-next]");
  const stamp = find("[data-ds-stamp]");
  const stampHead = find("[data-ds-stamp-head]");
  const stampQr = find("[data-ds-stamp-qr]");
  const stampLabel = find("[data-ds-stamp-label]");
  const stampDoc = find("[data-ds-stamp-doc]");
  const stampDate = find("[data-ds-stamp-date]");
  const stampUrl = find("[data-ds-stamp-url]");
  const stampModesWrap = find("[data-ds-stamp-modes]");
  const stampModeButtons = [...root.querySelectorAll("[data-ds-stamp-style]")];
  const jumpStamp = find("[data-ds-jump-stamp]");
  const certificate = find("[data-ds-certificate]");
  const target = find("[data-ds-target]");
  const visible = find("[data-ds-visible]");
  const placement = find("[data-ds-placement]");
  const placeOnPage = find("[data-ds-place-current]");
  const consent = find("[data-ds-consent]");
  const consentText = find("[data-ds-consent-text]");
  const authorize = find("[data-ds-authorize]");
  const authLabel = find("[data-ds-auth-label]");
  const status = find("[data-ds-status]");
  const report = find("[data-ds-result]");
  const confirmForm = find("[data-ds-sign-form]");
  const dialogStatus = find("[data-ds-confirm-status]");

  // Electronic signing elements & modality switcher
  const modeButtons = [...root.querySelectorAll("[data-ds-sign-mode]")];
  const modePanes = [...root.querySelectorAll("[data-ds-mode-pane]")];
  const elNameInput = find("[data-ds-el-name]");
  const elNameWrap = find("[data-ds-el-name-wrap]");
  const elNamePreview = find("[data-ds-el-name-preview]");
  const elNameToggle = find("[data-ds-el-name-toggle]");
  const elCpfInput = find("[data-ds-el-cpf]");
  const elChannelSelect = find("[data-ds-el-channel]");
  const elPhoneInput = find("[data-ds-el-phone]");
  const wsSelfChannelBox = find("[data-ds-ws-self-channel-box]");
  const stampToggleTitle = find("[data-ds-stamp-toggle-title]");
  const stampToggleSub = find("[data-ds-stamp-toggle-sub]");
  const verCode = root.dataset.verificationCode || "";
  const docTitle = root.dataset.docTitle || "este documento";

  elNameToggle?.addEventListener("click", () => {
    if (!elNameWrap) return;
    elNameWrap.hidden = !elNameWrap.hidden;
    elNameToggle.textContent = elNameWrap.hidden ? "Editar nome" : "Ocultar";
    if (!elNameWrap.hidden) elNameInput?.focus();
  });
  elNameInput?.addEventListener("input", () => {
    if (elNamePreview) elNamePreview.textContent = elNameInput.value.trim() || root.dataset.userName || "";
  });

  if (wsSelfChannelBox && elChannelSelect) {
    let wsSelfEditing = null;
    const renderWsSelfChannels = () => {
      const ch = elChannelSelect.value === "whatsapp" ? "whatsapp" : "email";
      const email = root.dataset.userEmail || "";
      const phone = elPhoneInput ? elPhoneInput.value : (root.dataset.userPhone || "");
      wsSelfChannelBox.innerHTML = renderChannelSelectorHtml({
        keyPrefix: "ws:self",
        activeChannel: ch,
        email,
        phone,
        allowEditEmail: false,
        editingKey: wsSelfEditing,
      });
      for (const btn of wsSelfChannelBox.querySelectorAll("[data-ds-set-ch]")) {
        btn.addEventListener("click", () => {
          const nextCh = btn.dataset.dsSetCh.endsWith(":whatsapp") ? "whatsapp" : "email";
          elChannelSelect.value = nextCh;
          if (nextCh === "whatsapp" && !dsIsValidPhone(elPhoneInput?.value || "")) {
            wsSelfEditing = "ws:self:whatsapp";
          } else {
            wsSelfEditing = null;
          }
          renderWsSelfChannels();
          if (wsSelfEditing) wsSelfChannelBox.querySelector("[data-ds-inline-input]")?.focus();
        });
      }
      for (const btn of wsSelfChannelBox.querySelectorAll("[data-ds-edit-contact]")) {
        btn.addEventListener("click", (ev) => {
          ev.stopPropagation();
          wsSelfEditing = wsSelfEditing === btn.dataset.dsEditContact ? null : btn.dataset.dsEditContact;
          renderWsSelfChannels();
          if (wsSelfEditing) wsSelfChannelBox.querySelector("[data-ds-inline-input]")?.focus();
        });
      }
      wsSelfChannelBox.querySelector("[data-ds-inline-cancel]")?.addEventListener("click", () => {
        wsSelfEditing = null;
        renderWsSelfChannels();
      });
      const saveWsPhone = (raw) => {
        const val = maskPhone(raw);
        if (!dsIsValidPhone(val)) {
          feedback(status, "Informe um WhatsApp válido com DDD (ex.: (35) 99999-0000).", true);
          return;
        }
        if (elPhoneInput) elPhoneInput.value = val;
        elChannelSelect.value = "whatsapp";
        wsSelfEditing = null;
        feedback(status);
        renderWsSelfChannels();
      };
      wsSelfChannelBox.querySelector("[data-ds-inline-save]")?.addEventListener("click", () => {
        const inp = wsSelfChannelBox.querySelector("[data-ds-inline-input]");
        if (inp) saveWsPhone(inp.value);
      });
      const inp = wsSelfChannelBox.querySelector("[data-ds-inline-input]");
      if (inp) {
        inp.addEventListener("input", () => { inp.value = maskPhone(inp.value); });
        inp.addEventListener("keydown", (ev) => {
          if (ev.key === "Enter") { ev.preventDefault(); saveWsPhone(inp.value); }
          if (ev.key === "Escape") { wsSelfEditing = null; renderWsSelfChannels(); }
        });
      }
    };
    renderWsSelfChannels();
  }

  let activeMode = root.dataset.defaultMode === "icp" ? "icp" : "electronic";
  if (activeMode === "electronic" && root.dataset.allowElectronic !== "yes") activeMode = "icp";
  if (activeMode === "icp" && root.dataset.allowIcp !== "yes") activeMode = "electronic";
  let stampStyle = "full"; // full | compact

  // Workspace view tabs
  const wsTabs = [...root.querySelectorAll("[data-ds-ws-tab]")];
  const wsPanels = [...root.querySelectorAll("[data-ds-ws-panel]")];
  const switchWsTab = (tab) => {
    for (const b of wsTabs) b.setAttribute("aria-current", b.dataset.dsWsTab === tab ? "page" : "false");
    for (const p of wsPanels) p.hidden = p.dataset.dsWsPanel !== tab;
    if (tab === "assinar" && pdf) render();
  };
  for (const btn of wsTabs) {
    btn.addEventListener("click", () => switchWsTab(btn.dataset.dsWsTab));
  }
  for (const btn of root.querySelectorAll("[data-ds-open-details]")) {
    btn.addEventListener("click", () => switchWsTab("detalhes"));
  }

  function setCeremonyStep(step) {
    for (const pane of root.querySelectorAll("[data-ds-step-pane]")) {
      pane.hidden = Number(pane.dataset.dsStepPane) !== step;
    }
    for (const pill of root.querySelectorAll("[data-ds-step-pill]")) {
      const num = Number(pill.dataset.dsStepPill);
      pill.classList.toggle("is-done", num < step);
      if (num === step) pill.setAttribute("aria-current", "step");
      else pill.removeAttribute("aria-current");
    }
    const grid = find("[data-ds-ceremony-grid]");
    const viewer = find("[data-ds-viewer-section]");
    const isDone = step === 3;
    if (grid) grid.classList.toggle("is-completed", isDone);
    if (viewer) viewer.hidden = isDone;
    if (isDone) window.scrollTo({top: 0, behavior: "smooth"});
  }

  let pdf;
  let pageNumber = 1;
  let renderedPage = 0;
  let zoom = 1;
  let viewport;
  let drawTask;
  let drawVersion = 0;
  let chosenLocation = null;
  let drag;
  let busy = false;
  let challengeId;

  function syncModeUI() {
    const isEl = activeMode === "electronic";
    for (const btn of modeButtons) {
      btn.setAttribute("aria-checked", String(btn.dataset.dsSignMode === activeMode));
    }
    for (const pane of modePanes) {
      pane.hidden = pane.dataset.dsModePane !== activeMode;
    }
    if (stampToggleTitle) {
      stampToggleTitle.textContent = isEl ? "Mostrar selo visual SiSFarm com QR Code" : "Mostrar carimbo visual ICP-Brasil na página";
    }
    if (stampToggleSub) {
      stampToggleSub.textContent = isEl
        ? "Exibe o selo próprio do SiSFarm com QR Code de validação no PDF."
        : "Exibe o selo ICP-Brasil diretamente no PDF.";
    }
    if (consentText) {
      consentText.replaceChildren();
      if (isEl) {
        consentText.append(
          document.createTextNode("Li o documento "),
          element("strong", docTitle),
          document.createTextNode(" e concordo expressamente em assiná-lo de forma eletrônica pelo SiSFarm, ciente do registro de CPF, data, hora, IP e dispositivo (MP 2.200-2/2001, Lei 14.063/2020 e LGPD)."),
        );
      } else {
        consentText.append(
          document.createTextNode("Li o documento "),
          element("strong", docTitle),
          document.createTextNode(" e autorizo expressamente minha assinatura digital ICP-Brasil nesta versão."),
        );
      }
    }
    if (authLabel) {
      const ch = isEl ? (elChannelSelect?.value || "email") : "email";
      authLabel.textContent = ch === "whatsapp" ? "Enviar código por WhatsApp" : "Enviar código por e-mail";
    }
    const blockedBanner = find("[data-ds-icp-blocked-banner]");
    if (blockedBanner) blockedBanner.hidden = isEl;
  }

  function syncAuthorization() {
    if (!consent || !authorize) return;
    syncModeUI();
    const consentBox = find("[data-ds-consent-box]");
    const isEl = activeMode === "electronic";

    if (isEl) {
      const nameOk = Boolean((elNameInput?.value || "").trim().length >= 3);
      const cpfOk = isValidCpf(elCpfInput?.value || "");
      const readyIdentity = nameOk && cpfOk;
      if (!readyIdentity) consent.checked = false;
      consent.disabled = !readyIdentity;
      if (consentBox) consentBox.classList.toggle("is-disabled", !readyIdentity);
      const canPlace = !visible?.checked || chosenLocation;
      authorize.disabled = authorize.dataset.elEnabled !== "yes" || !viewport || !consent.checked || !readyIdentity || !canPlace || busy;
      return;
    }

    const hasCert = Boolean(certificate && certificate.value);
    if (!hasCert) {
      consent.checked = false;
    }
    consent.disabled = !hasCert;
    if (consentBox) consentBox.classList.toggle("is-disabled", !hasCert);

    const certCardTop = find("[data-ds-cert-card-top]");
    const certName = find("[data-ds-cert-card-name]");
    const certMeta = find("[data-ds-cert-card-meta]");
    const opt = certificate?.selectedOptions?.[0];
    if (certCardTop && opt && opt.value) {
      certCardTop.hidden = false;
      if (certName) certName.textContent = opt.dataset.holder || opt.textContent;
      if (certMeta) certMeta.textContent = `${opt.dataset.type || "Certificado A1"} · vence em ${opt.dataset.until || ""}`;
    } else if (certCardTop) {
      certCardTop.hidden = true;
    }

    const canPlace = target?.value || !visible?.checked || chosenLocation;
    authorize.disabled = authorize.dataset.icpEnabled !== "yes" || !viewport || !consent.checked || !hasCert || !canPlace || busy;
  }

  function pixelRect(location) {
    const [x1, y1, x2, y2] = viewport.convertToViewportRectangle(location.box);
    return [Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.abs(y2 - y1)];
  }

  function fromPixels(left, top, width, height) {
    const [x1, y1] = viewport.convertToPdfPoint(left, top);
    const [x2, y2] = viewport.convertToPdfPoint(left + width, top + height);
    return {page: renderedPage - 1, box: [Math.min(x1, x2), Math.min(y1, y2), Math.max(x1, x2), Math.max(y1, y2)]};
  }

  function defaultLocation() {
    if (!viewport) return;
    const isCompact = stampStyle === "compact";
    const width = viewport.width * (isCompact ? 0.30 : 0.42);
    const height = Math.min(
      viewport.height * (isCompact ? 0.18 : 0.24),
      Math.max(viewport.height * (isCompact ? 0.095 : 0.12), width * (433 / 1024)),
    );
    chosenLocation = fromPixels(viewport.width - width - viewport.width * 0.06, viewport.height - height - viewport.height * 0.06, width, height);
  }

  const selectedField = () => options.fields.find((field) => field.name === target?.value);

  function focusStampElement() {
    if (!stamp || stamp.hidden) return;
    stamp.scrollIntoView({behavior: "smooth", block: "center"});
    stamp.focus({preventScroll: true});
  }

  function drawStamp() {
    if (!authorize) {
      if (stamp) stamp.hidden = true;
      if (jumpStamp) jumpStamp.hidden = true;
      return;
    }
    const isEl = activeMode === "electronic";
    const fixedField = isEl ? null : selectedField();
    const location = fixedField ? fixedField.location : visible?.checked ? chosenLocation : null;
    stamp.hidden = !viewport || !location || location.page + 1 !== renderedPage;
    stamp.classList.toggle("is-fixed", Boolean(fixedField));
    stamp.classList.toggle("is-compact", stampStyle === "compact");
    stamp.classList.toggle("is-electronic", isEl);
    stamp.tabIndex = fixedField ? -1 : 0;
    if (stampQr) stampQr.hidden = !isEl;
    if (jumpStamp) jumpStamp.hidden = !location;
    if (!stamp.hidden) {
      const [left, top, width, height] = pixelRect(location);
      Object.assign(stamp.style, {left: `${left}px`, top: `${top}px`, width: `${width}px`, height: `${height}px`});
      const nowStr = formatTime.format(new Date());
      if (isEl) {
        if (stampHead) stampHead.textContent = "SISFARM · ASSINATURA ELETRÔNICA · AVANÇADA";
        const signerName = ((elNameInput?.value || root.dataset.userName || "SIGNATÁRIO").trim()).toUpperCase();
        const maskedCpf = maskCpfPublic(elCpfInput?.value || "");
        stampLabel.textContent = signerName;
        if (stampStyle === "compact") {
          if (stampDoc) {
            stampDoc.textContent = [maskedCpf, nowStr].filter(Boolean).join(" · ");
            stampDoc.hidden = false;
          }
          if (stampDate) stampDate.hidden = true;
          if (stampUrl) stampUrl.textContent = `Código: ${verCode}`;
        } else {
          if (stampDoc) {
            stampDoc.textContent = maskedCpf ? `CPF ${maskedCpf} · Lei 14.063/2020` : "Lei 14.063/2020 · MP 2.200-2";
            stampDoc.hidden = false;
          }
          if (stampDate) {
            stampDate.textContent = `Data: ${nowStr} (BRT)`;
            stampDate.hidden = false;
          }
          if (stampUrl) stampUrl.textContent = `app.sisfarm.com.br/validar/${verCode}`;
        }
      } else {
        if (stampHead) stampHead.textContent = "ASSINADO DIGITALMENTE · ICP-BRASIL";
        const opt = certificate?.selectedOptions?.[0];
        const holder = (opt?.dataset.holder || "Assinatura digital").toUpperCase();
        const maskedDoc = opt?.dataset.doc || "";
        const certType = opt?.dataset.type || "";
        stampLabel.textContent = holder;
        if (stampStyle === "compact") {
          if (stampDoc) {
            stampDoc.textContent = [maskedDoc, nowStr].filter(Boolean).join(" · ");
            stampDoc.hidden = false;
          }
          if (stampDate) stampDate.hidden = true;
          if (stampUrl) stampUrl.textContent = "validar.iti.gov.br";
        } else {
          if (stampDoc) {
            stampDoc.textContent = [maskedDoc, certType].filter(Boolean).join(" · ");
            stampDoc.hidden = !stampDoc.textContent;
          }
          if (stampDate) {
            stampDate.textContent = `Data: ${nowStr} (BRT)`;
            stampDate.hidden = false;
          }
          if (stampUrl) stampUrl.textContent = "Verifique em validar.iti.gov.br";
        }
      }
    }
    if (placement) placement.hidden = Boolean(fixedField);
    if (stampModesWrap) stampModesWrap.hidden = !visible?.checked;
    if (placeOnPage) placeOnPage.hidden = !visible?.checked || !chosenLocation || chosenLocation.page + 1 === renderedPage;
    for (const btn of stampModeButtons) {
      btn.setAttribute("aria-checked", String(btn.dataset.dsStampStyle === stampStyle));
    }
    syncAuthorization();
  }

  async function render() {
    if (!pdf) return;
    const version = ++drawVersion;
    if (drawTask) drawTask.cancel();
    try {
      const page = await pdf.getPage(pageNumber);
      if (version !== drawVersion) return;
      const raw = page.getViewport({scale: 1});
      const padding = parseFloat(getComputedStyle(view).paddingLeft) * 2;
      const availableWidth = Math.max(220, view.clientWidth - padding);
      const fitted = Math.min(availableWidth / raw.width, 1.32);
      const scale = Math.min(fitted * zoom, 2000 / Math.max(raw.width, raw.height));
      const nextViewport = page.getViewport({scale: Math.max(0.02, scale)});
      const ratio = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(nextViewport.width * ratio);
      canvas.height = Math.round(nextViewport.height * ratio);
      canvas.style.width = `${nextViewport.width}px`;
      canvas.style.height = `${nextViewport.height}px`;
      drawTask = page.render({
        canvas, viewport: nextViewport, transform: ratio === 1 ? null : [ratio, 0, 0, ratio, 0, 0],
      });
      await drawTask.promise;
      if (version !== drawVersion) return;
      viewport = nextViewport;
      renderedPage = pageNumber;
      previewStatus.hidden = true;
      sheet.hidden = false;
      pageInput.value = String(pageNumber);
      pageInput.max = String(pdf.numPages);
      find("[data-ds-pages]").textContent = String(pdf.numPages);
      previous.disabled = pageNumber <= 1;
      next.disabled = pageNumber >= pdf.numPages;
      thumbnails.querySelector("[aria-current]")?.removeAttribute("aria-current");
      thumbnails.querySelector(`[data-page="${pageNumber}"]`)?.setAttribute("aria-current", "page");
      if (visible?.checked && !chosenLocation) defaultLocation();
      drawStamp();
    } catch (error) {
      if (version !== drawVersion || error.name === "RenderingCancelledException") return;
      viewport = null;
      sheet.hidden = true;
      previewStatus.hidden = false;
      previewStatus.textContent = "Não foi possível desenhar esta página do PDF.";
      syncAuthorization();
    }
  }

  function changePage(value) {
    if (!pdf) return;
    pageNumber = Math.max(1, Math.min(pdf.numPages, value || 1));
    pageInput.value = String(pageNumber);
    view.scrollTop = 0;
    viewport = null;
    syncAuthorization();
    render();
  }

  async function drawThumbnail(button) {
    try {
      const page = await pdf.getPage(Number(button.dataset.page));
      const thumb = element("canvas");
      const base = page.getViewport({scale: 1});
      const tiny = page.getViewport({scale: 60 / Math.max(base.width, base.height)});
      thumb.width = tiny.width;
      thumb.height = tiny.height;
      thumb.setAttribute("aria-hidden", "true");
      await page.render({canvas: thumb, viewport: tiny}).promise;
      button.prepend(thumb);
    } catch {
      // Page number remains visible.
    }
  }

  function initThumbnails() {
    const observer = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        observer.unobserve(entry.target);
        drawThumbnail(entry.target);
      }
    }, {root: thumbnails, rootMargin: "60px"});
    for (let number = 1; number <= pdf.numPages; number++) {
      const button = element("button", String(number));
      button.type = "button";
      button.dataset.page = String(number);
      button.setAttribute("aria-label", `Página ${number}`);
      button.addEventListener("click", () => changePage(number));
      thumbnails.append(button);
      observer.observe(button);
    }
  }

  async function loadPdf() {
    try {
      const lib = await import(vendor + "legacy/build/pdf.min.mjs");
      lib.GlobalWorkerOptions.workerSrc = vendor + "legacy/build/pdf.worker.min.mjs";
      pdf = await lib.getDocument({
        url: root.dataset.pdfUrl, cMapUrl: vendor + "cmaps/", cMapPacked: true,
        standardFontDataUrl: vendor + "standard_fonts/", wasmUrl: vendor + "wasm/",
        isEvalSupported: false, enableXfa: false, maxImageSize: 24_000_000, stopAtErrors: true,
      }).promise;
      if (pdf.numPages !== options.pages) throw new Error("A estrutura do PDF diverge da prévia.");
      if (selectedField()?.location) pageNumber = selectedField().location.page + 1;
      await render();
    } catch {
      previewStatus.hidden = false;
      previewStatus.textContent = "A prévia não está disponível. Use Baixar PDF ou abra o documento novamente.";
    }
  }

  window.addEventListener("resize", () => { if (pdf) render(); });
  previous.addEventListener("click", () => changePage(pageNumber - 1));
  next.addEventListener("click", () => changePage(pageNumber + 1));
  pageInput.addEventListener("change", () => changePage(Number(pageInput.value)));
  thumbnailToggle?.addEventListener("click", () => {
    if (!pdf) return;
    thumbnails.hidden = !thumbnails.hidden;
    thumbnailToggle.setAttribute("aria-expanded", String(!thumbnails.hidden));
    if (!thumbnails.children.length) initThumbnails();
    render();
  });
  for (const control of root.querySelectorAll("[data-ds-zoom]")) {
    control.addEventListener("click", () => {
      zoom = Math.max(0.65, Math.min(2.75, zoom + Number(control.dataset.dsZoom) * 0.2));
      render();
    });
  }
  find("[data-ds-fit]")?.addEventListener("click", () => { zoom = 1; render(); });

  jumpStamp?.addEventListener("click", () => {
    const fixedField = selectedField();
    const loc = fixedField ? fixedField.location : chosenLocation;
    if (!loc) return;
    if (loc.page + 1 !== renderedPage) {
      changePage(loc.page + 1);
    } else {
      focusStampElement();
    }
  });

  function resizeOrMove(dx, dy, start, resize) {
    let [left, top, width, height] = start;
    if (resize) {
      const minimum = Math.max(12, viewport.scale * 29);
      width = Math.max(Math.min(width, minimum), Math.min(viewport.width - left, width + dx));
      height = Math.max(Math.min(height, minimum), Math.min(viewport.height - top, height + dy));
    } else {
      left = Math.max(0, Math.min(viewport.width - width, left + dx));
      top = Math.max(0, Math.min(viewport.height - height, top + dy));
    }
    chosenLocation = fromPixels(left, top, width, height);
    syncChange();
  }

  function syncChange() {
    if (consent) consent.checked = false;
    drawStamp();
  }

  stamp.addEventListener("pointerdown", (event) => {
    if (!viewport || selectedField() || !chosenLocation || event.button !== 0) return;
    stamp.setPointerCapture(event.pointerId);
    drag = {x: event.clientX, y: event.clientY, start: pixelRect(chosenLocation), resize: event.target.matches("[data-ds-handle]")};
    event.preventDefault();
  });
  stamp.addEventListener("pointermove", (event) => {
    if (drag) resizeOrMove(event.clientX - drag.x, event.clientY - drag.y, drag.start, drag.resize);
  });
  stamp.addEventListener("lostpointercapture", () => { drag = null; });
  stamp.addEventListener("keydown", (event) => {
    const moves = {ArrowLeft: [-8, 0], ArrowRight: [8, 0], ArrowUp: [0, -8], ArrowDown: [0, 8]};
    if (!moves[event.key] || !chosenLocation || selectedField()) return;
    event.preventDefault();
    resizeOrMove(...moves[event.key], pixelRect(chosenLocation), event.shiftKey);
  });

  certificate?.addEventListener("change", syncChange);
  for (const btn of modeButtons) {
    btn.addEventListener("click", () => {
      activeMode = btn.dataset.dsSignMode === "icp" ? "icp" : "electronic";
      if (consent) consent.checked = false;
      feedback(status);
      drawStamp();
    });
  }
  elNameInput?.addEventListener("input", drawStamp);
  elCpfInput?.addEventListener("input", drawStamp);
  elChannelSelect?.addEventListener("change", syncAuthorization);
  visible?.addEventListener("change", () => {
    if (visible.checked && !chosenLocation) defaultLocation();
    syncChange();
    if (visible.checked) {
      requestAnimationFrame(() => focusStampElement());
    }
  });
  for (const btn of stampModeButtons) {
    btn.addEventListener("click", () => {
      stampStyle = btn.dataset.dsStampStyle === "compact" ? "compact" : "full";
      defaultLocation();
      syncChange();
      requestAnimationFrame(() => focusStampElement());
    });
  }
  placeOnPage?.addEventListener("click", () => {
    defaultLocation();
    syncChange();
    requestAnimationFrame(() => focusStampElement());
  });
  target?.addEventListener("change", () => {
    if (selectedField()?.location) changePage(selectedField().location.page + 1);
    syncChange();
  });
  consent?.addEventListener("change", syncAuthorization);

  async function dispatchWorkspaceAuthorize(overrideChannel = null, isResend = false) {
    const isEl = activeMode === "electronic";
    const field = isEl ? null : selectedField();
    const appearance = field
      ? {field: field.name}
      : visible?.checked
      ? {visible: true, style: stampStyle, ...chosenLocation}
      : {visible: false};
    busy = true;
    syncAuthorization();
    const channel = overrideChannel || (isEl ? (elChannelSelect?.value || "email") : "email");
    const targetFeedback = isResend ? dialogStatus : status;
    feedback(targetFeedback, channel === "whatsapp" ? "Enviando código de confirmação por WhatsApp…" : "Enviando código de confirmação por e-mail…");
    try {
      let data;
      if (isEl) {
        data = await request(root, endpoint + "/eletronica/autorizar", {
          json: {
            signer_name: (elNameInput?.value || "").trim(),
            cpf: (elCpfInput?.value || "").trim(),
            channel,
            phone: channel === "whatsapp" ? (elPhoneInput?.value || "").trim() : "",
            appearance,
            consent: consent.checked,
          },
        });
      } else {
        data = await request(root, endpoint + "/autorizar", {
          json: {certificate_id: certificate.value, appearance, consent: consent.checked, sha256: options.sha256},
        });
      }
      challengeId = data.challenge_id;
      const step2Title = find("[data-ds-step2-title]");
      const step2Sub = find("[data-ds-step2-sub]");
      if (isEl) {
        if (step2Title) step2Title.textContent = channel === "whatsapp" ? "Digite o código enviado no WhatsApp" : "Digite o código enviado ao seu e-mail";
        if (step2Sub) step2Sub.textContent = `Enviamos um código de 8 dígitos para ${data.destination_masked || "seu contato"}.`;
        find("[data-ds-confirm-holder]").textContent = `Assinatura Eletrônica · ${(elNameInput?.value || "").trim()}`;
      } else {
        if (step2Title) step2Title.textContent = "Digite o código enviado ao seu e-mail";
        if (step2Sub) step2Sub.textContent = `Enviamos um código de 8 dígitos para ${root.dataset.userEmail}. Não é necessário informar a senha do certificado.`;
        find("[data-ds-confirm-holder]").textContent = `ICP-Brasil A1 · ${certificate.selectedOptions[0]?.dataset.holder || data.holder}`;
      }
      wsOtp.start({
        channel,
        destinationMasked: data.destination_masked || root.dataset.userEmail || "",
        expiresSeconds: data.expires_seconds || (data.expires_minutes ? data.expires_minutes * 60 : 600),
        resendAfterSeconds: data.resend_after_seconds || 60,
        fallbackEmailAvailable: Boolean(isEl && data.fallback_email_available),
        fallbackEmailMasked: data.fallback_email_masked || "",
        resent: isResend,
      });
      feedback(status);
      feedback(dialogStatus, isResend ? "Novo código enviado! O código anterior foi invalidado." : "");
      confirmForm.reset();
      setCeremonyStep(2);
      confirmForm.elements.code.focus();
    } catch (error) {
      feedback(targetFeedback, error.message, true);
    } finally {
      busy = false;
      syncAuthorization();
    }
  }

  const wsOtp = createOtpController(root, {
    onResend: (ch) => dispatchWorkspaceAuthorize(ch, true),
    onFallbackEmail: () => dispatchWorkspaceAuthorize("email", true),
  });

  // Step 1 -> Step 2 (request authorization code — Electronic SiSFarm OR ICP-Brasil A1)
  authorize?.addEventListener("click", () => dispatchWorkspaceAuthorize(null, false));

  find("[data-ds-back-step1]")?.addEventListener("click", () => {
    wsOtp.stop();
    feedback(dialogStatus);
    setCeremonyStep(1);
  });

  // Step 2 -> Sign with 8-digit OTP
  confirmForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (wsOtp.isExpired()) {
      feedback(dialogStatus, "Este código expirou. Clique em \"Reenviar código\" para receber um novo.", true);
      return;
    }
    const isEl = activeMode === "electronic";
    const button = confirmForm.querySelector("[type=submit]");
    button.disabled = true;
    feedback(dialogStatus, isEl ? "Registrando assinatura eletrônica e evidências…" : "Aplicando assinatura digital e validando o PDF…");
    try {
      const url = isEl ? `${endpoint}/eletronica/assinar` : `${endpoint}/assinar`;
      const result = await request(root, url, {
        json: {consent: true, challenge_id: challengeId, code: confirmForm.elements.code.value.trim()},
      });
      const dlLink = find("[data-ds-signed-download]");
      if (dlLink && result.revision) {
        dlLink.href = `${endpoint}/revisoes/${result.revision}/pdf`;
      }
      const verLink = find("[data-ds-signed-verify]");
      const verCodeEl = find("[data-ds-signed-code]");
      if (result.verification_code) {
        if (verLink) verLink.href = `/validar/${result.verification_code}`;
        if (verCodeEl) verCodeEl.textContent = result.verification_code;
      }
      const successMsg = find("[data-ds-success-msg]");
      if (successMsg && result.sealed) {
        successMsg.textContent = "Todas as assinaturas foram concluídas! O Manifesto de Assinaturas e o selo criptográfico de integridade foram aplicados ao PDF.";
      }
      setCeremonyStep(3);
    } catch (error) {
      feedback(dialogStatus, error.message, true);
    } finally {
      button.disabled = false;
    }
  });

  find("[data-ds-reload-page]")?.addEventListener("click", () => location.reload());

  // Validation report & revalidation
  function drawReport(data) {
    reportInto(report, data);
    const count = (data.signatures || []).length;
    find("[data-ds-overview]").textContent = `${count} ${count === 1 ? "assinatura registrada" : "assinaturas registradas"} neste PDF`;
  }
  drawReport(JSON.parse(find("[data-ds-report]").textContent));

  const runRevalidate = async (button) => {
    const origText = button.textContent;
    button.disabled = true;
    button.textContent = "Verificando…";
    try {
      drawReport(await request(root, endpoint + "/validar"));
    } catch (error) {
      report.append(element("p", error.message, "ds-feedback is-error"));
    } finally {
      button.disabled = false;
      button.textContent = origText;
    }
  };
  for (const btn of root.querySelectorAll("[data-ds-revalidate], [data-ds-revalidate-secondary]")) {
    btn.addEventListener("click", () => runRevalidate(btn));
  }

  // ===== Aba "Assinaturas e histórico" =====
  const ddFind = (selector) => root.querySelector(selector) || document.querySelector(selector);
  for (const btn of document.querySelectorAll("[data-dd-go-sign]")) {
    btn.addEventListener("click", () => { switchWsTab("assinar"); window.scrollTo({top: 0, behavior: "smooth"}); });
  }

  // Comprovante de cada pessoa
  for (const btn of document.querySelectorAll("[data-dd-toggle-proof]")) {
    btn.addEventListener("click", () => {
      const panel = document.getElementById(btn.getAttribute("aria-controls"));
      const open = btn.getAttribute("aria-expanded") !== "true";
      btn.setAttribute("aria-expanded", open ? "true" : "false");
      btn.classList.toggle("is-open", open);
      btn.closest("[data-dd-person]")?.classList.toggle("is-expanded", open);
      if (panel) panel.hidden = !open;
    });
  }

  // Menus de três pontinhos: fecham ao clicar fora e com Esc
  const closeMenus = (except) => {
    for (const wrap of root.querySelectorAll(".dd-menu-wrap")) {
      if (wrap === except) continue;
      const menu = wrap.querySelector(".dd-menu");
      const trigger = wrap.querySelector("[data-dd-menu-btn]");
      if (menu && !menu.hidden) {
        menu.hidden = true;
        trigger?.setAttribute("aria-expanded", "false");
      }
    }
  };
  for (const trigger of document.querySelectorAll("[data-dd-menu-btn]")) {
    trigger.addEventListener("click", (event) => {
      event.stopPropagation();
      const wrap = trigger.closest(".dd-menu-wrap");
      const menu = wrap.querySelector(".dd-menu");
      closeMenus(wrap);
      const open = menu.hidden;
      menu.hidden = !open;
      trigger.setAttribute("aria-expanded", open ? "true" : "false");
      if (open) menu.querySelector("[role=menuitem]")?.focus();
    });
  }
  document.addEventListener("click", (event) => {
    if (!event.target.closest(".dd-menu-wrap")) closeMenus();
  });
  root.addEventListener("click", (event) => {
    if (event.target.closest(".dd-menu [role=menuitem]")) closeMenus();
  });

  // Camadas flutuantes vão para o <body> para não ficarem presas no layout da página
  for (const layer of document.querySelectorAll("[data-dd-drawer-backdrop], [data-dd-drawer], [data-dd-channel-backdrop], [data-dd-channel-modal], [data-dd-toast]")) {
    layer.classList.add("dd-layer");
    document.body.append(layer);
  }

  // Aviso rápido (toast)
  const toast = ddFind("[data-dd-toast]");
  const toastText = ddFind("[data-dd-toast-text]");
  const toastLink = ddFind("[data-dd-toast-link]");
  let toastTimer = null;
  const showToast = (text, {error = false, waUrl = ""} = {}) => {
    if (!toast) return;
    toastText.textContent = text;
    toast.classList.toggle("is-error", error);
    if (toastLink) {
      toastLink.hidden = !waUrl;
      toastLink.href = waUrl || "#";
    }
    toast.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { toast.hidden = true; }, waUrl || error ? 12000 : 5000);
  };
  ddFind("[data-dd-toast-close]")?.addEventListener("click", () => { toast.hidden = true; });

  const reloadDetails = () => {
    const url = new URL(location.href);
    url.searchParams.set("aba", "detalhes");
    url.searchParams.delete("fluxo");
    location.href = url.toString();
  };

  // Lembrar quem ainda não assinou
  for (const btn of document.querySelectorAll("[data-dd-remind]")) {
    btn.addEventListener("click", async () => {
      const name = btn.dataset.ddName || "a pessoa";
      const isExternal = btn.dataset.ddRemind === "external";
      const path = isExternal
        ? `${endpoint}/externos/${btn.dataset.ddId}/reenviar`
        : `${endpoint}/participantes/${btn.dataset.ddId}/lembrar`;
      btn.disabled = true;
      const orig = btn.textContent;
      btn.textContent = "Enviando…";
      try {
        const res = await request(root, path, {method: "POST"});
        if (res.notification_sent) {
          showToast(`Lembrete enviado para ${name}.`);
          btn.textContent = "Lembrado ✓";
          return;
        }
        showToast(res.notification_error || `Não conseguimos enviar o lembrete para ${name}.`, {error: true, waUrl: res.whatsapp_share_url || ""});
      } catch (error) {
        showToast(error.message, {error: true});
      } finally {
        if (btn.textContent === "Enviando…") btn.textContent = orig;
        btn.disabled = false;
      }
    });
  }

  // Copiar link de assinatura (gera link novo sem reenviar mensagem)
  for (const btn of document.querySelectorAll("[data-dd-copy-link]")) {
    btn.addEventListener("click", async () => {
      try {
        const res = await request(root, `${endpoint}/externos/${btn.dataset.ddCopyLink}/reenviar?somente_link=1`, {method: "POST"});
        try {
          await navigator.clipboard.writeText(res.sign_url);
          showToast("Link copiado. O link enviado antes deixou de funcionar.");
        } catch {
          window.prompt("Copie o link de assinatura:", res.sign_url);
        }
      } catch (error) {
        showToast(error.message, {error: true});
      }
    });
  }

  // Cancelar convite / tirar pessoa da equipe
  for (const btn of document.querySelectorAll("[data-dd-cancel-external]")) {
    btn.addEventListener("click", async () => {
      if (!window.confirm(`Cancelar o convite de ${btn.dataset.ddName}? O link dela deixa de funcionar.`)) return;
      try {
        await request(root, `${endpoint}/externos/${btn.dataset.ddCancelExternal}`, {method: "DELETE"});
        reloadDetails();
      } catch (error) {
        showToast(error.message, {error: true});
      }
    });
  }
  for (const btn of document.querySelectorAll("[data-dd-remove-team]")) {
    btn.addEventListener("click", async () => {
      if (!window.confirm(`Tirar ${btn.dataset.ddName} do documento? A pessoa perde o acesso ao PDF.`)) return;
      try {
        await request(root, `${endpoint}/participantes/${btn.dataset.ddRemoveTeam}`, {method: "DELETE"});
        reloadDetails();
      } catch (error) {
        showToast(error.message, {error: true});
      }
    });
  }

  // Trocar canal do convite (WhatsApp ↔ e-mail)
  const chModal = ddFind("[data-dd-channel-modal]");
  const chBackdrop = ddFind("[data-dd-channel-backdrop]");
  const chInput = ddFind("[data-dd-channel-input]");
  const chStatus = ddFind("[data-dd-channel-status]");
  const chConfirm = ddFind("[data-dd-channel-confirm]");
  let chState = null;
  const closeChannel = () => {
    if (!chModal) return;
    chModal.hidden = true;
    chBackdrop.hidden = true;
    chState?.trigger?.focus();
    chState = null;
  };
  for (const btn of document.querySelectorAll("[data-dd-change-channel]")) {
    btn.addEventListener("click", () => {
      const to = btn.dataset.ddTo;
      chState = {id: btn.dataset.ddChangeChannel, to, trigger: btn.closest(".dd-menu-wrap")?.querySelector("[data-dd-menu-btn]")};
      ddFind("[data-dd-channel-title]").textContent = to === "email" ? "Trocar para e-mail" : "Trocar para WhatsApp";
      ddFind("[data-dd-channel-note]").textContent = `${btn.dataset.ddName} vai receber um convite novo por ${to === "email" ? "e-mail" : "WhatsApp"}. O link enviado antes deixa de funcionar.`;
      ddFind("[data-dd-channel-label]").textContent = to === "email" ? "E-mail" : "WhatsApp com DDD";
      chInput.type = to === "email" ? "email" : "tel";
      chInput.placeholder = to === "email" ? "pessoa@exemplo.com.br" : "(35) 99999-0000";
      chInput.value = to === "email" ? (btn.dataset.ddEmail || "") : maskPhone(btn.dataset.ddPhone || "");
      chInput.oninput = to === "email" ? null : () => { chInput.value = maskPhone(chInput.value); };
      feedback(chStatus);
      chModal.hidden = false;
      chBackdrop.hidden = false;
      chInput.focus();
    });
  }
  ddFind("[data-dd-channel-cancel]")?.addEventListener("click", closeChannel);
  chBackdrop?.addEventListener("click", closeChannel);
  chConfirm?.addEventListener("click", async () => {
    if (!chState) return;
    const value = chInput.value.trim();
    if (!value) {
      feedback(chStatus, chState.to === "email" ? "Informe o e-mail." : "Informe o WhatsApp com DDD.", true);
      chInput.focus();
      return;
    }
    chConfirm.disabled = true;
    try {
      const res = await request(root, `${endpoint}/externos/${chState.id}/canal`, {
        json: {channel: chState.to, email: chState.to === "email" ? value : "", phone: chState.to === "whatsapp" ? value : ""},
      });
      if (!res.notification_sent) {
        feedback(chStatus, res.notification_error || "Canal trocado, mas o envio falhou. Copie o link pelo menu da pessoa.", true);
        setTimeout(reloadDetails, 2500);
        return;
      }
      reloadDetails();
    } catch (error) {
      feedback(chStatus, error.message, true);
    } finally {
      chConfirm.disabled = false;
    }
  });

  // Gaveta "Adicionar pessoa"
  const drawer = ddFind("[data-dd-drawer]");
  const drawerBackdrop = ddFind("[data-dd-drawer-backdrop]");
  const submitBtn = ddFind("[data-dd-submit]");
  const teamForm = ddFind("[data-dd-team-form]");
  const extForm = ddFind("[data-dd-ext-form]");
  const donePanel = ddFind("[data-dd-done]");
  let addTab = "team";
  let drawerTrigger = null;
  let extAppearance = {visible: true, style: "full"};
  let extRole = "Comprador";
  let extChannel = "whatsapp";
  let drawerChanged = false;

  const openDrawer = (trigger) => {
    if (!drawer) return;
    drawerTrigger = trigger || null;
    drawer.hidden = false;
    drawerBackdrop.hidden = false;
    document.body.classList.add("dd-no-scroll");
    requestAnimationFrame(() => drawer.classList.add("is-open"));
    (drawer.querySelector("[data-dd-add-panel]:not([hidden]) input") || drawer.querySelector("[data-dd-close-drawer]"))?.focus();
  };
  const closeDrawer = () => {
    if (!drawer || drawer.hidden) return;
    if (drawerChanged) { reloadDetails(); return; }
    drawer.classList.remove("is-open");
    drawer.hidden = true;
    drawerBackdrop.hidden = true;
    document.body.classList.remove("dd-no-scroll");
    drawerTrigger?.focus();
  };
  for (const btn of document.querySelectorAll("[data-dd-open-add]")) btn.addEventListener("click", () => openDrawer(btn));
  for (const btn of document.querySelectorAll("[data-dd-close-drawer]")) btn.addEventListener("click", closeDrawer);
  drawerBackdrop?.addEventListener("click", closeDrawer);
  ddFind("[data-dd-finish]")?.addEventListener("click", reloadDetails);
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (chModal && !chModal.hidden) { closeChannel(); return; }
    if (root.querySelector(".dd-menu:not([hidden])")) { closeMenus(); return; }
    if (drawer && !drawer.hidden && !find("[data-ds-placer-dialog]")?.open) closeDrawer();
  });

  const teamChecks = [...document.querySelectorAll("[data-dd-team-check]")];
  const selectedTeam = () => teamChecks.filter((c) => c.checked);
  const names = (list) => list.length <= 1 ? list.join("") : `${list.slice(0, -1).join(", ")} e ${list[list.length - 1]}`;

  function extValues() {
    if (!extForm) return null;
    const roleOther = (extForm.elements.role_other?.value || "").trim();
    return {
      name: extForm.elements.name.value.trim(),
      cpf: maskCpf(extForm.elements.cpf.value || ""),
      role_label: (extRole || roleOther || "Signatário").slice(0, 60),
      channel: extChannel,
      phone: extChannel === "whatsapp" ? extForm.elements.phone.value.trim() : "",
      email: extChannel === "email" ? extForm.elements.email.value.trim() : "",
    };
  }
  function extProblem(v) {
    if (v.name.length < 2) return "Informe o nome completo.";
    if (!isValidCpf(v.cpf)) return "Informe um CPF válido.";
    if (!extRole && !(extForm.elements.role_other.value || "").trim()) return "Escreva qual é o papel da pessoa.";
    if (v.channel === "whatsapp" && v.phone.replace(/\D/g, "").length < 10) return "Informe o WhatsApp com DDD.";
    if (v.channel === "email" && !/^\S+@\S+\.\S+$/.test(v.email)) return "Informe um e-mail válido.";
    return "";
  }

  function syncDrawer() {
    if (!drawer) return;
    const total = document.querySelectorAll("[data-dd-person]").length;
    if (addTab === "team") {
      const picked = selectedTeam();
      const summary = ddFind("[data-dd-team-summary]");
      if (summary) {
        summary.hidden = picked.length === 0;
        ddFind("[data-dd-team-summary-text]").textContent = picked.length
          ? `${names(picked.map((c) => c.dataset.ddName))} ${picked.length === 1 ? "vai" : "vão"} poder ver este PDF e todas as versões, e ${picked.length === 1 ? "recebe" : "recebem"} um aviso para assinar. O documento passa a ter ${total + picked.length} pessoas para assinar.`
          : "";
      }
      submitBtn.disabled = picked.length === 0;
      submitBtn.textContent = picked.length ? `Adicionar ${picked.length} ${picked.length === 1 ? "pessoa" : "pessoas"}` : "Adicionar";
      return;
    }
    const v = extValues();
    const problem = extProblem(v);
    const summary = ddFind("[data-dd-ext-summary]");
    const list = ddFind("[data-dd-ext-summary-list]");
    summary.hidden = Boolean(problem);
    list.replaceChildren();
    if (!problem) {
      const dest = v.channel === "whatsapp" ? `no WhatsApp ${v.phone}` : `no e-mail ${v.email}`;
      list.append(
        element("li", `${v.name} (${v.role_label.toLowerCase()}) vai receber o convite ${dest}.`),
        element("li", `O documento passa a ter ${total + 1} pessoas para assinar.`),
      );
    }
    submitBtn.disabled = Boolean(problem);
    submitBtn.textContent = "Enviar convite";
  }

  for (const tab of document.querySelectorAll("[data-dd-add-tab]")) {
    tab.addEventListener("click", () => {
      addTab = tab.dataset.ddAddTab;
      for (const t of document.querySelectorAll("[data-dd-add-tab]")) t.setAttribute("aria-selected", t === tab ? "true" : "false");
      for (const panel of document.querySelectorAll("[data-dd-add-panel]")) panel.hidden = panel.dataset.ddAddPanel !== addTab;
      syncDrawer();
    });
  }
  for (const c of teamChecks) c.addEventListener("change", syncDrawer);
  ddFind("[data-dd-team-search]")?.addEventListener("input", (event) => {
    const q = event.target.value.trim().toLowerCase();
    let shown = 0;
    for (const item of document.querySelectorAll("[data-dd-team-item]")) {
      const match = !q || item.dataset.ddSearchText.includes(q);
      item.hidden = !match;
      if (match) shown += 1;
    }
    const empty = ddFind("[data-dd-team-empty]");
    if (empty) empty.hidden = shown > 0;
  });

  for (const chip of document.querySelectorAll("[data-dd-role]")) {
    chip.addEventListener("click", () => {
      extRole = chip.dataset.ddRole;
      for (const c of document.querySelectorAll("[data-dd-role]")) {
        const on = c === chip;
        c.classList.toggle("is-on", on);
        c.setAttribute("aria-pressed", on ? "true" : "false");
      }
      const other = ddFind("[data-dd-role-other]");
      other.hidden = Boolean(extRole);
      if (!extRole) other.querySelector("input").focus();
      syncDrawer();
    });
  }
  for (const btn of document.querySelectorAll("[data-dd-channel]")) {
    btn.addEventListener("click", () => {
      extChannel = btn.dataset.ddChannel;
      for (const b of document.querySelectorAll("[data-dd-channel]")) b.setAttribute("aria-pressed", b === btn ? "true" : "false");
      ddFind("[data-dd-phone-wrap]").hidden = extChannel !== "whatsapp";
      ddFind("[data-dd-email-wrap]").hidden = extChannel !== "email";
      syncDrawer();
    });
  }
  extForm?.addEventListener("input", syncDrawer);

  const wsPlacer = createStampPlacer(root.querySelector("[data-ds-placer-dialog]"));
  ddFind("[data-dd-open-placer]")?.addEventListener("click", () => {
    wsPlacer.open({
      pdfUrl: root.dataset.pdfUrl,
      signerName: (extForm?.elements.name?.value || "").trim() || "Signatário externo",
      cpf: (extForm?.elements.cpf?.value || "").trim(),
      appearance: extAppearance,
      onSave: (res) => {
        extAppearance = res;
        ddFind("[data-dd-stamp-summary]").textContent = describeStampConfig(extAppearance);
      },
    });
  });

  function showDone({title, note, link = "", waUrl = ""}) {
    drawerChanged = true;
    for (const panel of document.querySelectorAll("[data-dd-add-panel], .dd-seg[role=tablist]")) panel.hidden = true;
    donePanel.hidden = false;
    ddFind("[data-dd-done-title]").textContent = title;
    ddFind("[data-dd-done-note]").textContent = note;
    ddFind("[data-dd-done-link-row]").hidden = !link;
    ddFind("[data-dd-done-link]").value = link;
    const wa = ddFind("[data-dd-done-wa]");
    wa.hidden = !waUrl;
    wa.href = waUrl || "#";
    ddFind("[data-dd-drawer-foot]").hidden = true;
    ddFind("[data-dd-done-foot]").hidden = false;
    ddFind("[data-dd-finish]").focus();
  }
  ddFind("[data-dd-copy-done-link]")?.addEventListener("click", async () => {
    const input = ddFind("[data-dd-done-link]");
    try { await navigator.clipboard.writeText(input.value); showToast("Link copiado."); } catch { input.select(); }
  });

  submitBtn?.addEventListener("click", async () => {
    submitBtn.disabled = true;
    if (addTab === "team") {
      const status = ddFind("[data-dd-team-status]");
      const picked = selectedTeam();
      const failed = [];
      feedback(status, "Adicionando…");
      for (const c of picked) {
        try {
          const res = await request(root, endpoint + "/participantes", {json: {email: c.value, consent: true}});
          drawerChanged = true;
          if (!res.notification_sent) failed.push(c.dataset.ddName);
        } catch (error) {
          feedback(status, `${c.dataset.ddName}: ${error.message}`, true);
          submitBtn.disabled = false;
          return;
        }
      }
      feedback(status);
      showDone({
        title: picked.length === 1 ? `${picked[0].dataset.ddName} foi adicionado(a)` : `${picked.length} pessoas adicionadas`,
        note: failed.length
          ? `Não conseguimos avisar ${names(failed)}. Use “Lembrar” na lista para tentar de novo.`
          : "Cada pessoa recebeu um aviso para assinar.",
      });
      return;
    }
    const status = ddFind("[data-dd-ext-status]");
    const v = extValues();
    const problem = extProblem(v);
    if (problem) { feedback(status, problem, true); submitBtn.disabled = false; return; }
    feedback(status, "Enviando convite…");
    try {
      const res = await request(root, endpoint + "/externos", {json: {...v, appearance: {...extAppearance}}});
      feedback(status);
      showDone({
        title: res.notification_sent ? `Convite enviado para ${res.signer_name}` : `Link pronto para ${res.signer_name}`,
        note: res.notification_sent
          ? "Se quiser, copie o link e mande também por outro meio."
          : (res.notification_error || "Não conseguimos enviar automaticamente. Copie o link ou envie pelo seu WhatsApp."),
        link: res.sign_url || "",
        waUrl: res.whatsapp_share_url || "",
      });
    } catch (error) {
      feedback(status, error.message, true);
      submitBtn.disabled = false;
    }
  });
  syncDrawer();

  const ceremonyGrid = find("[data-ds-ceremony-grid]");
  const viewerSection = find("[data-ds-viewer-section]");
  const mobileViewerSlot = find("[data-ds-mobile-viewer-slot]");
  const toggleViewerBtns = [...root.querySelectorAll("[data-ds-toggle-viewer]")];
  const isAlreadySigned = ceremonyGrid?.dataset.dsAlreadySigned === "yes";
  const mobileMq = window.matchMedia("(max-width: 1023px)");
  let pdfStarted = false;
  let mobileViewerExpanded = false;

  const ensurePdfLoaded = () => {
    if (!pdfStarted) {
      pdfStarted = true;
      loadPdf();
    } else if (pdf) {
      requestAnimationFrame(() => render());
    }
  };

  function syncMobileViewer() {
    if (!viewerSection || !ceremonyGrid) return;
    const isMobile = mobileMq.matches;
    if (isMobile && !isAlreadySigned && mobileViewerSlot) {
      mobileViewerSlot.hidden = false;
      if (viewerSection.parentElement !== mobileViewerSlot) {
        mobileViewerSlot.append(viewerSection);
      }
      viewerSection.classList.add("is-mobile-embedded");
      viewerSection.classList.remove("is-mobile-collapsed");
      ensurePdfLoaded();
    } else if (isMobile && isAlreadySigned) {
      if (mobileViewerSlot) mobileViewerSlot.hidden = true;
      if (viewerSection.parentElement !== ceremonyGrid) {
        ceremonyGrid.prepend(viewerSection);
      }
      viewerSection.classList.remove("is-mobile-embedded");
      viewerSection.classList.toggle("is-mobile-collapsed", !mobileViewerExpanded);
      if (mobileViewerExpanded) ensurePdfLoaded();
    } else {
      if (mobileViewerSlot) mobileViewerSlot.hidden = true;
      if (viewerSection.parentElement !== ceremonyGrid) {
        ceremonyGrid.prepend(viewerSection);
      }
      viewerSection.classList.remove("is-mobile-embedded", "is-mobile-collapsed");
      ensurePdfLoaded();
    }
  }

  for (const btn of toggleViewerBtns) {
    btn.addEventListener("click", () => {
      mobileViewerExpanded = !mobileViewerExpanded;
      syncMobileViewer();
      for (const b of toggleViewerBtns) {
        b.setAttribute("aria-expanded", String(mobileViewerExpanded));
        const label = mobileViewerExpanded ? "Ocultar PDF" : "Visualizar PDF";
        const svg = b.querySelector("svg");
        if (svg) {
          b.replaceChildren(svg, document.createTextNode(` ${label}`));
        } else {
          b.textContent = label;
        }
      }
      if (mobileViewerExpanded && viewerSection) {
        viewerSection.scrollIntoView({behavior: "smooth", block: "start"});
      }
    });
  }

  mobileMq.addEventListener?.("change", syncMobileViewer);

  syncAuthorization();
  syncMobileViewer();
}

function initCertificates(root) {
  const currentUserId = root.dataset.currentUserId || "";
  const regDialog = root.querySelector("[data-ds-register-dialog]");
  const regForm = root.querySelector("[data-ds-register]");
  const regDrop = root.querySelector("[data-ds-reg-drop]");
  const regFileLabel = root.querySelector("[data-ds-reg-file-label]");
  const regFileSub = root.querySelector("[data-ds-reg-file-sub]");
  const regStatus = root.querySelector("[data-ds-status]");
  const regHolder = root.querySelector("[data-ds-reg-holder]");
  const regEmail = root.querySelector("[data-ds-reg-email]");
  const regWhatsapp = root.querySelector("[data-ds-reg-whatsapp]");
  const regWhatsappHint = root.querySelector("[data-ds-reg-whatsapp-hint]");
  const regEnableWrap = root.querySelector("[data-ds-reg-enable-wrap]");
  const regEnable = root.querySelector("[data-ds-reg-enable]");
  const regOtherNote = root.querySelector("[data-ds-reg-other-note]");
  const regCpf = root.querySelector("[data-ds-reg-cpf]");
  let selectedPfxFile = null;

  // Generic close buttons on all dialogs
  for (const closeBtn of root.querySelectorAll("[data-ds-close-dialog]")) {
    closeBtn.addEventListener("click", () => closeBtn.closest("dialog")?.close());
  }

  function syncRegFields() {
    if (!regHolder) return;
    const opt = regHolder.selectedOptions[0];
    const isSelf = regHolder.value === currentUserId;
    if (regEmail) regEmail.value = opt?.dataset.email || "";
    const profilePhone = opt?.dataset.phone || "";
    if (regWhatsapp) {
      if (profilePhone) {
        regWhatsapp.value = maskPhone(profilePhone);
        regWhatsapp.readOnly = true;
        regWhatsapp.classList.add("ds-input-readonly");
        if (regWhatsappHint) regWhatsappHint.hidden = true;
      } else {
        regWhatsapp.value = "";
        regWhatsapp.readOnly = false;
        regWhatsapp.classList.remove("ds-input-readonly");
        if (regWhatsappHint) regWhatsappHint.hidden = false;
      }
    }
    if (regEnableWrap) regEnableWrap.hidden = !isSelf;
    if (!isSelf && regEnable) regEnable.checked = false;
    if (regOtherNote) regOtherNote.hidden = isSelf;
  }

  const pickPfx = (file) => {
    if (!file) return;
    selectedPfxFile = file;
    if (regFileLabel) regFileLabel.textContent = file.name;
    if (regFileSub) regFileSub.textContent = `${Math.max(1, Math.round(file.size / 1024))} KB · Pronto para validar`;
  };

  for (const btn of root.querySelectorAll("[data-ds-open-register]")) {
    btn.addEventListener("click", () => {
      regForm?.reset();
      selectedPfxFile = null;
      if (regFileLabel) regFileLabel.textContent = "Arraste o arquivo .pfx ou .p12 aqui";
      if (regFileSub) regFileSub.textContent = "ou clique para escolher do computador ou celular (até 200 KB)";
      feedback(regStatus);
      syncRegFields();
      regDialog?.showModal();
    });
  }

  regHolder?.addEventListener("change", syncRegFields);
  regForm?.elements.file?.addEventListener("change", () => pickPfx(regForm.elements.file.files?.[0]));
  if (regDrop && regForm?.elements.file) {
    bindAnimatedDropzone(regDrop, {
      dragTitle: "Solte o arquivo A1 aqui para carregar",
      dragSub: "O certificado .pfx ou .p12 será preparado automaticamente",
      onFile: (file) => {
        const dt = new DataTransfer();
        dt.items.add(file);
        regForm.elements.file.files = dt.files;
        pickPfx(file);
      },
    });
  }

  regForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = selectedPfxFile || regForm.elements.file.files?.[0];
    if (!file || file.size > 200_000) {
      feedback(regStatus, "Selecione um certificado A1 (.pfx ou .p12) de até 200 KB.", true);
      return;
    }
    const cpfVal = (regCpf?.value || "").trim();
    if (cpfVal && !isValidCpf(cpfVal)) {
      feedback(regStatus, "CPF inválido. Confira os 11 dígitos digitados.", true);
      regCpf?.focus();
      return;
    }
    if (regEnable?.checked && !regEnableWrap.hidden && !isValidCpf(cpfVal)) {
      feedback(regStatus, "Informe um CPF válido do titular para liberar a assinatura.", true);
      regCpf?.focus();
      return;
    }
    const button = regForm.querySelector('button[type="submit"]');
    button.disabled = true;
    feedback(regStatus, "Validando e salvando certificado no cofre…");
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      let binary = "";
      for (const byte of bytes) binary += String.fromCharCode(byte);
      const payload = {
        file: btoa(binary),
        password: regForm.elements.password.value,
      };
      if (regHolder?.value) payload.holder_user_id = Number(regHolder.value);
      if (cpfVal) payload.cpf = cpfVal;
      if (regWhatsapp && !regWhatsapp.readOnly && regWhatsapp.value.trim()) {
        payload.whatsapp = regWhatsapp.value.trim();
      }
      if (regEnable?.checked && !regEnableWrap.hidden) {
        payload.enable_documents = true;
      }
      await request(root, "/certificados-digitais", {json: payload});
      location.reload();
    } catch (error) {
      feedback(regStatus, error.message, true);
    } finally {
      regForm.elements.password.value = "";
      button.disabled = false;
    }
  });

  // Bind responsible modal (only for legacy unbound certificates)
  const bindDialog = root.querySelector("[data-ds-binding-dialog]");
  const bindForm = root.querySelector("[data-ds-binding-form]");
  const bindSelect = root.querySelector("[data-ds-binding-select]");
  const bindName = root.querySelector("[data-ds-binding-name]");
  const bindSub = root.querySelector("[data-ds-binding-sub]");
  const bindStatus = root.querySelector("[data-ds-binding-status]");
  let activeBindCertId = null;

  for (const btn of root.querySelectorAll("[data-ds-open-binding]")) {
    btn.addEventListener("click", () => {
      activeBindCertId = btn.dataset.dsOpenBinding;
      if (bindName) bindName.textContent = btn.dataset.certName || "";
      if (bindSub) bindSub.textContent = btn.dataset.certSub || "";
      feedback(bindStatus);
      bindDialog?.showModal();
    });
  }

  bindForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!activeBindCertId || !bindSelect || bindSelect.disabled) return;
    const userId = bindSelect.value;
    if (!userId) return;
    const button = bindForm.querySelector('button[type="submit"]');
    button.disabled = true;
    feedback(bindStatus, "Vinculando responsável…");
    try {
      await request(root, `/certificados-digitais/${activeBindCertId}/vinculo`, {json: {user_id: Number(userId)}});
      location.reload();
    } catch (error) {
      feedback(bindStatus, error.message, true);
    } finally {
      button.disabled = false;
    }
  });

  // Authorize / Deactivate purpose modal (button enabled ONLY when consent checkbox is checked!)
  const purposeDialog = root.querySelector("[data-ds-purpose-dialog]");
  const purposeForm = root.querySelector("[data-ds-purpose-form]");
  const purposeStatus = root.querySelector("[data-ds-purpose-status]");
  const purposeConsentChk = root.querySelector("[data-ds-purpose-consent-chk]");
  const purposeSubmit = root.querySelector("[data-ds-purpose-submit]");
  let selection = null;

  function syncPurposeSubmit() {
    if (!purposeSubmit || !purposeConsentChk) return;
    purposeSubmit.disabled = !purposeConsentChk.checked;
  }
  purposeConsentChk?.addEventListener("change", syncPurposeSubmit);

  for (const control of root.querySelectorAll("[data-ds-purpose]")) {
    control.addEventListener("click", () => {
      selection = control.dataset;
      const enable = selection.enable === "yes";
      const purpose = selection.dsPurpose === "fiscal" ? "emissão de Nota Fiscal" : "assinatura de documentos";
      const environment = selection.dsPurpose === "fiscal" ? ` em ${selection.env === "producao" ? "produção" : "homologação"}` : "";
      purposeDialog.querySelector("[data-ds-purpose-title]").textContent = enable ? `Liberar ${purpose}` : `Desativar ${purpose}`;
      purposeDialog.querySelector("[data-ds-purpose-subtitle]").textContent = enable
        ? "Autorize o uso deste certificado A1"
        : "Confirme a suspensão temporária deste uso";
      purposeDialog.querySelector("[data-ds-purpose-holder]").textContent = selection.holder || "";
      purposeDialog.querySelector("[data-ds-purpose-cert-sub]").textContent = selection.certSub || "";
      purposeDialog.querySelector("[data-ds-purpose-disable-note]").hidden = enable;
      purposeDialog.querySelector("[data-ds-purpose-password]").hidden = !enable;
      purposeForm.elements.password.required = enable;
      const confirmCpf = enable && selection.cpf === "yes";
      purposeDialog.querySelector("[data-ds-purpose-cpf]").hidden = !confirmCpf;
      purposeForm.elements.cpf.disabled = !confirmCpf;
      purposeForm.elements.cpf.required = confirmCpf;
      purposeDialog.querySelector("[data-ds-purpose-consent]").textContent = enable
        ? `Autorizo expressamente o uso deste certificado para ${purpose}${environment}.`
        : `Confirmo a desativação do uso para ${purpose}.`;
      purposeSubmit.textContent = enable ? "Liberar uso" : "Desativar uso";
      purposeSubmit.classList.toggle("ds-btn-danger", !enable);
      purposeSubmit.classList.toggle("sf-btn-primary", enable);
      purposeForm.reset();
      feedback(purposeStatus);
      syncPurposeSubmit();
      purposeDialog.showModal();
    });
  }
  purposeDialog?.addEventListener("close", () => { purposeForm.reset(); feedback(purposeStatus); syncPurposeSubmit(); });
  purposeForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!selection || !purposeConsentChk?.checked) return;
    if (!purposeForm.elements.cpf.disabled && !isValidCpf(purposeForm.elements.cpf.value)) {
      feedback(purposeStatus, "CPF inválido. Confira os 11 dígitos digitados.", true);
      return;
    }
    purposeSubmit.disabled = true;
    try {
      await request(root, `/certificados-digitais/${selection.cert}/finalidades`, {
        json: {
          purpose: selection.dsPurpose,
          enable: selection.enable === "yes",
          environment: selection.env,
          password: purposeForm.elements.password.value,
          cpf: purposeForm.elements.cpf.value,
          consent: purposeConsentChk.checked,
        },
      });
      location.reload();
    } catch (error) {
      feedback(purposeStatus, error.message, true);
    } finally {
      purposeForm.elements.password.value = "";
      purposeForm.elements.cpf.value = "";
      syncPurposeSubmit();
    }
  });

  // Delete certificate modal
  const delDialog = root.querySelector("[data-ds-delete-cert-dialog]");
  const delForm = root.querySelector("[data-ds-delete-cert-form]");
  const delSub = root.querySelector("[data-ds-delete-cert-sub]");
  const delFiscalWarn = root.querySelector("[data-ds-delete-fiscal-warn]");
  const delStatus = root.querySelector("[data-ds-delete-cert-status]");
  let deleteCertId = null;

  for (const btn of root.querySelectorAll("[data-ds-open-delete-cert]")) {
    btn.addEventListener("click", () => {
      deleteCertId = btn.dataset.dsOpenDeleteCert;
      if (delSub) delSub.textContent = `${btn.dataset.certName || ""} · ${btn.dataset.certSub || ""}`;
      if (delFiscalWarn) delFiscalWarn.hidden = btn.dataset.fiscalActive !== "yes";
      feedback(delStatus);
      delDialog?.showModal();
    });
  }
  delForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!deleteCertId) return;
    const button = delForm.querySelector('button[type="submit"]');
    button.disabled = true;
    feedback(delStatus, "Removendo certificado do cofre…");
    try {
      await request(root, `/certificados-digitais/${deleteCertId}`, {method: "DELETE"});
      location.reload();
    } catch (error) {
      feedback(delStatus, error.message, true);
      button.disabled = false;
    }
  });
}

function initExternalSigning(root) {
  const find = (selector) => root.querySelector(selector);
  const parseJson = (sel, fallback) => {
    const el = find(sel);
    if (!el) return fallback;
    try { return JSON.parse(el.textContent || ""); } catch { return fallback; }
  };
  const options = parseJson("[data-ds-options]", { pages: 1 });
  const presetAppearance = parseJson("[data-ds-preset-appearance]", { visible: false });
  const token = root.dataset.token;
  const lockedChannel = root.dataset.lockedChannel === "whatsapp" ? "whatsapp" : "email";
  const hasPresetCpf = root.dataset.hasPresetCpf === "yes";

  const view = find("[data-ds-scroll]");
  const previewStatus = find("[data-ds-preview-status]");
  const sheet = find("[data-ds-sheet]");
  const canvas = find("[data-ds-canvas]");
  const thumbnails = find("[data-ds-thumbs]");
  const pageInput = find("[data-ds-page]");
  const previous = find("[data-ds-prev]");
  const next = find("[data-ds-next]");
  const roStamp = find("[data-ds-ro-stamp]");
  const jumpStamp = find("[data-ds-jump-stamp]");
  const elCpfInput = find("[data-ds-el-cpf]");
  const consent = find("[data-ds-consent]");
  const authorize = find("[data-ds-ext-authorize]");
  const status = find("[data-ds-status]");
  const confirmForm = find("[data-ds-ext-confirm-form]");
  const dialogStatus = find("[data-ds-confirm-status]");

  const mobBar = find("[data-ds-ext-mob-bar]");
  const mobHint = find("[data-ds-ext-mob-hint]");
  const mobCta = find("[data-ds-ext-mob-cta]");
  const mobPill = document.querySelector("[data-ds-ext-mob-pill]");

  let pdf = null;
  let pageNumber = 1;
  let renderedPage = 0;
  let zoom = 1;
  let viewport = null;
  let drawTask = null;
  let drawVersion = 0;
  let busy = false;
  let challengeId = null;
  let currentStep = 1;

  const hasVisibleStamp = Boolean(presetAppearance && presetAppearance.visible);
  const stampTargetPage = typeof presetAppearance?.page === "number" ? presetAppearance.page + 1 : 1;

  function setStep(step) {
    currentStep = step;
    for (const pane of root.querySelectorAll("[data-ds-step-pane]")) {
      pane.hidden = Number(pane.dataset.dsStepPane) !== step;
    }
    for (const pill of root.querySelectorAll("[data-ds-step-pill]")) {
      const num = Number(pill.dataset.dsStepPill);
      pill.classList.toggle("is-done", num < step);
      if (num === step) pill.setAttribute("aria-current", "step");
      else pill.removeAttribute("aria-current");
    }
    const grid = find("[data-ds-ceremony-grid]");
    const viewer = find("[data-ds-viewer-section]");
    const isDone = step === 3;
    if (grid) grid.classList.toggle("is-completed", isDone);
    if (viewer) viewer.hidden = isDone;
    if (mobBar) mobBar.hidden = isDone;
    if (mobPill) mobPill.textContent = isDone ? "✓" : `${step}/2`;
    syncAuthorization();
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function syncAuthorization() {
    if (!consent || !authorize) return;
    const cpfOk = hasPresetCpf || isValidCpf(elCpfInput?.value || "");
    if (!cpfOk) consent.checked = false;
    consent.disabled = !cpfOk;
    const consentBox = find("[data-ds-consent-box]");
    if (consentBox) consentBox.classList.toggle("is-disabled", !cpfOk);

    const canSend = Boolean(consent.checked && cpfOk && !busy);
    authorize.disabled = !canSend;

    if (mobCta) {
      if (currentStep === 1) {
        mobCta.disabled = !canSend;
        mobCta.textContent = lockedChannel === "whatsapp" ? "Enviar código no WhatsApp →" : "Enviar código por e-mail →";
        if (mobHint) {
          mobHint.textContent = !cpfOk
            ? "Informe seu CPF acima"
            : (consent.checked ? "Pronto para enviar o código" : 'Marque "Li e concordo" acima');
        }
      } else if (currentStep === 2) {
        mobCta.disabled = busy;
        mobCta.textContent = "Concluir assinatura agora";
        if (mobHint) mobHint.textContent = "Digite o código de 8 dígitos acima";
      }
    }
  }

  function drawReadOnlyStamp() {
    if (!roStamp) return;
    if (!hasVisibleStamp || !viewport) {
      roStamp.hidden = true;
      if (jumpStamp) jumpStamp.hidden = true;
      return;
    }
    if (jumpStamp) jumpStamp.hidden = stampTargetPage === renderedPage;
    if (stampTargetPage !== renderedPage) {
      roStamp.hidden = true;
      return;
    }
    roStamp.hidden = false;
    if (Array.isArray(presetAppearance.box) && presetAppearance.box.length === 4) {
      const [x1, y1, x2, y2] = viewport.convertToViewportRectangle(presetAppearance.box);
      Object.assign(roStamp.style, {
        left: `${Math.min(x1, x2)}px`,
        top: `${Math.min(y1, y2)}px`,
        width: `${Math.abs(x2 - x1)}px`,
        height: `${Math.abs(y2 - y1)}px`,
      });
    } else {
      const isCompact = presetAppearance.style === "compact";
      const w = viewport.width * (isCompact ? 0.30 : 0.42);
      const h = Math.min(viewport.height * (isCompact ? 0.18 : 0.24), Math.max(viewport.height * 0.10, w * (433 / 1024)));
      Object.assign(roStamp.style, {
        left: `${viewport.width - w - viewport.width * 0.06}px`,
        top: `${viewport.height - h - viewport.height * 0.06}px`,
        width: `${w}px`,
        height: `${h}px`,
      });
    }
  }

  async function render() {
    if (!pdf) return;
    const version = ++drawVersion;
    if (drawTask) drawTask.cancel();
    try {
      const page = await pdf.getPage(pageNumber);
      if (version !== drawVersion) return;
      const raw = page.getViewport({ scale: 1 });
      const padding = parseFloat(getComputedStyle(view).paddingLeft) * 2;
      const availableWidth = Math.max(200, view.clientWidth - padding);
      const fitted = Math.min(availableWidth / raw.width, 1.32);
      const scale = Math.min(fitted * zoom, 2000 / Math.max(raw.width, raw.height));
      const nextViewport = page.getViewport({ scale: Math.max(0.02, scale) });
      const ratio = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(nextViewport.width * ratio);
      canvas.height = Math.round(nextViewport.height * ratio);
      canvas.style.width = `${nextViewport.width}px`;
      canvas.style.height = `${nextViewport.height}px`;
      drawTask = page.render({ canvas, viewport: nextViewport, transform: ratio === 1 ? null : [ratio, 0, 0, ratio, 0, 0] });
      await drawTask.promise;
      if (version !== drawVersion) return;
      viewport = nextViewport;
      renderedPage = pageNumber;
      if (previewStatus) previewStatus.hidden = true;
      if (sheet) sheet.hidden = false;
      if (pageInput) {
        pageInput.value = String(pageNumber);
        pageInput.max = String(pdf.numPages);
      }
      const pagesLabel = find("[data-ds-pages]");
      if (pagesLabel) pagesLabel.textContent = String(pdf.numPages);
      if (previous) previous.disabled = pageNumber <= 1;
      if (next) next.disabled = pageNumber >= pdf.numPages;
      thumbnails?.querySelector("[aria-current]")?.removeAttribute("aria-current");
      thumbnails?.querySelector(`[data-page="${pageNumber}"]`)?.setAttribute("aria-current", "page");
      drawReadOnlyStamp();
      syncAuthorization();
    } catch (error) {
      if (version !== drawVersion || error.name === "RenderingCancelledException") return;
      viewport = null;
      if (sheet) sheet.hidden = true;
      if (previewStatus) {
        previewStatus.hidden = false;
        previewStatus.textContent = "Não foi possível desenhar esta página do PDF.";
      }
    }
  }

  function changePage(value) {
    if (!pdf) return;
    pageNumber = Math.max(1, Math.min(pdf.numPages, value || 1));
    if (pageInput) pageInput.value = String(pageNumber);
    if (view) view.scrollTop = 0;
    viewport = null;
    render();
  }

  async function loadPdf() {
    try {
      const lib = await import(vendor + "legacy/build/pdf.min.mjs");
      lib.GlobalWorkerOptions.workerSrc = vendor + "legacy/build/pdf.worker.min.mjs";
      pdf = await lib.getDocument({
        url: root.dataset.pdfUrl, cMapUrl: vendor + "cmaps/", cMapPacked: true,
        standardFontDataUrl: vendor + "standard_fonts/", wasmUrl: vendor + "wasm/",
        isEvalSupported: false, enableXfa: false, maxImageSize: 24_000_000, stopAtErrors: true,
      }).promise;
      if (hasVisibleStamp && stampTargetPage >= 1 && stampTargetPage <= pdf.numPages) {
        pageNumber = stampTargetPage;
      }
      await render();
    } catch {
      if (previewStatus) {
        previewStatus.hidden = false;
        previewStatus.textContent = "A prévia não está disponível. Use Baixar PDF acima para ler o documento.";
      }
    }
  }

  window.addEventListener("resize", () => { if (pdf) render(); });
  previous?.addEventListener("click", () => changePage(pageNumber - 1));
  next?.addEventListener("click", () => changePage(pageNumber + 1));
  pageInput?.addEventListener("change", () => changePage(Number(pageInput.value)));
  for (const control of root.querySelectorAll("[data-ds-zoom]")) {
    control.addEventListener("click", () => {
      zoom = Math.max(0.65, Math.min(2.75, zoom + Number(control.dataset.dsZoom) * 0.2));
      render();
    });
  }
  find("[data-ds-fit]")?.addEventListener("click", () => { zoom = 1; render(); });
  jumpStamp?.addEventListener("click", () => {
    if (stampTargetPage !== renderedPage) changePage(stampTargetPage);
    else roStamp?.scrollIntoView({ behavior: "smooth", block: "center" });
  });

  elCpfInput?.addEventListener("input", syncAuthorization);
  consent?.addEventListener("change", syncAuthorization);

  const step1FallbackBox = find("[data-ds-step1-fallback-email]");
  const step1FallbackBtn = find("[data-ds-step1-fallback-btn]");
  const hasFallbackEmail = root.dataset.hasFallbackEmail === "yes";

  async function triggerAuthorize(overrideChannel = null, isResend = false) {
    if (busy || !consent?.checked) return;
    busy = true;
    syncAuthorization();
    const targetChannel = overrideChannel || lockedChannel;
    const targetFeedback = isResend ? dialogStatus : status;
    if (step1FallbackBox && !isResend) step1FallbackBox.hidden = true;
    feedback(targetFeedback, targetChannel === "whatsapp" ? "Enviando código por WhatsApp…" : "Enviando código por e-mail…");
    try {
      const payload = { consent: true };
      if (!hasPresetCpf && elCpfInput) payload.cpf = elCpfInput.value.trim();
      if (overrideChannel) payload.channel = overrideChannel;
      const data = await request(root, `/assinar/${token}/autorizar`, { json: payload });
      challengeId = data.challenge_id;
      const actualChannel = data.channel || targetChannel;
      const step2Title = find("[data-ds-step2-title]");
      const step2Sub = find("[data-ds-step2-sub]");
      if (step2Title) {
        step2Title.textContent = actualChannel === "whatsapp"
          ? "Digite o código enviado no WhatsApp"
          : "Digite o código enviado ao seu e-mail";
      }
      if (step2Sub) {
        step2Sub.textContent = `Enviamos um código de 8 dígitos para ${data.destination_masked || "seu contato"}.`;
      }
      extOtp.start({
        channel: actualChannel,
        destinationMasked: data.destination_masked || "",
        expiresSeconds: data.expires_seconds || (data.expires_minutes ? data.expires_minutes * 60 : 600),
        resendAfterSeconds: data.resend_after_seconds || 60,
        fallbackEmailAvailable: Boolean(data.fallback_email_available),
        fallbackEmailMasked: data.fallback_email_masked || root.dataset.emailMasked || "",
        resent: isResend,
      });
      feedback(status);
      feedback(dialogStatus, isResend ? "Novo código enviado! O código anterior foi invalidado." : "");
      confirmForm?.reset();
      setStep(2);
      confirmForm?.elements.code?.focus();
    } catch (error) {
      feedback(targetFeedback, error.message, true);
      if (!isResend && targetChannel === "whatsapp" && hasFallbackEmail && step1FallbackBox) {
        step1FallbackBox.hidden = false;
      }
    } finally {
      busy = false;
      syncAuthorization();
    }
  }

  const extOtp = createOtpController(root, {
    onResend: (ch) => triggerAuthorize(ch, true),
    onFallbackEmail: () => triggerAuthorize("email", true),
  });

  authorize?.addEventListener("click", () => triggerAuthorize(null, false));
  step1FallbackBtn?.addEventListener("click", () => triggerAuthorize("email", false));
  mobCta?.addEventListener("click", () => {
    if (currentStep === 1) triggerAuthorize(null, false);
    else if (currentStep === 2) confirmForm?.requestSubmit();
  });

  find("[data-ds-back-step1]")?.addEventListener("click", () => {
    extOtp.stop();
    feedback(dialogStatus);
    setStep(1);
  });

  confirmForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (extOtp.isExpired()) {
      feedback(dialogStatus, "Este código expirou. Clique em \"Reenviar código\" para receber um novo.", true);
      return;
    }
    const button = confirmForm.querySelector("[type=submit]");
    if (button) button.disabled = true;
    busy = true;
    syncAuthorization();
    feedback(dialogStatus, "Registrando sua assinatura eletrônica…");
    try {
      const result = await request(root, `/assinar/${token}/assinar`, {
        json: { consent: true, challenge_id: challengeId, code: confirmForm.elements.code.value.trim() },
      });
      extOtp.stop();
      const successMsg = find("[data-ds-success-msg]");
      if (successMsg && result.sealed) {
        successMsg.textContent = "Todas as assinaturas foram concluídas! O Manifesto de Assinaturas e o selo de integridade foram aplicados ao documento.";
      }
      setStep(3);
    } catch (error) {
      feedback(dialogStatus, error.message, true);
    } finally {
      busy = false;
      if (button && !extOtp.isExpired()) button.disabled = false;
      syncAuthorization();
    }
  });

  syncAuthorization();
  loadPdf();
}

function renderSealSvg(tone) {
  const map = {
    valid: { ring: "#294127", fill: "#E6F4E2", accent: "#446A36", word: "AUTÊNTICO", sym: "check" },
    partial: { ring: "#7A4F00", fill: "#FFF6E5", accent: "#C98200", word: "PARCIAL", sym: "alert" },
    invalid: { ring: "#912018", fill: "#FDECEA", accent: "#B42318", word: "INVÁLIDO", sym: "x" },
    indeterminate: { ring: "#7A4F00", fill: "#FFF6E5", accent: "#C98200", word: "PENDENTE", sym: "alert" },
    unsigned: { ring: "#5B6660", fill: "#F3F5F2", accent: "#7A857E", word: "SEM ASSINATURA", sym: "minus" },
  }[tone] || { ring: "#294127", fill: "#E6F4E2", accent: "#446A36", word: "AUTÊNTICO", sym: "check" };

  const iconPath = map.sym === "check"
    ? `<path d="M49 61 L57 69 L72 52" fill="none" stroke="${map.ring}" stroke-width="4.2" stroke-linecap="round" stroke-linejoin="round"/>`
    : map.sym === "x"
    ? `<path d="M52 52 L68 68 M68 52 L52 68" fill="none" stroke="${map.ring}" stroke-width="4.2" stroke-linecap="round"/>`
    : map.sym === "alert"
    ? `<path d="M60 50 V62 M60 68 V69" fill="none" stroke="${map.ring}" stroke-width="4.2" stroke-linecap="round"/>`
    : `<path d="M51 60 H69" fill="none" stroke="${map.ring}" stroke-width="4.2" stroke-linecap="round"/>`;

  return `
    <svg width="108" height="108" viewBox="0 0 120 120" aria-hidden="true" style="flex-shrink:0">
      <circle cx="60" cy="60" r="56" fill="${map.fill}" stroke="${map.ring}" stroke-width="2.2"/>
      <circle cx="60" cy="60" r="50" fill="none" stroke="${map.accent}" stroke-width="1" stroke-dasharray="3 2.5"/>
      <circle cx="60" cy="60" r="38" fill="#FFFFFF" stroke="${map.ring}" stroke-width="1.3"/>
      <text x="60" y="34" text-anchor="middle" font-size="7.2" font-weight="800" fill="${map.ring}" letter-spacing="1.4">SISFARM</text>
      ${iconPath}
      <rect x="20" y="81" width="80" height="17" rx="8.5" fill="${map.ring}"/>
      <text x="60" y="92.5" text-anchor="middle" font-size="${map.word.length > 10 ? "6.6" : "8"}" font-weight="800" fill="#FFFFFF" letter-spacing="0.8">${dsEsc(map.word)}</text>
    </svg>
  `;
}

function buildReceiptPreviewHtml(payload) {
  const tone = payload.tone || "valid";
  const signers = payload.signers || [];
  const verifyUrl = payload.code ? `app.sisfarm.com.br/validar/${payload.code}` : "app.sisfarm.com.br/validar";
  const rowsHtml = signers.length
    ? signers.map((s) => `
        <tr>
          <td><strong>${dsEsc(s.name)}</strong></td>
          <td class="ds-tabular">${dsEsc(s.cpf || "Conforme certificado")}</td>
          <td class="ds-tabular">${dsEsc(s.signed_at || "Pendente")}</td>
          <td><span class="sv-pill sv-pill--${dsEsc(s.type_tone || "brand")}">${dsEsc(s.type)}</span></td>
        </tr>
      `).join("")
    : `<tr><td colspan="4">Nenhuma assinatura digital encontrada neste arquivo.</td></tr>`;

  return `
    <div class="sv-a4">
      <div class="sv-a4-top">
        <div class="sv-a4-brand">
          <div>
            <small>SiSFarm · Validação Pública</small>
            <strong>Comprovante Oficial de Autenticidade</strong>
          </div>
        </div>
        <div class="sv-a4-proto ds-tabular">
          <div>Protocolo: <strong>${dsEsc(payload.protocol || "SiSFarm")}</strong></div>
          <div>Emitido em ${dsEsc(payload.checked_at || "")}</div>
        </div>
      </div>
      <div class="sv-a4-body">
        <div class="sv-a4-verdict sv-a4-verdict--${dsEsc(tone)}">
          <div>
            <h3>${dsEsc(payload.headline)}</h3>
            <p>${dsEsc(payload.summary)}</p>
          </div>
          ${renderSealSvg(tone)}
        </div>
        <div>
          <h4 class="sv-a4-sec-title">1. Identificação do documento</h4>
          <div class="sv-a4-box">
            <div class="sv-a4-doc-top">
              <small>Título do documento</small>
              <strong>${dsEsc(payload.title)}</strong>
            </div>
            <div class="sv-a4-cols">
              <div><small>Origem</small><strong>${dsEsc(payload.origin || "SiSFarm")}</strong></div>
              <div><small>Código de verificação</small><strong>${dsEsc(payload.code || "Documento externo")}</strong></div>
              <div><small>Integridade</small><strong>${dsEsc(payload.integrity || "Confirmada")}</strong></div>
            </div>
          </div>
        </div>
        <div>
          <h4 class="sv-a4-sec-title">2. Signatários registrados (${signers.length})</h4>
          <div class="sv-a4-box">
            <table class="sv-a4-table">
              <thead><tr><th>Signatário</th><th>CPF (LGPD)</th><th>Data e hora</th><th>Modalidade</th></tr></thead>
              <tbody>${rowsHtml}</tbody>
            </table>
          </div>
        </div>
        <div class="sv-a4-foot">
          <div><strong>Conferência online:</strong> <code>${dsEsc(verifyUrl)}</code></div>
          ${payload.sha256 ? `<div><strong>SHA-256:</strong> <code>${dsEsc(payload.sha256)}</code></div>` : ""}
          ${payload.seal_note ? `<div>${dsEsc(payload.seal_note)}</div>` : ""}
          <div>Conformidade: Lei 14.063/2020 · MP 2.200-2/2001 · LGPD (Lei 13.709/2018)</div>
        </div>
      </div>
    </div>
  `;
}

function renderDynamicUploadResult(container, payload) {
  const tone = payload.tone || "valid";
  const icTone = tone === "valid" ? "is-ok" : tone === "invalid" ? "is-err" : tone === "unsigned" ? "is-neutral" : "is-warn";
  const icSvg = tone === "valid"
    ? '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m5 12 4.5 4.5L19 7"/></svg>'
    : tone === "invalid"
    ? '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 6 6 18M6 6l12 12"/></svg>'
    : '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 9v4m0 4h.01M10.3 3.9 1.8 18.4A2 2 0 0 0 3.5 21h17a2 2 0 0 0 1.7-2.6L13.7 3.9a2 2 0 0 0-3.4 0Z"/></svg>';

  const signers = payload.signers || [];
  const signersHtml = signers.length
    ? `<ul class="sv-signer-list">${signers.map((s) => {
        const dotCls = s.status === "valid" ? "is-ok" : s.status === "invalid" ? "is-err" : "is-warn";
        const dotTxt = s.status === "valid" ? "✓" : s.status === "invalid" ? "✕" : "!";
        return `
          <li class="sv-signer-row">
            <span class="sv-signer-dot ${dotCls}" aria-hidden="true">${dotTxt}</span>
            <div class="sv-signer-body">
              <div>
                <strong>${dsEsc(s.name)}</strong>
                <p class="ds-tabular">CPF ${dsEsc(s.cpf || "Conforme certificado")} · ${dsEsc(s.signed_at || "Pendente")}</p>
              </div>
              <span class="sv-pill sv-pill--${dsEsc(s.type_tone || "brand")}">${dsEsc(s.type)}</span>
            </div>
          </li>
        `;
      }).join("")}</ul>`
    : `<p class="sv-muted-txt">Nenhuma assinatura digital encontrada neste arquivo.</p>`;

  const techEvHtml = signers.length
    ? `<div class="sv-tech-sep"><strong class="sv-tech-subhead">Registros individuais dos signatários</strong><div class="sv-tech-ev-list">${signers.map((s) => `<div class="sv-tech-ev-item"><strong>${dsEsc(s.name)}:</strong> ${dsEsc(s.tech || "")}</div>`).join("")}</div></div>`
    : "";

  container.innerHTML = `
    <div class="sv-action-bar">
      <button type="button" class="sf-btn sf-btn-secondary sv-btn-sm" data-sv-reset-home>← Validar outro</button>
      <div class="sv-action-right">
        <button type="button" class="sf-btn sf-btn-primary sv-btn-sm" data-sv-open-receipt>Comprovante PDF</button>
      </div>
    </div>

    <article class="sv-card" aria-label="Resultado da validação">
      <header class="sv-hero sv-hero--${dsEsc(tone)} sv-card-pad">
        <div class="sv-hero-row">
          <div class="sv-verdict-ic ${icTone}">${icSvg}</div>
          <div class="sv-hero-copy">
            <h1 class="sv-title">${dsEsc(payload.headline)}</h1>
            <p class="sv-summary">${dsEsc(payload.summary)}</p>
            ${payload.guidance ? `<p class="sv-guidance">${dsEsc(payload.guidance)}</p>` : ""}
          </div>
        </div>
      </header>

      <section class="sv-section sv-card-pad" aria-label="Dados do documento">
        <div class="sv-doc-head">
          <div>
            <span class="sv-eyebrow">Documento</span>
            <h2 class="sv-doc-title">${dsEsc(payload.title)}</h2>
          </div>
          ${payload.code ? `
            <div class="sv-code-pill">
              <code>${dsEsc(payload.code)}</code>
              <button type="button" class="sv-copy-btn" data-ds-copy-text="${dsEsc(payload.code)}">Copiar</button>
            </div>
          ` : ""}
        </div>
        <dl class="sv-meta-grid">
          <div><dt>Origem</dt><dd>${dsEsc(payload.origin || "Documento PDF")}</dd></div>
          <div><dt>Data</dt><dd class="ds-tabular">${dsEsc(payload.date || "—")}</dd></div>
          <div><dt>Integridade</dt><dd class="${payload.integrity_bad ? "is-err-txt" : tone === "valid" ? "is-ok-txt" : ""}">${dsEsc(payload.integrity || "Verificada")}</dd></div>
        </dl>
      </section>

      <section class="sv-card-pad" aria-label="Signatários">
        <div class="sv-signers-head">
          <span class="sv-eyebrow">Quem assinou ${signers.length ? `(${signers.length})` : ""}</span>
          ${signers.length ? `<span class="sv-lgpd-note">CPF protegido pela LGPD</span>` : ""}
        </div>
        ${signersHtml}
      </section>
    </article>

    <details class="sv-tech">
      <summary>
        <span>Detalhes técnicos e base legal</span>
        <svg class="sv-chevron" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>
      </summary>
      <div class="sv-tech-body">
        <dl class="sv-tech-list">
          ${payload.file_name ? `<div><dt>Arquivo analisado</dt><dd>${dsEsc(payload.file_name)}</dd></div>` : ""}
          <div><dt>Verificado em</dt><dd class="ds-tabular">${dsEsc(payload.checked_at || "")}</dd></div>
          <div><dt>Verificação técnica</dt><dd>${dsEsc(payload.policy || "")}</dd></div>
          ${payload.sha256 ? `
            <div>
              <dt>SHA-256</dt>
              <dd class="sv-hash-dd">
                <code>${dsEsc(payload.sha256)}</code>
                <button type="button" class="sv-copy-inline" data-ds-copy-text="${dsEsc(payload.sha256)}">Copiar</button>
              </dd>
            </div>
          ` : ""}
        </dl>
        ${techEvHtml}
        <div class="sv-tech-sep">
          <strong class="sv-tech-subhead">Selo de integridade e base legal</strong>
          <p>${dsEsc(payload.seal_note || "")}</p>
          <p>Validação em conformidade com a Lei 14.063/2020, MP 2.200-2/2001 e LGPD (Lei 13.709/2018).</p>
        </div>
      </div>
    </details>
  `;
}

function initPublicValidate(root) {
  const code = root.dataset.code;
  const codeForm = root.querySelector("[data-ds-val-code-form]");
  const codeInput = root.querySelector("[data-ds-code-mask]");
  const receiptDialog = document.querySelector("[data-sv-receipt-dialog]");
  const receiptPreview = receiptDialog?.querySelector("[data-sv-receipt-preview]");
  const printMount = document.querySelector("[data-sv-print-mount]");
  let activePayload = null;

  const initialPayloadEl = root.querySelector("[data-sv-initial-payload]");
  if (initialPayloadEl) {
    try { activePayload = JSON.parse(initialPayloadEl.textContent || ""); } catch {}
  }

  const openReceipt = () => {
    if (!activePayload || !receiptDialog || !receiptPreview) return;
    const html = buildReceiptPreviewHtml(activePayload);
    receiptPreview.innerHTML = html;
    if (printMount) printMount.innerHTML = html;
    receiptDialog.showModal();
  };

  document.addEventListener("click", (event) => {
    if (event.target.closest("[data-sv-open-receipt]")) openReceipt();
  });

  receiptDialog?.querySelector("[data-sv-receipt-close]")?.addEventListener("click", () => receiptDialog.close());
  receiptDialog?.addEventListener("click", (ev) => { if (ev.target === receiptDialog) receiptDialog.close(); });
  receiptDialog?.querySelector("[data-sv-receipt-print]")?.addEventListener("click", () => {
    if (printMount) printMount.hidden = false;
    window.print();
    if (printMount) printMount.hidden = true;
  });

  receiptDialog?.querySelector("[data-sv-receipt-download]")?.addEventListener("click", async () => {
    if (!activePayload) return;
    if (activePayload.receipt_url) {
      location.assign(activePayload.receipt_url);
      return;
    }
    const btn = receiptDialog.querySelector("[data-sv-receipt-download]");
    if (btn) btn.disabled = true;
    try {
      const resp = await fetch("/validar/comprovante.pdf", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": root.dataset.csrf,
        },
        body: JSON.stringify(activePayload),
        cache: "no-store",
      });
      if (!resp.ok) throw new Error("Não foi possível gerar o comprovante PDF.");
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `Comprovante_Validacao_${(activePayload.protocol || "SiSFarm").replace(/[^A-Za-z0-9_-]/g, "")}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 4000);
    } catch (err) {
      window.alert(err.message || "Não foi possível baixar o comprovante.");
    } finally {
      if (btn) btn.disabled = false;
    }
  });

  codeForm?.addEventListener("submit", (event) => {
    const formattedCode = maskVerificationCode(codeInput?.value || "");
    if (codeInput) codeInput.value = formattedCode;
    if (formattedCode.length >= 14) {
      event.preventDefault();
      location.assign(`/validar/${encodeURIComponent(formattedCode)}`);
    }
  });

  // Upload direto de PDF na página inicial /validar
  const homeView = root.querySelector("[data-sv-home-view]");
  const uploadDrop = root.querySelector("[data-sv-upload-drop]");
  const uploadInput = root.querySelector("[data-sv-upload-input]");
  const uploadError = root.querySelector("[data-sv-upload-error]");
  const checkingCard = root.querySelector("[data-sv-checking]");
  const uploadResult = root.querySelector("[data-sv-upload-result]");

  if (uploadDrop && uploadInput) {
    let busy = false;
    const showUploadError = (title, detail) => {
      if (!uploadError) return;
      uploadError.innerHTML = `<strong>${dsEsc(title)}</strong><span>${dsEsc(detail)}</span>`;
      uploadError.hidden = false;
    };

    const runDirectUpload = async (file) => {
      if (!file || busy) return;
      if (uploadError) uploadError.hidden = true;
      if (!file.name.toLowerCase().endsWith(".pdf")) {
        showUploadError("Este arquivo não é um PDF", "Somente arquivos .pdf carregam assinaturas digitais verificáveis.");
        return;
      }
      if (file.size > 12 * 1024 * 1024) {
        showUploadError("Arquivo acima de 12 MB", "Escolha um arquivo PDF de até 12 MB para validar.");
        return;
      }
      busy = true;
      if (homeView) homeView.hidden = true;
      if (uploadResult) uploadResult.hidden = true;
      if (checkingCard) checkingCard.hidden = false;
      try {
        const data = await request(root, "/validar/arquivo", { pdf: file, name: file.name });
        activePayload = data;
        if (checkingCard) checkingCard.hidden = true;
        if (uploadResult) {
          renderDynamicUploadResult(uploadResult, data);
          uploadResult.hidden = false;
          uploadResult.focus();
          uploadResult.querySelector("[data-sv-reset-home]")?.addEventListener("click", () => {
            uploadResult.hidden = true;
            if (homeView) homeView.hidden = false;
            window.scrollTo({ top: 0, behavior: "smooth" });
          });
        }
      } catch (err) {
        if (checkingCard) checkingCard.hidden = true;
        if (homeView) homeView.hidden = false;
        showUploadError("Não foi possível validar este PDF", err.message || "Tente novamente com o arquivo PDF original.");
      } finally {
        uploadInput.value = "";
        busy = false;
      }
    };

    uploadDrop.addEventListener("click", (ev) => {
      if (ev.target.closest("label") || ev.target === uploadInput) return;
      uploadInput.click();
    });
    uploadDrop.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); uploadInput.click(); }
    });
    uploadInput.addEventListener("change", () => runDirectUpload(uploadInput.files?.[0]));
    bindAnimatedDropzone(uploadDrop, {
      dragTitle: "Solte o PDF aqui",
      dragSub: "A validação de autenticidade iniciará automaticamente",
      onFile: runDirectUpload,
    });
  }

  // Conferência cruzada de PDF na página /validar/{code}
  if (!code) return;
  const dropEl = root.querySelector("[data-ds-pub-check-drop]");
  const input = root.querySelector("[data-ds-pub-pdf-input]");
  const statusEl = root.querySelector("[data-ds-pub-check-status]");
  const verdictEl = root.querySelector("[data-ds-pub-verdict]");
  let busyCheck = false;

  const checkPdf = async (file) => {
    if (!file || busyCheck) return;
    if (!file.name.toLowerCase().endsWith(".pdf") || file.size > 12 * 1024 * 1024) {
      feedback(statusEl, "Selecione um arquivo PDF de até 12 MB.", true);
      return;
    }
    busyCheck = true;
    feedback(statusEl, "Conferindo se o arquivo é idêntico ao documento assinado…");
    try {
      const data = await request(root, `/validar/${encodeURIComponent(code)}/conferir`, { pdf: file });
      feedback(statusEl);
      if (verdictEl) {
        verdictEl.className = `sv-cross-verdict ${data.matches ? "is-ok" : "is-err"}`;
        verdictEl.replaceChildren(
          element("strong", data.headline || (data.matches ? "Documento original confirmado" : "Este arquivo não é o mesmo que foi assinado")),
          element("p", data.message || ""),
        );
        verdictEl.hidden = false;
        verdictEl.focus();
      }
    } catch (error) {
      feedback(statusEl, error.message, true);
    } finally {
      if (input) input.value = "";
      busyCheck = false;
    }
  };

  input?.addEventListener("change", () => checkPdf(input.files?.[0]));
  bindAnimatedDropzone(dropEl, {
    dragTitle: "Solte o PDF aqui para comparar",
    dragSub: "A comparação com o documento original começa na hora",
    onFile: checkPdf,
  });
}

function initAdminSeal(root) {
  const form = root.querySelector("[data-ds-admin-seal-form]");
  const dropEl = root.querySelector("[data-ds-admin-seal-drop]");
  const statusEl = root.querySelector("[data-ds-admin-seal-status]");
  const deactivateBtn = root.querySelector("[data-ds-deactivate-seal]");
  const deactivateStatus = root.querySelector("[data-ds-deactivate-status]");
  let selectedPfx = null;

  const pickPfx = (file) => {
    if (!file) return;
    selectedPfx = file;
    const titleEl = dropEl?.querySelector("[data-ds-drop-title]");
    const subEl = dropEl?.querySelector("[data-ds-drop-sub]");
    if (titleEl) titleEl.textContent = file.name;
    if (subEl) subEl.textContent = `${Math.max(1, Math.round(file.size / 1024))} KB · Certificado pronto para envio`;
  };

  form?.elements.file?.addEventListener("change", () => pickPfx(form.elements.file.files?.[0]));
  bindAnimatedDropzone(dropEl, {
    dragTitle: "Solte o arquivo e-CNPJ A1 aqui",
    dragSub: "O certificado .pfx ou .p12 será preparado automaticamente",
    onFile: (file) => {
      if (form?.elements.file) {
        const dt = new DataTransfer();
        dt.items.add(file);
        form.elements.file.files = dt.files;
      }
      pickPfx(file);
    },
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = selectedPfx || form.elements.file?.files?.[0];
    if (!file || file.size > 200_000) {
      feedback(statusEl, "Selecione um certificado e-CNPJ A1 (.pfx ou .p12) de até 200 KB.", true);
      return;
    }
    if (!form.elements.consent?.checked) {
      feedback(statusEl, "Confirme o termo de uso exclusivamente institucional.", true);
      return;
    }
    const button = form.querySelector('button[type="submit"]');
    button.disabled = true;
    feedback(statusEl, "Validando e salvando o certificado institucional…");
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      let binary = "";
      for (const byte of bytes) binary += String.fromCharCode(byte);
      await request(root, "/admin/selo-institucional", {
        json: {
          file: btoa(binary),
          password: form.elements.password.value,
          consent: true,
        },
      });
      location.reload();
    } catch (error) {
      feedback(statusEl, error.message, true);
    } finally {
      form.elements.password.value = "";
      button.disabled = false;
    }
  });

  deactivateBtn?.addEventListener("click", async () => {
    if (!window.confirm("Desativar o e-CNPJ A1 institucional e voltar a selar novos documentos com o selo criptográfico interno do SiSFarm?")) return;
    deactivateBtn.disabled = true;
    feedback(deactivateStatus, "Desativando certificado institucional…");
    try {
      await request(root, "/admin/selo-institucional/desativar", {json: {}});
      location.reload();
    } catch (error) {
      feedback(deactivateStatus, error.message, true);
      deactivateBtn.disabled = false;
    }
  });
}

initActionMenus();
initPasswordToggles();
initInputMasks();
initCopyButtons();
const home = document.querySelector("[data-ds-home]");
const wizard = document.querySelector("[data-ds-wizard]");
const workspace = document.querySelector("[data-ds-workspace]");
const center = document.querySelector("[data-ds-certificates]");
const externalRoot = document.querySelector("[data-ds-external-root]");
const validateRoot = document.querySelector("[data-ds-validate-root]");
const adminSealRoot = document.querySelector("[data-ds-admin-seal]");
if (home) initHome(home);
if (wizard) initWizard(wizard);
if (workspace) initWorkspace(workspace);
if (center) initCertificates(center);
if (externalRoot) initExternalSigning(externalRoot);
if (validateRoot) initPublicValidate(validateRoot);
if (adminSealRoot) initAdminSeal(adminSealRoot);
