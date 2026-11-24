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

function formatted(value, formatter) {
  const date = new Date(value);
  return value && Number.isFinite(date.getTime()) ? formatter.format(date) : "";
}

function badge(state) {
  return element("span", states[state] || "A verificar", `ds-pill ds-pill--${states[state] ? state : "unsigned"}`);
}

function cleanText(value) {
  return (value || "").normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase().trim();
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
    const nextUrl = targetTab === "validate" ? "/assinatura-digital?aba=validar" : "/assinatura-digital";
    window.history.replaceState({}, "", nextUrl);
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

  // Filters & search
  const rows = [...root.querySelectorAll("[data-ds-list-row]")];
  const filterChips = [...root.querySelectorAll("[data-ds-filter]")];
  const categorySelect = root.querySelector("[data-ds-category-filter]");
  const searchInput = root.querySelector("[data-ds-search]");
  const visibleCount = root.querySelector("[data-ds-visible-count]");
  const emptySearch = root.querySelector("[data-ds-search-empty]");
  let activeFilter = "todos";

  function applyFilters() {
    const query = cleanText(searchInput?.value);
    const category = categorySelect?.value || "todas";
    let shown = 0;
    for (const row of rows) {
      const matchStatus = activeFilter === "todos" || row.dataset.status === activeFilter;
      const matchCategory = category === "todas" || row.dataset.category === category;
      const matchSearch = !query || cleanText(row.dataset.searchText).includes(query);
      row.hidden = !(matchStatus && matchCategory && matchSearch);
      if (!row.hidden) shown++;
    }
    if (visibleCount) visibleCount.textContent = `(${shown})`;
    if (emptySearch) emptySearch.hidden = shown !== 0 || rows.length === 0;
    for (const chip of filterChips) {
      chip.setAttribute("aria-pressed", String(chip.dataset.dsFilter === activeFilter));
    }
  }

  for (const chip of filterChips) {
    chip.addEventListener("click", () => {
      activeFilter = chip.dataset.dsFilter;
      applyFilters();
    });
  }
  for (const trigger of root.querySelectorAll("[data-ds-filter-trigger]")) {
    trigger.addEventListener("click", () => {
      activeFilter = trigger.dataset.dsFilterTrigger;
      switchTab("overview");
      applyFilters();
      root.querySelector(".ds-list-card")?.scrollIntoView({behavior: "smooth", block: "start"});
    });
  }
  categorySelect?.addEventListener("change", applyFilters);
  searchInput?.addEventListener("input", applyFilters);
  root.querySelector("[data-ds-reset-filters]")?.addEventListener("click", () => {
    activeFilter = "todos";
    if (categorySelect) categorySelect.value = "todas";
    if (searchInput) searchInput.value = "";
    applyFilters();
  });

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

function initWizard(root) {
  const form = root.querySelector("[data-ds-wiz-form]");
  if (!form) return;
  const dropEl = root.querySelector("[data-ds-wiz-drop]");
  const fileInput = form.elements.file;
  const loadedBox = root.querySelector("[data-ds-wiz-loaded]");
  const loadedName = root.querySelector("[data-ds-wiz-file-name]");
  const loadedMeta = root.querySelector("[data-ds-wiz-file-meta]");
  const clearFileBtn = root.querySelector("[data-ds-wiz-clear-file]");
  const titleInput = root.querySelector("[data-ds-wiz-title]");
  const catSelect = root.querySelector("[data-ds-wiz-category]");
  const subtitle = root.querySelector("[data-ds-wiz-subtitle]");
  const pane1 = root.querySelector('[data-ds-wiz-pane="1"]');
  const pane2 = root.querySelector('[data-ds-wiz-pane="2"]');
  const cancelBtn = root.querySelector("[data-ds-wiz-cancel]");
  const backBtn = root.querySelector("[data-ds-wiz-back]");
  const nextBtn = root.querySelector("[data-ds-wiz-next]");
  const submitBtn = root.querySelector("[data-ds-wiz-submit]");
  const statusEl = root.querySelector("[data-ds-wiz-status]");
  const whoCards = [...root.querySelectorAll("[data-ds-who]")];
  const policyCards = [...root.querySelectorAll("[data-ds-policy]")];

  // Top step indicators
  const stepDoc = root.querySelector('[data-wiz-step="doc"]');
  const stepSigners = root.querySelector('[data-wiz-step="signers"]');
  const stepReview = root.querySelector('[data-wiz-step="review"]');
  const stepConfirm = root.querySelector('[data-wiz-step="confirm"]');
  const stepSendReview = root.querySelector('[data-wiz-step="send_review"]');

  // Summary sidebar elements
  const sumStep = root.querySelector("[data-ds-sum-step]");
  const sumTitle = root.querySelector("[data-ds-sum-title]");
  const sumCat = root.querySelector("[data-ds-sum-cat]");
  const sumFile = root.querySelector("[data-ds-sum-file]");
  const sumPolicy = root.querySelector("[data-ds-sum-policy]");
  const sumWho = root.querySelector("[data-ds-sum-who]");
  const sumNext = root.querySelector("[data-ds-sum-next]");
  const sumPeopleWrap = root.querySelector("[data-ds-sum-people-wrap]");
  const sumPeopleList = root.querySelector("[data-ds-sum-people]");

  // External signers elements
  const extIcpWarn = root.querySelector("[data-ds-ext-icp-warn]");
  const extAdder = root.querySelector("[data-ds-ext-adder]");
  const extName = root.querySelector("[data-ds-ext-name]");
  const extRole = root.querySelector("[data-ds-ext-role]");
  const extChannel = root.querySelector("[data-ds-ext-channel]");
  const extPhoneWrap = root.querySelector("[data-ds-ext-phone-wrap]");
  const extEmailWrap = root.querySelector("[data-ds-ext-email-wrap]");
  const extPhone = root.querySelector("[data-ds-ext-phone]");
  const extEmail = root.querySelector("[data-ds-ext-email]");
  const extAddBtn = root.querySelector("[data-ds-ext-add-btn]");
  const extErr = root.querySelector("[data-ds-ext-err]");
  const extListEl = root.querySelector("[data-ds-ext-list]");
  const manifestoNote = root.querySelector("[data-ds-manifesto-note]");

  let selectedFile = null;
  let whoSigns = "only_me";
  let signingPolicy = "flexible";
  let currentStep = 1;
  const externalSigners = [];

  const POLICY_LABELS = {
    flexible: "Livre escolha",
    electronic: "Eletrônica SiSFarm",
    icp_brasil: "Somente ICP-Brasil A1",
  };

  function selectedColleagues() {
    return [...root.querySelectorAll("[data-ds-colleague-card]")].filter(
      (card) => card.querySelector("[data-ds-colleague-switch]")?.checked
    );
  }

  function renderExternalList() {
    if (!extListEl) return;
    extListEl.hidden = externalSigners.length === 0;
    extListEl.replaceChildren(...externalSigners.map((ext, idx) => {
      const li = element("li", "", "ds-ext-added-item");
      const info = element("div");
      info.append(
        element("strong", `${ext.name} (${ext.role_label})`),
        element("small", `${ext.channel === "whatsapp" ? "WhatsApp" : "E-mail"} · ${ext.channel === "whatsapp" ? ext.phone : ext.email}`),
      );
      const rmBtn = element("button", "Remover", "ds-inline-action is-danger");
      rmBtn.type = "button";
      rmBtn.addEventListener("click", () => {
        externalSigners.splice(idx, 1);
        renderExternalList();
        syncUI();
      });
      li.append(info, rmBtn);
      return li;
    }));
  }

  function syncUI() {
    const needsStep2 = whoSigns !== "only_me";
    if (!needsStep2) currentStep = 1;
    const totalSteps = whoSigns === "me_and_others" ? 4 : 3;
    const allowExternal = signingPolicy !== "icp_brasil";

    pane1.hidden = currentStep !== 1;
    pane2.hidden = currentStep !== 2;
    cancelBtn.hidden = currentStep !== 1;
    backBtn.hidden = currentStep !== 2;
    nextBtn.hidden = !(currentStep === 1 && needsStep2);
    submitBtn.hidden = currentStep === 1 && needsStep2;

    if (extIcpWarn) extIcpWarn.hidden = allowExternal;
    if (extAdder) extAdder.hidden = !allowExternal;
    if (manifestoNote) manifestoNote.hidden = !allowExternal;

    // Adaptive top steps bar
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

    if (subtitle) {
      subtitle.textContent = currentStep === 1
        ? "Envie o PDF, escolha a modalidade e defina quem vai assinar."
        : "Selecione equipe da fazenda e/ou convide signatários externos por link.";
    }
    if (submitBtn) {
      submitBtn.textContent = whoSigns === "only_others"
        ? "Enviar convites e abrir documento →"
        : "Continuar para Revisar e assinar →";
    }

    for (const card of whoCards) {
      card.setAttribute("aria-checked", String(card.dataset.dsWho === whoSigns));
    }
    for (const card of policyCards) {
      card.setAttribute("aria-checked", String(card.dataset.dsPolicy === signingPolicy));
    }

    // Update Sticky Summary Card in real time
    if (sumStep) sumStep.textContent = `Passo ${currentStep} de ${totalSteps}`;
    const docName = (titleInput?.value || "").trim() || (selectedFile ? selectedFile.name.replace(/\.pdf$/i, "") : "Nenhum arquivo selecionado");
    if (sumTitle) sumTitle.textContent = docName;
    if (sumCat) sumCat.textContent = catSelect?.value || "Outros";
    if (sumFile) sumFile.textContent = selectedFile ? selectedFile.name : "Aguardando envio";
    if (sumPolicy) sumPolicy.textContent = POLICY_LABELS[signingPolicy] || "Livre escolha";
    const chosen = selectedColleagues();
    const activeExternals = allowExternal ? externalSigners : [];
    const totalOthers = chosen.length + activeExternals.length;
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
        ...chosen.map((card) => element("li", `• ${card.dataset.name}`)),
        ...activeExternals.map((ext) => element("li", `• ${ext.name} (externo)`)),
      ];
      sumPeopleList.replaceChildren(...items);
    }
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

  for (const card of whoCards) {
    card.addEventListener("click", () => {
      whoSigns = card.dataset.dsWho;
      syncUI();
    });
  }
  for (const card of policyCards) {
    card.addEventListener("click", () => {
      signingPolicy = card.dataset.dsPolicy || "flexible";
      syncUI();
    });
  }
  extChannel?.addEventListener("change", () => {
    const isWa = extChannel.value === "whatsapp";
    if (extPhoneWrap) extPhoneWrap.hidden = !isWa;
    if (extEmailWrap) extEmailWrap.hidden = isWa;
    feedback(extErr);
  });
  function tryCommitPendingExternal() {
    if (signingPolicy === "icp_brasil") return true;
    const name = (extName?.value || "").trim();
    const phone = (extPhone?.value || "").trim();
    const email = (extEmail?.value || "").trim();
    if (!name && !phone && !email) return true;
    const role_label = (extRole?.value || "").trim() || "Signatário";
    const channel = extChannel?.value === "email" ? "email" : "whatsapp";
    if (name.length < 3) {
      feedback(extErr, "Informe o nome completo do signatário externo.", true);
      extName?.focus();
      return false;
    }
    if (channel === "whatsapp" && phone.replace(/\D/g, "").length < 10) {
      feedback(extErr, "Informe um número de WhatsApp válido com DDD.", true);
      extPhone?.focus();
      return false;
    }
    if (channel === "email" && !email.includes("@")) {
      feedback(extErr, "Informe um e-mail válido para o signatário externo.", true);
      extEmail?.focus();
      return false;
    }
    externalSigners.push({name, role_label, channel, phone: channel === "whatsapp" ? phone : "", email: channel === "email" ? email : ""});
    if (extName) extName.value = "";
    if (extPhone) extPhone.value = "";
    if (extEmail) extEmail.value = "";
    feedback(extErr);
    renderExternalList();
    syncUI();
    return true;
  }

  extAddBtn?.addEventListener("click", () => {
    tryCommitPendingExternal();
  });
  titleInput?.addEventListener("input", syncUI);
  catSelect?.addEventListener("change", syncUI);

  for (const item of root.querySelectorAll("[data-ds-colleague-card]")) {
    const sw = item.querySelector("[data-ds-colleague-switch]");
    const sel = item.querySelector("[data-ds-colleague-channel]");
    const contactInput = item.querySelector("[data-ds-colleague-contact]");
    const contactLabel = item.querySelector("[data-ds-colleague-contact-label]");
    sw?.addEventListener("change", syncUI);
    sel?.addEventListener("change", () => {
      const isWa = sel.value === "whatsapp";
      if (contactLabel) contactLabel.textContent = isWa ? "WhatsApp" : "E-mail";
      if (isWa) {
        const phone = contactInput.dataset.phone || "";
        if (phone) {
          contactInput.value = maskPhone(phone);
          contactInput.classList.remove("ds-input-warn");
          contactInput.classList.add("ds-input-readonly");
        } else {
          contactInput.value = "Nenhum telefone cadastrado no perfil";
          contactInput.classList.remove("ds-input-readonly");
          contactInput.classList.add("ds-input-warn");
        }
      } else {
        contactInput.value = contactInput.dataset.email || "";
        contactInput.classList.remove("ds-input-warn");
        contactInput.classList.add("ds-input-readonly");
      }
    });
  }

  nextBtn?.addEventListener("click", () => {
    const file = selectedFile || fileInput?.files?.[0];
    if (!file) {
      feedback(statusEl, "Selecione ou arraste um arquivo PDF antes de continuar.", true);
      return;
    }
    feedback(statusEl);
    currentStep = 2;
    syncUI();
    window.scrollTo({top: 0, behavior: "smooth"});
  });

  backBtn?.addEventListener("click", () => {
    currentStep = 1;
    syncUI();
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
    const chosen = whoSigns !== "only_me" ? selectedColleagues() : [];
    const activeExternals = (whoSigns !== "only_me" && signingPolicy !== "icp_brasil") ? externalSigners : [];
    if (whoSigns !== "only_me" && chosen.length === 0 && activeExternals.length === 0) {
      feedback(statusEl, "Selecione pelo menos um usuário da equipe ou adicione um signatário externo.", true);
      return;
    }
    submitBtn.disabled = true;
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
      for (const card of chosen) {
        const email = card.querySelector("[data-ds-colleague-switch]")?.value;
        const channel = card.querySelector("[data-ds-colleague-channel]")?.value || "email";
        if (email) {
          try {
            const res = await request(root, `${created.url}/participantes`, {
              json: {email, channel, consent: true},
            });
            if (res && res.notification_error) warnings.push(res.notification_error);
          } catch (err) {
            warnings.push(err.message || `Não foi possível compartilhar com ${email}.`);
          }
        }
      }
      for (const ext of activeExternals) {
        try {
          const res = await request(root, `${created.url}/externos`, {json: ext});
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
      submitBtn.disabled = false;
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
  const elCpfInput = find("[data-ds-el-cpf]");
  const elChannelSelect = find("[data-ds-el-channel]");
  const stampToggleTitle = find("[data-ds-stamp-toggle-title]");
  const stampToggleSub = find("[data-ds-stamp-toggle-sub]");
  const verCode = root.dataset.verificationCode || "";
  const docTitle = root.dataset.docTitle || "este documento";

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
          if (stampUrl) stampUrl.textContent = `Validar: /validar/${verCode}`;
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

  // Step 1 -> Step 2 (request authorization code — Electronic SiSFarm OR ICP-Brasil A1)
  authorize?.addEventListener("click", async () => {
    const isEl = activeMode === "electronic";
    const field = isEl ? null : selectedField();
    const appearance = field
      ? {field: field.name}
      : visible?.checked
      ? {visible: true, style: stampStyle, ...chosenLocation}
      : {visible: false};
    busy = true;
    syncAuthorization();
    const channel = isEl ? (elChannelSelect?.value || "email") : "email";
    feedback(status, channel === "whatsapp" ? "Enviando código de confirmação por WhatsApp…" : "Enviando código de confirmação por e-mail…");
    try {
      let data;
      if (isEl) {
        data = await request(root, endpoint + "/eletronica/autorizar", {
          json: {
            signer_name: (elNameInput?.value || "").trim(),
            cpf: (elCpfInput?.value || "").trim(),
            channel,
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
      feedback(status);
      feedback(dialogStatus);
      confirmForm.reset();
      setCeremonyStep(2);
      confirmForm.elements.code.focus();
    } catch (error) {
      feedback(status, error.message, true);
    } finally {
      busy = false;
      syncAuthorization();
    }
  });

  find("[data-ds-back-step1]")?.addEventListener("click", () => {
    feedback(dialogStatus);
    setCeremonyStep(1);
  });

  // Step 2 -> Sign with 8-digit OTP
  confirmForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
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

  // Share with organization colleagues
  const share = find("[data-ds-share]");
  const shareStatus = find("[data-ds-share-status]");
  const colleaguePick = find("[data-ds-colleague-pick]");
  colleaguePick?.addEventListener("change", () => {
    if (colleaguePick.value && share) share.elements.email.value = colleaguePick.value;
  });
  share?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = share.querySelector('button[type="submit"]');
    button.disabled = true;
    try {
      await request(root, endpoint + "/participantes", {
        json: {email: share.elements.email.value, consent: share.elements.consent.checked},
      });
      location.reload();
    } catch (error) {
      feedback(shareStatus, error.message, true);
    } finally {
      button.disabled = false;
    }
  });
  for (const control of root.querySelectorAll("[data-ds-remove]")) {
    control.addEventListener("click", async () => {
      if (!window.confirm("Remover o acesso deste participante ao documento?")) return;
      control.disabled = true;
      try {
        await request(root, endpoint + "/participantes/" + control.dataset.dsRemove, {method: "DELETE"});
        location.reload();
      } catch (error) {
        feedback(shareStatus, error.message, true);
      } finally {
        control.disabled = false;
      }
    });
  }

  // External signers management in Details tab
  const extForm = find("[data-ds-add-external]");
  const extWsChannel = find("[data-ds-ws-ext-channel]");
  const extWsPhoneWrap = find("[data-ds-ws-ext-phone-wrap]");
  const extWsEmailWrap = find("[data-ds-ws-ext-email-wrap]");
  const extWsStatus = find("[data-ds-ext-ws-status]");
  const extLinkBox = find("[data-ds-ext-link-box]");
  const extLinkTitle = find("[data-ds-ext-link-title]");
  const extLinkNote = find("[data-ds-ext-link-note]");
  const extLinkInput = find("[data-ds-ext-link-input]");
  const extWaBtn = find("[data-ds-ext-wa-btn]");
  const copyExtLinkBtn = find("[data-ds-copy-ext-link]");

  function showExternalLinkBox(res) {
    if (!extLinkBox || !extLinkInput) return;
    extLinkBox.hidden = false;
    extLinkInput.value = res.sign_url || "";
    if (extLinkTitle) {
      extLinkTitle.textContent = res.notification_sent
        ? `Convite enviado para ${res.signer_name || "o signatário"}!`
        : `Link exclusivo pronto para ${res.signer_name || "o signatário"}`;
    }
    if (extLinkNote) {
      extLinkNote.textContent = res.notification_sent
        ? "Convite disparado automaticamente! Se quiser, você também pode copiar o link ou enviar pelo seu WhatsApp."
        : (res.notification_error || "Link gerado com sucesso! Clique abaixo para enviar pelo seu WhatsApp ou copie o link.");
    }
    if (extWaBtn) {
      if (res.whatsapp_share_url) {
        extWaBtn.href = res.whatsapp_share_url;
        extWaBtn.hidden = false;
      } else {
        extWaBtn.hidden = true;
      }
    }
    extLinkBox.scrollIntoView({behavior: "smooth", block: "nearest"});
  }

  extWsChannel?.addEventListener("change", () => {
    const isWa = extWsChannel.value === "whatsapp";
    if (extWsPhoneWrap) extWsPhoneWrap.hidden = !isWa;
    if (extWsEmailWrap) extWsEmailWrap.hidden = isWa;
  });

  copyExtLinkBtn?.addEventListener("click", async () => {
    const url = extLinkInput?.value || "";
    if (!url) return;
    try {
      await navigator.clipboard.writeText(url);
      copyExtLinkBtn.textContent = "✓ Link copiado";
      setTimeout(() => { copyExtLinkBtn.textContent = "Copiar link"; }, 2000);
    } catch {
      extLinkInput?.select();
    }
  });

  extForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = extForm.querySelector('button[type="submit"]');
    button.disabled = true;
    feedback(extWsStatus, "Gerando link exclusivo e enviando convite…");
    try {
      const channel = extForm.elements.channel.value || "whatsapp";
      const res = await request(root, endpoint + "/externos", {
        json: {
          name: extForm.elements.name.value.trim(),
          role_label: (extForm.elements.role_label.value || "Signatário").trim(),
          channel,
          phone: channel === "whatsapp" ? extForm.elements.phone.value.trim() : "",
          email: channel === "email" ? extForm.elements.email.value.trim() : "",
        },
      });
      extForm.reset();
      if (extWsPhoneWrap) extWsPhoneWrap.hidden = false;
      if (extWsEmailWrap) extWsEmailWrap.hidden = true;
      feedback(extWsStatus);
      showExternalLinkBox(res);
    } catch (error) {
      feedback(extWsStatus, error.message, true);
    } finally {
      button.disabled = false;
    }
  });

  for (const control of root.querySelectorAll("[data-ds-resend-external]")) {
    control.addEventListener("click", async () => {
      control.disabled = true;
      feedback(extWsStatus, "Gerando novo link e reenviando convite…");
      try {
        const res = await request(root, `${endpoint}/externos/${control.dataset.dsResendExternal}/reenviar`, {method: "POST"});
        feedback(extWsStatus);
        showExternalLinkBox(res);
      } catch (error) {
        feedback(extWsStatus, error.message, true);
      } finally {
        control.disabled = false;
      }
    });
  }

  for (const control of root.querySelectorAll("[data-ds-remove-external]")) {
    control.addEventListener("click", async () => {
      if (!window.confirm("Revogar o link exclusivo deste signatário externo?")) return;
      control.disabled = true;
      try {
        await request(root, endpoint + "/externos/" + control.dataset.dsRemoveExternal, {method: "DELETE"});
        location.reload();
      } catch (error) {
        feedback(extWsStatus, error.message, true);
      } finally {
        control.disabled = false;
      }
    });
  }

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
  const options = JSON.parse(find("[data-ds-options]").textContent);
  const token = root.dataset.token;
  const verCode = root.dataset.verificationCode || "";
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
  const stampLabel = find("[data-ds-stamp-label]");
  const stampDoc = find("[data-ds-stamp-doc]");
  const stampDate = find("[data-ds-stamp-date]");
  const stampUrl = find("[data-ds-stamp-url]");
  const stampModesWrap = find("[data-ds-stamp-modes]");
  const stampModeButtons = [...root.querySelectorAll("[data-ds-stamp-style]")];
  const jumpStamp = find("[data-ds-jump-stamp]");
  const visible = find("[data-ds-visible]");
  const placeOnPage = find("[data-ds-place-current]");
  const elNameInput = find("[data-ds-el-name]");
  const elCpfInput = find("[data-ds-el-cpf]");
  const elChannelSelect = find("[data-ds-el-channel]");
  const elOverrideEmailWrap = find("[data-ds-el-override-email-wrap]");
  const elOverrideEmail = find("[data-ds-el-override-email]");
  const waActivateNote = find("[data-ds-wa-activate-note]");
  const consent = find("[data-ds-consent]");
  const authorize = find("[data-ds-ext-authorize]");
  const status = find("[data-ds-status]");
  const confirmForm = find("[data-ds-ext-confirm-form]");
  const dialogStatus = find("[data-ds-confirm-status]");

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
  let stampStyle = "full";

  function setStep(step) {
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

  function syncExternalChannelFields() {
    const ch = elChannelSelect?.value || "email";
    if (elOverrideEmailWrap) elOverrideEmailWrap.hidden = ch !== "email";
    if (waActivateNote) waActivateNote.hidden = ch !== "whatsapp";
  }

  function syncAuthorization() {
    if (!consent || !authorize) return;
    syncExternalChannelFields();
    const nameOk = Boolean((elNameInput?.value || "").trim().length >= 3);
    const cpfOk = isValidCpf(elCpfInput?.value || "");
    const ch = elChannelSelect?.value || "email";
    const emailOk = ch !== "email" || !elOverrideEmail || elOverrideEmailWrap?.hidden || (elOverrideEmail.value || "").trim().includes("@");
    const readyIdentity = nameOk && cpfOk && emailOk;
    if (!readyIdentity) consent.checked = false;
    consent.disabled = !readyIdentity;
    const consentBox = find("[data-ds-consent-box]");
    if (consentBox) consentBox.classList.toggle("is-disabled", !readyIdentity);
    const canPlace = !visible?.checked || chosenLocation;
    authorize.disabled = !viewport || !consent.checked || !readyIdentity || !canPlace || busy;
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
    const location = visible?.checked ? chosenLocation : null;
    stamp.hidden = !viewport || !location || location.page + 1 !== renderedPage;
    stamp.classList.toggle("is-compact", stampStyle === "compact");
    if (jumpStamp) jumpStamp.hidden = !location;
    if (!stamp.hidden) {
      const [left, top, width, height] = pixelRect(location);
      Object.assign(stamp.style, {left: `${left}px`, top: `${top}px`, width: `${width}px`, height: `${height}px`});
      const nowStr = formatTime.format(new Date());
      const signerName = ((elNameInput?.value || "SIGNATÁRIO").trim()).toUpperCase();
      const maskedCpf = maskCpfPublic(elCpfInput?.value || "");
      stampLabel.textContent = signerName;
      if (stampStyle === "compact") {
        if (stampDoc) { stampDoc.textContent = [maskedCpf, nowStr].filter(Boolean).join(" · "); stampDoc.hidden = false; }
        if (stampDate) stampDate.hidden = true;
        if (stampUrl) stampUrl.textContent = `Código: ${verCode}`;
      } else {
        if (stampDoc) { stampDoc.textContent = maskedCpf ? `CPF ${maskedCpf} · Lei 14.063/2020` : "Lei 14.063/2020 · MP 2.200-2"; stampDoc.hidden = false; }
        if (stampDate) { stampDate.textContent = `Data: ${nowStr} (BRT)`; stampDate.hidden = false; }
        if (stampUrl) stampUrl.textContent = `Validar: /validar/${verCode}`;
      }
    }
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
      drawTask = page.render({canvas, viewport: nextViewport, transform: ratio === 1 ? null : [ratio, 0, 0, ratio, 0, 0]});
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

  async function loadPdf() {
    try {
      const lib = await import(vendor + "legacy/build/pdf.min.mjs");
      lib.GlobalWorkerOptions.workerSrc = vendor + "legacy/build/pdf.worker.min.mjs";
      pdf = await lib.getDocument({
        url: root.dataset.pdfUrl, cMapUrl: vendor + "cmaps/", cMapPacked: true,
        standardFontDataUrl: vendor + "standard_fonts/", wasmUrl: vendor + "wasm/",
        isEvalSupported: false, enableXfa: false, maxImageSize: 24_000_000, stopAtErrors: true,
      }).promise;
      await render();
    } catch {
      previewStatus.hidden = false;
      previewStatus.textContent = "A prévia não está disponível. Use Baixar PDF acima para ler o documento.";
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
    if (!chosenLocation) return;
    if (chosenLocation.page + 1 !== renderedPage) changePage(chosenLocation.page + 1);
    else focusStampElement();
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
    drawStamp();
  }

  stamp?.addEventListener("pointerdown", (event) => {
    if (!viewport || !chosenLocation || event.button !== 0) return;
    stamp.setPointerCapture(event.pointerId);
    drag = {x: event.clientX, y: event.clientY, start: pixelRect(chosenLocation), resize: event.target.matches("[data-ds-handle]")};
    event.preventDefault();
  });
  stamp?.addEventListener("pointermove", (event) => {
    if (drag) resizeOrMove(event.clientX - drag.x, event.clientY - drag.y, drag.start, drag.resize);
  });
  stamp?.addEventListener("lostpointercapture", () => { drag = null; });
  stamp?.addEventListener("keydown", (event) => {
    const moves = {ArrowLeft: [-8, 0], ArrowRight: [8, 0], ArrowUp: [0, -8], ArrowDown: [0, 8]};
    if (!moves[event.key] || !chosenLocation) return;
    event.preventDefault();
    resizeOrMove(...moves[event.key], pixelRect(chosenLocation), event.shiftKey);
  });

  elNameInput?.addEventListener("input", drawStamp);
  elCpfInput?.addEventListener("input", drawStamp);
  elChannelSelect?.addEventListener("change", syncAuthorization);
  elOverrideEmail?.addEventListener("input", syncAuthorization);
  visible?.addEventListener("change", () => {
    if (visible.checked && !chosenLocation) defaultLocation();
    drawStamp();
    if (visible.checked) requestAnimationFrame(() => focusStampElement());
  });
  for (const btn of stampModeButtons) {
    btn.addEventListener("click", () => {
      stampStyle = btn.dataset.dsStampStyle === "compact" ? "compact" : "full";
      defaultLocation();
      drawStamp();
      requestAnimationFrame(() => focusStampElement());
    });
  }
  placeOnPage?.addEventListener("click", () => {
    defaultLocation();
    drawStamp();
    requestAnimationFrame(() => focusStampElement());
  });
  consent?.addEventListener("change", syncAuthorization);

  authorize?.addEventListener("click", async () => {
    const appearance = visible?.checked
      ? {visible: true, style: stampStyle, ...chosenLocation}
      : {visible: false};
    const channel = elChannelSelect?.value || "email";
    busy = true;
    syncAuthorization();
    feedback(status, channel === "whatsapp" ? "Enviando código por WhatsApp…" : "Enviando código por e-mail…");
    try {
      const data = await request(root, `/assinar/${token}/autorizar`, {
        json: {
          signer_name: (elNameInput?.value || "").trim(),
          cpf: (elCpfInput?.value || "").trim(),
          channel,
          email: (elOverrideEmail?.value || "").trim(),
          appearance,
          consent: consent.checked,
        },
      });
      challengeId = data.challenge_id;
      const step2Sub = find("[data-ds-step2-sub]");
      if (step2Sub) step2Sub.textContent = `Enviamos um código de 8 dígitos para ${data.destination_masked || "seu contato"}.`;
      const holderEl = find("[data-ds-confirm-holder]");
      if (holderEl) holderEl.textContent = (elNameInput?.value || "").trim();
      feedback(status);
      feedback(dialogStatus);
      confirmForm.reset();
      setStep(2);
      confirmForm.elements.code.focus();
    } catch (error) {
      feedback(status, error.message, true);
    } finally {
      busy = false;
      syncAuthorization();
    }
  });

  find("[data-ds-back-step1]")?.addEventListener("click", () => {
    feedback(dialogStatus);
    setStep(1);
  });

  confirmForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = confirmForm.querySelector("[type=submit]");
    button.disabled = true;
    feedback(dialogStatus, "Registrando sua assinatura eletrônica…");
    try {
      const result = await request(root, `/assinar/${token}/assinar`, {
        json: {consent: true, challenge_id: challengeId, code: confirmForm.elements.code.value.trim()},
      });
      const successMsg = find("[data-ds-success-msg]");
      if (successMsg && result.sealed) {
        successMsg.textContent = "Todas as assinaturas foram concluídas! O Manifesto de Assinaturas e o selo de integridade foram aplicados ao documento.";
      }
      setStep(3);
    } catch (error) {
      feedback(dialogStatus, error.message, true);
    } finally {
      button.disabled = false;
    }
  });

  // Mobile embedded viewer parity
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
      if (viewerSection.parentElement !== mobileViewerSlot) mobileViewerSlot.append(viewerSection);
      viewerSection.classList.add("is-mobile-embedded");
      viewerSection.classList.remove("is-mobile-collapsed");
      ensurePdfLoaded();
    } else if (isMobile && isAlreadySigned) {
      if (mobileViewerSlot) mobileViewerSlot.hidden = true;
      if (viewerSection.parentElement !== ceremonyGrid) ceremonyGrid.prepend(viewerSection);
      viewerSection.classList.remove("is-mobile-embedded");
      viewerSection.classList.toggle("is-mobile-collapsed", !mobileViewerExpanded);
      if (mobileViewerExpanded) ensurePdfLoaded();
    } else {
      if (mobileViewerSlot) mobileViewerSlot.hidden = true;
      if (viewerSection.parentElement !== ceremonyGrid) ceremonyGrid.prepend(viewerSection);
      viewerSection.classList.remove("is-mobile-embedded", "is-mobile-collapsed");
      ensurePdfLoaded();
    }
  }

  for (const btn of toggleViewerBtns) {
    btn.addEventListener("click", () => {
      mobileViewerExpanded = !mobileViewerExpanded;
      syncMobileViewer();
      btn.setAttribute("aria-expanded", String(mobileViewerExpanded));
    });
  }
  mobileMq.addEventListener?.("change", syncMobileViewer);
  syncAuthorization();
  syncMobileViewer();
}

function initPublicValidate(root) {
  const code = root.dataset.code;
  const codeForm = root.querySelector("[data-ds-val-code-form]");
  const codeInput = root.querySelector("[data-ds-code-mask]");
  codeForm?.addEventListener("submit", (event) => {
    const formattedCode = maskVerificationCode(codeInput?.value || "");
    if (codeInput) codeInput.value = formattedCode;
    if (formattedCode.length >= 14) {
      event.preventDefault();
      location.assign(`/validar/${encodeURIComponent(formattedCode)}`);
    }
  });

  if (!code) return;
  const dropEl = root.querySelector("[data-ds-pub-check-drop]");
  const input = root.querySelector("[data-ds-pub-pdf-input]");
  const statusEl = root.querySelector("[data-ds-pub-check-status]");
  const verdictEl = root.querySelector("[data-ds-pub-verdict]");
  let busy = false;

  const checkPdf = async (file) => {
    if (!file || busy) return;
    if (!file.name.toLowerCase().endsWith(".pdf") || file.size > 12 * 1024 * 1024) {
      feedback(statusEl, "Selecione um arquivo PDF de até 12 MB.", true);
      return;
    }
    busy = true;
    feedback(statusEl, "Conferindo se o arquivo é idêntico ao documento assinado…");
    try {
      const data = await request(root, `/validar/${encodeURIComponent(code)}/conferir`, {pdf: file});
      feedback(statusEl);
      if (verdictEl) {
        verdictEl.className = `ds-pub-verdict ${data.matches ? "is-ok" : "is-err"}`;
        const details = element("details", "", "ds-tech-details");
        details.append(element("summary", "Ver detalhes técnicos de conferência"));
        const dl = element("dl", "", "ds-tech-dl");
        const r1 = element("div");
        r1.append(element("dt", "Arquivo enviado (SHA-256)"), element("dd", data.uploaded_sha256 || ""));
        const r2 = element("div");
        r2.append(element("dt", "Documento registrado no SiSFarm (SHA-256)"), element("dd", data.expected_sha256 || ""));
        dl.append(r1, r2);
        details.append(dl);
        verdictEl.replaceChildren(
          element("strong", data.headline || (data.matches ? "Documento original confirmado" : "Este arquivo não é o mesmo que foi assinado")),
          element("p", data.message || ""),
          details,
        );
        verdictEl.hidden = false;
        verdictEl.focus();
      }
    } catch (error) {
      feedback(statusEl, error.message, true);
    } finally {
      if (input) input.value = "";
      busy = false;
    }
  };

  input?.addEventListener("change", () => checkPdf(input.files?.[0]));
  bindAnimatedDropzone(dropEl, {
    dragTitle: "Solte o PDF aqui para conferir",
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
