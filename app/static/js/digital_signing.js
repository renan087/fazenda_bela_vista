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
  return element("span", states[state] || "A verificar", `ds-status ds-status--${states[state] ? state : "unsigned"}`);
}

function reportInto(container, report, filename) {
  const content = element("div", "", "ds-result-content");
  const title = element("div", "", "ds-result-title");
  if (filename) title.append(element("strong", filename));
  title.append(badge(report.state));
  content.append(title);
  if (report.checked_at) content.append(element("p", `Verificado em ${formatted(report.checked_at, formatTime)}`, "ds-hint"));
  if (report.message) content.append(element("p", report.message, "ds-hint"));
  if (report.state === "unsigned") content.append(element("p", "Nenhuma assinatura digital encontrada neste PDF.", "ds-hint"));

  for (const [index, signature] of (report.signatures || []).entries()) {
    const details = element("details", "", "ds-signature");
    const summary = element("summary", "");
    summary.append(element("span", signature.holder || `Assinatura ${index + 1}`), " ", badge(signature.state));
    details.append(summary, element("p", signature.message));
    const keyValues = [
      ["Autoridade certificadora", signature.issuer],
      ["Vigência do certificado", signature.valid_from && signature.valid_until
        ? `${formatted(signature.valid_from, formatDate)} a ${formatted(signature.valid_until, formatDate)}` : ""],
      ["Integridade da revisão", signature.integrity ? "Verificada" : "Não comprovada"],
      ["Cadeia de confiança", signature.chain ? "Verificada" : "Não comprovada"],
      ["Revogação", {checked: "Verificada", revoked: "Evidência de revogação", unknown: "Não comprovada"}[signature.revocation]],
      ["Carimbo de tempo verificado", formatted(signature.signed_at, formatTime)],
      ["Política de assinatura", signature.policy],
      ["Impressão digital SHA-256", signature.fingerprint],
    ];
    const values = element("dl");
    for (const [label, value] of keyValues) {
      if (value) values.append(element("dt", label), element("dd", value));
    }
    details.append(values);
    content.append(details);
  }
  container.replaceChildren(content);
  container.hidden = false;
}

async function request(root, path, {method = "POST", json, pdf, name} = {}) {
  const headers = {"X-CSRF-Token": root.dataset.csrf, Accept: "application/json"};
  if (json) headers["Content-Type"] = "application/json";
  if (pdf) headers["Content-Type"] = "application/pdf";
  if (name) headers["X-Document-Name"] = encodeURIComponent(name);
  const response = await fetch(path, {
    method, headers, body: json ? JSON.stringify(json) : pdf, cache: "no-store",
  });
  if (response.redirected && new URL(response.url).pathname === "/login") {
    throw new Error("Sua sessão terminou. Entre no SiSFarm novamente.");
  }
  const data = await response.json().catch(() => ({}));
  if (!response.ok || response.redirected) throw new Error(typeof data.detail === "string" ? data.detail : "A operação não pôde ser concluída. Tente novamente.");
  return data;
}

function initHome(root) {
  const status = root.querySelector("[data-ds-status]");
  const result = root.querySelector("[data-ds-result]");
  let busy = false;

  for (const drop of root.querySelectorAll("[data-ds-drop]")) {
    const input = drop.querySelector("input");
    const importMode = drop.dataset.dsDrop === "import";
    const useFile = async (file) => {
      if (!file || busy) return;
      if (!file.name.toLowerCase().endsWith(".pdf") || file.size > 12 * 1024 * 1024) {
        feedback(status, "Escolha um PDF de até 12 MB.", true);
        return;
      }
      busy = true;
      drop.setAttribute("aria-busy", "true");
      feedback(status, importMode ? "Importando PDF…" : "Verificando as assinaturas…");
      if (!importMode) result.hidden = true;
      try {
        const path = importMode ? "/assinatura-digital/documentos" : "/assinatura-digital/validar";
        const data = await request(root, path, {pdf: file, name: file.name});
        if (importMode) {
          location.assign(data.url);
          return;
        }
        reportInto(result, data, file.name);
        result.focus();
        feedback(status);
      } catch (error) {
        feedback(status, error.message, true);
      } finally {
        input.value = "";
        busy = false;
        drop.removeAttribute("aria-busy");
      }
    };
    input.addEventListener("change", () => useFile(input.files?.[0]));
    drop.addEventListener("dragover", (event) => {
      event.preventDefault();
      drop.classList.add("is-dragging");
    });
    drop.addEventListener("dragleave", (event) => {
      if (!drop.contains(event.relatedTarget)) drop.classList.remove("is-dragging");
    });
    drop.addEventListener("drop", (event) => {
      event.preventDefault();
      drop.classList.remove("is-dragging");
      useFile(event.dataTransfer.files?.[0]);
    });
  }

  const search = root.querySelector("[data-ds-search]");
  if (search) {
    const rows = root.querySelectorAll("[data-ds-list-row]");
    const clean = (value) => value.normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase().trim();
    search.addEventListener("input", () => {
      const query = clean(search.value);
      let visible = 0;
      for (const row of rows) {
        row.hidden = !clean(row.querySelector("strong").textContent).includes(query);
        if (!row.hidden) visible++;
      }
      root.querySelector("[data-ds-search-empty]").hidden = visible !== 0;
    });
  }
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
  const stampLabel = find("[data-ds-stamp-label]");
  const certificate = find("[data-ds-certificate]");
  const target = find("[data-ds-target]");
  const visible = find("[data-ds-visible]");
  const placement = find("[data-ds-placement]");
  const placementHint = find("[data-ds-placement-hint]");
  const placeOnPage = find("[data-ds-place-current]");
  const consent = find("[data-ds-consent]");
  const authorize = find("[data-ds-authorize]");
  const status = find("[data-ds-status]");
  const report = find("[data-ds-result]");
  const dialog = find("[data-ds-confirm]");
  const confirmForm = find("[data-ds-sign-form]");
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

  function syncAuthorization() {
    const canPlace = target?.value || !visible?.checked || chosenLocation;
    authorize.disabled = authorize.dataset.enabled !== "yes" || !viewport || !consent.checked || !certificate.value || !canPlace || busy;
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
    const width = viewport.width * .32;
    const height = Math.min(viewport.height * .25, Math.max(viewport.height * .1, width * .27));
    chosenLocation = fromPixels(viewport.width * .62, viewport.height * .82, width, height);
  }

  const selectedField = () => options.fields.find((field) => field.name === target?.value);

  function drawStamp() {
    const fixedField = selectedField();
    const location = fixedField ? fixedField.location : visible?.checked ? chosenLocation : null;
    stamp.hidden = !viewport || !location || location.page + 1 !== renderedPage;
    stamp.classList.toggle("is-fixed", Boolean(fixedField));
    stamp.tabIndex = fixedField ? -1 : 0;
    if (!stamp.hidden) {
      const [left, top, width, height] = pixelRect(location);
      Object.assign(stamp.style, {left: `${left}px`, top: `${top}px`, width: `${width}px`, height: `${height}px`});
      stampLabel.textContent = certificate.selectedOptions[0]?.dataset.holder || "Assinatura digital";
    }
    if (placement) placement.hidden = Boolean(fixedField);
    if (placementHint) placementHint.hidden = !visible.checked;
    if (placeOnPage) placeOnPage.hidden = !visible.checked || !chosenLocation || chosenLocation.page + 1 === renderedPage;
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
      const fitted = Math.min((view.clientWidth - padding) / raw.width, 1.32);
      const scale = Math.min(fitted * zoom, 2000 / Math.max(raw.width, raw.height));
      const nextViewport = page.getViewport({scale: Math.max(.02, scale)});
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
      // The page number remains available when a thumbnail cannot be drawn.
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

  window.addEventListener("resize", () => {
    if (pdf) render();
  });
  previous.addEventListener("click", () => changePage(pageNumber - 1));
  next.addEventListener("click", () => changePage(pageNumber + 1));
  pageInput.addEventListener("change", () => changePage(Number(pageInput.value)));
  thumbnailToggle.addEventListener("click", () => {
    if (!pdf) return;
    thumbnails.hidden = !thumbnails.hidden;
    thumbnailToggle.setAttribute("aria-expanded", String(!thumbnails.hidden));
    if (!thumbnails.children.length) initThumbnails();
    render();
  });
  for (const control of root.querySelectorAll("[data-ds-zoom]")) {
    control.addEventListener("click", () => {
      zoom = Math.max(.65, Math.min(2.75, zoom + Number(control.dataset.dsZoom) * .2));
      render();
    });
  }
  find("[data-ds-fit]").addEventListener("click", () => { zoom = 1; render(); });

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
    consent.checked = false;
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
  certificate.addEventListener("change", syncChange);
  visible?.addEventListener("change", () => {
    if (visible.checked && !chosenLocation) defaultLocation();
    syncChange();
  });
  placeOnPage?.addEventListener("click", () => { defaultLocation(); syncChange(); });
  target?.addEventListener("change", () => {
    if (selectedField()?.location) changePage(selectedField().location.page + 1);
    syncChange();
  });
  consent.addEventListener("change", syncAuthorization);
  authorize.addEventListener("click", async () => {
    const field = selectedField();
    const appearance = field ? {field: field.name} : visible?.checked ? {visible: true, ...chosenLocation} : {visible: false};
    busy = true;
    syncAuthorization();
    feedback(status, "Solicitando o código de autorização…");
    try {
      const data = await request(root, endpoint + "/autorizar", {
        json: {certificate_id: certificate.value, appearance, consent: consent.checked, sha256: options.sha256},
      });
      challengeId = data.challenge_id;
      find("[data-ds-confirm-holder]").textContent = data.holder;
      find("[data-ds-confirm-hash]").textContent = `SHA-256 ${data.sha256}`;
      feedback(status);
      dialog.showModal();
      confirmForm.elements.code.focus();
    } catch (error) {
      feedback(status, error.message, true);
    } finally {
      busy = false;
      syncAuthorization();
    }
  });

  find("[data-ds-close]").addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", () => {
    confirmForm.reset();
    feedback(dialogStatus);
  });
  confirmForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = confirmForm.querySelector("[type=submit]");
    button.disabled = true;
    feedback(dialogStatus, "Assinando e conferindo o PDF…");
    try {
      await request(root, endpoint + "/assinar", {
        json: {consent: true, challenge_id: challengeId, code: confirmForm.elements.code.value,
          password: confirmForm.elements.password.value},
      });
      location.reload();
    } catch (error) {
      feedback(dialogStatus, error.message, true);
    } finally {
      confirmForm.elements.password.value = "";
      button.disabled = false;
    }
  });

  function drawReport(data) {
    reportInto(report, data);
    find("[data-ds-overview]").textContent = `${data.signatures.length} ${data.signatures.length === 1 ? "assinatura" : "assinaturas"} no PDF`;
  }
  drawReport(JSON.parse(find("[data-ds-report]").textContent));
  const revalidate = find("[data-ds-revalidate]");
  revalidate.addEventListener("click", async () => {
    revalidate.disabled = true;
    revalidate.textContent = "Verificando…";
    try {
      drawReport(await request(root, endpoint + "/validar"));
    } catch (error) {
      report.append(element("p", error.message, "ds-feedback is-error"));
    } finally {
      revalidate.disabled = false;
      revalidate.textContent = "Revalidar PDF";
    }
  });

  const share = find("[data-ds-share]");
  const shareStatus = find("[data-ds-share-status]");
  share?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = share.querySelector("button");
    button.disabled = true;
    try {
      await request(root, endpoint + "/participantes", {json: {email: share.elements.email.value,
        consent: share.elements.consent.checked}});
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

  loadPdf();
}

function initCertificates(root) {
  const form = root.querySelector("[data-ds-register]");
  if (!form) return;
  const status = root.querySelector("[data-ds-status]");
  const dialog = root.querySelector("[data-ds-purpose-dialog]");
  const purposeForm = root.querySelector("[data-ds-purpose-form]");
  const purposeStatus = root.querySelector("[data-ds-purpose-status]");
  let selection;

  for (const binding of root.querySelectorAll("form[data-ds-binding]")) {
    binding.addEventListener("submit", async (event) => {
      event.preventDefault();
      const userId = binding.elements.holder.value;
      const message = binding.querySelector("[data-ds-binding-status]");
      const button = binding.querySelector("button");
      const previousHolder = binding.dataset.currentHolder;
      if (previousHolder && previousHolder !== userId &&
          !window.confirm("Trocar o responsável encerra as autorizações da Central para este certificado. Confirmar a troca?")) return;
      button.disabled = true;
      feedback(message, "Vinculando responsável…");
      try {
        await request(root, `/certificados-digitais/${binding.dataset.dsBinding}/vinculo`, {json: {user_id: Number(userId)}});
        binding.dataset.currentHolder = userId;
        feedback(message, "Responsável vinculado.");
        location.reload();
      } catch (error) {
        feedback(message, error.message, true);
      } finally {
        button.disabled = false;
      }
    });
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = form.elements.file.files[0];
    if (!file || file.size > 200_000) {
      feedback(status, "Selecione um certificado A1 de até 200 KB.", true);
      return;
    }
    const button = form.querySelector("button");
    button.disabled = true;
    feedback(status, "Cadastrando o certificado no cofre…");
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      let binary = "";
      for (const byte of bytes) binary += String.fromCharCode(byte);
      await request(root, "/certificados-digitais", {json: {file: btoa(binary), password: form.elements.password.value}});
      location.reload();
    } catch (error) {
      feedback(status, error.message, true);
    } finally {
      form.elements.password.value = "";
      button.disabled = false;
    }
  });

  for (const control of root.querySelectorAll("[data-ds-purpose]")) {
    control.addEventListener("click", () => {
      selection = control.dataset;
      const enable = selection.enable === "yes";
      const purpose = selection.dsPurpose === "fiscal" ? "emissão fiscal" : "assinatura de documentos";
      const environment = selection.dsPurpose === "fiscal" ? ` em ${selection.env === "producao" ? "produção" : "homologação"}` : "";
      dialog.querySelector("[data-ds-purpose-title]").textContent = enable ? "Autorizar finalidade" : "Revogar finalidade";
      dialog.querySelector("[data-ds-purpose-holder]").textContent = selection.holder;
      dialog.querySelector("[data-ds-purpose-password]").hidden = !enable;
      purposeForm.elements.password.required = enable;
      const confirmCpf = enable && selection.cpf === "yes";
      dialog.querySelector("[data-ds-purpose-cpf]").hidden = !confirmCpf;
      purposeForm.elements.cpf.disabled = !confirmCpf;
      purposeForm.elements.cpf.required = confirmCpf;
      dialog.querySelector("[data-ds-purpose-consent]").textContent = enable
        ? `Autorizo expressamente o uso deste certificado para ${purpose}${environment}.`
        : `Confirmo a revogação do uso para ${purpose}.`;
      dialog.querySelector("[data-ds-purpose-submit]").textContent = enable ? "Autorizar uso" : "Revogar uso";
      dialog.showModal();
    });
  }
  dialog.querySelector("[data-ds-close]").addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", () => { purposeForm.reset(); feedback(purposeStatus); });
  purposeForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = purposeForm.querySelector("[type=submit]");
    button.disabled = true;
    try {
      await request(root, `/certificados-digitais/${selection.cert}/finalidades`, {
        json: {purpose: selection.dsPurpose, enable: selection.enable === "yes", environment: selection.env,
          password: purposeForm.elements.password.value, cpf: purposeForm.elements.cpf.value, consent: purposeForm.elements.consent.checked},
      });
      location.reload();
    } catch (error) {
      feedback(purposeStatus, error.message, true);
    } finally {
      purposeForm.elements.password.value = "";
      purposeForm.elements.cpf.value = "";
      button.disabled = false;
    }
  });
}

const home = document.querySelector("[data-ds-home]");
const workspace = document.querySelector("[data-ds-workspace]");
const center = document.querySelector("[data-ds-certificates]");
if (home) initHome(home);
if (workspace) initWorkspace(workspace);
if (center) initCertificates(center);
