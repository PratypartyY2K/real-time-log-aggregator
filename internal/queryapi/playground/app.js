const form = document.querySelector("#query-form");
const apiKey = document.querySelector("#api-key");
const start = document.querySelector("#start");
const end = document.querySelector("#end");
const summary = document.querySelector("#summary");
const empty = document.querySelector("#empty-state");
const errorBox = document.querySelector("#error");
const list = document.querySelector("#log-list");

const toLocalInput = (date) => {
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 19);
};
const now = new Date();
end.value = toLocalInput(now);
start.value = toLocalInput(new Date(now.getTime() - 60 * 60 * 1000));
apiKey.value = localStorage.getItem("logagg-api-key") || "local-dev-key";

const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, char => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
}[char]));

async function runQuery() {
  const button = form.querySelector("button");
  button.disabled = true;
  summary.textContent = "Querying…";
  errorBox.hidden = true;
  localStorage.setItem("logagg-api-key", apiKey.value);

  const params = new URLSearchParams({
    start: new Date(start.value).toISOString(),
    end: new Date(end.value).toISOString(),
    limit: document.querySelector("#limit").value
  });
  [["service", "#service"], ["level", "#level"], ["trace_id", "#trace-id"]].forEach(([key, selector]) => {
    const value = document.querySelector(selector).value.trim();
    if (value) params.set(key, value);
  });

  const began = performance.now();
  try {
    const response = await fetch(`/v1/logs?${params}`, { headers: { "X-API-Key": apiKey.value } });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    const elapsed = Math.round(performance.now() - began);
    summary.textContent = `${payload.count} records · ${elapsed} ms${payload.partial ? " · partial" : ""}`;
    empty.hidden = true;
    list.innerHTML = payload.logs.map(log => `
      <article class="log">
        <time>${escapeHTML(new Date(log.timestamp).toISOString())}</time>
        <span class="level ${escapeHTML(log.level)}">${escapeHTML(log.level)}</span>
        <span class="service">${escapeHTML(log.service)} / ${escapeHTML(log.environment)}</span>
        <span class="message">
          <strong>${escapeHTML(log.message)}</strong>
          <small>${escapeHTML(log.trace_id || log.ingest_id)}</small>
        </span>
      </article>`).join("");
    if (!payload.count) {
      empty.hidden = false;
      empty.querySelector("p").textContent = "No logs matched this query.";
    }
  } catch (error) {
    summary.textContent = "Failed";
    errorBox.textContent = error.message;
    errorBox.hidden = false;
  } finally {
    button.disabled = false;
  }
}

form.addEventListener("submit", event => {
  event.preventDefault();
  runQuery();
});
document.addEventListener("keydown", event => {
  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") runQuery();
});
