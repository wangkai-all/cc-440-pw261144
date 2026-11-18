/* incmerge dashboard - vanilla ES module, no build step. */
"use strict";

const state = {
  data: null,
  selectedHour: null,
  auditFilter: "",
  hiddenPending: new Set(),
};

const $ = (sel) => document.querySelector(sel);
const el = (tag, attrs = {}, children = []) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === undefined || v === null) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k === "html") node.innerHTML = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const child of [].concat(children)) {
    if (child === null || child === undefined) continue;
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
};

const STATE_LABEL = {
  published: "Published",
  blocked: "Applied · withheld",
  missing: "Missing",
  conflict: "Conflict",
  pending: "Pending merge",
};

const KIND_LABEL = {
  content_mismatch: "Content mismatch",
  intra_batch_multi_update: "Multiple updates in one batch",
  event_ts_outside_hour: "Event time outside hour",
  late_event_ignored: "Late event ignored",
  out_of_order_arrival: "Out-of-order arrival",
  delete_missing_key: "Delete for unknown key",
  redundant_delete: "Repeated delete",
  tombstone_applied: "Delete applied",
  resurrected_key: "Key resurrected",
  empty_batch: "Empty batch",
};

function fmtHour(hour) {
  if (!hour) return "—";
  const [, time] = hour.split("T");
  return time ? `${time}:00` : hour;
}

function fmtDay(hour) {
  return hour ? hour.slice(0, 10) : "";
}

function actor() {
  return ($("#actor").value || "dashboard").trim() || "dashboard";
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = { error: `HTTP ${res.status}` };
  }
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body;
}

function toast(message, isError = false) {
  const node = el("div", { class: `toast${isError ? " is-error" : ""}`, text: message });
  document.body.appendChild(node);
  setTimeout(() => node.remove(), 4200);
}

async function load() {
  const select = $("#dataset-select");
  const dataset = select.value || undefined;
  const qs = dataset ? `?dataset=${encodeURIComponent(dataset)}` : "";
  try {
    const data = await api(`/api/state${qs}`);
    state.data = data;
    renderDatasets(data);
    render();
  } catch (err) {
    toast(`Load failed: ${err.message}`, true);
  }
}

function renderDatasets(data) {
  const select = $("#dataset-select");
  if (select.options.length !== data.datasets.length ||
      [...select.options].some((o, i) => o.value !== data.datasets[i])) {
    select.replaceChildren(...data.datasets.map((name) =>
      el("option", { value: name, text: name })));
  }
  if (data.selected) select.value = data.selected;
}

function render() {
  const data = state.data;
  if (!data || !data.selected) {
    $("#wm-value").textContent = "—";
    $("#kpis").replaceChildren();
    $("#issues").replaceChildren(el("li", { class: "detail-empty", text: "No datasets yet. Create one with `incmerge init`." }));
    return;
  }
  renderChainBadge(data.audit);
  renderHero(data.status);
  renderChain(data.timeline);
  renderIssues(data);
  renderDetail();
  renderAudit(data);
  renderRecords(data);
}

function renderChainBadge(audit) {
  const badge = $("#chain-badge");
  badge.classList.toggle("badge-good", audit.ok);
  badge.classList.toggle("badge-critical", !audit.ok);
  $("#chain-text").textContent = audit.ok
    ? `chain verified · ${audit.entries} entries`
    : `chain broken at #${audit.broken_at}`;
}

function renderHero(status) {
  const wm = $("#wm-value");
  wm.textContent = status.watermark ? fmtHour(status.watermark) : "no watermark";
  const note = $("#wm-note");
  if (status.watermark) {
    note.textContent = `${fmtDay(status.watermark)} · published through this hour`;
  } else {
    note.textContent = status.missing_hours.length
      ? `First missing hour: ${status.missing_hours[0]}`
      : "No contiguous published run yet";
  }
  const kpis = [
    ["Head hour", status.head ? fmtHour(status.head) : "—", status.head ? fmtDay(status.head) : ""],
    ["Applied hours", String(status.applied_hours), `${status.rows_delivered.toLocaleString()} rows delivered`],
    ["Published snapshots", String(status.published_snapshots), `${status.blocked_snapshots} blocked`],
    ["Missing hours", String(status.missing_hours.length),
      status.missing_hours.length ? `blocks downstream · ${status.missing_hours[0]}` : "contiguous run",
      status.missing_hours.length ? "is-bad" : ""],
    ["Open conflicts", String(status.unresolved_conflicts),
      status.blocking_conflicts.length ? `blocking ${status.blocking_conflicts.join(", ")}` : "none blocking",
      status.unresolved_conflicts ? "is-warn" : ""],
    ["Active records", status.records.toLocaleString(), `${status.pending_batches} pending batches`],
  ];
  $("#kpis").replaceChildren(...kpis.map(([label, value, sub, cls]) =>
    el("div", { class: "kpi" }, [
      el("dt", { text: label }),
      el("dd", {}, [value, sub ? el("span", { class: `sub ${cls || ""}`, text: sub }) : null]),
    ])
  ));
}

function chainTooltip(hour) {
  const tip = $("#chain-tip");
  tip.hidden = false;
  const stats = `${hour.active_count ?? 0} active · ${hour.deleted_count ?? 0} tombstoned · ${hour.row_count} rows`;
  const extra = hour.state === "missing"
    ? "no file delivered"
    : hour.blocked_by
      ? `${stats} · withheld by ${hour.blocked_by}`
      : stats;
  tip.replaceChildren(el("div", { class: "chain-tip" }, [
    el("b", { text: hour.hour }),
    el("br"),
    document.createTextNode(STATE_LABEL[hour.state] || hour.state),
    el("br"),
    el("span", { text: extra }),
  ]));
  return tip.firstChild;
}

function positionTip(tip, clientX, clientY) {
  // Pointer-anchored, so the tooltip never looks pinned over a neighbouring
  // hour's bar. Keyboard focus passes the cell centre instead.
  const layer = $("#chain-tip").getBoundingClientRect();
  const halfWidth = tip.offsetWidth / 2;
  let x = clientX - layer.left;
  x = Math.min(Math.max(x, halfWidth + 2), layer.width - halfWidth - 2);
  const height = tip.offsetHeight;
  let y = clientY - layer.top - height - 14;
  if (y < 2) y = clientY - layer.top + 18; // no room above: drop below the cursor
  tip.style.left = `${x}px`;
  tip.style.top = `${y}px`;
}

function renderChain(timeline) {
  const chain = $("#chain");
  const hours = timeline.hours;
  const maxActive = Math.max(1, ...hours.map((h) => h.active_count || 0));
  const dayChanged = hours.map((h, i) => i === 0 || fmtDay(h.hour) !== fmtDay(hours[i - 1].hour));
  // Always keep a comfortable minimum width so a short dataset still reads as a
  // timeline rather than a few slabs floating in whitespace.
  chain.style.minWidth = `${Math.max(hours.length * 26, 420)}px`;

  chain.replaceChildren(...hours.map((hour, i) => {
    const active = hour.active_count || 0;
    const height = Math.max(8, Math.round((active / maxActive) * 96));
    const cell = el("button", {
      class: `chain-cell${dayChanged[i] ? " is-day-start" : ""}`,
      type: "button",
      role: "listitem",
      "aria-pressed": state.selectedHour === hour.hour ? "true" : "false",
      "aria-label": `${hour.hour}: ${STATE_LABEL[hour.state] || hour.state}`,
      onclick: () => { state.selectedHour = hour.hour; render(); },
      onmouseenter: (e) => positionTip(chainTooltip(hour), e.clientX, e.clientY),
      onmousemove: (e) => positionTip(chainTooltip(hour), e.clientX, e.clientY),
      onfocus: () => {
        const rect = cell.getBoundingClientRect();
        positionTip(chainTooltip(hour), rect.left + rect.width / 2, rect.top);
      },
      onmouseleave: () => { $("#chain-tip").hidden = true; },
      onblur: () => { $("#chain-tip").hidden = true; },
    }, [
      el("div", { class: "chain-plot" }, [
        el("div", {
          class: `chain-bar${hour.state === "missing" ? " is-missing" : ""}${hour.state === "published" && hour.row_count === 0 ? " is-empty" : ""}`,
          style: `--c: var(--state-${hour.state}); height: ${height}px;`,
          "aria-hidden": "true",
        }),
      ]),
      el("div", { class: "chain-label" }, dayChanged[i]
        ? [el("span", { text: fmtDay(hour.hour).slice(5) }), el("span", { text: `${fmtHour(hour.hour).slice(0, 2)}h` })]
        : [el("span", { text: " " }), el("span", { text: `${fmtHour(hour.hour).slice(0, 2)}h` })]),
    ]);
    return cell;
  }));

  const first = hours[0]?.hour || "";
  const last = hours[hours.length - 1]?.hour || "";
  $("#chain-scale").replaceChildren(
    el("span", { text: first ? `${first} (start)` : "" }),
    el("span", { text: last ? `${last} (head)` : "" }),
  );
  const hint = $("#chain-hint");
  hint.textContent = state.selectedHour
    ? `Selected ${state.selectedHour} · click another hour to inspect`
    : "Select an hour to inspect its batch, snapshot and recovery actions.";
}

function issueCard(item) {
  const blocking = item.kind === "content_mismatch" || item.blocking;
  const open = !item.resolution;
  const deliveryId = item.delivery_id || (item.detail && item.detail.delivery_id);
  const detailText = item.detail
    ? Object.entries(item.detail)
        .filter(([, v]) => v !== null && v !== undefined)
        .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`)
        .join("  ·  ")
    : "";
  const actions = [];
  if (blocking && open && deliveryId) {
    actions.push(el("button", {
      class: "btn btn-ghost", type: "button",
      onclick: (e) => { e.stopPropagation(); resolveConflict(deliveryId, "keep_existing"); },
      text: "Keep applied batch",
    }));
    actions.push(el("button", {
      class: "btn btn-ghost", type: "button",
      onclick: (e) => { e.stopPropagation(); openResolveDialog({ ...item, delivery_id: deliveryId }); },
      text: "Review incoming",
    }));
  }
  return el("li", {
    class: `issue${blocking && open ? " is-blocking" : ""}`,
    tabindex: "0",
    onclick: () => { state.selectedHour = item.hour; render(); },
    onkeydown: (e) => { if (e.key === "Enter") { state.selectedHour = item.hour; render(); } },
  }, [
    el("div", { class: "issue-top" }, [
      el("span", { class: "issue-kind", text: KIND_LABEL[item.kind] || item.kind }),
      el("span", { class: "issue-hour", text: item.hour }),
      blocking && open ? el("span", { class: "tag tag-critical", text: "blocking" }) : null,
      item.resolution ? el("span", { class: "tag tag-good", text: item.resolution }) : null,
      !blocking ? el("span", { class: "tag", text: "observed" }) : null,
    ]),
    item.key ? el("p", { class: "issue-body", text: `key ${item.key}` }) : null,
    el("p", { class: "issue-body", text: item.rule }),
    detailText ? el("p", { class: "issue-detail", text: detailText }) : null,
    actions.length ? el("div", { class: "issue-actions" }, actions) : null,
  ]);
}

function renderIssues(data) {
  const issues = data.conflicts;
  $("#conflict-count").textContent = `${issues.length} item${issues.length === 1 ? "" : "s"}`;
  const list = $("#issues");
  if (!issues.length) {
    list.replaceChildren(el("li", { class: "detail-empty", text: "No conflicts observed. Every batch folded cleanly." }));
    return;
  }
  const rank = (i) => (i.blocking ? 0 : i.kind === "content_mismatch" ? 1 : 2);
  const sorted = [...issues].sort((a, b) => rank(a) - rank(b) || (a.hour < b.hour ? 1 : -1));
  list.replaceChildren(...sorted.map(issueCard));
}

function renderDetail() {
  const data = state.data;
  const box = $("#detail");
  const sub = $("#detail-sub");
  const hour = state.selectedHour;
  if (!hour) {
    sub.textContent = "No hour selected.";
    box.replaceChildren(el("div", { class: "detail-empty", text: "Pick an hour from the chain or the watchlist." }));
    return;
  }
  const entry = data.timeline.hours.find((h) => h.hour === hour);
  if (!entry) {
    sub.textContent = "No hour selected.";
    box.replaceChildren(el("div", { class: "detail-empty", text: "This hour is outside the dataset range." }));
    return;
  }
  sub.textContent = `${hour} · ${STATE_LABEL[entry.state] || entry.state}`;
  const rows = [
    ["State", STATE_LABEL[entry.state] || entry.state],
    ["Batch", entry.batch_id ? `#${entry.batch_id}` : "not delivered"],
    ["Source file", entry.filename || "—"],
    ["Rows / deletes", `${entry.row_count} / ${entry.delete_count}`],
    ["Applied at", entry.applied_at || "—"],
    ["Snapshot version", entry.snapshot_version ?? "—"],
    ["Active / tombstoned", `${entry.active_count ?? "—"} / ${entry.deleted_count ?? "—"}`],
    ["Blocked by", entry.blocked_by || "—"],
    ["Content hash", entry.content_hash ? entry.content_hash.slice(0, 24) + "…" : "—"],
  ];
  const actions = [];
  if (entry.state === "pending" && entry.batch_id) {
    actions.push(el("button", {
      class: "btn btn-primary", type: "button",
      onclick: () => mergeBatch(entry.batch_id),
      text: "Apply batch (retry-safe)",
    }));
  }
  if (entry.state === "missing") {
    actions.push(el("button", {
      class: "btn btn-primary", type: "button",
      onclick: () => openBackfillDialog(entry.hour),
      text: "Backfill this hour",
    }));
  }
  if (entry.state === "blocked" && entry.batch_id) {
    actions.push(el("button", {
      class: "btn btn-ghost", type: "button",
      onclick: () => mergeBatch(entry.batch_id),
      text: "Re-run merge",
    }));
    actions.push(el("button", {
      class: "btn btn-ghost", type: "button",
      onclick: () => openBackfillDialog(entry.hour),
      text: "Re-deliver file",
    }));
  }
  // An unresolved mismatch needs a decision whether or not the hour published:
  // a conflict on an already-published hour is held for review without
  // retracting the watermark, so it must still be reachable from here.
  const unresolved = data.unresolved.filter((c) => c.hour === entry.hour);
  if (unresolved.length) {
    actions.push(el("button", {
      class: "btn btn-primary", type: "button",
      onclick: () => openResolveDialog(unresolved[0]),
      text: "Review mismatch",
    }));
  }

  const notes = [];
  if (entry.state === "blocked" && entry.blocked_by) {
    notes.push(el("p", { class: "detail-note" }, [
      el("strong", { text: "Withheld from downstream. " }),
      `The snapshot is materialised but not published because ${entry.blocked_by}. ` +
      "Fill the gap or resolve the conflict; publication resumes automatically and this snapshot is re-emitted.",
    ]));
  }
  const hourObs = data.conflicts.filter((c) => c.hour === entry.hour);
  if (hourObs.length) {
    notes.push(el("p", { class: "detail-note" }, [
      el("strong", { text: `${hourObs.length} observation${hourObs.length === 1 ? "" : "s"} for this hour. ` }),
      hourObs.map((c) => KIND_LABEL[c.kind] || c.kind).join(", "),
    ]));
  }

  box.replaceChildren(
    el("dl", { class: "kv" }, rows.flatMap(([k, v]) => [
      el("dt", { text: k }),
      el("dd", { text: String(v) }),
    ])),
    actions.length ? el("div", { class: "detail-actions" }, actions) : null,
    ...notes,
  );
}

function renderAudit(data) {
  const filter = state.auditFilter.toLowerCase();
  const entries = data.audit_entries.filter((e) =>
    !filter || e.event.toLowerCase().includes(filter) ||
    (e.dataset || "").toLowerCase().includes(filter) ||
    (e.hour || "").toLowerCase().includes(filter));
  $("#audit-count").textContent = `${entries.length} / ${data.audit_entries.length}`;
  const body = $("#audit-table tbody");
  body.replaceChildren(...entries.map((e) => el("tr", {}, [
    el("td", { class: "mono muted", text: String(e.seq) }),
    el("td", { class: "mono", text: e.at.replace("T", " ").replace("Z", "") }),
    el("td", { text: e.event }),
    el("td", { class: "muted", text: e.dataset || "—" }),
    el("td", { class: "mono muted", text: e.hour || "—" }),
    el("td", {}, [
      el("span", { class: "ellipsis mono", text: JSON.stringify(e.detail), title: JSON.stringify(e.detail, null, 2) }),
    ]),
    el("td", { class: "mono muted", text: e.entry_hash.slice(0, 10) }),
  ])));
}

function renderRecords(data) {
  $("#records-count").textContent = `${data.records.length} shown`;
  const body = $("#records-table tbody");
  body.replaceChildren(...data.records.map((r) => el("tr", {}, [
    el("td", { class: "mono", text: r.key }),
    el("td", {}, [el("span", {
      class: `tag ${r.deleted ? "tag-warn" : "tag-good"}`,
      text: r.deleted ? "tombstone" : "active",
    })]),
    el("td", { class: "mono", text: r.event_ts.replace("T", " ") }),
    el("td", { class: "mono muted", text: `batch #${r.batch_id}` }),
    el("td", { class: "mono muted", text: r.payload ? JSON.stringify(r.payload) : "—" }),
  ])));
}

/* ------------------------------ actions ------------------------------ */

async function mergeBatch(batchId) {
  try {
    const res = await api("/api/merge", {
      method: "POST",
      body: JSON.stringify({ batch_id: batchId, actor: actor() }),
    });
    toast(res.outcome === "noop"
      ? `Batch #${batchId} already applied (no-op)`
      : `Batch #${batchId} ${res.outcome}: watermark ${res.watermark || "—"}`);
    await load();
  } catch (err) {
    toast(`Merge failed: ${err.message}`, true);
  }
}

async function resolveConflict(deliveryId, action) {
  try {
    const res = await api("/api/resolve", {
      method: "POST",
      body: JSON.stringify({ delivery_id: deliveryId, action, actor: actor() }),
    });
    toast(`Conflict resolved (${action}) · watermark ${res.watermark || "—"}`);
    await load();
  } catch (err) {
    toast(`Resolve failed: ${err.message}`, true);
  }
}

function openResolveDialog(conflict) {
  // The same dialog is opened from two shapes: a conflict row from the
  // watchlist (hashes live in `detail`) and an unresolved delivery from the
  // hour detail (hashes are top-level columns).
  const detail = conflict.detail || {};
  const appliedHash = conflict.applied_hash || detail.existing_hash || detail.applied_hash;
  const incomingHash = conflict.content_hash || detail.incoming_hash;
  const filename = conflict.filename || detail.filename;
  const rowCount = conflict.row_count ?? detail.incoming_rows;
  const dialog = $("#action-dialog");
  $("#dialog-title").textContent = `Mismatch on ${conflict.hour}`;
  $("#dialog-sub").textContent =
    "The same hour was delivered twice with different content. If this hour is not yet published, publication is withheld from here onward until you choose a version.";
  $("#dialog-body").replaceChildren(
    el("dl", { class: "kv" }, [
      el("dt", { text: "Dataset" }),
      el("dd", { text: conflict.dataset }),
      el("dt", { text: "Applied batch" }),
      el("dd", { text: `#${conflict.batch_id}` }),
      el("dt", { text: "Applied hash" }),
      el("dd", { text: appliedHash || "—" }),
      el("dt", { text: "Incoming hash" }),
      el("dd", { text: incomingHash || "—" }),
      el("dt", { text: "Incoming file" }),
      el("dd", { text: filename
        ? `${filename} (${rowCount ?? "?"} row${rowCount === 1 ? "" : "s"})`
        : "unnamed" }),
    ]),
    el("p", { class: "detail-note", html:
      "<strong>Keep applied batch</strong> discards the re-delivery. " +
      "<strong>Accept incoming</strong> replaces the stored batch and recomputes every " +
      "snapshot from this hour onward. Both are audited." }),
  );
  $("#dialog-result").hidden = true;
  const run = $("#dialog-run");
  run.textContent = "Accept incoming";
  run.onclick = async () => {
    await resolveConflict(conflict.delivery_id || conflict.id, "accept_incoming");
    dialog.close();
  };
  dialog.showModal();
}

function openBackfillDialog(hour) {
  const dialog = $("#action-dialog");
  $("#dialog-title").textContent = `Backfill ${hour}`;
  $("#dialog-sub").textContent =
    "Paste the hourly CSV. The hour is delivered, merged, and all affected snapshots are recomputed.";
  const body = $("#dialog-body");
  body.replaceChildren(
    el("label", { class: "field", style: "flex-direction:column;align-items:stretch" }, [
      el("span", { text: "CSV content" }),
      el("textarea", { class: "input", id: "dialog-csv", placeholder: "key,event_ts,op,status,amount\nA,2026-11-18T04:10:00,upsert,paid,10" }),
    ]),
  );
  $("#dialog-result").hidden = true;
  const run = $("#dialog-run");
  run.textContent = "Deliver & merge";
  run.onclick = async () => {
    const csv = $("#dialog-csv").value;
    if (!csv.trim()) { toast("CSV is empty; use an empty file to mark a complete hour", true); }
    try {
      const res = await api("/api/backfill", {
        method: "POST",
        body: JSON.stringify({ dataset: state.data.selected, hour, csv, actor: actor() }),
      });
      const changed = res.merge?.recomputed_snapshots?.length || 0;
      dialog.close();
      const label = res.outcome === "backfilled" ? "Backfill complete" : `Backfill ${res.outcome}`;
      toast(
        `${label} · watermark ${res.merge?.watermark || "—"} · ` +
        `${changed} snapshot${changed === 1 ? "" : "s"} recomputed`,
      );
      await load();
    } catch (err) {
      showResult({ error: err.message }, true);
    }
  };
  dialog.showModal();
}

function showResult(value, isError = false) {
  const pre = $("#dialog-result");
  pre.hidden = false;
  pre.textContent = JSON.stringify(value, null, 2);
  pre.classList.toggle("is-error", isError);
}

/* ------------------------------ wiring ------------------------------ */

$("#dataset-select").addEventListener("change", () => {
  state.selectedHour = null;
  load();
});
$("#refresh").addEventListener("click", load);
$("#audit-filter").addEventListener("input", (e) => {
  state.auditFilter = e.target.value;
  if (state.data) renderAudit(state.data);
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    const dialog = $("#action-dialog");
    if (dialog.open) dialog.close();
  }
  if (e.key === "r" && (e.metaKey || e.ctrlKey) === false && e.target === document.body) {
    load();
  }
});

load();
setInterval(() => {
  if (!document.hidden && !$("#action-dialog").open) load();
}, 15000);
