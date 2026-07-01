"use strict";
/* Penny Console frontend — rail via SSE (/events), chat via NDJSON (/turn). */
const $ = id => document.getElementById(id);
const money0 = c => "$" + Math.round(c / 100).toLocaleString();
const esc = s => { const d = document.createElement("div"); d.textContent = s ?? ""; return d.innerHTML; };
const SESSION = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random())).slice(0, 8);

/* ---- clock ---- */
const fmtTime = d => d.toTimeString().slice(0, 8);
const fmtDate = d => d.toLocaleDateString("en-US", { weekday: "short", day: "numeric", month: "short", year: "numeric" });
function tickClock() { const n = new Date(); $("clock").innerHTML = `${fmtDate(n)} · <b>${fmtTime(n)}</b>`; }
tickClock(); setInterval(tickClock, 1000);

/* ---- KPIs ---- */
function paintKpis(s) {
  $("k-scan").textContent = s.scanned; $("k-inv").textContent = s.investigating;
  $("k-flag").textContent = s.flagged; $("k-exp").textContent = money0(s.exposure_cents) + " exposure";
  $("k-clr").textContent = s.cleared;
  $("f-in").textContent = s.scanned; $("f-inv").textContent = s.investigating;
  $("f-flag").textContent = s.flagged; $("f-clr").textContent = s.cleared;
}

/* ---- rail rendering ---- */
function tickerRow(ev) {
  const d = document.createElement("div");
  d.className = "tk enter" + (ev.cand ? " cand" : "");
  const t = fmtTime(new Date((ev.ts || Date.now() / 1000) * 1000)).slice(3);
  d.innerHTML = ev.cand
    ? `<span class="t1">${t}</span><span class="t2">▸ ${esc(ev.label)}</span>`
    : `<span class="t1">${t}</span><span class="t2">${esc(ev.branch)}</span><span>${esc(ev.txn)}</span><span class="t4">${money0(ev.amount_cents)}</span>`;
  const tk = $("ticker"); tk.prepend(d); while (tk.children.length > 7) tk.lastChild.remove();
}
function badgeFor(k) { return k.status === "routed" ? "routed" : k.status === "dismissed" ? "dismissed" : "new"; }
function flagCard(k) {
  const d = document.createElement("div");
  d.className = "fcard enter" + (k.status !== "open" ? " processed" : "");
  d.id = "fcard_" + k.id;
  d.innerHTML = `<div class="fh"><div class="ft">${esc(k.title)}</div>${k.amount_cents ? `<div class="fa">${money0(k.amount_cents)}</div>` : ""}<span class="badge ${badgeFor(k)}">${badgeFor(k)}</span></div>
    <div class="fs">${esc(k.duty)} · ${esc(k.verdict_status)}${k.confidence ? ` · conf ${k.confidence.toFixed(2)}` : ""}</div>
    <div class="hint">▸ click to process in chat</div>`;
  d.onclick = () => openCase(k.id);
  return d;
}
function clearCard(k) {
  const d = document.createElement("div"); d.className = "ccard enter";
  d.innerHTML = `<b>${esc(k.title)}</b> — ${esc(k.verdict_status)}`;
  return d;
}
function addFlag(k) { $("flags").prepend(flagCard(k)); }
function addClear(k) { const c = $("clears"); c.prepend(clearCard(k)); while (c.children.length > 6) c.lastChild.remove(); }
function updateFlag(k) { const el = $("fcard_" + k.id); if (el) el.replaceWith(flagCard(k)); }

/* ---- SSE ---- */
function connect() {
  const es = new EventSource("/events");
  es.onopen = () => { $("conn").textContent = "streaming"; };
  es.onerror = () => { $("conn").textContent = "reconnecting"; };
  es.onmessage = m => {
    const ev = JSON.parse(m.data);
    if (ev.type === "snapshot") {
      paintKpis(ev.stats);
      $("flags").innerHTML = ""; ev.flags.forEach(addFlag);
      $("clears").innerHTML = ""; ev.clears.forEach(addClear);
    }
    else if (ev.type === "ticker") tickerRow(ev);
    else if (ev.type === "kpis") paintKpis(ev.stats);
    else if (ev.type === "case.flagged") addFlag(ev.case);
    else if (ev.type === "case.updated") updateFlag(ev.case);
    else if (ev.type === "case.cleared") addClear(ev.case);
  };
}
connect();

/* ---- chat primitives ---- */
const thread = $("thread");
const scrollDown = () => { thread.scrollTop = thread.scrollHeight; };
function userMsg(t) { const d = document.createElement("div"); d.className = "msg user enter"; d.textContent = t; thread.appendChild(d); scrollDown(); }
function sysMsg(html) { const d = document.createElement("div"); d.className = "msg sys enter"; d.innerHTML = html; thread.appendChild(d); scrollDown(); }
function pennyMsg(html) {
  const d = document.createElement("div"); d.className = "msg penny enter";
  d.innerHTML = `<div class="pav" aria-hidden="true"></div><div class="pbody">${html || ""}</div>`;
  thread.appendChild(d); scrollDown(); return d.querySelector(".pbody");
}
function traceLine(body, tool, text) {
  let ul = body.querySelector(".ptrace");
  if (!ul) { ul = document.createElement("ul"); ul.className = "ptrace"; body.appendChild(ul); }
  const li = document.createElement("li");
  li.innerHTML = (tool ? `<span class="tchip">${esc(tool)}</span> ` : "") + esc(text);
  ul.appendChild(li); scrollDown();
}

/* ---- streamed turn (shared by ask + why) ---- */
async function streamInto(body, url, payload) {
  const spin = document.createElement("div");
  spin.innerHTML = `<span class="dots" style="font-family:var(--mono);font-size:12px;color:var(--mc-accent-blue)">investigating</span>`;
  body.appendChild(spin); scrollDown();
  try {
    const res = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1);
        if (!line) continue;
        const ev = JSON.parse(line);
        if (ev.type === "trace") traceLine(body, ev.tool, ev.text);
        else if (ev.type === "sys") sysMsg(ev.html);
        else if (ev.type === "verdict") {
          spin.remove();
          const v = document.createElement("div"); v.className = "pverdict " + (ev.kind === "flag" ? "flag" : "clr");
          v.innerHTML = ev.html;  // server-provided, trusted
          body.appendChild(v); scrollDown();
        }
      }
    }
  } catch (e) {
    spin.remove();
    const v = document.createElement("div"); v.className = "pverdict flag";
    v.innerHTML = `<b>Connection problem:</b> ${esc(e.message)}`;
    body.appendChild(v); scrollDown();
  }
}
function askPenny(q) { userMsg(q); streamInto(pennyMsg(""), "/turn", { session_id: SESSION, text: q }); }

$("send").onclick = () => { const q = $("ask").value.trim(); if (q) { $("ask").value = ""; askPenny(q); } };
$("ask").addEventListener("keydown", e => { if (e.key === "Enter") $("send").onclick(); });
document.querySelectorAll(".chip").forEach(ch => ch.onclick = () => askPenny(ch.dataset.q));
$("injectBtn").onclick = () => fetch("/inject", { method: "POST" });

/* ---- case processing in chat ---- */
async function openCase(id) {
  const res = await fetch(`/cases/${id}/open`, { method: "POST" });
  if (!res.ok) return;
  const k = await res.json();
  if (k.status !== "open") { sysMsg(`Case already ${esc(k.status)} — ask Penny for an update if needed.`); return; }
  userMsg(`Process: ${k.title}`);
  const body = pennyMsg(`Here's the case with my evidence. <b>Confirm</b> routes it to ${esc(k.route_lane)} (graduated autonomy: ${esc(k.tier)}); <b>Dismiss</b> logs it as a decoy and feeds my ledger.`);
  const w = document.createElement("div"); w.className = "case"; w.dataset.id = k.id;
  const traceHtml = (k.trace || []).map(([t, x]) => `<li>${t ? `<span class="tchip">${esc(t)}</span> ` : ""}${esc(x)}</li>`).join("");
  w.innerHTML = `<div class="ch">${k.amount_cents ? `<div class="camt">${money0(k.amount_cents)}</div>` : ""}<div><div class="cttl">${esc(k.title)}</div><div class="csub">${esc(k.subtitle)}</div></div><div class="cduty">${esc(k.duty)}</div></div>
    ${k.confidence ? `<div class="conf"><div class="cl"><span>confidence</span><span class="tnum">${k.confidence.toFixed(2)}</span></div><div class="track"><div class="fill" style="width:${Math.round(k.confidence * 100)}%"></div></div></div>` : ""}
    <ul class="ptrace">${traceHtml}</ul>
    <div class="cacts">
      <button class="abtn pri" data-act="confirm">Confirm &amp; route → ${esc(k.route_lane)}</button><span class="tierchip">${esc(k.tier)}</span>
      <button class="abtn" data-act="dismiss">Dismiss — not a leak</button>
      <button class="abtn" data-act="why">Ask why</button>
    </div><div class="cstatus"></div>`;
  body.appendChild(w); scrollDown();
  w.querySelector("[data-act=confirm]").onclick = () => actCase(w, k, "confirm");
  w.querySelector("[data-act=dismiss]").onclick = () => actCase(w, k, "dismiss");
  w.querySelector("[data-act=why]").onclick = () => streamInto(pennyMsg(""), `/cases/${k.id}/why`, { session_id: SESSION });
}
async function actCase(w, k, action) {
  const res = await fetch(`/cases/${k.id}/${action}`, { method: "POST" });
  if (!res.ok) { sysMsg("Case already processed."); return; }
  const updated = await res.json();
  w.querySelectorAll(".abtn").forEach(b => b.disabled = true);
  const st = w.querySelector(".cstatus");
  if (action === "confirm") {
    st.className = "cstatus routed";
    st.innerHTML = `✓ Routed to <b>${esc(updated.route_lane)}</b> · ${esc(updated.tier)} · ${fmtTime(new Date())} — evidence packet attached, audit-logged.`;
    sysMsg(`Case routed → ${esc(updated.route_lane)} · awaiting ${updated.tier === "approval" ? "human approval" : "acknowledgement"}`);
  } else {
    st.className = "cstatus ok";
    st.innerHTML = `✓ Dismissed as not-a-leak · ${fmtTime(new Date())} — logged to the decision ledger; Penny won't re-flag this pattern without new evidence.`;
  }
  scrollDown();
}

/* ---- opening beat ---- */
fetch("/healthz").then(r => r.json()).then(h => { $("backend-chip").textContent = `backend: ${h.backend} · feed: ${h.feed} · session ${SESSION}`; });
sysMsg(`Session started · ${fmtDate(new Date())} — Penny watches the stream and answers here.`);
setTimeout(() => pennyMsg(`Morning. I’m watching all ten branches across six duties. Ask me anything — or click a <b>flagged case</b> in the rail and we’ll process it together right here.`), 500);
