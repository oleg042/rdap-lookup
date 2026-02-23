import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from rdap_lookup import worker

app = FastAPI(title="RDAP Lookup Monitor")

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RDAP Lookup</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #09090b; --surface: #18181b; --surface2: #27272a;
    --border: #3f3f46; --text: #fafafa; --muted: #a1a1aa;
    --green: #22c55e; --green-bg: #052e16; --green-border: #15803d;
    --red: #ef4444; --red-bg: #450a0a; --red-border: #b91c1c;
    --blue: #3b82f6; --purple: #a78bfa; --amber: #f59e0b;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }

  .container { max-width: 1100px; margin: 0 auto; padding: 40px 24px; }

  /* Header */
  .header { margin-bottom: 48px; }
  .header h1 { font-size: 2.5rem; font-weight: 900; letter-spacing: -0.03em; line-height: 1.1; }
  .header h1 span { background: linear-gradient(135deg, var(--blue), var(--purple)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
  .header p { color: var(--muted); font-size: 1rem; margin-top: 8px; }

  /* Status badge */
  .badge { display: inline-flex; align-items: center; gap: 8px; padding: 8px 20px; border-radius: 100px; font-size: 0.85rem; font-weight: 600; letter-spacing: 0.04em; margin-bottom: 32px; }
  .badge .dot { width: 10px; height: 10px; border-radius: 50%; }
  .badge.running { background: var(--green-bg); border: 1px solid var(--green-border); color: var(--green); }
  .badge.running .dot { background: var(--green); box-shadow: 0 0 12px var(--green); animation: pulse 2s infinite; }
  .badge.stopped { background: var(--red-bg); border: 1px solid var(--red-border); color: var(--red); }
  .badge.stopped .dot { background: var(--red); }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }

  /* Progress */
  .progress-section { margin-bottom: 40px; }
  .progress-header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 12px; }
  .progress-pct { font-size: 3.5rem; font-weight: 900; letter-spacing: -0.04em; }
  .progress-pct small { font-size: 1.5rem; color: var(--muted); font-weight: 600; }
  .progress-track { width: 100%; height: 16px; background: var(--surface2); border-radius: 100px; overflow: hidden; }
  .progress-fill { height: 100%; border-radius: 100px; background: linear-gradient(90deg, var(--blue), var(--purple), var(--green)); background-size: 200% 100%; animation: shimmer 3s linear infinite; transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1); }
  @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

  /* Stats grid */
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 40px; }
  @media (max-width: 700px) { .stats { grid-template-columns: 1fr 1fr; } }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 16px; padding: 24px; }
  .stat .label { font-size: 0.75rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }
  .stat .val { font-size: 2rem; font-weight: 800; letter-spacing: -0.03em; }
  .stat .val.accent-blue { color: var(--blue); }
  .stat .val.accent-green { color: var(--green); }
  .stat .val.accent-purple { color: var(--purple); }
  .stat .val.accent-amber { color: var(--amber); }

  /* Error banner */
  .error-banner { background: var(--red-bg); border: 1px solid var(--red-border); color: var(--red); padding: 16px 20px; border-radius: 12px; margin-bottom: 24px; font-weight: 500; display: none; }

  /* Controls */
  .controls { display: flex; gap: 12px; margin-bottom: 40px; }
  .btn { padding: 14px 36px; border: none; border-radius: 12px; font-family: 'Inter', sans-serif; font-size: 1rem; font-weight: 700; cursor: pointer; transition: all 0.2s; letter-spacing: -0.01em; }
  .btn:active { transform: scale(0.97); }
  .btn:disabled { opacity: 0.3; cursor: not-allowed; transform: none; }
  .btn-start { background: var(--green); color: #000; }
  .btn-start:hover:not(:disabled) { background: #16a34a; box-shadow: 0 0 30px rgba(34,197,94,0.3); }
  .btn-stop { background: var(--red); color: #fff; }
  .btn-stop:hover:not(:disabled) { background: #dc2626; box-shadow: 0 0 30px rgba(239,68,68,0.3); }

  /* Logs */
  .logs-section h2 { font-size: 1rem; font-weight: 700; color: var(--muted); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.06em; }
  .logs { background: var(--surface); border: 1px solid var(--border); border-radius: 16px; padding: 20px; height: 360px; overflow-y: auto; font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; line-height: 1.8; }
  .logs::-webkit-scrollbar { width: 6px; }
  .logs::-webkit-scrollbar-track { background: transparent; }
  .logs::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
  .logs div { color: var(--muted); white-space: nowrap; }
  .logs div:nth-last-child(-n+3) { color: var(--text); }
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1><span>RDAP</span> Domain Lookup</h1>
    <p>Registration date enrichment for 1.2M domains</p>
  </div>

  <div id="badge" class="badge stopped"><div class="dot"></div><span id="badgeText">STOPPED</span></div>

  <div class="progress-section">
    <div class="progress-header">
      <div class="progress-pct"><span id="pctNum">0.0</span><small>%</small></div>
    </div>
    <div class="progress-track"><div id="bar" class="progress-fill" style="width:0%"></div></div>
  </div>

  <div class="stats">
    <div class="stat"><div class="label">Dates Found</div><div class="val accent-green" id="done">-</div></div>
    <div class="stat"><div class="label">Checked</div><div class="val accent-blue" id="checked">-</div></div>
    <div class="stat"><div class="label">Remaining</div><div class="val" id="remaining">-</div></div>
    <div class="stat"><div class="label">Total Domains</div><div class="val" id="total">-</div></div>
    <div class="stat"><div class="label">Speed</div><div class="val accent-purple" id="rate">-</div></div>
    <div class="stat"><div class="label">Session Writes</div><div class="val" id="session">-</div></div>
    <div class="stat"><div class="label">ETA</div><div class="val accent-amber" id="eta">-</div></div>
    <div class="stat"><div class="label">Round</div><div class="val" id="round">-</div></div>
  </div>

  <div id="errorBanner" class="error-banner"></div>

  <div class="controls">
    <button class="btn btn-start" id="startBtn" onclick="doStart()">Start Lookup</button>
    <button class="btn btn-stop" id="stopBtn" onclick="doStop()" disabled>Stop</button>
  </div>

  <div class="logs-section">
    <h2>Live Logs</h2>
    <div class="logs" id="logs"></div>
  </div>

</div>

<script>
function fmt(n) { return n == null ? '-' : n.toLocaleString(); }

function formatETA(remaining, rate) {
  if (!rate || rate <= 0 || !remaining) return '-';
  const secs = remaining / rate;
  if (secs < 60) return Math.round(secs) + 's';
  if (secs < 3600) return Math.round(secs / 60) + 'm';
  const h = Math.floor(secs / 3600);
  const m = Math.round((secs % 3600) / 60);
  return h + 'h ' + m + 'm';
}

async function refresh() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    document.getElementById('done').textContent = fmt(d.done);
    document.getElementById('checked').textContent = fmt(d.checked);
    document.getElementById('remaining').textContent = fmt(d.remaining);
    document.getElementById('total').textContent = fmt(d.total);
    document.getElementById('rate').textContent = d.rate ? d.rate + '/s' : '-';
    document.getElementById('session').textContent = fmt(d.total_updated_this_session);
    document.getElementById('eta').textContent = formatETA(d.remaining, d.rate);
    document.getElementById('round').textContent = d.round || '-';
    const pct = d.pct || 0;
    document.getElementById('bar').style.width = pct + '%';
    document.getElementById('pctNum').textContent = pct.toFixed(1);
    const badge = document.getElementById('badge');
    const badgeText = document.getElementById('badgeText');
    badge.className = 'badge ' + (d.running ? 'running' : 'stopped');
    badgeText.textContent = d.running ? 'RUNNING' : 'STOPPED';
    document.getElementById('startBtn').disabled = d.running;
    document.getElementById('stopBtn').disabled = !d.running;
    const errEl = document.getElementById('errorBanner');
    if (d.error) { errEl.textContent = d.error; errEl.style.display = 'block'; }
    else { errEl.style.display = 'none'; }
  } catch(e) {}
}

async function refreshLogs() {
  try {
    const r = await fetch('/api/logs');
    const d = await r.json();
    const el = document.getElementById('logs');
    const atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 30;
    el.innerHTML = d.logs.map(l => '<div>' + l + '</div>').join('');
    if (atBottom) el.scrollTop = el.scrollHeight;
  } catch(e) {}
}

async function doStart() {
  document.getElementById('startBtn').disabled = true;
  await fetch('/api/start', {method:'POST'});
  refresh(); refreshLogs();
}

async function doStop() {
  document.getElementById('stopBtn').disabled = true;
  await fetch('/api/stop', {method:'POST'});
  refresh(); refreshLogs();
}

setInterval(refresh, 3000);
setInterval(refreshLogs, 2000);
refresh(); refreshLogs();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


@app.get("/api/status")
async def status():
    return JSONResponse(await worker.get_progress())


@app.get("/api/logs")
async def logs():
    return JSONResponse({"logs": list(worker.logs)})


@app.post("/api/start")
async def start():
    ok = worker.start()
    return JSONResponse({"started": ok})


@app.post("/api/stop")
async def stop():
    ok = worker.stop()
    return JSONResponse({"stopped": ok})


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})
