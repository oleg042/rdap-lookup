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
<title>RDAP Lookup Monitor</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; padding: 24px; }
  h1 { font-size: 1.5rem; margin-bottom: 20px; color: #58a6ff; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .card .label { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; }
  .card .value { font-size: 1.75rem; font-weight: 700; margin-top: 4px; }
  .progress-bar { width: 100%; height: 24px; background: #21262d; border-radius: 12px; overflow: hidden; margin-bottom: 24px; }
  .progress-fill { height: 100%; background: linear-gradient(90deg, #238636, #2ea043); transition: width 0.5s ease; border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 0.75rem; font-weight: 600; min-width: 40px; }
  .controls { margin-bottom: 24px; display: flex; gap: 12px; }
  button { padding: 10px 24px; border: none; border-radius: 6px; font-size: 0.9rem; font-weight: 600; cursor: pointer; transition: opacity 0.2s; }
  button:hover { opacity: 0.85; }
  button:disabled { opacity: 0.4; cursor: not-allowed; }
  .btn-start { background: #238636; color: #fff; }
  .btn-stop { background: #da3633; color: #fff; }
  .status { display: inline-block; padding: 4px 12px; border-radius: 12px; font-size: 0.8rem; font-weight: 600; margin-bottom: 16px; }
  .status.running { background: #0d4429; color: #3fb950; }
  .status.stopped { background: #3d1f1f; color: #f85149; }
  .logs { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 12px; height: 320px; overflow-y: auto; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8rem; line-height: 1.6; }
  .logs div { color: #8b949e; }
  .logs div:last-child { color: #e1e4e8; }
  #eta { color: #d2a8ff; }
</style>
</head>
<body>
<h1>RDAP Domain Registration Lookup</h1>

<div id="status" class="status stopped">STOPPED</div>

<div class="progress-bar"><div id="bar" class="progress-fill" style="width:0%">0%</div></div>

<div class="grid">
  <div class="card"><div class="label">Done</div><div class="value" id="done">-</div></div>
  <div class="card"><div class="label">Remaining</div><div class="value" id="remaining">-</div></div>
  <div class="card"><div class="label">Total</div><div class="value" id="total">-</div></div>
  <div class="card"><div class="label">Rate</div><div class="value" id="rate">-</div></div>
  <div class="card"><div class="label">Session Updated</div><div class="value" id="session">-</div></div>
  <div class="card"><div class="label">ETA</div><div class="value" id="eta">-</div></div>
</div>

<div id="errorBanner" style="display:none; background:#3d1f1f; border:1px solid #f85149; color:#f85149; padding:12px; border-radius:8px; margin-bottom:16px; font-size:0.9rem;"></div>

<div class="controls">
  <button class="btn-start" id="startBtn" onclick="doStart()">Start</button>
  <button class="btn-stop" id="stopBtn" onclick="doStop()" disabled>Stop</button>
</div>

<h2 style="font-size:1rem; margin-bottom:8px; color:#8b949e;">Live Logs</h2>
<div class="logs" id="logs"></div>

<script>
function fmt(n) { return n === null || n === undefined ? '-' : n.toLocaleString(); }

function formatETA(remaining, rate) {
  if (!rate || rate <= 0 || !remaining) return '-';
  const secs = remaining / rate;
  if (secs < 3600) return Math.round(secs / 60) + 'm';
  return (secs / 3600).toFixed(1) + 'h';
}

async function refresh() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    document.getElementById('done').textContent = fmt(d.done);
    document.getElementById('remaining').textContent = fmt(d.remaining);
    document.getElementById('total').textContent = fmt(d.total);
    document.getElementById('rate').textContent = d.rate ? d.rate + '/s' : '-';
    document.getElementById('session').textContent = fmt(d.total_updated_this_session);
    document.getElementById('eta').textContent = formatETA(d.remaining, d.rate);
    const pct = d.pct || 0;
    document.getElementById('bar').style.width = pct + '%';
    document.getElementById('bar').textContent = pct.toFixed(1) + '%';
    const st = document.getElementById('status');
    st.textContent = d.running ? 'RUNNING' : 'STOPPED';
    st.className = 'status ' + (d.running ? 'running' : 'stopped');
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
  await fetch('/api/start', {method:'POST'});
  refresh(); refreshLogs();
}

async function doStop() {
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
