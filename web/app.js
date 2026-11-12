"use strict";
const $ = (selector) => document.querySelector(selector);
const form = $("#settings-form");
let latest = null;
let initialized = false;
let busy = false;
let history = Array(60).fill(0);
let noticeTimer;
let disconnected = false;

function notice(message, error = false, persistent = false) {
  clearTimeout(noticeTimer);
  const element = $("#notice");
  element.textContent = message;
  element.classList.toggle("error", error);
  element.hidden = false;
  if (!persistent) noticeTimer = setTimeout(() => { element.hidden = true; }, 6000);
}

async function api(path, body) {
  const response = await fetch(path, {
    cache: "no-store", credentials: "same-origin",
    ...(body === undefined ? {} : {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body)}),
    signal: AbortSignal.timeout(8000),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "请求失败，请稍后重试");
  return data;
}

function volume(value) {
  if (value >= 1024 ** 3) return [(value / 1024 ** 3).toFixed(2), "GiB"];
  return [(value / 1024 ** 2).toFixed(2), "MiB"];
}

function duration(value) {
  const seconds = Math.max(0, Math.ceil(value));
  if (seconds >= 3600) return `${Math.floor(seconds / 3600)} 小时 ${Math.floor(seconds % 3600 / 60)} 分钟`;
  if (seconds >= 60) return `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`;
  return `${seconds} 秒`;
}

function syncFields(settings) {
  $("#urls").value = settings.urls.join("\n");
  form.elements.mode.value = settings.mode;
  ["run_seconds", "pause_seconds", "rate_mbps", "daily_limit_gib", "schedule_start", "schedule_end"].forEach(name => { form.elements[name].value = settings[name]; });
  $("#schedule-enabled").checked = settings.schedule_enabled;
  updateDependencies();
}

function updateDependencies() {
  const intermittent = form.elements.mode.value === "intermittent";
  $("#cycle-fields").classList.toggle("inactive", !intermittent);
  $("#run-seconds").disabled = !intermittent;
  $("#pause-seconds").disabled = !intermittent;
  const scheduled = $("#schedule-enabled").checked;
  $(".schedule-fields").classList.toggle("inactive", !scheduled);
  $("#schedule-start").disabled = !scheduled;
  $("#schedule-end").disabled = !scheduled;
}

function render(data) {
  latest = data;
  $("#connection").textContent = "服务已连接";
  $("#connection").className = "connection online";
  $("#speed").textContent = data.rate_mbps.toFixed(2);
  const [daily, dailyUnit] = volume(data.stats.daily_bytes);
  const [total, totalUnit] = volume(data.stats.total_bytes);
  $("#daily").textContent = daily;
  $("#daily-unit").textContent = dailyUnit;
  $("#total").textContent = total;
  $("#total-unit").textContent = totalUnit;
  $("#quota-caption").textContent = data.settings.daily_limit_gib ? `每日上限 ${data.settings.daily_limit_gib} GiB · 00:00 重置` : "每日 00:00 重置 · 北京时间";
  const labels = {stopped: "已停止", missing_source: "待配置", waiting: "等待定时", quota: "已达上限", paused: "间歇暂停", running: "运行中"};
  $("#state-label").textContent = data.retry_seconds && data.state === "running" ? "等待重试" : labels[data.state] || data.state;
  let detail = "点击启动，按保存的设置运行";
  if (data.state === "stopped" && data.connection_open) detail = "正在结束当前连接，请稍候";
  else if (data.state === "running") detail = data.last_error ? `${data.retry_seconds} 秒后重试下载源` : data.settings.mode === "intermittent" ? `${duration(data.next_change_seconds)}后暂停` : "持续接收 · 不保存下载文件";
  else if (["waiting", "quota", "paused"].includes(data.state)) detail = `${duration(data.next_change_seconds)}后恢复`;
  $("#state-detail").textContent = detail;
  const toggle = $("#toggle-run");
  toggle.textContent = data.enabled ? "停止任务  □" : "启动任务  ↗";
  toggle.classList.toggle("stop", data.enabled);
  toggle.disabled = busy;
  if (!initialized) { syncFields(data.settings); initialized = true; }
  history = data.history;
  $("#chart-empty").hidden = history.some(value => value > 0);
  drawChart();
  const items = data.events.map(event => {
    const li = document.createElement("li");
    const timestamp = document.createElement("time");
    timestamp.textContent = event.time;
    const message = document.createElement("span");
    message.textContent = event.message;
    li.append(timestamp, message);
    return li;
  });
  $("#event-list").replaceChildren(...items);
}

function drawChart() {
  const canvas = $("#chart");
  const {width, height} = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.round(width * ratio);
  canvas.height = Math.round(height * ratio);
  const context = canvas.getContext("2d");
  context.scale(ratio, ratio);
  const left = 43, top = 10, bottom = height - 10, right = width - 3;
  const peak = Math.max(10, ...history);
  const ceiling = Math.ceil(peak / 5) * 5;
  context.font = "9px -apple-system, sans-serif";
  for (let index = 0; index <= 4; index++) {
    const y = top + (bottom - top) * index / 4;
    context.strokeStyle = "#edf2f0";
    context.lineWidth = 1;
    context.beginPath(); context.moveTo(left, y); context.lineTo(right, y); context.stroke();
    context.fillStyle = "#a0ada8";
    context.fillText((ceiling * (1 - index / 4)).toFixed(ceiling >= 100 ? 0 : 1), 1, y + 3);
  }
  const points = history.map((value, index) => [left + (right - left) * index / 59, bottom - (bottom - top) * value / ceiling]);
  const gradient = context.createLinearGradient(0, top, 0, bottom);
  gradient.addColorStop(0, "#3b9d7c35"); gradient.addColorStop(1, "#3b9d7c02");
  context.beginPath(); context.moveTo(left, bottom);
  points.forEach(([x,y]) => context.lineTo(x,y));
  context.lineTo(right, bottom); context.closePath(); context.fillStyle = gradient; context.fill();
  context.beginPath(); points.forEach(([x,y], index) => index ? context.lineTo(x,y) : context.moveTo(x,y));
  context.lineWidth = 1.8; context.strokeStyle = "#409e80"; context.stroke();
}

form.addEventListener("input", () => {
  $("#dirty-note").textContent = "有未保存的修改";
  $("#dirty-note").classList.add("dirty");
  updateDependencies();
});

form.addEventListener("submit", async event => {
  event.preventDefault();
  if (busy) return;
  const settings = {
    urls: $("#urls").value.split("\n").map(value => value.trim()).filter(Boolean),
    mode: form.elements.mode.value,
    run_seconds: Number($("#run-seconds").value), pause_seconds: Number($("#pause-seconds").value),
    rate_mbps: Number($("#rate-mbps").value), daily_limit_gib: Number($("#daily-limit").value),
    schedule_enabled: $("#schedule-enabled").checked,
    schedule_start: $("#schedule-start").value, schedule_end: $("#schedule-end").value,
  };
  busy = true; $("#save-settings").disabled = true; $("#toggle-run").disabled = true;
  try {
    const data = await api("/api/settings", settings);
    render(data);
    $("#dirty-note").textContent = "设置已同步";
    $("#dirty-note").classList.remove("dirty");
    notice("设置已保存。");
  } catch (error) { notice(error.message, true); }
  finally { busy = false; $("#save-settings").disabled = false; $("#toggle-run").disabled = !latest; }
});

$("#toggle-run").addEventListener("click", async () => {
  if (busy || !latest) return;
  if (!latest.enabled && $("#dirty-note").classList.contains("dirty")) {
    notice("请先保存修改，再启动任务。", true);
    return;
  }
  busy = true; $("#toggle-run").disabled = true;
  try { render(await api(latest.enabled ? "/api/stop" : "/api/start", {})); }
  catch (error) { notice(error.message, true); }
  finally { busy = false; $("#toggle-run").disabled = false; }
});

async function poll() {
  try {
    if (!busy) {
      render(await api("/api/status"));
      if (disconnected) $("#notice").hidden = true;
      disconnected = false;
    }
  }
  catch (error) {
    disconnected = true;
    $("#connection").textContent = "服务连接中断";
    $("#connection").className = "connection offline";
    $("#toggle-run").disabled = true;
    notice(`无法连接服务：${error.message}。页面会自动重连。`, true, true);
  } finally { setTimeout(poll, 1000); }
}

new ResizeObserver(drawChart).observe($("#chart").parentElement);
poll();
