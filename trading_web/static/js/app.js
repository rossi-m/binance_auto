(function() {
    'use strict';

    // DOM refs
    const els = {
        statusDot: document.getElementById('status-dot'),
        statusText: document.getElementById('status-text'),
        pidLabel: document.getElementById('pid-label'),
        btnStart: document.getElementById('btn-start'),
        btnPause: document.getElementById('btn-pause'),
        todayPnl: document.getElementById('today-pnl'),
        totalPnl: document.getElementById('total-pnl'),
        tradeCount: document.getElementById('trade-count'),
        todayCount: document.getElementById('today-count'),
        logBox: document.getElementById('log-box'),
        autoscroll: document.getElementById('autoscroll'),
        tradesTbody: document.getElementById('trades-tbody'),
        tradesFile: document.getElementById('trades-file'),
        tradesUpdated: document.getElementById('trades-updated'),
        modalOverlay: document.getElementById('modal-overlay'),
        verifyInput: document.getElementById('verify-input'),
        modalCancel: document.getElementById('modal-cancel'),
        modalConfirm: document.getElementById('modal-confirm'),
        modalError: document.getElementById('modal-error'),
        chartCanvas: document.getElementById('pnl-chart'),
        // Start verification
        startModalOverlay: document.getElementById('start-modal-overlay'),
        startVerifyInput: document.getElementById('start-verify-input'),
        startModalCancel: document.getElementById('start-modal-cancel'),
        startModalConfirm: document.getElementById('start-modal-confirm'),
        startModalError: document.getElementById('start-modal-error'),
        // Theme
        btnTheme: document.getElementById('btn-theme'),
        themeIcon: document.getElementById('theme-icon'),
        themeLabel: document.getElementById('theme-label'),
        // Daily emoji
        dailyEmoji: document.getElementById('daily-emoji'),
    };

    let pnlChart = null;
    let statusTimer = null;
    let statsTimer = null;
    let tradesTimer = null;
    let logTimer = null;
    let lastTradesFetch = 0;
    const TRADES_REFRESH_MS = 2 * 60 * 60 * 1000; // 2 hours
    const LOG_REFRESH_MS = 3000; // 3秒刷新日志
    const MAX_LOG_LINES = 15;
    let currentTheme = 'dark';
    let userScrolled = false; // 用户手动滚动时暂停自动滚动

    // ---------- Theme ----------

    function initTheme() {
        const saved = localStorage.getItem('theme');
        if (saved === 'light' || saved === 'dark') {
            currentTheme = saved;
        }
        applyTheme();
        els.btnTheme.addEventListener('click', () => {
            currentTheme = currentTheme === 'dark' ? 'light' : 'dark';
            localStorage.setItem('theme', currentTheme);
            applyTheme();
            updateChartTheme();
        });
    }

    function applyTheme() {
        document.documentElement.setAttribute('data-theme', currentTheme);
        if (currentTheme === 'light') {
            els.themeIcon.textContent = '☀️';
            els.themeLabel.textContent = '浅色';
        } else {
            els.themeIcon.textContent = '🌙';
            els.themeLabel.textContent = '深色';
        }
    }

    function updateChartTheme() {
        if (!pnlChart) return;
        const isLight = currentTheme === 'light';
        Chart.defaults.color = isLight ? '#6b7280' : '#8892a0';
        Chart.defaults.borderColor = isLight ? '#e5e7eb' : '#22262e';
        pnlChart.options.scales.x.grid = { display: false };
        pnlChart.options.scales.y.grid = { color: isLight ? '#e5e7eb' : '#22262e' };
        pnlChart.update();
    }

    // ---------- Daily Emoji ----------

    function initDailyEmoji() {
        const emojis = [
            { label: '开心', emoji: '😊' },
            { label: '难过', emoji: '😢' },
            { label: '幸福', emoji: '🥰' },
            { label: '衰', emoji: '😫' },
        ];
        // Use date as seed for daily random
        const today = new Date();
        const seed = today.getFullYear() * 10000 + (today.getMonth() + 1) * 100 + today.getDate();
        const index = seed % emojis.length;
        els.dailyEmoji.textContent = emojis[index].emoji;
        els.dailyEmoji.title = emojis[index].label;
    }

    // ---------- Utils ----------

    function fmtNum(n) {
        if (n === undefined || n === null) return '--';
        const v = Number(n);
        if (isNaN(v)) return '--';
        return v.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function setPnlEl(el, val) {
        el.textContent = fmtNum(val);
        el.classList.remove('positive', 'negative');
        if (typeof val === 'number') {
            if (val > 0) el.classList.add('positive');
            else if (val < 0) el.classList.add('negative');
        }
    }

    function formatDateTime(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        if (isNaN(d)) return iso;
        return d.toLocaleString('zh-CN');
    }

    // ---------- API ----------

    async function apiGet(path) {
        const r = await fetch(path);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
    }

    async function apiPost(path, body) {
        const r = await fetch(path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: body ? JSON.stringify(body) : undefined
        });
        const data = await r.json().catch(() => ({}));
        if (!r.ok || !data.success) {
            throw new Error(data.error || `HTTP ${r.status}`);
        }
        return data;
    }

    // ---------- Status ----------

    async function updateStatus() {
        try {
            const data = await apiGet('/api/status');
            const running = data.running;
            els.statusDot.className = 'dot ' + (running ? 'running' : 'stopped');
            els.statusText.textContent = running ? '运行中' : '已停止';
            els.pidLabel.textContent = running && data.pid ? `PID: ${data.pid}` : '';
            els.btnStart.disabled = running;
            els.btnPause.disabled = !running;
        } catch (e) {
            console.error('status error', e);
        }
    }

    // ---------- Stats & Chart ----------

    function initChart() {
        const isLight = currentTheme === 'light';
        Chart.defaults.color = isLight ? '#6b7280' : '#8892a0';
        Chart.defaults.borderColor = isLight ? '#e5e7eb' : '#22262e';
        const ctx = els.chartCanvas.getContext('2d');
        pnlChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '净利润 (USDT)',
                    data: [],
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59,130,246,0.1)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            label: (ctx) => `净利润: ${fmtNum(ctx.parsed.y)} USDT`
                        }
                    }
                },
                scales: {
                    x: { grid: { display: false } },
                    y: {
                        grid: { color: isLight ? '#e5e7eb' : '#22262e' },
                        ticks: {
                            callback: (v) => fmtNum(v)
                        }
                    }
                }
            }
        });
    }

    async function updateStats() {
        try {
            const data = await apiGet('/api/stats');
            setPnlEl(els.todayPnl, data.today_pnl);
            setPnlEl(els.totalPnl, data.total_pnl);
            els.tradeCount.textContent = data.trade_count ?? '--';
            els.todayCount.textContent = data.today_trade_count ?? '--';

            // Update chart
            if (pnlChart && data.daily_chart) {
                const labels = data.daily_chart.map(d => d.date.slice(5)); // MM-DD
                const values = data.daily_chart.map(d => d.pnl);
                pnlChart.data.labels = labels;
                pnlChart.data.datasets[0].data = values;
                // Dynamic color based on cumulative sign
                const total = values.reduce((a, b) => a + b, 0);
                const color = total >= 0 ? '#22c55e' : '#ef4444';
                pnlChart.data.datasets[0].borderColor = color;
                pnlChart.data.datasets[0].backgroundColor = total >= 0
                    ? 'rgba(34,197,94,0.1)' : 'rgba(239,68,68,0.1)';
                pnlChart.update();
            }
        } catch (e) {
            console.error('stats error', e);
        }
    }

    // ---------- Logs (Polling) ----------

    function connectLogs() {
        // 用轮询方式直接加载日志文件内容，避免SSE丢失数据
        updateLogs();
        logTimer = setInterval(updateLogs, LOG_REFRESH_MS);

        // 检测用户手动滚动，暂停自动滚动
        els.logBox.addEventListener('scroll', () => {
            const atBottom = els.logBox.scrollHeight - els.logBox.scrollTop - els.logBox.clientHeight < 30;
            if (!atBottom) {
                userScrolled = true;
            } else {
                userScrolled = false;
            }
        });
    }

    async function updateLogs() {
        try {
            const data = await apiGet('/api/log-content?lines=' + MAX_LOG_LINES);
            const lines = data.lines || [];
            renderLogs(lines);
        } catch (e) {
            console.error('logs error', e);
        }
    }

    function renderLogs(lines) {
        // 只在内容变化时更新DOM，避免闪烁
        const html = lines.map(text => {
            let color = '';
            if (text.includes('[ERR]')) color = 'color:#ef4444;';
            else if (text.includes('【已开仓】') || text.includes('开仓成功')) color = 'color:#22c55e;';
            else if (text.includes('【已平仓】') || text.includes('清仓成功')) color = 'color:#f59e0b;';
            return `<div class="log-line" style="${color}">${escHtml(text)}</div>`;
        }).join('');
        els.logBox.innerHTML = html;

        // 自动滚动到底部（除非用户手动滚动过）
        if (els.autoscroll.checked && !userScrolled) {
            els.logBox.scrollTop = els.logBox.scrollHeight;
        }
    }

    function escHtml(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    // ---------- Trades Table ----------

    async function updateTrades(force = false) {
        const now = Date.now();
        if (!force && now - lastTradesFetch < TRADES_REFRESH_MS) return;
        lastTradesFetch = now;

        try {
            const data = await apiGet('/api/trades');
            els.tradesFile.textContent = data.file || '';
            els.tradesUpdated.textContent = formatDateTime(data.updated_at);
            renderTrades(data.trades || []);
        } catch (e) {
            console.error('trades error', e);
        }
    }

    function renderTrades(rows) {
        if (!rows || rows.length === 0) {
            els.tradesTbody.innerHTML = '<tr><td colspan="10" class="empty">暂无数据</td></tr>';
            return;
        }
        const html = rows.map(r => {
            const pnl = parseFloat(r['净利润(USDT)'] || 0);
            const pnlClass = pnl > 0 ? 'positive' : (pnl < 0 ? 'negative' : '');
            const profitText = r['是否盈利'] === 'True' ? '是' : (r['是否盈利'] === 'False' ? '否' : r['是否盈利']);
            return `<tr>
                <td>${esc(r['建仓时间']||'')}</td>
                <td>${esc(r['趋势方向']||'')}</td>
                <td>${esc(r['入场原因']||'')}</td>
                <td>${esc(r['平仓时间']||'')}</td>
                <td>${esc(r['平仓原因']||'')}</td>
                <td>${esc(r['点数盈亏']||'')}</td>
                <td>${esc(r['手续费']||'')}</td>
                <td class="${pnlClass}">${esc(r['净利润(USDT)']||'')}</td>
                <td>${esc(profitText)}</td>
                <td>${esc(r['持仓秒数']||'')}</td>
            </tr>`;
        }).join('');
        els.tradesTbody.innerHTML = html;
    }

    function esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    // ---------- Controls ----------

    // Start strategy with email verification
    els.btnStart.addEventListener('click', async () => {
        els.btnStart.disabled = true;
        els.startModalError.textContent = '';
        els.startVerifyInput.value = '';
        try {
            await apiPost('/api/start-request');
            els.startModalOverlay.classList.remove('hidden');
            els.startVerifyInput.focus();
        } catch (e) {
            alert('请求启动失败: ' + e.message);
            els.btnStart.disabled = false;
        }
    });

    els.startModalCancel.addEventListener('click', () => {
        els.startModalOverlay.classList.add('hidden');
        updateStatus();
    });

    els.startModalConfirm.addEventListener('click', async () => {
        const code = els.startVerifyInput.value.trim();
        if (!/^\d{6}$/.test(code)) {
            els.startModalError.textContent = '请输入6位数字验证码';
            return;
        }
        els.startModalConfirm.disabled = true;
        try {
            await apiPost('/api/start-verify', { code });
            els.startModalOverlay.classList.add('hidden');
            await updateStatus();
        } catch (e) {
            els.startModalError.textContent = e.message;
        } finally {
            els.startModalConfirm.disabled = false;
        }
    });

    els.startVerifyInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') els.startModalConfirm.click();
    });

    // Pause strategy with email verification
    els.btnPause.addEventListener('click', async () => {
        els.modalError.textContent = '';
        els.verifyInput.value = '';
        try {
            await apiPost('/api/pause-request');
            els.modalOverlay.classList.remove('hidden');
            els.verifyInput.focus();
        } catch (e) {
            alert('请求暂停失败: ' + e.message);
        }
    });

    els.modalCancel.addEventListener('click', () => {
        els.modalOverlay.classList.add('hidden');
    });

    els.modalConfirm.addEventListener('click', async () => {
        const code = els.verifyInput.value.trim();
        if (!/^\d{6}$/.test(code)) {
            els.modalError.textContent = '请输入6位数字验证码';
            return;
        }
        els.modalConfirm.disabled = true;
        try {
            await apiPost('/api/pause-verify', { code });
            els.modalOverlay.classList.add('hidden');
            await updateStatus();
        } catch (e) {
            els.modalError.textContent = e.message;
        } finally {
            els.modalConfirm.disabled = false;
        }
    });

    els.verifyInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') els.modalConfirm.click();
    });

    // ---------- Init ----------

    function init() {
        initTheme();
        initDailyEmoji();
        initChart();
        connectLogs();
        updateStatus();
        updateStats();
        updateTrades(true);

        statusTimer = setInterval(updateStatus, 5000);
        statsTimer = setInterval(updateStats, 30000);
        tradesTimer = setInterval(() => updateTrades(false), 60000);
    }

    init();
})();
