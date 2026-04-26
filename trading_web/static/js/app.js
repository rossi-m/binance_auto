(function() {
    'use strict';

    // DOM refs
    const els = {
        statusDot: document.getElementById('status-dot'),
        statusText: document.getElementById('status-text'),
        pidLabel: document.getElementById('pid-label'),
        btnStart: document.getElementById('btn-start'),
        btnPause: document.getElementById('btn-pause'),
        btnStartMobile: document.getElementById('btn-start-mobile'),
        btnPauseMobile: document.getElementById('btn-pause-mobile'),
        todayPnl: document.getElementById('today-pnl'),
        monthPnl: document.getElementById('month-pnl'),
        yearPnl: document.getElementById('year-pnl'),
        totalPnl: document.getElementById('total-pnl'),
        tradeCount: document.getElementById('trade-count'),
        todayCount: document.getElementById('today-count'),
        summaryYear: document.getElementById('summary-year'),
        yearlySummary: document.getElementById('yearly-summary'),
        monthlySummary: document.getElementById('monthly-summary'),
        chartWinDays: document.getElementById('chart-win-days'),
        chartLossDays: document.getElementById('chart-loss-days'),
        chartBestDay: document.getElementById('chart-best-day'),
        logBox: document.getElementById('log-box'),
        autoscroll: document.getElementById('autoscroll'),
        tradesTbody: document.getElementById('trades-tbody'),
        tradesMobileList: document.getElementById('trades-mobile-list'),
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
        sectionToggles: Array.from(document.querySelectorAll('.section-toggle')),
    };

    let pnlChart = null;
    let statusTimer = null;
    let statsTimer = null;
    let tradesTimer = null;
    let logTimer = null;
    let lastTradesFetch = 0;
    let latestStats = null;
    const TRADES_REFRESH_MS = 2 * 60 * 60 * 1000; // 2 hours
    const LOG_REFRESH_MS = 3000; // 3秒刷新日志
    const MAX_LOG_LINES = 500;
    const MOBILE_BREAKPOINT = 480;
    const CHART_COLORS = {
        profit: '#22c55e',
        profitFill: 'rgba(34,197,94,0.18)',
        loss: '#ef4444',
        lossFill: 'rgba(239,68,68,0.18)',
        crossover: '#f59e0b',
        baseline: 'rgba(148,163,184,0.75)',
        gridLight: '#e5e7eb',
        gridDark: '#243041',
        pointBorderLight: '#ffffff',
        pointBorderDark: '#0f172a',
    };
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
        pnlChart.options.scales.y.grid = {
            color: (ctx) => chartGridColor(ctx.tick.value),
            lineWidth: (ctx) => Number(ctx.tick.value) === 0 ? 1.6 : 1,
        };
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

    function formatSignedValue(n) {
        if (n === undefined || n === null || Number.isNaN(Number(n))) return '--';
        const v = Number(n);
        const sign = v > 0 ? '+' : '';
        return `${sign}${fmtNum(v)}`;
    }

    function chartGridColor(value) {
        if (Number(value) === 0) return CHART_COLORS.baseline;
        return currentTheme === 'light' ? CHART_COLORS.gridLight : CHART_COLORS.gridDark;
    }

    function chartPointBorderColor() {
        return currentTheme === 'light' ? CHART_COLORS.pointBorderLight : CHART_COLORS.pointBorderDark;
    }

    function chartSegmentColor(startY, endY) {
        if (startY >= 0 && endY >= 0) return CHART_COLORS.profit;
        if (startY <= 0 && endY <= 0) return CHART_COLORS.loss;
        return CHART_COLORS.crossover;
    }

    function pointColor(value) {
        if (value > 0) return CHART_COLORS.profit;
        if (value < 0) return CHART_COLORS.loss;
        return CHART_COLORS.baseline;
    }

    function isMobileLayout() {
        return window.innerWidth <= MOBILE_BREAKPOINT;
    }

    function getSectionStorageKey(sectionKey) {
        return `section-collapsed:${sectionKey}`;
    }

    function setSectionCollapsed(section, collapsed) {
        if (!section) return;
        section.classList.toggle('is-collapsed', collapsed && isMobileLayout());
        const toggle = section.querySelector('.section-toggle');
        if (toggle) {
            toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
            const text = toggle.querySelector('.section-toggle-text');
            if (text) text.textContent = collapsed ? '展开' : '收起';
        }
    }

    function initSectionToggles() {
        const sections = Array.from(document.querySelectorAll('.collapsible-section'));
        sections.forEach(section => {
            const sectionKey = section.dataset.sectionKey;
            const stored = localStorage.getItem(getSectionStorageKey(sectionKey));
            const shouldCollapse = stored === null ? sectionKey === 'logs' : stored === '1';
            setSectionCollapsed(section, shouldCollapse);
        });

        els.sectionToggles.forEach(toggle => {
            toggle.addEventListener('click', () => {
                const target = document.getElementById(toggle.dataset.target);
                if (!target) return;
                const sectionKey = target.dataset.sectionKey;
                const nextCollapsed = !target.classList.contains('is-collapsed');
                localStorage.setItem(getSectionStorageKey(sectionKey), nextCollapsed ? '1' : '0');
                setSectionCollapsed(target, nextCollapsed);
            });
        });

        window.addEventListener('resize', () => {
            sections.forEach(section => {
                const sectionKey = section.dataset.sectionKey;
                const stored = localStorage.getItem(getSectionStorageKey(sectionKey));
                const shouldCollapse = stored === null ? sectionKey === 'logs' : stored === '1';
                setSectionCollapsed(section, shouldCollapse);
            });
        });
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
            els.btnStartMobile.disabled = running;
            els.btnPauseMobile.disabled = !running;
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
                    borderColor: CHART_COLORS.profit,
                    backgroundColor: CHART_COLORS.profitFill,
                    fill: {
                        target: 'origin',
                        above: CHART_COLORS.profitFill,
                        below: CHART_COLORS.lossFill,
                    },
                    segment: {
                        borderColor: (ctx) => chartSegmentColor(ctx.p0.parsed.y, ctx.p1.parsed.y),
                    },
                    tension: 0.36,
                    borderWidth: 3,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    pointBorderWidth: 2,
                    pointBorderColor: chartPointBorderColor(),
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
                            title: (items) => items[0] ? `日期: ${items[0].label}` : '',
                            label: (ctx) => `净利润: ${formatSignedValue(ctx.parsed.y)} USDT`
                        }
                    }
                },
                scales: {
                    x: { grid: { display: false } },
                    y: {
                        grid: {
                            color: (ctx) => chartGridColor(ctx.tick.value),
                            lineWidth: (ctx) => Number(ctx.tick.value) === 0 ? 1.6 : 1,
                        },
                        ticks: {
                            callback: (v) => formatSignedValue(v)
                        }
                    }
                }
            }
        });
    }

    function updateChartSummary(values, rawChartRows) {
        const profitDays = values.filter(v => v > 0).length;
        const lossDays = values.filter(v => v < 0).length;
        const bestDay = rawChartRows.reduce((best, item) => {
            if (!best || item.pnl > best.pnl) return item;
            return best;
        }, null);

        els.chartWinDays.textContent = profitDays;
        els.chartWinDays.className = 'chart-metric-value positive';
        els.chartLossDays.textContent = lossDays;
        els.chartLossDays.className = 'chart-metric-value negative';

        if (bestDay) {
            els.chartBestDay.textContent = `${bestDay.date.slice(5)} · ${formatSignedValue(bestDay.pnl)}`;
            els.chartBestDay.className = 'chart-metric-value' + (bestDay.pnl >= 0 ? ' positive' : ' negative');
        } else {
            els.chartBestDay.textContent = '--';
            els.chartBestDay.className = 'chart-metric-value';
        }
    }

    function updateChartScale(values) {
        if (!values.length) return;
        const minValue = Math.min(...values, 0);
        const maxValue = Math.max(...values, 0);
        const span = Math.max(maxValue - minValue, Math.abs(maxValue), Math.abs(minValue), 1);
        const padding = Math.max(span * 0.14, 1);
        pnlChart.options.scales.y.suggestedMin = minValue - padding;
        pnlChart.options.scales.y.suggestedMax = maxValue + padding;
    }

    function syncSummaryYearOptions(years, fallbackYear) {
        const options = Array.isArray(years) ? years : [];
        const currentValue = els.summaryYear.value;
        const nextValue = options.includes(currentValue)
            ? currentValue
            : (options.includes(fallbackYear) ? fallbackYear : (options[0] || ''));

        els.summaryYear.innerHTML = options.map(year => (
            `<option value="${esc(year)}">${esc(year)} 年</option>`
        )).join('');
        els.summaryYear.disabled = options.length === 0;

        if (nextValue) {
            els.summaryYear.value = nextValue;
        }
    }

    function renderSummaryList(container, rows, labelBuilder) {
        if (!rows || rows.length === 0) {
            container.innerHTML = '<div class="summary-empty">暂无数据</div>';
            return;
        }

        container.innerHTML = rows.map(row => {
            const pnl = Number(row.pnl || 0);
            const pnlClass = pnl > 0 ? 'positive' : (pnl < 0 ? 'negative' : '');
            return `<div class="summary-row">
                <div class="summary-name">${esc(labelBuilder(row))}</div>
                <div class="summary-trades">${esc(`${row.trade_count ?? 0} 笔`)}</div>
                <div class="summary-pnl ${pnlClass}">${esc(formatSignedValue(pnl))}</div>
            </div>`;
        }).join('');
    }

    function renderPerformanceSummary(data) {
        latestStats = data;
        const years = data.available_years || [];
        syncSummaryYearOptions(years, data.current_year);

        renderSummaryList(
            els.yearlySummary,
            data.yearly_summary || [],
            row => `${row.year} 年`
        );

        const selectedYear = els.summaryYear.value || data.current_year || '';
        const monthlyMap = data.monthly_summary_by_year || {};
        renderSummaryList(
            els.monthlySummary,
            monthlyMap[selectedYear] || [],
            row => `${row.month.slice(5)} 月`
        );
    }

    async function updateStats() {
        try {
            const data = await apiGet('/api/stats');
            setPnlEl(els.todayPnl, data.today_pnl);
            setPnlEl(els.monthPnl, data.month_pnl);
            setPnlEl(els.yearPnl, data.year_pnl);
            setPnlEl(els.totalPnl, data.total_pnl);
            els.tradeCount.textContent = data.trade_count ?? '--';
            els.todayCount.textContent = data.today_trade_count ?? '--';
            renderPerformanceSummary(data);

            // Update chart
            if (pnlChart && data.daily_chart) {
                const labels = data.daily_chart.map(d => d.date.slice(5)); // MM-DD
                const values = data.daily_chart.map(d => d.pnl);
                pnlChart.data.labels = labels;
                pnlChart.data.datasets[0].data = values;
                pnlChart.data.datasets[0].pointBackgroundColor = values.map(pointColor);
                pnlChart.data.datasets[0].pointBorderColor = values.map(() => chartPointBorderColor());
                updateChartScale(values);
                updateChartSummary(values, data.daily_chart);
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
        const previousScrollTop = els.logBox.scrollTop;
        const previousBottomOffset = els.logBox.scrollHeight - previousScrollTop - els.logBox.clientHeight;

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
        } else {
            const nextScrollTop = els.logBox.scrollHeight - els.logBox.clientHeight - previousBottomOffset;
            els.logBox.scrollTop = Math.max(0, nextScrollTop);
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
            els.tradesMobileList.innerHTML = '<div class="trade-mobile-empty">暂无数据</div>';
            return;
        }
        const tableHtml = rows.map(r => {
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
        const mobileHtml = rows.map(r => renderTradeCard(r)).join('');
        els.tradesTbody.innerHTML = tableHtml;
        els.tradesMobileList.innerHTML = mobileHtml;
    }

    function renderTradeCard(row) {
        const pnl = parseFloat(row['净利润(USDT)'] || 0);
        const pnlClass = pnl > 0 ? 'positive' : (pnl < 0 ? 'negative' : '');
        const directionClass = getDirectionClass(row['趋势方向'] || '');
        const outcomeClass = getOutcomeClass(row['是否盈利'] || '');
        const outcomeText = row['是否盈利'] === 'True' ? '盈利' : (row['是否盈利'] === 'False' ? '亏损' : (row['是否盈利'] || '未标记'));

        return `<article class="trade-card">
            <div class="trade-card-head">
                <div class="trade-card-title">
                    <div class="trade-badges">
                        <span class="trade-badge ${directionClass}">${esc(row['趋势方向'] || '未知方向')}</span>
                        <span class="trade-badge ${outcomeClass}">${esc(outcomeText)}</span>
                    </div>
                    <div class="trade-time">建仓: ${esc(row['建仓时间'] || '--')}</div>
                </div>
                <div>
                    <div class="trade-pnl ${pnlClass}">${esc(row['净利润(USDT)'] || '0')}</div>
                    <div class="trade-pnl-note">净利润 (USDT)</div>
                </div>
            </div>
            <div class="trade-grid">
                ${renderTradeField('平仓时间', row['平仓时间'])}
                ${renderTradeField('持仓秒数', row['持仓秒数'])}
                ${renderTradeField('点数盈亏', row['点数盈亏'])}
                ${renderTradeField('手续费', row['手续费'])}
                ${renderTradeField('入场原因', row['入场原因'], true)}
                ${renderTradeField('平仓原因', row['平仓原因'], true)}
            </div>
        </article>`;
    }

    function renderTradeField(label, value, wide = false) {
        return `<div class="trade-field${wide ? ' wide' : ''}">
            <span class="trade-field-label">${esc(label)}</span>
            <span class="trade-field-value">${esc(value || '--')}</span>
        </div>`;
    }

    function getDirectionClass(direction) {
        const normalized = String(direction || '').toLowerCase();
        if (normalized.includes('多') || normalized.includes('long') || normalized.includes('buy')) {
            return 'direction-long';
        }
        if (normalized.includes('空') || normalized.includes('short') || normalized.includes('sell')) {
            return 'direction-short';
        }
        return '';
    }

    function getOutcomeClass(outcome) {
        if (outcome === 'True') return 'outcome-win';
        if (outcome === 'False') return 'outcome-loss';
        return '';
    }

    function esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    // ---------- Controls ----------

    els.btnStartMobile.addEventListener('click', () => {
        els.btnStart.click();
    });

    els.btnPauseMobile.addEventListener('click', () => {
        els.btnPause.click();
    });

    // Start strategy with email verification
    els.btnStart.addEventListener('click', async () => {
        els.btnStart.disabled = true;
        els.btnStartMobile.disabled = true;
        els.startModalError.textContent = '';
        els.startVerifyInput.value = '';
        try {
            await apiPost('/api/start-request');
            els.startModalOverlay.classList.remove('hidden');
            els.startVerifyInput.focus();
        } catch (e) {
            alert('请求启动失败: ' + e.message);
            els.btnStart.disabled = false;
            els.btnStartMobile.disabled = false;
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

    els.summaryYear.addEventListener('change', () => {
        if (latestStats) {
            renderPerformanceSummary(latestStats);
        }
    });

    // ---------- Init ----------

    function init() {
        initTheme();
        initDailyEmoji();
        initSectionToggles();
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
