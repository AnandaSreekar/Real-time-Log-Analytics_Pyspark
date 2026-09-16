// Client application logic for PySpark Real-Time Analytics Dashboard

let timelineChart = null;
let statusChart = null;
let endpointsChart = null;

// Initialize Chart.js instances
function initCharts() {
  const timelineCtx = document.getElementById('timelineChart').getContext('2d');
  timelineChart = new Chart(timelineCtx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        {
          label: 'Requests / Batch',
          data: [],
          borderColor: '#06b6d4',
          backgroundColor: 'rgba(6, 182, 212, 0.1)',
          fill: true,
          tension: 0.35,
          yAxisID: 'y'
        },
        {
          label: 'Avg Latency (ms)',
          data: [],
          borderColor: '#f59e0b',
          backgroundColor: 'transparent',
          borderDash: [5, 5],
          tension: 0.35,
          yAxisID: 'y1'
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#9ca3af', maxTicksLimit: 8 }
        },
        y: {
          type: 'linear',
          position: 'left',
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#9ca3af' },
          title: { display: true, text: 'Requests', color: '#06b6d4' }
        },
        y1: {
          type: 'linear',
          position: 'right',
          grid: { drawOnChartArea: false },
          ticks: { color: '#9ca3af' },
          title: { display: true, text: 'Latency (ms)', color: '#f59e0b' }
        }
      },
      plugins: {
        legend: { labels: { color: '#e5e7eb' } }
      }
    }
  });

  const statusCtx = document.getElementById('statusChart').getContext('2d');
  statusChart = new Chart(statusCtx, {
    type: 'doughnut',
    data: {
      labels: ['2xx Success', '3xx Redirect', '4xx Client Err', '5xx Server Err'],
      datasets: [{
        data: [0, 0, 0, 0],
        backgroundColor: ['#10b981', '#3b82f6', '#f59e0b', '#ef4444'],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom',
          labels: { color: '#e5e7eb', boxWidth: 12 }
        }
      }
    }
  });

  const endpointsCtx = document.getElementById('endpointsChart').getContext('2d');
  endpointsChart = new Chart(endpointsCtx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Hits',
        data: [],
        backgroundColor: 'rgba(59, 130, 246, 0.7)',
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#9ca3af' }
        },
        y: {
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#e5e7eb', font: { family: 'monospace' } }
        }
      },
      plugins: {
        legend: { display: false }
      }
    }
  });
}

// Fetch Latest Metrics Snapshot
async function fetchLatestMetrics() {
  try {
    const res = await fetch('/api/metrics');
    const data = await res.json();

    if (data.status === 'WAITING') {
      document.getElementById('streamStatusText').innerText = 'WAITING FOR DATA';
      document.getElementById('pulseDot').style.background = '#f59e0b';
      return;
    }

    document.getElementById('streamStatusText').innerText = 'STREAMING LIVE';
    document.getElementById('pulseDot').style.background = '#10b981';
    document.getElementById('lastUpdated').innerText = `Batch #${data.batch_id} (${data.timestamp})`;

    // Update KPI cards
    document.getElementById('kpiRequests').innerText = data.total_requests;
    document.getElementById('kpiLatency').innerText = `${data.avg_response_time_ms} ms`;
    document.getElementById('kpiMaxLatency').innerText = `${data.max_response_time_ms} ms`;
    document.getElementById('kpi5xx').innerText = data.status_5xx;

    const errorRateEl = document.getElementById('kpiErrorRate');
    errorRateEl.innerText = `${data.error_rate_pct}%`;
    if (data.error_rate_pct >= 15.0) {
      errorRateEl.style.color = '#ef4444';
    } else if (data.error_rate_pct >= 5.0) {
      errorRateEl.style.color = '#f59e0b';
    } else {
      errorRateEl.style.color = '#10b981';
    }

    // Update Anomaly Banner
    const alertBanner = document.getElementById('alertBanner');
    if (data.alerts && data.alerts.length > 0) {
      const topAlert = data.alerts[0];
      alertBanner.className = `alert-banner ${topAlert.severity.toLowerCase()}`;
      document.getElementById('alertTitle').innerText = `[${topAlert.severity}] ${topAlert.type}: ${topAlert.message}`;
      document.getElementById('alertDesc').innerText = `Observed: ${topAlert.metric} (Threshold: ${topAlert.threshold})`;
      alertBanner.style.display = 'flex';
    } else {
      alertBanner.style.display = 'none';
    }

    // Update HTTP Status Doughnut
    if (statusChart) {
      statusChart.data.datasets[0].data = [
        data.status_2xx || 0,
        data.status_3xx || 0,
        data.status_4xx || 0,
        data.status_5xx || 0
      ];
      statusChart.update('none');
    }

    // Update Top Endpoints Chart
    if (endpointsChart && data.top_endpoints) {
      endpointsChart.data.labels = data.top_endpoints.map(e => e.endpoint);
      endpointsChart.data.datasets[0].data = data.top_endpoints.map(e => e.hits);
      endpointsChart.update('none');
    }

    // Update Live Log Table
    const logsBody = document.getElementById('logsTableBody');
    if (logsBody && data.recent_logs) {
      logsBody.innerHTML = data.recent_logs.map(log => {
        let codeClass = 'code-2xx';
        if (log.status_code >= 500) codeClass = 'code-5xx';
        else if (log.status_code >= 400) codeClass = 'code-4xx';
        else if (log.status_code >= 300) codeClass = 'code-3xx';

        return `
          <tr>
            <td>${log.timestamp.split(' ')[1]}</td>
            <td><code>${log.ip_address}</code></td>
            <td><span class="method-badge">${log.http_method}</span></td>
            <td><code>${log.endpoint}</code></td>
            <td><span class="status-badge-code ${codeClass}">${log.status_code}</span></td>
            <td>${log.response_time_ms} ms</td>
          </tr>
        `;
      }).join('');
    }

  } catch (err) {
    console.error('Failed to fetch latest metrics:', err);
  }
}

// Fetch Historical Trend Data
async function fetchHistory() {
  try {
    const res = await fetch('/api/history');
    const history = await res.json();
    if (timelineChart && Array.isArray(history) && history.length > 0) {
      timelineChart.data.labels = history.map(h => h.timestamp.split(' ')[1]);
      timelineChart.data.datasets[0].data = history.map(h => h.total_requests);
      timelineChart.data.datasets[1].data = history.map(h => h.avg_response_time_ms);
      timelineChart.update('none');
    }
  } catch (err) {
    console.error('Failed to fetch history:', err);
  }
}

// Polling loops
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  fetchLatestMetrics();
  fetchHistory();

  setInterval(fetchLatestMetrics, 2000);
  setInterval(fetchHistory, 3000);
});
