const API_BASE_URL = window.location.origin + '/api';

let currentData = [];
let filteredData = [];
let currentPage = 1;
let itemsPerPage = 100;
let sortColumn = null; 
let sortDirection = 'asc';
let columnFilters = {
    Location: '',
    any_bad: ''
};

document.addEventListener('DOMContentLoaded', () => {
    initializeApp();
});

/**
 * Initialize the application by checking health, loading sensors, and setting up listeners.
 */
async function initializeApp() {
    console.log('Initializing Sensor Data Dashboard...');
    
    await checkHealth();
    
    await loadSensors();
    
    await checkPipelineStatus();
    
    setupEventListeners();
    
    startPipelineStatusPolling();
}

/**
 * Check API health status and update UI indicator.
 */
async function checkHealth() {
    const statusIndicator = document.getElementById('statusIndicator');
    const statusDot = statusIndicator.querySelector('.status-dot');
    const statusText = statusIndicator.querySelector('.status-text');
    
    try {
        const response = await fetch(`${API_BASE_URL}/health`);
        const data = await response.json();
        
        if (data.status === 'healthy') {
            statusDot.classList.add('connected');
            statusText.textContent = 'Connected';
        } else {
            throw new Error('API not healthy');
        }
    } catch (error) {
        statusDot.classList.add('disconnected');
        statusText.textContent = 'Disconnected';
        console.error('Health check failed:', error);
        showError('Unable to connect to API. Please ensure the backend is running.');
    }
}

/**
 * Load available sensors from the API and populate the sensor dropdown.
 */
async function loadSensors() {
    try {
        let url = `${API_BASE_URL}/sensors`;
        
        const response = await fetch(url);
        const sensors = await response.json();
        
        const sensorSelect = document.getElementById('sensorSelect');
        sensorSelect.innerHTML = '<option value="">Select a sensor...</option>';
        
        sensors.forEach(sensor => {
            const option = document.createElement('option');
            option.value = sensor.Sensor_ID;
            option.textContent = `${sensor.Sensor_ID} - ${sensor.total_readings} readings`;
            sensorSelect.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading sensors:', error);
        showError('Failed to load sensors. Please try again.');
    }
}

/**
 * Load detailed sensor data for the selected sensor.
 */
async function loadSensorData() {
    const sensorId = document.getElementById('sensorSelect').value;
    if (!sensorId) {
        showError('Please select a sensor first.');
        return;
    }
    
    showLoading(true);
    
    try {
        await loadSensorSummary(sensorId);
        
        let url = `${API_BASE_URL}/sensors/${encodeURIComponent(sensorId)}/data?limit=1000`;
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        currentData = data;
        
        columnFilters = {
            Location: '',
            any_bad: ''
        };
        
        populateColumnFilters();
        
        applyColumnFilters();
        
        currentPage = 1;
        
        sortColumn = 'ts_date';
        sortDirection = 'asc';
        filteredData.sort((a, b) => {
            let valueA = a[sortColumn];
            let valueB = b[sortColumn];
            if (valueA == null) return 1;
            if (valueB == null) return -1;
            if (valueA < valueB) return -1;
            if (valueA > valueB) return 1;
            return 0;
        });
        
        renderTable();
        updatePagination();
        
        const sortedHeader = document.querySelector(`th[data-sort="ts_date"]`);
        if (sortedHeader) {
            sortedHeader.classList.add('sorted-asc');
        }
        
        document.getElementById('exportBtn').style.display = 'inline-block';
        
    } catch (error) {
        console.error('Error loading sensor data:', error);
        showError(`Failed to load sensor data: ${error.message}`);
    } finally {
        showLoading(false);
    }
}

/**
 * Load and display sensor summary statistics.
 * @param {string} sensorId - The sensor ID to load summary for.
 */
async function loadSensorSummary(sensorId) {
    try {
        const response = await fetch(`${API_BASE_URL}/sensors/${encodeURIComponent(sensorId)}/summary`);
        const summary = await response.json();
        
        const summarySection = document.getElementById('summarySection');
        const summaryGrid = document.getElementById('summaryGrid');
        
        summaryGrid.innerHTML = `
            <div class="summary-card">
                <h3>Top Location</h3>
                <div class="value">${summary.Location}</div>
            </div>
            <div class="summary-card">
                <h3>Total Readings</h3>
                <div class="value">${summary.total_readings.toLocaleString()}</div>
            </div>
            <div class="summary-card">
                <h3>Days of Data</h3>
                <div class="value">${summary.days_of_data}</div>
            </div>
            <div class="summary-card">
                <h3>Days Active</h3>
                <div class="value">${summary.days_active}</div>
            </div>
            <div class="summary-card">
                <h3>Bad Readings</h3>
                <div class="value">${summary.total_pct_bad.toFixed(1)}%</div>
                <div class="label">${summary.total_bad} of ${summary.total_readings}</div>
            </div>
            <div class="summary-card">
                <h3>Monitoring Period</h3>
                <div class="value">${formatDate(summary.first_reading)}</div>
                <div class="label">to ${formatDate(summary.last_reading)}</div>
            </div>
            <div class="summary-card">
                <h3>Parameters</h3>
                <div class="value">${summary.parameters.length}</div>
                <div class="label">${summary.parameters.join(', ')}</div>
            </div>
            <div class="summary-card">
                <h3>Average Values</h3>
                <div class="value" style="font-size: 0.8em;">
                    ${Object.entries(summary.avg_values || {}).map(([k, v]) => `${k}: ${v.toFixed(2)}`).join('<br>')}
                </div>
            </div>
        `;
        
        summarySection.style.display = 'block';
    } catch (error) {
        console.error('Error loading sensor summary:', error);
    }
}

/**
 * Render the data table with current page data.
 */
function renderTable() {
    const tbody = document.getElementById('tableBody');
    
    if (filteredData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="16" class="empty-state">No data found matching your criteria.</td></tr>';
        return;
    }
    
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = Math.min(startIndex + itemsPerPage, filteredData.length);
    const pageData = filteredData.slice(startIndex, endIndex);
    
    tbody.innerHTML = pageData.map(row => `
        <tr>
            <td>${formatDate(row.ts_date)}</td>
            <td>${row.Location}</td>
            <td><span class="status-badge status-${row.any_bad === 1 ? 'bad' : 'good'}">${row.any_bad === 1 ? 'Bad' : 'Good'}</span></td>
            <td>${row.good_streak_days || 0}</td>
            <td>${formatNumber(row.Temperature)}</td>
            <td>${formatDelta(row.delta_temperature)}</td>
            <td>${formatNumber(row.Pressure)}</td>
            <td>${formatDelta(row.delta_pressure)}</td>
            <td>${formatNumber(row.Humidity)}</td>
            <td>${formatNumber(row.mean_7r_humidity)}</td>
            <td>${formatNumber(row.Vibration)}</td>
            <td>${formatNumber(row.mean_7r_vibration)}</td>
            <td>${formatNumber(row.Noise)}</td>
            <td>${formatNumber(row.prev_noise)}</td>
            <td>${formatNumber(row.Current)}</td>
            <td>${formatNumber(row.mean_7r_current)}</td>
        </tr>
    `).join('');
}

/**
 * Update pagination controls and info display.
 */
function updatePagination() {
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    const pageInfo = document.getElementById('pageInfo');
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');
    const pagination = document.getElementById('pagination');
    
    if (totalPages > 1) {
        pagination.style.display = 'flex';
        pageInfo.textContent = `Page ${currentPage} of ${totalPages} (${filteredData.length} records)`;
        prevBtn.disabled = currentPage === 1;
        nextBtn.disabled = currentPage === totalPages;
    } else {
        pagination.style.display = 'none';
    }
}

/**
 * Populate column filter dropdowns with unique values from current data.
 */
function populateColumnFilters() {
    const filterableColumns = ['Location'];
    
    filterableColumns.forEach(column => {
        const uniqueValues = [...new Set(currentData.map(row => row[column]))].filter(v => v != null).sort();
        const selectId = `filter${column}`;
        const select = document.getElementById(selectId);
        
        if (select) {
            select.innerHTML = '<option value="">All</option>';
            uniqueValues.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
            });
        }
    });
}

/**
 * Apply active column filters to the current data.
 */
function applyColumnFilters() {
    filteredData = currentData.filter(row => {
        for (const [column, filterValue] of Object.entries(columnFilters)) {
            if (filterValue && String(row[column]) !== String(filterValue)) {
                return false;
            }
        }
        return true;
    });
}

/**
 * Filter table data based on search term and column filters.
 * @param {string} searchTerm - The search term to filter by.
 */
function filterTable(searchTerm) {
    const term = searchTerm.toLowerCase();
    
    applyColumnFilters();
    
    if (term) {
        filteredData = filteredData.filter(row => {
            return Object.values(row).some(value => 
                String(value).toLowerCase().includes(term)
            );
        });
    }
    
    currentPage = 1;
    renderTable();
    updatePagination();
}

/**
 * Sort table by the specified column.
 * @param {string} column - The column name to sort by.
 */
function sortTable(column) {
    if (sortColumn === column) {
        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        sortColumn = column;
        sortDirection = 'asc';
    }
    
    filteredData.sort((a, b) => {
        let valueA = a[column];
        let valueB = b[column];
        
        if (valueA == null) return 1;
        if (valueB == null) return -1;
        
        if (typeof valueA === 'string') {
            valueA = valueA.toLowerCase();
            valueB = valueB.toLowerCase();
        }
        
        if (valueA < valueB) return sortDirection === 'asc' ? -1 : 1;
        if (valueA > valueB) return sortDirection === 'asc' ? 1 : -1;
        return 0;
    });
    
    document.querySelectorAll('.data-table th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
    });
    
    const sortedHeader = document.querySelector(`th[data-sort="${column}"]`);
    if (sortedHeader) {
        sortedHeader.classList.add(`sorted-${sortDirection}`);
    }
    
    renderTable();
}

/**
 * Export filtered data to CSV file.
 */
function exportToCSV() {
    if (filteredData.length === 0) {
        showError('No data to export.');
        return;
    }
    
    const headers = ['ts_date', 'Location', 'any_bad', 'good_streak_days', 'Temperature', 'delta_temperature', 
                     'Pressure', 'delta_pressure', 'Humidity', 'mean_7r_humidity', 'Vibration', 'mean_7r_vibration', 
                     'Noise', 'prev_noise', 'Current', 'mean_7r_current'];
    
    let csv = headers.join(',') + '\n';
    
    filteredData.forEach(row => {
        const values = headers.map(header => {
            let value = row[header];
            if (value === null || value === undefined) value = '';
            if (typeof value === 'string' && value.includes(',')) {
                value = `"${value}"`;
            }
            return value;
        });
        csv += values.join(',') + '\n';
    });
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sensor_data_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
}

/**
 * Check and update the status of all pipeline steps.
 */
async function checkPipelineStatus() {
    try {
        const response = await fetch(`${API_BASE_URL}/pipeline/status`);
        const status = await response.json();
        
        updatePipelineStatusUI('generate', status.generate);
        updatePipelineStatusUI('process', status.process);
        updatePipelineStatusUI('load', status.load);
        
        if (!isRunningCompletePipeline) {
            const runAllBtn = document.getElementById('runAllBtn');
            const runAllBtnText = document.getElementById('runAllBtnText');
            const anyRunning = status.generate.running || status.process.running || status.load.running;
            
            if (!anyRunning && runAllBtn.disabled) {
                runAllBtn.disabled = false;
                runAllBtnText.textContent = 'Run Complete Pipeline';
            }
        }
    } catch (error) {
        console.error('Error checking pipeline status:', error);
    }
}

/**
 * Update the UI for a specific pipeline step status.
 * @param {string} step - The pipeline step name.
 * @param {Object} statusData - The status data for the step.
 */
function updatePipelineStatusUI(step, statusData) {
    const statusElement = document.getElementById(`${step}Status`);
    const badge = statusElement.querySelector('.status-badge');
    const button = document.getElementById(`${step}Btn`);
    
    badge.classList.remove('status-idle', 'status-running', 'status-success', 'status-error', 'status-started');
    
    badge.classList.add(`status-${statusData.status}`);
    
    if (statusData.status === 'running') {
        badge.textContent = '⏳ Running...';
        button.disabled = true;
    } else if (statusData.status === 'success') {
        badge.textContent = '✅ Success';
        button.disabled = false;
    } else if (statusData.status === 'error') {
        badge.textContent = '❌ Error';
        badge.title = statusData.message;
        button.disabled = false;
    } else {
        badge.textContent = 'Idle';
        button.disabled = false;
    }
}

/**
 * Start polling pipeline status at regular intervals.
 */
function startPipelineStatusPolling() {
    setInterval(async () => {
        await checkPipelineStatus();
    }, 3000);
}

/**
 * Run a single pipeline step.
 * @param {string} step - The pipeline step to run (generate, process, or load).
 */
async function runPipelineStep(step) {
    const stepNames = {
        'generate': 'Data Generation',
        'process': 'Data Processing',
        'load': 'Data Loading'
    };
    
    try {
        let url = `${API_BASE_URL}/pipeline/${step}`;
        let requestOptions = {
            method: 'POST'
        };
        
        if (step === 'generate') {
            const rowCount = document.getElementById('rowCountInput').value;
            url += `?num_rows=${rowCount}`;
        }
        
        const response = await fetch(url, requestOptions);
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || error.error);
        }
        
        const result = await response.json();
        
        await checkPipelineStatus();
        
    } catch (error) {
        showNotification(`Error: ${error.message}`, 'error');
    }
}

/**
 * Run the complete pipeline (generate, process, and load steps sequentially).
 */
async function runCompletePipeline() {
    const runAllBtn = document.getElementById('runAllBtn');
    const runAllBtnText = document.getElementById('runAllBtnText');
    const originalText = runAllBtnText.textContent;
    
    try {
        runAllBtn.disabled = true;
        runAllBtnText.textContent = 'Running...';
        
        const rowCount = document.getElementById('rowCountInput').value;
        
        const response1 = await fetch(`${API_BASE_URL}/pipeline/generate?num_rows=${rowCount}`, {
            method: 'POST'
        });
        if (!response1.ok) {
            throw new Error((await response1.json()).detail || (await response1.json()).error);
        }

        await checkPipelineStatus();

        const response2 = await fetch(`${API_BASE_URL}/pipeline/process`, {
            method: 'POST'
        });
        if (!response2.ok) {
            throw new Error((await response2.json()).detail || (await response2.json()).error);
        }

        await checkPipelineStatus();

        const response3 = await fetch(`${API_BASE_URL}/pipeline/load`, {
            method: 'POST'
        });
        
        await checkPipelineStatus();

        if (!response1.ok || !response2.ok || !response3.ok) {
            const error = await response1.json();
            const error2 = await response2.json();
            const error3 = await response3.json();
            throw new Error(error.detail || error.error || error2.detail || error2.error || error3.detail || error3.error);
        }
        
        const result1 = await response1.json();
        const result2 = await response2.json();
        const result3 = await response3.json();
     
        runAllBtn.disabled = false;
        runAllBtnText.textContent = 'Run Complete Pipeline';
        
        
    } catch (error) {
        showNotification(`Error: ${error.message}`, 'error');
        runAllBtn.disabled = false;
        runAllBtnText.textContent = originalText;
    }
}

/**
 * Clear all pipeline data (files and database).
 */
async function clearAllData() {
    try {
        const response = await fetch(`${API_BASE_URL}/pipeline/data`, {
            method: 'DELETE'
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || error.error);
        }
        
        const result = await response.json();
        
        await checkPipelineStatus();
        
    } catch (error) {
        showNotification(`Error: ${error.message}`, 'error');
    }
}

/**
 * Show a notification to the user.
 * @param {string} message - The message to display.
 * @param {string} type - The notification type (info, success, or error).
 */
function showNotification(message, type = 'info') {
    const icon = type === 'success' ? '✅' : type === 'error' ? '❌' : 'ℹ️';
    alert(`${icon} ${message}`);
}

/**
 * Setup all event listeners for the application.
 */
function setupEventListeners() {
    document.getElementById('generateBtn').addEventListener('click', () => runPipelineStep('generate'));
    document.getElementById('processBtn').addEventListener('click', () => runPipelineStep('process'));
    document.getElementById('loadBtn').addEventListener('click', () => runPipelineStep('load'));
    document.getElementById('runAllBtn').addEventListener('click', runCompletePipeline);
    document.getElementById('clearDataBtn').addEventListener('click', clearAllData);
    
    document.getElementById('sensorSelect').addEventListener('change', (e) => {
        if (e.target.value) {
            loadSensorData();
        }
    });
    
    document.getElementById('refreshBtn').addEventListener('click', () => {
        document.getElementById('sensorSelect').value = '';
        
        document.getElementById('summarySection').style.display = 'none';
        
        const tbody = document.getElementById('tableBody');
        tbody.innerHTML = '<tr><td colspan="16" class="empty-state">Select a sensor to view sensor readings</td></tr>';
        
        document.getElementById('pagination').style.display = 'none';
        document.getElementById('exportBtn').style.display = 'none';
        
        document.getElementById('searchInput').value = '';
        
        currentData = [];
        filteredData = [];
        currentPage = 1;
        
        loadSensors();
    });
    
    const searchInput = document.getElementById('searchInput');
    let searchTimeout;
    searchInput.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            filterTable(e.target.value);
        }, 300);
    });
    
    document.getElementById('filterLocation')?.addEventListener('change', (e) => {
        e.stopPropagation();
        columnFilters.Location = e.target.value;
        filterTable(document.getElementById('searchInput').value);
    });
    
    document.getElementById('filterStatus')?.addEventListener('change', (e) => {
        e.stopPropagation();
        columnFilters.any_bad = e.target.value;
        filterTable(document.getElementById('searchInput').value);
    });
    
    document.querySelectorAll('.column-filter').forEach(filter => {
        filter.addEventListener('click', (e) => {
            e.stopPropagation();
        });
    });
    
    document.querySelectorAll('.data-table th[data-sort]').forEach(th => {
        th.addEventListener('click', (e) => {
            if (e.target.classList.contains('column-filter')) {
                return;
            }
            const column = th.getAttribute('data-sort');
            sortTable(column);
        });
    });
    
    document.getElementById('prevBtn').addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            renderTable();
            updatePagination();
        }
    });
    
    document.getElementById('nextBtn').addEventListener('click', () => {
        const totalPages = Math.ceil(filteredData.length / itemsPerPage);
        if (currentPage < totalPages) {
            currentPage++;
            renderTable();
            updatePagination();
        }
    });
    
    document.getElementById('exportBtn').addEventListener('click', exportToCSV);
}

/**
 * Format a date string for display.
 * @param {string} dateString - The date string to format.
 * @returns {string} Formatted date string.
 */
function formatDate(dateString) {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
}

/**
 * Format a number for display with 2 decimal places.
 * @param {number} num - The number to format.
 * @returns {string} Formatted number or 'N/A'.
 */
function formatNumber(num) {
    if (num === null || num === undefined) return 'N/A';
    return typeof num === 'number' ? num.toFixed(2) : num;
}

/**
 * Format a delta value with color coding for positive/negative.
 * @param {number} num - The delta value to format.
 * @returns {string} Formatted HTML string with color.
 */
function formatDelta(num) {
    if (num === null || num === undefined) return 'N/A';
    if (typeof num !== 'number') return num;
    const formatted = num.toFixed(2);
    if (num > 0) {
        return `<span style="color: #ef4444;">+${formatted}</span>`;
    } else if (num < 0) {
        return `<span style="color: #3b82f6;">${formatted}</span>`;
    }
    return formatted;
}

/**
 * Show or hide the loading spinner.
 * @param {boolean} show - Whether to show the loading spinner.
 */
function showLoading(show) {
    const spinner = document.getElementById('loadingSpinner');
    const tableContainer = document.querySelector('.table-container');
    
    if (show) {
        spinner.style.display = 'block';
        tableContainer.style.opacity = '0.5';
    } else {
        spinner.style.display = 'none';
        tableContainer.style.opacity = '1';
    }
}

/**
 * Show an error message to the user.
 * @param {string} message - The error message to display.
 */
function showError(message) {
    alert(message);
}
