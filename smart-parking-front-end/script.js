const BACKEND_API_URL = 'http://localhost:8085/api/parking-status';
const UPDATE_INTERVAL_MS = 3000;
const TOTAL_SLOTS_PER_LOT = 50;
const MAX_DATA_POINTS = {
    '10m': 200, // 10 min / 3s ~ 200 points
    '1h': 1200, // 1 hr / 3s ~ 1200 points
    '1d': 28800 // 1 day / 3s ~ 28800 points
};

// Historical data
let occupancyHistory = { '10m': [], '1h': [], '1d': [] };
let temperatureHistory = { '10m': [], '1h': [], '1d': [] };
let turnoverCounts = [];
let charts = { occupancyTrend: null, statusPie: null, temperatureTrend: null };
let currentTimeScale = '10m';
let is3DView = false;
let isHeatmap = false;
let lastData = [];

// Web Worker for data fetching
const dataWorker = new Worker(URL.createObjectURL(new Blob([`
    self.onmessage = async function() {
        try {
            const response = await fetch('${BACKEND_API_URL}');
            if (!response.ok) {
                throw new Error('Network response was not ok: ' + response.status);
            }
            const data = await response.json();
            self.postMessage({ success: true, data });
        } catch (error) {
            self.postMessage({ success: false, error: error.message });
        }
    };
`], { type: 'text/javascript' })));

// Simple predictive model for availability
function predictAvailability(history) {
    if (history.length < 5) return 0;
    const recentRates = history.slice(-5).map(d => d.rate);
    const avgRate = recentRates.reduce((sum, rate) => sum + rate, 0) / recentRates.length;
    const totalSpots = TOTAL_SLOTS_PER_LOT * 3;
    return Math.round(totalSpots * (1 - avgRate / 100));
}

// Show alert
function showAlert(message) {
    const alertsContainer = document.getElementById('alerts-container');
    if (!alertsContainer) {
        console.error('Alerts container not found');
        return;
    }
    const alert = document.createElement('div');
    alert.classList.add('alert');
    alert.textContent = message;
    alertsContainer.appendChild(alert);
    setTimeout(() => alert.remove(), 5000);
}

// Render parking lots
function renderParkingLots(parkingData, lotFilter = 'all', statusFilter = 'all') {
    if (!parkingData || !Array.isArray(parkingData)) {
        console.error('Invalid parking data:', parkingData);
        showAlert('Error: Invalid or missing parking data');
        return;
    }

    // Map statuses using the correct API field names
    const slotStatuses = parkingData.reduce((acc, spot) => {
        if (spot && spot.parkingLotName && spot.slotId && spot.status) {
            if (!acc[spot.parkingLotName]) acc[spot.parkingLotName] = {};
            acc[spot.parkingLotName][spot.slotId] = spot.status.toLowerCase();
        } else {
            console.warn('Skipping invalid spot data:', spot);
        }
        return acc;
    }, {});

    console.log('Slot statuses mapped:', slotStatuses); // Debug the mapped statuses

    ['A', 'B', 'C'].forEach(lotLetter => {
        const lotName = `Parking Lot ${lotLetter}`;
        if (lotFilter !== 'all' && lotFilter !== lotName) return;

        const lotElement = document.getElementById(`parking-lot-${lotLetter}`);
        if (!lotElement) {
            console.error(`Parking lot element not found: parking-lot-${lotLetter}`);
            return;
        }
        const spotsGrid = lotElement.querySelector('.parking-map-grid');
        if (!spotsGrid) {
            console.error(`Parking map grid not found in lot ${lotLetter}`);
            return;
        }
        spotsGrid.innerHTML = '';

        for (let i = 1; i <= TOTAL_SLOTS_PER_LOT; i++) {
            let status = slotStatuses[lotName]?.[i] || 'free';
            if (statusFilter !== 'all' && status !== statusFilter) continue;

            console.log(`Rendering slot ${lotName} #${i} with status: ${status}`); // Debug each slot

            const spotElement = document.createElement('div');
            spotElement.classList.add('parking-spot', status);
            spotElement.dataset.lot = lotName;
            spotElement.dataset.slot = i;

            const iconElement = document.createElement('i');
            iconElement.classList.add('spot-icon', 'fas');
            if (status === 'occupied') iconElement.classList.add('fa-car');
            else if (status === 'malfunction') iconElement.classList.add('fa-tools');
            else if (status === 'sensor_failure') iconElement.classList.add('fa-exclamation-triangle');

            const labelElement = document.createElement('span');
            labelElement.classList.add('spot-label');
            labelElement.textContent = `${i}`;

            if (status !== 'free') spotElement.appendChild(iconElement);
            spotElement.appendChild(labelElement);
            spotsGrid.appendChild(spotElement);

            spotElement.addEventListener('click', () => showSlotDetails(
                parkingData.find(s => s.parkingLotName === lotName && s.slotId === i) || { status: 'free' },
                spotElement
            ));
        }

        spotsGrid.classList.toggle('isometric', is3DView);
        spotsGrid.classList.toggle('heatmap', isHeatmap);
    });
}

// Show slot details in modal
function showSlotDetails(spotInfo, spotElement) {
    const modal = document.getElementById('slot-details');
    if (!modal) {
        console.error('Slot details modal not found');
        return;
    }
    modal.classList.remove('hidden');
    modal.style.top = `${spotElement.getBoundingClientRect().bottom + window.scrollY + 5}px`;
    modal.style.left = `${spotElement.getBoundingClientRect().left}px`;

    modal.innerHTML = `
        <div class="detail-item"><strong>Slot:</strong> ${spotInfo?.parkingLotName || 'Unknown'} #${spotInfo?.slotId || 'N/A'}</div>
        <div class="detail-item"><strong>Status:</strong> ${spotInfo?.status || 'Free'}</div>
        <div class="detail-item"><strong>License Plate:</strong> ${spotInfo?.vehicle_license_plate || 'N/A'}</div>
        <div class="detail-item"><strong>Duration:</strong> ${spotInfo?.duration ? Math.round(spotInfo.duration / 60) + ' min' : 'N/A'}</div>
        <div class="detail-item"><strong>Temperature:</strong> ${spotInfo?.temperature ? spotInfo.temperature + ' °C' : 'N/A'}</div>
    `;
}

// Update statistics and charts
function updateOverallStatistics(parkingData, lotFilter = 'all') {
    if (!parkingData || !Array.isArray(parkingData)) {
        console.error('Invalid parking data for statistics:', parkingData);
        return;
    }

    let totalSpots = lotFilter === 'all' ? TOTAL_SLOTS_PER_LOT * 3 : TOTAL_SLOTS_PER_LOT;
    let availableSpots = 0, occupiedSpots = 0, malfunctionSpots = 0, sensorFailureSpots = 0;
    let totalDuration = 0, occupiedCount = 0, totalTemperature = 0, temperatureCount = 0;
    let turnovers = 0;

    parkingData.forEach((spot, index) => {
        if (lotFilter !== 'all' && spot.parkingLotName !== lotFilter) return;
        const status = spot.status?.toLowerCase() || 'free';
        const lastSpot = lastData[index] || {};

        if (lastSpot.status === 'occupied' && status === 'free') turnovers++;

        switch (status) {
            case 'free': availableSpots++; break;
            case 'occupied':
                occupiedSpots++;
                if (spot.duration) {
                    totalDuration += Number(spot.duration);
                    occupiedCount++;
                }
                break;
            case 'malfunction': malfunctionSpots++; break;
            case 'sensor_failure': sensorFailureSpots++; break;
        }

        if (spot.temperature && !isNaN(Number(spot.temperature))) {
            totalTemperature += Number(spot.temperature);
            temperatureCount++;
        }
    });

    const occupancyRate = totalSpots > 0 ? ((occupiedSpots / totalSpots) * 100).toFixed(1) : 0;
    const avgDuration = occupiedCount > 0 ? (totalDuration / occupiedCount / 60).toFixed(0) : 0;
    const avgTemperature = temperatureCount > 0 ? (totalTemperature / temperatureCount).toFixed(1) : 0;
    const turnoverRate = turnovers / (UPDATE_INTERVAL_MS / 3600000);

    const updateElement = (id, value) => {
        const element = document.getElementById(id);
        if (element) element.textContent = value;
        else console.error(`Element not found: ${id}`);
    };
    updateElement('total-spots', totalSpots);
    updateElement('available-spots', availableSpots);
    updateElement('occupied-spots', occupiedSpots);
    updateElement('malfunction-spots', malfunctionSpots + sensorFailureSpots);
    updateElement('occupancy-rate', `${occupancyRate}%`);
    updateElement('avg-duration', `${avgDuration} min`);
    updateElement('avg-temperature', `${avgTemperature} °C`);
    updateElement('turnover-rate', `${turnoverRate.toFixed(1)}/hr`);
    updateElement('predicted-availability', `${predictAvailability(occupancyHistory[currentTimeScale])} spots`);

    const occupancyProgress = document.getElementById('occupancy-progress');
    if (occupancyProgress) {
        occupancyProgress.style.width = `${occupancyRate}%`;
    }

    if (malfunctionSpots + sensorFailureSpots > totalSpots * 0.1) {
        showAlert('Warning: High number of malfunctions or sensor failures detected!');
    }
    if (occupancyRate > 90) {
        showAlert('Alert: Parking lots are nearly full!');
    }

    const timestamp = new Date().toLocaleTimeString();
    occupancyHistory[currentTimeScale].push({ time: timestamp, rate: parseFloat(occupancyRate) });
    temperatureHistory[currentTimeScale].push({ time: timestamp, temp: parseFloat(avgTemperature) });
    turnoverCounts.push({ time: timestamp, count: turnovers });

    if (occupancyHistory[currentTimeScale].length > MAX_DATA_POINTS[currentTimeScale]) {
        occupancyHistory[currentTimeScale].shift();
        temperatureHistory[currentTimeScale].shift();
        turnoverCounts.shift();
    }

    updateCharts(availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots);
}

// Initialize and update charts
function updateCharts(availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots) {
    const isDarkMode = document.body.classList.contains('dark-mode');
    const colors = isDarkMode ? {
        available: '#2f855a',
        occupied: '#c82333',
        malfunction: '#e0a800',
        sensorFailure: '#6c757d'
    } : {
        available: '#28a745',
        occupied: '#dc3545',
        malfunction: '#ffab00',
        sensorFailure: '#6c757d'
    };

    if (!charts.occupancyTrend) {
        const ctx = document.getElementById('occupancy-trend-chart')?.getContext('2d');
        if (!ctx) {
            console.error('Occupancy trend chart canvas not found');
            return;
        }
        charts.occupancyTrend = new Chart(ctx, {
            type: 'line',
            data: {
                labels: occupancyHistory[currentTimeScale].map(d => d.time),
                datasets: [{
                    label: 'Occupancy Rate (%)',
                    data: occupancyHistory[currentTimeScale].map(d => d.rate),
                    borderColor: '#1a73e8',
                    backgroundColor: 'rgba(26, 115, 232, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: { title: { display: true, text: 'Time' } },
                    y: { title: { display: true, text: 'Occupancy Rate (%)' }, min: 0, max: 100 }
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                    zoom: {
                        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
                        pan: { enabled: true, mode: 'x' }
                    }
                }
            }
        });
    } else {
        charts.occupancyTrend.data.labels = occupancyHistory[currentTimeScale].map(d => d.time);
        charts.occupancyTrend.data.datasets[0].data = occupancyHistory[currentTimeScale].map(d => d.rate);
        charts.occupancyTrend.update();
    }

    if (!charts.statusPie) {
        const ctx = document.getElementById('status-pie-chart')?.getContext('2d');
        if (!ctx) {
            console.error('Status pie chart canvas not found');
            return;
        }
        charts.statusPie = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Available', 'Occupied', 'Malfunction', 'Sensor Failure'],
                datasets: [{
                    data: [availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots],
                    backgroundColor: [colors.available, colors.occupied, colors.malfunction, colors.sensorFailure],
                    borderColor: isDarkMode ? '#2a3b57' : '#ffffff',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'right' },
                    datalabels: { color: isDarkMode ? '#d9e2ec' : '#2a2a2a', formatter: (value, ctx) => value > 0 ? value : '' }
                },
                onClick: (e, elements) => {
                    if (elements.length) {
                        const index = elements[0].index;
                        const status = ['free', 'occupied', 'malfunction', 'sensor_failure'][index];
                        const statusFilter = document.getElementById('status-filter');
                        if (statusFilter) {
                            statusFilter.value = status;
                            renderParkingLots(lastData, document.getElementById('lot-filter')?.value || 'all', status);
                        }
                    }
                }
            }
        });
    } else {
        charts.statusPie.data.datasets[0].data = [availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots];
        charts.statusPie.update();
    }

    if (!charts.temperatureTrend) {
        const ctx = document.getElementById('temperature-trend-chart')?.getContext('2d');
        if (!ctx) {
            console.error('Temperature trend chart canvas not found');
            return;
        }
        charts.temperatureTrend = new Chart(ctx, {
            type: 'line',
            data: {
                labels: temperatureHistory[currentTimeScale].map(d => d.time),
                datasets: [{
                    label: 'Avg Temperature (°C)',
                    data: temperatureHistory[currentTimeScale].map(d => d.temp),
                    borderColor: '#17a2b8',
                    backgroundColor: 'rgba(23, 162, 184, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: { title: { display: true, text: 'Time' } },
                    y: { title: { display: true, text: 'Temperature (°C)' }, min: 0, max: 40 }
                },
                plugins: {
                    legend: { position: 'top' }
                }
            }
        });
    } else {
        charts.temperatureTrend.data.labels = temperatureHistory[currentTimeScale].map(d => d.time);
        charts.temperatureTrend.data.datasets[0].data = temperatureHistory[currentTimeScale].map(d => d.temp);
        charts.temperatureTrend.update();
    }
}

// Initialize dashboard
function initDashboard() {
    const statusMessage = document.getElementById('status-message');
    if (!statusMessage) {
        console.error('Status message element not found');
    }

    dataWorker.postMessage({});
    dataWorker.onmessage = ({ data }) => {
        if (data.success && Array.isArray(data.data)) {
            lastData = data.data;
            console.log('Updated lastData:', lastData); // Debug the received data
            statusMessage.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
            const lotFilter = document.getElementById('lot-filter')?.value || 'all';
            const statusFilter = document.getElementById('status-filter')?.value || 'all';
            renderParkingLots(lastData, lotFilter, statusFilter);
            updateOverallStatistics(lastData, lotFilter);
        } else {
            statusMessage.textContent = `Error: ${data.error || 'Invalid data format'}`;
            showAlert(`Failed to fetch data: ${data.error || 'Invalid data format'}`);
            console.error('Response error:', data.error);
        }
    };

    const toggleSidebar = document.getElementById('toggle-sidebar');
    if (toggleSidebar) {
        toggleSidebar.addEventListener('click', () => {
            const panel = document.getElementById('stats-panel');
            if (panel) {
                panel.classList.toggle('collapsed');
                const collapseIcon = document.getElementById('collapse-sidebar')?.querySelector('i');
                if (collapseIcon) {
                    collapseIcon.classList.toggle('fa-chevron-left');
                    collapseIcon.classList.toggle('fa-chevron-right');
                }
            }
        });
    }

    const collapseSidebar = document.getElementById('collapse-sidebar');
    if (collapseSidebar) {
        collapseSidebar.addEventListener('click', () => {
            toggleSidebar.click();
        });
    }

    const toggleTheme = document.getElementById('toggle-theme');
    if (toggleTheme) {
        toggleTheme.addEventListener('click', () => {
            document.body.classList.toggle('dark-mode');
            const themeIcon = toggleTheme.querySelector('i');
            if (themeIcon) {
                themeIcon.classList.toggle('fa-moon');
                themeIcon.classList.toggle('fa-sun');
            }
            Object.values(charts).forEach(chart => chart && chart.update());
        });
    }

    const lotFilter = document.getElementById('lot-filter');
    if (lotFilter) {
        lotFilter.addEventListener('change', (e) => {
            renderParkingLots(lastData, e.target.value, document.getElementById('status-filter')?.value || 'all');
            updateOverallStatistics(lastData, e.target.value);
        });
    }

    const statusFilter = document.getElementById('status-filter');
    if (statusFilter) {
        statusFilter.addEventListener('change', (e) => {
            renderParkingLots(lastData, document.getElementById('lot-filter')?.value || 'all', e.target.value);
        });
    }

    const toggle3D = document.getElementById('toggle-3d');
    if (toggle3D) {
        toggle3D.addEventListener('click', () => {
            is3DView = !is3DView;
            document.querySelectorAll('.parking-map-grid').forEach(grid => {
                if (grid) grid.classList.toggle('isometric', is3DView);
            });
        });
    }

    const toggleHeatmap = document.getElementById('toggle-heatmap');
    if (toggleHeatmap) {
        toggleHeatmap.addEventListener('click', () => {
            isHeatmap = !isHeatmap;
            document.querySelectorAll('.parking-map-grid').forEach(grid => {
                if (grid) grid.classList.toggle('heatmap', isHeatmap);
            });
        });
    }

    document.querySelectorAll('.time-scale-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.time-scale-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentTimeScale = btn.dataset.scale;
            if (charts.occupancyTrend) {
                charts.occupancyTrend.data.labels = occupancyHistory[currentTimeScale].map(d => d.time);
                charts.occupancyTrend.data.datasets[0].data = occupancyHistory[currentTimeScale].map(d => d.rate);
                charts.occupancyTrend.update();
            }
            if (charts.temperatureTrend) {
                charts.temperatureTrend.data.labels = temperatureHistory[currentTimeScale].map(d => d.time);
                charts.temperatureTrend.data.datasets[0].data = temperatureHistory[currentTimeScale].map(d => d.temp);
                charts.temperatureTrend.update();
            }
        });
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.parking-spot') && !e.target.closest('#slot-details')) {
            const slotDetails = document.getElementById('slot-details');
            if (slotDetails) {
                slotDetails.classList.add('hidden');
            }
        }
    });
}

document.addEventListener('DOMContentLoaded', () => {
    initDashboard();
    setInterval(() => dataWorker.postMessage({}), UPDATE_INTERVAL_MS);
});