const BACKEND_API_URL = 'http://localhost:8085/api/parking-status';
const SUBSCRIBE_API_URL = 'http://localhost:8085/api/subscribe';
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

// Subscribe button handler
document.getElementById('subscribe-btn').addEventListener('click', async () => {
    const emailInput = document.getElementById('email-input');
    const email = emailInput.value.trim();
    if (!email || !email.match(/^[^\s@]+@[^\s@]+\.[^\s@]+$/)) {
        showAlert('Please enter a valid email address');
        return;
    }

    try {
        const response = await fetch(SUBSCRIBE_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email })
        });
        const result = await response.json();
        if (response.ok) {
            showAlert('Successfully subscribed!');
            emailInput.value = ''; // Clear input on success
        } else {
            showAlert(`Subscription failed: ${result.error || 'Unknown error'}`);
        }
    } catch (error) {
        showAlert(`Failed to subscribe: ${error.message}`);
    }
});

// Simple predictive model for availability
function predictAvailability(history) {
    if (history.length < 5) return 0;
    const recentRates = history.slice(-5).map(d => d.rate);
    const avgRate = recentRates.reduce((sum, rate) => sum + rate, 0) / recentRates.length;
    const totalSpots = TOTAL_SLOTS_PER_LOT * 3;
    return Math.round(totalSpots * (1 - avgRate / 100));
}

// Highlight a recommended spot
function highlightSpot(lotName, slotId) {
    const spotElement = document.querySelector(`[data-lot="${lotName}"][data-slot="${slotId}"]`);
    if (spotElement) {
        spotElement.classList.add('recommended');
    } else {
        console.warn(`Spot element not found for ${lotName} #${slotId}`);
    }
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
        showAlert('Error: No parking data available');
        document.getElementById('status-message').textContent = 'No data available';
        return;
    }

    document.getElementById('status-message').textContent = 'Parking data loaded';

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
        document.getElementById('status-message').textContent = 'No data available';
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
            case 'free':
                availableSpots++;
                break;
            case 'occupied':
                occupiedSpots++;
                if (spot.duration) {
                    totalDuration += Number(spot.duration);
                    occupiedCount++;
                }
                if (spot.temperature) {
                    totalTemperature += Number(spot.temperature);
                    temperatureCount++;
                }
                break;
            case 'malfunction':
                malfunctionSpots++;
                break;
            case 'sensor_failure':
                sensorFailureSpots++;
                break;
        }
    });

    const occupancyRate = totalSpots > 0 ? (occupiedSpots / totalSpots) * 100 : 0;
    const avgDuration = occupiedCount > 0 ? totalDuration / occupiedCount / 60 : 0;
    const avgTemperature = temperatureCount > 0 ? totalTemperature / temperatureCount : 0;
    const turnoverRate = turnovers;

    // Update DOM
    document.getElementById('total-spots').textContent = totalSpots;
    document.getElementById('available-spots').textContent = availableSpots;
    document.getElementById('occupied-spots').textContent = occupiedSpots;
    document.getElementById('malfunction-spots').textContent = malfunctionSpots;
    document.getElementById('occupancy-rate').textContent = `${Math.round(occupancyRate)}%`;
    document.getElementById('avg-duration').textContent = `${Math.round(avgDuration)} min`;
    document.getElementById('avg-temperature').textContent = `${Math.round(avgTemperature)} °C`;
    document.getElementById('turnover-rate').textContent = `${turnoverRate}/hr`;
document.getElementById('predicted-availability').textContent = `${predictAvailability(occupancyHistory[currentTimeScale])} spots`;

const progressBar = document.getElementById('occupancy-progress');
progressBar.style.width = `${occupancyRate}%`;

// Update historical data
const now = new Date();
occupancyHistory[currentTimeScale].push({ time: now, rate: occupancyRate });
temperatureHistory[currentTimeScale].push({ time: now, temp: avgTemperature });
turnoverCounts.push({ time: now, count: turnoverRate });

// Trim history
if (occupancyHistory[currentTimeScale].length > MAX_DATA_POINTS[currentTimeScale]) {
    occupancyHistory[currentTimeScale].shift();
}
if (temperatureHistory[currentTimeScale].length > MAX_DATA_POINTS[currentTimeScale]) {
    temperatureHistory[currentTimeScale].shift();
}
if (turnoverCounts.length > MAX_DATA_POINTS[currentTimeScale]) {
    turnoverCounts.shift();
}

// Update charts
updateCharts(occupancyRate, availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots);
}

// Update charts
function updateCharts(occupancyRate, availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots) {
    // Occupancy Trend Chart
    if (charts.occupancyTrend) {
        charts.occupancyTrend.data.labels = occupancyHistory[currentTimeScale].map(d => d.time.toLocaleTimeString());
        charts.occupancyTrend.data.datasets[0].data = occupancyHistory[currentTimeScale].map(d => d.rate);
        charts.occupancyTrend.update();
    } else {
        charts.occupancyTrend = new Chart(document.getElementById('occupancy-trend-chart'), {
            type: 'line',
            data: {
                labels: occupancyHistory[currentTimeScale].map(d => d.time.toLocaleTimeString()),
                datasets: [{
                    label: 'Occupancy Rate (%)',
                    data: occupancyHistory[currentTimeScale].map(d => d.rate),
                    borderColor: '#1e88e5',
                    fill: false
                }]
            },
            options: {
                scales: { y: { beginAtZero: true, max: 100 } },
                plugins: { zoom: { zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'xy' } } }
            }
        });
    }

    // Status Pie Chart
    if (charts.statusPie) {
        charts.statusPie.data.datasets[0].data = [availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots];
        charts.statusPie.update();
    } else {
        charts.statusPie = new Chart(document.getElementById('status-pie-chart'), {
            type: 'pie',
            data: {
                labels: ['Available', 'Occupied', 'Malfunction', 'Sensor Failure'],
                datasets: [{
                    data: [availableSpots, occupiedSpots, malfunctionSpots, sensorFailureSpots],
                    backgroundColor: ['#4caf50', '#f44336', '#ff9800', '#ffeb3b']
                }]
            },
            options: {
                plugins: {
                    datalabels: { color: '#fff', font: { weight: 'bold' } }
                }
            }
        });
    }

    // Temperature Trend Chart
    if (charts.temperatureTrend) {
        charts.temperatureTrend.data.labels = temperatureHistory[currentTimeScale].map(d => d.time.toLocaleTimeString());
        charts.temperatureTrend.data.datasets[0].data = temperatureHistory[currentTimeScale].map(d => d.temp);
        charts.temperatureTrend.update();
    } else {
        charts.temperatureTrend = new Chart(document.getElementById('temperature-trend-chart'), {
            type: 'line',
            data: {
                labels: temperatureHistory[currentTimeScale].map(d => d.time.toLocaleTimeString()),
                datasets: [{
                    label: 'Temperature (°C)',
                    data: temperatureHistory[currentTimeScale].map(d => d.temp),
                    borderColor: '#d81b60',
                    fill: false
                }]
            },
            options: {
                scales: { y: { beginAtZero: true } },
                plugins: { zoom: { zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'xy' } } }
            }
        });
    }
}

// Event listeners for controls
document.getElementById('lot-filter').addEventListener('change', (e) => {
    renderParkingLots(lastData, e.target.value, document.getElementById('status-filter').value);
    updateOverallStatistics(lastData, e.target.value);
});

document.getElementById('status-filter').addEventListener('change', (e) => {
    renderParkingLots(lastData, document.getElementById('lot-filter').value, e.target.value);
});

document.getElementById('toggle-3d').addEventListener('click', () => {
    is3DView = !is3DView;
    renderParkingLots(lastData, document.getElementById('lot-filter').value, document.getElementById('status-filter').value);
});

document.getElementById('toggle-heatmap').addEventListener('click', () => {
    isHeatmap = !isHeatmap;
    renderParkingLots(lastData, document.getElementById('lot-filter').value, document.getElementById('status-filter').value);
});

document.querySelectorAll('.time-scale-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.time-scale-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentTimeScale = btn.dataset.scale;
        updateCharts();
    });
});

document.getElementById('toggle-sidebar').addEventListener('click', () => {
    document.getElementById('stats-panel').classList.toggle('collapsed');
});

document.getElementById('collapse-sidebar').addEventListener('click', () => {
    document.getElementById('stats-panel').classList.toggle('collapsed');
});

document.getElementById('toggle-theme').addEventListener('click', () => {
    document.body.classList.toggle('dark-theme');
});

// Periodic data fetch
dataWorker.onmessage = (e) => {
    if (e.data.success) {
        lastData = e.data.data || [];
        const lotFilter = document.getElementById('lot-filter').value;
        const statusFilter = document.getElementById('status-filter').value;
        renderParkingLots(lastData, lotFilter, statusFilter);
        updateOverallStatistics(lastData, lotFilter);
    } else {
        console.error('Data fetch error:', e.data.error);
        showAlert(`Failed to fetch parking data: ${e.data.error}`);
        document.getElementById('status-message').textContent = 'Error fetching data';
    }
};

setInterval(() => dataWorker.postMessage({}), UPDATE_INTERVAL_MS);

// Initial fetch
dataWorker.postMessage({});
