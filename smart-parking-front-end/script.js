const BACKEND_API_URL = 'http://localhost:8085/api/parking-status'; // Ensure this matches your Java backend port
const UPDATE_INTERVAL_MS = 3000; // Update every 3 seconds
const TOTAL_SLOTS_PER_LOT = 50; // Each parking lot has 50 spots
const MAX_DATA_POINTS = 20; // Store last 20 data points for the line chart (1 hour with 3s updates ~ 1200s / 60 = 20 points)

// Store historical data for occupancy trend
let occupancyHistory = [];

// Chart.js instances
let occupancyTrendChart = null;
let statusPieChart = null;

// Function to fetch parking data from the backend API
async function fetchParkingData() {
    const statusMessage = document.getElementById('status-message');
    statusMessage.textContent = 'Fetching data...';

    try {
        const response = await fetch(BACKEND_API_URL);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        statusMessage.textContent = `Last updated: ${new Date().toLocaleTimeString()} (Data from ${BACKEND_API_URL})`;
        return data;
    } catch (error) {
        console.error("Error fetching parking data:", error);
        statusMessage.textContent = `Error: Could not connect to backend. Please ensure the Java backend is running at ${BACKEND_API_URL}.`;
        return [];
    }
}

// Function to render or update the parking lot visualization
function renderParkingLots(parkingData) {
    const groupedData = parkingData.reduce((acc, spot) => {
        if (!acc[spot.parkingLotName]) {
            acc[spot.parkingLotName] = {};
        }
        acc[spot.parkingLotName][spot.slotId] = spot;
        return acc;
    }, {});

    const parkingLotNames = ['Parking Lot A', 'Parking Lot B', 'Parking Lot C'];

    parkingLotNames.forEach(lotName => {
        const lotLetter = lotName.split(' ').pop();
        const lotElement = document.getElementById(`parking-lot-${lotLetter}`);

        if (!lotElement) {
            console.warn(`Parking lot container for "${lotName}" not found in HTML. Skipping.`);
            return;
        }

        const spotsGrid = lotElement.querySelector('.parking-map-grid');
        spotsGrid.innerHTML = '';

        const currentLotData = groupedData[lotName] || {};

        for (let i = 1; i <= TOTAL_SLOTS_PER_LOT; i++) {
            const spotElement = document.createElement('div');
            spotElement.classList.add('parking-spot');

            const spotInfo = currentLotData[i];
            const status = spotInfo && spotInfo.status ? spotInfo.status.toLowerCase() : 'available';

            const iconElement = document.createElement('i');
            iconElement.classList.add('spot-icon', 'fas');

            const labelElement = document.createElement('span');
            labelElement.classList.add('spot-label');
            labelElement.textContent = `${i}`;

            spotElement.classList.remove('available', 'occupied', 'malfunction');
            iconElement.className = 'spot-icon fas';

            switch (status) {
                case 'occupied':
                    spotElement.classList.add('occupied');
                    iconElement.classList.add('fa-car');
                    spotElement.appendChild(iconElement);
                    break;
                case 'malfunction':
                    spotElement.classList.add('malfunction');
                    iconElement.classList.add('fa-tools');
                    spotElement.appendChild(iconElement);
                    break;
                case 'available':
                case 'free':
                default:
                    spotElement.classList.add('available');
                    break;
            }

            spotElement.appendChild(labelElement);
            spotsGrid.appendChild(spotElement);
        }
    });
}

// Function to update overall statistics and charts
function updateOverallStatistics(parkingData) {
    let totalSpots = TOTAL_SLOTS_PER_LOT * 3;
    let availableSpots = 0;
    let occupiedSpots = 0;
    let malfunctionSpots = 0;
    let totalOccupancyDuration = 0;
    let occupiedCountForDuration = 0;
    let totalTemperature = 0;
    let temperatureCount = 0;

    parkingData.forEach(spot => {
        const status = spot.status ? String(spot.status).toLowerCase() : '';
        switch (status) {
            case 'available':
            case 'free':
                availableSpots++;
                break;
            case 'occupied':
                occupiedSpots++;
                if (spot.duration !== undefined && spot.duration !== null && !isNaN(Number(spot.duration))) {
                    totalOccupancyDuration += Number(spot.duration);
                    occupiedCountForDuration++;
                }
                break;
            case 'malfunction':
                malfunctionSpots++;
                break;
        }

        if (spot.temperature !== undefined && spot.temperature !== null && !isNaN(Number(spot.temperature))) {
            totalTemperature += Number(spot.temperature);
            temperatureCount++;
        }
    });

    const occupancyRate = totalSpots > 0 ? ((occupiedSpots / totalSpots) * 100).toFixed(1) : 0;
    const avgDuration = occupiedCountForDuration > 0 ? (totalOccupancyDuration / occupiedCountForDuration).toFixed(0) : 0;
    const avgTemperature = temperatureCount > 0 ? (totalTemperature / temperatureCount).toFixed(1) : 0;

    document.getElementById('total-spots').textContent = totalSpots;
    document.getElementById('available-spots').textContent = availableSpots;
    document.getElementById('occupied-spots').textContent = occupiedSpots;
    document.getElementById('malfunction-spots').textContent = malfunctionSpots;
    document.getElementById('occupancy-rate').textContent = `${occupancyRate}%`;
    document.getElementById('avg-duration').textContent = `${avgDuration} min`;
    document.getElementById('avg-temperature').textContent = `${avgTemperature} °C`;

    // Update occupancy trend data
    const timestamp = new Date().toLocaleTimeString();
    occupancyHistory.push({ time: timestamp, rate: parseFloat(occupancyRate) });
    if (occupancyHistory.length > MAX_DATA_POINTS) {
        occupancyHistory.shift(); // Remove oldest data point
    }

    // Update charts
    updateCharts(availableSpots, occupiedSpots, malfunctionSpots);
}

// Function to initialize and update charts
function updateCharts(availableSpots, occupiedSpots, malfunctionSpots) {
    // Initialize or update occupancy trend line chart
    if (!occupancyTrendChart) {
        const ctx = document.getElementById('occupancy-trend-chart').getContext('2d');
        occupancyTrendChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: occupancyHistory.map(data => data.time),
                datasets: [{
                    label: 'Occupancy Rate (%)',
                    data: occupancyHistory.map(data => data.rate),
                    borderColor: '#007bff',
                    backgroundColor: 'rgba(0, 123, 255, 0.1)',
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
                    y: { 
                        title: { display: true, text: 'Occupancy Rate (%)' },
                        min: 0,
                        max: 100
                    }
                },
                plugins: {
                    legend: { display: true, position: 'top' }
                }
            }
        });
    } else {
        occupancyTrendChart.data.labels = occupancyHistory.map(data => data.time);
        occupancyTrendChart.data.datasets[0].data = occupancyHistory.map(data => data.rate);
        occupancyTrendChart.update();
    }

    // Initialize or update status pie chart
    if (!statusPieChart) {
        const ctx = document.getElementById('status-pie-chart').getContext('2d');
        statusPieChart = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: ['Available', 'Occupied', 'Malfunction'],
                datasets: [{
                    data: [availableSpots, occupiedSpots, malfunctionSpots],
                    backgroundColor: ['#28a745', '#dc3545', '#ffc107'],
                    borderColor: ['#ffffff', '#ffffff', '#ffffff'],
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: true, position: 'right' }
                }
            }
        });
    } else {
        statusPieChart.data.datasets[0].data = [availableSpots, occupiedSpots, malfunctionSpots];
        statusPieChart.update();
    }
}

// Main function to initialize the dashboard and set up periodic updates
async function initDashboard() {
    const data = await fetchParkingData();
    renderParkingLots(data);
    updateOverallStatistics(data);
}

// Ensure the DOM is fully loaded before running JavaScript
document.addEventListener('DOMContentLoaded', () => {
    initDashboard();
    setInterval(initDashboard, UPDATE_INTERVAL_MS);
});