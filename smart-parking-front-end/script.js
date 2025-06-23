const BACKEND_API_URL = 'http://localhost:8085/api/parking-status'; // Ensure this matches your Java backend port
const UPDATE_INTERVAL_MS = 3000; // Update every 3 seconds for a smoother feel (adjust as needed)
const TOTAL_SLOTS_PER_LOT = 50; // Each parking lot has 50 spots, as per your design

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
        return []; // Return empty array on error to prevent further issues
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
            // Default to 'available' if no data or status is unclear
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

// Function to update overall statistics
function updateOverallStatistics(parkingData) {
    let totalSpots = TOTAL_SLOTS_PER_LOT * 3; // Assuming 3 lots
    let availableSpots = 0;
    let occupiedSpots = 0;
    let malfunctionSpots = 0;
    let totalOccupancyDuration = 0;
    let occupiedCountForDuration = 0;
    let totalTemperature = 0;
    let temperatureCount = 0;

    parkingData.forEach(spot => {
        // Ensure status exists and convert to lower case safely
        const status = spot.status ? String(spot.status).toLowerCase() : '';

        switch (status) {
            case 'available':
            case 'free':
                availableSpots++;
                break;
            case 'occupied':
                occupiedSpots++;
                // **** CHANGED: Now using spot.duration ****
                if (spot.duration !== undefined && spot.duration !== null && !isNaN(Number(spot.duration))) {
                    totalOccupancyDuration += Number(spot.duration);
                    occupiedCountForDuration++;
                }
                break;
            case 'malfunction':
                malfunctionSpots++;
                break;
        }

        // **** CHANGED: Now using spot.temperature ****
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