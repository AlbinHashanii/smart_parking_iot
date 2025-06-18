const BACKEND_API_URL = 'http://localhost:8085/api/parking-status'; // Ensure this matches your Java backend port
const UPDATE_INTERVAL_MS = 5000; // Update every 5 seconds (adjust as needed)
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
    // Group incoming data by parking lot name for easier access
    const groupedData = parkingData.reduce((acc, spot) => {
        if (!acc[spot.parkingLotName]) {
            acc[spot.parkingLotName] = {};
        }
        acc[spot.parkingLotName][spot.slotId] = spot.status;
        return acc;
    }, {});

    // Define the parking lots we expect to visualize
    // IMPORTANT: Make sure these names exactly match what your Java backend sends (e.g., "Parking Lot A")
    const parkingLotNames = ['Parking Lot A', 'Parking Lot B', 'Parking Lot C'];

    parkingLotNames.forEach(lotName => {
        // This extracts "A", "B", "C" from "Parking Lot A"
        const lotLetter = lotName.split(' ').pop();
        // Uses the new card ID from HTML
        const lotElement = document.getElementById(`parking-lot-${lotLetter}`);

        if (!lotElement) {
            console.warn(`Parking lot container for "${lotName}" not found in HTML. Skipping.`);
            return;
        }

        // Use the new class name for the grid container
        const spotsGrid = lotElement.querySelector('.parking-map-grid');
        spotsGrid.innerHTML = ''; // Clear existing spots before re-rendering/updating

        const currentLotData = groupedData[lotName] || {}; // Get data for this specific lot

        // Create/update individual parking spots
        for (let i = 1; i <= TOTAL_SLOTS_PER_LOT; i++) {
            const spotElement = document.createElement('div');
            spotElement.classList.add('parking-spot');

            const status = currentLotData[i]; // Get the status from the fetched data

            // Create an icon element
            const iconElement = document.createElement('i');
            iconElement.classList.add('fas'); // Font Awesome base class

            // Create a label for the slot ID
            const labelElement = document.createElement('span');
            labelElement.classList.add('spot-label');
            labelElement.textContent = `S${i}`;

            if (status) {
                const lowerCaseStatus = status.toLowerCase();
                if (lowerCaseStatus === 'occupied') {
                    spotElement.classList.add('occupied');
                    iconElement.classList.add('fa-car'); // Car icon
                    spotElement.appendChild(iconElement); // Add icon to spot
                } else if (lowerCaseStatus === 'available' || lowerCaseStatus === 'free') {
                    spotElement.classList.add('available');
                    // No icon for available/free spots
                } else if (lowerCaseStatus === 'malfunction') {
                    spotElement.classList.add('malfunction');
                    iconElement.classList.add('fa-tools'); // Tools icon
                    spotElement.appendChild(iconElement); // Add icon to spot
                } else {
                    // Default for unexpected status values
                    spotElement.classList.add('available');
                }
            } else {
                // If a slot ID from 1-50 is not present in fetched data, assume it's available
                spotElement.classList.add('available');
            }

            // Append the slot label always
            spotElement.appendChild(labelElement);
            spotsGrid.appendChild(spotElement);
        }
    });
}

// Main function to initialize the dashboard and set up periodic updates
async function initDashboard() {
    const data = await fetchParkingData();
    renderParkingLots(data);
}

// Ensure the DOM is fully loaded before running JavaScript
document.addEventListener('DOMContentLoaded', () => {
    initDashboard(); // Perform initial data fetch and render
    setInterval(initDashboard, UPDATE_INTERVAL_MS); // Set up periodic updates
});