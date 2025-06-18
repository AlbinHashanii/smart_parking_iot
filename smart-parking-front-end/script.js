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
    // Group incoming data by parking lot name for easier access
    const groupedData = parkingData.reduce((acc, spot) => {
        if (!acc[spot.parkingLotName]) {
            acc[spot.parkingLotName] = {};
        }
        // Ensure slotId is a string if it comes as number, for consistent keying
        acc[spot.parkingLotName][spot.slotId] = spot; // Store the whole spot object
        return acc;
    }, {});

    // Define the parking lots we expect to visualize
    // IMPORTANT: Make sure these names exactly match what your Java backend sends (e.g., "Parking Lot A")
    const parkingLotNames = ['Parking Lot A', 'Parking Lot B', 'Parking Lot C'];

    parkingLotNames.forEach(lotName => {
        // Extract "A", "B", "C" from "Parking Lot A" to match HTML IDs (parking-lot-A)
        const lotLetter = lotName.split(' ').pop();
        const lotElement = document.getElementById(`parking-lot-${lotLetter}`);

        if (!lotElement) {
            console.warn(`Parking lot container for "${lotName}" not found in HTML. Skipping.`);
            return;
        }

        const spotsGrid = lotElement.querySelector('.parking-map-grid');
        // Clear existing spots only if needed, or update existing ones for performance
        // For simplicity and robustness on initial render/full update, clearing is fine.
        spotsGrid.innerHTML = '';

        const currentLotData = groupedData[lotName] || {}; // Get data for this specific lot

        // Create/update individual parking spots
        for (let i = 1; i <= TOTAL_SLOTS_PER_LOT; i++) {
            const spotElement = document.createElement('div');
            spotElement.classList.add('parking-spot');

            const spotInfo = currentLotData[i]; // Get the whole spot object for this ID
            const status = spotInfo ? spotInfo.status.toLowerCase() : 'available'; // Default to available if no data

            // Icon element
            const iconElement = document.createElement('i');
            iconElement.classList.add('spot-icon');
            iconElement.classList.add('fas'); // Font Awesome base class

            // Label for the slot ID
            const labelElement = document.createElement('span');
            labelElement.classList.add('spot-label');
            labelElement.textContent = `S${i}`;

            // Apply status-specific classes and icons
            spotElement.classList.remove('available', 'occupied', 'malfunction'); // Clear previous
            iconElement.className = 'spot-icon fas'; // Reset icon classes

            switch (status) {
                case 'occupied':
                    spotElement.classList.add('occupied');
                    iconElement.classList.add('fa-car'); // Car icon
                    spotElement.appendChild(iconElement); // Add icon
                    break;
                case 'malfunction':
                    spotElement.classList.add('malfunction');
                    iconElement.classList.add('fa-tools'); // Tools icon
                    spotElement.appendChild(iconElement); // Add icon
                    break;
                case 'available':
                case 'free': // Treat "free" as "available"
                default: // Fallback for unexpected status
                    spotElement.classList.add('available');
                    // No specific icon for available spots by default, but you could add one if desired.
                    // If you want a circle/dot for available, you'd add:
                    // iconElement.classList.add('fa-circle');
                    // spotElement.appendChild(iconElement);
                    break;
            }

            // Always append the label
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