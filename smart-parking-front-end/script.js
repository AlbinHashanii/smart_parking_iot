console.log("Pristina Smart Parking Map loaded.");

const BACKEND_API_URL = 'http://localhost:8085/api/parking-status';
const SUBSCRIBE_API_URL = 'http://localhost:8085/api/subscribe';
const UPDATE_INTERVAL_MS = 3000;
const TOTAL_SLOTS_PER_LOT = 50;
const PRISTINA_CENTER = [42.6727, 21.1669];
const DEFAULT_ZOOM = 13;

let map;
let parkingLotMarkers = {};
let aggregatedLotData = {};

const PARKING_LOT_COORDINATES = {
    "City Center": { lat: 42.6595317, lng: 21.1602549, totalSlots: TOTAL_SLOTS_PER_LOT }, // Mother Teresa Square
    "Arberia": { lat: 42.662349, lng: 21.148689, totalSlots: TOTAL_SLOTS_PER_LOT }, 
    "Dardania": { lat: 42.651348 , lng: 21.154443, totalSlots: TOTAL_SLOTS_PER_LOT }, // South, residential
    "Ulpiana": { lat: 42.651188, lng: 21.161378, totalSlots: TOTAL_SLOTS_PER_LOT }, // Modern residential
    "Bregu i Diellit": { lat: 42.653587, lng: 21.174288, totalSlots: TOTAL_SLOTS_PER_LOT }, // Southwest, residential
    "Kalabria": { lat: 42.642933, lng: 21.143615, totalSlots: TOTAL_SLOTS_PER_LOT }, // Urban, not Veternik
    "Lakrishte": { lat: 42.656792, lng: 21.153727, totalSlots: TOTAL_SLOTS_PER_LOT }, // Near Arberia
    "Kodra e Trimave": { lat: 42.678117, lng: 21.163883, totalSlots: TOTAL_SLOTS_PER_LOT } // Central, residential
};

const BACKEND_TO_FRONTEND_MAP = {
    "Lot-A": "City Center",
    "Lot-B": "Arberia",
    "Lot-C": "Dardania",
    "Lot-D": "Ulpiana",
    "Lot-E": "Bregu i Diellit",
    "Lot-F": "Kalabria",
    "Lot-G": "Lakrishte",
    "Lot-H": "Kodra e Trimave"
};

function showErrorMessage(message) {
    const errorDiv = document.getElementById('error-message');
    if (errorDiv) {
        errorDiv.textContent = `Error: ${message}`;
        errorDiv.style.display = 'block';
        console.error(message);
    }
}

function showAlert(message) {
    const alertsContainer = document.getElementById('alerts-container');
    if (!alertsContainer) {
        console.error('Alerts container not found.');
        return;
    }
    const alert = document.createElement('div');
    alert.className = 'alert';
    alert.textContent = message;
    alertsContainer.appendChild(alert);
    setTimeout(() => alert.remove(), 5000);
}

function getStatusClass(available, total, malfunctions) {
    if (!total || total <= 0) {
        console.warn(`Invalid total slots: ${total}. Defaulting to yellow.`);
        return 'status-yellow';
    }
    if (malfunctions >= total) {
        console.warn(`All slots malfunctioning: ${malfunctions}/${total}. Setting gray status.`);
        return 'status-gray';
    }
    const ratio = available / total;
    console.log(`Status calc: available=${available}, total=${total}, ratio=${ratio.toFixed(2)}`);
    if (ratio > 0.5) return 'status-green';
    if (ratio > 0.15) return 'status-yellow';
    return 'status-red';
}

function createCustomMarkerIcon(lotName, statusColorClass) {
    const words = lotName.split(' ');
    let initials = '';
    if (words.length > 0 && words[0].length > 0) {
        initials += words[0].charAt(0);
    }
    if (words.length > 1 && words[1].length > 0) {
        initials += words[1].charAt(0);
    }
    if (initials.length === 0 && lotName.length > 0) {
        initials = lotName.substring(0, 2);
    }
    if (initials.length === 0) {
        initials = '??';
    }

    return L.divIcon({
        className: `map-marker-icon ${statusColorClass}`,
        html: `<span>${initials.toUpperCase()}</span>`,
        iconSize: [35, 35],
        iconAnchor: [17, 17],
        popupAnchor: [0, -17]
    });
}

function setupMarkerInteraction(marker, lotName, coords) {
    const lotStats = aggregatedLotData[lotName] || { totalSlots: coords.totalSlots, free: 0, occupied: 0, malfunction: 0, lastUpdated: new Date().toISOString() };
    const total = lotStats.totalSlots || TOTAL_SLOTS_PER_LOT;
    const available = lotStats.free;
    const occupied = lotStats.occupied;
    const malfunctions = lotStats.malfunction;
    const lastUpdated = lotStats.lastUpdated ? new Date(lotStats.lastUpdated).toLocaleString() : 'Unknown';

    console.log(`Marker ${lotName}: free=${available}, occupied=${occupied}, malfunction=${malfunctions}, total=${total}`);

    marker.bindTooltip(`<b>${lotName}</b><br>${available}/${total} vende te lira`, {
        offset: [0, -20],
        direction: 'top',
        permanent: false
    });

    const popupContent = `
        <b>${lotName}</b><br>
        Coordinates: ${coords.lat.toFixed(4)}, ${coords.lng.toFixed(4)}<br>
        Total Slots: ${total}<br>
        Free: ${available}<br>
        Occupied: ${occupied}<br>
        Malfunction: ${malfunctions}<br>
        Occupancy: ${(total > 0 ? (occupied / total * 100).toFixed(1) : 0)}%<br>
        Last Updated: ${lastUpdated}
    `;

    marker.bindPopup(popupContent, {
        autoPan: true,
        closeOnClick: false,
        autoClose: false
    });

    marker.options.interactive = true;
    marker.options.keyboard = true;

    marker.on({
        mouseover: function() {
            console.log(`Mouseover: ${lotName}`);
            this.openTooltip();
        },
        mouseout: function() {
            console.log(`Mouseout: ${lotName}`);
            this.closeTooltip();
        },
        click: function(e) {
            console.log(`Click: ${lotName} at ${new Date().toLocaleTimeString()}`);
            this.openPopup();
            L.DomEvent.stopPropagation(e);
        },
        mousedown: function(e) {
            console.log(`Mousedown: ${lotName}`);
            L.DomEvent.stopPropagation(e);
        },
        add: function() {
            const markerDomElement = this.getElement();
            if (markerDomElement) {
                markerDomElement.style.setProperty('pointer-events', 'auto', 'important');
                markerDomElement.style.setProperty('cursor', 'pointer', 'important');
                markerDomElement.setAttribute('tabindex', '0');
                console.log(`Marker DOM initialized for ${lotName}`);
            } else {
                console.warn(`Marker DOM element for ${lotName} not available`);
            }
        }
    });
}

async function fetchAndUpdateParkingData() {
    if (typeof L === 'undefined' || !map) {
        console.warn("Skipping fetch: Leaflet map not initialized.");
        showErrorMessage("Map not initialized for data fetch.");
        return;
    }

    console.log(`[${new Date().toLocaleTimeString()}] Fetching data from: ${BACKEND_API_URL}`);
    try {
        const response = await fetch(BACKEND_API_URL);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log(`[${new Date().toLocaleTimeString()}] Data received:`, data);

        if (!Array.isArray(data) || data.length === 0) {
            console.warn("Received data is empty or not an array:", data);
            showAlert('Error: No valid parking data received. Using default status.');
            for (const lotName in PARKING_LOT_COORDINATES) {
                aggregatedLotData[lotName] = {
                    totalSlots: PARKING_LOT_COORDINATES[lotName].totalSlots,
                    free: 0,
                    occupied: 0,
                    malfunction: 0,
                    lastUpdated: new Date().toISOString()
                };
                const marker = parkingLotMarkers[lotName];
                if (marker) {
                    const wasTooltipOpen = marker.isTooltipOpen();
                    marker.setIcon(createCustomMarkerIcon(lotName, 'status-yellow'));
                    setupMarkerInteraction(marker, lotName, PARKING_LOT_COORDINATES[lotName]);
                    if (wasTooltipOpen) marker.openTooltip();
                }
            }
            return;
        }

        aggregatedLotData = {};

        data.forEach(sensor => {
            const backendLotName = sensor.parkingLotName;
            const frontendLotName = BACKEND_TO_FRONTEND_MAP[backendLotName];
            if (!frontendLotName) {
                console.warn(`No matching frontend lot for backend name: ${backendLotName}`);
                return;
            }

            if (!PARKING_LOT_COORDINATES[frontendLotName]) {
                console.warn(`No coordinates defined for lot: ${frontendLotName}`);
                return;
            }

            if (!aggregatedLotData[frontendLotName]) {
                aggregatedLotData[frontendLotName] = {
                    totalSlots: PARKING_LOT_COORDINATES[frontendLotName].totalSlots,
                    free: 0,
                    occupied: 0,
                    malfunction: 0,
                    rawSensorData: [],
                    lastUpdated: sensor.lastUpdated
                };
            }

            aggregatedLotData[frontendLotName].rawSensorData.push(sensor);
            aggregatedLotData[frontendLotName].lastUpdated = sensor.lastUpdated;

            const status = sensor.status?.toLowerCase();
            if (!['free', 'occupied', 'sensor_failure'].includes(status)) {
                console.warn(`Invalid status '${status}' for sensor ${sensor.sensorId} in lot ${backendLotName}`);
                aggregatedLotData[frontendLotName].malfunction++;
                return;
            }

            switch (status) {
                case 'free':
                    aggregatedLotData[frontendLotName].free++;
                    break;
                case 'occupied':
                    aggregatedLotData[frontendLotName].occupied++;
                    break;
                case 'sensor_failure':
                    aggregatedLotData[frontendLotName].malfunction++;
                    break;
            }
        });

        for (const lotName in PARKING_LOT_COORDINATES) {
            if (PARKING_LOT_COORDINATES.hasOwnProperty(lotName)) {
                const lotStats = aggregatedLotData[lotName] || {
                    totalSlots: PARKING_LOT_COORDINATES[lotName].totalSlots,
                    free: 0,
                    occupied: 0,
                    malfunction: 0,
                    lastUpdated: new Date().toISOString()
                };
                const marker = parkingLotMarkers[lotName];
                if (marker) {
                    const wasTooltipOpen = marker.isTooltipOpen();
                    const statusClass = getStatusClass(lotStats.free, lotStats.totalSlots, lotStats.malfunction);
                    marker.setIcon(createCustomMarkerIcon(lotName, statusClass));
                    setupMarkerInteraction(marker, lotName, PARKING_LOT_COORDINATES[lotName]);
                    if (wasTooltipOpen) {
                        marker.openTooltip();
                    }
                    console.log(`Updated ${lotName}: status=${statusClass}, free=${lotStats.free}, total=${lotStats.totalSlots}, malfunctions=${lotStats.malfunction}`);
                }
            }
        }
    } catch (error) {
        console.error("Failed to fetch parking data:", error);
        showAlert(`Failed to update data: ${error.message}`);
    }
}

async function handleSubscribeForm(event) {
    event.preventDefault();
    const emailInput = document.getElementById('subscribe-email');
    const email = emailInput.value.trim();
    if (!email.match(/^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$/)) {
        showAlert('Please enter a valid email address.');
        return;
    }

    try {
        const response = await fetch(SUBSCRIBE_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email })
        });
        const result = await response.json();
        if (response.ok && result.status === 'success') {
            showAlert(`Subscribed successfully with ${email}!`);
            emailInput.value = '';
        } else {
            throw new Error(result.message || `HTTP error! status: ${response.status}`);
        }
    } catch (error) {
        console.error('Subscription failed:', error);
        showAlert(`Subscription failed: ${error.message}. Please try again or contact support at https://x.ai/api.`);
    }
}

function initMap() {
    console.log("initMap called.");
    const mapElement = document.getElementById('parking-map');
    if (!mapElement) {
        showErrorMessage("Map container (#parking-map) not found in DOM.");
        return;
    }

    const computedStyle = getComputedStyle(mapElement);
    const width = parseFloat(computedStyle.width);
    const height = parseFloat(computedStyle.height);
    console.log(`Map container dimensions: ${width}x${height}px`);

    if (width <= 0 || height <= 0) {
        showErrorMessage("Map container has invalid dimensions. Check CSS.");
        return;
    }

    try {
        if (typeof L === 'undefined') {
            throw new Error("Leaflet library not loaded.");
        }

        mapElement.textContent = '';
        map = L.map('parking-map', {
            zoomControl: true,
            doubleClickZoom: true,
            scrollWheelZoom: true,
            tap: true,
            touchZoom: true
        }).setView(PRISTINA_CENTER, DEFAULT_ZOOM);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            tileSize: 256,
            maxZoom: 19
        }).on('tileerror', function(error) {
            console.error("Tile loading error:", error);
            showErrorMessage("Failed to load map tiles.");
        }).addTo(map);

        map.whenReady(function() {
            console.log("Map is ready.");
            setTimeout(() => {
                map.invalidateSize();
                console.log("map.invalidateSize() called.");

                let markerCount = 0;
                for (const lotName in PARKING_LOT_COORDINATES) {
                    if (PARKING_LOT_COORDINATES.hasOwnProperty(lotName)) {
                        const coords = PARKING_LOT_COORDINATES[lotName];
                        const marker = L.marker([coords.lat, coords.lng], {
                            icon: createCustomMarkerIcon(lotName, 'status-yellow'),
                            interactive: true,
                            keyboard: true
                        }).addTo(map);

                        parkingLotMarkers[lotName] = marker;
                        markerCount++;
                        setupMarkerInteraction(marker, lotName, coords);
                        console.log(`Marker added for ${lotName} at [${coords.lat}, ${coords.lng}]`);
                    }
                }
                console.log(`Total markers added: ${markerCount}`);
                if (markerCount === 0) {
                    showErrorMessage("No markers added. Check coordinates data.");
                }

                fetchAndUpdateParkingData();
                setInterval(fetchAndUpdateParkingData, UPDATE_INTERVAL_MS);
            }, 500);
        });
    } catch (e) {
        showErrorMessage(`Map initialization failed: ${e.message}`);
    }
}

document.getElementById('subscribe-form').addEventListener('submit', handleSubscribeForm);

window.addEventListener('load', () => {
    if (typeof L === 'undefined') {
        showErrorMessage("Leaflet library failed to load. Check connection.");
    }
});

let pageInitialized = false;
document.addEventListener('DOMContentLoaded', () => {
    if (pageInitialized) return;
    pageInitialized = true;
    console.log("DOMContentLoaded: Initializing map...");
    initMap();
});

if (document.readyState === 'complete' && !pageInitialized) {
    console.log("Document already loaded. Running setup...");
    initMap();
}