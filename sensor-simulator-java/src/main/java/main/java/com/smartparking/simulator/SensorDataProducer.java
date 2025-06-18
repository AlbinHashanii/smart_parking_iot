package main.java.com.smartparking.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SensorDataProducer {

    private static final String KAFKA_TOPIC = "parking-sensor-data";
    private static final String KAFKA_BROKERS = "localhost:9092";

    private static final String[] PARKING_LOT_NAMES = {"Parking Lot A", "Parking Lot B", "Parking Lot C"};
    private static final int NUM_SLOTS_PER_LOT = 50; // Each lot has 50 slots
    private static final Random random = new Random();

    // New: Sample license plates
    private static final String[] LICENSE_PLATES = {"ABC-123", "XYZ-789", "DEF-456", "MNO-007", "QWE-111", "RTY-222", "IOP-333"};

    // --- Simulation parameters - ADJUSTED for your requirements ---
    // Probability for a *single available spot* to get occupied per second
    // Adjusted downwards to slow down arrivals a bit, expecting longer average duration.
    // This value will need fine-tuning based on how "busy" you want the lots to appear.
    private static final double ARRIVAL_PROBABILITY_PER_SECOND = 0.002; // Roughly 0.2% chance per available spot per second

    // Probability for any spot to malfunction per second. Very low chance.
    private static final double MALFUNCTION_PROBABILITY_PER_SECOND = 0.00005; // 0.005% chance per spot per second (very rare)

    // Average occupancy duration: 1 minute (60 seconds)
    private static final long AVG_OCCUPANCY_DURATION_SECONDS = 60;
    // Variance: +/- 30 seconds (so 30s to 90s duration)
    private static final long OCCUPANCY_DURATION_VARIANCE_SECONDS = 30;

    // Average malfunction duration: 5 minutes (300 seconds)
    private static final long AVG_MALFUNCTION_DURATION_SECONDS = 5 * 60;
    // Variance: +/- 2 minutes (so 3 to 7 minutes duration)
    private static final long MALFUNCTION_DURATION_VARIANCE_SECONDS = 2 * 60;

    // In-memory state for each parking spot
    private static final Map<String, ParkingSlotState> parkingSlotStates = new ConcurrentHashMap<>();

    private KafkaProducer<String, String> producer;

    public SensorDataProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        this.producer = new KafkaProducer<>(props);

        // Initialize all parking slots to "free"
        for (String lotName : PARKING_LOT_NAMES) {
            for (int i = 1; i <= NUM_SLOTS_PER_LOT; i++) {
                String slotKey = lotName + "-" + i;
                parkingSlotStates.put(slotKey, new ParkingSlotState(lotName, i, "free"));
            }
        }
    }

    private void simulateAndSendUpdates() {
        long currentTimeMillis = System.currentTimeMillis();
        // The simulation interval is 100ms. We need to convert per-second probabilities
        // to per-interval probabilities.
        double intervalSeconds = 100.0 / 1000.0; // 0.1 seconds

        for (Map.Entry<String, ParkingSlotState> entry : parkingSlotStates.entrySet()) {
            ParkingSlotState slot = entry.getValue();
            String oldStatus = slot.getStatus(); // Store old status for comparison

            // --- Malfunction Logic ---
            if (slot.getStatus().equals("malfunction")) {
                if (currentTimeMillis >= slot.getEventEndTimeMillis()) {
                    // Malfunction duration over, return to previous state (e.g., free)
                    System.out.println("Slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " malfunction ended.");
                    slot.setStatus("free");
                    slot.setVehicleLicensePlate(null); // No car in a malfunctioning spot
                    slot.setEventEndTimeMillis(0); // Reset
                }
            } else if (random.nextDouble() < MALFUNCTION_PROBABILITY_PER_SECOND * intervalSeconds) { // Adjusted probability
                // Random chance for a spot to malfunction
                System.out.println("Slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " went into malfunction!");
                slot.setStatus("malfunction");
                slot.setVehicleLicensePlate(null); // No car in a malfunctioning spot
                slot.setEventEndTimeMillis(currentTimeMillis + generateRandomDurationMillis(AVG_MALFUNCTION_DURATION_SECONDS, MALFUNCTION_DURATION_VARIANCE_SECONDS));
            }

            // --- Occupancy Logic (only if not in malfunction) ---
            if (!slot.getStatus().equals("malfunction")) { // Ensure we don't try to occupy a malfunctioning spot
                if (slot.getStatus().equals("occupied")) {
                    if (currentTimeMillis >= slot.getEventEndTimeMillis()) {
                        // Car leaves
                        System.out.println("Car left slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " (" + slot.getVehicleLicensePlate() + ").");
                        slot.setStatus("free");
                        slot.setVehicleLicensePlate(null);
                        slot.setEventEndTimeMillis(0); // Reset
                    }
                } else if (slot.getStatus().equals("free")) {
                    if (random.nextDouble() < ARRIVAL_PROBABILITY_PER_SECOND * intervalSeconds) { // Adjusted probability
                        // Car arrives
                        String licensePlate = LICENSE_PLATES[random.nextInt(LICENSE_PLATES.length)];
                        System.out.println("Car arrived at slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " (" + licensePlate + ").");
                        slot.setStatus("occupied");
                        slot.setVehicleLicensePlate(licensePlate);
                        slot.setEventEndTimeMillis(currentTimeMillis + generateRandomDurationMillis(AVG_OCCUPANCY_DURATION_SECONDS, OCCUPANCY_DURATION_VARIANCE_SECONDS));
                    }
                }
            }

            // If status changed, send an update
            if (!oldStatus.equals(slot.getStatus())) {
                sendParkingEvent(slot);
            }
        }
    }

    private long generateRandomDurationMillis(long averageSeconds, long varianceSeconds) {
        long baseDuration = averageSeconds * 1000;
        long variance = varianceSeconds * 1000;
        // Generate a random duration within [average - variance, average + variance]
        // Ensure minimum duration is positive
        return Math.max(1000, baseDuration + (long) (random.nextDouble() * 2 * variance) - variance);
    }


    private void sendParkingEvent(ParkingSlotState slot) {
        JSONObject sensorData = new JSONObject();
        sensorData.put("parking_lot_name", slot.getParkingLotName());
        sensorData.put("slot_id", slot.getSlotId());
        sensorData.put("timestamp", Instant.now().toString());
        sensorData.put("status", slot.getStatus());

        // Duration in seconds (approximated remaining duration for occupied/malfunction states)
        long durationMillisRemaining = slot.getEventEndTimeMillis() - System.currentTimeMillis();
        if (slot.getStatus().equals("occupied") || slot.getStatus().equals("malfunction")) {
            sensorData.put("duration", Math.max(0, (int)(durationMillisRemaining / 1000))); // Remaining duration in seconds
        } else {
            sensorData.put("duration", 0); // Not applicable for free spots
        }

        sensorData.put("temperature", 20 + random.nextInt(15)); // Still random temp

        // Vehicle license plate is only relevant for "occupied" status
        if (slot.getVehicleLicensePlate() != null && slot.getStatus().equals("occupied")) {
            sensorData.put("vehicle_license_plate", slot.getVehicleLicensePlate());
        } else {
            sensorData.put("vehicle_license_plate", null); // Explicitly send JSON null if not occupied
        }

        String jsonString = sensorData.toJSONString();
        String recordKey = slot.getParkingLotName() + "-" + slot.getSlotId();
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, recordKey, jsonString);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                // Uncomment for detailed log of sent records
                // System.out.println("Sent record for " + recordKey + " with status " + slot.getStatus() + " to topic " + metadata.topic() + " offset " + metadata.offset());
            } else {
                System.err.println("Error sending record for " + recordKey + ": " + exception.getMessage());
                exception.printStackTrace();
            }
        });
    }

    public void startSimulation() {
        System.out.println("Starting sensor data simulation. Sending data to topic: " + KAFKA_TOPIC);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // Run simulation every 100ms
        scheduler.scheduleAtFixedRate(this::simulateAndSendUpdates, 0, 100, TimeUnit.MILLISECONDS);

        // Add a shutdown hook to flush and close producer on exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down sensor data producer...");
            producer.flush();
            producer.close();
            scheduler.shutdownNow();
            System.out.println("Sensor data producer stopped.");
        }));
    }

    // --- Inner Class to hold parking slot state ---
    private static class ParkingSlotState {
        private String parkingLotName;
        private int slotId;
        private String status; // "free", "occupied", "malfunction"
        private String vehicleLicensePlate;
        private long eventEndTimeMillis; // Timestamp when current state (occupied/malfunction) should end

        public ParkingSlotState(String parkingLotName, int slotId, String status) {
            this.parkingLotName = parkingLotName;
            this.slotId = slotId;
            this.status = status;
            this.vehicleLicensePlate = null;
            this.eventEndTimeMillis = 0;
        }

        // Getters and Setters
        public String getParkingLotName() { return parkingLotName; }
        public int getSlotId() { return slotId; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getVehicleLicensePlate() { return vehicleLicensePlate; }
        public void setVehicleLicensePlate(String vehicleLicensePlate) { this.vehicleLicensePlate = vehicleLicensePlate; }
        public long getEventEndTimeMillis() { return eventEndTimeMillis; }
        public void setEventEndTimeMillis(long eventEndTimeMillis) { this.eventEndTimeMillis = eventEndTimeMillis; }
    }


    public static void main(String[] args) {
        new SensorDataProducer().startSimulation();
    }
}