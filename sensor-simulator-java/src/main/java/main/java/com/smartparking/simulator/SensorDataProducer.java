package main.java.com.smartparking.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;

import java.time.Instant;
import java.time.LocalTime;
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

    // Sample license plates
    private static final String[] LICENSE_PLATES = {"ABC-123", "XYZ-789", "DEF-456", "MNO-007", "QWE-111", "RTY-222", "IOP-333"};

    // --- Base malfunction probability per second per spot ---
    private static final double MALFUNCTION_PROBABILITY_PER_SECOND = 0.00005;

    // Average occupancy duration and variance (seconds)
    private static final long AVG_OCCUPANCY_DURATION_SECONDS = 60;
    private static final long OCCUPANCY_DURATION_VARIANCE_SECONDS = 30;

    // Average malfunction duration and variance (seconds)
    private static final long AVG_MALFUNCTION_DURATION_SECONDS = 5 * 60;
    private static final long MALFUNCTION_DURATION_VARIANCE_SECONDS = 2 * 60;

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

        for (String lotName : PARKING_LOT_NAMES) {
            for (int i = 1; i <= NUM_SLOTS_PER_LOT; i++) {
                String slotKey = lotName + "-" + i;
                parkingSlotStates.put(slotKey, new ParkingSlotState(lotName, i, "free"));
            }
        }
    }

    private double getTimeBasedArrivalProbability() {
        // Define time ranges in hours (24h format)
        LocalTime time = LocalTime.now();
        int hour = time.getHour();

        // Base probabilities by time of day (per second)
        // Night low traffic: 0.005
        // Morning rush (7-9): 0.05
        // Midday normal (10-16): 0.015
        // Evening rush (17-19): 0.045
        // Late evening (20-23): 0.01

        if (hour >= 7 && hour < 9) {
            return 0.05;
        } else if (hour >= 17 && hour < 19) {
            return 0.045;
        } else if (hour >= 10 && hour < 17) {
            return 0.015;
        } else if (hour >= 20 && hour < 24) {
            return 0.01;
        } else {
            return 0.005; // Night & early morning low traffic
        }
    }

    private void simulateAndSendUpdates() {
        long currentTimeMillis = System.currentTimeMillis();
        double intervalSeconds = 100.0 / 1000.0; // 0.1 seconds interval

        double baseArrivalProb = getTimeBasedArrivalProbability();

        // Add small ±10% randomness to arrival probability per tick
        double arrivalProb = baseArrivalProb * (0.9 + 0.2 * random.nextDouble());
        // Add small ±20% randomness to malfunction probability per tick
        double malfunctionProb = MALFUNCTION_PROBABILITY_PER_SECOND * (0.8 + 0.4 * random.nextDouble());

        for (Map.Entry<String, ParkingSlotState> entry : parkingSlotStates.entrySet()) {
            ParkingSlotState slot = entry.getValue();
            String oldStatus = slot.getStatus();

            // Malfunction logic
            if ("malfunction".equals(slot.getStatus())) {
                if (currentTimeMillis >= slot.getEventEndTimeMillis()) {
                    System.out.println("Slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " malfunction ended.");
                    slot.setStatus("free");
                    slot.setVehicleLicensePlate(null);
                    slot.setEventEndTimeMillis(0);
                }
            } else if (random.nextDouble() < malfunctionProb * intervalSeconds) {
                System.out.println("Slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " went into malfunction!");
                slot.setStatus("malfunction");
                slot.setVehicleLicensePlate(null);
                slot.setEventEndTimeMillis(currentTimeMillis + generateRandomDurationMillis(AVG_MALFUNCTION_DURATION_SECONDS, MALFUNCTION_DURATION_VARIANCE_SECONDS));
            }

            // Occupancy logic
            if (!"malfunction".equals(slot.getStatus())) {
                if ("occupied".equals(slot.getStatus())) {
                    if (currentTimeMillis >= slot.getEventEndTimeMillis()) {
                        System.out.println("Car left slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " (" + slot.getVehicleLicensePlate() + ").");
                        slot.setStatus("free");
                        slot.setVehicleLicensePlate(null);
                        slot.setEventEndTimeMillis(0);
                    }
                } else if ("free".equals(slot.getStatus())) {
                    if (random.nextDouble() < arrivalProb * intervalSeconds) {
                        String licensePlate = LICENSE_PLATES[random.nextInt(LICENSE_PLATES.length)];
                        System.out.println("Car arrived at slot " + slot.getParkingLotName() + "-" + slot.getSlotId() + " (" + licensePlate + ").");
                        slot.setStatus("occupied");
                        slot.setVehicleLicensePlate(licensePlate);
                        slot.setEventEndTimeMillis(currentTimeMillis + generateRandomDurationMillis(AVG_OCCUPANCY_DURATION_SECONDS, OCCUPANCY_DURATION_VARIANCE_SECONDS));
                    }
                }
            }

            if (!oldStatus.equals(slot.getStatus())) {
                sendParkingEvent(slot);
            }
        }
    }

    private long generateRandomDurationMillis(long averageSeconds, long varianceSeconds) {
        long baseDuration = averageSeconds * 1000;
        long variance = varianceSeconds * 1000;
        return Math.max(1000, baseDuration + (long) (random.nextDouble() * 2 * variance) - variance);
    }

    private double simulateTemperature() {
        LocalTime time = LocalTime.now();
        double hour = time.getHour() + time.getMinute() / 60.0;
        double baseTemp = 22 + 7 * Math.sin((hour - 6) / 24 * 2 * Math.PI);
        double noise = (random.nextDouble() - 0.5); // ±0.5 degree
        return baseTemp + noise;
    }

    private void sendParkingEvent(ParkingSlotState slot) {
        JSONObject sensorData = new JSONObject();
        sensorData.put("parking_lot_name", slot.getParkingLotName());
        sensorData.put("slot_id", slot.getSlotId());
        sensorData.put("timestamp", Instant.now().toString());
        sensorData.put("status", slot.getStatus());

        long durationMillisRemaining = slot.getEventEndTimeMillis() - System.currentTimeMillis();
        if ("occupied".equals(slot.getStatus()) || "malfunction".equals(slot.getStatus())) {
            sensorData.put("duration", Math.max(0, (int) (durationMillisRemaining / 1000)));
        } else {
            sensorData.put("duration", 0);
        }

        sensorData.put("temperature", (int) Math.round(simulateTemperature()));

        if (slot.getVehicleLicensePlate() != null && "occupied".equals(slot.getStatus())) {
            sensorData.put("vehicle_license_plate", slot.getVehicleLicensePlate());
        } else {
            sensorData.put("vehicle_license_plate", null);
        }

        String jsonString = sensorData.toJSONString();
        String recordKey = slot.getParkingLotName() + "-" + slot.getSlotId();
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, recordKey, jsonString);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending record for " + recordKey + ": " + exception.getMessage());
                exception.printStackTrace();
            }
        });
    }

    public void startSimulation() {
        System.out.println("Starting sensor data simulation. Sending data to topic: " + KAFKA_TOPIC);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::simulateAndSendUpdates, 0, 100, TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down sensor data producer...");
            producer.flush();
            producer.close();
            scheduler.shutdownNow();
            System.out.println("Sensor data producer stopped.");
        }));
    }

    private static class ParkingSlotState {
        private final String parkingLotName;
        private final int slotId;
        private String status; // free, occupied, malfunction
        private String vehicleLicensePlate;
        private long eventEndTimeMillis;

        public ParkingSlotState(String parkingLotName, int slotId, String status) {
            this.parkingLotName = parkingLotName;
            this.slotId = slotId;
            this.status = status;
            this.vehicleLicensePlate = null;
            this.eventEndTimeMillis = 0;
        }

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
