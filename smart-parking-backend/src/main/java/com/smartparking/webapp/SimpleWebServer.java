package main.java.com.smartparking.webapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList; // Added for list conversion

public class SimpleWebServer {

    private static final int PORT = 8085;
    private static final String API_PATH = "/api/parking-status";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "parking-sensor-data";
    private static final String KAFKA_GROUP_ID = "parking-status-aggregator";

    // In-memory state for parking spots, updated by Kafka Consumer
    private static final Map<String, ParkingSlotState> parkingSlotStates = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        System.out.println("Starting Simple Parking Backend...");

        startKafkaConsumer(); // Start Kafka Consumer in a separate thread

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext(API_PATH, new ParkingStatusHandler());
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();

        System.out.println("HTTP Server started on port " + PORT + ". Access the API at http://localhost:" + PORT + API_PATH);
        System.out.println("Ensure Kafka producer is sending data to topic '" + KAFKA_TOPIC + "' at " + KAFKA_BROKERS);
        System.out.println("Now, open the 'index.html' file in your web browser.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down SimpleWebServer...");
            if (server != null) {
                server.stop(0);
            }
            System.out.println("SimpleWebServer stopped.");
        }));
    }

    private static void startKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
                System.out.println("Kafka Consumer started, subscribed to topic: " + KAFKA_TOPIC);

                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            JsonNode jsonNode = objectMapper.readTree(record.value());
                            String parkingLotName = jsonNode.get("parking_lot_name").asText();
                            int slotId = jsonNode.get("slot_id").asInt();
                            String status = jsonNode.get("status").asText();
                            String vehicleLicensePlate = jsonNode.has("vehicle_license_plate") && !jsonNode.get("vehicle_license_plate").isNull() ? jsonNode.get("vehicle_license_plate").asText() : null;
                            int duration = jsonNode.has("duration") ? jsonNode.get("duration").asInt() : 0; // Get duration
                            int temperature = jsonNode.has("temperature") ? jsonNode.get("temperature").asInt() : 0; // Get temperature

                            String slotKey = parkingLotName + "-" + slotId;
                            // Updated ParkingSlotState constructor
                            ParkingSlotState state = new ParkingSlotState(parkingLotName, slotId, status, vehicleLicensePlate, duration, temperature);
                            parkingSlotStates.put(slotKey, state);

                            // System.out.println("Received: " + parkingLotName + " S" + slotId + " -> " + status + (vehicleLicensePlate != null ? " (" + vehicleLicensePlate + ")" : ""));

                        } catch (Exception e) {
                            System.err.println("Error parsing Kafka message: " + record.value() + " - " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Kafka Consumer error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("Kafka Consumer stopped.");
            }
        }, "KafkaConsumerThread").start();
    }


   static class ParkingStatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Set CORS headers
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1); // No Content for OPTIONS
                return;
            }

            // --- Construct JSON response from current parkingSlotStates map ---
            ArrayNode jsonArray = objectMapper.createArrayNode(); // <--- Creates a JSON array
            for (ParkingSlotState slot : parkingSlotStates.values()) {
                ObjectNode slotNode = objectMapper.createObjectNode(); // <--- Creates a JSON object for each slot
                slotNode.put("parkingLotName", slot.getParkingLotName());
                slotNode.put("slotId", slot.getSlotId());
                slotNode.put("status", slot.getStatus());
                slotNode.put("duration", slot.getDuration());
                slotNode.put("temperature", slot.getTemperature());

                if (slot.getVehicleLicensePlate() != null) {
                    slotNode.put("vehicle_license_plate", slot.getVehicleLicensePlate());
                } else {
                    slotNode.putNull("vehicle_license_plate");
                }
                jsonArray.add(slotNode); // Adds the slot object to the array
            }

            String jsonResponse = jsonArray.toString(); // <--- Converts the JSON array to a JSON string

            exchange.getResponseHeaders().set("Content-Type", "application/json"); // <--- Sets the Content-Type header to application/json
            exchange.sendResponseHeaders(200, jsonResponse.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(jsonResponse.getBytes());
            }
        }
    }

    // --- Updated Inner Class to hold parking slot state with more fields ---
    // Make this class public or add a default constructor for Jackson if you were serializing it directly.
    // For manual JSON construction as above, getters are sufficient.
    public static class ParkingSlotState { // Changed to public static
        private String parkingLotName;
        private int slotId;
        private String status; // "free", "occupied", "malfunction"
        private String vehicleLicensePlate;
        private int duration;    // Added duration
        private int temperature; // Added temperature

        // Updated constructor
        public ParkingSlotState(String parkingLotName, int slotId, String status, String vehicleLicensePlate, int duration, int temperature) {
            this.parkingLotName = parkingLotName;
            this.slotId = slotId;
            this.status = status;
            this.vehicleLicensePlate = vehicleLicensePlate;
            this.duration = duration;
            this.temperature = temperature;
        }

        // Getters (needed for JSON serialization or access)
        public String getParkingLotName() { return parkingLotName; }
        public int getSlotId() { return slotId; }
        public String getStatus() { return status; }
        public String getVehicleLicensePlate() { return vehicleLicensePlate; }
        public int getDuration() { return duration; }       // New getter
        public int getTemperature() { return temperature; } // New getter

        // Setters (if internal state needs to be modified after creation, not strictly needed if always creating new states)
        // public void setStatus(String status) { this.status = status; }
        // public void setVehicleLicensePlate(String vehicleLicensePlate) { this.vehicleLicensePlate = vehicleLicensePlate; }
        // public void setDuration(int duration) { this.duration = duration; }
        // public void setTemperature(int temperature) { this.temperature = temperature; }
    }
}