package com.smartparking.webapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class SimpleWebServer {

    private static final int PORT = 8085;
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "parking-sensor-data";
    private static final String KAFKA_GROUP = "parking-status-aggregator";
    private static final String KEYSPACE = "parking";
    private static final String SUB_TABLE = "subscriber";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, ParkingSlotState> parkingState = new ConcurrentHashMap<>();
    private static CqlSession cassSession;

    public static void main(String[] args) throws IOException {
        cassSession = CqlSession.builder()
                .withKeyspace(KEYSPACE)
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        startKafkaConsumer();

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/api/parking-status", new ParkingStatusHandler());
        server.createContext("/api/subscribe", new SubscribeHandler());
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();
        System.out.println("HTTP server running on http://localhost:" + PORT);
    }

    private static void startKafkaConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        new Thread(() -> {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                c.subscribe(Collections.singletonList(KAFKA_TOPIC));
                while (true) {
                    for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(100))) {
                        JsonNode j = MAPPER.readTree(r.value());
                        String lot = j.get("parking_lot_name").asText();
                        int id = j.get("slot_id").asInt();
                        String st = j.get("status").asText();
                        String plate = j.path("vehicle_license_plate").isNull() ? null : j.get("vehicle_license_plate").asText();
                        int dur = j.path("duration").asInt(0);
                        int temp = j.path("temperature").asInt(0);
                        parkingState.put(lot + "-" + id, new ParkingSlotState(lot, id, st, plate, dur, temp));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "KafkaConsumerThread").start();
    }

    static class ParkingStatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
            cors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1);
                return;
            }

            ArrayNode arr = MAPPER.createArrayNode();

            try {
                String query = "SELECT sensor_id, parking_lot_name, slot_id, status, last_updated FROM parking.parking_spot_current_status";
                cassSession.execute(query).forEach(row -> {
                    ObjectNode n = MAPPER.createObjectNode();
                    n.put("sensorId", row.getString("sensor_id"));
                    n.put("parkingLotName", row.getString("parking_lot_name"));
                    n.put("slotId", row.getInt("slot_id"));
                    n.put("status", row.getString("status"));
                    n.put("lastUpdated", row.getInstant("last_updated").toString());
                    arr.add(n);
                });

                byte[] json = arr.toString().getBytes();
                ex.getResponseHeaders().set("Content-Type", "application/json");
                ex.sendResponseHeaders(200, json.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(json);
                }

            } catch (Exception e) {
                e.printStackTrace();
                ex.sendResponseHeaders(500, 0);
                ex.getResponseBody().write(("Error fetching data: " + e.getMessage()).getBytes());
            }
        }
    }


    static class SubscribeHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            cors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1);
                return;
            }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(405, -1);
                return;
            }

            try {
                JsonNode b = MAPPER.readTree(ex.getRequestBody());
                String email = b.get("email").asText();
                String name = b.path("name").asText("");
                boolean alerts = b.path("subscribed_to_parking_alerts").asBoolean(false);
                boolean news = b.path("subscribed_to_newsletter").asBoolean(false);

                if (email.isBlank()) {
                    ex.sendResponseHeaders(400, 0);
                    ex.getResponseBody().write("Email required".getBytes());
                    return;
                }

                String row = String.format("%s,%s,%b,%b,%s%n", email, name, alerts, news, Instant.now());
                Files.write(Paths.get("subscribers.csv"), row.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

                SimpleStatement stmt = SimpleStatement.builder(
                        "INSERT INTO " + SUB_TABLE + " (email,name,subscribed_to_parking_alerts,subscribed_to_newsletter,created_at) VALUES (?,?,?,?,?)")
                        .addPositionalValues(email, name, alerts, news, Instant.now())
                        .build();
                cassSession.execute(stmt);

                byte[] ok = "{\"success\":true}".getBytes();
                ex.getResponseHeaders().set("Content-Type", "application/json");
                ex.sendResponseHeaders(200, ok.length);
                ex.getResponseBody().write(ok);
            } catch (Exception e) {
                e.printStackTrace();
                ex.sendResponseHeaders(500, 0);
                ex.getResponseBody().write(e.getMessage().getBytes());
            }
        }
    }

    private static void cors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type,Authorization");
        ex.getResponseHeaders().set("Access-Control-Max-Age", "3600");
    }

    static class ParkingSlotState {
        final String lot;
        final int id;
        final String status;
        final String plate;
        final int duration;
        final int temp;

        ParkingSlotState(String l, int i, String s, String p, int d, int t) {
            lot = l;
            id = i;
            status = s;
            plate = p;
            duration = d;
            temp = t;
        }
    }
}
