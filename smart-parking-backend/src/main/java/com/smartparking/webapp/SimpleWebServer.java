package com.smartparking.webapp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class SimpleWebServer {
    private static final int PORT = 8085;
    private static final String API_PATH = "/api/parking-status";
    private static final String SUBSCRIBE_PATH = "/api/subscribe";

    // Email configuration
    private static final String SMTP_HOST = "smtp.gmail.com";
    private static final String SMTP_PORT = "587";
    private static final String EMAIL_USERNAME = "arijanaternava1@gmail.com";
    private static final String EMAIL_PASSWORD = "sekret";

    // In-memory state
    private static final Map<String, ParkingSlotState> parkingSlotStates = new ConcurrentHashMap<>();
    private static final List<OccupancyRecord> occupancyHistory = Collections.synchronizedList(new ArrayList<>());
    private static final Set<String> subscribers = Collections.synchronizedSet(new HashSet<>());
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Cassandra session and statements
    private static CqlSession cassandra;
    private static PreparedStatement selectCurrentStmt;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        // 1. Initialize Cassandra
        String cassHost = System.getenv().getOrDefault("CASSANDRA_HOST", "127.0.0.1");
        cassandra = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassHost, 9042))
                .withLocalDatacenter("datacenter1")
                .build();
        selectCurrentStmt = cassandra.prepare(
                "SELECT parking_lot_name, slot_id, status, last_updated FROM parking.parking_spot_current_status"
        );

        // 2. Load existing current status into memory
        ResultSet rs = cassandra.execute(selectCurrentStmt.bind());
        for (Row row : rs) {
            String lotName = row.getString("parking_lot_name");
            int slotId = row.getInt("slot_id");
            String status = row.getString("status");
            Instant lastUpdated = row.getInstant("last_updated");
            String key = lotName + "-" + slotId;
            parkingSlotStates.put(key, new ParkingSlotState(lotName, slotId, status, null, 0, 0, lastUpdated));
        }

        // 3. Scheduled tasks
        startAlertChecker();
        startPredictionAndEmailScheduler();

        // 4. HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext(API_PATH, new ParkingStatusHandler());
        server.createContext(SUBSCRIBE_PATH, new SubscribeHandler());
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();
        System.out.println("HTTP Server started on port " + PORT);
        System.out.println("Access the API at http://localhost:" + PORT + API_PATH);
        System.out.println("Data will be served directly from Cassandra-backed cache.");

        // 5. Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            if (cassandra != null) cassandra.close();
            scheduler.shutdown();
            server.stop(0);
        }));
    }

    private static void sendEmail(String subject, String body, String recipient) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", SMTP_HOST);
        props.put("mail.smtp.port", SMTP_PORT);
        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(EMAIL_USERNAME, EMAIL_PASSWORD);
            }
        });
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(EMAIL_USERNAME));
            msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
            msg.setSubject(subject);
            msg.setText(body);
            Transport.send(msg);
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }

    private static void startAlertChecker() {
        scheduler.scheduleAtFixedRate(() -> {
            int total = 50 * 3;
            int occ = 0, malf = 0, fail = 0;
            for (ParkingSlotState s : parkingSlotStates.values()) {
                switch (s.getStatus().toLowerCase()) {
                    case "occupied": occ++; break;
                    case "malfunction": malf++; break;
                    case "sensor_failure": fail++; break;
                }
            }
            double rate = total > 0 ? (100.0 * occ / total) : 0;
            synchronized (occupancyHistory) {
                occupancyHistory.add(new OccupancyRecord(System.currentTimeMillis(), rate));
                if (occupancyHistory.size() > 288) occupancyHistory.remove(0);
            }
            if (rate > 90)
                sendEmail("Alert: High Occupancy", String.format("Occupancy %.1f%%", rate), String.join(",", subscribers));
            if ((malf + fail) > total * 0.1)
                sendEmail("Alert: Malfunction Rate", String.format("Malfunctions %d", malf + fail), String.join(",", subscribers));
        }, 0, 5, TimeUnit.MINUTES);
    }

    private static void startPredictionAndEmailScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            double pred = predictOccupancyRate();
            int total = 50 * 3;
            int avail = (int) Math.round(total * (1 - pred / 100));
            String subj = "Hourly Prediction";
            String bdy = String.format("Next-hour occupancy: %.1f%%, available: %d", pred, avail);
            if (!subscribers.isEmpty())
                sendEmail(subj, bdy, String.join(",", subscribers));
        }, 0, 1, TimeUnit.HOURS);
    }

    private static double predictOccupancyRate() {
        synchronized (occupancyHistory) {
            int n = occupancyHistory.size();
            if (n < 12) return n > 0 ? occupancyHistory.get(n - 1).occupancyRate : 0;
            double[] x = new double[n], y = new double[n];
            for (int i = 0; i < n; i++) {
                x[i] = i;
                y[i] = occupancyHistory.get(i).occupancyRate;
            }
            double mx = Arrays.stream(x).average().orElse(0), my = Arrays.stream(y).average().orElse(0);
            double num = 0, den = 0;
            for (int i = 0; i < n; i++) {
                num += (x[i] - mx) * (y[i] - my);
                den += Math.pow(x[i] - mx, 2);
            }
            double slope = den != 0 ? num / den : 0;
            double inter = my - slope * mx;
            return Math.max(0, Math.min(100, slope * (n + 12) + inter));
        }
    }

    public static class ParkingStatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            ArrayNode jsonArray = objectMapper.createArrayNode();
            for (ParkingSlotState slot : parkingSlotStates.values()) {
                ObjectNode slotNode = objectMapper.createObjectNode();
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
                jsonArray.add(slotNode);
            }

            String jsonResponse = jsonArray.toString();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jsonResponse.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(jsonResponse.getBytes());
            }
        }
    }

    public static class SubscribeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(405, -1);
                return;
            }
            JsonNode j = objectMapper.readTree(ex.getRequestBody());
            String email = j.path("email").asText();
            ObjectNode resp = objectMapper.createObjectNode();
            if (email.matches("^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$")) {
                subscribers.add(email);
                resp.put("status", "subscribed");
                ex.sendResponseHeaders(200, 0);
            } else {
                resp.put("status", "error").put("error", "invalid email");
                ex.sendResponseHeaders(400, 0);
            }
            try (OutputStream os = ex.getResponseBody()) {
                os.write(resp.toString().getBytes());
            }
        }
    }

    public static class ParkingSlotState {
        private final String parkingLotName;
        private final int slotId;
        private final String status;
        private final String vehicleLicensePlate;
        private final int duration;
        private final int temperature;
        private final Instant lastUpdated;

        public ParkingSlotState(String pl, int si, String st, String vp, int du, int te, Instant lu) {
            this.parkingLotName = pl;
            this.slotId = si;
            this.status = st;
            this.vehicleLicensePlate = vp;
            this.duration = du;
            this.temperature = te;
            this.lastUpdated = lu;
        }

        public String getParkingLotName() { return parkingLotName; }
        public int getSlotId() { return slotId; }
        public String getStatus() { return status; }
        public String getVehicleLicensePlate() { return vehicleLicensePlate; }
        public int getDuration() { return duration; }
        public int getTemperature() { return temperature; }
        public Instant getLastUpdated() { return lastUpdated; }
    }

    private static class OccupancyRecord {
        final long timestamp;
        final double occupancyRate;
        OccupancyRecord(long t, double r) { timestamp = t; occupancyRate = r; }
    }
}
