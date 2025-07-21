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

// Importe të reja për dërgimin e email-eve dhe detyrat e planifikuara
import jakarta.mail.*;
import jakarta.mail.internet.*;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleWebServer {

    private static final int PORT = 8085;
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "parking-sensor-data";
    private static final String KAFKA_GROUP = "parking-status-aggregator";
    private static final String KEYSPACE = "parking";
    private static final String SUB_TABLE = "subscriber";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SENDER_EMAIL = "albinhashani06@gmail.com";
    private static final String SENDER_EMAIL_PASSWORD = "htipmcweqnxukxme";
    private static final String EMPLOYEE_EMAIL = "albinhashani02@gmail.com";

    private static ExecutorService emailSenderExecutor;
    private static ScheduledExecutorService alertScheduler;

    private static final ConcurrentHashMap<String, String> lastNotifiedLotStatus = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> notifiedFailedSensors = new ConcurrentHashMap<>();

    private static final double NEAR_FULL_THRESHOLD_PERCENT = 0.80;
    private static final double VERY_HIGH_CAPACITY_THRESHOLD_PERCENT = 0.93;
    private static final double FULL_THRESHOLD_PERCENT = 1.00;

    private static final Map<String, ParkingSlotState> parkingState = new ConcurrentHashMap<>();
    private static CqlSession cassSession;

    private static AtomicReference<ArrayNode> cachedParkingStatus = new AtomicReference<>(MAPPER.createArrayNode());
    private static volatile long lastCacheUpdateTime = 0;
    private static final long CACHE_EXPIRATION_MILLIS = 5 * 1000;

    public static void main(String[] args) throws IOException {
        cassSession = CqlSession.builder()
                .withKeyspace(KEYSPACE)
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        startKafkaConsumer();

        emailSenderExecutor = Executors.newFixedThreadPool(2);
        alertScheduler = Executors.newScheduledThreadPool(1);

        alertScheduler.scheduleAtFixedRate(new AlertCheckTask(), 10, 5 * 60, TimeUnit.SECONDS);

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/api/parking-status", new ParkingStatusHandler());
        server.createContext("/api/subscribe", new SubscribeHandler());
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();
        System.out.println("HTTP server running on http://localhost:" + PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down executors...");
            emailSenderExecutor.shutdown();
            alertScheduler.shutdown();
            try {
                if (!emailSenderExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    emailSenderExecutor.shutdownNow();
                }
                if (!alertScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    alertScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                emailSenderExecutor.shutdownNow();
                alertScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            if (cassSession != null) {
                cassSession.close();
                System.out.println("Cassandra session closed.");
            }
            System.out.println("HTTP server stopped.");
        }));
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
                        // Kujdes: Sigurohuni që këto fusha ekzistojnë në payload-in e Kafka-s
                        String lot = j.has("parking_lot_name") ? j.get("parking_lot_name").asText() : "UNKNOWN_LOT";
                        int id = j.has("slot_id") ? j.get("slot_id").asInt() : -1;
                        String st = j.has("status") ? j.get("status").asText() : "unknown";
                        String plate = j.path("vehicle_license_plate").isNull() ? null : j.get("vehicle_license_plate").asText();
                        int dur = j.path("duration").asInt(0);
                        double temp = j.path("temperature").asDouble(0.0);
                        parkingState.put(lot + "-" + id, new ParkingSlotState(lot, id, st, plate, dur, temp));
                    }
                }
            } catch (Exception e) {
                System.err.println("Kafka consumer error: " + e.getMessage());
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

            ArrayNode currentStatusArray;
            long currentTime = System.currentTimeMillis();

            if (currentTime - lastCacheUpdateTime > CACHE_EXPIRATION_MILLIS) {
                System.out.println("Faqja e statusit: Cache ka skaduar, rifreskoj nga Cassandra.");
                try {
                    ArrayNode arr = MAPPER.createArrayNode();
                    // Sigurohu që `parking_lot_name` ekziston në tabelën Cassandra
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
                    cachedParkingStatus.set(arr);
                    lastCacheUpdateTime = currentTime;
                    currentStatusArray = arr;
                } catch (Exception e) {
                    System.err.println("Error fetching data for parking status API and refreshing cache: " + e.getMessage());
                    e.printStackTrace();
                    currentStatusArray = cachedParkingStatus.get();
                    if (currentStatusArray == null || currentStatusArray.isEmpty()) {
                        ex.sendResponseHeaders(500, 0);
                        ex.getResponseBody().write(("Error fetching data: " + e.getMessage()).getBytes());
                        return;
                    }
                }
            } else {
                System.out.println("Faqja e statusit: Po perdor cache-in.");
                currentStatusArray = cachedParkingStatus.get();
            }

            try {
                byte[] json = currentStatusArray.toString().getBytes();
                ex.getResponseHeaders().set("Content-Type", "application/json");
                ex.sendResponseHeaders(200, json.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(json);
                }
            } catch (Exception e) {
                System.err.println("Error sending cached data for parking status API: " + e.getMessage());
                e.printStackTrace();
                ex.sendResponseHeaders(500, 0);
                ex.getResponseBody().write(("Error sending data: " + e.getMessage()).getBytes());
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

                // Shkrimi në CSV mund të jetë opsional nëse përdorni vetëm Cassandra
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
                System.err.println("Error during subscribe API call: " + e.getMessage());
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
        final double temp;

        ParkingSlotState(String l, int i, String s, String p, int d, double t) {
            lot = l;
            id = i;
            status = s;
            plate = p;
            duration = d;
            temp = t;
        }
    }

    static class EmailSender {
        public static void send(String toEmail, String subject, String body) {
            Properties props = new Properties();
            props.put("mail.smtp.host", "smtp.gmail.com");
            props.put("mail.smtp.port", "587");
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");

            Session session = Session.getInstance(props, new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(SENDER_EMAIL, SENDER_EMAIL_PASSWORD);
                }
            });

            try {
                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(SENDER_EMAIL));
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail));
                message.setSubject(subject);
                message.setText(body);

                Transport.send(message);
                System.out.println("Email sent successfully to " + toEmail + " with subject: " + subject);

            } catch (MessagingException e) {
                System.err.println("Failed to send email to " + toEmail + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Detyra e planifikuar që kontrollon periodikisht statusin e parkingut dhe dërgon alarme.
     */
    static class AlertCheckTask implements Runnable {
        @Override
        public void run() {
            System.out.println("\n--- Duke ekzekutuar kontrollin e alarmeve në: " + LocalDateTime.now() + " ---");
            try {
                String parkingQuery = "SELECT sensor_id, parking_lot_name, slot_id, status FROM parking.parking_spot_current_status";
                List<com.datastax.oss.driver.api.core.cql.Row> parkingRows = cassSession.execute(parkingQuery).all();

                if (parkingRows.isEmpty()) {
                    System.out.println("Nuk ka të dhëna për parkingun në Cassandra. Nuk mund të llogaris statusin e parkingut.");
                }

                String subQuery = "SELECT email, subscribed_to_parking_alerts FROM parking.subscriber WHERE subscribed_to_parking_alerts = true ALLOW FILTERING";
                List<com.datastax.oss.driver.api.core.cql.Row> subscriberRows = cassSession.execute(subQuery).all();
                Set<String> alertSubscribers = new HashSet<>();
                for (com.datastax.oss.driver.api.core.cql.Row row : subscriberRows) {
                    alertSubscribers.add(row.getString("email"));
                }
                System.out.println("Abonentë aktivë për paralajmërimet (Cassandra): " + alertSubscribers.size() + " (" + (alertSubscribers.isEmpty() ? "Asnje" : String.join(", ", alertSubscribers)) + ")");

                if (alertSubscribers.isEmpty()) {
                    System.out.println("Nuk ka asnjë abonent aktiv për alarme parkimi. Nuk dërgohen emaile për statusin e parkingut.");
                }

                Map<String, LotStatusCounts> lotCounts = new HashMap<>();
                Set<String> currentFailedSensors = new HashSet<>();

                for (com.datastax.oss.driver.api.core.cql.Row row : parkingRows) {
                    String lotName = row.getString("parking_lot_name");
                    String status = row.getString("status");
                    String sensorId = row.getString("sensor_id");

                    // Kujdes: Sigurohuni që lotName nuk është null ose bosh
                    if (lotName == null || lotName.trim().isEmpty()) {
                        System.err.println("Warning: Rresht i Cassandra-s me parking_lot_name bosh ose null për sensor_id: " + sensorId + ". Duke e anashkaluar.");
                        continue;
                    }

                    lotCounts.computeIfAbsent(lotName, k -> new LotStatusCounts());
                    LotStatusCounts counts = lotCounts.get(lotName);
                    counts.totalSlots++;

                    if ("occupied".equals(status)) {
                        counts.occupiedSlots++;
                    } else if ("malfunction".equals(status)) {
                        counts.malfunctionSlots++;
                    } else if ("sensor_failure".equals(status)) {
                        counts.failureSlots++;
                        currentFailedSensors.add(sensorId);
                    }
                }

                // Logjika e emailit për abonentët
                for (Map.Entry<String, LotStatusCounts> entry : lotCounts.entrySet()) {
                    String lotName = entry.getKey();
                    LotStatusCounts counts = entry.getValue();

                    if (counts.totalSlots == 0) {
                        System.out.println(String.format("Parkingu '%s' nuk ka vende të regjistruara. Nuk llogaritet problemPercentage.", lotName));
                        continue;
                    }

                    double problemPercentage = (double) (counts.occupiedSlots + counts.malfunctionSlots + counts.failureSlots) / counts.totalSlots;
                   
                    System.out.println(String.format("Statusi Parkingu: %s, Totali Vende: %d, Vende Problematike (zënë/difekt): %d, Problematike Perqindja: %.2f%%",
                                                     lotName, counts.totalSlots, (counts.occupiedSlots + counts.malfunctionSlots + counts.failureSlots), problemPercentage * 100));


                    NotificationStatus currentLotStatus = calculateNotificationStatus(problemPercentage);
                    String lastNotifiedStatus = lastNotifiedLotStatus.getOrDefault(lotName, NotificationStatus.NONE.toString());

                    // Kontrollon nëse statusi i njoftimit ka ndryshuar
                    if (!currentLotStatus.toString().equals(lastNotifiedStatus)) {
                        String subject = "";
                        String body = "";

                        switch (currentLotStatus) {
                            case FULL:
                                subject = "ALARM: Parkingu " + lotName + " eshte TANI I PLOTË!";
                                body = String.format("I/E dashur abonent,\n\nParkingu '%s' eshte aktualisht I PLOTË (%.0f%% kapacitet, %d/%d vende te zëna/problematike).\n\nMe respekt,\nSistemi Smart Parking", lotName, problemPercentage * 100, counts.occupiedSlots + counts.malfunctionSlots + counts.failureSlots, counts.totalSlots);
                                break;
                            case VERY_HIGH_CAPACITY:
                                subject = "ALARM: Parkingu " + lotName + " arriti " + String.format("%.0f", problemPercentage * 100) + "%% kapacitet!";
                                body = String.format("I/E dashur abonent,\n\nParkingu '%s' ka arritur %.0f%% te kapacitetit (%d/%d vende te zëna/problematike).\n\nMe respekt,\nSistemi Smart Parking", lotName, problemPercentage * 100, counts.occupiedSlots + counts.malfunctionSlots + counts.failureSlots, counts.totalSlots);
                                break;
                            case NEAR_FULL:
                                subject = "NJOFTIM: Parkingu " + lotName + " eshte TANI AFËR I PLOTË!";
                                body = String.format("I/E dashur abonent,\n\nParkingu '%s' eshte afer i plote (%.0f%% kapacitet, %d/%d vende te zëna/problematike).\n\nMe respekt,\nSistemi Smart Parking", lotName, problemPercentage * 100, counts.occupiedSlots + counts.malfunctionSlots + counts.failureSlots, counts.totalSlots);
                                break;
                            case AVAILABLE:
                                // Dërgo njoftim "AVAILABLE" vetëm nëse më parë ka qenë në një status të zënë
                                if (lastNotifiedStatus.equals(NotificationStatus.FULL.toString()) ||
                                    lastNotifiedStatus.equals(NotificationStatus.NEAR_FULL.toString()) ||
                                    lastNotifiedStatus.equals(NotificationStatus.VERY_HIGH_CAPACITY.toString())) {
                                    subject = "PERDITESIM: Parkingu " + lotName + " eshte TANI I DISPONUESHËM!";
                                    body = String.format("I/E dashur abonent,\n\nParkingu '%s' tani ka me shume vende te lira (%.0f%% zënie, %d/%d vende te zëna/problematike).\n\nMe respekt,\nSistemi Smart Parking", lotName, problemPercentage * 100, counts.occupiedSlots + counts.malfunctionSlots + counts.failureSlots, counts.totalSlots);
                                } else {
                                    System.out.println(String.format("Parkingu '%s' është AVAILBLE, por nuk ka ndryshuar nga një status zënie më i lartë. Nuk dërgohet email për abonentët.", lotName));
                                }
                                break;
                            case NONE:
                                // Kjo nuk duhet te ndodhe shpesh pas inicializimit, por e kemi si fallback
                                System.out.println(String.format("Statusi i llogaritur per parkungun '%s' eshte NONE. Nuk dërgohet email.", lotName));
                                break;
                        }

                        if (!subject.isEmpty() && !alertSubscribers.isEmpty()) {
                            System.out.println(String.format("Tentativë për të dërguar email për Lot: %s, Statusi: %s. Subjekti: %s", lotName, currentLotStatus.toString(), subject));
                            for (String email : alertSubscribers) {
                                final String finalSubject = subject;
                                final String finalBody = body;
                                emailSenderExecutor.submit(() -> EmailSender.send(email, finalSubject, finalBody));
                            }
                            lastNotifiedLotStatus.put(lotName, currentLotStatus.toString());
                        } else if (subject.isEmpty()) {
                            System.out.println(String.format("Nuk dërgohet email për Lot: %s, Statusi: %s, Last Notified: %s. Nuk ka subjekt (mund te jete AVAILABLE pa ndryshim te madh).", lotName, currentLotStatus.toString(), lastNotifiedStatus));
                        } else if (alertSubscribers.isEmpty()) {
                            System.out.println(String.format("Nuk ka abonentë aktivë për Lot: %s. Nuk dërgohet email.", lotName));
                        }
                    } else {
                        System.out.println(String.format("Lot: %s, Statusi: %s nuk ndryshoi nga i fundit i njoftuar. Nuk dërgohet email për abonentët.", lotName, currentLotStatus.toString()));
                    }
                }

                // Logjika e emailit për punonjësit (dështimi/rregullimi i sensorëve) - kjo pjesë ka funksionuar
                Set<String> newlyFailedSensors = new HashSet<>();
                for (String sensorId : currentFailedSensors) {
                    if (!notifiedFailedSensors.containsKey(sensorId) || !notifiedFailedSensors.get(sensorId)) {
                        newlyFailedSensors.add(sensorId);
                    }
                }

                Set<String> fixedSensors = new HashSet<>();
                for (String notifiedSensorId : notifiedFailedSensors.keySet()) {
                    if (notifiedFailedSensors.get(notifiedSensorId) && !currentFailedSensors.contains(notifiedSensorId)) {
                        fixedSensors.add(notifiedSensorId);
                    }
                }

                if (!newlyFailedSensors.isEmpty()) {
                    StringBuilder bodyBuilder = new StringBuilder("I/E dashur punonjës,\n\nSensorët e mëposhtëm të parkimit kanë raportuar statusin 'sensor_failure':\n");
                    for (String sensorId : newlyFailedSensors) {
                        bodyBuilder.append("  - Sensor ID: ").append(sensorId).append("\n");
                    }
                    bodyBuilder.append("\nJu lutemi, hetoni këta sensorë menjëherë.\n\nMe respekt,\nSistemi Smart Parking");

                    final String subject = "URGENT: Dështime të Sensorëve të Parkingut të Detektuara!";
                    final String body = bodyBuilder.toString();
                    emailSenderExecutor.submit(() -> EmailSender.send(EMPLOYEE_EMAIL, subject, body));
                    // Përditëso notifiedFailedSensors pasi emaili dërgohet
                    newlyFailedSensors.forEach(sensorId -> notifiedFailedSensors.put(sensorId, true));
                    System.out.println("Dërguar email për sensorët e rinj të dështuar.");
                }

                if (!fixedSensors.isEmpty()) {
                    StringBuilder bodyBuilder = new StringBuilder("I/E dashur punonjës,\n\nSensorët e mëposhtëm të parkimit janë RREGULLUAR:\n");
                    for (String sensorId : fixedSensors) {
                        bodyBuilder.append("  - Sensor ID: ").append(sensorId).append("\n");
                    }
                    bodyBuilder.append("\nJu lutemi, verifikoni rregullimin.\n\nMe respekt,\nSistemi Smart Parking");

                    final String subject = "Përditësim: Sensorët e Parkingut janë RREGULLUAR!";
                    final String body = bodyBuilder.toString();
                    emailSenderExecutor.submit(() -> EmailSender.send(EMPLOYEE_EMAIL, subject, body));
                    // Përditëso notifiedFailedSensors pasi emaili dërgohet
                    fixedSensors.forEach(sensorId -> notifiedFailedSensors.remove(sensorId));
                    System.out.println("Dërguar email për sensorët e rregulluar.");
                }

            } catch (Exception e) {
                System.err.println("Gabim gjatë kontrollit të alarmit: " + e.getMessage());
                e.printStackTrace();
            } finally {
                 System.out.println("--- Kontrolli i alarmeve i përfunduar. ---");
            }
        }

        private enum NotificationStatus {
            FULL,
            VERY_HIGH_CAPACITY,
            NEAR_FULL,
            AVAILABLE,
            NONE
        }

        private NotificationStatus calculateNotificationStatus(double problemPercentage) {
            if (problemPercentage >= FULL_THRESHOLD_PERCENT) {
                return NotificationStatus.FULL;
            } else if (problemPercentage >= VERY_HIGH_CAPACITY_THRESHOLD_PERCENT) {
                return NotificationStatus.VERY_HIGH_CAPACITY;
            } else if (problemPercentage >= NEAR_FULL_THRESHOLD_PERCENT) {
                return NotificationStatus.NEAR_FULL;
            } else {
                return NotificationStatus.AVAILABLE;
            }
        }
    }

    static class LotStatusCounts {
        int totalSlots = 0;
        int occupiedSlots = 0;
        int malfunctionSlots = 0;
        int failureSlots = 0;
    }
}