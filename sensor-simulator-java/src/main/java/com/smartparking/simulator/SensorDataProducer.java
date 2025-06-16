package main.java.com.smartparking.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject; // For JSON creation

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class SensorDataProducer {

    private static final String KAFKA_TOPIC = "parking-sensor-data";
    private static final String KAFKA_BROKERS = "localhost:9092";

    private static final String[] PARKING_LOTS = {"LotA", "LotB", "LotC"};
    private static final int NUM_SLOTS_PER_LOT = 50;
    private static final String[] STATUSES = {"free", "occupied", "malfunction"};
    private static final Random random = new Random();

    // New: Sample license plates for simulation
    private static final String[] LICENSE_PLATES = {"ABC-123", "XYZ-789", "DEF-456", "MNO-007", "QWE-111", "RTY-222", "IOP-333"};

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            System.out.println("Starting sensor data simulation. Sending data to topic: " + KAFKA_TOPIC);
            while (true) {
                String parkingLot = PARKING_LOTS[random.nextInt(PARKING_LOTS.length)];
                int slotId = random.nextInt(NUM_SLOTS_PER_LOT) + 1;
                String status = STATUSES[random.nextInt(STATUSES.length)];
                int temperature = 20 + random.nextInt(15);
                int duration = 0;
                String vehicleLicensePlate = null; // Initialize to null

                if ("occupied".equals(status)) {
                    duration = random.nextInt(3600) + 60;
                    // Assign a random license plate if occupied
                    vehicleLicensePlate = LICENSE_PLATES[random.nextInt(LICENSE_PLATES.length)];
                } else if ("malfunction".equals(status)) {
                    duration = 0;
                }

                String timestamp = Instant.now().toString();

                JSONObject sensorData = new JSONObject();
                sensorData.put("parking_lot_name", parkingLot);
                sensorData.put("slot_id", slotId);
                sensorData.put("timestamp", timestamp);
                sensorData.put("status", status);
                sensorData.put("duration", duration);
                sensorData.put("temperature", temperature);
                // Add vehicle_license_plate only if it's occupied or you want to send null for others
                if (vehicleLicensePlate != null) {
                    sensorData.put("vehicle_license_plate", vehicleLicensePlate);
                } else {
                    sensorData.put("vehicle_license_plate", JSONObject.NULL); // Explicitly send null for consistency
                }


                String jsonString = sensorData.toJSONString();

                ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, parkingLot + "-" + slotId, jsonString);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        // System.out.println("Sent record to topic " + metadata.topic() + " offset " + metadata.offset());
                    } else {
                        System.err.println("Error sending record: " + exception.getMessage());
                    }
                });

                Thread.sleep(100);

                if (random.nextInt(100) == 0) {
                    producer.flush();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Sensor data producer interrupted.");
        } finally {
            producer.flush();
            producer.close();
            System.out.println("Sensor data producer stopped.");
        }
    }
}