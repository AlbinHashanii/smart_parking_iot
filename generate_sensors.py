from datetime import datetime, timezone

lots = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
slots_per_lot = 50
base_lat = 42.650000
base_long = 21.170000
firmware_version = 'v1.0.0'
installation_date = '2023-01-01T00:00:00Z'
last_maintenance_date = '2024-06-01T00:00:00Z'
last_data_received = datetime.now(timezone.utc).isoformat()

for lot in lots:
    for slot in range(1, slots_per_lot + 1):
        sensor_id = f"SENSOR-{lot}-{slot:03d}"
        serial_number = f"SN-{lot}-{slot:05d}"
        sensor_name = f"Sensor {lot}-{slot}"
        latitude = base_lat + slot * 0.0001
        longitude = base_long - slot * 0.0001
        parking_lot_id = f"Lot-{lot}"

        print(f"""
INSERT INTO parking.sensor_metadata (
    sensor_id, serial_number, battery_level, connectivity_type, firmware_version,
    floor_number, installation_date, is_anomaly_detected,
    last_data_received, last_maintenance_date,
    latitude, longitude, parking_lot_id, sensor_name,
    sensor_type, slot_id, status, zone
) VALUES (
    '{sensor_id}', '{serial_number}', 100, 'LoRaWAN', '{firmware_version}',
    0, '{installation_date}', false,
    '{last_data_received}', '{last_maintenance_date}',
    {latitude:.6f}, {longitude:.6f}, '{parking_lot_id}', '{sensor_name}',
    'ultrasonic', {slot}, 'active', 'Main'
);""")
