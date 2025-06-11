from datetime import datetime, timedelta
from confluent_kafka import Producer
import json
import time
import random

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = 'parking_data'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')

# Parking slot state management
class ParkingSlot:
    def __init__(self, slot_id, is_premium=False):
        self.slot_id = slot_id
        self.sensor_id = slot_id  # 1:1 sensor-slot mapping
        self.occupied = False
        self.last_change = datetime.now()
        self.is_premium = is_premium  # Premium slots (e.g., near entrance) more likely occupied

    def update_state(self, current_time):
        hour = current_time.hour
        is_weekday = current_time.weekday() < 5
        time_since_change = (current_time - self.last_change).total_seconds() / 60

        # Define occupancy probability based on time and slot type
        if is_weekday:
            if 8 <= hour <= 10 or 16 <= hour <= 18:  # Peak hours
                base_prob = 0.8 if self.is_premium else 0.7
            elif 22 <= hour or hour <= 6:  # Night
                base_prob = 0.3 if self.is_premium else 0.2
            else:  # Normal hours
                base_prob = 0.6 if self.is_premium else 0.5
        else:  # Weekend
            base_prob = 0.5 if self.is_premium else 0.4

        # State transition logic
        if self.occupied and time_since_change > random.uniform(30, 240):  # Occupied for 30-240 min
            self.occupied = random.random() > base_prob
            self.last_change = current_time
        elif not self.occupied and time_since_change > random.uniform(10, 60):  # Free for 10-60 min
            self.occupied = random.random() < base_prob
            self.last_change = current_time

        return {
            'sensor_id': self.sensor_id,
            'slot_id': self.slot_id,
            'occupied': self.occupied,
            'timestamp': current_time.isoformat()
        }

# Initialize 50 slots (S1-S50), first 10 are premium
slots = [ParkingSlot(f'S{i+1}', is_premium=i < 10) for i in range(50)]

def generate_parking_data():
    start_time = datetime.now()
    while True:
        current_time = start_time + timedelta(seconds=time.time() - start_time.timestamp())
        for slot in slots:
            data = slot.update_state(current_time)
            producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.flush()
        time.sleep(2)  # Generate data every 2 seconds

if __name__ == '__main__':
    generate_parking_data()