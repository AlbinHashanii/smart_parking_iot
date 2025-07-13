import tkinter as tk
from kafka import KafkaProducer
import json
import random
import time
import threading
from datetime import datetime
import math

# Configuration
LOTS = ['A', 'B', 'C']
SLOTS_PER_LOT = 50
KAFKA_TOPIC = 'parking-sensor-data'
KAFKA_BROKER = 'localhost:9092'
SEND_INTERVAL = 3  # seconds
MALFUNCTION_CHANCE = 0.00002
FREE_DURATION_RANGE = (60, 180)
OCCUPIED_DURATION_RANGE = (180, 480)
MALFUNCTION_DURATION_RANGE = (300, 900)
UNSTABLE_THRESHOLD = 4  # changes
UNSTABLE_TIME_WINDOW = 10  # seconds

HOURLY_ARRIVAL_PROB = {
    (0, 6): 0.002,
    (6, 9): 0.05,
    (9, 16): 0.025,
    (16, 20): 0.07,
    (20, 24): 0.01
}

def current_temperature():
    hour = datetime.now().hour + datetime.now().minute / 60.0
    base_temp = 17 + 7 * math.sin((hour - 6) / 24 * 2 * math.pi)
    return round(base_temp + random.uniform(-1, 1), 1)

def generate_license_plate():
    if random.random() < 0.8:
        return f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(100, 999)}-{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')*2}"
    return None

class Slot:
    def __init__(self, lot, slot_id, button):
        self.lot = lot
        self.slot_id = slot_id
        self.sensor_id = f"SENSOR-{lot}-{slot_id:03d}"
        self.status = "free"
        self.until = 0
        self.button = button
        self.last_sent = 0
        self.license_plate = None
        self.status_history = []

    def toggle_status(self):
        self.status = {
            "free": "occupied",
            "occupied": "malfunction",
            "malfunction": "free"
        }[self.status]
        self.until = 0
        self.license_plate = generate_license_plate() if self.status == "occupied" else None
        self.record_status()
        self.update_button()
        self.send_event(force=True)

    def update_button(self):
        colors = {"free": "green", "occupied": "red", "malfunction": "yellow"}
        self.button.config(text=f"{self.status.capitalize()}\nSlot {self.slot_id}", bg=colors[self.status])

    def record_status(self):
        now = time.time()
        self.status_history.append((now, self.status))
        self.status_history = [(t, s) for t, s in self.status_history if now - t <= UNSTABLE_TIME_WINDOW]

    def is_unstable(self):
        # Count how many times the state has flipped between 'free' and 'occupied'
        last_state = None
        flips = 0
        for _, s in self.status_history:
            if s in ["free", "occupied"]:
                if s != last_state:
                    flips += 1
                    last_state = s
        return flips >= UNSTABLE_THRESHOLD

    def simulate(self):
        now = time.time()
        if self.until > 0 and now >= self.until:
            self.status = "free"
            self.until = 0
            self.license_plate = None
            self.record_status()
            self.update_button()
        elif self.status == "free" and random.random() < get_arrival_prob() * 0.05:
            self.status = "occupied"
            self.until = now + random.randint(*OCCUPIED_DURATION_RANGE)
            self.license_plate = generate_license_plate()
            self.record_status()
            self.update_button()
        elif (
            self.status == "occupied" and
            self.until - now > 600 and
            random.random() < MALFUNCTION_CHANCE
        ):
            self.status = "malfunction"
            self.until = now + random.randint(*MALFUNCTION_DURATION_RANGE)
            self.record_status()
            self.update_button()

        if self.is_unstable() and self.status != "malfunction":
            self.status = "malfunction"
            self.until = now + random.randint(*MALFUNCTION_DURATION_RANGE)
            self.record_status()
            self.update_button()

        if now - self.last_sent >= SEND_INTERVAL:
            self.send_event()

    def send_event(self, force=False):
        now = time.time()
        if not force and now - self.last_sent < SEND_INTERVAL:
            return
        self.last_sent = now
        payload = {
            "sensor_id": self.sensor_id,
            "parking_lot_name": f"Parking Lot {self.lot}",
            "slot_id": self.slot_id,
            "reading_ts": datetime.utcnow().isoformat() + "Z",
            "status": self.status,
            "duration": max(0, int(self.until - now)),
            "temperature": current_temperature(),
            "vehicle_license_plate": self.license_plate
        }
        producer.send(KAFKA_TOPIC, key=self.sensor_id.encode(), value=json.dumps(payload).encode())

def get_arrival_prob():
    hour = datetime.now().hour
    for (start, end), prob in HOURLY_ARRIVAL_PROB.items():
        if start <= hour < end:
            return prob
    return 0.005

def simulation_loop():
    while True:
        for slot in all_slots:
            slot.simulate()
        time.sleep(0.5)

def reset_all():
    for slot in all_slots:
        slot.status = "free"
        slot.until = 0
        slot.license_plate = None
        slot.status_history = []
        slot.update_button()
        slot.send_event(force=True)

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

root = tk.Tk()
root.title("Smart Parking GUI Simulator")
root.geometry("1280x850")
root.configure(bg="white")

header = tk.Label(root, text="Smart Parking Simulator", font=("Helvetica", 26, "bold"), fg="#2c3e50", bg="white", pady=12)
header.pack()

info_bar = tk.Label(root, text="Click a slot to change its state manually. Colors: Green = Free, Red = Occupied, Yellow = Malfunction.",
                    fg="#555", bg="white", font=("Arial", 11))
info_bar.pack()

outer_frame = tk.Frame(root, bg="white")
outer_frame.pack(fill="both", expand=True)

canvas = tk.Canvas(outer_frame, bg="white")
scroll_y = tk.Scrollbar(outer_frame, orient="vertical", command=canvas.yview)
scroll_frame = tk.Frame(canvas, bg="white")

scroll_frame.bind(
    "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
)

canvas.create_window((0, 0), window=scroll_frame, anchor='nw')
canvas.configure(yscrollcommand=scroll_y.set)

canvas.pack(fill="both", expand=True, side="left")
scroll_y.pack(fill="y", side="right")

all_slots = []

for lot in LOTS:
    section = tk.LabelFrame(scroll_frame, text=f"Lot {lot}", font=("Arial", 14, "bold"), padx=10, pady=10, bg="white", fg="#2c3e50")
    section.pack(fill="x", padx=15, pady=10)
    for i in range(SLOTS_PER_LOT):
        btn = tk.Button(section, text=f"Free\nSlot {i+1}", width=12, height=3, bg="green", fg="white",
                        font=("Arial", 9, "bold"))
        btn.grid(row=i // 10, column=i % 10, padx=4, pady=4)
        slot = Slot(lot, i + 1, btn)
        btn.config(command=slot.toggle_status)
        all_slots.append(slot)

reset_btn = tk.Button(scroll_frame, text="Reset All Slots", command=reset_all, bg="#3498db", fg="white",
                      font=("Arial", 11, "bold"), padx=12, pady=6)
reset_btn.pack(pady=20)

threading.Thread(target=simulation_loop, daemon=True).start()

root.mainloop()
