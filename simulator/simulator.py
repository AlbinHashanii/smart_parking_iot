import tkinter as tk
from kafka import KafkaProducer
import json
import random
import time
import threading
from datetime import datetime
import math

# Configuration
config = {
    "lots": [chr(i) for i in range(ord('A'), ord('H') + 1)],  # Lots A to H
    "slots_per_lot": 50,
    "send_interval": 3,
    "car_flow_multiplier": 0.1,
    "avg_free_time": 120,
    "var_free_time": 30,
    "avg_occupied_time": 450,
    "var_occupied_time": 60,
    "garbage_data_chance": 0.00005,
    "unstable_threshold": 4,
    "unstable_time_window": 10,
    "silent_failure_chance": 0.0001,
    "hourly_arrival_prob": {
        (0, 6): 0.002,
        (6, 9): 0.05,
        (9, 16): 0.025,
        (16, 20): 0.07,
        (20, 24): 0.01
    }
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
        self.failed = False
        self.send_garbage = False
        self.silent_failed = False

    def update_button(self):
        if self.silent_failed:
            color = "gray"
        else:
            colors = {"free": "green", "occupied": "red"}
            color = colors.get(self.status, "gray")
        self.button.config(text=f"{self.status.capitalize()}\nSlot {self.slot_id}", bg=color)

    def record_status(self):
        now = time.time()
        self.status_history.append((now, self.status))
        self.status_history = [(t, s) for t, s in self.status_history if now - t <= config["unstable_time_window"]]

    def toggle_status(self):
        if self.silent_failed:
            self.silent_failed = False
            self.update_button()
            return
        self.status = "occupied" if self.status == "free" else "free"
        self.until = 0
        self.license_plate = generate_license_plate() if self.status == "occupied" else None
        self.record_status()
        self.update_button()
        self.send_event(force=True)

    def simulate(self):
        now = time.time()
        if not self.silent_failed and random.random() < config["silent_failure_chance"]:
            self.silent_failed = True
            self.update_button()
            return

        if self.until > 0 and now >= self.until:
            self.status = "free"
            self.until = 0
            self.license_plate = None
            self.record_status()
            self.update_button()
        elif self.status == "free" and random.random() < get_arrival_prob() * config["car_flow_multiplier"]:
            self.status = "occupied"
            self.until = now + random.randint(config["avg_occupied_time"] - config["var_occupied_time"],
                                              config["avg_occupied_time"] + config["var_occupied_time"])
            self.license_plate = generate_license_plate()
            self.record_status()
            self.update_button()
        if now - self.last_sent >= config["send_interval"]:
            self.send_event()

    def send_event(self, force=False):
        now = time.time()
        if self.silent_failed or self.failed or (not force and now - self.last_sent < config["send_interval"]):
            return
        self.last_sent = now
        if self.send_garbage:
            payload = {
                "sensor_id": None,
                "parking_lot_name": 123,
                "slot_id": "??",
                "reading_ts": "not-a-date",
                "status": "???",
                "duration": -99,
                "temperature": 9999.9,
                "vehicle_license_plate": 123456789
            }
        else:
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
        producer.send('parking-sensor-data', key=self.sensor_id.encode(), value=json.dumps(payload).encode())

def get_arrival_prob():
    hour = datetime.now().hour
    for (start, end), prob in config["hourly_arrival_prob"].items():
        if start <= hour < end:
            return prob
    return 0.005

def apply_config():
    config["car_flow_multiplier"] = float(entry_flow.get())
    config["avg_occupied_time"] = int(entry_occ_avg.get())
    config["var_occupied_time"] = int(entry_occ_var.get())
    config["avg_free_time"] = int(entry_free_avg.get())
    config["var_free_time"] = int(entry_free_var.get())
    config["silent_failure_chance"] = float(entry_silent_fail.get())

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
        slot.failed = False
        slot.send_garbage = False
        slot.silent_failed = False
        slot.update_button()
        slot.send_event(force=True)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

root = tk.Tk()
root.title("Smart Parking Simulator")
root.geometry("1400x950")
root.configure(bg="white")

tk.Label(root, text="Smart Parking Configurations", font=("Arial", 16), bg="white").pack(pady=5)

frame = tk.Frame(root, bg="white")
frame.pack()

# --- Row 0 ---
tk.Label(frame, text="Car Flow Multiplier", bg="white").grid(row=0, column=0, padx=5, pady=2, sticky="e")
entry_flow = tk.Entry(frame, width=5)
entry_flow.insert(0, str(config["car_flow_multiplier"]))
entry_flow.grid(row=0, column=1, padx=5, pady=2)

tk.Label(frame, text="Silent Failure Chance", bg="white").grid(row=0, column=2, padx=5, pady=2, sticky="e")
entry_silent_fail = tk.Entry(frame, width=8)
entry_silent_fail.insert(0, str(config["silent_failure_chance"]))
entry_silent_fail.grid(row=0, column=3, padx=5, pady=2)

# --- Row 1 ---
tk.Label(frame, text="Avg Occupied Time", bg="white").grid(row=1, column=0, padx=5, pady=2, sticky="e")
entry_occ_avg = tk.Entry(frame, width=5)
entry_occ_avg.insert(0, str(config["avg_occupied_time"]))
entry_occ_avg.grid(row=1, column=1, padx=5, pady=2)

tk.Label(frame, text="± Occupied Variability", bg="white").grid(row=1, column=2, padx=5, pady=2, sticky="e")
entry_occ_var = tk.Entry(frame, width=5)
entry_occ_var.insert(0, str(config["var_occupied_time"]))
entry_occ_var.grid(row=1, column=3, padx=5, pady=2)

# --- Row 2 ---
tk.Label(frame, text="Avg Free Time", bg="white").grid(row=2, column=0, padx=5, pady=2, sticky="e")
entry_free_avg = tk.Entry(frame, width=5)
entry_free_avg.insert(0, str(config["avg_free_time"]))
entry_free_avg.grid(row=2, column=1, padx=5, pady=2)

tk.Label(frame, text="± Free Variability", bg="white").grid(row=2, column=2, padx=5, pady=2, sticky="e")
entry_free_var = tk.Entry(frame, width=5)
entry_free_var.insert(0, str(config["var_free_time"]))
entry_free_var.grid(row=2, column=3, padx=5, pady=2)

# --- Apply Button Row ---
tk.Button(frame, text="Apply", command=apply_config, bg="green", fg="white").grid(
    row=3, column=0, columnspan=4, pady=8
)

scroll_frame = tk.Frame(root)
scroll_frame.pack(expand=True, fill="both")

canvas = tk.Canvas(scroll_frame)
scrollbar = tk.Scrollbar(scroll_frame, orient="vertical", command=canvas.yview)
canvas.configure(yscrollcommand=scrollbar.set)

inner_frame = tk.Frame(canvas)
canvas.create_window((0, 0), window=inner_frame, anchor="nw")
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")

inner_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

all_slots = []
for lot in config["lots"]:
    section = tk.LabelFrame(inner_frame, text=f"Lot {lot}", bg="white", font=("Arial", 12, "bold"))
    section.pack(padx=10, pady=10, fill="x")
    for i in range(config["slots_per_lot"]):
        btn = tk.Button(section, text=f"Free\nSlot {i+1}", width=10, height=2, bg="green", fg="white")
        btn.grid(row=i // 10, column=i % 10, padx=2, pady=2)
        slot = Slot(lot, i + 1, btn)
        btn.config(command=slot.toggle_status)
        all_slots.append(slot)

tk.Button(root, text="Reset All Slots", command=reset_all, bg="blue", fg="white").pack(pady=10)

threading.Thread(target=simulation_loop, daemon=True).start()
root.mainloop()
