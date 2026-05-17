import paho.mqtt.client as mqtt
import pandas as pd
import json
import time

DATASET_PATH = "../data/combined_energy_dataset.csv"
CHUNK_SIZE   = 10_000   # read in chunks — file is 7.68 GB

print("Starting Smart Grid MQTT Publisher...")
print(f"Dataset: {DATASET_PATH} (7.68 GB — 25 meters)")

client = mqtt.Client()
client.connect("localhost", 1883)
print("Connected to MQTT broker\n")

msg_count = 0

for chunk in pd.read_csv(DATASET_PATH, chunksize=CHUNK_SIZE):
    for _, row in chunk.iterrows():
        try:
            payload = json.dumps({
                "timestamp":      str(row["timestamp"]),
                "power":          float(row["power"]),
                "voltage":        float(row["voltage"]),
                "current":        float(row["current"]),
                "reactive_power": float(row["reactive_power"]),
                "apparent_power": float(row["apparent_power"]),
                "power_factor":   float(row["power_factor"]),
                "sub_meter_1":    float(row["sub_meter_1"]),
                "sub_meter_2":    float(row["sub_meter_2"]),
                "sub_meter_3":    float(row["sub_meter_3"]),
                "frequency":      float(row["frequency"]),
                "temperature_c":  float(row["temperature_c"]),
                "humidity_pct":   float(row["humidity_pct"]),
                "anomaly":        int(row["anomaly"]),
                "co2_kg":         float(row["co2_kg"]),
                "cost_usd":       float(row["cost_usd"]),
                "meter_id":       str(row["meter_id"]),
                "building_type":  str(row["building_type"]),
                "tariff_zone":    str(row["tariff_zone"]),
                "grid_region":    str(row["grid_region"]),
            })

            client.publish("smartgrid/power", payload)
            msg_count += 1

            if msg_count % 100 == 0:
                print(f"[{msg_count:,}] meter={row['meter_id']} | "
                      f"type={row['building_type']} | "
                      f"power={row['power']:.3f} kW | "
                      f"anomaly={'YES ⚠' if row['anomaly'] else 'no'}")

            time.sleep(0.5)   # 2 msgs/sec — faster for demo

        except Exception as e:
            print(f"Error on row: {e}")
            continue