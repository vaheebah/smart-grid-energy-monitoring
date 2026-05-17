# scripts/combine_datasets.py
import pandas as pd
import numpy as np
import os

output_path = "../data/combined_energy_dataset.csv"

print("Loading household power consumption...")
df1 = pd.read_csv(
    "../data/household_power_consumption.txt",
    sep=";", parse_dates=[["Date","Time"]],
    dayfirst=True, na_values=["?"], low_memory=False
).dropna()

df1 = df1.rename(columns={
    "Date_Time":              "timestamp",
    "Global_active_power":    "power",
    "Voltage":                "voltage",
    "Global_intensity":       "current",
    "Global_reactive_power":  "reactive_power",
    "Sub_metering_1":         "sub_meter_1",
    "Sub_metering_2":         "sub_meter_2",
    "Sub_metering_3":         "sub_meter_3",
})

for col in ["power","voltage","current","reactive_power","sub_meter_1","sub_meter_2","sub_meter_3"]:
    df1[col] = pd.to_numeric(df1[col], errors="coerce")

df1 = df1[["timestamp","power","voltage","current","reactive_power",
           "sub_meter_1","sub_meter_2","sub_meter_3"]].dropna().reset_index(drop=True)

print(f"Base dataset: {len(df1):,} rows")

building_configs = [
    ("residential", 1.0,  230, 10),
    ("commercial",  3.2,  380,  8),
    ("industrial",  8.5,  415,  7),
]

tariff_choices = np.array(["peak", "off-peak", "shoulder"])
zone_choices   = np.array(["Zone_A", "Zone_B", "Zone_C", "Zone_D"])
region_choices = np.array(["North", "South", "East", "West"])
freq_choices   = np.array([49.95, 50.0, 50.05, 50.1])

n = len(df1)
meter_id = 1
first_write = True

for building_type, pf, vbase, count in building_configs:
    for i in range(count):
        print(f"  Meter {meter_id:03d} ({building_type} {i+1}/{count})...", end=" ", flush=True)

        temp = df1.copy()
        rng  = np.random.default_rng(seed=meter_id * 7)

        noise_p = rng.uniform(0.88, 1.15, n)
        noise_v = rng.uniform(0.97, 1.03, n)
        noise_c = rng.uniform(0.88, 1.15, n)

        temp["meter_id"]       = f"meter_{meter_id:03d}"
        temp["building_type"]  = building_type
        temp["building_id"]    = f"{building_type}_{i+1:02d}"
        temp["floor"]          = rng.integers(1, 20, n)
        temp["zone"]           = zone_choices[rng.integers(0, 4, n)]
        temp["grid_region"]    = region_choices[rng.integers(0, 4, n)]
        temp["tariff_zone"]    = tariff_choices[rng.integers(0, 3, n)]
        temp["power"]          = (temp["power"]          * pf * noise_p).round(3)
        temp["voltage"]        = (vbase * noise_v).round(2)
        temp["current"]        = (temp["current"]        * pf * noise_c).round(3)
        temp["reactive_power"] = (temp["reactive_power"] * noise_p).round(3)

        ap = np.sqrt(temp["power"]**2 + temp["reactive_power"]**2)
        temp["apparent_power"] = ap.round(3)
        temp["power_factor"]   = (temp["power"] / ap.replace(0, np.nan)).fillna(0).round(4)
        temp["frequency"]      = freq_choices[rng.integers(0, 4, n)]
        temp["temperature_c"]  = (20 + rng.normal(0, 3, n)).round(1)
        temp["humidity_pct"]   = rng.uniform(30, 80, n).round(1)
        temp["anomaly"]        = (temp["power"] > temp["power"].quantile(0.97)).astype(int)
        temp["co2_kg"]         = (temp["power"] * 0.233).round(4)
        temp["cost_usd"]       = (temp["power"] * 0.12 / 60).round(5)

        temp.to_csv(output_path,
                    mode="w" if first_write else "a",
                    header=first_write,
                    index=False)
        first_write = False

        size_mb = os.path.getsize(output_path) / (1024**2)
        print(f"done — {size_mb:.0f} MB written so far")

        del temp
        meter_id += 1

size_gb = os.path.getsize(output_path) / (1024**3)
print(f"\n✅ File size: {size_gb:.2f} GB")
print(f"✅ Total meters: {meter_id - 1}")
print(f"✅ Saved to: {output_path}")