#!/usr/bin/env python3
"""
Hybrid Canonical CSV Replayer for PRODUCTION ZONE
-------------------------------------------------
- Zone: PRODUCTION (~80 devices concept)
- Giữ nguyên logic gốc: đọc CSV → timing → lọc publish → publish payload ngẫu nhiên
- Thêm "zone": "production" vào JSON
Usage:
  python mqtt_csv_replayer_production.py --indir datasets --broker emqx --port 1883
"""

from __future__ import annotations
import argparse, json, os, random, threading, time
from datetime import datetime, timezone
from typing import List, Optional, Tuple
import pandas as pd
import paho.mqtt.client as mqtt

# ----------------------------------------------------------------------------- 
# Canonical column candidates
# -----------------------------------------------------------------------------
TIMESTAMP_CANDIDATES = [
    "timestamp", "ts", "time", "frame.time_epoch", "frame.time_relative",
    "Time", "SniffTimestamp"
]
MSGTYP_CANDIDATES = ["mqtt.msgtype", "msg_type", "message_type", "packet_type", "mqtt.msgtype_str"]

# ----------------------------------------------------------------------------- 
# Zone & tenancy
# -----------------------------------------------------------------------------
TENANT = "energy"
ZONE = "energy"

# ----------------------------------------------------------------------------- 
# Device set cho Production Floor
# (Name, CSV filename, username-key để random_value_for_device)
# -----------------------------------------------------------------------------
DEVICES = [
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler1"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler2"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler3"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler4"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler5"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler6"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler7"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler8"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler9"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler10"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler11"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler12"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler13"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler14"),
    ("CoolerMotor", "cooler-motor_gotham.csv", "sensor_cooler15"),
    ("FanSpeed", "FanSpeedControllerMQTTset.csv", "sensor_fanspeed1"),
    ("FanSpeed", "FanSpeedControllerMQTTset.csv", "sensor_fanspeed2"),
    ("FanSpeed", "FanSpeedControllerMQTTset.csv", "sensor_fanspeed3"),
    ("FanSpeed", "FanSpeedControllerMQTTset.csv", "sensor_fanspeed4"),
    ("FanSpeed", "FanSpeedControllerMQTTset.csv", "sensor_fanspeed5"),
    ("FanSensor", "FansensorMQTTset.csv", "sensor_fan1"),
    ("FanSensor", "FansensorMQTTset.csv", "sensor_fan2"),
    ("FanSensor", "FansensorMQTTset.csv", "sensor_fan3"),
    ("FanSensor", "FansensorMQTTset.csv", "sensor_fan4"),
    ("FanSensor", "FansensorMQTTset.csv", "sensor_fan5"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion1"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion2"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion3"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion4"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion5"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion6"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion7"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion8"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion9"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion10"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion11"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion12"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion13"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion14"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion15"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion16"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion17"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion18"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion19"),
    ("Motion", "MotionMQTTset.csv", "sensor_motion20"),

]

# ----------------------------------------------------------------------------- 
# Helpers (y như file gốc)
# -----------------------------------------------------------------------------
def resolve_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _parse_timestamp_series(ts: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(ts):
        s = ts.astype(float)
        if s.dropna().median() > 1e12:
            s = s / 1000.0
        return s
    dt = pd.to_datetime(ts, errors="coerce", utc=True)
    return dt.view("int64") / 1e9

def _median_interval(seconds: pd.Series) -> float:
    diffs = seconds.diff().dropna()
    if diffs.empty:
        return 1.0
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return 1.0
    return float(diffs.median())

def _is_publish(row: pd.Series, msgtype_col: Optional[str]) -> bool:
    if not msgtype_col or msgtype_col not in row or pd.isna(row[msgtype_col]):
        return True
    v = row[msgtype_col]
    try:
        if str(v).strip().isdigit():
            return int(v) == 3
    except Exception:
        pass
    s = str(v).lower()
    return ("publish" in s) and ("command" not in s) and ("req" not in s)

def mk_client(client_id: str, username: Optional[str] = None) -> mqtt.Client:
    c = mqtt.Client(client_id=client_id)
    if username:
        c.username_pw_set(username)
    return c

def random_value_for_device(username: str) -> float:
    ranges: dict[str, Tuple[float, float]] = {
        "sensor_temp": (15.0, 40.0),
        "sensor_light": (0.0, 2000.0),
        "sensor_hum": (20.0, 90.0),
        "sensor_motion": (0, 1),
        "sensor_co": (0.0, 50.0),
        "sensor_smoke": (0.0, 10.0),
        "sensor_fanspeed": (500, 3000),
        "sensor_door": (0, 1),
        "sensor_fan": (500, 2500),
        "sensor_air": (0.0, 150.0),
        "sensor_cooler": (0.5, 5.0),
        "sensor_distance": (1.0, 400.0),
        "sensor_flame": (0, 1),
        "sensor_ph": (5.5, 8.5),
        "sensor_soil": (5.0, 60.0),
        "sensor_sound": (30.0, 100.0),
        "sensor_water": (0.0, 300.0),
        "sensor_hydraulic": (50.0, 250.0),
        "sensor_predictive": (0.0, 1.0),
    }
    lo, hi = ranges.get(username, (0.0, 100.0))
    val = random.uniform(lo, hi)
    if hi - lo <= 5 or (lo == 0 and hi <= 1):
        return round(val, 3)
    elif hi <= 100:
        return round(val, 2)
    else:
        return round(val, 1)

# ----------------------------------------------------------------------------- 
# Device thread (y như gốc, chỉ thêm "zone" vào payload)
# -----------------------------------------------------------------------------
def device_thread(device_name: str, csv_path: str, broker: str, port: int,
                  username: Optional[str], speed_factor: float, min_interval: float):
    topic = f"factory/{TENANT}/{device_name}/telemetry"
    client_id = f"{ZONE}-{username}-replayer"
    client = mk_client(client_id, username)

    # connect with retry
    connected = False
    while not connected:
        try:
            client.connect(broker, port, keepalive=60)
            client.loop_start()
            connected = True
            print(f"[{ZONE}:{device_name}] Connected to {broker}:{port}")
        except Exception as e:
            print(f"[{ZONE}:{device_name}] Connection failed, retrying in 5s: {e}")
            time.sleep(5)

    # load csv
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"[{ZONE}:{device_name}] Loaded {len(df)} rows from {csv_path}")
    except Exception as e:
        print(f"[{ZONE}:{device_name}] Error loading CSV: {e}")
        client.loop_stop(); client.disconnect()
        return

    ts_col = resolve_column(df, TIMESTAMP_CANDIDATES)
    msg_col = resolve_column(df, MSGTYP_CANDIDATES)

    if not ts_col:
        print(f"[{ZONE}:{device_name}] No timestamp column found; using 1.0s default interval.")
        seconds = pd.Series(range(len(df)), dtype=float)
        base_interval = 1.0
    else:
        seconds = _parse_timestamp_series(df[ts_col])
        base_interval = _median_interval(seconds)
        if pd.isna(seconds).all():
            seconds = pd.Series(range(len(df)), dtype=float)
            base_interval = 1.0

    # precompute intervals
    intervals: List[float] = []
    for i in range(len(df)):
        if i < len(df) - 1 and ts_col and pd.notna(seconds.iloc[i]) and pd.notna(seconds.iloc[i+1]):
            delta = float(seconds.iloc[i+1] - seconds.iloc[i])
        else:
            delta = base_interval
        if not (delta > 0):
            delta = base_interval
        delta = max(delta / max(speed_factor, 1e-6), min_interval)
        intervals.append(delta)

    # publish loop
    i = 0
    try:
        while True:
            row = df.iloc[i]
            if _is_publish(row, msg_col):
                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "value": random_value_for_device(username),
                    "client_id": client_id,
                    "zone": ZONE,
                }
                try:
                    client.publish(topic, json.dumps(payload))
                    print(f"[{ZONE}:{device_name}] Row {i+1}/{len(df)} → published: {payload}")
                except Exception as e:
                    print(f"[{ZONE}:{device_name}] Publish error: {e}")
            else:
                print(f"[{ZONE}:{device_name}] Row {i+1}/{len(df)} skipped (msgtype not publish)")

            time.sleep(intervals[i])
            i = (i + 1) % len(df)
    finally:
        client.loop_stop()
        client.disconnect()

# ----------------------------------------------------------------------------- 
# CLI
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="CSV Replayer (Production Zone)")
    parser.add_argument("--indir", default="datasets", help="Folder containing device CSV files")
    parser.add_argument("--broker", default="emqx", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--speed-factor", type=float, default=1.0, help=">1 speeds up, <1 slows down (default 1.0)")
    parser.add_argument("--min-interval", type=float, default=0.05, help="Minimum seconds between publishes after scaling")
    args = parser.parse_args()

    print("CSV Replayer (Production Zone) Starting...")
    print(f"Broker: {args.broker}:{args.port}")
    print(f"Data directory: {args.indir}")
    print(f"Speed factor: {args.speed_factor}")
    print("=" * 70)

    threads: List[threading.Thread] = []
    for name, fname, username in DEVICES:
        path = os.path.join(args.indir, fname)
        if not os.path.exists(path):
            print(f"Missing {path} - skipping {name}")
            continue
        t = threading.Thread(
            target=device_thread,
            args=(name, path, args.broker, args.port, username, args.speed_factor, args.min_interval),
            daemon=True,
        )
        t.start()
        threads.append(t)
        print(f"Started {name} → topic factory/{TENANT}/{name}/telemetry (file: {fname})")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping replayer...")

if __name__ == "__main__":
    main()
