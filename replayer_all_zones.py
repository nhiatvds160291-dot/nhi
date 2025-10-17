#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Canonical CSV -> MQTT Replayer (5 Zones, Light Device Set)
-----------------------------------------------------------------
- Mục tiêu: gộp tất cả replayer (office, production, storage, energy, security) thành 1 file.
- Đã GIẢM SỐ THIẾT BỊ để nhẹ máy nhưng vẫn đủ 5 phòng/khu (zone): office, production, storage, energy, security.
- Gửi payload JSON chuẩn: {timestamp, value, client_id, zone}.
- Chủ đề (topic): factory/{TENANT}/{DEVICE}/telemetry

Chạy ví dụ (không TLS):
  python replayer_all_zones.py --indir datasets --broker 192.168.182.141 --port 1883 --speed-factor 1.0

Tuỳ chọn:
  --zones office,production,storage,energy,security   # mặc định bật cả 5
  --min-interval 0.05                                 # chống gửi quá dày khi tăng speed-factor
  --per-zone N                                        # (không bắt buộc) giới hạn tối đa N thiết bị mỗi zone

Yêu cầu: pandas, paho-mqtt
  pip install pandas paho-mqtt

Lưu ý:
  - Script này chỉ phát "bình thường" (normal) để tạo nền traffic hợp lý; không gắn nhãn tấn công.
  - CSV có thể thiếu cột timestamp/msg_type: script tự suy đoán khoảng cách thời gian.
"""
import argparse, json, os, random, threading, time
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict
import pandas as pd
import paho.mqtt.client as mqtt

# =============================== Helpers chung ===============================
TIMESTAMP_CANDIDATES = [
    "timestamp", "ts", "time", "frame.time_epoch", "frame.time_relative",
    "Time", "SniffTimestamp"
]
MSGTYP_CANDIDATES = ["mqtt.msgtype", "msg_type", "message_type", "packet_type", "mqtt.msgtype_str"]

def resolve_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _parse_timestamp_series(ts: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(ts):
        s = ts.astype(float)
        # epoch ms?
        if s.dropna().median() > 1e12:
            s = s / 1000.0
        return s
    dt = pd.to_datetime(ts, errors="coerce", utc=True)
    return dt.view("int64") / 1e9  # NaT -> NaN

def _median_interval(seconds: pd.Series) -> float:
    diffs = seconds.diff().dropna()
    if diffs.empty:
        return 1.0
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return 1.0
    return float(diffs.median())

def _is_publish(row: pd.Series, msgtype_col: Optional[str]) -> bool:
    if not msgtype_col or msgtype_col not in row or pd.isna(row.get(msgtype_col)):
        return True
    v = row[msgtype_col]
    try:
        if str(v).strip().isdigit():
            return int(v) == 3  # MQTT PUBLISH code
    except Exception:
        pass
    s = str(v).lower()
    return ("publish" in s) and ("command" not in s) and ("req" not in s)

def mk_client(client_id: str, username: Optional[str] = None, password: Optional[str] = None) -> mqtt.Client:
    c = mqtt.Client(client_id=client_id)
    if username:
        if password is None:
            c.username_pw_set(username)
        else:
            c.username_pw_set(username, password)
    return c

def random_value_for_device(username: str) -> float:
    ranges: Dict[str, Tuple[float, float]] = {
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
    # match by prefix
    for key in ranges.keys():
        if username.startswith(key):
            lo, hi = ranges[key]
            break
    else:
        lo, hi = (0.0, 100.0)
    val = random.uniform(lo, hi)
    if hi - lo <= 5 or (lo == 0 and hi <= 1):
        return round(val, 3)
    elif hi <= 100:
        return round(val, 2)
    else:
        return round(val, 1)

# =============================== Danh sách thiết bị rút gọn (5 zones) ===============================
# Cấu trúc: (zone, tenant, device_name, csv_filename, username, password_optional)
DEVICES: List[Tuple[str, str, str, str, str, Optional[str]]] = [
    # ---- OFFICE ----
    ("office", "office", "Temperature", "TemperatureMQTTset.csv", "sensor_temp", "temp123"),
    ("office", "office", "Humidity",    "HumidityMQTTset.csv",    "sensor_hum",  "hum123"),
    ("office", "office", "Light",       "LightIntensityMQTTset.csv", "sensor_light", "light123"),
    ("office", "office", "DoorLock",    "DoorlockMQTTset.csv",    "sensor_door", "door123"),

    # ---- PRODUCTION ----
    ("production", "production", "PredictiveMaintenance", "predictive-maintenance_gotham.csv", "sensor_predictive1", None),
    ("production", "production", "HydraulicSystem",       "hydraulic-system_gotham.csv",       "sensor_hydraulic1",  None),
    ("production", "production", "FanSpeed",              "FanSpeedControllerMQTTset.csv",     "sensor_fanspeed1",   None),

    # ---- STORAGE ----
    ("storage", "storage", "Temperature", "TemperatureMQTTset.csv", "sensor_temp1", None),
    ("storage", "storage", "Humidity",    "HumidityMQTTset.csv",    "sensor_hum1",  None),
    ("storage", "storage", "Smoke",       "SmokeMQTTset.csv",       "sensor_smoke1",None),

    # ---- ENERGY ----
    ("energy", "energy", "CoolerMotor",   "cooler-motor_gotham.csv",       "sensor_cooler1",  None),
    ("energy", "energy", "FanSpeed",      "FanSpeedControllerMQTTset.csv", "sensor_fanspeed1",None),
    ("energy", "energy", "Motion",        "MotionMQTTset.csv",             "sensor_motion1",  None),

    # ---- SECURITY ----
    ("security", "security", "DoorLock",   "DoorlockMQTTset.csv",    "sensor_door1",  None),
    ("security", "security", "AirQuality", "air-quality_gotham.csv", "sensor_air1",   None),
    ("security", "security", "Smoke",      "SmokeMQTTset.csv",       "sensor_smoke1", None),
]

# =============================== Worker ===============================
def device_worker(indir: str, broker: str, port: int, speed_factor: float, min_interval: float,
                  zone: str, tenant: str, device_name: str, csv_filename: str,
                  username: Optional[str], password: Optional[str]):

    topic = f"factory/{tenant}/{device_name}/telemetry"
    client_id = f"{zone}-{username or 'anon'}-replayer"
    client = mk_client(client_id, username, password)

    # Connect with retry
    connected = False
    while not connected:
        try:
            client.connect(broker, port, keepalive=60)
            client.loop_start()
            connected = True
            print(f"[{zone}:{device_name}] Connected to {broker}:{port}")
        except Exception as e:
            print(f"[{zone}:{device_name}] Connection failed, retrying in 5s: {e}")
            time.sleep(5)

    # Load CSV
    path = os.path.join(indir, csv_filename)
    try:
        df = pd.read_csv(path, low_memory=False)
        print(f"[{zone}:{device_name}] Loaded {len(df)} rows from {path}")
    except Exception as e:
        print(f"[{zone}:{device_name}] Error loading CSV '{path}': {e}")
        client.loop_stop(); client.disconnect()
        return

    ts_col = resolve_column(df, TIMESTAMP_CANDIDATES)
    msg_col = resolve_column(df, MSGTYP_CANDIDATES)

    if not ts_col:
        print(f"[{zone}:{device_name}] No timestamp column found; using 1.0s default interval.")
        seconds = pd.Series(range(len(df)), dtype=float)
        base_interval = 1.0
    else:
        seconds = _parse_timestamp_series(df[ts_col])
        base_interval = _median_interval(seconds)
        if pd.isna(seconds).all():
            seconds = pd.Series(range(len(df)), dtype=float)
            base_interval = 1.0

    # Precompute intervals (đã scale & clamp min_interval)
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

    # Publish loop (vòng tròn dữ liệu)
    i = 0
    try:
        while True:
            row = df.iloc[i]
            if _is_publish(row, msg_col):
                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "value": random_value_for_device(username or ""),
                    "client_id": client_id,
                    "zone": zone,
                }
                try:
                    client.publish(topic, json.dumps(payload))
                    print(f"[{zone}:{device_name}] Row {i+1}/{len(df)} → published: {payload}")
                except Exception as e:
                    print(f"[{zone}:{device_name}] Publish error: {e}")
            else:
                print(f"[{zone}:{device_name}] Row {i+1}/{len(df)} skipped (msgtype not publish)")

            time.sleep(intervals[i])
            i = (i + 1) % len(df)
    finally:
        client.loop_stop()
        client.disconnect()

# =============================== CLI & Main ===============================
def main():
    parser = argparse.ArgumentParser(description="Unified CSV->MQTT Replayer (5 Zones, Light Set)")
    parser.add_argument("--indir", default="datasets", help="Folder containing device CSV files")
    parser.add_argument("--broker", default="emqx", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--speed-factor", type=float, default=1.0, help=">1 tăng tốc, <1 làm chậm (default 1.0)")
    parser.add_argument("--min-interval", type=float, default=0.05, help="Khoảng nghỉ tối thiểu giữa 2 bản tin (s)")
    parser.add_argument("--zones", default="office,production,storage,energy,security",
                        help="Danh sách zone bật, phân tách bằng dấu phẩy")
    parser.add_argument("--per-zone", type=int, default=0,
                        help="(tuỳ chọn) giới hạn tối đa N thiết bị mỗi zone (0 = không giới hạn)")
    args = parser.parse_args()

    enabled_zones = {z.strip().lower() for z in args.zones.split(",") if z.strip()}
    print("Unified CSV->MQTT Replayer Starting...")
    print(f"Broker: {args.broker}:{args.port}")
    print(f"Data directory: {args.indir}")
    print(f"Speed factor: {args.speed_factor}")
    print(f"Zones enabled: {', '.join(sorted(enabled_zones))}")
    print("=" * 72)

    # Group devices theo zone rồi áp dụng per-zone limit
    grouped: Dict[str, List[Tuple[str, str, str, str, str, Optional[str]]]] = {}
    for d in DEVICES:
        zone = d[0]
        if zone not in enabled_zones:
            continue
        grouped.setdefault(zone, []).append(d)

    threads: List[threading.Thread] = []
    for zone, devs in grouped.items():
        count = 0
        for (zone, tenant, device_name, csv_filename, username, password) in devs:
            if args.per_zone and count >= args.per_zone:
                break
            path = os.path.join(args.indir, csv_filename)
            if not os.path.exists(path):
                print(f"Missing {path} - skipping {zone}/{device_name}")
                continue
            t = threading.Thread(
                target=device_worker,
                args=(args.indir, args.broker, args.port, args.speed_factor, args.min_interval,
                      zone, tenant, device_name, csv_filename, username, password),
                daemon=True,
            )
            t.start()
            threads.append(t)
            count += 1
            print(f"Started {zone}/{device_name} → topic factory/{tenant}/{device_name}/telemetry (file: {csv_filename})")

    if not threads:
        print("No device started. Kiểm tra tham số --zones, --per-zone và thư mục --indir.")
        return

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping replayer...")

if __name__ == "__main__":
    main()
