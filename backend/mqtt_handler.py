# mqtt_handler.py
import json
import time
import paho.mqtt.client as mqtt
from datetime import datetime
from .config import MQTT_BROKER, MQTT_PORT, TOPIC_ELECTRIC, TOPIC_WATER, TOPIC_GAS
from .db import get_connection

def insert_data(table, device, timestamp, field1, field2):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {table} (device, timestamp, {field1[0]}, {field2[0]}) VALUES (%s,%s,%s,%s)",
        (device, timestamp, field1[1], field2[1])
    )
    conn.commit()
    conn.close()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        device = payload.get("device", "unknown")
        ts = datetime.fromisoformat(payload.get("timestamp"))
        if msg.topic == TOPIC_ELECTRIC:
            insert_data("electric_data", device, ts, ("power_kw", payload["power_kw"]), ("total_kwh", payload["total_kwh"]))
        elif msg.topic == TOPIC_WATER:
            insert_data("water_data", device, ts, ("flow_lpm", payload["flow_lpm"]), ("total_l", payload["total_l"]))
        elif msg.topic == TOPIC_GAS:
            insert_data("gas_data", device, ts, ("flow_m3h", payload["flow_m3h"]), ("total_m3", payload["total_m3"]))
    except Exception as e:
        print(f"[MQTT] 消息处理错误: {e}")

def start_mqtt():
    client = mqtt.Client()
    client.on_message = on_message

    while True:
        try:
            print(f"[MQTT] 尝试连接 {MQTT_BROKER}:{MQTT_PORT} ...")
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.subscribe([
                (TOPIC_ELECTRIC, 0),
                (TOPIC_WATER, 0),
                (TOPIC_GAS, 0)
            ])
            print("[MQTT] 连接成功，开始监听")
            client.loop_forever()
        except Exception as e:
            print(f"[MQTT] 连接失败: {e}，5 秒后重试")
            time.sleep(5)
