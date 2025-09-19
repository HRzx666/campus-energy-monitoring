import json
import random
import time
import threading
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime

# ========== MySQL 配置 ==========
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="3138998404HRzx",
    database="campus_energy"
)
cursor = db.cursor()

# ========== MQTT 配置 ==========
MQTT_BROKER = "192.168.87.80"   # EMQX 服务器 IP
MQTT_PORT = 1883
MQTT_TOPIC = "campus/energy"


# ========== 处理接收到的 MQTT 消息 ==========
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        print("📩 收到数据:", data)

        ts = datetime.fromtimestamp(data["timestamp"])

        if data.get("type") == "water":
            sql = """INSERT INTO water_data (device, timestamp, flow_lpm, total_l)
                     VALUES (%s, %s, %s, %s)"""
            values = (
                data["device"], ts,
                float(data["flow_lpm"]), float(data["total_l"])
            )

        elif data.get("type") == "electric":
            sql = """INSERT INTO electric_data (device, timestamp, power_kw, total_kwh)
                     VALUES (%s, %s, %s, %s)"""
            values = (
                data["device"], ts,
                float(data["power_kw"]), float(data["total_kwh"])
            )

        elif data.get("type") == "gas":
            sql = """INSERT INTO gas_data (device, timestamp, flow_m3h, total_m3)
                     VALUES (%s, %s, %s, %s)"""
            values = (
                data["device"], ts,
                float(data["flow_m3h"]), float(data["total_m3"])
            )
        else:
            print("⚠️ 未知数据类型:", data)
            return

        cursor.execute(sql, values)
        db.commit()
        print("✅ 已写入数据库:", values)

    except Exception as e:
        print("❌ 错误:", e)


# ========== 连接 & 重连处理 ==========
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("✅ 已连接到 MQTT Broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print("❌ 连接失败，原因:", reason_code)


def on_disconnect(client, userdata, rc, properties=None):
    print("⚠️ 与 MQTT Broker 断开，正在尝试重连...")
    while True:
        try:
            client.reconnect()
            print("🔄 重连成功")
            break
        except Exception as e:
            print("❌ 重连失败:", e)
            time.sleep(5)


# ========== 启动 MQTT 客户端 ==========
# ⚠️ 这里使用新版 API，避免 deprecated 警告
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)


# ========== 虚拟数据生成器 ==========
def data_generator():
    while True:
        now = int(time.time())

        electric_data = {
            "device": "B1-201",
            "timestamp": now,
            "type": "electric",
            "power_kw": round(random.uniform(5.0, 15.0), 2),
            "total_kwh": round(random.uniform(100, 600), 2)
        }

        water_data = {
            "device": "B1-201",
            "timestamp": now,
            "type": "water",
            "flow_lpm": round(random.uniform(5.0, 20.0), 2),
            "total_l": round(random.uniform(100, 500), 2)   # ✅ 修正范围 (小数不会倒挂)
        }

        gas_data = {
            "device": "B1-201",
            "timestamp": now,
            "type": "gas",
            "flow_m3h": round(random.uniform(5.0, 20.0), 2),
            "total_m3": round(random.uniform(500, 2000), 2)
        }

        for data in [electric_data, water_data, gas_data]:
            payload = json.dumps(data)
            result = client.publish(MQTT_TOPIC, payload)
            status = result[0]
            if status == 0:
                print(f"📤 已发送: {payload}")
            else:
                print(f"⚠️ 发送失败: {payload}")

        time.sleep(2)


# ========== 主程序 ==========
if __name__ == "__main__":
    threading.Thread(target=client.loop_forever, daemon=True).start()
    data_generator()
