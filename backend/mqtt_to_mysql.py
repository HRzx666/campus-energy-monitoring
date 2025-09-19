import json
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime, timezone, timedelta

# ========== MySQL 配置 ==========
db = mysql.connector.connect(
    host="localhost",        # 如果数据库在远程服务器，就改成对应IP
    user="root",             # 数据库用户名
    password="3138998404HRzx", # 数据库密码
    database="campus_energy" # 数据库名
)
cursor = db.cursor()

# ========== MQTT 配置 ==========
MQTT_BROKER = "192.168.87.80"   # EMQX 服务器 IP
MQTT_PORT = 1883
MQTT_TOPIC = "campus/energy"

# 台北时区
TAIPEI_TZ = timezone(timedelta(hours=8))

# ========== 处理接收到的 MQTT 消息 ==========
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())  # 解析 JSON
        print("📩 收到数据:", data)

        if data.get("type") == "water":  # 判断是不是水流数据
            sql = """INSERT INTO water_data (device, timestamp, flow_lpm, total_l)
                     VALUES (%s, %s, %s, %s)"""
            values = (
                data["device"],
                datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S"),  # ✅ 台北时间
                float(data["flow_lpm"]),
                float(data["total_l"])
            )
            cursor.execute(sql, values)
            db.commit()
            print("✅ 已写入数据库:", values)

        elif data.get("type") == "electric":  # ⚡ 电
            sql = """INSERT INTO electric_data (device, timestamp, power_kw, total_kwh)
                     VALUES (%s, %s, %s, %s)"""
            values = (
                data["device"],
                datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                float(data["power_kw"]),
                float(data["total_kwh"])
            )
            cursor.execute(sql, values)
            db.commit()
            print("✅ 已写入数据库:", values)

        elif data.get("type") == "gas":  # 🔥 气
            sql = """INSERT INTO gas_data (device, timestamp, flow_m3h, total_m3)
                     VALUES (%s, %s, %s, %s)"""
            values = (
                data["device"],
                datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                float(data["flow_m3h"]),
                float(data["total_m3"])
            )
            cursor.execute(sql, values)
            db.commit()
            print("✅ 已写入数据库:", values)

    except Exception as e:
        print("❌ 错误:", e)

# ========== 连接 MQTT ==========
client = mqtt.Client()
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC)

print("🚀 等待接收 ESP32 数据...")
client.loop_forever()

