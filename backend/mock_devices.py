import json
import mysql.connector
import paho.mqtt.client as mqtt
from datetime import datetime
import time

# ==================== 数据库配置 ====================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",          # 你的数据库用户
    "password": "3138998404HRzx",    # 你的数据库密码
    "database": "campus_energy"
}

# ==================== MQTT 配置 ====================
MQTT_BROKER = "192.168.87.80"  # 你的 EMQX IP
MQTT_PORT = 1883
MQTT_TOPIC = "campus/energy"

# ==================== 数据库操作 ====================
def insert_data(device, timestamp, flow_lpm, total_l):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO water_data (device, timestamp, flow_lpm, total_l) VALUES (%s, %s, %s, %s)",
            (device, timestamp, flow_lpm, total_l)
        )
        conn.commit()
        conn.close()
        print(f"✅ 已写入数据库: ({device}, {timestamp}, {flow_lpm}, {total_l})")
    except Exception as e:
        print(f"❌ 数据库写入失败: {e}")

# ==================== MQTT 回调 ====================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ 成功连接到 MQTT Broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ 连接失败，返回码: {rc}")

def on_disconnect(client, userdata, rc):
    print("⚠️ MQTT 断开连接，尝试重连...")
    while True:
        try:
            time.sleep(5)
            client.reconnect()
            print("🔄 重连成功")
            break
        except Exception as e:
            print(f"重连失败: {e}")
            time.sleep(5)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"📩 收到数据: {payload}")

        device = payload.get("device", "unknown")
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        flow_lpm = float(payload.get("flow_lpm", 0))
        total_l = float(payload.get("total_l", 0))

        insert_data(device, ts, flow_lpm, total_l)

    except Exception as e:
        print(f"❌ 消息处理失败: {e}")

# ==================== 主函数 ====================
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print("🚀 等待接收 ESP32 数据...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n🛑 用户中断，MQTT 已停止")
        client.disconnect()
    except Exception as e:
        print(f"❌ 主进程错误: {e}")

if __name__ == "__main__":
    main()

