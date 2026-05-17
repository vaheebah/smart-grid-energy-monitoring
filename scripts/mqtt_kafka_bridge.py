import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json

print("Starting MQTT → Kafka bridge...")

# ── Kafka producer ────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ── MQTT callbacks ────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker (rc={rc})")
    client.subscribe("smartgrid/power")
    print("Subscribed to topic: smartgrid/power")

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode("utf-8"))
    producer.send("energy_stream", value=data)
    print(f"Forwarded → Kafka | power={data.get('power')} kW")

# ── Start ─────────────────────────────────────────────────────
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883)
client.loop_forever()