"""
Mock Producer: Orders avec Timestamp ISO
Produit des commandes avec un champ 'event_time' au format ISO (TIMESTAMP)
pour tester les watermarks, fenêtres temporelles et les joins stream-stream.

Topic cible : raw_orders
"""
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 8, 1),
)

TOPIC_NAME = 'raw_orders'

STATUSES = ["PENDING", "COMPLETED", "CANCELLED", "SHIPPED", "RETURNED"]
PRODUCTS = ["Laptop", "Phone", "Tablet", "Headphones", "Keyboard", "Mouse", "Monitor", "Webcam"]
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille", "Nice"]

print(f"🚀 Démarrage du mock producer ORDERS pour '{TOPIC_NAME}'")
print("   ➜ Produit: order_id, user_id, product, amount, status, city, event_time")
print("   Appuyez sur Ctrl+C pour arrêter.\n")

order_id = 1

try:
    while True:
        # Timestamp ISO pour PySpark TIMESTAMP — parfois avec un petit retard simulé
        delay_seconds = random.choice([0, 0, 0, 0, 5, 10, 30, 60])  # ~50% à l'heure, ~50% en léger retard
        event_time = (datetime.utcnow() - timedelta(seconds=delay_seconds)).strftime("%Y-%m-%d %H:%M:%S")

        mock_data = {
            "order_id": order_id,
            "user_id": random.choice([100, 200, 300, 400, 500, 600, 700, 800, 900, 999]),
            "product": random.choice(PRODUCTS),
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(STATUSES),
            "city": random.choice(CITIES),
            "event_time": event_time,
        }

        producer.send(TOPIC_NAME, mock_data)
        print(f"✅ Order #{order_id}: {mock_data['product']} | {mock_data['amount']}€ | {mock_data['status']} | {event_time}")

        order_id += 1
        time.sleep(random.uniform(0.5, 2.0))  # Intervalle variable pour simuler du trafic réaliste

except KeyboardInterrupt:
    print("\n🛑 Arrêt du générateur orders.")
finally:
    producer.close()
