import json
import time
import random
from kafka import KafkaProducer

# Configuration du Producer Kafka
# Il se connecte au Kafka local via le port 9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw_orders'

print(f"Démarrage du générateur de fausses données pour le topic '{TOPIC_NAME}'...")
print("Appuyez sur Ctrl+C pour arrêter.\n")

order_id = 1

try:
    while True:
        # Création d'une fausse donnée de commande (Order)
        mock_data = {
            "order_id": order_id,
            "user_id": random.choice([100, 200, 300, 400, 500, 600, 700, 800, 900, 999]),
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(["PENDING", "COMPLETED", "CANCELLED"]),
            "timestamp": int(time.time())
        }
        
        # Envoi dans Kafka
        producer.send(TOPIC_NAME, mock_data)
        print(f"✅ Envoyé: {mock_data}")
        
        order_id += 1
        
        # Pause d'une seconde avant le prochain envoi
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Arrêt du générateur.")
finally:
    producer.close()
