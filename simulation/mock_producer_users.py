import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 8, 1),
)

TOPIC_NAME = 'users'

# Liste de faux utilisateurs
USERS = [
    {"user_id": uid, "name": name, "city": city}
    for uid, name, city in [
        (100, "Alice", "Paris"),
        (200, "Bob", "Lyon"),
        (300, "Charlie", "Marseille"),
        (400, "Diana", "Toulouse"),
        (500, "Eve", "Bordeaux"),
        (600, "Frank", "Nantes"),
        (700, "Grace", "Lille"),
        (800, "Hugo", "Nice"),
        (900, "Iris", "Strasbourg"),
        (999, "Jules", "Montpellier"),
    ]
]

print(f"🚀 Envoi des données utilisateurs dans le topic '{TOPIC_NAME}'...")
print(f"   {len(USERS)} utilisateurs à envoyer.\n")

try:
    while True:
        for user in USERS:
            producer.send(TOPIC_NAME, user)
            print(f"✅ Envoyé: {user}")
        
        print(f"\n🔄 Cycle complet. Renvoi dans 5 secondes...\n")
        time.sleep(5)
except KeyboardInterrupt:
    print("\n🛑 Arrêt du générateur.")
finally:
    producer.close()
