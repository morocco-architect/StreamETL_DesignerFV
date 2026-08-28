"""
Mock Producer: Users (référentiel utilisateurs)
Produit des données utilisateurs dans le topic 'users' pour les tests de JOIN.
Les user_id correspondent à ceux du mock_producer_orders_ts.py.

2 modes :
  - BATCH : envoie une seule fois tous les utilisateurs (pour test stream-batch join)
  - STREAM : envoie en continu avec event_time (pour test stream-stream join)

Topic cible : users
"""
import json
import time
import random
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 8, 1),
)

TOPIC_NAME = 'users'

# Référentiel de base
USERS_BASE = [
    {"user_id": 100, "name": "Alice",   "city": "Paris",       "age": 28, "plan": "premium"},
    {"user_id": 200, "name": "Bob",     "city": "Lyon",        "age": 34, "plan": "free"},
    {"user_id": 300, "name": "Charlie", "city": "Marseille",   "age": 22, "plan": "premium"},
    {"user_id": 400, "name": "Diana",   "city": "Toulouse",    "age": 45, "plan": "pro"},
    {"user_id": 500, "name": "Eve",     "city": "Bordeaux",    "age": 31, "plan": "free"},
    {"user_id": 600, "name": "Frank",   "city": "Nantes",      "age": 27, "plan": "premium"},
    {"user_id": 700, "name": "Grace",   "city": "Lille",       "age": 39, "plan": "pro"},
    {"user_id": 800, "name": "Hugo",    "city": "Nice",        "age": 52, "plan": "free"},
    {"user_id": 900, "name": "Iris",    "city": "Strasbourg",  "age": 25, "plan": "premium"},
    {"user_id": 999, "name": "Jules",   "city": "Montpellier", "age": 33, "plan": "pro"},
]

mode = sys.argv[1] if len(sys.argv) > 1 else "stream"

if mode == "batch":
    print(f"📦 Mode BATCH — Envoi unique de {len(USERS_BASE)} utilisateurs dans '{TOPIC_NAME}'")
    for user in USERS_BASE:
        user_with_ts = {**user, "event_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
        producer.send(TOPIC_NAME, user_with_ts)
        print(f"   ✅ {user['name']} (user_id={user['user_id']})")
    producer.flush()
    print(f"\n✅ Tous les utilisateurs envoyés. Le topic '{TOPIC_NAME}' peut être lu en mode Batch.")
else:
    print(f"🔄 Mode STREAM — Envoi continu dans '{TOPIC_NAME}'")
    print("   Appuyez sur Ctrl+C pour arrêter.\n")
    try:
        cycle = 0
        while True:
            cycle += 1
            for user in USERS_BASE:
                # Ajouter de légères variations pour simuler des mises à jour
                user_data = {
                    **user,
                    "last_login": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "event_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "session_score": round(random.uniform(0, 100), 1),
                }
                producer.send(TOPIC_NAME, user_data)
                print(f"   ✅ Cycle {cycle} — {user['name']} | score={user_data['session_score']}")
                time.sleep(random.uniform(0.3, 1.0))

            print(f"\n🔄 Cycle {cycle} terminé. Prochain dans 3s...\n")
            time.sleep(3)
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du générateur users stream.")
    finally:
        producer.close()
