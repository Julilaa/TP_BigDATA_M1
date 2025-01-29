from kafka import KafkaProducer
import json
import time
from capteur import generate_transaction  # capteur.py doit être dans le même dossier


# Fonction pour créer un producteur Kafka
def create_producer():
    return KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],  # Adresse du cluster Kafka (l'adresse dans Conduktor)
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


# Fonction pour envoyer des transactions dans Kafka
def send_transactions(producer, topic, interval=1):
    try:
        while True:
            # Générer une transaction
            transaction = generate_transaction()

            # Envoyer la transaction au topic Kafka
            producer.send(topic, value=transaction)

            # Afficher la transaction envoyée
            print(f"Transaction envoyée : {transaction}")

            # Pause entre les envois
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Arrêt du producteur Kafka.")
    finally:
        producer.close()


if __name__ == "__main__":
    # Nom du topic dans Conduktor
    kafka_topic = "transactions"

    # Créer le producteur Kafka
    kafka_producer = create_producer()

    # Envoyer des transactions au topic
    send_transactions(kafka_producer, kafka_topic)
