import time
from confluent_kafka import Producer

FICHIER = "yellow_tripdata_2026-01.csv"
TOPIC   = "taxi-stream"
VITESSE = 100

conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'retries': 5,
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f"Erreur envoi : {err}")

def main():
    print(f"Producteur Kafka prêt → topic '{TOPIC}'")
    try:
        with open(FICHIER, "r") as f:
            next(f)  # saute l'en-tête
            for i, line in enumerate(f):
                producer.produce(
                    topic=TOPIC,
                    value=line.strip().encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                time.sleep(1 / VITESSE)

                if i % 1000 == 0:
                    print(f"{i} lignes envoyées...")
                    producer.flush()

    except KeyboardInterrupt:
        print("\nArrêt producteur.")
    finally:
        producer.flush()
        print("Terminé.")

if __name__ == "__main__":
    main()
