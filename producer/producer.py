"""
Producteur de tickets clients pour InduTechData.

Ce script génère des tickets clients simulés et les envoie
dans le topic Redpanda 'client_tickets' en temps réel.

Champs produits (conformes à l'énoncé) :
    - ticket_id : identifiant unique du ticket
    - client_id : identifiant du client
    - created_at : date et heure de création (ISO 8601)
    - request : description de la demande
    - request_type : catégorie de la demande
    - priority : niveau de priorité (low, medium, high, critical)
"""

import json
import os
import time
import uuid
import random
from datetime import datetime

from confluent_kafka import Producer
from faker import Faker

# ── Configuration ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "client_tickets")
PRODUCE_INTERVAL = float(os.getenv("PRODUCE_INTERVAL", "1.0"))

fake = Faker("fr_FR")

# ── Données métier réalistes ───────────────────────────────────
REQUEST_TYPES = {
    "incident_technique": [
        "Capteur IoT ne remonte plus de données depuis 2h",
        "Erreur de connexion au dashboard de monitoring",
        "Latence excessive sur le flux de données temps réel",
        "Perte de données intermittente sur la ligne de production",
        "Alarme critique non déclenchée malgré seuil dépassé",
    ],
    "demande_information": [
        "Comment configurer un nouveau capteur sur la plateforme ?",
        "Demande de documentation API pour l'intégration ERP",
        "Quelle est la fréquence d'échantillonnage recommandée ?",
        "Comment exporter les rapports mensuels en CSV ?",
        "Besoin d'informations sur les limites de rétention des données",
    ],
    "demande_evolution": [
        "Ajout d'un nouveau type de capteur (température + humidité)",
        "Création d'un dashboard personnalisé pour l'équipe qualité",
        "Intégration avec le nouveau système MES de l'usine",
        "Mise en place d'alertes prédictives basées sur le ML",
        "Extension de la capacité de stockage pour les logs IoT",
    ],
    "maintenance": [
        "Mise à jour planifiée du firmware des capteurs",
        "Recalibration nécessaire des sondes de pression",
        "Nettoyage et optimisation de la base de données historique",
        "Renouvellement des certificats SSL de la plateforme",
        "Vérification de l'intégrité des backups mensuels",
    ],
    "facturation": [
        "Erreur sur la facture du mois de janvier",
        "Demande de détail de consommation par service cloud",
        "Question sur la tarification du nouveau plan entreprise",
        "Demande d'avoir suite à une interruption de service",
        "Mise à jour des coordonnées de facturation",
    ],
}

# Distribution réaliste des priorités (plus de low/medium que de critical)
PRIORITY_WEIGHTS = {
    "low": 0.30,
    "medium": 0.40,
    "high": 0.20,
    "critical": 0.10,
}

# Pool de clients simulés (industriels)
CLIENT_IDS = [f"CLI-{str(i).zfill(4)}" for i in range(1, 51)]


def create_ticket() -> dict:
    """Génère un ticket client réaliste."""
    request_type = random.choice(list(REQUEST_TYPES.keys()))
    request_text = random.choice(REQUEST_TYPES[request_type])
    priority = random.choices(
        list(PRIORITY_WEIGHTS.keys()),
        weights=list(PRIORITY_WEIGHTS.values()),
        k=1,
    )[0]

    return {
        "ticket_id": f"TK-{uuid.uuid4().hex[:8].upper()}",
        "client_id": random.choice(CLIENT_IDS),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "request": request_text,
        "request_type": request_type,
        "priority": priority,
    }


def delivery_report(err, msg):
    """Callback appelé à chaque livraison de message."""
    if err is not None:
        print(f"[ERREUR] Échec livraison: {err}")
    else:
        print(
            f"[OK] Ticket envoyé -> partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():
    """Boucle principale de production de tickets."""
    print("=" * 60)
    print("  InduTechData - Producteur de Tickets Clients")
    print(f"  Broker: {KAFKA_BROKER}")
    print(f"  Topic:  {TOPIC_NAME}")
    print(f"  Intervalle: {PRODUCE_INTERVAL}s")
    print("=" * 60)

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "indutech-ticket-producer",
        "acks": "all",  # Garantie de livraison maximale
    })

    ticket_count = 0

    try:
        while True:
            ticket = create_ticket()
            ticket_json = json.dumps(ticket, ensure_ascii=False)

            # Utilise le client_id comme clé de partitionnement
            # -> Tous les tickets d'un même client vont dans la même partition
            # -> Garantit l'ordre de traitement par client
            producer.produce(
                topic=TOPIC_NAME,
                key=ticket["client_id"],
                value=ticket_json.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            ticket_count += 1
            if ticket_count % 10 == 0:
                print(
                    f"\n📊 Statistiques: {ticket_count} tickets produits | "
                    f"Dernier: {ticket['ticket_id']} | "
                    f"Type: {ticket['request_type']} | "
                    f"Priorité: {ticket['priority']}\n"
                )

            time.sleep(PRODUCE_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n🛑 Arrêt du producteur. Total: {ticket_count} tickets produits.")
    finally:
        producer.flush(timeout=10)
        print("✅ Buffer vidé. Producteur arrêté proprement.")


if __name__ == "__main__":
    main()
