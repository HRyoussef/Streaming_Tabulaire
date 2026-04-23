import socket
import time

HOST = "localhost"
PORT = 9999
FICHIER = "yellow_tripdata_2026-01.csv"
VITESSE = 100  # lignes par seconde

def demarrer_serveur():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"Producteur prêt — port {PORT} — vitesse {VITESSE} lignes/s")
    return server

def emettre_donnees(client):
    try:
        with open(FICHIER, "r") as f:
            for i, line in enumerate(f):
                client.send((line + "\n").encode())
                time.sleep(1 / VITESSE)
                if i % 1000 == 0:
                    print(f"{i} lignes envoyées...")
    except BrokenPipeError:
        print("Client déconnecté.")
    finally:
        client.close()
        print("Connexion fermée.")

def main():
    server = demarrer_serveur()
    try:
        while True:  # accepte plusieurs connexions successives
            print("En attente d'un client...")
            client, address = server.accept()
            print(f"Client connecté : {address}")
            emettre_donnees(client)
    except KeyboardInterrupt:
        print("\nServeur arrêté (Ctrl+C).")
    finally:
        server.close()

if __name__ == "__main__":
    main()