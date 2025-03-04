import psycopg2
import time
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import sys
import io

# Charger le fichier .env
load_dotenv(override=True)

# Pour voir les logs en temps réel dans Docker
sys.stdout.reconfigure(line_buffering=True)
sys.stdout.flush() 

# Chargement des variables d'environnement
DBNAME_REMOTE = os.getenv("DBNAME_REMOTE")
USER_REMOTE = os.getenv("USER_REMOTE")
PASSWORD_REMOTE = os.getenv("PASSWORD_REMOTE")
HOST_REMOTE = os.getenv("HOST_REMOTE")
PORT_REMOTE = os.getenv("PORT_REMOTE")

DBNAME_LOCAL = os.getenv("DBNAME_LOCAL")
USER_LOCAL = os.getenv("USER_LOCAL")
PASSWORD_LOCAL = os.getenv("PASSWORD_LOCAL")
HOST_LOCAL = os.getenv("HOST_LOCAL")
PORT_LOCAL = os.getenv("PORT_LOCAL")


def connect_postgres(dbname, user, password, host, port):
    """Établit une connexion PostgreSQL et gère les erreurs."""
    while True:
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port,
                connect_timeout=10
            )
            print(f"✅ Connexion PostgreSQL {host} établie.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"⚠️ Erreur PostgreSQL {host} : {e}. Nouvelle tentative dans 5 secondes...")
            time.sleep(5)


# Connexion initiale
conn_local = connect_postgres(DBNAME_LOCAL, USER_LOCAL, PASSWORD_LOCAL, HOST_LOCAL, PORT_LOCAL)
conn_remote = connect_postgres(DBNAME_REMOTE, USER_REMOTE, PASSWORD_REMOTE, HOST_REMOTE, PORT_REMOTE)

cur_local = conn_local.cursor()
cur_remote = conn_remote.cursor()

## créer la table remote si elle n'existe pas. Toujours à faire sur postgresql        
cur_remote.execute("""
    CREATE TABLE IF NOT EXISTS modbus2 (
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        register_name TEXT NOT NULL,
        register_value INTEGER NOT NULL
    )
""")

while True:
    try:
        # Charger le dernier timestamp enregistré
        with open('./data/lastSuccessFullTime.json') as f:
            last_successful_time = datetime.strptime(json.load(f)["lastSuccessFullTime"], "%Y-%m-%d %H:%M:%S")

        print(f"Tentative de synchronisation depuis {last_successful_time}...")

        # Requête SQL pour récupérer les nouvelles données locales
        cur_local.execute("""
            SELECT timestamp, register_name, register_value 
            FROM modbus2 
            WHERE timestamp > %s 
            ORDER BY timestamp ASC
        """, (last_successful_time,))
        
        rows = cur_local.fetchall()
        print(f" {len(rows)} nouvelles entrées récupérées de la base locale.")

        # utiliser la méthode COPY pour gagner en efficacité lors de l'envoi des données (sinon trop long)
        if rows:
            buffer = io.StringIO()
            
            for row in rows:
                buffer.write("\t".join(map(str, row)) + "\n")  
            
            buffer.seek(0)  
            
            with conn_remote.cursor() as cur_remote:
                cur_remote.copy_from(buffer, 'modbus2', columns=('timestamp', 'register_name', 'register_value'), sep="\t")
            
            conn_remote.commit()
            print(f"{len(rows)} entrées envoyées à la base distante via COPY.")

            # Mettre à jour le fichier JSON avec le dernier timestamp
            last_timestamp = rows[-1][0]
            dataToWrite = {"lastSuccessFullTime": last_timestamp.strftime("%Y-%m-%d %H:%M:%S")}
            with open('./data/lastSuccessFullTime.json', 'w') as f:
                json.dump(dataToWrite, f)

        else:
            print("Aucune nouvelle donnée à synchroniser.")

    except psycopg2.OperationalError:
        print("Perte connexion PostgreSQL, tentative de reconnexion...")
        conn_local = connect_postgres(DBNAME_LOCAL, USER_LOCAL, PASSWORD_LOCAL, HOST_LOCAL, PORT_LOCAL)
        conn_remote = connect_postgres(DBNAME_REMOTE, USER_REMOTE, PASSWORD_REMOTE, HOST_REMOTE, PORT_REMOTE)
        cur_local = conn_local.cursor()
        cur_remote = conn_remote.cursor()

    except Exception as e:
        print(f"Erreur inconnue : {e}")

    time.sleep(15)
