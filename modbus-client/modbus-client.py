import time
import yaml
import psycopg2
from psycopg2 import OperationalError
from pymodbus.client import ModbusTcpClient
import sys

sys.stdout.reconfigure(line_buffering=True)


# Charger la configuration depuis le fichier YAML
with open("modbus-data.yml", "r") as file:
    config = yaml.safe_load(file)

def connect_postgres():
    while True:
        try:
            conn = psycopg2.connect(
                dbname="mydb",
                user="admin",
                password="admin",
                host="postgres",
                port="5432"
            )
            print("Connexion PostgreSQL établie.")
            return conn
        except OperationalError as e:
            print(f"Erreur PostgreSQL : {e}. Nouvelle tentative dans 5 secondes...")
            time.sleep(5)

def connect_modbus():
    while True:
        try:
            client = ModbusTcpClient("modbus-server", port=1502)
            if client.connect():
                print("Connexion Modbus établie.")
                return client
            else:
                print("Échec connexion Modbus. Nouvelle tentative dans 5 secondes...")
                time.sleep(5)
        except Exception as e:
            print(f"Erreur connexion Modbus : {e}. Nouvelle tentative dans 5 secondes...")
            time.sleep(5)

# Initialisation des connexions
conn = connect_postgres()
cur = conn.cursor()
modbus_client = connect_modbus()

def read_register(register):
    address = register["address"]
    data_type = register["data_type"]
    reg_type = register["type"]

    if reg_type == "holding":
        result = modbus_client.read_holding_registers(address, count=2 if data_type == "INT32" else 1)
    elif reg_type == "input":
        result = modbus_client.read_input_registers(address, count=2 if data_type == "INT32" else 1)
    else:
        print(f"Type de registre non supporté: {reg_type}")
        return None

    if result.isError():
        print(f"Erreur lecture registre {register['name']}")
        return None

    # Récupération de la valeur brute
    if data_type == "INT16":
        return int(result.registers[0])
    elif data_type == "INT32":
        return (result.registers[0] << 16) + result.registers[1]  # Gérer INT32 (Big-Endian)
    else:
        print(f"Type de donnée non supporté: {data_type}")
        return None

def insert_into_postgres(register_name, value):
    global conn, cur
    try:
        ## pour Postgre, il faut créer la table avant d'insérer des données
        cur.execute("""
            CREATE TABLE IF NOT EXISTS modbus2 (
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                register_name TEXT NOT NULL,
                register_value INTEGER NOT NULL
            )
        """)
        
        cur.execute(
            "INSERT INTO modbus2 (register_name, register_value) VALUES (%s, %s)",
            (register_name, value)
        )
        conn.commit()
    except OperationalError:
        print("Perte connexion PostgreSQL, tentative de reconnexion...")
        conn = connect_postgres()
        cur = conn.cursor()  # Recréer le curseur
    except Exception as e:
        print(f"Erreur SQL : {e}")

while True:
    try:
        for register in config["registers"]:
            value = read_register(register)
            if value is not None:
                insert_into_postgres(register["name"], value)
                print(f"Enregistré: {register['name']} -> {value}")

        time.sleep(2) 
    
    except Exception as e:
        print(f"⚠️ Erreur principale : {e}, nouvelle tentative dans 5 secondes...")
        time.sleep(5)
