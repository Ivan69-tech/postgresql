import time
import yaml
import psycopg2
from pymodbus.client import ModbusTcpClient

# Charger la configuration depuis le fichier YAML
with open("modbus-data.yml", "r") as file:
    config = yaml.safe_load(file)

# Connexion PostgreSQL
conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Connexion Modbus
modbus_client = ModbusTcpClient("localhost", port=1502)

def read_register(register):
    """Lit un registre Modbus et retourne sa valeur."""
    address = register["address"]
    data_type = register["data_type"]
    reg_type = register["type"]

    if reg_type == "holding":
        result = modbus_client.read_holding_registers(address)
    elif reg_type == "input":
        result = modbus_client.read_input_registers(address)
    else:
        print(f"Type de registre non supporté: {reg_type}")
        return None

    if result.isError():
        print(f"Erreur lecture registre {register['name']}")
        return None
    
    raw_value = result.registers[0]

    if data_type == "INT16":
        return int(raw_value)
    elif data_type == "INT32":
        return int(raw_value)  # TODO: Modifier pour gérer les INT32
    else:
        print(f"Type de donnée non supporté: {data_type}")
        return None

def insert_into_postgres(register_name, value):


    cur.execute("""
    CREATE TABLE IF NOT EXISTS modbus (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        register_name TEXT NOT NULL,
        register_value INTEGER NOT NULL
    )
""")
    
    cur.execute(
        "INSERT INTO modbus (register_name, register_value) VALUES (%s, %s)",
        (register_name, value)
    )
    conn.commit()

while True:
    for register in config["registers"]:
        value = read_register(register)
        if value is not None:
            insert_into_postgres(register["name"], value)
            print(f"Enregistré: {register['name']} -> {value}")
    
    time.sleep(2)  # Attente entre deux lectures

# Fermer les connexions
modbus_client.close()
cur.close()
conn.close()
