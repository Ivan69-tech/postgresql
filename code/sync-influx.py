import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import time
from datetime import datetime
import json
import os
from dotenv import load_dotenv
import sys

#load le .env
load_dotenv(override=True)

#pour avoir la visu des logs dans docker
sys.stdout.reconfigure(line_buffering=True)


def conversion_time(last_successful_time):
    if isinstance(last_successful_time, datetime):
        return last_successful_time.strftime("%Y-%m-%dT%H:%M:%SZ")  
    return last_successful_time  


REMOTE_BUCKET = os.getenv("REMOTE_BUCKET")
REMOTE_ORG = os.getenv("REMOTE_ORG")
REMOTE_TOKEN = os.getenv("REMOTE_TOKEN")
REMOTE_URL = os.getenv("REMOTE_URL")

LOCAL_BUCKET = os.getenv("LOCAL_BUCKET")
LOCAL_ORG = os.getenv("LOCAL_ORG")
LOCAL_TOKEN = os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN")
LOCAL_URL = os.getenv("LOCAL_URL")

print(LOCAL_URL)

client_remote = influxdb_client.InfluxDBClient(url=REMOTE_URL, token=REMOTE_TOKEN, org=REMOTE_ORG)
client_local = influxdb_client.InfluxDBClient(url=LOCAL_URL, token=LOCAL_TOKEN, org=LOCAL_ORG)


write_api = client_remote.write_api(write_options=SYNCHRONOUS)
query_api = client_local.query_api()



while True:
    
    with open('./data/lastSuccessFullTime.json') as f:
        last_successful_time = datetime.strptime(json.load(f)["lastSuccessFullTime"], "%Y-%m-%d %H:%M:%S")
    
    try:
        last_successful_time_str = conversion_time(last_successful_time)
        print(f"Tentative de synchronisation depuis {last_successful_time_str}...")

        query = f"""
        from(bucket: "{LOCAL_BUCKET}")
        |> range(start: {last_successful_time_str})
        |> filter(fn: (r) => r["_measurement"] == "modbus")
        |> sort(columns: ["_time"])  
        """

        result = query_api.query(org=LOCAL_ORG, query=query)

        print("pull de la database locale ok")
        points = []  
        data_sent = False

        #stockage dans la variable points pour ensuite envoyer tout d'un coup.
        for table in result:
            for record in table.records:
                field = record.get_field()
                value = int(record.get_value())  
                timestamp = record.get_time()
                p = influxdb_client.Point("Modbus").field(field, value).time(timestamp)
                points.append(p)  
        
        #envoyer les données sur le serveur distant d'un coup
        write_api.write(bucket=REMOTE_BUCKET, org=REMOTE_ORG, record=points)  
        print(f"{len(points)} points envoyés en une seule requête.")
        data_sent = True
        
        #mettre à jour la dernière date d'envoie des données au serveur
        if data_sent :
            dataToWrite = {"lastSuccessFullTime": timestamp.strftime("%Y-%m-%d %H:%M:%S")}
            with open('./data/lastSuccessFullTime.json', 'w') as f:
                json.dump(dataToWrite, f, default=str)
        else :
            print("Aucune nouvelle donnée à envoyer.")

    #perte de com
    except influxdb_client.rest.ApiException as e:
        print(f"Erreur InfluxDB distante : {e}")
        print("Attente avant une nouvelle tentative...")

    except Exception as e:
        print(f"Erreur inconnue : {e}")

    time.sleep(15)
