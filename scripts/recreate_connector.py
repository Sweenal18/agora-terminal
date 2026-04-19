import subprocess
import time

import requests

# Stop connector first
requests.delete("http://localhost:8083/connectors/agora-instruments-cdc")
print("Connector deleted, waiting 5s...")
time.sleep(5)

# Now drop the slot
result = subprocess.run([
    "docker", "exec", "agora-postgres", "psql", "-U", "agora", "-d", "agora",
    "-c", "SELECT pg_drop_replication_slot('agora_instruments_slot');"
], capture_output=True, text=True)
print("Drop slot:", result.stdout, result.stderr)

time.sleep(2)

# Recreate connector with snapshot.mode=always
config = {
    "name": "agora-instruments-cdc",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "agora",
        "database.password": "change_me_in_production",
        "database.dbname": "agora",
        "database.server.name": "agora",
        "topic.prefix": "agora",
        "table.include.list": "public.instruments",
        "plugin.name": "pgoutput",
        "slot.name": "agora_instruments_slot",
        "publication.name": "agora_instruments_pub",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "snapshot.mode": "always"
    }
}
res = requests.post("http://localhost:8083/connectors", json=config, headers={"Content-Type": "application/json"})
print("Recreate:", res.status_code)
