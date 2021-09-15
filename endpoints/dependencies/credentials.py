from google.cloud import secretmanager
import google.auth.transport.requests
import google.oauth2.id_token
from secrets import compare_digest
import datetime
import requests
import json

secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_api = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_api/versions/1"}).payload.data.decode()
elastic_password_api = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_api/versions/1"}).payload.data.decode()
neo4j_connection = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_connection/versions/1"}).payload.data.decode()
neo4j_username_api = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_username_api/versions/1"}).payload.data.decode()
neo4j_password_api = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_password_api/versions/1"}).payload.data.decode()
service_url = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_service_url/versions/1"}).payload.data.decode()
clients = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_clients/versions/1"}).payload.data.decode().split(",")

# generate pairs of keys
pairs = []
for client in clients:
    client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_" + client + "_client_id/versions/1"}).payload.data.decode()
    client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_" + client + "_client_secret/versions/1"}).payload.data.decode()
    pairs.append({"client": client, "client_id": client_id, "client_secret": client_secret})

# checks the credentials against valid ones
def authenticate(username, password):
    for pair in pairs:
        if compare_digest(username, pair["client_id"]) and compare_digest(password, pair["client_secret"]):
            return {"user": pair["client"], "metered": False, "calls": 0}
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, service_url)
    response = requests.post(service_url, json={"client_id": username, "client_secret": password}, headers={'Authorization': 'Bearer ' + id_token})
    if response.status_code == 200:
        return json.loads(response.text)
    return False
