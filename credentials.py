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
explore_client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_explore_client_id/versions/1"}).payload.data.decode()
explore_client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_explore_client_secret/versions/1"}).payload.data.decode()
watchdog_client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_watchdog_client_id/versions/1"}).payload.data.decode()
watchdog_client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_watchdog_client_secret/versions/1"}).payload.data.decode()
calc_client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_calc_client_id/versions/1"}).payload.data.decode()
calc_client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_calc_client_secret/versions/1"}).payload.data.decode()
service_url = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/api_service_url/versions/1"}).payload.data.decode()

# helper function to check for pairs of credentials
def correct(username, password, correct_username, correct_password):
    if compare_digest(username, correct_username) and compare_digest(password, correct_password):
        return True
    return False

# checks the credentials against valid ones
def authenticate(username, password):
    if correct(username, password, watchdog_client_id, watchdog_client_secret):
        return {"user": "watchdog", "metered": False, "calls": 0}
    elif correct(username, password, explore_client_id, explore_client_secret):
        return {"user": "explore", "metered": False, "calls": 0}
    elif correct(username, password, calc_client_id, calc_client_secret):
        return {"user": "calc", "metered": False, "calls": 0}
    else:
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, service_url)
        response = requests.post(service_url, json={"client_id": username, "client_secret": password}, headers={'Authorization': 'Bearer ' + id_token})
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            return False
    return False
