from neo4j import GraphDatabase
from elasticsearch import Elasticsearch
from google.cloud import firestore

from . import credentials

#########################################################
# databases
#########################################################

# neo4j
driver = GraphDatabase.driver(credentials.neo4j_connection, auth=(credentials.neo4j_username_api, credentials.neo4j_password_api))

# elasticsearch
es = Elasticsearch(credentials.elastic_host, http_auth=(credentials.elastic_username_api, credentials.elastic_password_api), scheme="https", port=443)

# firestore
db = firestore.Client()
