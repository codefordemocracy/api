from fastapi import FastAPI, Request, Depends, HTTPException, status, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import ORJSONResponse

from neo4j import GraphDatabase
from elasticsearch import Elasticsearch
from google.cloud import firestore

import datetime
import pytz
import json
import pandas as pd
import math

import credentials
import cypher
import query
import helpers

# initialize app
app = FastAPI(
    title="Code for Democracy",
    description="""This API helps you access the data behind Code for Democracy's workflows. Use it to explore documents, audit analyses, or build your own apps.
    <dl>
        <dt>[Read Documentation](https://docs.codefordemocracy.org/data/api/)</dt>
        <dt>[View on GitHub](https://github.com/codefordemocracy/api/)</dt>
    </dl>
    """,
    version="0.0.1",
    docs_url="/view/endpoints/",
    redoc_url=None,
    default_response_class=ORJSONResponse
)
security = HTTPBasic()

# set up static files
templates = Jinja2Templates(directory="templates")

# connect to neo4j
driver = GraphDatabase.driver(credentials.neo4j_connection, auth=(credentials.neo4j_username_api, credentials.neo4j_password_api))

# connect to ElasticSearch
es = Elasticsearch(credentials.elastic_host, http_auth=(credentials.elastic_username_api, credentials.elastic_password_api), scheme="https", port=443)

# connect to Firestore
db = firestore.Client()

# set default min and max years
def get_years():
    return {
        "calendar": {
            "min": 1990,
            "max": datetime.datetime.now().year
        },
        "default": {
            "min": 2019,
            "max": 2020
        }
    }

# set default min and max cycles
def get_cycles():
    return {
        "min": 2016,
        "max": math.ceil(datetime.datetime.now().year/2.)*2,
        "current": 2020
    }

#########################################################
# handle authentication
#########################################################

def get_auth(header: HTTPBasicCredentials = Depends(security)):
    auth = credentials.authenticate(header.username, header.password)
    if not auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    if auth["metered"] and auth["calls"] > 1500:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS)
    return auth["user"]

#########################################################
# serve homepage
#########################################################

@app.get("/", include_in_schema=False)
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

#########################################################
# find graph elements
#########################################################

@app.get("/graph/find/elements/id/", summary="Find Graph Elements by ID", tags=["find"])
def graph_find_elements_id(nodes: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), edges: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), user: str = Depends(get_auth)):
    try:
        nodes = [int(n) for n in nodes.split(",")]
    except:
        nodes = None
    try:
        edges = [int(n) for n in edges.split(",")]
    except:
        edges = None
    if nodes is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_find_elements_id, nodes=nodes, edges=edges))

@app.get("/graph/find/elements/uuid/", summary="Find Graph Elements by UUID", tags=["find"])
def graph_find_elements_uuid(nodes: str, edges: str = None, user: str = Depends(get_auth)):
    try:
        nodes = nodes.split(",")
    except:
        nodes = None
    try:
        edges = edges.split(",")
    except:
        edges = None
    if nodes is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_find_elements_uuid, nodes=nodes, edges=edges))

#########################################################
# search for entities
#########################################################

@app.get("/graph/search/candidates/", summary="Search for Candidates", tags=["search"])
def graph_search_candidates(cand_name: str = None, cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_candidates, cand_name=cand_name, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, context=context, skip=skip, limit=limit, concise=False))

@app.get("/graph/search/committees/", summary="Search for Committees", tags=["search"])
def graph_search_committees(cmte_nm: str = None, cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if cmte_nm is not None:
        cmte_nm = "\""+cmte_nm+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_committees, cmte_nm=cmte_nm, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@app.get("/graph/search/donors/", summary="Search for Donors", tags=["search"])
def graph_search_donors(name: str = None, employer: str = None, occupation: str = None, state: str = Query(None, min_length=2, max_length=2), zip_code: int = Query(None, ge=500, le=99999), entity_tp: str = Query(None, min_length=3, max_length=3), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if employer is not None:
        employer = "\""+employer+"\""
    if occupation is not None:
        occupation = "\""+occupation+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_donors, name=name, employer=employer, occupation=occupation, state=state, zip_code=zip_code, entity_tp=entity_tp, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@app.get("/graph/search/payees/", summary="Search for Payees", tags=["search"])
def graph_search_payees(name: str = None, context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if name is not None:
        name = "\""+name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_payees, name=name, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@app.get("/graph/search/tweeters/", summary="Search for Tweeters", tags=["search"])
def graph_search_tweeters(name: str = None, username: str = None, candidate: bool = False, cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if name is not None:
        name = "\""+name+"\""
    if username is not None:
        username = username[1:] if username.startswith("@") else username
        username = "\""+username+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_tweeters, name=name, username=username, candidate=candidate, cand_pty_affiliation=cand_pty_affiliation, cand_election_yr=cand_election_yr, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@app.get("/graph/search/sources/", summary="Search for Sources", tags=["search"])
def graph_search_sources(domain: str = None, bias_score: str = None, factually_questionable_flag: int = Query(None, ge=0, le=1), conspiracy_flag: int = Query(None, ge=0, le=1), hate_group_flag: int = Query(None, ge=0, le=1), propaganda_flag: int = Query(None, ge=0, le=1), satire_flag: int = Query(None, ge=0, le=1), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if domain is not None:
        if domain.startswith("www."):
            domain = domain.split("www.",1)[1]
        domain = "\""+domain+"\""
    if bias_score is not None:
        try:
            bias_score = [int(i) for i in bias_score.split(",")]
        except:
            bias_score = None
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_sources, domain=domain, bias_score=bias_score, factually_questionable_flag=factually_questionable_flag, conspiracy_flag=conspiracy_flag, hate_group_flag=hate_group_flag, propaganda_flag=propaganda_flag, satire_flag=satire_flag, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@app.get("/graph/search/buyers/", summary="Search for Buyers", tags=["search"])
def graph_search_buyers(name: str = None, context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if name is not None:
        name = "\""+name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_buyers, name=name, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@app.get("/graph/search/pages/", summary="Search for Pages", tags=["search"])
def graph_search_pages(name: str = None, context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    if name is not None:
        name = "\""+name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_pages, name=name, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

#########################################################
# traverse graph
#########################################################

@app.get("/graph/traverse/neighbors/", summary="Traverse Graph and Find Neighbors", tags=["traverse"])
def graph_traverse_neighbors(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), labels: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        labels = ["OR b:" + i for i in labels.split(",")]
        labels[0] = labels[0].replace("OR ", "")
        labels = (" ").join(labels)
    except:
        labels = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_neighbors, ids=ids, labels=labels, skip=skip, limit=limit))

# associations - candidates

@app.get("/graph/traverse/associations/candidate/committee/", summary="Traverse Graph and Find Associations Between Candidates and Committees", tags=["traverse"])
def graph_traverse_associations_candidate_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("linkage", regex="linkage|expenditure"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_candidate_committee, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, intermediaries=intermediaries, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/associations/candidate/tweeter/", summary="Traverse Graph and Find Associations Between Candidates and Tweeters", tags=["traverse"])
def graph_traverse_associations_candidate_tweeter(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_candidate_tweeter, ids=ids, ids2=ids2, skip=skip, limit=limit))

# associations - committees

@app.get("/graph/traverse/associations/committee/candidate/", summary="Traverse Graph and Find Associations Between Committees and Candidates", tags=["traverse"])
def graph_traverse_associations_committee_candidate(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("linkage", regex="linkage|expenditure"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_candidate, ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, intermediaries=intermediaries, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/associations/committee/committee/", summary="Traverse Graph and Find Associations Between Committees and Committees", tags=["traverse"])
def graph_traverse_associations_committee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("contribution", regex="contribution|expenditure"), direction: str = Query(None, regex="receipts|disbursements"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_committee, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, intermediaries=intermediaries, direction=direction, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/associations/committee/donor/", summary="Traverse Graph and Find Associations Between Committees and Donors", tags=["traverse"])
def graph_traverse_associations_committee_donor(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), employer: str = None, occupation: str = None, state: str = Query(None, min_length=2, max_length=2), zip_code: int = Query(None, ge=500, le=99999), entity_tp: str = Query(None, min_length=3, max_length=3), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_donor, ids=ids, ids2=ids2, employer=employer, occupation=occupation, state=state, zip_code=zip_code, entity_tp=entity_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/associations/committee/payee/", summary="Traverse Graph and Find Associations Between Committees and Payees", tags=["traverse"])
def graph_traverse_associations_committee_payee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_payee, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# associations - donors

@app.get("/graph/traverse/associations/donor/committee/", summary="Traverse Graph and Find Associations Between Donors and Committees", tags=["traverse"])
def graph_traverse_associations_donor_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_donor_committee, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# associations - payees

@app.get("/graph/traverse/associations/payee/committee/", summary="Traverse Graph and Find Associations Between Payees and Committees", tags=["traverse"])
def graph_traverse_associations_payee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_payee_committee, ids=ids, ids2=ids2, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# associations - tweeters

@app.get("/graph/traverse/associations/tweeter/candidate/", summary="Traverse Graph and Find Associations Between Tweeters and Candidates", tags=["traverse"])
def graph_traverse_associations_tweeter_candidate(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_tweeter_candidate, ids=ids, ids2=ids2, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, skip=skip, limit=limit))

@app.get("/graph/traverse/associations/tweeter/source/", summary="Traverse Graph and Find Associations Between Tweeters and Sources", tags=["traverse"])
def graph_traverse_associations_tweeter_source(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), bias_score: str = None, factually_questionable_flag: int = Query(None, ge=0, le=1), conspiracy_flag: int = Query(None, ge=0, le=1), hate_group_flag: int = Query(None, ge=0, le=1), propaganda_flag: int = Query(None, ge=0, le=1), satire_flag: int = Query(None, ge=0, le=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if bias_score is not None:
        try:
            bias_score = [int(i) for i in bias_score.split(",")]
        except:
            bias_score = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_tweeter_source, ids=ids, ids2=ids2, bias_score=bias_score, factually_questionable_flag=factually_questionable_flag, conspiracy_flag=conspiracy_flag, hate_group_flag=hate_group_flag, propaganda_flag=propaganda_flag, satire_flag=satire_flag, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# associations - sources

@app.get("/graph/traverse/associations/source/tweeter/", summary="Traverse Graph and Find Associations Between Sources and Tweeters", tags=["traverse"])
def graph_traverse_associations_source_tweeter(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_source_tweeter, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# associations - buyers

@app.get("/graph/traverse/associations/buyer/page/", summary="Traverse Graph and Find Associations Between Buyers and Pages", tags=["traverse"])
def graph_traverse_associations_buyer_page(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_buyer_page, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# associations - pages

@app.get("/graph/traverse/associations/page/buyer/", summary="Traverse Graph and Find Associations Between Pages and Buyers", tags=["traverse"])
def graph_traverse_associations_page_buyer(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids2 is not None:
        try:
            ids2 = [int(i) for i in ids2.split(",")]
        except:
            ids2 = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_page_buyer, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - candidates

@app.get("/graph/traverse/intermediaries/candidate/committee/", summary="Traverse Graph and Find Intermediaries Between Candidates and Committees", tags=["traverse"])
def graph_traverse_intermediaries_candidate_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_candidate_committee, ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - committees

@app.get("/graph/traverse/intermediaries/committee/candidate/", summary="Traverse Graph and Find Intermediaries Between Committees and Candidates", tags=["traverse"])
def graph_traverse_intermediaries_committee_candidate(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_candidate, ids=ids, ids2=ids2, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/intermediaries/committee/committee/", summary="Traverse Graph and Find Intermediaries Between Committees and Committees", tags=["traverse"])
def graph_traverse_intermediaries_committee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), transaction_tp: str = Query(None, min_length=2, max_length=3), transaction_pgi: str = Query(None, min_length=1, max_length=1), rpt_tp: str = Query(None, min_length=2, max_length=3), amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("contribution", regex="contribution|expenditure"), direction: str = Query(None, regex="receipts|disbursements"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_committee, ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, intermediaries=intermediaries, direction=direction, sup_opp=sup_opp, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/intermediaries/committee/donor/", summary="Traverse Graph and Find Intermediaries Between Committees and Donors", tags=["traverse"])
def graph_traverse_intermediaries_committee_donor(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), transaction_tp: str = Query(None, min_length=2, max_length=3), transaction_pgi: str = Query(None, min_length=1, max_length=1), rpt_tp: str = Query(None, min_length=2, max_length=3), amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_donor, ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

@app.get("/graph/traverse/intermediaries/committee/payee/", summary="Traverse Graph and Find Intermediaries Between Committees and Payees", tags=["traverse"])
def graph_traverse_intermediaries_committee_payee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), type: str = Query(None, regex="independent|operating"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_payee, ids=ids, ids2=ids2, type=type, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - donors

@app.get("/graph/traverse/intermediaries/donor/committee/", summary="Traverse Graph and Find Intermediaries Between Donors and Committees", tags=["traverse"])
def graph_traverse_intermediaries_donor_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), transaction_tp: str = Query(None, min_length=2, max_length=3), transaction_pgi: str = Query(None, min_length=1, max_length=1), rpt_tp: str = Query(None, min_length=2, max_length=3), amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_donor_committee, ids=ids, ids2=ids2, transaction_tp=transaction_tp, transaction_pgi=transaction_pgi, rpt_tp=rpt_tp, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - payees

@app.get("/graph/traverse/intermediaries/payee/committee/", summary="Traverse Graph and Find Intermediaries Between Payees and Committees", tags=["traverse"])
def graph_traverse_intermediaries_payee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), type: str = Query(None, regex="independent|operating"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_payee_committee, ids=ids, ids2=ids2, type=type, sup_opp=sup_opp, purpose=purpose, amndt_ind=amndt_ind, gt=gt, lte=lte, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - tweeters

@app.get("/graph/traverse/intermediaries/tweeter/source/", summary="Traverse Graph and Find Intermediaries Between Tweeters and Sources", tags=["traverse"])
def graph_traverse_intermediaries_tweeter_source(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_tweeter_source, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - sources

@app.get("/graph/traverse/intermediaries/source/tweeter/", summary="Traverse Graph and Find Intermediaries Between Sources and Tweeters", tags=["traverse"])
def graph_traverse_intermediaries_source_tweeter(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_source_tweeter, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - buyers

@app.get("/graph/traverse/intermediaries/buyer/page/", summary="Traverse Graph and Find Intermediaries Between Buyers and Pages", tags=["traverse"])
def graph_traverse_intermediaries_buyer_page(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_buyer_page, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# intermediaries - pages

@app.get("/graph/traverse/intermediaries/page/buyer/", summary="Traverse Graph and Find Intermediaries Between Pages and Buyers", tags=["traverse"])
def graph_traverse_intermediaries_page_buyer(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    try:
        ids2 = [int(i) for i in ids2.split(",")]
    except:
        ids2 = None
    if ids is not None and ids2 is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_page_buyer, ids=ids, ids2=ids2, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day))

# relationships

@app.get("/graph/traverse/relationships/contribution/contributor/", summary="Traverse Graph and Find Contributors for Contributions", tags=["traverse"])
def graph_traverse_relationships_contribution_contributor(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_relationships_contribution_contributor, ids=ids, skip=skip, limit=limit))

@app.get("/graph/traverse/relationships/contribution/recipient/", summary="Traverse Graph and Find Recipients for Contributions", tags=["traverse"])
def graph_traverse_relationships_contribution_recipient(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), user: str = Depends(get_auth)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_relationships_contribution_recipient, ids=ids, skip=skip, limit=limit))

#########################################################
# uncover graph insights
#########################################################

# Uncover contributors

@app.get("/graph/uncover/donors/", summary="Uncover contributors to nodes", tags=["uncover"])
def graph_uncover_donors(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), labels: str = None, minTransactionAmt: int = Query(None, ge=1, le=999999999), limit: int = Query(None, ge=1, le=999999999), user: str = Depends(get_auth)):
    try:
        ids = [int(id) for id in ids.split(",")]
    except:
        ids = None
    try:
        labels = ["OR d:" + label for label in labels.split(",")]
        labels[0] = labels[0].replace("OR ", "")
        labels = (" ").join(labels)
    except:
        labels = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_uncover_donors, ids=ids, labels=labels, min_transaction_amt=minTransactionAmt, limit=limit))

#########################################################
# explore elasticsearch
#########################################################

@app.get("/documents/browse/news/articles/source/questionable/", summary="Explore Articles from Factually Questionable Sources", tags=["browse"])
def documents_browse_news_articles_source_questionable(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=1, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/conspiracy/", summary="Explore Articles from Conspiracy Sources", tags=["browse"])
def documents_browse_news_articles_source_conspiracy(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=1, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/hate/", summary="Explore Articles from Hate Group Sources", tags=["browse"])
def documents_browse_news_articles_source_hate(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=1, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/propaganda/", summary="Explore Articles from Propaganda Sources", tags=["browse"])
def documents_browse_news_articles_source_propaganda(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=1, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/satire/", summary="Explore Articles from Satire Sources", tags=["browse"])
def documents_browse_news_articles_source_satire(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=None, factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=1, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/liberal/", summary="Explore Articles from Liberal Sources", tags=["browse"])
def documents_browse_news_articles_source_liberal(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[-2], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/left/", summary="Explore Articles from Left Leaning Sources", tags=["browse"])
def documents_browse_news_articles_source_left(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[-1], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/moderate/", summary="Explore Articles from Moderate Sources", tags=["browse"])
def documents_browse_news_articles_source_moderate(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[0], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/right/", summary="Explore Articles from Right Leaning Sources", tags=["browse"])
def documents_browse_news_articles_source_right(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[1], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/news/articles/source/conservative/", summary="Explore Articles from Conservative Sources", tags=["browse"])
def documents_browse_news_articles_source_conservative(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        sources = neo4j.read_transaction(cypher.graph_search_sources, domain=None, bias_score=[2], factually_questionable_flag=None, conspiracy_flag=None, hate_group_flag=None, propaganda_flag=None, satire_flag=None, context=False, skip=0, limit=5000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    domains = [i["domain"] for i in sources]
    return query.documents_browse_news_articles_source(es, domains=domains, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/twitter/tweets/candidate/dem/", summary="Explore Tweets from Democratic Candidates", tags=["browse"])
def documents_browse_twitter_tweets_candidate_dem(text: str = None, cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        tweeters = neo4j.read_transaction(cypher.graph_search_tweeters, name=None, username=None, candidate=True, cand_pty_affiliation="DEM", cand_election_yr=cand_election_yr, context=False, skip=0, limit=50000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    user_ids = [i["user_id"] for i in tweeters]
    return query.documents_browse_twitter_tweets_user(es, user_ids=user_ids, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/twitter/tweets/candidate/rep/", summary="Explore Tweets from Republican Candidates", tags=["browse"])
def documents_browse_twitter_tweets_candidate_rep(text: str = None, cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    with driver.session() as neo4j:
        tweeters = neo4j.read_transaction(cypher.graph_search_tweeters, name=None, username=None, candidate=True, cand_pty_affiliation="REP", cand_election_yr=cand_election_yr, context=False, skip=0, limit=50000, min_year=0, max_year=0, min_month=0, max_month=0, min_day=0, max_day=0, concise=True)
    user_ids = [i["user_id"] for i in tweeters]
    return query.documents_browse_twitter_tweets_user(es, user_ids=user_ids, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

@app.get("/documents/browse/facebook/ads/", summary="Explore Ads", tags=["browse"])
def documents_browse_facebook_ads(text: str = None, histogram: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
    return query.documents_browse_facebook_ads(es, text=text, histogram=histogram, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate)

#########################################################
# preview entities
#########################################################

@app.get("/data/preview/organization/committee/", summary="Preview Committees", tags=["preview"])
def data_preview_organization_committee(lists: str = None, terms: str = None, ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        return query.data_preview_organization_committee(es, terms=terms, ids=ids, skip=skip, limit=limit, count=count)
    return []

@app.get("/data/preview/organization/employer/", summary="Preview Employers", tags=["preview"])
def data_preview_organization_employer(lists: str = None, terms: str = None, ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        return query.data_preview_organization_employer(es, terms=terms, ids=ids, skip=skip, limit=limit, count=count)
    return []

@app.get("/data/preview/person/candidate/", summary="Preview Candidates", tags=["preview"])
def data_preview_person_candidate(lists: str = None, terms: str = None, ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        return query.data_preview_person_candidate(es, terms=terms, ids=ids, skip=skip, limit=limit, count=count)
    return []

@app.get("/data/preview/person/donor/", summary="Preview Donors", tags=["preview"])
def data_preview_person_donor(lists: str = None, terms: str = None, ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        return query.data_preview_person_donor(es, terms=terms, ids=ids, skip=skip, limit=limit, count=count)
    return []

@app.get("/data/preview/job/", summary="Preview Jobs", tags=["preview"])
def data_preview_job(lists: str = None, terms: str = None, ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        return query.data_preview_job(es, terms=terms, ids=ids, skip=skip, limit=limit, count=count)
    return []

@app.get("/data/preview/topic/", summary="Preview Topics", tags=["preview"])
def data_preview_topic(lists: str = None, terms: str = None, ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    elements = []
    for term in terms or []:
        elements.append({
            "term": term
        })
    for id in ids or []:
        elements.append({
            "id": id
        })
    return elements

#########################################################
# calculate recipes
#########################################################

@app.get("/data/calculate/recipe/ad/", summary="Calculate Recipe for Ads", tags=["calculate"])
def data_calculate_recipe_ad(lists: str = None, terms: str = None, ids: str = None, template: str = Query(..., regex="D3WE|BuW8|N7Jk|P2HG|8HcR"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="amount"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_ad(template, es, terms=terms, ids=ids, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

@app.get("/data/calculate/recipe/contribution/", summary="Calculate Recipe for Contributions", tags=["calculate"])
def data_calculate_recipe_contribution(lists: str = None, terms: str = None, ids: str = None, template: str = Query(..., regex="ReqQ|NcFz|m4YC|7v4P|T5xv|Bs5W|6peF|F2mS|IQL2|P3JF|VqHR"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="amount|date"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_contribution(template, es, terms=terms, ids=ids, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

@app.get("/data/calculate/recipe/lobbying/", summary="Calculate Recipe for Lobbying Activity", tags=["calculate"])
def data_calculate_recipe_lobbying(lists: str = None, terms: str = None, ids: str = None, template: str = Query(..., regex="kMER|wLvp|MJdb|WGb3|PjyR|MK93|3Nrt|V5Gh|Q23x"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="date"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        if template in ["kMER", "wLvp", "MJdb"]:
            return query.data_calculate_recipe_lobbying_disclosures(template, es, terms=terms, ids=ids, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
        elif template in ["WGb3", "PjyR", "MK93", "3Nrt", "V5Gh", "Q23x"]:
            if template in ["WGb3", "3Nrt"]:
                template2 = "kMER"
            elif template in ["PjyR", "V5Gh"]:
                template2 = "wLvp"
            elif template in ["MK93", "Q23x"]:
                template2 = "MJdb"
            disclosures = query.data_calculate_recipe_lobbying_disclosures(template2, es, terms=terms, ids=ids, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=False, histogram=False, concise=True)
            ids[0] = [d["registrant_senate_id"] for d in disclosures]
            return query.data_calculate_recipe_lobbying_contributions(template, es, terms=terms, ids=ids, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

@app.get("/data/calculate/recipe/990/", summary="Calculate Recipe for IRS 990s", tags=["calculate"])
def data_calculate_recipe_990(lists: str = None, terms: str = None, ids: str = None, template: str = Query(..., regex="K23r|GCv2|P34n"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="amount"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False, user: str = Depends(get_auth)):
    clean = helpers.prepare_lists(lists, terms, ids, db)
    terms = clean["terms"]
    ids = clean["ids"]
    # grab elements
    if terms is not None or ids is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_990(template, es, terms=terms, ids=ids, skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

#########################################################
# analyze elements
#########################################################

@app.get("/data/analyze/count/in/", summary="Analyze Count of Unique Sources for Inflows of Money", tags=["analyze"])
def data_analyze_count_in(min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/count/in/candidate/", summary="Analyze Count of Unique Sources for Inflows of Money for a Candidate", tags=["analyze"])
def data_analyze_count_in_candidate(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/count/in/committee/", summary="Analyze Count of Unique Sources for Inflows of Money for a Committee", tags=["analyze"])
def data_analyze_count_in_committee(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/count/out/", summary="Analyze Count of Unique Targets for Outflows of Money", tags=["analyze"])
def data_analyze_count_out(min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/count/out/donor/", summary="Analyze Count of Unique Targets for Outflows of Money for a Donor", tags=["analyze"])
def data_analyze_count_out_donor(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/count/out/committee/", summary="Analyze Count of Unique Targets for Outflows of Money for a Committee", tags=["analyze"])
def data_analyze_count_out_committee(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/affinity/in/candidate/candidate/", summary="Analyze Affinity Between Candidate and Candidate Based on Inflows of Money", tags=["analyze"])
def data_analyze_affinity_in_candidate_candidate(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_candidate_candidate, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/in/candidate/committee/", summary="Analyze Affinity Between Candidate and Committee Based on Inflows of Money", tags=["analyze"])
def data_analyze_affinity_in_candidate_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_candidate_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/in/committee/committee/", summary="Analyze Affinity Between Committee and Committee Based on Inflows of Money", tags=["analyze"])
def data_analyze_affinity_in_committee_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_committee_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/in/committee/candidate/", summary="Analyze Affinity Between Committee and Candidate Based on Inflows of Money", tags=["analyze"])
def data_analyze_affinity_in_committee_candidate(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_committee_candidate, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/out/committee/committee/", summary="Analyze Affinity Between Donor and Committee Based on Outflows of Money", tags=["analyze"])
def data_analyze_affinity_out_committee_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_committee_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/out/committee/donor/", summary="Analyze Affinity Between Donor and Donor Based on Outflows of Money", tags=["analyze"])
def data_analyze_affinity_out_committee_donor(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_committee_donor, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/out/donor/committee/", summary="Analyze Affinity Between Donor and Committee Based on Outflows of Money", tags=["analyze"])
def data_analyze_affinity_out_donor_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_donor_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/affinity/out/donor/donor/", summary="Analyze Affinity Between Donor and Donor Based on Outflows of Money", tags=["analyze"])
def data_analyze_affinity_out_donor_donor(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_donor_donor, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@app.get("/data/analyze/sum/revenue/candidate/", summary="Analyze the Sum of Revenue for a Candidate", tags=["analyze"])
def data_analyze_sum_revenue_candidate(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/sum/revenue/committee/", summary="Analyze the Sum of Revenue for a Committee", tags=["analyze"])
def data_analyze_sum_revenue_committee(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/sum/wallet/committee/", summary="Analyze the Sum of Wallet for a Committee", tags=["analyze"])
def data_analyze_sum_wallet_committee(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/sum/wallet/donor/", summary="Analyze the Sum of Wallet for a Donor", tags=["analyze"])
def data_analyze_sum_wallet_donor(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@app.get("/data/analyze/share/revenue/candidate/committee/", summary="Analyze the Share of Revenue that a Committee Comprises for a Candidate", tags=["analyze"])
def data_analyze_share_revenue_candidate_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/revenue/candidate/donor/", summary="Analyze the Share of Revenue that a Donor Comprises for a Candidate", tags=["analyze"])
def data_analyze_share_revenue_candidate_donor(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate_donor, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/revenue/committee/committee/", summary="Analyze the Share of Revenue that a Committee Comprises for a Committee", tags=["analyze"])
def data_analyze_share_revenue_committee_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/revenue/committee/donor/", summary="Analyze the Share of Revenue that a Donor Comprises for a Committee", tags=["analyze"])
def data_analyze_share_revenue_committee_donor(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee_donor, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/wallet/committee/candidate/", summary="Analyze the Share of Wallet that a Candidate Comprises for a Committee", tags=["analyze"])
def data_analyze_share_wallet_committee_candidate(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee_candidate, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/wallet/committee/committee/", summary="Analyze the Share of Wallet that a Committee Comprises for a Committee", tags=["analyze"])
def data_analyze_share_wallet_committee_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/wallet/donor/candidate/", summary="Analyze the Share of Wallet that a Candidate Comprises for a Donor", tags=["analyze"])
def data_analyze_share_wallet_donor_candidate(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor_candidate, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@app.get("/data/analyze/share/wallet/donor/committee/", summary="Analyze the Share of Wallet that a Committee Comprises for a Donor", tags=["analyze"])
def data_analyze_share_wallet_donor_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), user: str = Depends(get_auth)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)
