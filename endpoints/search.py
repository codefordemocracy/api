from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.cypher import search as cypher

import datetime

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/search",
    tags=["search"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# search for entities
#########################################################

@router.get("/candidates/", summary="Search for Candidates")
def graph_search_candidates(cand_name: str = None, cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_candidates, cand_name=cand_name, cand_pty_affiliation=cand_pty_affiliation, cand_office=cand_office, cand_office_st=cand_office_st, cand_office_district=cand_office_district, cand_election_yr=cand_election_yr, cand_ici=cand_ici, context=context, skip=skip, limit=limit, concise=False))

@router.get("/committees/", summary="Search for Committees")
def graph_search_committees(cmte_nm: str = None, cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    if cmte_nm is not None:
        cmte_nm = "\""+cmte_nm+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_committees, cmte_nm=cmte_nm, cmte_pty_affiliation=cmte_pty_affiliation, cmte_dsgn=cmte_dsgn, cmte_tp=cmte_tp, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@router.get("/donors/", summary="Search for Donors")
def graph_search_donors(name: str = None, employer: str = None, occupation: str = None, state: str = Query(None, min_length=2, max_length=2), zip_code: int = Query(None, ge=500, le=99999), entity_tp: str = Query(None, min_length=3, max_length=3), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    if employer is not None:
        employer = "\""+employer+"\""
    if occupation is not None:
        occupation = "\""+occupation+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_donors, name=name, employer=employer, occupation=occupation, state=state, zip_code=zip_code, entity_tp=entity_tp, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@router.get("/payees/", summary="Search for Payees")
def graph_search_payees(name: str = None, context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    if name is not None:
        name = "\""+name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_payees, name=name, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@router.get("/tweeters/", summary="Search for Tweeters")
def graph_search_tweeters(name: str = None, username: str = None, candidate: bool = False, cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    if name is not None:
        name = "\""+name+"\""
    if username is not None:
        username = username[1:] if username.startswith("@") else username
        username = "\""+username+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_tweeters, name=name, username=username, candidate=candidate, cand_pty_affiliation=cand_pty_affiliation, cand_election_yr=cand_election_yr, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@router.get("/sources/", summary="Search for Sources")
def graph_search_sources(domain: str = None, bias_score: str = None, factually_questionable_flag: int = Query(None, ge=0, le=1), conspiracy_flag: int = Query(None, ge=0, le=1), hate_group_flag: int = Query(None, ge=0, le=1), propaganda_flag: int = Query(None, ge=0, le=1), satire_flag: int = Query(None, ge=0, le=1), context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/buyers/", summary="Search for Buyers")
def graph_search_buyers(name: str = None, context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    if name is not None:
        name = "\""+name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_buyers, name=name, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))

@router.get("/pages/", summary="Search for Pages")
def graph_search_pages(name: str = None, context: bool = False, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    if name is not None:
        name = "\""+name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_pages, name=name, context=context, skip=skip, limit=limit, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day, concise=False))
