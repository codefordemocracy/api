from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.cypher import traverse as cypher

import datetime

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/traverse",
    tags=["traverse"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# traverse graph
#########################################################

@router.get("/neighbors/", summary="Traverse Graph and Find Neighbors")
def graph_traverse_neighbors(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), labels: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
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

@router.get("/associations/candidate/committee/", summary="Traverse Graph and Find Associations Between Candidates and Committees")
def graph_traverse_associations_candidate_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("linkage", regex="linkage|expenditure"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/candidate/tweeter/", summary="Traverse Graph and Find Associations Between Candidates and Tweeters")
def graph_traverse_associations_candidate_tweeter(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
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

@router.get("/associations/committee/candidate/", summary="Traverse Graph and Find Associations Between Committees and Candidates")
def graph_traverse_associations_committee_candidate(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("linkage", regex="linkage|expenditure"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/committee/committee/", summary="Traverse Graph and Find Associations Between Committees and Committees")
def graph_traverse_associations_committee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("contribution", regex="contribution|expenditure"), direction: str = Query(None, regex="receipts|disbursements"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/committee/donor/", summary="Traverse Graph and Find Associations Between Committees and Donors")
def graph_traverse_associations_committee_donor(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), employer: str = None, occupation: str = None, state: str = Query(None, min_length=2, max_length=2), zip_code: int = Query(None, ge=500, le=99999), entity_tp: str = Query(None, min_length=3, max_length=3), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/committee/payee/", summary="Traverse Graph and Find Associations Between Committees and Payees")
def graph_traverse_associations_committee_payee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/donor/committee/", summary="Traverse Graph and Find Associations Between Donors and Committees")
def graph_traverse_associations_donor_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/payee/committee/", summary="Traverse Graph and Find Associations Between Payees and Committees")
def graph_traverse_associations_payee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cmte_pty_affiliation: str = Query(None, min_length=3, max_length=3), cmte_dsgn: str = Query(None, min_length=1, max_length=1), cmte_tp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/tweeter/candidate/", summary="Traverse Graph and Find Associations Between Tweeters and Candidates")
def graph_traverse_associations_tweeter_candidate(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
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

@router.get("/associations/tweeter/source/", summary="Traverse Graph and Find Associations Between Tweeters and Sources")
def graph_traverse_associations_tweeter_source(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), bias_score: str = None, factually_questionable_flag: int = Query(None, ge=0, le=1), conspiracy_flag: int = Query(None, ge=0, le=1), hate_group_flag: int = Query(None, ge=0, le=1), propaganda_flag: int = Query(None, ge=0, le=1), satire_flag: int = Query(None, ge=0, le=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/source/tweeter/", summary="Traverse Graph and Find Associations Between Sources and Tweeters")
def graph_traverse_associations_source_tweeter(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/buyer/page/", summary="Traverse Graph and Find Associations Between Buyers and Pages")
def graph_traverse_associations_buyer_page(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/associations/page/buyer/", summary="Traverse Graph and Find Associations Between Pages and Buyers")
def graph_traverse_associations_page_buyer(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(None, regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/candidate/committee/", summary="Traverse Graph and Find Intermediaries Between Candidates and Committees")
def graph_traverse_intermediaries_candidate_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/committee/candidate/", summary="Traverse Graph and Find Intermediaries Between Committees and Candidates")
def graph_traverse_intermediaries_committee_candidate(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/committee/committee/", summary="Traverse Graph and Find Intermediaries Between Committees and Committees")
def graph_traverse_intermediaries_committee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), transaction_tp: str = Query(None, min_length=2, max_length=3), transaction_pgi: str = Query(None, min_length=1, max_length=1), rpt_tp: str = Query(None, min_length=2, max_length=3), amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, cand_pty_affiliation: str = Query(None, min_length=3, max_length=3), cand_office: str = Query(None, min_length=1, max_length=1), cand_office_st: str = Query(None, min_length=2, max_length=2), cand_office_district: str = Query(None, min_length=2, max_length=2), cand_election_yr: int = Query(None, ge=1990, le=datetime.datetime.now().year), cand_ici: str = Query(None, min_length=1, max_length=1), intermediaries: str = Query("contribution", regex="contribution|expenditure"), direction: str = Query(None, regex="receipts|disbursements"), sup_opp: str = Query(None, min_length=1, max_length=1), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/committee/donor/", summary="Traverse Graph and Find Intermediaries Between Committees and Donors")
def graph_traverse_intermediaries_committee_donor(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), transaction_tp: str = Query(None, min_length=2, max_length=3), transaction_pgi: str = Query(None, min_length=1, max_length=1), rpt_tp: str = Query(None, min_length=2, max_length=3), amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/committee/payee/", summary="Traverse Graph and Find Intermediaries Between Committees and Payees")
def graph_traverse_intermediaries_committee_payee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), type: str = Query(None, regex="independent|operating"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/donor/committee/", summary="Traverse Graph and Find Intermediaries Between Donors and Committees")
def graph_traverse_intermediaries_donor_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), transaction_tp: str = Query(None, min_length=2, max_length=3), transaction_pgi: str = Query(None, min_length=1, max_length=1), rpt_tp: str = Query(None, min_length=2, max_length=3), amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/payee/committee/", summary="Traverse Graph and Find Intermediaries Between Payees and Committees")
def graph_traverse_intermediaries_payee_committee(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), type: str = Query(None, regex="independent|operating"), sup_opp: str = Query(None, min_length=1, max_length=1), purpose: str = None, amndt_ind: str = Query(None, min_length=1, max_length=2), gt: int = None, lte: int = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/tweeter/source/", summary="Traverse Graph and Find Intermediaries Between Tweeters and Sources")
def graph_traverse_intermediaries_tweeter_source(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/source/tweeter/", summary="Traverse Graph and Find Intermediaries Between Sources and Tweeters")
def graph_traverse_intermediaries_source_tweeter(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/buyer/page/", summary="Traverse Graph and Find Intermediaries Between Buyers and Pages")
def graph_traverse_intermediaries_buyer_page(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/intermediaries/page/buyer/", summary="Traverse Graph and Find Intermediaries Between Pages and Buyers")
def graph_traverse_intermediaries_page_buyer(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), ids2: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
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

@router.get("/relationships/contribution/contributor/", summary="Traverse Graph and Find Contributors for Contributions")
def graph_traverse_relationships_contribution_contributor(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_relationships_contribution_contributor, ids=ids, skip=skip, limit=limit))

@router.get("/relationships/contribution/recipient/", summary="Traverse Graph and Find Recipients for Contributions")
def graph_traverse_relationships_contribution_recipient(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
    try:
        ids = [int(i) for i in ids.split(",")]
    except:
        ids = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_relationships_contribution_recipient, ids=ids, skip=skip, limit=limit))
