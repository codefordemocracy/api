from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.cypher import analyze as cypher

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/data/analyze",
    tags=["analyze"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# analyze elements
#########################################################

@router.get("/count/in/", summary="Analyze Count of Unique Sources for Inflows of Money")
def data_analyze_count_in(min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/count/in/candidate/", summary="Analyze Count of Unique Sources for Inflows of Money for a Candidate")
def data_analyze_count_in_candidate(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/count/in/committee/", summary="Analyze Count of Unique Sources for Inflows of Money for a Committee")
def data_analyze_count_in_committee(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/count/out/", summary="Analyze Count of Unique Targets for Outflows of Money")
def data_analyze_count_out(min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/count/out/donor/", summary="Analyze Count of Unique Targets for Outflows of Money for a Donor")
def data_analyze_count_out_donor(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/count/out/committee/", summary="Analyze Count of Unique Targets for Outflows of Money for a Committee")
def data_analyze_count_out_committee(uuid: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/affinity/in/candidate/candidate/", summary="Analyze Affinity Between Candidate and Candidate Based on Inflows of Money")
def data_analyze_affinity_in_candidate_candidate(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_candidate_candidate, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/in/candidate/committee/", summary="Analyze Affinity Between Candidate and Committee Based on Inflows of Money")
def data_analyze_affinity_in_candidate_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_candidate_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/in/committee/committee/", summary="Analyze Affinity Between Committee and Committee Based on Inflows of Money")
def data_analyze_affinity_in_committee_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_committee_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/in/committee/candidate/", summary="Analyze Affinity Between Committee and Candidate Based on Inflows of Money")
def data_analyze_affinity_in_committee_candidate(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_in, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_in_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_in_candidate, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_committee_candidate, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/out/committee/committee/", summary="Analyze Affinity Between Donor and Committee Based on Outflows of Money")
def data_analyze_affinity_out_committee_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_committee_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/out/committee/donor/", summary="Analyze Affinity Between Donor and Donor Based on Outflows of Money")
def data_analyze_affinity_out_committee_donor(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_committee_donor, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/out/donor/committee/", summary="Analyze Affinity Between Donor and Committee Based on Outflows of Money")
def data_analyze_affinity_out_donor_committee(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_committee, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_donor_committee, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/affinity/out/donor/donor/", summary="Analyze Affinity Between Donor and Donor Based on Outflows of Money")
def data_analyze_affinity_out_donor_donor(uuid_a: str, uuid_b: str, count_total: int = Query(None, ge=0), count_a: int = Query(None, ge=0), count_b: int = Query(None, ge=0), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if count_total is None:
            count_total = neo4j.read_transaction(cypher.data_analyze_count_out, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_a is None:
            count_a = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_a, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        if count_b is None:
            count_b = neo4j.read_transaction(cypher.data_analyze_count_out_donor, uuid=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_donor_donor, uuid=uuid_a, uuid2=uuid_b, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_affinity(count_a, count_b, count_both, count_total)

@router.get("/sum/revenue/candidate/", summary="Analyze the Sum of Revenue for a Candidate")
def data_analyze_sum_revenue_candidate(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/sum/revenue/committee/", summary="Analyze the Sum of Revenue for a Committee")
def data_analyze_sum_revenue_committee(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/sum/wallet/committee/", summary="Analyze the Sum of Wallet for a Committee")
def data_analyze_sum_wallet_committee(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/sum/wallet/donor/", summary="Analyze the Sum of Wallet for a Donor")
def data_analyze_sum_wallet_donor(uuid: str, uuid2: str, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)

@router.get("/share/revenue/candidate/committee/", summary="Analyze the Share of Revenue that a Committee Comprises for a Candidate")
def data_analyze_share_revenue_candidate_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/revenue/candidate/donor/", summary="Analyze the Share of Revenue that a Donor Comprises for a Candidate")
def data_analyze_share_revenue_candidate_donor(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate_donor, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/revenue/committee/committee/", summary="Analyze the Share of Revenue that a Committee Comprises for a Committee")
def data_analyze_share_revenue_committee_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/revenue/committee/donor/", summary="Analyze the Share of Revenue that a Donor Comprises for a Committee")
def data_analyze_share_revenue_committee_donor(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee_donor, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/wallet/committee/candidate/", summary="Analyze the Share of Wallet that a Candidate Comprises for a Committee")
def data_analyze_share_wallet_committee_candidate(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee_candidate, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/wallet/committee/committee/", summary="Analyze the Share of Wallet that a Committee Comprises for a Committee")
def data_analyze_share_wallet_committee_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/wallet/donor/candidate/", summary="Analyze the Share of Wallet that a Candidate Comprises for a Donor")
def data_analyze_share_wallet_donor_candidate(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor_candidate, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)

@router.get("/share/wallet/donor/committee/", summary="Analyze the Share of Wallet that a Committee Comprises for a Donor")
def data_analyze_share_wallet_donor_committee(uuid: str, uuid2: str, sum_total: int = None, min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31)):
    with driver.session() as neo4j:
        if sum_total is None:
            sum_total = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor, uuid=uuid, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor_committee, uuid=uuid, uuid2=uuid2, min_year=min_year, max_year=max_year, min_month=min_month, max_month=max_month, min_day=min_day, max_day=max_day)
    return helpers.calc_share(sum, sum_total)
