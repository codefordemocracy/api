from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from uuid import UUID

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies import helpers
from .dependencies.cypher import analyze as cypher
from .dependencies.models import PaginationConfig, DatesConfig

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/data/analyze",
    tags=["analyze"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# define models
#########################################################

class DataAnalyzeUUIDsConfig(BaseModel):
    a: UUID = Field(...)
    b: UUID = Field(...)

class DataAnalyzeCountsConfig(BaseModel):
    a: int = Field(None)
    b: int = Field(None)
    total: int = Field(None)

class DataAnalyzeShareConfig(BaseModel):
    a: float = Field(None)

class DataAnalyzeBaseBody(BaseModel):
    dates: DatesConfig = DatesConfig()

class DataAnalyzeNodeBaseBody(DataAnalyzeBaseBody):
    node: UUID = Field(...)

class DataAnalyzeNodesBaseBody(DataAnalyzeBaseBody):
    nodes: DataAnalyzeUUIDsConfig

class DataAnalyzeNodesWithCountsBaseBody(DataAnalyzeNodesBaseBody):
    counts: DataAnalyzeCountsConfig = DataAnalyzeCountsConfig()

class DataAnalyzeNodesShareBaseBody(DataAnalyzeNodesBaseBody):
    totals: DataAnalyzeShareConfig = DataAnalyzeShareConfig()

#########################################################
# analyze elements
#########################################################

@router.post("/count/in/", summary="Analyze Count of Unique Sources for Inflows of Money")
def data_analyze_count_in(body: DataAnalyzeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/count/in/candidate/", summary="Analyze Count of Unique Sources for Inflows of Money for a Candidate")
def data_analyze_count_in_candidate(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in_candidate,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/count/in/committee/", summary="Analyze Count of Unique Sources for Inflows of Money for a Committee")
def data_analyze_count_in_committee(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_in_committee,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/count/out/", summary="Analyze Count of Unique Targets for Outflows of Money")
def data_analyze_count_out(body: DataAnalyzeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/count/out/donor/", summary="Analyze Count of Unique Targets for Outflows of Money for a Donor")
def data_analyze_count_out_donor(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out_donor,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/count/out/committee/", summary="Analyze Count of Unique Targets for Outflows of Money for a Committee")
def data_analyze_count_out_committee(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_count_out_committee,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/affinity/in/candidate/candidate/", summary="Analyze Affinity Between Candidate and Candidate Based on Inflows of Money")
def data_analyze_affinity_in_candidate_candidate(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_in_candidate,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_in_candidate,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_candidate_candidate,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/in/candidate/committee/", summary="Analyze Affinity Between Candidate and Committee Based on Inflows of Money")
def data_analyze_affinity_in_candidate_committee(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_in_candidate,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_in_committee,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_candidate_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/in/committee/committee/", summary="Analyze Affinity Between Committee and Committee Based on Inflows of Money")
def data_analyze_affinity_in_committee_committee(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_in_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_in_committee,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_committee_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/in/committee/candidate/", summary="Analyze Affinity Between Committee and Candidate Based on Inflows of Money")
def data_analyze_affinity_in_committee_candidate(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_in_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_in_candidate,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_in_committee_candidate,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/out/committee/committee/", summary="Analyze Affinity Between Donor and Committee Based on Outflows of Money")
def data_analyze_affinity_out_committee_committee(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_out_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_out_committee,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_committee_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/out/committee/donor/", summary="Analyze Affinity Between Donor and Donor Based on Outflows of Money")
def data_analyze_affinity_out_committee_donor(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_out_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_out_donor,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_committee_donor,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/out/donor/committee/", summary="Analyze Affinity Between Donor and Committee Based on Outflows of Money")
def data_analyze_affinity_out_donor_committee(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_out_donor,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_out_committee,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_donor_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/affinity/out/donor/donor/", summary="Analyze Affinity Between Donor and Donor Based on Outflows of Money")
def data_analyze_affinity_out_donor_donor(body: DataAnalyzeNodesWithCountsBaseBody):
    with driver.session() as neo4j:
        if body.counts.total is None:
            body.counts.total = neo4j.read_transaction(cypher.data_analyze_count_in,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.a is None:
            body.counts.a = neo4j.read_transaction(cypher.data_analyze_count_out_donor,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        if body.counts.b is None:
            body.counts.b = neo4j.read_transaction(cypher.data_analyze_count_out_donor,
                uuid=body.nodes.b,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        count_both = neo4j.read_transaction(cypher.data_analyze_count_out_donor_donor,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_affinity(body.counts.a, body.counts.b, count_both, body.counts.total)

@router.post("/sum/revenue/candidate/", summary="Analyze the Sum of Revenue for a Candidate")
def data_analyze_sum_revenue_candidate(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/sum/revenue/committee/", summary="Analyze the Sum of Revenue for a Committee")
def data_analyze_sum_revenue_committee(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/sum/wallet/committee/", summary="Analyze the Sum of Wallet for a Committee")
def data_analyze_sum_wallet_committee(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/sum/wallet/donor/", summary="Analyze the Sum of Wallet for a Donor")
def data_analyze_sum_wallet_donor(body: DataAnalyzeNodeBaseBody):
    with driver.session() as neo4j:
        return neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor,
            uuid=body.node,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )

@router.post("/share/revenue/candidate/committee/", summary="Analyze the Share of Revenue that a Committee Comprises for a Candidate")
def data_analyze_share_revenue_candidate_committee(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/revenue/candidate/donor/", summary="Analyze the Share of Revenue that a Donor Comprises for a Candidate")
def data_analyze_share_revenue_candidate_donor(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_candidate_donor,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/revenue/committee/committee/", summary="Analyze the Share of Revenue that a Committee Comprises for a Committee")
def data_analyze_share_revenue_committee_committee(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/revenue/committee/donor/", summary="Analyze the Share of Revenue that a Donor Comprises for a Committee")
def data_analyze_share_revenue_committee_donor(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_revenue_committee_donor,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/wallet/committee/candidate/", summary="Analyze the Share of Wallet that a Candidate Comprises for a Committee")
def data_analyze_share_wallet_committee_candidate(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee_candidate,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/wallet/committee/committee/", summary="Analyze the Share of Wallet that a Committee Comprises for a Committee")
def data_analyze_share_wallet_committee_committee(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_committee_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/wallet/donor/candidate/", summary="Analyze the Share of Wallet that a Candidate Comprises for a Donor")
def data_analyze_share_wallet_donor_candidate(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor_candidate,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)

@router.post("/share/wallet/donor/committee/", summary="Analyze the Share of Wallet that a Committee Comprises for a Donor")
def data_analyze_share_wallet_donor_committee(body: DataAnalyzeNodesShareBaseBody):
    with driver.session() as neo4j:
        if body.totals.a is None:
            body.totals.a = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor,
                uuid=body.nodes.a,
                min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
            )
        sum = neo4j.read_transaction(cypher.data_analyze_sum_wallet_donor_committee,
            uuid=body.nodes.a, uuid2=body.nodes.b,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        )
    return helpers.calc_share(sum, body.totals.a)
