from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies import helpers
from .dependencies.cypher import search as cypher
from .dependencies.models import PaginationConfig, DatesConfig
from .dependencies.models import GraphCandidateAttributesConfig, GraphCommitteeAttributesConfig, GraphDonorAttributesConfig, GraphTweeterAttributesConfig, GraphSourceAttributesConfig

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/search",
    tags=["search"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# define models
#########################################################

class GraphSearchBaseBody(BaseModel):
    context: bool = False
    pagination: PaginationConfig = PaginationConfig()

class GraphSearchDatesBody(GraphSearchBaseBody):
    dates: DatesConfig = DatesConfig()

class GraphSearchNamesDatesBody(GraphSearchDatesBody):
    name: str = Field(None)

class GraphSearchCandidatesBody(GraphSearchNamesDatesBody):
    attributes: GraphCandidateAttributesConfig = GraphCandidateAttributesConfig()

class GraphSearchCommitteesBody(GraphSearchNamesDatesBody):
    attributes: GraphCommitteeAttributesConfig = GraphCommitteeAttributesConfig()

class GraphSearchDonorsBody(GraphSearchNamesDatesBody):
    attributes: GraphDonorAttributesConfig = GraphDonorAttributesConfig()

class GraphSearchTweetersBody(GraphSearchNamesDatesBody):
    attributes: GraphTweeterAttributesConfig = GraphTweeterAttributesConfig()
    candidates: GraphCandidateAttributesConfig = GraphCandidateAttributesConfig()

class GraphSearchSourcesBody(GraphSearchDatesBody):
    domain: str = Field(None)
    attributes: GraphSourceAttributesConfig = GraphSourceAttributesConfig()

#########################################################
# search for entities
#########################################################

@router.post("/candidates/", summary="Search for Candidates")
def graph_search_candidates(body: GraphSearchCandidatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_candidates,
            cand_name=body.name, cand_pty_affiliation=body.attributes.cand_pty_affiliation, cand_office=body.attributes.cand_office, cand_office_st=body.attributes.cand_office_st, cand_office_district=body.attributes.cand_office_district, cand_election_yr=body.attributes.cand_election_yr, cand_ici=body.attributes.cand_ici,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            concise=False
        ))

@router.post("/committees/", summary="Search for Committees")
def graph_search_committees(body: GraphSearchCommitteesBody):
    if body.name is not None:
        body.name = "\""+body.name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_committees,
            cmte_nm=body.name, cmte_pty_affiliation=body.attributes.cmte_pty_affiliation, cmte_dsgn=body.attributes.cmte_dsgn, cmte_tp=body.attributes.cmte_tp,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))

@router.post("/donors/", summary="Search for Donors")
def graph_search_donors(body: GraphSearchDonorsBody):
    if body.attributes.employer is not None:
        body.attributes.employer = "\""+body.attributes.employer+"\""
    if body.attributes.occupation is not None:
        body.attributes.occupation = "\""+body.attributes.occupation+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_donors,
            name=body.name, employer=body.attributes.employer, occupation=body.attributes.occupation, state=body.attributes.state, zip_code=body.attributes.zip_code, entity_tp=body.attributes.entity_tp,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))

@router.post("/payees/", summary="Search for Payees")
def graph_search_payees(body: GraphSearchNamesDatesBody):
    if body.name is not None:
        body.name = "\""+body.name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_payees,
            name=body.name,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))

@router.post("/tweeters/", summary="Search for Tweeters")
def graph_search_tweeters(body: GraphSearchTweetersBody):
    if body.name is not None:
        body.name = "\""+body.name+"\""
    if body.attributes.username is not None:
        body.attributes.username = body.attributes.username[1:] if body.attributes.username.startswith("@") else body.attributes.username
        body.attributes.username = "\""+body.attributes.username+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_tweeters,
            name=body.name, username=body.attributes.username, candidate=body.attributes.candidate,
            cand_pty_affiliation=body.candidates.cand_pty_affiliation, cand_election_yr=body.candidates.cand_election_yr,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))

@router.post("/sources/", summary="Search for Sources")
def graph_search_sources(body: GraphSearchSourcesBody):
    if body.domain is not None:
        if body.domain.startswith("www."):
            body.domain = body.domain.split("www.",1)[1]
        body.domain = "\""+body.domain+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_sources,
            domain=body.domain, bias_score=body.attributes.bias_score, factually_questionable_flag=body.attributes.factually_questionable_flag, conspiracy_flag=body.attributes.conspiracy_flag, hate_group_flag=body.attributes.hate_group_flag, propaganda_flag=body.attributes.propaganda_flag, satire_flag=body.attributes.satire_flag,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))

@router.post("/buyers/", summary="Search for Buyers")
def graph_search_buyers(body: GraphSearchNamesDatesBody):
    if body.name is not None:
        body.name = "\""+body.name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_buyers,
            name=body.name,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))

@router.post("/pages/", summary="Search for Pages")
def graph_search_pages(body: GraphSearchNamesDatesBody):
    if body.name is not None:
        body.name = "\""+body.name+"\""
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_search_pages,
            name=body.name,
            context=body.context,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
            concise=False
        ))
