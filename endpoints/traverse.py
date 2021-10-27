from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies import helpers
from .dependencies.cypher import traverse as cypher
from .dependencies.models import PaginationConfig, DatesConfig
from .dependencies.models import GraphCandidateAttributesConfig, GraphCommitteeAttributesConfig, GraphDonorAttributesConfig, GraphSourceAttributesConfig, GraphContributionAttributesConfig, GraphExpenditureAttributesConfig

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
# define models
#########################################################

# configs

class GraphAssociationsEndpointsConfig(BaseModel):
    ids: List[int] = Field(...)
    ids2: List[int] = Field(None)

class GraphIntermediariesEndpointsConfig(BaseModel):
    ids: List[int] = Field(...)
    ids2: List[int] = Field(...)

class GraphIntermediariesLinkagesExpendituresConfig(BaseModel):
    type: str = Field("linkage", regex="linkage|expenditure")
    expenditures: GraphExpenditureAttributesConfig = GraphExpenditureAttributesConfig()

class GraphIntermediariesContributionsConfig(BaseModel):
    contributions: GraphContributionAttributesConfig = GraphContributionAttributesConfig()

class GraphIntermediariesExpendituresConfig(BaseModel):
    expenditures: GraphExpenditureAttributesConfig = GraphExpenditureAttributesConfig()

class GraphIntermediariesContributionsExpendituresConfig(BaseModel):
    type: str = Field("contribution", regex="contribution|expenditure")
    contributions: GraphContributionAttributesConfig = GraphContributionAttributesConfig()
    expenditures: GraphExpenditureAttributesConfig = GraphExpenditureAttributesConfig()

# bodies

class GraphTraverseNeighborsBody(BaseModel):
    nodes: List[int] = Field(...)
    labels: List[str] = Field(None)
    pagination: PaginationConfig = PaginationConfig()

class GraphTraverseAssociationsBaseBody(BaseModel):
    nodes: GraphAssociationsEndpointsConfig
    pagination: PaginationConfig = PaginationConfig()

class GraphTraverseAssociationsDatesBody(GraphTraverseAssociationsBaseBody):
    dates: DatesConfig = DatesConfig()

class GraphTraverseIntermediariesBaseBody(BaseModel):
    nodes: GraphIntermediariesEndpointsConfig
    pagination: PaginationConfig = PaginationConfig()

class GraphTraverseIntermediariesDatesBody(GraphTraverseIntermediariesBaseBody):
    dates: DatesConfig = DatesConfig()

class GraphTraverseAssociationsCandidateCommitteeBody(GraphTraverseAssociationsDatesBody):
    committees: GraphCommitteeAttributesConfig = GraphCommitteeAttributesConfig()
    intermediaries: GraphIntermediariesLinkagesExpendituresConfig = GraphIntermediariesLinkagesExpendituresConfig()

class GraphTraverseAssociationsCommitteeCandidateBody(GraphTraverseAssociationsDatesBody):
    candidates: GraphCandidateAttributesConfig = GraphCandidateAttributesConfig()
    intermediaries: GraphIntermediariesLinkagesExpendituresConfig = GraphIntermediariesLinkagesExpendituresConfig()

class GraphTraverseAssociationsCommitteeCommitteeBody(GraphTraverseAssociationsDatesBody):
    committees: GraphCommitteeAttributesConfig = GraphCommitteeAttributesConfig()
    intermediaries: GraphIntermediariesContributionsExpendituresConfig = GraphIntermediariesContributionsExpendituresConfig()

class GraphTraverseAssociationsCommitteeDonorBody(GraphTraverseAssociationsDatesBody):
    donors: GraphDonorAttributesConfig = GraphDonorAttributesConfig()

class GraphTraverseAssociationsNodeCommitteeBody(GraphTraverseAssociationsDatesBody):
    committees: GraphCommitteeAttributesConfig = GraphCommitteeAttributesConfig()

class GraphTraverseAssociationsNodeCandidateBody(GraphTraverseAssociationsDatesBody):
    candidates: GraphCandidateAttributesConfig = GraphCandidateAttributesConfig()

class GraphTraverseAssociationsNodeSourceBody(GraphTraverseAssociationsDatesBody):
    sources: GraphSourceAttributesConfig = GraphSourceAttributesConfig()

class GraphTraverseExpenditureIntermediariesBody(GraphTraverseIntermediariesDatesBody):
    intermediaries: GraphIntermediariesExpendituresConfig = GraphIntermediariesExpendituresConfig()

class GraphTraverseContributionIntermediariesBody(GraphTraverseIntermediariesDatesBody):
    intermediaries: GraphIntermediariesContributionsConfig = GraphIntermediariesContributionsConfig()

class GraphTraverseIntermediariesCommitteeCommitteeBody(GraphTraverseIntermediariesDatesBody):
    intermediaries: GraphIntermediariesContributionsExpendituresConfig = GraphIntermediariesContributionsExpendituresConfig()
    candidates: GraphCandidateAttributesConfig = GraphCandidateAttributesConfig()

class GraphTraverseIntermediariesCommitteePayeeBody(GraphTraverseIntermediariesDatesBody):
    intermediaries: GraphIntermediariesLinkagesExpendituresConfig = GraphIntermediariesLinkagesExpendituresConfig()

class GraphTraverseRelationshipsContribution(BaseModel):
    nodes: List[int] = Field(...)
    pagination: PaginationConfig = PaginationConfig()

#########################################################
# traverse graph
#########################################################

@router.post("/neighbors/", summary="Traverse Graph and Find Neighbors")
def graph_traverse_neighbors(body: GraphTraverseNeighborsBody):
    if body.labels is not None:
        body.labels = ["OR b:" + i for i in body.labels]
        body.labels[0] = body.labels[0].replace("OR ", "")
        body.labels = (" ").join(body.labels)
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_neighbors,
        ids=body.nodes,
        labels=body.labels,
        skip=body.pagination.skip, limit=body.pagination.limit
    ))

# associations - candidates

@router.post("/associations/candidate/committee/", summary="Traverse Graph and Find Associations Between Candidates and Committees")
def graph_traverse_associations_candidate_committee(body: GraphTraverseAssociationsCandidateCommitteeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_candidate_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cmte_pty_affiliation=body.committees.cmte_pty_affiliation, cmte_dsgn=body.committees.cmte_dsgn, cmte_tp=body.committees.cmte_tp, org_tp=body.committees.org_tp,
            intermediaries=body.intermediaries.type,
                sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, amndt_ind=body.intermediaries.expenditures.amndt_ind, gt=body.intermediaries.expenditures.gt, lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/associations/candidate/tweeter/", summary="Traverse Graph and Find Associations Between Candidates and Tweeters")
def graph_traverse_associations_candidate_tweeter(body: GraphTraverseAssociationsBaseBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_candidate_tweeter,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit
        ))

# associations - committees

@router.post("/associations/committee/candidate/", summary="Traverse Graph and Find Associations Between Committees and Candidates")
def graph_traverse_associations_committee_candidate(body: GraphTraverseAssociationsCommitteeCandidateBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_candidate,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cand_pty_affiliation=body.candidates.cand_pty_affiliation, cand_office=body.candidates.cand_office, cand_office_st=body.candidates.cand_office_st, cand_office_district=body.candidates.cand_office_district, cand_election_yr=body.candidates.cand_election_yr, cand_ici=body.candidates.cand_ici,
            intermediaries=body.intermediaries.type,
                sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, amndt_ind=body.intermediaries.expenditures.amndt_ind, gt=body.intermediaries.expenditures.gt, lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/associations/committee/committee/", summary="Traverse Graph and Find Associations Between Committees and Committees")
def graph_traverse_associations_committee_committee(body: GraphTraverseAssociationsCommitteeCommitteeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cmte_pty_affiliation=body.committees.cmte_pty_affiliation, cmte_dsgn=body.committees.cmte_dsgn, cmte_tp=body.committees.cmte_tp, org_tp=body.committees.org_tp,
            intermediaries=body.intermediaries.type,
                direction=body.intermediaries.contributions.direction, transaction_tp=body.intermediaries.contributions.transaction_tp, transaction_pgi=body.intermediaries.contributions.transaction_pgi, rpt_tp=body.intermediaries.contributions.rpt_tp, contribution_amndt_ind=body.intermediaries.contributions.amndt_ind, contribution_gt=body.intermediaries.contributions.gt, contribution_lte=body.intermediaries.contributions.lte,
                sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, expenditure_amndt_ind=body.intermediaries.expenditures.amndt_ind, expenditure_gt=body.intermediaries.expenditures.gt, expenditure_lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/associations/committee/donor/", summary="Traverse Graph and Find Associations Between Committees and Donors")
def graph_traverse_associations_committee_donor(body: GraphTraverseAssociationsCommitteeDonorBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_donor,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            employer=body.donors.employer, occupation=body.donors.occupation, state=body.donors.state, zip_code=body.donors.zip_code, entity_tp=body.donors.entity_tp,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/associations/committee/payee/", summary="Traverse Graph and Find Associations Between Committees and Payees")
def graph_traverse_associations_committee_payee(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_committee_payee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# associations - donors

@router.post("/associations/donor/committee/", summary="Traverse Graph and Find Associations Between Donors and Committees")
def graph_traverse_associations_donor_committee(body: GraphTraverseAssociationsNodeCommitteeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_donor_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cmte_pty_affiliation=body.committees.cmte_pty_affiliation, cmte_dsgn=body.committees.cmte_dsgn, cmte_tp=body.committees.cmte_tp, org_tp=body.committees.org_tp,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# associations - payees

@router.post("/associations/payee/committee/", summary="Traverse Graph and Find Associations Between Payees and Committees")
def graph_traverse_associations_payee_committee(body: GraphTraverseAssociationsNodeCommitteeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_payee_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cmte_pty_affiliation=body.committees.cmte_pty_affiliation, cmte_dsgn=body.committees.cmte_dsgn, cmte_tp=body.committees.cmte_tp, org_tp=body.committees.org_tp,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# associations - tweeters

@router.post("/associations/tweeter/candidate/", summary="Traverse Graph and Find Associations Between Tweeters and Candidates")
def graph_traverse_associations_tweeter_candidate(body: GraphTraverseAssociationsNodeCandidateBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_tweeter_candidate,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cand_pty_affiliation=body.candidates.cand_pty_affiliation, cand_office=body.candidates.cand_office, cand_office_st=body.candidates.cand_office_st, cand_office_district=body.candidates.cand_office_district, cand_election_yr=body.candidates.cand_election_yr, cand_ici=body.candidates.cand_ici,
            skip=body.pagination.skip, limit=body.pagination.limit
        ))

@router.post("/associations/tweeter/source/", summary="Traverse Graph and Find Associations Between Tweeters and Sources")
def graph_traverse_associations_tweeter_source(body: GraphTraverseAssociationsNodeSourceBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_tweeter_source,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            bias_score=body.sources.bias_score, factually_questionable_flag=body.sources.factually_questionable_flag, conspiracy_flag=body.sources.conspiracy_flag, hate_group_flag=body.sources.hate_group_flag, propaganda_flag=body.sources.propaganda_flag, satire_flag=body.sources.satire_flag,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# associations - sources

@router.post("/associations/source/tweeter/", summary="Traverse Graph and Find Associations Between Sources and Tweeters")
def graph_traverse_associations_source_tweeter(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_source_tweeter,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# associations - buyers

@router.post("/associations/buyer/page/", summary="Traverse Graph and Find Associations Between Buyers and Pages")
def graph_traverse_associations_buyer_page(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_buyer_page,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# associations - pages

@router.post("/associations/page/buyer/", summary="Traverse Graph and Find Associations Between Pages and Buyers")
def graph_traverse_associations_page_buyer(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_associations_page_buyer,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - candidates

@router.post("/intermediaries/candidate/committee/", summary="Traverse Graph and Find Intermediaries Between Candidates and Committees")
def graph_traverse_intermediaries_candidate_committee(body: GraphTraverseExpenditureIntermediariesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_candidate_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, amndt_ind=body.intermediaries.expenditures.amndt_ind, gt=body.intermediaries.expenditures.gt, lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - committees

@router.post("/intermediaries/committee/candidate/", summary="Traverse Graph and Find Intermediaries Between Committees and Candidates")
def graph_traverse_intermediaries_committee_candidate(body: GraphTraverseExpenditureIntermediariesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_candidate,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, amndt_ind=body.intermediaries.expenditures.amndt_ind, gt=body.intermediaries.expenditures.gt, lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/intermediaries/committee/committee/", summary="Traverse Graph and Find Intermediaries Between Committees and Committees")
def graph_traverse_intermediaries_committee_committee(body: GraphTraverseIntermediariesCommitteeCommitteeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            cand_pty_affiliation=body.candidates.cand_pty_affiliation, cand_office=body.candidates.cand_office, cand_office_st=body.candidates.cand_office_st, cand_office_district=body.candidates.cand_office_district, cand_election_yr=body.candidates.cand_election_yr, cand_ici=body.candidates.cand_ici,
            intermediaries=body.intermediaries.type,
                direction=body.intermediaries.contributions.direction, transaction_tp=body.intermediaries.contributions.transaction_tp, transaction_pgi=body.intermediaries.contributions.transaction_pgi, rpt_tp=body.intermediaries.contributions.rpt_tp, contribution_amndt_ind=body.intermediaries.contributions.amndt_ind, contribution_gt=body.intermediaries.contributions.gt, contribution_lte=body.intermediaries.contributions.lte,
                sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, expenditure_amndt_ind=body.intermediaries.expenditures.amndt_ind, expenditure_gt=body.intermediaries.expenditures.gt, expenditure_lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/intermediaries/committee/donor/", summary="Traverse Graph and Find Intermediaries Between Committees and Donors")
def graph_traverse_intermediaries_committee_donor(body: GraphTraverseContributionIntermediariesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_donor,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            transaction_tp=body.intermediaries.transaction_tp, transaction_pgi=body.intermediaries.transaction_pgi, rpt_tp=body.intermediaries.rpt_tp, amndt_ind=body.intermediaries.amndt_ind, gt=body.intermediaries.gt, lte=body.intermediaries.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

@router.post("/intermediaries/committee/payee/", summary="Traverse Graph and Find Intermediaries Between Committees and Payees")
def graph_traverse_intermediaries_committee_payee(body: GraphTraverseIntermediariesCommitteePayeeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_committee_payee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, amndt_ind=body.intermediaries.amndt_ind, gt=body.intermediaries.expenditures.gt, lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - donors

@router.post("/intermediaries/donor/committee/", summary="Traverse Graph and Find Intermediaries Between Donors and Committees")
def graph_traverse_intermediaries_donor_committee(body: GraphTraverseContributionIntermediariesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_donor_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            transaction_tp=body.intermediaries.transaction_tp, transaction_pgi=body.intermediaries.transaction_pgi, rpt_tp=body.intermediaries.rpt_tp, amndt_ind=body.intermediaries.amndt_ind, gt=body.intermediaries.gt, lte=body.intermediaries.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - payees

@router.post("/intermediaries/payee/committee/", summary="Traverse Graph and Find Intermediaries Between Payees and Committees")
def graph_traverse_intermediaries_payee_committee(body: GraphTraverseIntermediariesCommitteePayeeBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_payee_committee,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            sup_opp=body.intermediaries.expenditures.sup_opp, purpose=body.intermediaries.expenditures.purpose, amndt_ind=body.intermediaries.amndt_ind, gt=body.intermediaries.expenditures.gt, lte=body.intermediaries.expenditures.lte,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - tweeters

@router.post("/intermediaries/tweeter/source/", summary="Traverse Graph and Find Intermediaries Between Tweeters and Sources")
def graph_traverse_intermediaries_tweeter_source(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_tweeter_source,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - sources

@router.post("/intermediaries/source/tweeter/", summary="Traverse Graph and Find Intermediaries Between Sources and Tweeters")
def graph_traverse_intermediaries_source_tweeter(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_source_tweeter,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - buyers

@router.post("/intermediaries/buyer/page/", summary="Traverse Graph and Find Intermediaries Between Buyers and Pages")
def graph_traverse_intermediaries_buyer_page(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_buyer_page,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# intermediaries - pages

@router.post("/intermediaries/page/buyer/", summary="Traverse Graph and Find Intermediaries Between Pages and Buyers")
def graph_traverse_intermediaries_page_buyer(body: GraphTraverseAssociationsDatesBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_intermediaries_page_buyer,
            ids=body.nodes.ids, ids2=body.nodes.ids2,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day,
        ))

# relationships

@router.post("/relationships/contribution/contributor/", summary="Traverse Graph and Find Contributors for Contributions")
def graph_traverse_relationships_contribution_contributor(body: GraphTraverseRelationshipsContribution):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_relationships_contribution_contributor,
            ids=body.nodes,
            skip=body.pagination.skip, limit=body.pagination.limit
        ))

@router.post("/relationships/contribution/recipient/", summary="Traverse Graph and Find Recipients for Contributions")
def graph_traverse_relationships_contribution_recipient(body: GraphTraverseRelationshipsContribution):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_traverse_relationships_contribution_recipient,
            ids=body.nodes,
            skip=body.pagination.skip, limit=body.pagination.limit
        ))
