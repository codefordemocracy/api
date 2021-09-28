from pydantic import BaseModel, Field
from typing import List
import datetime

# general models

class PaginationConfig(BaseModel):
    skip: int = Field(0, ge=0)
    limit: int = Field(30, ge=0, le=1000)

class DatesConfig(BaseModel):
    min: datetime.date = Field(datetime.datetime.strptime('2020-01-01', '%Y-%m-%d').date())
    max: datetime.date = Field(datetime.datetime.now().date())

# graph models

class GraphCandidateAttributesConfig(BaseModel):
    cand_pty_affiliation: str = Field(None, min_length=3, max_length=3)
    cand_office: str = Field(None, min_length=1, max_length=1)
    cand_office_st: str = Field(None, min_length=2, max_length=2)
    cand_office_district: str = Field(None, min_length=2, max_length=2)
    cand_election_yr: int = Field(None, ge=1990, le=datetime.datetime.now().year)
    cand_ici: str = Field(None, min_length=1, max_length=1)

class GraphCommitteeAttributesConfig(BaseModel):
    cmte_pty_affiliation: str = Field(None, min_length=3, max_length=3)
    cmte_dsgn: str = Field(None, min_length=1, max_length=1)
    cmte_tp: str = Field(None, min_length=1, max_length=1)
    org_tp: str = Field(None, min_length=1, max_length=1)

class GraphDonorAttributesConfig(BaseModel):
    employer: str = Field(None)
    occupation: str = Field(None)
    state: str = Field(None, min_length=2, max_length=2)
    zip_code: int = Field(None, ge=500, le=99999)
    entity_tp: str = Field(None, min_length=3, max_length=3)

class GraphTweeterAttributesConfig(BaseModel):
    username: str = Field(None)
    candidate: bool = Field(True)

class GraphSourceAttributesConfig(BaseModel):
    bias_score: List[int] = Field(None)
    factually_questionable_flag: int = Field(None, ge=0, le=1)
    conspiracy_flag: int = Field(None, ge=0, le=1)
    hate_group_flag: int = Field(None, ge=0, le=1)
    propaganda_flag: int = Field(None, ge=0, le=1)
    satire_flag: int = Field(None, ge=0, le=1)

class GraphContributionAttributesConfig(BaseModel):
    direction: str = Field(None, regex="receipts|disbursements")
    transaction_tp: str = Field(None, min_length=2, max_length=3)
    transaction_pgi: str = Field(None, min_length=1, max_length=1)
    rpt_tp: str = Field(None, min_length=2, max_length=3)
    amndt_ind: str = Field(None, min_length=1, max_length=2)
    gt: int = Field(None)
    lte: int = Field(None)

class GraphExpenditureAttributesConfig(BaseModel):
    sup_opp: str = Field(None, min_length=1, max_length=1)
    purpose: str = Field(None)
    amndt_ind: str = Field(None, min_length=1, max_length=2)
    gt: int = Field(None)
    lte: int = Field(None)

# data models

class DataListConfig(BaseModel):
    terms: List[str] = Field(None)
    ids: List[str] = Field(None)
