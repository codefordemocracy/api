from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List

from .dependencies.authentication import get_auth
from .dependencies.connections import driver, es, db
from .dependencies.query import preview as query
from .dependencies.models import PaginationConfig, DataListConfig

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/data/preview",
    tags=["preview"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# define models
#########################################################

class DataPreviewCandidateAttributesConfig(BaseModel):
    cand_pty_affiliation: List[str] = Field(None, min_length=3, max_length=3)
    cand_office: List[str] = Field(None, min_length=1, max_length=1)
    cand_office_st: List[str] = Field(None, min_length=2, max_length=2)
    cand_office_district: List[str] = Field(None, min_length=2, max_length=2)
    cand_election_yr: List[int] = Field(None, ge=1900, le=2100)
    cand_ici: List[str] = Field(None, min_length=1, max_length=1)

class DataPreviewCommitteeAttributesConfig(BaseModel):
    cmte_pty_affiliation: List[str] = Field(None, min_length=3, max_length=3)
    cmte_dsgn: List[str] = Field(None, min_length=1, max_length=1)
    cmte_tp: List[str] = Field(None, min_length=1, max_length=1)
    org_tp: List[str] = Field(None, min_length=1, max_length=1)

class DataPreviewDonorAttributesConfig(BaseModel):
    employer: List[str] = Field(None)
    occupation: List[str] = Field(None)
    state: List[str] = Field(None, min_length=2, max_length=2)
    zip_code: List[int] = Field(None, ge=500, le=99999)
    entity_tp: List[str] = Field(None, min_length=3, max_length=3)

class DataPreviewCommitteeListConfig(DataListConfig):
    filters: DataPreviewCommitteeAttributesConfig = DataPreviewCommitteeAttributesConfig()

class DataPreviewCandidateListConfig(DataListConfig):
    filters: DataPreviewCandidateAttributesConfig = DataPreviewCandidateAttributesConfig()

class DataPreviewDonorListConfig(DataListConfig):
    filters: DataPreviewDonorAttributesConfig = DataPreviewDonorAttributesConfig()

class DataPreviewBaseBody(BaseModel):
    pagination: PaginationConfig = PaginationConfig()
    count: bool = Field(False)

class DataPreviewEntityBody(DataPreviewBaseBody):
    include: DataListConfig = DataListConfig()
    exclude: DataListConfig = DataListConfig()

class DataPreviewCommitteeBody(DataPreviewBaseBody):
    include: DataPreviewCommitteeListConfig = DataPreviewCommitteeListConfig()
    exclude: DataPreviewCommitteeListConfig = DataPreviewCommitteeListConfig()

class DataPreviewCandidateBody(DataPreviewBaseBody):
    include: DataPreviewCandidateListConfig = DataPreviewCandidateListConfig()
    exclude: DataPreviewCandidateListConfig = DataPreviewCandidateListConfig()

class DataPreviewDonorBody(DataPreviewBaseBody):
    include: DataPreviewDonorListConfig = DataPreviewDonorListConfig()
    exclude: DataPreviewDonorListConfig = DataPreviewDonorListConfig()

#########################################################
# preview entities
#########################################################

@router.post("/organization/committee/", summary="Preview Committees")
def data_preview_organization_committee(body: DataPreviewCommitteeBody):
    if body.include.terms is not None or body.include.ids is not None or body.include.filters is not None:
        return query.data_preview_organization_committee(es,
            include_terms=body.include.terms, include_ids=body.include.ids, include_filters=body.include.filters.dict(),
            exclude_terms=body.exclude.terms, exclude_ids=body.exclude.ids, exclude_filters=body.exclude.filters.dict(),
            skip=body.pagination.skip, limit=body.pagination.limit,
            count=body.count
        )
    return []

@router.post("/organization/employer/", summary="Preview Employers")
def data_preview_organization_employer(body: DataPreviewEntityBody):
    if body.include.terms is not None or body.include.ids is not None:
        return query.data_preview_organization_employer(es,
            include_terms=body.include.terms, include_ids=body.include.ids,
            exclude_terms=body.exclude.terms, exclude_ids=body.exclude.ids,
            skip=body.pagination.skip, limit=body.pagination.limit,
            count=body.count
        )
    return []

@router.post("/person/candidate/", summary="Preview Candidates")
def data_preview_person_candidate(body: DataPreviewCandidateBody):
    if body.include.terms is not None or body.include.ids is not None or body.include.filters is not None:
        return query.data_preview_person_candidate(es,
            include_terms=body.include.terms, include_ids=body.include.ids, include_filters=body.include.filters.dict(),
            exclude_terms=body.exclude.terms, exclude_ids=body.exclude.ids, exclude_filters=body.exclude.filters.dict(),
            skip=body.pagination.skip, limit=body.pagination.limit,
            count=body.count
        )
    return []

@router.post("/person/donor/", summary="Preview Donors")
def data_preview_person_donor(body: DataPreviewDonorBody):
    if body.include.terms is not None or body.include.ids is not None or body.include.filters is not None:
        results = query.data_preview_person_donor_fec(es,
            include_terms=body.include.terms, include_ids=body.include.ids, include_filters=body.include.filters.dict(),
            exclude_terms=body.exclude.terms, exclude_ids=body.exclude.ids, exclude_filters=body.exclude.filters.dict(),
            skip=body.pagination.skip, limit=body.pagination.limit,
            count=body.count
        )
        if len(results) == 0:
            results = query.data_preview_person_donor_lobbying(es,
                include_terms=body.include.terms, include_ids=body.include.ids, include_filters=body.include.filters.dict(),
                exclude_terms=body.exclude.terms, exclude_ids=body.exclude.ids, exclude_filters=body.exclude.filters.dict(),
                skip=body.pagination.skip, limit=body.pagination.limit,
                count=body.count
            )
        return results
    return []

@router.post("/job/", summary="Preview Jobs")
def data_preview_job(body: DataPreviewEntityBody):
    if body.include.terms is not None or body.include.ids is not None:
        return query.data_preview_job(es,
            include_terms=body.include.terms, include_ids=body.include.ids,
            exclude_terms=body.exclude.terms, exclude_ids=body.exclude.ids,
            skip=body.pagination.skip, limit=body.pagination.limit,
            count=body.count
        )
    return []

@router.post("/topic/", summary="Preview Topics")
def data_preview_topic(body: DataPreviewEntityBody):
    elements = []
    for term in body.include.terms or []:
        for exclude in body.exclude.terms or []:
            if term in exclude or exclude in term:
                continue
        elements.append({
            "term": term,
            "id": None
        })
    for id in body.include.ids or []:
        for exclude in body.exclude.ids or []:
            if id in exclude:
                continue
        elements.append({
            "term": None,
            "id": id
        })
    if body.count is True:
        return [{"count": len(elements)}]
    return elements
