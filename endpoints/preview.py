from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver, es, db
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.query import preview as query

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/data/preview",
    tags=["preview"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# preview entities
#########################################################

@router.get("/organization/committee/", summary="Preview Committees")
def data_preview_organization_committee(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        return query.data_preview_organization_committee(es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, count=count)
    return []

@router.get("/organization/employer/", summary="Preview Employers")
def data_preview_organization_employer(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        return query.data_preview_organization_employer(es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, count=count)
    return []

@router.get("/person/candidate/", summary="Preview Candidates")
def data_preview_person_candidate(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        return query.data_preview_person_candidate(es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, count=count)
    return []

@router.get("/person/donor/", summary="Preview Donors")
def data_preview_person_donor(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        return query.data_preview_person_donor(es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, count=count)
    return []

@router.get("/job/", summary="Preview Jobs")
def data_preview_job(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        return query.data_preview_job(es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, count=count)
    return []

@router.get("/topic/", summary="Preview Topics")
def data_preview_topic(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), count: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    elements = []
    for term in clean["include"]["terms"] or []:
        for exclude in clean["exclude"]["terms"] or []:
            if term in exclude or exclude in term:
                continue
        elements.append({
            "term": term,
            "id": None
        })
    for id in clean["include"]["ids"] or []:
        for exclude in clean["exclude"]["ids"] or []:
            if id in exclude:
                continue
        elements.append({
            "term": None,
            "id": id
        })
    if count is True:
        return [{"count": len(elements)}]
    return elements
