from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver, es, db
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.query import calculate as query

import datetime
import pytz

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/data/calculate",
    tags=["calculate"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# calculate recipes
#########################################################

@router.get("/recipe/ad/", summary="Calculate Recipe for Ads")
def data_calculate_recipe_ad(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, template: str = Query(..., regex="D3WE|BuW8|N7Jk|P2HG|8HcR"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="amount"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_ad(template, es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

@router.get("/recipe/contribution/", summary="Calculate Recipe for Contributions")
def data_calculate_recipe_contribution(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, template: str = Query(..., regex="ReqQ|NcFz|m4YC|7v4P|T5xv|Bs5W|6peF|F2mS|IQL2|P3JF|VqHR"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="amount|date"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_contribution(template, es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

@router.get("/recipe/lobbying/", summary="Calculate Recipe for Lobbying Activity")
def data_calculate_recipe_lobbying(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, template: str = Query(..., regex="kMER|wLvp|MJdb|WGb3|PjyR|MK93|3Nrt|V5Gh|Q23x"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="date"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        if template in ["kMER", "wLvp", "MJdb"]:
            return query.data_calculate_recipe_lobbying_disclosures(template, es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
        elif template in ["WGb3", "PjyR", "MK93", "3Nrt", "V5Gh", "Q23x"]:
            if template in ["WGb3", "3Nrt"]:
                template2 = "kMER"
            elif template in ["PjyR", "V5Gh"]:
                template2 = "wLvp"
            elif template in ["MK93", "Q23x"]:
                template2 = "MJdb"
            disclosures = query.data_calculate_recipe_lobbying_disclosures(template2, es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=False, histogram=False, concise=True)
            clean["include"]["ids"][0] = [d["registrant_senate_id"] for d in disclosures]
            return query.data_calculate_recipe_lobbying_contributions(template, es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []

@router.get("/recipe/990/", summary="Calculate Recipe for IRS 990s")
def data_calculate_recipe_990(lists: str = None, include_terms: str = None, include_ids: str = None, exclude_terms: str = None, exclude_ids: str = None, template: str = Query(..., regex="K23r|GCv2|P34n"), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000), min_year: int = Query(get_years()["default"]["min"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), max_year: int = Query(get_years()["default"]["max"], ge=get_years()["calendar"]["min"], le=get_years()["calendar"]["max"]), min_month: int = Query(1, ge=1, le=12), max_month: int = Query(12, ge=1, le=12), min_day: int = Query(1, ge=1, le=31), max_day: int = Query(31, ge=1, le=31), orderby: str = Query(None, regex="amount"), orderdir: str = Query("desc", regex="asc|desc"), count: bool = False, histogram: bool = False):
    clean = helpers.prepare_lists(lists, include_terms, include_ids, exclude_terms, exclude_ids, db)
    # grab elements
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(min_year, min_month, min_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(max_year, max_month, max_day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_990(template, es, include_terms=clean["include"]["terms"], include_ids=clean["include"]["ids"], exclude_terms=clean["exclude"]["terms"], exclude_ids=clean["exclude"]["ids"], skip=skip, limit=limit, mindate=mindate, maxdate=maxdate, orderby=orderby, orderdir=orderdir, count=count, histogram=histogram)
    return []
