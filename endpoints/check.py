from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List

from .dependencies.authentication import get_auth
from .dependencies.connections import es
from .dependencies.query import check as query

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/status/check",
    tags=["check"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# check status of data
#########################################################

@router.get("/data/articles/", summary="Check Status of Articles Data")
def status_check_data_articles():
    return query.status_check_data_articles(es)

@router.get("/data/ads/", summary="Check Status of Ads Data")
def status_check_data_ads():
    return query.status_check_data_ads(es)

@router.get("/data/contributions/", summary="Check Status of Contributions Data")
def status_check_data_contributions():
    return query.status_check_data_contributions(es)

@router.get("/data/expenditures/", summary="Check Status of Expenditures Data")
def status_check_data_expenditures():
    return query.status_check_data_expenditures(es)

@router.get("/data/lobbying/", summary="Check Status of Lobbying Data")
def status_check_data_lobbying():
    return query.status_check_data_lobbying(es)

@router.get("/data/990/", summary="Check Status of IRS 990 Data")
def status_check_data_990():
    return query.status_check_data_990(es)
