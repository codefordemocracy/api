from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List

from .dependencies.authentication import get_auth
from .dependencies.connections import driver, es, db
from .dependencies import helpers
from .dependencies.query import calculate as query
from .dependencies.models import PaginationConfig, DatesConfig, DataListConfig

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
# define models
#########################################################

class DataCalculateFiltersAmountConfig(BaseModel):
    min: float = Field(None)
    max: float = Field(None)

class DataCalculateFiltersConfig(BaseModel):
    amount: DataCalculateFiltersAmountConfig = DataCalculateFiltersAmountConfig()

class DataCalculateBaseBody(BaseModel):
    lists: List[str] = Field(None)
    pagination: PaginationConfig = PaginationConfig()
    dates: DatesConfig = DatesConfig()
    count: bool = Field(False)
    histogram: bool = Field(False)

class DataCalculateRecipeArticleBody(DataCalculateBaseBody):
    template: str = Field(..., regex="PMYZ|WdMv|RasK|EBli|GSmB")
    orderby: str = Field(None, regex="date")
    orderdir: str = Field("desc", regex="asc|desc")

class DataCalculateRecipeAdBody(DataCalculateBaseBody):
    template: str = Field(..., regex="D3WE|BuW8|P2HG|N7Jk|Jphg|8HcR")
    orderby: str = Field(None, regex="date")
    orderdir: str = Field("desc", regex="asc|desc")

class DataCalculateRecipeContributionBody(DataCalculateBaseBody):
    template: str = Field(..., regex="ReqQ|VqHR|DXhw|dFMy|KWYZ|IQL2|WK3K|NcFz|m4YC|Bs5W|KR64|7v4P|6peF|F7Xn|T5xv|F2mS|gXjA")
    filters: DataCalculateFiltersConfig = DataCalculateFiltersConfig()
    orderby: str = Field(None, regex="amount|date")
    orderdir: str = Field("desc", regex="asc|desc")

class DataCalculateRecipeLobbyingBody(DataCalculateBaseBody):
    template: str = Field(..., regex="wLvp|kMER|MJdb|PLWg|QJeb|nNKT|PjyR|WGb3|MK93|A3ue|rXwv|i5xq|V5Gh|3Nrt|Q23x|Hsqk|JCXA|7EyP")
    orderby: str = Field(None, regex="amount|date")
    orderdir: str = Field("desc", regex="asc|desc")

class DataCalculateRecipe990Body(DataCalculateBaseBody):
    template: str = Field(..., regex="GCv2|P34n|K23r|mFF7|9q84")
    orderby: str = Field(None, regex="date")
    orderdir: str = Field("desc", regex="asc|desc")

#########################################################
# calculate recipes
#########################################################

@router.post("/recipe/article/", summary="Calculate Recipe for Articles")
def data_calculate_recipe_article(body: DataCalculateRecipeArticleBody):
    clean = helpers.prepare_lists(body.lists, db)
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_article(body.template, es,
            include = clean["include"], exclude = clean["exclude"],
            skip=body.pagination.skip, limit=body.pagination.limit,
            mindate=mindate, maxdate=maxdate,
            orderby=body.orderby, orderdir=body.orderdir,
            count=body.count,
            histogram=body.histogram
        )
    return []

@router.post("/recipe/ad/", summary="Calculate Recipe for Ads")
def data_calculate_recipe_ad(body: DataCalculateRecipeAdBody):
    clean = helpers.prepare_lists(body.lists, db)
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_ad(body.template, es,
            include = clean["include"], exclude = clean["exclude"],
            skip=body.pagination.skip, limit=body.pagination.limit,
            mindate=mindate, maxdate=maxdate,
            orderby=body.orderby, orderdir=body.orderdir,
            count=body.count,
            histogram=body.histogram
        )
    return []

@router.post("/recipe/contribution/", summary="Calculate Recipe for Contributions")
def data_calculate_recipe_contribution(body: DataCalculateRecipeContributionBody):
    clean = helpers.prepare_lists(body.lists, db)
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_contribution(body.template, es,
            include = clean["include"], exclude = clean["exclude"],
            skip=body.pagination.skip, limit=body.pagination.limit,
            mindate=mindate, maxdate=maxdate,
            filters=body.filters.dict(),
            orderby=body.orderby, orderdir=body.orderdir,
            count=body.count,
            histogram=body.histogram
        )
    return []

@router.post("/recipe/lobbying/", summary="Calculate Recipe for Lobbying Activity")
def data_calculate_recipe_lobbying(body: DataCalculateRecipeLobbyingBody):
    clean = helpers.prepare_lists(body.lists, db)
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        if body.template in ["wLvp", "kMER", "MJdb"]:
            return query.data_calculate_recipe_lobbying_disclosures(body.template, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=body.pagination.skip, limit=body.pagination.limit,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=body.count,
                histogram=body.histogram
            )
        elif body.template in ["PLWg", "QJeb", "nNKT"]:
            return query.data_calculate_recipe_lobbying_disclosures_nested(body.template, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=body.pagination.skip, limit=body.pagination.limit,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=body.count,
                histogram=body.histogram
            )
        elif body.template in ["A3ue", "Hsqk"]:
            return query.data_calculate_recipe_lobbying_contributions_nested(body.template, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=body.pagination.skip, limit=body.pagination.limit,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=body.count,
                histogram=body.histogram
            )
        elif body.template in ["PjyR", "WGb3", "MK93", "V5Gh", "3Nrt", "Q23x"]:
            if body.template in ["PjyR", "V5Gh"]:
                template2 = "wLvp"
            elif body.template in ["WGb3", "3Nrt"]:
                template2 = "kMER"
            elif body.template in ["MK93", "Q23x"]:
                template2 = "MJdb"
            disclosures = query.data_calculate_recipe_lobbying_disclosures(template2, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=0, limit=10000,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=False,
                histogram=False,
                collapse="processed.registrant.senate_id.keyword"
            )
            clean["include"]["ids"][0] = [d["registrant_senate_id"] for d in disclosures]
            return query.data_calculate_recipe_lobbying_contributions_nested(body.template, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=body.pagination.skip, limit=body.pagination.limit,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=body.count,
                histogram=body.histogram
            )
        elif body.template in ["rXwv", "i5xq", "JCXA", "7EyP"]:
            if body.template in ["rXwv", "JCXA"]:
                template2 = "QJeb"
            elif body.template in ["i5xq", "7EyP"]:
                template2 = "nNKT"
            disclosures = query.data_calculate_recipe_lobbying_disclosures(template2, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=0, limit=10000,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=False,
                histogram=False,
                collapse="processed.registrant.senate_id.keyword"
            )
            lobbyist_names = query.data_calculate_recipe_lobbying_disclosures_nested(template2, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=0, limit=10000,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=False,
                histogram=False,
                collapse="child.lobbyist.name.keyword"
            )
            lobbyist_ids = query.data_calculate_recipe_lobbying_disclosures_nested(template2, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=0, limit=10000,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=False,
                histogram=False,
                collapse="child.lobbyist.id"
            )
            clean["include"]["terms"] = [[], []]
            clean["include"]["ids"] = [[], []]
            clean["exclude"]["terms"] = [[], []]
            clean["exclude"]["ids"] = [[], []]
            clean["include"]["ids"][0] = [d["registrant_senate_id"] for d in disclosures]
            clean["include"]["terms"][1] = [d["lobbyist_name"] for d in lobbyist_names]
            clean["include"]["ids"][1] = [d["lobbyist_id"] for d in lobbyist_ids]
            clean["include"]["ids"][0] = list(set(clean["include"]["ids"][0]))
            clean["include"]["terms"][1] = list(set(clean["include"]["terms"][1]))
            clean["include"]["ids"][1] = list(set(clean["include"]["ids"][1]))
            clean["include"]["terms"][1] = [term.replace(".", "").replace("-", " ") for term in clean["include"]["terms"][1]]
            return query.data_calculate_recipe_lobbying_contributions_nested(body.template, es,
                include = clean["include"], exclude = clean["exclude"],
                skip=body.pagination.skip, limit=body.pagination.limit,
                mindate=mindate, maxdate=maxdate,
                orderby=body.orderby, orderdir=body.orderdir,
                count=body.count,
                histogram=body.histogram
            )
    return []

@router.post("/recipe/990/", summary="Calculate Recipe for IRS 990s")
def data_calculate_recipe_990(body: DataCalculateRecipe990Body):
    clean = helpers.prepare_lists(body.lists, db)
    if clean["include"]["terms"] is not None or clean["include"]["ids"] is not None:
        mindate = datetime.datetime(body.dates.min.year, body.dates.min.month, body.dates.min.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        maxdate = datetime.datetime(body.dates.max.year, body.dates.max.month, body.dates.max.day, 0, 0, 0, 0, pytz.timezone('US/Eastern'))
        return query.data_calculate_recipe_990(body.template, es,
            include = clean["include"], exclude = clean["exclude"],
            skip=body.pagination.skip, limit=body.pagination.limit,
            mindate=mindate, maxdate=maxdate,
            orderby=body.orderby, orderdir=body.orderdir,
            count=body.count,
            histogram=body.histogram
        )
    return []
