from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies import helpers
from .dependencies.analytics import log_endpoint
from .dependencies.cypher import uncover as cypher
from .dependencies.models import PaginationConfig, DatesConfig

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/uncover",
    tags=["uncover"],
    dependencies=[Depends(get_auth), Depends(log_endpoint)],
)

#########################################################
# define models
#########################################################

class GraphUncoverContributorsBody(BaseModel):
    nodes: List[int] = Field(...)
    labels: List[str] = Field(None)
    min_transaction_amt: int = Field(None, ge=0, le=999999999)
    pagination: PaginationConfig = PaginationConfig()
    dates: DatesConfig = DatesConfig()

#########################################################
# uncover graph insights
#########################################################

# Uncover contributors

@router.post("/contributors/", summary="Uncover Contributors to Nodes")
def graph_uncover_contributors(body: GraphUncoverContributorsBody):
    if body.labels is not None:
        body.labels = ["OR d:" + i for i in body.labels]
        body.labels[0] = body.labels[0].replace("OR ", "")
        body.labels = (" ").join(body.labels)
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_uncover_contributors,
            ids=body.nodes,
            labels=body.labels,
            min_transaction_amt=body.min_transaction_amt,
            skip=body.pagination.skip, limit=body.pagination.limit,
            min_year=body.dates.min.year, max_year=body.dates.max.year, min_month=body.dates.min.month, max_month=body.dates.max.month, min_day=body.dates.min.day, max_day=body.dates.max.day
        ))
