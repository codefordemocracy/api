from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import List
from uuid import UUID

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies import helpers
from .dependencies.analytics import log_endpoint
from .dependencies.cypher import find as cypher
from .dependencies.models import PaginationConfig

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/find",
    tags=["find"],
    dependencies=[Depends(get_auth), Depends(log_endpoint)],
)

#########################################################
# define models
#########################################################

class GraphFindElementsIDBody(BaseModel):
    nodes: List[int] = Field(...)
    edges: List[int] = Field(None)
    pagination: PaginationConfig = PaginationConfig()

class GraphFindElementsUUIDBody(BaseModel):
    nodes: List[UUID] = Field(...)
    edges: List[UUID] = Field(None)
    pagination: PaginationConfig = PaginationConfig()

#########################################################
# find graph elements
#########################################################

@router.post("/elements/id/", summary="Find Graph Elements by ID")
def graph_find_elements_id(body: GraphFindElementsIDBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_find_elements_id,
            nodes=body.nodes,
            edges=body.edges,
            skip=body.pagination.skip, limit=body.pagination.limit
        ))

@router.post("/elements/uuid/", summary="Find Graph Elements by UUID")
def graph_find_elements_uuid(body: GraphFindElementsUUIDBody):
    with driver.session() as neo4j:
        return helpers.format_graph(neo4j.read_transaction(cypher.graph_find_elements_uuid,
            nodes=[str(x) for x in body.nodes],
            edges=[str(x) for x in body.edges],
            skip=body.pagination.skip, limit=body.pagination.limit
        ))
