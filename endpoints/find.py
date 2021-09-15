from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies import helpers
from .dependencies.cypher import find as cypher

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/find",
    tags=["find"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# find graph elements
#########################################################

@router.get("/elements/id/", summary="Find Graph Elements by ID")
def graph_find_elements_id(nodes: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), edges: str = Query(None, regex="^[0-9]+(,[0-9]+)*$")):
    try:
        nodes = [int(n) for n in nodes.split(",")]
    except:
        nodes = None
    try:
        edges = [int(n) for n in edges.split(",")]
    except:
        edges = None
    if nodes is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_find_elements_id, nodes=nodes, edges=edges))

@router.get("/elements/uuid/", summary="Find Graph Elements by UUID")
def graph_find_elements_uuid(nodes: str, edges: str = None):
    try:
        nodes = nodes.split(",")
    except:
        nodes = None
    try:
        edges = edges.split(",")
    except:
        edges = None
    if nodes is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_find_elements_uuid, nodes=nodes, edges=edges))
