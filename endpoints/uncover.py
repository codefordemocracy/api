from fastapi import APIRouter, Depends, Query

from .dependencies.authentication import get_auth
from .dependencies.connections import driver
from .dependencies.defaults import get_years
from .dependencies import helpers
from .dependencies.cypher import uncover as cypher

#########################################################
# initialize route
#########################################################

router = APIRouter(
    prefix="/graph/uncover",
    tags=["uncover"],
    dependencies=[Depends(get_auth)],
)

#########################################################
# uncover graph insights
#########################################################

# Uncover contributors

@router.get("/contributors/", summary="Uncover contributors to nodes")
def graph_uncover_contributors(ids: str = Query(..., regex="^[0-9]+(,[0-9]+)*$"), labels: str = None, min_transaction_amt: int = Query(None, ge=1, le=999999999), skip: int = Query(0, ge=0), limit: int = Query(30, ge=0, le=1000)):
    try:
        ids = [int(id) for id in ids.split(",")]
    except:
        ids = None
    try:
        labels = ["OR d:" + label for label in labels.split(",")]
        labels[0] = labels[0].replace("OR ", "")
        labels = (" ").join(labels)
    except:
        labels = None
    if ids is not None:
        with driver.session() as neo4j:
            return helpers.format_graph(neo4j.read_transaction(cypher.graph_uncover_contributors, ids=ids, labels=labels, min_transaction_amt=min_transaction_amt, skip=skip, limit=limit))
