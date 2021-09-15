from fastapi import FastAPI, Request
from fastapi.security import HTTPBasic
from fastapi.templating import Jinja2Templates
from fastapi.responses import ORJSONResponse

from endpoints import find, search, traverse, uncover, browse, preview, calculate

#########################################################
# initialize app
#########################################################

app = FastAPI(
    title="Code for Democracy",
    description="""This API helps you access the data behind Code for Democracy's workflows. Use it to explore documents, audit analyses, or build your own apps.
    <dl>
        <dt>[Read Documentation](https://docs.codefordemocracy.org/data/api/)</dt>
        <dt>[View on GitHub](https://github.com/codefordemocracy/api/)</dt>
    </dl>
    """,
    version="0.0.1",
    docs_url="/view/endpoints/",
    redoc_url=None,
    default_response_class=ORJSONResponse
)

#########################################################
# configure routes
#########################################################

app.include_router(find.router)
app.include_router(search.router)
app.include_router(traverse.router)
app.include_router(uncover.router)
app.include_router(browse.router)
app.include_router(preview.router)
app.include_router(calculate.router)

#########################################################
# serve homepage
#########################################################

templates = Jinja2Templates(directory="templates")

@app.get("/", include_in_schema=False)
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
