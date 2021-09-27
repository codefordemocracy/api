from fastapi import FastAPI, Request
from fastapi.security import HTTPBasic
from fastapi.templating import Jinja2Templates
from fastapi.responses import ORJSONResponse

from endpoints import check, find, search, traverse, uncover, browse, preview, calculate, analyze

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

app.include_router(check.router)
app.include_router(find.router)
app.include_router(search.router)
app.include_router(traverse.router)
app.include_router(uncover.router)
app.include_router(browse.router)
app.include_router(preview.router)
app.include_router(calculate.router)
app.include_router(analyze.router)

#########################################################
# serve pages
#########################################################

templates = Jinja2Templates(directory="templates")

@app.get("/", include_in_schema=False)
def route_home(request: Request):
    return templates.TemplateResponse("home.html.j2", {"request": request})

@app.get("/view/status/", include_in_schema=False)
def route_view_status(request: Request):
    return templates.TemplateResponse("status.html.j2", {"request": request})

#########################################################
# serve data to front end
#########################################################

@app.get("/api/status/articles/", include_in_schema=False)
def route_api_status_articles(request: Request):
    return check.status_check_data_articles()

@app.get("/api/status/ads/", include_in_schema=False)
def route_api_status_ads(request: Request):
    return check.status_check_data_ads()

@app.get("/api/status/contributions/", include_in_schema=False)
def route_api_status_contributions(request: Request):
    return check.status_check_data_contributions()

@app.get("/api/status/lobbying/", include_in_schema=False)
def route_api_status_lobbying(request: Request):
    return check.status_check_data_lobbying()

@app.get("/api/status/990/", include_in_schema=False)
def route_api_status_990(request: Request):
    return check.status_check_data_990()
