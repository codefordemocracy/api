import datetime
from starlette.requests import Request

from .connections import es

async def log_endpoint(request: Request):
    es.index(index="log_endpoints", body={
        "obj": {
            "endpoint": request.url.path,
            "user-agent": request.headers.get("user-agent"),
            "body": await request.json()
        },
        "context": {
            "logged": datetime.datetime.now(datetime.timezone.utc)
        }
    })
