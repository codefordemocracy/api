import datetime

def log_query(body, es):
    if body.histogram is False and body.count is False:
        obj = body.dict()
        obj.pop("histogram")
        obj.pop("count")
        record = {
            "obj": obj,
            "context": {
                "logged": datetime.datetime.now(datetime.timezone.utc)
            }
        }
        es.index(index="log_queries", body=record)
