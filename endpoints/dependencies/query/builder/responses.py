def determine_histogram_interval(mindate, maxdate):
    delta = maxdate-mindate
    if delta.days <= 180:
        return "day"
    elif delta.days <= 1095:
        return "week"
    else:
        return "month"

def get_response(es, index, q, skip, limit, count, histogram, date_field=None, mindate=None, maxdate=None, filter_path=None):
    if count is True:
        response = es.count(index=index, body=q)
        try:
            return [{"count": response["count"]}]
        except:
            return []
    elif histogram is True:
        q["aggs"] = {
            "dates": {
                "date_histogram": {
                    "field": date_field,
                    "calendar_interval": determine_histogram_interval(mindate, maxdate),
                    "time_zone": "America/New_York"
                }
            }
        }
        response = es.search(index=index, body=q, filter_path=["aggregations"])
        try:
            return response["aggregations"]["dates"]["buckets"]
        except:
            return []
    else:
        q["from"] = skip
        q["size"] = limit
        response = es.search(index=index, body=q, filter_path=filter_path)
        try:
            elements = []
            for hit in response["hits"]["hits"]:
                elements.append(hit["_source"])
            return elements
        except:
            return []
    return []
